# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import asyncio
import contextvars
import gc
import threading
import time
import traceback
import weakref
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Any, cast
from unittest import mock

import pytest
from botocore.exceptions import ClientError

from pyathena.filesystem.s3_executor import S3AioExecutor, S3Executor, S3ThreadPool, _S3PoolScope
from pyathena.util import RetryConfig, override, retry_api_call, stop_retries_on
from tests.pyathena.util import (
    EVENT_TIMEOUT,
    ThrottledOperation,
    forking,
    overlapping_calls,
    run_in_forked_child,
    shutdown_bounded,
)


class TestS3ThreadPool:
    def test_default_max_workers(self):
        # None takes the default of ThreadPoolExecutor, as documented.
        with ThreadPoolExecutor() as executor:
            assert S3ThreadPool().max_workers == executor._max_workers

    def test_run_beyond_default_executor(self):
        # GH-1159: the calls run in threads of the pool, so that max_workers,
        # not the default executor of the event loop, bounds them.
        call, state = overlapping_calls(4)
        pool = S3ThreadPool(4)

        async def main():
            asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(max_workers=1))
            await asyncio.gather(*[pool.run(call) for _ in range(6)])

        asyncio.run(main())
        pool.shutdown()

        assert state["peak"] == 4

    def test_run_context(self):
        # The call sees the context variables of the caller, as with
        # asyncio.to_thread().
        variable: contextvars.ContextVar[str] = contextvars.ContextVar("variable")
        pool = S3ThreadPool(1)

        async def main():
            variable.set("caller")
            return await pool.run(variable.get)

        assert asyncio.run(main()) == "caller"
        pool.shutdown()

    @pytest.mark.parametrize("shielded", [False, True])
    def test_run_cancelled_while_retrying(self, shielded):
        # GH-1203: cancelling the waiting task stops the retries of a running
        # call, unless it runs shielded. Without the stop, the backoff wait
        # would take at least EVENT_TIMEOUT seconds.
        operation = ThrottledOperation(block=True, failures=None if not shielded else 2)
        finished = threading.Event()
        pool = S3ThreadPool(1)

        def work():
            try:
                config = RetryConfig(attempt=3, multiplier=0 if shielded else EVENT_TIMEOUT)
                return retry_api_call(operation, config)
            finally:
                finished.set()

        async def main():
            run = pool.run_shielded if shielded else pool.run
            task = asyncio.ensure_future(run(work))
            await asyncio.to_thread(operation.started.wait, EVENT_TIMEOUT)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            operation.release.set()
            started = time.monotonic()
            assert await asyncio.to_thread(finished.wait, EVENT_TIMEOUT)
            return time.monotonic() - started

        try:
            assert asyncio.run(main()) < EVENT_TIMEOUT
        finally:
            operation.release.set()
            pool.shutdown()

        assert operation.calls == (3 if shielded else 1)

    def test_run_cancelled_after_return(self):
        # GH-1203: a cancellation that arrives after the call has returned does
        # not stop the work that the call left running, such as the parts of a
        # write in a transaction.
        operation = ThrottledOperation(block=True, failures=1)
        left = []
        go = threading.Event()
        pool = S3ThreadPool(1)
        submit = pool.submit

        def submit_and_cancel_after_return(fn, *args, **kwargs):
            future = submit(fn, *args, **kwargs)
            task = cast(asyncio.Task[None], asyncio.current_task())

            def cancel_after_return():
                # Runs once the task waits for the call, and blocks the event
                # loop until the call has returned, so that the task is
                # cancelled before it resumes.
                go.set()
                future.result(EVENT_TIMEOUT)
                task.cancel()

            asyncio.get_running_loop().call_soon(cancel_after_return)
            return future

        pool.submit = submit_and_cancel_after_return  # type: ignore[method-assign]

        def work():
            assert go.wait(EVENT_TIMEOUT)
            context = contextvars.copy_context()
            thread = threading.Thread(
                target=context.run,
                args=(retry_api_call, operation, RetryConfig(attempt=2, multiplier=0)),
            )
            thread.start()
            left.append(thread)

        async def main():
            task = asyncio.ensure_future(pool.run(work))
            with pytest.raises(asyncio.CancelledError):
                await task

        try:
            asyncio.run(main())
            operation.release.set()
            left[0].join(EVENT_TIMEOUT)
        finally:
            operation.release.set()
            pool.shutdown()

        assert operation.calls == 2

    @pytest.mark.parametrize("shielded", [False, True])
    def test_run_cancelled_before_start(self, shielded):
        # Cancelling the waiting task drops a call that has not started,
        # unless it runs shielded.
        events = []
        started = threading.Event()
        release = threading.Event()
        pool = S3ThreadPool(1)

        def work():
            started.set()
            release.wait(EVENT_TIMEOUT)

        running = pool.submit(work)
        submit = pool.submit

        async def main():
            submitted = asyncio.Event()

            def submit_and_notify(fn, *args, **kwargs):
                future = submit(fn, *args, **kwargs)
                submitted.set()
                return future

            pool.submit = submit_and_notify  # type: ignore[method-assign]
            run = pool.run_shielded if shielded else pool.run
            pending = asyncio.ensure_future(run(events.append, "pending finished"))
            assert await asyncio.to_thread(started.wait, EVENT_TIMEOUT)
            # The call waits for the only thread.
            await asyncio.wait_for(submitted.wait(), EVENT_TIMEOUT)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            release.set()
            await asyncio.wrap_future(running)

        asyncio.run(main())
        pool.shutdown()

        assert events == (["pending finished"] if shielded else [])

    def test_shutdown(self):
        # The calls submitted before the shutdown still run.
        events = []
        release = threading.Event()
        pool = S3ThreadPool(1)
        pool.submit(release.wait, EVENT_TIMEOUT)
        pending = pool.submit(events.append, "pending finished")
        pool.shutdown(wait=False)
        # The shutdown did not wait for the running call.
        assert not pending.done()
        with pytest.raises(RuntimeError):
            pool.submit(sum, [1, 2])
        release.set()
        pending.result(timeout=EVENT_TIMEOUT)

        assert events == ["pending finished"]

    def test_release_after_calls(self):
        # GH-1209: release() keeps the threads, and the max_workers bound,
        # until no call runs or waits, and then ends them.
        pool = S3ThreadPool(1)
        threads = []
        started, release = threading.Event(), threading.Event()

        def block():
            threads.append(threading.current_thread())
            started.set()
            release.wait(EVENT_TIMEOUT)

        first = pool.submit(block)
        assert started.wait(EVENT_TIMEOUT)
        pool.release()
        second = pool.submit(lambda: threads.append(threading.current_thread()))
        release.set()
        first.result(timeout=EVENT_TIMEOUT)
        second.result(timeout=EVENT_TIMEOUT)

        assert threads[1] is threads[0]
        threads[0].join(EVENT_TIMEOUT)
        assert not threads[0].is_alive()
        pool.shutdown()

    def test_kept_future_keeps_no_pool(self):
        # GH-1209: a future that the caller keeps after its call has ended does
        # not keep the pool, whose threads end once the pool is collected.
        pool = S3ThreadPool(1)
        threads = []
        release = threading.Event()

        def call():
            threads.append(threading.current_thread())
            release.wait(EVENT_TIMEOUT)

        # Pending while its callback is added, which the future then keeps.
        future = pool.submit(call)
        # Runs after the callback of the pool, which has then returned.
        ended = threading.Event()
        future.add_done_callback(lambda _: ended.set())
        release.set()
        assert ended.wait(EVENT_TIMEOUT)
        collected = weakref.ref(pool)
        del pool
        gc.collect()

        assert collected() is None
        threads[0].join(EVENT_TIMEOUT)
        assert not threads[0].is_alive()

    def test_release_after_interrupted_submit(self):
        # GH-1209: a call whose submit() an interrupt stopped after scheduling
        # it does not keep release() from ending the threads once it has ended.
        pool = S3ThreadPool(1)
        threads = []
        with (
            mock.patch.object(Future, "add_done_callback", side_effect=KeyboardInterrupt),
            pytest.raises(KeyboardInterrupt),
        ):
            pool.submit(lambda: threads.append(threading.current_thread()))
        # The calls start in order, so the interrupted one has ended.
        pool.submit(sum, [1, 2]).result(timeout=EVENT_TIMEOUT)
        pool.release()

        threads[0].join(EVENT_TIMEOUT)
        assert not threads[0].is_alive()
        pool.shutdown()

    @forking
    def test_fork(self):
        # A forked child, whose copy of the pool has no threads, creates new
        # ones, also if a thread of the parent held the lock of the pool.
        pool = S3ThreadPool(1)
        assert pool.submit(sum, [1, 2]).result(timeout=EVENT_TIMEOUT) == 3
        # No public call holds the lock across a fork; a thread of the parent
        # holds it while the main thread forks.
        held, release = threading.Event(), threading.Event()

        def hold_lock():
            with pool._lock:
                held.set()
                release.wait()

        holder = threading.Thread(target=hold_lock, daemon=True)
        holder.start()
        try:
            assert held.wait(EVENT_TIMEOUT)
            # A child whose pool waits for the threads of the parent would
            # not exit.
            code = run_in_forked_child(
                lambda: pool.submit(sum, [3, 4]).result(timeout=EVENT_TIMEOUT) == 7
            )
        finally:
            release.set()
            holder.join(EVENT_TIMEOUT)
            pool.shutdown()
        assert code == 0


class TestS3PoolScope:
    def test_window(self):
        # GH-1332: at most max_workers calls of a scope are in the shared
        # pool at once, as a file with its own pool of that size used to run.
        pool = S3ThreadPool(4)
        call, state = overlapping_calls(2)
        scope = _S3PoolScope(pool, 2)
        try:
            calls = [scope.submit(call) for _ in range(4)]
            wait(calls, timeout=EVENT_TIMEOUT)
        finally:
            shutdown_bounded(scope)
            pool.shutdown()
        assert all(c.done() for c in calls)
        assert state["peak"] == 2

    def test_waiting_call_context(self):
        # GH-1332: a call that waits for a place in the pool runs with the
        # context of its submitter, so that its stop scope reaches its
        # retries, although it enters the pool from the thread of the call
        # that ends before it.
        variable: contextvars.ContextVar[str] = contextvars.ContextVar("variable")
        stop = threading.Event()
        stop.set()
        operation = ThrottledOperation()
        release = threading.Event()

        def work():
            with pytest.raises(ClientError):
                retry_api_call(operation, RetryConfig(attempt=3, multiplier=0))
            return variable.get()

        pool = S3ThreadPool(2)
        scope = _S3PoolScope(pool, 1)
        try:
            blocking = scope.submit(release.wait, EVENT_TIMEOUT)
            variable.set("submitter")
            with stop_retries_on(stop):
                waiting = scope.submit(work)
            variable.set("other")
            release.set()
            assert waiting.result(EVENT_TIMEOUT) == "submitter"
            assert blocking.result(EVENT_TIMEOUT)
        finally:
            release.set()
            shutdown_bounded(scope)
            pool.shutdown()
        assert operation.calls == 1

    def test_shutdown_waits_for_own_calls(self):
        # GH-1332: shutting a scope down waits for its own calls, also those
        # that wait for a place, and not for those of another scope; the pool
        # keeps running for the other scopes.
        pool = S3ThreadPool(2)
        own_release, other_release = threading.Event(), threading.Event()
        own, other = _S3PoolScope(pool, 1), _S3PoolScope(pool, 1)
        events = []

        def slow_append(event):
            # Still running when a shutdown that does not wait returns.
            time.sleep(0.2)
            events.append(event)

        try:
            other_call = other.submit(other_release.wait, EVENT_TIMEOUT)
            own.submit(own_release.wait, EVENT_TIMEOUT)
            own.submit(slow_append, "waiting call")
            own_release.set()
            shutdown_bounded(own)
            assert events == ["waiting call"]
            assert not other_call.done()
            with pytest.raises(RuntimeError, match="after shutdown"):
                own.submit(sum, [1, 2])
            # The pool, whose other thread the other scope holds, still runs calls.
            assert pool.submit(sum, [1, 2]).result(EVENT_TIMEOUT) == 3
        finally:
            own_release.set()
            other_release.set()
            shutdown_bounded(other)
            pool.shutdown()
        assert other_call.result(EVENT_TIMEOUT)

    def test_cancel_waiting_call(self):
        # GH-1332: a call that waits for a place in the pool can be cancelled
        # through its future, and then never runs.
        pool = S3ThreadPool(2)
        scope = _S3PoolScope(pool, 1)
        release = threading.Event()
        events = []
        try:
            running = scope.submit(release.wait, EVENT_TIMEOUT)
            waiting = scope.submit(events.append, "waiting call")
            assert waiting.cancel()
            release.set()
            shutdown_bounded(scope)
        finally:
            release.set()
            pool.shutdown()
        assert running.result(EVENT_TIMEOUT)
        assert events == []


class TestS3AioExecutor:
    def test_loop_ignored(self):
        # GH-1332: the loop argument is ignored with a warning, and the calls
        # run in the pool without a running loop. They used to be dispatched
        # through the loop, which had to run.
        loop = asyncio.new_event_loop()
        try:
            with pytest.warns(DeprecationWarning, match="loop argument of S3AioExecutor"):
                executor = S3AioExecutor(loop=loop, max_workers=1)
            with executor:
                assert executor.submit(sum, [1, 2]).result(timeout=EVENT_TIMEOUT) == 3
        finally:
            loop.close()


class _DeferringExecutor(S3Executor):
    """Holds the submitted calls, which a test drops or fails before they run."""

    def __init__(self) -> None:
        self.submitted: list[Future[Any]] = []

    @override
    def submit(self, fn, *args, **kwargs):
        future: Future[Any] = Future()
        self.submitted.append(future)
        return future

    @override
    def shutdown(self, wait=True):
        pass


class TestS3Executor:
    def test_submit_context(self):
        # GH-1203: a callable runs with a copy of the context of its submitter,
        # so that the stop scope of the submitter reaches its retries.
        variable: contextvars.ContextVar[str] = contextvars.ContextVar("variable")
        stop = threading.Event()
        stop.set()
        operation = ThrottledOperation()

        def work():
            with pytest.raises(ClientError):
                retry_api_call(operation, RetryConfig(attempt=3, multiplier=0))
            return variable.get()

        variable.set("submitter")
        with S3ThreadPool(max_workers=1) as executor, stop_retries_on(stop):
            future: Future[str] = Future()
            executor.submit_to(future, work)
            assert future.result(EVENT_TIMEOUT) == "submitter"
        assert operation.calls == 1

    def test_submit_to(self):
        # The callable's outcome resolves the future of the caller.
        with S3ThreadPool(max_workers=1) as executor:
            succeeded: Future[int] = Future()
            failed: Future[int] = Future()
            executor.submit_to(succeeded, sum, [1, 2])
            executor.submit_to(failed, int, "x")
            wait([succeeded, failed], timeout=5)

        assert succeeded.result() == 3
        with pytest.raises(ValueError, match="invalid literal"):
            failed.result()

    def test_submit_to_failure_frees_arguments(self):
        # GH-1186: the arguments of a failed callable, such as the body of a
        # part, are freed when it fails, while the error keeps the frames of
        # its traceback. The traceback used to keep them, in a reference
        # cycle with the future until the garbage collector ran.
        class Body:
            pass

        def upload(body):
            raise PermissionError("Access Denied")

        body = Body()
        body_ref = weakref.ref(body)
        future: Future[None] = Future()
        gc.disable()
        try:
            with S3ThreadPool(max_workers=1) as executor:
                executor.submit_to(future, upload, body)
                del body
            error = future.exception()
            assert isinstance(error, PermissionError)
            assert traceback.extract_tb(error.__traceback__)[-1].name == "upload"
            assert body_ref() is None
        finally:
            gc.enable()

    def test_submit_to_cancelled_before_start(self):
        # Cancelling the future keeps a callable that has not started from running.
        events = []
        started = threading.Event()
        release = threading.Event()

        def work():
            started.set()
            release.wait(5)

        with S3ThreadPool(max_workers=1) as executor:
            executor.submit(work)
            pending: Future[None] = Future()
            executor.submit_to(pending, events.append, "pending finished")
            started.wait(5)
            assert pending.cancel()
            release.set()

        assert events == []
        assert pending.cancelled()

    def test_submit_to_dropped(self):
        # A callable that the executor drops before it starts is never run,
        # and the future of the caller is cancelled and settled, so that
        # wait() returns, instead of left pending.
        events = []
        executor = _DeferringExecutor()
        pending: Future[None] = Future()
        executor.submit_to(pending, events.append, "pending finished")
        (submitted,) = executor.submitted
        assert submitted.cancel()

        _, not_done = wait([pending], timeout=EVENT_TIMEOUT)
        assert not not_done
        assert pending.cancelled()
        assert events == []

    def test_submit_to_executor_failure(self):
        # An error that the executor reports before the callable starts is
        # set on the future of the caller.
        executor = _DeferringExecutor()
        future: Future[int] = Future()
        executor.submit_to(future, sum, [1, 2])
        (submitted,) = executor.submitted
        submitted.set_running_or_notify_cancel()
        submitted.set_exception(RuntimeError("cannot schedule new futures after shutdown"))

        with pytest.raises(RuntimeError, match="after shutdown"):
            future.result(timeout=EVENT_TIMEOUT)

    def test_submit_to_cancelled_executor_failure(self):
        # A future that the caller cancels before the executor reports the
        # error is marked as notified, so that wait() counts it as done.
        executor = _DeferringExecutor()
        future: Future[int] = Future()
        executor.submit_to(future, sum, [1, 2])
        assert future.cancel()
        (submitted,) = executor.submitted
        submitted.set_running_or_notify_cancel()
        submitted.set_exception(RuntimeError("cannot schedule new futures after shutdown"))

        _, not_done = wait([future], timeout=EVENT_TIMEOUT)
        assert not not_done
        assert future.cancelled()
