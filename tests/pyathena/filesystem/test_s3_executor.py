# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import asyncio
import contextvars
import gc
import os
import signal
import threading
import time
import traceback
import weakref
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import cast
from unittest import mock

import pytest
from botocore.exceptions import ClientError

from pyathena.filesystem.s3_executor import S3AioExecutor, S3AioThreadPool, S3ThreadPoolExecutor
from pyathena.util import RetryConfig, retry_api_call, stop_retries_on
from tests.pyathena.util import EVENT_TIMEOUT, ThrottledOperation, overlapping_calls


class TestS3AioThreadPool:
    def test_default_max_workers(self):
        # None takes the default of ThreadPoolExecutor, as documented.
        with ThreadPoolExecutor() as executor:
            assert S3AioThreadPool().max_workers == executor._max_workers

    def test_run_beyond_default_executor(self):
        # GH-1159: the calls run in threads of the pool, so that max_workers,
        # not the default executor of the event loop, bounds them.
        call, state = overlapping_calls(4)
        pool = S3AioThreadPool(4)

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
        pool = S3AioThreadPool(1)

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
        pool = S3AioThreadPool(1)

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
        pool = S3AioThreadPool(1)
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
        pool = S3AioThreadPool(1)

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
        pool = S3AioThreadPool(1)
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
        pool = S3AioThreadPool(1)
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

    @pytest.mark.skipif(not hasattr(os, "fork"), reason="os.fork() is not available.")
    @pytest.mark.filterwarnings("ignore:.*fork\\(\\) may lead to deadlocks:DeprecationWarning")
    def test_fork(self):
        # A forked child, whose copy of the pool has no threads, creates new
        # ones, also if a thread of the parent held the lock of the pool.
        pool = S3AioThreadPool(1)
        assert pool.submit(sum, [1, 2]).result(timeout=EVENT_TIMEOUT) == 3
        # No public call holds the lock across a fork; a thread of the parent
        # holds it while the main thread forks.
        held, release = threading.Event(), threading.Event()

        def hold_lock():
            with pool._lock:
                held.set()
                release.wait(EVENT_TIMEOUT)

        holder = threading.Thread(target=hold_lock)
        holder.start()
        try:
            assert held.wait(EVENT_TIMEOUT)
            pid = os.fork()
        finally:
            release.set()
        if pid == 0:
            # The child exits here, whatever happens, instead of running pytest.
            code = 1
            try:
                if pool.submit(sum, [3, 4]).result(timeout=EVENT_TIMEOUT) == 7:
                    code = 0
            finally:
                os._exit(code)
        status = (0, 0)
        try:
            # A child whose pool waits for the threads of the parent would
            # not exit.
            deadline = time.monotonic() + 2 * EVENT_TIMEOUT
            while (status := os.waitpid(pid, os.WNOHANG)) == (0, 0):
                assert time.monotonic() < deadline, "the child did not exit"
                time.sleep(0.05)
        finally:
            if status == (0, 0):
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
            holder.join(EVENT_TIMEOUT)
            pool.shutdown()
        assert os.waitstatus_to_exitcode(status[1]) == 0


class TestS3AioExecutor:
    def test_submit_context(self):
        # GH-1203: a function runs with the context of its submitter, as the
        # task that dispatches it does, so that the stop scope of the
        # submitter reaches its retries.
        variable: contextvars.ContextVar[str] = contextvars.ContextVar("variable")
        stop = threading.Event()
        stop.set()
        operation = ThrottledOperation()

        def work():
            with pytest.raises(ClientError):
                retry_api_call(operation, RetryConfig(attempt=3, multiplier=0))
            return variable.get()

        async def main():
            executor = S3AioExecutor(loop=asyncio.get_running_loop())

            def submit():
                variable.set("submitter")
                with stop_retries_on(stop):
                    return executor.submit(work).result(EVENT_TIMEOUT)

            variable.set("loop")
            result = await asyncio.to_thread(submit)
            executor.shutdown()
            return result

        assert asyncio.run(main()) == "submitter"
        assert operation.calls == 1

    def test_submit_beyond_default_executor(self):
        # GH-1159: max_workers functions run at once, whatever the size of the
        # default executor of the event loop. They used to run in it.
        call, state = overlapping_calls(3)

        async def main():
            loop = asyncio.get_running_loop()
            loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
            executor = S3AioExecutor(loop=loop, max_workers=3)
            futures = [executor.submit(call) for _ in range(3)]
            await asyncio.gather(*[asyncio.wrap_future(f) for f in futures])
            executor.shutdown()

        asyncio.run(main())

        assert state["peak"] == 3

    def test_submit_from_default_executor(self):
        # GH-1159: a function submitted from a thread of the default executor
        # of the event loop does not wait for that thread. With a default
        # executor of one thread, this used to deadlock a multipart write
        # through the loop's asyncio.to_thread().
        async def main():
            loop = asyncio.get_running_loop()
            loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
            executor = S3AioExecutor(loop=loop)
            # Waits in the only thread of the default executor, as a
            # multipart write waits for the creation of its upload.
            result = await asyncio.to_thread(
                lambda: executor.submit(sum, [1, 2]).result(EVENT_TIMEOUT)
            )
            executor.shutdown()
            return result

        assert asyncio.run(main()) == 3

    def test_shutdown_before_dispatch(self):
        # GH-1159: a function submitted before the shutdown runs, although it
        # reaches the pool of the executor only when the event loop runs its
        # task. An AioS3File in a transaction shuts its executor down when it
        # is closed, before its parts may have reached the pool.
        async def main():
            executor = S3AioExecutor(loop=asyncio.get_running_loop())
            future = executor.submit(sum, [1, 2])
            executor.shutdown(wait=False)
            with pytest.raises(RuntimeError, match="after shutdown"):
                executor.submit(sum, [3, 4])
            return await asyncio.wrap_future(future)

        assert asyncio.run(main()) == 3

    def test_shutdown_on_loop_thread(self):
        # Shutting down with wait=True on the thread of the event loop, such
        # as when a file is garbage collected there, does not wait for the
        # functions that only the loop dispatches; they still run.
        async def main():
            executor = S3AioExecutor(loop=asyncio.get_running_loop())
            future = executor.submit(sum, [1, 2])
            executor.shutdown(wait=True)
            assert not future.done()
            return await asyncio.wrap_future(future)

        assert asyncio.run(main()) == 3

    def test_shutdown_loop_stopped(self):
        # Shutting down with wait=True returns when the event loop stops
        # before it runs the task that shuts the pool down.
        loop = asyncio.new_event_loop()
        busy = threading.Event()
        release = threading.Event()

        def block():
            busy.set()
            release.wait(EVENT_TIMEOUT)
            # The loop exits after this callback, without running the task
            # that the shutdown schedules meanwhile.
            loop.stop()

        scheduled = threading.Event()
        run_coroutine_threadsafe = asyncio.run_coroutine_threadsafe

        def schedule(coro, loop):
            future = run_coroutine_threadsafe(coro, loop)
            scheduled.set()
            return future

        thread = threading.Thread(target=loop.run_forever)
        thread.start()
        try:
            loop.call_soon_threadsafe(block)
            assert busy.wait(EVENT_TIMEOUT)
            executor = S3AioExecutor(loop=loop)
            shut_down = threading.Event()
            waiter = threading.Thread(
                target=lambda: (executor.shutdown(wait=True), shut_down.set())
            )
            with mock.patch("asyncio.run_coroutine_threadsafe", side_effect=schedule):
                waiter.start()
                # The loop stops only after the task has been scheduled, so
                # that the shutdown waits for a task that never runs.
                assert scheduled.wait(EVENT_TIMEOUT)
            assert not shut_down.is_set()
            release.set()
            assert shut_down.wait(EVENT_TIMEOUT)
            waiter.join()
        finally:
            release.set()
            thread.join()
            # Runs the pending task, which would otherwise be dropped
            # unawaited when the loop is closed.
            loop.run_until_complete(asyncio.sleep(0))
            loop.close()

    def test_shutdown_interpreter_finalizing(self):
        # Shutting down with wait=True while the interpreter exits returns,
        # although the loop, whose daemon thread no longer runs, still
        # reports running.
        loop = asyncio.new_event_loop()
        busy = threading.Event()
        release = threading.Event()

        def block():
            busy.set()
            # Held until the shutdown has returned, so that the loop cannot
            # run the task that the shutdown waits for.
            release.wait(6 * EVENT_TIMEOUT)

        thread = threading.Thread(target=loop.run_forever)
        thread.start()
        try:
            loop.call_soon_threadsafe(block)
            assert busy.wait(EVENT_TIMEOUT)
            executor = S3AioExecutor(loop=loop)
            shut_down = threading.Event()
            waiter = threading.Thread(
                target=lambda: (executor.shutdown(wait=True), shut_down.set())
            )
            with mock.patch("pyathena.filesystem.s3_executor.sys.is_finalizing", return_value=True):
                waiter.start()
                assert shut_down.wait(EVENT_TIMEOUT)
            waiter.join()
        finally:
            release.set()
            loop.call_soon_threadsafe(loop.stop)
            thread.join()
            loop.run_until_complete(asyncio.sleep(0))
            loop.close()

    def test_submit_without_running_loop(self):
        with pytest.raises(RuntimeError, match="requires a running event loop"):
            S3AioExecutor().submit(sum, [1, 2])

    def test_cancel(self):
        # GH-976: as with ThreadPoolExecutor, a future can be cancelled only
        # before its function starts, so that wait() waits for a running one.
        events = []
        started = threading.Event()
        cancelled = threading.Event()

        def work():
            started.set()
            cancelled.wait(5)
            time.sleep(0.05)
            events.append("finished")

        def cancel(executor: S3AioExecutor) -> tuple[Future[None], Future[None]]:
            running = executor.submit(work)
            pending = executor.submit(events.append, "pending finished")
            started.wait(5)
            assert not running.cancel()
            assert pending.cancel()
            cancelled.set()
            _, not_done = wait([running, pending], timeout=5)
            assert not not_done
            events.append("waited")
            return running, pending

        async def main():
            # The function runs in the only thread of the executor, so that
            # the pending one waits for it, and cancel() in another thread.
            executor = S3AioExecutor(loop=asyncio.get_running_loop(), max_workers=1)
            return await asyncio.to_thread(cancel, executor)

        running, pending = asyncio.run(main())

        assert events == ["finished", "waited"]
        assert running.done()
        assert not running.cancelled()
        assert pending.cancelled()


class TestS3Executor:
    @pytest.mark.parametrize("executor_class", [S3ThreadPoolExecutor, S3AioThreadPool])
    def test_submit_context(self, executor_class):
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
        with executor_class(max_workers=1) as executor, stop_retries_on(stop):
            future: Future[str] = Future()
            executor.submit_to(future, work)
            assert future.result(EVENT_TIMEOUT) == "submitter"
        assert operation.calls == 1

    def test_submit_to(self):
        # The callable's outcome resolves the future of the caller.
        with S3ThreadPoolExecutor(max_workers=1) as executor:
            succeeded: Future[int] = Future()
            failed: Future[int] = Future()
            executor.submit_to(succeeded, sum, [1, 2])
            executor.submit_to(failed, int, "x")
            wait([succeeded, failed], timeout=5)

        assert succeeded.result() == 3
        with pytest.raises(ValueError, match="invalid literal"):
            failed.result()

    @pytest.mark.parametrize("executor_class", [S3ThreadPoolExecutor, S3AioThreadPool])
    def test_submit_to_failure_frees_arguments(self, executor_class):
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
            with executor_class(max_workers=1) as executor:
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

        with S3ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(work)
            pending: Future[None] = Future()
            executor.submit_to(pending, events.append, "pending finished")
            started.wait(5)
            assert pending.cancel()
            release.set()

        assert events == []
        assert pending.cancelled()

    def test_submit_to_loop_shutdown(self):
        # A callable that S3AioExecutor drops when its event loop shuts down
        # is never run, and its future, whether submit() returned it or the
        # caller passed it to submit_to(), is cancelled and settled, so that
        # wait() returns, instead of left pending.
        events = []
        started = threading.Event()
        settled = threading.Semaphore(0)

        def work():
            started.set()
            # Running until both pending futures are settled, so that their
            # callables cannot start before the shutdown.
            for _ in range(2):
                assert settled.acquire(timeout=EVENT_TIMEOUT)
            events.append("finished")

        pending: Future[None] = Future()
        pending.add_done_callback(lambda _: settled.release())

        async def main():
            # The pending callables wait for the only thread of the executor.
            executor = S3AioExecutor(loop=asyncio.get_running_loop(), max_workers=1)
            running = executor.submit(work)
            submitted = executor.submit(events.append, "submitted finished")
            submitted.add_done_callback(lambda _: settled.release())
            executor.submit_to(pending, events.append, "pending finished")
            assert await asyncio.to_thread(started.wait, EVENT_TIMEOUT)
            return running, submitted

        running, submitted = asyncio.run(main())

        _, not_done = wait([running, submitted, pending], timeout=EVENT_TIMEOUT)
        assert not not_done
        assert events == ["finished"]
        assert not running.cancelled()
        assert submitted.cancelled()
        assert pending.cancelled()

    def test_submit_to_executor_shut_down(self):
        # An error that S3AioExecutor reports before the callable starts is
        # set on the future of the caller.
        future: Future[int] = Future()

        async def main():
            executor = S3AioExecutor(loop=asyncio.get_running_loop())
            # Fails the dispatch of the callable to the pool.
            executor._pool.shutdown()
            executor.submit_to(future, sum, [1, 2])
            with pytest.raises(RuntimeError, match="after shutdown"):
                await asyncio.wait_for(asyncio.wrap_future(future), EVENT_TIMEOUT)

        asyncio.run(main())

        assert not future.cancelled()

    def test_submit_to_cancelled_executor_shut_down(self):
        # A future that the caller cancels before S3AioExecutor reports the
        # error is marked as notified, so that wait() counts it as done.
        future: Future[int] = Future()

        async def main():
            loop = asyncio.get_running_loop()
            executor = S3AioExecutor(loop=loop)
            # Fails the dispatch of the callable to the pool.
            executor._pool.shutdown()
            executor.submit_to(future, sum, [1, 2])
            assert future.cancel()
            with ThreadPoolExecutor(max_workers=1) as waiter:
                return await loop.run_in_executor(
                    waiter, lambda: wait([future], timeout=EVENT_TIMEOUT)
                )

        _, not_done = asyncio.run(main())

        assert not not_done
        assert future.cancelled()
