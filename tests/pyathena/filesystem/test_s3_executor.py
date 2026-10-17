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
import threading
import time
import traceback
import weakref
from concurrent.futures import Future, ThreadPoolExecutor, wait
from unittest import mock

import pytest
from botocore.exceptions import ClientError

from pyathena.filesystem.s3_executor import S3AioExecutor, S3AioThreadPool, S3ThreadPoolExecutor
from pyathena.util import RetryConfig, retry_api_call, stop_retries_on
from tests.pyathena.util import EVENT_TIMEOUT, ThrottledOperation, overlapping_calls


class TestS3AioThreadPool:
    def test_init(self):
        # None takes the default of ThreadPoolExecutor.
        cpus = getattr(os, "process_cpu_count", os.cpu_count)()
        assert S3AioThreadPool(None).max_workers == min(32, (cpus or 1) + 4)
        with pytest.raises(ValueError, match="max_workers must be greater than 0"):
            S3AioThreadPool(0)

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
        returned = threading.Event()
        left = []
        pool = S3AioThreadPool(1)

        def work():
            context = contextvars.copy_context()
            thread = threading.Thread(
                target=context.run,
                args=(retry_api_call, operation, RetryConfig(attempt=2, multiplier=0)),
            )
            thread.start()
            left.append(thread)
            returned.set()

        async def main():
            task = asyncio.ensure_future(pool.run(work))
            await asyncio.sleep(0)
            # Blocks the event loop, so that the task is cancelled before it
            # resumes after the call has returned, which follows the event at
            # once.
            assert returned.wait(EVENT_TIMEOUT)
            time.sleep(0.1)
            task.cancel()
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

        async def main():
            running = pool.submit(work)
            run = pool.run_shielded if shielded else pool.run
            pending = asyncio.ensure_future(run(events.append, "pending finished"))
            await asyncio.to_thread(started.wait, EVENT_TIMEOUT)
            # Lets the task submit the call, which waits for the thread.
            while not pending.done() and not pool._get_executor()._work_queue.qsize():
                await asyncio.sleep(0.01)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            release.set()
            await asyncio.wrap_future(running)

        asyncio.run(main())
        pool.shutdown()

        assert events == (["pending finished"] if shielded else [])

    def test_wait_shielded_cancelled(self):
        # GH-1186: cancelling the waiting task neither cancels the call nor
        # lets asyncio log its failure, which ends after the cancellation;
        # the caller can still wait for its future.
        started = threading.Event()
        release = threading.Event()
        pool = S3AioThreadPool(1)

        def work():
            started.set()
            # The finally block of the test always releases it.
            release.wait()
            raise PermissionError("Access Denied")

        async def main():
            logged = []
            asyncio.get_running_loop().set_exception_handler(
                lambda _, context: logged.append(context["message"])
            )
            running = pool.submit(work)
            try:
                waiting = asyncio.ensure_future(pool.wait_shielded(running))
                await asyncio.to_thread(started.wait, EVENT_TIMEOUT)
                # Lets the task start waiting before it is cancelled.
                deadline = time.monotonic() + EVENT_TIMEOUT
                while not running._done_callbacks:
                    assert time.monotonic() < deadline
                    await asyncio.sleep(0)
                waiting.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await waiting
            finally:
                release.set()
                # The worker has run the callbacks of the call once it is
                # shut down, and the event loop runs theirs on the next
                # iterations.
                await asyncio.to_thread(pool.shutdown, True)
            with pytest.raises(PermissionError, match="Access Denied"):
                running.result()
            # The callbacks of the future of the call keep its waiter.
            del waiting, running
            gc.collect()
            for _ in range(3):
                await asyncio.sleep(0)
            return logged

        assert asyncio.run(main()) == []

    def test_shutdown(self):
        # The calls submitted before the shutdown still run.
        events = []
        release = threading.Event()
        pool = S3AioThreadPool(1)
        pool.submit(release.wait, EVENT_TIMEOUT)
        pending = pool.submit(events.append, "pending finished")
        pool.shutdown(wait=False)
        with pytest.raises(RuntimeError, match="after shutdown"):
            pool.submit(sum, [1, 2])
        release.set()
        pending.result(timeout=EVENT_TIMEOUT)

        assert events == ["pending finished"]

    def test_fork(self):
        # A forked child, whose copy of the pool has no threads, creates new
        # ones, also if a thread of the parent held the lock of the pool.
        pool = S3AioThreadPool(1)
        parent = pool._get_executor()
        pool._lock.acquire()
        # As os.register_at_fork() runs it in a forked child.
        S3AioThreadPool._reset_after_fork()
        # The copied executor is kept, so that its collection does not touch
        # the locks that the parent's threads may have held.
        assert parent in S3AioThreadPool._forked_executors
        child = pool._get_executor()
        assert pool.submit(sum, [1, 2]).result(timeout=EVENT_TIMEOUT) == 3
        pool.shutdown()
        assert child is not parent
        S3AioThreadPool._forked_executors.remove(parent)
        parent.shutdown()


class TestS3AioExecutor:
    def test_init(self):
        # max_workers is optional, as before it was added.
        S3AioExecutor(loop=None)
        with pytest.raises(ValueError, match="max_workers must be greater than 0"):
            S3AioExecutor(loop=None, max_workers=0)

    def test_submit(self):
        async def main():
            executor = S3AioExecutor(loop=asyncio.get_running_loop())
            succeeded = executor.submit(sum, [1, 2])
            failed = executor.submit(int, "x")
            await asyncio.to_thread(wait, [succeeded, failed])
            return succeeded, failed

        succeeded, failed = asyncio.run(main())

        assert succeeded.result() == 3
        with pytest.raises(ValueError, match="invalid literal"):
            failed.result()

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

    def test_loop_shutdown(self):
        # A function that has not started when the event loop shuts down is
        # never run, and its future is cancelled and settled, so that wait()
        # returns, instead of left pending.
        events = []
        started = threading.Event()
        settled = threading.Event()

        def work():
            started.set()
            # Running until the pending future is settled, so that the
            # pending function cannot start before the shutdown.
            settled.wait(5)
            events.append("finished")

        async def main():
            # The pending function waits for the only thread of the executor.
            executor = S3AioExecutor(loop=asyncio.get_running_loop(), max_workers=1)
            running = executor.submit(work)
            pending = executor.submit(events.append, "pending finished")
            pending.add_done_callback(lambda _: settled.set())
            while not started.is_set():
                await asyncio.sleep(0.01)
            return running, pending

        running, pending = asyncio.run(main())

        _, not_done = wait([running, pending], timeout=5)
        assert not not_done
        assert events == ["finished"]
        assert not running.cancelled()
        assert pending.cancelled()

    def test_executor_shut_down(self):
        # An error raised before the function starts is set on the future.
        async def main():
            executor = S3AioExecutor(loop=asyncio.get_running_loop())
            # Fails the dispatch of the function to the pool.
            executor._pool.shutdown()
            future = executor.submit(sum, [1, 2])
            with pytest.raises(RuntimeError, match="after shutdown"):
                await asyncio.wrap_future(future)
            return future

        future = asyncio.run(main())

        assert not future.cancelled()


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
        # cancels the future of the caller, so that wait() returns.
        events = []
        started = threading.Event()
        settled = threading.Event()

        def work():
            started.set()
            settled.wait(5)
            events.append("finished")

        pending: Future[None] = Future()
        pending.add_done_callback(lambda _: settled.set())

        async def main():
            executor = S3AioExecutor(loop=asyncio.get_running_loop(), max_workers=1)
            running = executor.submit(work)
            executor.submit_to(pending, events.append, "pending finished")
            while not started.is_set():
                await asyncio.sleep(0.01)
            return running

        running = asyncio.run(main())

        _, not_done = wait([running, pending], timeout=5)
        assert not not_done
        assert events == ["finished"]
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
                await asyncio.wrap_future(future)

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
                return await loop.run_in_executor(waiter, lambda: wait([future], timeout=5))

        _, not_done = asyncio.run(main())

        assert not not_done
        assert future.cancelled()
