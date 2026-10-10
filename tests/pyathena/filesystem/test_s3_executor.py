# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import asyncio
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait

import pytest

from pyathena.filesystem.s3_executor import S3AioExecutor, S3ThreadPoolExecutor


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
            loop = asyncio.get_running_loop()
            loop.set_default_executor(ThreadPoolExecutor(max_workers=2))
            executor = S3AioExecutor(loop=loop)
            # The function runs in one of the two threads, and cancel() in the other.
            return await asyncio.to_thread(cancel, executor)

        running, pending = asyncio.run(main())

        assert events == ["finished", "waited"]
        assert running.done()
        assert not running.cancelled()
        assert pending.cancelled()

    @pytest.mark.parametrize(
        ("threads", "max_workers"),
        [
            (1, 5),  # The pending function waits for a thread.
            (2, 1),  # The pending function waits for a permit.
        ],
    )
    def test_loop_shutdown(self, threads, max_workers):
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
            loop = asyncio.get_running_loop()
            loop.set_default_executor(ThreadPoolExecutor(max_workers=threads))
            executor = S3AioExecutor(loop=loop, max_workers=max_workers)
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
            loop = asyncio.get_running_loop()
            default_executor = ThreadPoolExecutor(max_workers=1)
            loop.set_default_executor(default_executor)
            default_executor.shutdown()
            future = S3AioExecutor(loop=loop).submit(sum, [1, 2])
            with pytest.raises(RuntimeError, match="after shutdown"):
                await asyncio.wrap_future(future)
            return future

        future = asyncio.run(main())

        assert not future.cancelled()


class TestS3Executor:
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
            loop = asyncio.get_running_loop()
            loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
            executor = S3AioExecutor(loop=loop)
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
            loop = asyncio.get_running_loop()
            default_executor = ThreadPoolExecutor(max_workers=1)
            loop.set_default_executor(default_executor)
            default_executor.shutdown()
            S3AioExecutor(loop=loop).submit_to(future, sum, [1, 2])
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
            default_executor = ThreadPoolExecutor(max_workers=1)
            loop.set_default_executor(default_executor)
            default_executor.shutdown()
            S3AioExecutor(loop=loop).submit_to(future, sum, [1, 2])
            assert future.cancel()
            with ThreadPoolExecutor(max_workers=1) as waiter:
                return await loop.run_in_executor(waiter, lambda: wait([future], timeout=5))

        _, not_done = asyncio.run(main())

        assert not not_done
        assert future.cancelled()
