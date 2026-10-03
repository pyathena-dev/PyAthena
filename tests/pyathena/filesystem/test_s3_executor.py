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

from pyathena.filesystem.s3_executor import S3AioExecutor


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
            loop = asyncio.get_running_loop()
            loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
            executor = S3AioExecutor(loop=loop)
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

