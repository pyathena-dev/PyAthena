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
from concurrent.futures import ThreadPoolExecutor

import pytest

from pyathena.aio.util import async_retry_api_call, create_in_thread, run_in_thread
from pyathena.util import RetryConfig, retry_api_call, stop_retries_on
from tests.pyathena.util import EVENT_TIMEOUT, ThrottledOperation, wait_until

_VALUE: contextvars.ContextVar[str] = contextvars.ContextVar("_VALUE", default="unset")


async def _cancel_once_started(task, operation, stop_in):
    """Cancel a task once its operation has made its first attempt.

    Args:
        task: The task that runs the operation in a thread.
        operation: The blocking ``ThrottledOperation`` of the task.
        stop_in: "request" to cancel during the first attempt, "backoff" to
            cancel during the backoff wait after it.
    """
    # Polled, since the default executor may have no free thread.
    await wait_until(operation.started.is_set)
    if stop_in == "backoff":
        # Lets the first attempt fail and the backoff wait begin.
        await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    operation.release.set()


class _Resource:
    """A closable object that records its constructor and close threads."""

    def __init__(self):
        self.created_in = threading.get_ident()
        self.value = _VALUE.get()
        self.closed = threading.Event()
        self.closed_in = None

    def close(self):
        self.closed_in = threading.get_ident()
        self.closed.set()


async def test_create_in_thread():
    """The constructor runs in another thread with the caller's context."""
    _VALUE.set("caller")
    resource = await create_in_thread(_Resource)
    assert resource.created_in != threading.get_ident()
    assert resource.value == "caller"
    assert not resource.closed.is_set()


async def test_create_in_thread_cancelled_during_construction():
    """An object constructed after the caller was cancelled is closed off the loop."""
    started = threading.Event()
    release = threading.Event()
    created = []

    def factory():
        resource = _Resource()
        created.append(resource)
        started.set()
        assert release.wait(EVENT_TIMEOUT)
        return resource

    task = asyncio.create_task(create_in_thread(factory))
    assert await asyncio.to_thread(started.wait, EVENT_TIMEOUT)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release.set()

    assert await asyncio.to_thread(created[0].closed.wait, EVENT_TIMEOUT)
    assert created[0].closed_in is not None
    assert created[0].closed_in != threading.get_ident()


async def test_create_in_thread_cancelled_construction_error():
    """A construction that fails after the caller was cancelled reports nothing."""
    loop = asyncio.get_running_loop()
    errors = []
    loop.set_exception_handler(lambda _, context: errors.append(context))
    # One worker runs the construction and then, in order, the no-op below.
    executor = ThreadPoolExecutor(max_workers=1)
    loop.set_default_executor(executor)
    started = threading.Event()
    release = threading.Event()

    def factory():
        started.set()
        assert release.wait(EVENT_TIMEOUT)
        raise OSError("read failed")

    with ThreadPoolExecutor(max_workers=1) as waiter:
        try:
            task = asyncio.create_task(create_in_thread(factory))
            assert await loop.run_in_executor(waiter, started.wait, EVENT_TIMEOUT)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            del task
            release.set()
            # The construction has completed once the no-op behind it has run.
            await loop.run_in_executor(None, lambda: None)
            await asyncio.sleep(0)
            gc.collect()
            await asyncio.sleep(0)
            assert errors == []
        finally:
            release.set()
            executor.shutdown(wait=True)


async def test_create_in_thread_cancelled_after_construction(monkeypatch):
    """An object constructed before the cancellation reaches the caller is closed in a thread."""
    created = []

    def factory():
        resource = _Resource()
        created.append(resource)
        return resource

    async def cancelled_after(futures):
        # The construction completes, then the caller is cancelled before it resumes.
        await next(iter(futures))
        raise asyncio.CancelledError

    monkeypatch.setattr("pyathena.aio.util.asyncio.wait", cancelled_after)
    with pytest.raises(asyncio.CancelledError):
        await create_in_thread(factory)

    assert await asyncio.to_thread(created[0].closed.wait, EVENT_TIMEOUT)
    assert created[0].closed_in is not None
    assert created[0].closed_in != threading.get_ident()


async def test_create_in_thread_cancelled_before_construction():
    """A construction that has not started when the caller is cancelled is skipped."""
    loop = asyncio.get_running_loop()
    # One busy worker keeps the construction queued until the caller is cancelled.
    executor = ThreadPoolExecutor(max_workers=1)
    loop.set_default_executor(executor)
    busy = threading.Event()
    calls = []
    try:
        blocker = loop.run_in_executor(None, busy.wait, EVENT_TIMEOUT)
        task = asyncio.create_task(create_in_thread(lambda: calls.append(1) or _Resource()))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        busy.set()
        await blocker
        # The queued construction runs after the blocker and returns without calling factory.
        await loop.run_in_executor(None, lambda: None)
        assert calls == []
    finally:
        busy.set()
        executor.shutdown(wait=True)


async def test_create_in_thread_cleanup_after_executor_shutdown():
    """An abandoned object is closed even if the default executor shuts down meanwhile."""
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=1)
    loop.set_default_executor(executor)
    started = threading.Event()
    release = threading.Event()
    created = []

    def factory():
        resource = _Resource()
        created.append(resource)
        started.set()
        assert release.wait(EVENT_TIMEOUT)
        return resource

    with ThreadPoolExecutor(max_workers=1) as waiter:
        try:
            task = asyncio.create_task(create_in_thread(factory))
            assert await loop.run_in_executor(waiter, started.wait, EVENT_TIMEOUT)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            # As at the end of asyncio.run(), while the construction still runs.
            executor.shutdown(wait=False)
            release.set()
            assert await loop.run_in_executor(waiter, created[0].closed.wait, EVENT_TIMEOUT)
        finally:
            release.set()
            executor.shutdown(wait=True)


async def test_run_in_thread():
    """The function runs in another thread with the caller's context."""
    _VALUE.set("caller")
    thread, value = await run_in_thread(lambda: (threading.get_ident(), _VALUE.get()))
    assert thread != threading.get_ident()
    assert value == "caller"


@pytest.mark.parametrize("stop_in", ["request", "backoff"])
async def test_async_retry_api_call_cancelled(stop_in):
    """GH-1202: cancelling the caller makes no further attempt and ends the backoff wait.

    Without the stop, the thread would wait at least EVENT_TIMEOUT seconds before
    the next attempt, holding the only thread of the default executor.
    """
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=1)
    loop.set_default_executor(executor)
    operation = ThrottledOperation(block=stop_in == "request")
    try:
        task = asyncio.create_task(
            async_retry_api_call(operation, RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT))
        )
        await _cancel_once_started(task, operation, stop_in)
        started = time.monotonic()
        # Runs once the thread of the cancelled call is free.
        await loop.run_in_executor(None, lambda: None)
        assert time.monotonic() - started < EVENT_TIMEOUT
        assert operation.calls == 1
    finally:
        operation.release.set()
        executor.shutdown(wait=True)


async def test_async_retry_api_call_not_stoppable():
    """GH-1202: a cleanup request that is not stoppable keeps its retries after a cancellation."""
    operation = ThrottledOperation(block=True, failures=2)
    task = asyncio.create_task(
        async_retry_api_call(operation, RetryConfig(attempt=3, multiplier=0), stoppable=False)
    )
    try:
        await _cancel_once_started(task, operation, "request")
        await wait_until(lambda: operation.calls == 3)
    finally:
        operation.release.set()


async def test_async_retry_api_call_not_stoppable_in_stopped_scope():
    """A request that is not stoppable ignores the stop scopes of its caller."""
    stop = threading.Event()
    stop.set()
    operation = ThrottledOperation(failures=2)
    with stop_retries_on(stop):
        result = await async_retry_api_call(
            operation, RetryConfig(attempt=3, multiplier=0), stoppable=False
        )
    assert result == "success"
    assert operation.calls == 3


async def test_async_retry_api_call_retries():
    """Without a cancellation, the retries continue."""
    operation = ThrottledOperation(failures=2)
    assert await async_retry_api_call(operation, RetryConfig(attempt=3, multiplier=0)) == "success"
    assert operation.calls == 3


@pytest.mark.parametrize("stop_in", ["request", "backoff"])
async def test_create_in_thread_cancelled_stops_retries(stop_in):
    """GH-1202: cancelling the caller stops the retries of a running construction."""
    operation = ThrottledOperation(block=stop_in == "request")
    finished = threading.Event()

    def factory():
        try:
            retry_api_call(operation, RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT))
        finally:
            finished.set()

    task = asyncio.create_task(create_in_thread(factory))
    try:
        await _cancel_once_started(task, operation, stop_in)
        started = time.monotonic()
        assert await asyncio.to_thread(finished.wait, EVENT_TIMEOUT)
        assert time.monotonic() - started < EVENT_TIMEOUT
        assert operation.calls == 1
    finally:
        operation.release.set()


async def test_create_in_thread_cleanup_not_stopped():
    """The close of an object constructed after a cancellation retries as usual."""
    started = threading.Event()
    release = threading.Event()
    close_operation = ThrottledOperation(failures=2)
    closed = threading.Event()

    class _ClosedWithRequest(_Resource):
        def close(self):
            retry_api_call(close_operation, RetryConfig(attempt=3, multiplier=0))
            closed.set()

    def factory():
        started.set()
        assert release.wait(EVENT_TIMEOUT)
        return _ClosedWithRequest()

    task = asyncio.create_task(create_in_thread(factory))
    try:
        assert await asyncio.to_thread(started.wait, EVENT_TIMEOUT)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()
        assert await asyncio.to_thread(closed.wait, EVENT_TIMEOUT)
        assert close_operation.calls == 3
    finally:
        release.set()
