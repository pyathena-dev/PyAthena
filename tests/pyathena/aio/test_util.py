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

from pyathena.aio.util import (
    async_retry_api_call,
    create_in_thread,
    run_in_thread,
    run_in_thread_or_close,
)
from pyathena.util import RetryConfig, retry_api_call, stop_retries_on
from tests.pyathena.util import (
    EVENT_TIMEOUT,
    ThrottledOperation,
    cancel_when_started,
)

_VALUE: contextvars.ContextVar[str] = contextvars.ContextVar("_VALUE", default="unset")


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


async def test_create_in_thread_cancelled_during_construction_release():
    """An object constructed after the cancellation is released with ``release``, not closed."""
    started = threading.Event()
    release = threading.Event()
    released = threading.Event()
    created = []

    def factory():
        resource = _Resource()
        created.append(resource)
        started.set()
        assert release.wait(EVENT_TIMEOUT)
        return resource

    task = asyncio.create_task(create_in_thread(factory, release=lambda resource: released.set()))
    await cancel_when_started(task, started, release)

    assert await asyncio.to_thread(released.wait, EVENT_TIMEOUT)
    assert not created[0].closed.is_set()


async def test_create_in_thread_executor():
    """The constructor runs in the given executor with the caller's context."""
    _VALUE.set("caller")
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="given") as executor:
        resource = await create_in_thread(
            lambda: (_Resource(), threading.current_thread().name), executor=executor
        )
    assert resource[0].value == "caller"
    assert resource[1].startswith("given")


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


async def test_run_in_thread_or_close():
    """The function runs in another thread with the caller's context, and nothing is closed."""
    _VALUE.set("caller")
    resource = _Resource()
    thread, value = await run_in_thread_or_close(
        resource, lambda: (threading.get_ident(), _VALUE.get())
    )
    assert thread != threading.get_ident()
    assert value == "caller"
    assert not resource.closed.is_set()


async def test_run_in_thread_or_close_cancelled_during_function():
    """GH-1206: the object is closed by the function's thread once the function ends."""
    started, release = threading.Event(), threading.Event()
    resource = _Resource()
    threads = []

    def func():
        threads.append(threading.get_ident())
        started.set()
        assert release.wait(EVENT_TIMEOUT)

    task = asyncio.create_task(run_in_thread_or_close(resource, func))
    try:
        assert await asyncio.to_thread(started.wait, EVENT_TIMEOUT)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not resource.closed.is_set()
    finally:
        release.set()
    assert await asyncio.to_thread(resource.closed.wait, EVENT_TIMEOUT)
    assert resource.closed_in == threads[0]


async def test_run_in_thread_or_close_cancelled_after_function():
    """GH-1206: an object whose function ended before the cancellation is closed in a new thread."""
    loop = asyncio.get_running_loop()
    # With one worker, a later job starts only after the function's call has returned.
    executor = ThreadPoolExecutor(max_workers=1)
    loop.set_default_executor(executor)
    resource = _Resource()
    threads = []
    started, release = threading.Event(), threading.Event()

    def func():
        threads.append(threading.get_ident())
        started.set()
        assert release.wait(EVENT_TIMEOUT)

    try:
        task = asyncio.create_task(run_in_thread_or_close(resource, func))
        await asyncio.sleep(0)
        # From here, blocking the event loop keeps the task from resuming after the
        # call returns, which the next job of the single worker waits for.
        assert started.wait(EVENT_TIMEOUT)
        release.set()
        assert executor.submit(lambda: None).result(EVENT_TIMEOUT) is None
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert resource.closed.wait(EVENT_TIMEOUT)
        assert resource.closed_in not in (None, threads[0], threading.get_ident())
    finally:
        release.set()
        executor.shutdown(wait=True)


async def test_run_in_thread_or_close_cancelled_before_function():
    """GH-1206: a function that has not started is skipped, and a new thread closes the object."""
    loop = asyncio.get_running_loop()
    # One busy worker keeps the function queued until the caller is cancelled.
    executor = ThreadPoolExecutor(max_workers=1)
    loop.set_default_executor(executor)
    busy = threading.Event()
    calls = []
    resource = _Resource()
    try:
        blocker = loop.run_in_executor(None, busy.wait, EVENT_TIMEOUT)
        task = asyncio.create_task(run_in_thread_or_close(resource, lambda: calls.append(1)))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # The only worker of the default executor is busy, so this waits on the loop.
        assert resource.closed.wait(EVENT_TIMEOUT)
        assert resource.closed_in != threading.get_ident()
        busy.set()
        await blocker
        # A function that the executor did not drop returns without running.
        await loop.run_in_executor(None, lambda: None)
        assert calls == []
    finally:
        busy.set()
        executor.shutdown(wait=True)


async def test_run_in_thread():
    """The function runs in another thread with the caller's context."""
    _VALUE.set("caller")
    thread, value = await run_in_thread(lambda: (threading.get_ident(), _VALUE.get()))
    assert thread != threading.get_ident()
    assert value == "caller"


# The synchronous test_retry_api_call_stopped_by_event checks the stop during the backoff wait.
async def test_async_retry_api_call_cancelled():
    """GH-1202: cancelling the caller makes no further attempt and ends the backoff wait.

    Without the stop, the thread would wait at least EVENT_TIMEOUT seconds before
    the next attempt, holding the only thread of the default executor.
    """
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=1)
    loop.set_default_executor(executor)
    operation = ThrottledOperation(block=True)
    try:
        task = asyncio.create_task(
            async_retry_api_call(operation, RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT))
        )
        await cancel_when_started(task, operation.started, operation.release)
        started = time.monotonic()
        # Runs once the thread of the cancelled call is free.
        await loop.run_in_executor(None, lambda: None)
        assert time.monotonic() - started < EVENT_TIMEOUT
        assert operation.calls == 1
    finally:
        operation.release.set()
        executor.shutdown(wait=True)


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


async def test_create_in_thread_cancelled_stops_retries():
    """GH-1202: cancelling the caller stops the retries of a running construction."""
    operation = ThrottledOperation(block=True)
    finished = threading.Event()

    def factory():
        try:
            retry_api_call(operation, RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT))
        finally:
            finished.set()

    task = asyncio.create_task(create_in_thread(factory))
    try:
        await cancel_when_started(task, operation.started, operation.release)
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
