# Copyright 2025 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Asyncio helpers for retrying AWS API calls and closing resources in a thread."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import logging
import operator
import threading
from collections.abc import Callable
from concurrent.futures import Future
from typing import Any, Protocol, TypeVar, cast, overload

from pyathena.util import RetryConfig, retry_api_call, stop_retries_on, unstoppable_retries

_logger = logging.getLogger(__name__)

T = TypeVar("T")


class _Closable(Protocol):
    def close(self) -> Any: ...


_ClosableT = TypeVar("_ClosableT", bound=_Closable)


class _Executor(Protocol):
    def submit(self, fn: Callable[..., T], /, *args: Any) -> Future[T]: ...


# The default release of create_in_thread(): the blocking close() of the object.
_CLOSE = operator.methodcaller("close")


async def run_in_thread(func: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    """Run a blocking function in the default executor, stopping its retries on cancellation.

    Like ``asyncio.to_thread()``, the function runs in the event loop's default
    executor with a copy of the caller's context, and a cancellation of the
    caller skips a function that has not started. A cancellation also stops
    the retries of the AWS API calls that the running function makes (see
    :func:`~pyathena.util.stop_retries_on`). A request that has been sent
    still runs to its end, and the function returns or raises in its
    thread, where nobody awaits it.

    Args:
        func: The blocking function.
        *args: Positional arguments for ``func``.
        **kwargs: Keyword arguments for ``func``.

    Returns:
        The return value of ``func``.
    """
    stop = threading.Event()

    def call() -> T:
        """Run the function in a scope that the cancellation stops."""
        with stop_retries_on(stop):
            return func(*args, **kwargs)

    future = asyncio.get_running_loop().run_in_executor(None, contextvars.copy_context().run, call)
    try:
        return await future
    except asyncio.CancelledError:
        stop.set()
        raise


async def async_retry_api_call(
    func: Callable[..., Any],
    config: RetryConfig,
    logger: logging.Logger | None = None,
    *args: Any,
    stop_on: Callable[[BaseException], bool] | None = None,
    stoppable: bool = True,
    **kwargs: Any,
) -> Any:
    """Execute a function with retry logic in a thread to avoid blocking the event loop.

    Runs ``retry_api_call`` with :func:`run_in_thread` so that blocking boto3
    calls do not block the asyncio event loop. Cancelling the caller makes no
    further attempt and ends the backoff wait, unless ``stoppable`` is false.

    Args:
        func: The AWS API function to call.
        config: RetryConfig instance specifying retry behavior.
        logger: Optional logger for retry attempt logging.
        *args: Positional arguments to pass to ``retry_api_call``.
        stop_on: Passed to ``retry_api_call``.
        stoppable: Whether a cancellation of the caller stops the retries. A
            request that cleans up, such as one that stops a query or
            terminates a session, passes False: its retries then run to their
            end in the thread (see :func:`~pyathena.util.unstoppable_retries`),
            as with ``asyncio.to_thread()``.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The result of the successful function call.
    """

    def call() -> Any:
        """Make the call, ignoring the stop scopes unless it is stoppable."""
        with contextlib.nullcontext() if stoppable else unstoppable_retries():
            return retry_api_call(func, config, logger, *args, stop_on=stop_on, **kwargs)

    return await (run_in_thread(call) if stoppable else asyncio.to_thread(call))


async def close_in_thread(close: Callable[[], Any]) -> None:
    """Run a blocking close in the event loop's default executor.

    The close is submitted at once and awaited through ``asyncio.shield()``, so
    it runs to completion even if the caller is cancelled before or while it
    runs. ``asyncio.to_thread()`` would drop a close that is still queued when
    the caller is cancelled.

    Args:
        close: The blocking function that closes the resource.
    """
    await asyncio.shield(asyncio.get_running_loop().run_in_executor(None, close))


@overload
async def create_in_thread(
    factory: Callable[[], _ClosableT], /, *, executor: _Executor | None = ...
) -> _ClosableT: ...


@overload
async def create_in_thread(
    factory: Callable[[], T],
    /,
    *,
    release: Callable[[T], Any],
    executor: _Executor | None = ...,
) -> T: ...


async def create_in_thread(
    factory: Callable[[], Any],
    /,
    *,
    release: Callable[[Any], Any] = _CLOSE,
    executor: _Executor | None = None,
) -> Any:
    """Run a blocking constructor of a closable object in the default executor.

    Like ``asyncio.to_thread()``, the constructor runs in the event loop's
    default executor, or in ``executor``, with the caller's context. If the
    caller is cancelled, a construction that has not started is skipped, the
    retries of a running one are stopped as by :func:`run_in_thread`, an
    object constructed after the cancellation is released by the constructing
    thread, and an object constructed just before it is released in a new
    thread: nothing else can release it, and a close by the garbage collector
    could block the event loop's thread. The release needs neither the event
    loop nor its executor, which may be shutting down.

    Args:
        factory: The constructor, which takes no arguments; bind them with
            ``functools.partial()``.
        release: The blocking function that releases an object constructed for
            a cancelled caller, for an object whose ``close()`` is a coroutine.
            Defaults to calling the object's ``close()``.
        executor: The executor that runs the constructor instead of the
            default executor, such as the thread pool of a filesystem.

    Returns:
        The constructed object.
    """
    lock = threading.Lock()
    cancelled = threading.Event()
    constructed: list[Any] = []

    def construct() -> Any:
        """Construct the object in a scope that the cancellation stops.

        Returns:
            The object, or None if the caller was cancelled, after which an
            object constructed here is released here.
        """
        if cancelled.is_set():
            return None
        with stop_retries_on(cancelled):
            obj = factory()
        with lock:
            if not cancelled.is_set():
                constructed.append(obj)
                return obj
        _release_logging_errors(obj, release)
        return None

    context = contextvars.copy_context()
    future = (
        asyncio.get_running_loop().run_in_executor(None, context.run, construct)
        if executor is None
        else asyncio.wrap_future(executor.submit(context.run, construct))
    )
    try:
        # Unlike asyncio.shield(), asyncio.wait() reports nothing when the
        # construction fails after the caller is cancelled.
        await asyncio.wait((future,))
    except asyncio.CancelledError:
        with lock:
            cancelled.set()
        # Retrieve a later construction error, which nobody else awaits.
        future.add_done_callback(lambda f: f.cancelled() or f.exception())
        if constructed:
            threading.Thread(
                target=_release_logging_errors,
                args=(constructed[0], release),
                name="create_in_thread-close",
            ).start()
        raise
    return future.result()


async def run_in_thread_or_close(
    closable: _Closable, func: Callable[..., T], /, *args: Any, **kwargs: Any
) -> T:
    """Run a blocking function that uses a closable object in the default executor.

    Like :func:`run_in_thread`, the function runs in the event loop's default
    executor with a copy of the caller's context, a cancellation of the caller
    skips a function that has not started, and it stops the retries of the AWS
    API calls that a running function makes. A cancellation also closes the
    object once the function is no longer running: a running function closes it
    in its thread when it ends, and otherwise a new thread closes it. As with
    :func:`create_in_thread`, the close needs neither the event loop nor its
    executor.

    Args:
        closable: The object that the function uses.
        func: The blocking function.
        *args: Positional arguments for ``func``.
        **kwargs: Keyword arguments for ``func``.

    Returns:
        The return value of ``func``.
    """
    lock = threading.Lock()
    cancelled = threading.Event()
    running = False

    def call() -> T | None:
        """Run the function unless the caller was cancelled, closing the object if it was then.

        Returns:
            The return value of the function, or None if the caller was cancelled
            before it started.
        """
        nonlocal running
        with lock:
            if cancelled.is_set():
                return None
            running = True
        try:
            with stop_retries_on(cancelled):
                return func(*args, **kwargs)
        finally:
            with lock:
                running = False
                close = cancelled.is_set()
            if close:
                _release_logging_errors(closable)

    future = asyncio.get_running_loop().run_in_executor(None, contextvars.copy_context().run, call)
    try:
        return cast(T, await future)
    except asyncio.CancelledError:
        with lock:
            cancelled.set()
            close = not running
        if close:
            threading.Thread(
                target=_release_logging_errors,
                args=(closable,),
                name="run_in_thread_or_close-close",
            ).start()
        raise


def _release_logging_errors(obj: Any, release: Callable[[Any], Any] = _CLOSE) -> None:
    """Release an object for a cancelled caller, logging an error that nobody else sees.

    Args:
        obj: The object to release.
        release: The blocking function that releases it. Defaults to its
            ``close()``.
    """
    try:
        with unstoppable_retries():
            release(obj)
    except Exception:
        _logger.exception(f"Failed to close {obj!r} after a cancellation.")
