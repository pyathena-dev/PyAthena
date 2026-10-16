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
import threading
from collections.abc import Callable
from typing import Any, Protocol, TypeVar, cast

from pyathena.util import RetryConfig, retry_api_call, shield_retries, stop_retries_on

T = TypeVar("T")


class _Closable(Protocol):
    def close(self) -> Any: ...


_ClosableT = TypeVar("_ClosableT", bound=_Closable)


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
    stop_when: Callable[[], bool] | None = None,
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
        stop_when: Passed to ``retry_api_call``.
        stoppable: Whether a cancellation of the caller stops the retries. A
            request that cleans up, such as one that stops a query or
            terminates a session, passes False: its retries then ignore the
            stop scopes (see :func:`~pyathena.util.shield_retries`) and run to
            their end in the thread, as with ``asyncio.to_thread()``.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The result of the successful function call.
    """

    def call() -> Any:
        """Make the call, ignoring the stop scopes unless it is stoppable."""
        with contextlib.nullcontext() if stoppable else shield_retries():
            return retry_api_call(
                func, config, logger, *args, stop_on=stop_on, stop_when=stop_when, **kwargs
            )

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


async def create_in_thread(
    factory: Callable[..., _ClosableT], *args: Any, **kwargs: Any
) -> _ClosableT:
    """Run a blocking constructor of a closable object in the default executor.

    Like ``asyncio.to_thread()``, the constructor runs in the event loop's
    default executor with the caller's context. If the caller is cancelled, a
    construction that has not started is skipped, the retries of a running
    one are stopped as by :func:`run_in_thread`, an object constructed after
    the cancellation is closed by the constructing thread, and an object
    constructed just before it is closed in a new thread: nothing else can
    close it, and a close by the garbage collector could block the event
    loop's thread. The cleanup needs neither the event loop nor its executor,
    which may be shutting down.

    Args:
        factory: The constructor.
        *args: Positional arguments for ``factory``.
        **kwargs: Keyword arguments for ``factory``.

    Returns:
        The constructed object.
    """
    lock = threading.Lock()
    cancelled = threading.Event()
    constructed: list[_ClosableT] = []

    def construct() -> _ClosableT | None:
        """Construct the object in a scope that the cancellation stops.

        Returns:
            The object, or None if the caller was cancelled, after which an
            object constructed here is closed here.
        """
        if cancelled.is_set():
            return None
        with stop_retries_on(cancelled):
            obj = factory(*args, **kwargs)
        with lock:
            if not cancelled.is_set():
                constructed.append(obj)
                return obj
        with shield_retries():
            obj.close()
        return None

    future = asyncio.get_running_loop().run_in_executor(
        None, contextvars.copy_context().run, construct
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
            threading.Thread(target=constructed[0].close, name="create_in_thread-close").start()
        raise
    return cast(_ClosableT, future.result())
