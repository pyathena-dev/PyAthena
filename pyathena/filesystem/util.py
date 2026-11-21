# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Helpers of the filesystem: the user agent, ``override``, and constructing in a thread."""

from __future__ import annotations

import asyncio
import contextvars
import logging
import operator
import threading
from collections.abc import Callable
from concurrent.futures import Future
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, overload

from pyathena.filesystem.s3_retry import stop_retries_on, unstoppable_retries

_logger = logging.getLogger(__name__)

_T = TypeVar("_T")

# Replace with ``from typing import override`` when Python 3.11 support ends.
if TYPE_CHECKING:
    from typing_extensions import override
else:

    def override(func):
        """Mark a method as overriding a base class method (PEP 698).

        Type checkers use ``typing_extensions.override`` and report a marked
        method whose base class method does not exist. At runtime this
        decorator does nothing.

        Args:
            func: The overriding method.

        Returns:
            The method unchanged.
        """
        return func


try:
    _version = version("PyAthena")
except PackageNotFoundError:
    _version = "unknown"
#: The ``user_agent_extra`` of the botocore configuration of the clients that
#: the filesystem builds.
user_agent_extra: str = f"PyAthena/{_version}"


class _Closable(Protocol):
    def close(self) -> Any: ...


_ClosableT = TypeVar("_ClosableT", bound=_Closable)


class _Executor(Protocol):
    def submit(self, fn: Callable[..., _T], /, *args: Any) -> Future[_T]: ...


# The default release of create_in_thread(): the blocking close() of the object.
_CLOSE = operator.methodcaller("close")


@overload
async def create_in_thread(
    factory: Callable[[], _ClosableT], /, *, executor: _Executor | None = ...
) -> _ClosableT: ...


@overload
async def create_in_thread(
    factory: Callable[[], _T],
    /,
    *,
    release: Callable[[_T], Any],
    executor: _Executor | None = ...,
) -> _T: ...


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
    retries of a running one are stopped (see
    :func:`~pyathena.filesystem.s3_retry.stop_retries_on`), an object
    constructed after the cancellation is released by the constructing
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
