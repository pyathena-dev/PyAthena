# Copyright 2017 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Executors that run S3 filesystem operations in parallel."""

from __future__ import annotations

import asyncio
import threading
from abc import ABCMeta, abstractmethod
from collections.abc import Callable
from concurrent.futures import Future
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Any, TypeVar

from pyathena.util import override

T = TypeVar("T")


class S3Executor(metaclass=ABCMeta):
    """Abstract executor for parallel S3 operations.

    Defines the interface used by ``S3File`` and ``S3FileSystem`` for submitting
    work to run in parallel and for shutting down the executor when done.
    Both ``submit`` and ``shutdown`` mirror the ``concurrent.futures.Executor``
    interface so that ``as_completed()`` and ``Future.cancel()`` work unchanged.
    """

    @abstractmethod
    def submit(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> Future[T]:
        """Submit a callable for execution and return a Future."""
        ...

    @abstractmethod
    def shutdown(self, wait: bool = True) -> None:
        """Shut down the executor, freeing any resources."""
        ...

    def __enter__(self) -> S3Executor:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.shutdown(wait=True)


class S3ThreadPoolExecutor(S3Executor):
    """Executor that delegates to a ``ThreadPoolExecutor``.

    This is the default executor used by ``S3File`` and ``S3FileSystem``
    for synchronous parallel operations.
    """

    def __init__(self, max_workers: int) -> None:
        """Initialize the executor with a new ``ThreadPoolExecutor``.

        Args:
            max_workers: The maximum number of threads of the thread pool.
        """
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    @override
    def submit(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> Future[T]:
        return self._executor.submit(fn, *args, **kwargs)

    @override
    def shutdown(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)


class S3AioExecutor(S3Executor):
    """Executor that schedules work on an asyncio event loop.

    Uses ``asyncio.run_coroutine_threadsafe(asyncio.to_thread(fn), loop)`` to
    dispatch blocking functions onto the event loop's thread pool, returning
    ``concurrent.futures.Future`` objects that are compatible with
    ``as_completed()``, ``wait()`` and ``Future.cancel()``. As with
    ``ThreadPoolExecutor``, a future cannot be cancelled once its function has
    started.

    This avoids thread-in-thread nesting when ``S3File`` is used from within
    ``asyncio.to_thread()`` calls (the pattern used by ``AioS3FileSystem``).

    Args:
        loop: A running asyncio event loop.

    Raises:
        RuntimeError: If the event loop is not running when ``submit`` is called.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Initialize the executor with the event loop to schedule work on.

        Args:
            loop: The asyncio event loop. ``submit`` raises ``RuntimeError``
                if it is None or not running.
        """
        self._loop = loop

    @override
    def submit(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> Future[T]:
        if self._loop is not None and self._loop.is_running():
            # The future of run_coroutine_threadsafe can be cancelled while
            # the function keeps running in its thread, so the returned future
            # is started and resolved by the function's thread instead.
            future: Future[T] = Future()
            # Acquired once, by run() or by settle(), whichever comes first,
            # so that the future is started or settled exactly once.
            claim = threading.Lock()

            def run() -> None:
                """Run the function and resolve the future unless it was cancelled."""
                if not claim.acquire(blocking=False) or not future.set_running_or_notify_cancel():
                    return
                try:
                    result = fn(*args, **kwargs)
                except BaseException as e:
                    future.set_exception(e)
                else:
                    future.set_result(result)

            def settle(task: Future[None]) -> None:
                """Resolve the future if the task ended before the function started.

                This happens, for example, when the event loop shuts down.

                Args:
                    task: The finished future of the task that runs the function.
                """
                if not claim.acquire(blocking=False):
                    # run() has claimed the future and resolves it.
                    return
                if task.cancelled():
                    future.cancel()
                    # Notify the waiters of the cancellation, as an executor
                    # does when it drops a cancelled function.
                    future.set_running_or_notify_cancel()
                elif future.set_running_or_notify_cancel():
                    future.set_exception(task.exception())

            task = asyncio.run_coroutine_threadsafe(asyncio.to_thread(run), self._loop)
            task.add_done_callback(settle)
            return future
        raise RuntimeError(
            "S3AioExecutor requires a running event loop. "
            "Use S3ThreadPoolExecutor for synchronous usage."
        )

    @override
    def shutdown(self, wait: bool = True) -> None:
        # No resources to release — work is dispatched to the event loop.
        pass
