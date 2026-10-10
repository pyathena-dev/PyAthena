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
from multiprocessing import cpu_count
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

    def submit_to(self, future: Future[T], fn: Callable[..., T], *args: Any, **kwargs: Any) -> None:
        """Submit a callable whose outcome resolves a future of the caller.

        The caller creates the future before calling this method, so that it
        keeps a reference to the future even if an interrupt stops the
        scheduling midway, before :meth:`submit` returns. Cancelling the
        future keeps a callable that has not started from running; a started
        callable sets the future's result or exception. If the executor drops
        the callable before it starts, as :class:`S3AioExecutor` does when its
        event loop shuts down, the future is cancelled or gets the error that
        the executor reported.

        Args:
            future: A pending future that no executor has started.
            fn: The callable to run.
            *args: Positional arguments passed to the callable.
            **kwargs: Keyword arguments passed to the callable.
        """

        def run() -> None:
            """Run the callable and resolve the future unless it was cancelled."""
            if not future.set_running_or_notify_cancel():
                return
            try:
                result = fn(*args, **kwargs)
            except BaseException as e:
                future.set_exception(e)
            else:
                future.set_result(result)

        def settle(submitted: Future[None]) -> None:
            """Resolve the future if the executor dropped the callable before it ran.

            run() raises nothing for a future that no executor has started,
            so a cancelled or failed submission means that it did not run,
            and the future is still pending or cancelled by the caller.

            Args:
                submitted: The finished future of the submitted run().
            """
            error: BaseException | None = None
            if submitted.cancelled():
                future.cancel()
            elif (error := submitted.exception()) is None:
                # run() resolved the future.
                return
            # A future cancelled here or by the caller is only marked as
            # notified, which an executor does when it drops a cancelled
            # callable, so that wait() counts it as done.
            if future.set_running_or_notify_cancel():
                future.set_exception(error)

        self.submit(run).add_done_callback(settle)

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
    started. At most ``max_workers`` of the submitted functions run at once.

    This avoids thread-in-thread nesting when ``S3File`` is used from within
    ``asyncio.to_thread()`` calls (the pattern used by ``AioS3FileSystem``).

    Args:
        loop: A running asyncio event loop.
        max_workers: The maximum number of submitted functions that run at once.

    Raises:
        RuntimeError: If the event loop is not running when ``submit`` is called.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop | None = None,
        max_workers: int = (cpu_count() or 1) * 5,
    ) -> None:
        """Initialize the executor with the event loop to schedule work on.

        Args:
            loop: The asyncio event loop. ``submit`` raises ``RuntimeError``
                if it is None or not running.
            max_workers: The maximum number of submitted functions that run
                at once.

        Raises:
            ValueError: If ``max_workers`` is not positive.
        """
        if max_workers <= 0:
            # As ThreadPoolExecutor does; a semaphore of 0 would never run anything.
            raise ValueError("max_workers must be greater than 0")
        self._loop = loop
        self._semaphore = asyncio.Semaphore(max_workers)

    async def _run(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Run the function in a thread once fewer than ``max_workers`` run.

        Args:
            fn: The blocking function to run.
            *args: Positional arguments passed to the function.
            **kwargs: Keyword arguments passed to the function.

        Returns:
            The return value of the function.
        """
        async with self._semaphore:
            return await asyncio.to_thread(fn, *args, **kwargs)

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

            task = asyncio.run_coroutine_threadsafe(self._run(run), self._loop)
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
