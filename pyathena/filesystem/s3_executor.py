# Copyright 2017 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Executors that run S3 filesystem operations in parallel."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import os
import threading
import weakref
from abc import ABCMeta, abstractmethod
from collections.abc import Callable
from concurrent import futures
from concurrent.futures import Future
from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing import cpu_count
from typing import Any, ClassVar, TypeVar

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


class S3AioThreadPool(S3Executor):
    """A thread pool that runs the blocking calls of asyncio code.

    Unlike ``asyncio.to_thread()``, which runs a call in the default executor
    of the event loop, this pool runs at most ``max_workers`` calls at once in
    threads of its own, so that neither the size nor the load of the default
    executor limits or blocks them. The threads are created as the calls need
    them, and each call runs with a copy of the caller's context variables,
    as with ``asyncio.to_thread()``. In a forked child process, whose copy
    has no threads, the pool creates new ones. The pool can be shared by
    several event loops and threads.

    Args:
        max_workers: The maximum number of calls that run at once.

    Raises:
        ValueError: If ``max_workers`` is not positive.
    """

    # The pools of this process, which a forked child resets.
    _instances: ClassVar[weakref.WeakSet[S3AioThreadPool]] = weakref.WeakSet()

    def __init__(self, max_workers: int | None = None) -> None:
        """Initialize the pool without starting any thread.

        Args:
            max_workers: The maximum number of calls that run at once. If
                None, ``min(32, os.cpu_count() + 4)``, as with
                ``ThreadPoolExecutor``.

        Raises:
            ValueError: If ``max_workers`` is not positive.
        """
        if max_workers is None:
            max_workers = min(32, (os.cpu_count() or 1) + 4)
        if max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")
        self.max_workers = max_workers
        self._lock = threading.Lock()
        self._executor: ThreadPoolExecutor | None = None
        self._shutdown = False
        S3AioThreadPool._instances.add(self)

    @override
    def submit(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> Future[T]:
        """Schedule a blocking call in a thread of the pool.

        As with ``ThreadPoolExecutor``, the returned future can be cancelled
        only before the call starts. The calls start in the order they were
        submitted, so that a call that waits for the calls submitted before it
        does not wait for a thread that it holds.

        Args:
            fn: The blocking callable.
            *args: Positional arguments passed to the callable.
            **kwargs: Keyword arguments passed to the callable.

        Returns:
            The future of the call.

        Raises:
            RuntimeError: If the pool has been shut down.
        """
        context = contextvars.copy_context()
        return self._get_executor().submit(context.run, fn, *args, **kwargs)

    async def run(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Run a blocking call in a thread of the pool and wait for it.

        Cancelling the waiting task cancels a call that has not started; a
        running call keeps running, as with ``asyncio.to_thread()``.

        Args:
            fn: The blocking callable.
            *args: Positional arguments passed to the callable.
            **kwargs: Keyword arguments passed to the callable.

        Returns:
            The return value of the call.

        Raises:
            RuntimeError: If the pool has been shut down.
        """
        return await asyncio.wrap_future(self.submit(fn, *args, **kwargs))

    async def run_shielded(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Run a blocking call that a cancellation of the waiting task does not stop.

        The call runs to its end, even if it has not started when the waiting
        task is cancelled; its outcome is then dropped.

        Args:
            fn: The blocking callable.
            *args: Positional arguments passed to the callable.
            **kwargs: Keyword arguments passed to the callable.

        Returns:
            The return value of the call.

        Raises:
            RuntimeError: If the pool has been shut down.
        """
        future = asyncio.wrap_future(self.submit(fn, *args, **kwargs))
        # Retrieved, so that asyncio does not log the failure of a call whose
        # waiting task was cancelled as never retrieved.
        future.add_done_callback(lambda f: f.cancelled() or f.exception())
        return await asyncio.shield(future)

    @override
    def shutdown(self, wait: bool = True) -> None:
        """Stop accepting calls and release the threads once they are idle.

        The calls that were submitted before still run.

        Args:
            wait: Whether to wait for the submitted calls to finish.
        """
        with self._lock:
            self._shutdown = True
            executor = self._executor
        if executor is not None:
            executor.shutdown(wait=wait)

    @classmethod
    def _reset_after_fork(cls) -> None:
        """Drop the threads and locks that a forked child copied from its parent.

        The threads of the parent do not exist in the child, and a lock that
        one of them held stays held, so each pool creates new ones.
        """
        for pool in list(cls._instances):
            pool._lock = threading.Lock()
            pool._executor = None

    def _get_executor(self) -> ThreadPoolExecutor:
        """Return the thread pool of this process, creating it if needed.

        Returns:
            The thread pool.

        Raises:
            RuntimeError: If the pool has been shut down.
        """
        if (executor := self._executor) is not None:
            # Its submit() raises after the shutdown.
            return executor
        with self._lock:
            if self._shutdown:
                raise RuntimeError("cannot schedule new futures after shutdown")
            if self._executor is None:
                self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
            return self._executor


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=S3AioThreadPool._reset_after_fork)


class S3AioExecutor(S3Executor):
    """Executor that schedules work on an asyncio event loop.

    Uses ``asyncio.run_coroutine_threadsafe()`` to dispatch blocking functions
    through the event loop to an ``S3AioThreadPool`` of its own, returning
    ``concurrent.futures.Future`` objects that are compatible with
    ``as_completed()``, ``wait()`` and ``Future.cancel()``. As with
    ``ThreadPoolExecutor``, a future cannot be cancelled once its function has
    started. At most ``max_workers`` of the submitted functions run at once,
    whatever the size of the default executor of the event loop.

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
        self._loop = loop
        self._pool = S3AioThreadPool(max_workers)
        # Orders the dispatches with the shutdown.
        self._lock = threading.Lock()
        self._shutdown = False

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

            with self._lock:
                if self._shutdown:
                    raise RuntimeError("cannot schedule new futures after shutdown")
                task = asyncio.run_coroutine_threadsafe(self._pool.run(run), self._loop)
            task.add_done_callback(settle)
            return future
        raise RuntimeError(
            "S3AioExecutor requires a running event loop. "
            "Use S3ThreadPoolExecutor for synchronous usage."
        )

    async def _shutdown_pool(self) -> None:
        """Shut down the pool once the functions submitted before have reached it.

        Run as a task of the event loop, which starts its tasks in the order
        they were scheduled, so the tasks that dispatch the functions
        submitted before have handed them to the pool.
        """
        self._pool.shutdown(wait=False)

    @override
    def shutdown(self, wait: bool = True) -> None:
        with self._lock:
            self._shutdown = True
            closing = None
            if self._loop is not None and self._loop.is_running():
                # A function reaches the pool only when the event loop runs
                # its task, so the pool is shut down after the dispatches.
                closing = asyncio.run_coroutine_threadsafe(self._shutdown_pool(), self._loop)
        if closing is None:
            self._pool.shutdown(wait=wait)
            return
        try:
            on_loop = asyncio.get_running_loop() is self._loop
        except RuntimeError:
            on_loop = False
        # On the thread of the event loop, such as when a file is garbage
        # collected there, waiting would keep the loop from dispatching.
        if wait and not on_loop:
            # Cancelled if the event loop shuts down before it runs.
            with contextlib.suppress(futures.CancelledError):
                closing.result()
            self._pool.shutdown(wait=True)
