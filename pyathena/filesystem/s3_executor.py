# Copyright 2017 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Executors that run S3 filesystem operations in parallel."""

from __future__ import annotations

import asyncio
import contextvars
import os
import sys
import threading
import traceback
import warnings
import weakref
from abc import ABCMeta, abstractmethod
from collections import deque
from collections.abc import Callable, Iterable
from concurrent.futures import CancelledError, Future, wait
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from multiprocessing import cpu_count
from typing import Any, ClassVar, Generic, TypeVar, cast

from pyathena.util import override, stop_retries_on

T = TypeVar("T")
ItemT = TypeVar("ItemT")


class _S3WorkItem(Generic[T]):
    """A submitted callable and the future of the caller that its outcome resolves.

    The executor calls :meth:`run`, and :meth:`settle` is the done callback of
    the submission, which resolves the future if the executor drops the
    callable before it runs. Whichever comes first claims the future, so that
    it is started or settled once, and drops the references of the item to
    the future, the callable, and its arguments, which the frames and
    callbacks of the executor may keep after the call. The arguments of a
    failed callable, such as the body of a part, are therefore freed when it
    fails, and the traceback of its error, which keeps the frame of
    :meth:`run`, forms no reference cycle with the future.
    """

    def __init__(
        self,
        future: Future[T],
        fn: Callable[..., T],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        """Initialize the work item.

        Args:
            future: A pending future that no executor has started.
            fn: The callable to run.
            args: Positional arguments passed to the callable.
            kwargs: Keyword arguments passed to the callable.
        """
        self._future: Future[T] | None = future
        self._fn: Callable[..., T] | None = fn
        self._args = args
        self._kwargs = kwargs
        self._claim = threading.Lock()
        self._submitted: Future[Any] | None = None

    def link(self, future: Future[T], submitted: Future[Any]) -> None:
        """Link the future of the caller with the submission of :meth:`run`.

        The submission settles the future if the executor drops the callable
        (:meth:`settle`), and a cancellation of the future cancels the
        submission, so that a scope drops a waiting call at once instead of
        letting it pass through the pool without running.

        Args:
            future: The future of the caller.
            submitted: The future of the submitted :meth:`run`.
        """
        submitted.add_done_callback(self.settle)
        self._submitted = submitted
        future.add_done_callback(self._cancel_submission)

    def _cancel_submission(self, future: Future[T]) -> None:
        """Cancel the submission if the future of the caller was cancelled.

        The future keeps its callbacks after it has ended, so the reference
        to the submission is dropped here.

        Args:
            future: The future of the caller, which has ended.
        """
        submitted, self._submitted = self._submitted, None
        if submitted is not None and future.cancelled():
            submitted.cancel()

    def run(self) -> None:
        """Run the callable and resolve the future unless it was cancelled."""
        if not self._claim.acquire(blocking=False):
            return
        if not cast(Future[T], self._future).set_running_or_notify_cancel():
            self._take()
            return
        try:
            result = cast(Callable[..., T], self._fn)(*self._args, **self._kwargs)
        except BaseException as e:
            # The frames below this one hold the arguments. clear_frames()
            # skips this frame, which is still executing.
            traceback.clear_frames(e.__traceback__)
            self._take().set_exception(e)
        else:
            self._take().set_result(result)

    def settle(self, submitted: Future[Any]) -> None:
        """Resolve the future if the executor dropped the callable before it ran.

        This happens, for example, when an executor cancels the calls that
        have not started as it shuts down.

        Args:
            submitted: The finished future of the submitted :meth:`run`.
        """
        if not self._claim.acquire(blocking=False):
            # run() has claimed the future and resolves it.
            return
        future = self._take()
        if submitted.cancelled():
            future.cancel()
        # A cancelled future is only marked as notified, as an executor does
        # when it drops a cancelled callable, so that wait() counts it as done.
        if future.set_running_or_notify_cancel():
            future.set_exception(
                submitted.exception() or RuntimeError("The executor did not run the call.")
            )

    def _take(self) -> Future[T]:
        """Drop the references of the item and return its future.

        Returns:
            The future of the caller.
        """
        future = cast(Future[T], self._future)
        self._future = self._fn = None
        self._args, self._kwargs = (), {}
        return future


@dataclass(frozen=True)
class S3MapResult(Generic[T]):
    """The outcomes of :meth:`S3Executor.map` and :meth:`S3Executor.map_async`.

    Attributes:
        outcomes: The result of each item, or the exception that its call
            raised, in the order of the items.
        failure: The first failure that counted, in the order the calls
            ended, or None.
    """

    outcomes: list[T | BaseException]
    failure: BaseException | None


class _S3Batch(Generic[ItemT, T]):
    """The calls of one map of :class:`S3Executor`, and the state that they share.

    A call checks the stop of the batch before it starts, and records the
    first failure that counts in its own thread, before that thread can take
    another call, so that no call starts after the failure that stops the
    batch, and the failure recorded is the first in the order the calls
    ended.

    Args:
        fn: The callable to run for each item.
        stop_on_failure: Whether a failure that counts stops the batch.
        counts: Whether a failure counts; every failure if None.
    """

    def __init__(
        self,
        fn: Callable[[ItemT], T],
        stop_on_failure: bool,
        counts: Callable[[BaseException], bool] | None,
    ) -> None:
        """Initialize the batch.

        Args:
            fn: The callable to run for each item.
            stop_on_failure: Whether a failure that counts stops the batch.
            counts: Whether a failure counts; every failure if None.
        """
        self._fn = fn
        self._stop_on_failure = stop_on_failure
        self._counts = counts
        # Set when the batch stops, so that the calls that have not started
        # make no request and the running ones make no further attempt.
        self.stop = threading.Event()
        self._failure: BaseException | None = None
        self._lock = threading.Lock()
        # Set when every call has settled, or when a failure stops the
        # batch, so that the waiting thread waits for one future, which also
        # wakes it for a failure that is a cancellation.
        self._ended: Future[None] = Future()
        self._remaining = 0
        self._submitting = True
        # The futures of the calls, each tracked before it is scheduled, so
        # that an interrupt while it is being scheduled still lets it be
        # cancelled.
        self.futures: list[Future[T]] = []

    def submit(self, executor: S3Executor, items: Iterable[ItemT]) -> None:
        """Submit a call for each item.

        Args:
            executor: The executor that runs the calls.
            items: The items.
        """
        with stop_retries_on(self.stop):
            for item in items:
                with self._lock:
                    self._remaining += 1
                self.futures.append(Future())
                # Records the outcome when the future is set, before the
                # thread that sets it takes another call, also of a call that
                # the executor fails or cancels without running it.
                self.futures[-1].add_done_callback(self._settled)
                executor.submit_to(self.futures[-1], self._call, item)
        with self._lock:
            self._submitting = False
            ended = self._remaining == 0
        if ended:
            self._end()

    def wait(self) -> None:
        """Wait until every call has ended or a failure has stopped the batch.

        The states are also checked every second, since an interrupt of a
        cancellation can skip the callbacks that end the wait.
        """
        while not self._ended.done() and not all(f.done() for f in self.futures):
            wait([self._ended], timeout=1.0)

    async def wait_async(self) -> None:
        """Await until every call has ended or a failure has stopped the batch."""
        ended = S3Executor.waiter(self._ended)
        while not self._ended.done() and not all(f.done() for f in self.futures):
            await asyncio.wait([ended], timeout=1.0)

    def result(self) -> S3MapResult[T]:
        """Return the outcomes of the calls, which have all ended, and drop their futures.

        The futures are dropped here, not by the map, so that an interrupt
        after this method returns finds no future kept by the batch; their
        callbacks refer to the batch, which would otherwise form a reference
        cycle with them that keeps the outcomes until the cyclic garbage
        collector frees them.

        Returns:
            The outcomes.

        Raises:
            BaseException: The failure that stopped the batch.
        """
        for future in self.futures:
            if future.done():
                # Also here, since a future wakes its waiters before it runs
                # the callback that records it; exception() would wait for a
                # call that has not ended.
                self._record(self._outcome(future))
        if self._stopped_by_failure():
            raise cast(BaseException, self._failure)
        result = S3MapResult(
            [e if (e := self._outcome(f)) is not None else f.result() for f in self.futures],
            self._failure,
        )
        self.clear()
        return result

    @staticmethod
    def _outcome(future: Future[Any]) -> BaseException | None:
        """Return the failure of a call that has ended, or None if it succeeded.

        Args:
            future: The future of the call.

        Returns:
            Its exception, a ``CancelledError`` if the executor cancelled it,
            or None.
        """
        return CancelledError() if future.cancelled() else future.exception()

    def drain(self) -> None:
        """Stop the batch, cancel the calls that have not started, and wait for the others.

        The states are also checked every second, since an interrupt of a
        cancellation can skip the notification that ``wait()`` relies on.
        """
        started = self._stop()
        try:
            while not all(future.done() for future in started):
                wait(started, timeout=1.0)
        finally:
            started.clear()

    async def drain_async(self) -> None:
        """Stop the batch, cancel the calls that have not started, and await the others."""
        started = self._stop()
        try:
            waiters = [S3Executor.waiter(future) for future in started]
            while not all(future.done() for future in started):
                await asyncio.wait(waiters, timeout=1.0)
        finally:
            started.clear()

    def release(self, error: BaseException) -> None:
        """Drop the futures of a batch that raises, and their outcomes, such as the bodies of reads.

        The frames below the map, which the traceback of the error keeps, are
        cleared; :func:`traceback.clear_frames` skips the frames that are
        executing.

        Args:
            error: The error that the map raises.
        """
        try:
            traceback.clear_frames(error.__traceback__)
            # A further interrupt that stopped the drain propagates instead;
            # the frames of the drain, which keep the futures, are in its
            # traceback.
            if (propagating := sys.exception()) is not None and propagating is not error:
                traceback.clear_frames(propagating.__traceback__)
        finally:
            # Also if a further interrupt stopped the drain before its stop.
            self.stop.set()
            self.clear()

    def clear(self) -> None:
        """Drop the futures of the calls.

        The done callback of each future refers to the batch, so a batch
        that kept its futures would form a reference cycle with them, and
        keep their outcomes until the cyclic garbage collector frees them.
        """
        self.futures.clear()

    def _stop(self) -> list[Future[T]]:
        """Stop the batch and cancel the calls that have not started.

        The stop is set first, so that a further interrupt during the
        cancellation still stops the retries of the running calls; a thread
        that the cancellation frees finds the stop before it starts a call.
        The futures are popped, so that no frame keeps them.

        Returns:
            The futures of the calls that have started.
        """
        self.stop.set()
        started = []
        while self.futures:
            future = self.futures.pop()
            if not future.cancel():
                started.append(future)
        return started

    def _stopped_by_failure(self) -> bool:
        """Return whether a failure has stopped the batch.

        Returns:
            Whether a failure that counts has stopped the batch.
        """
        return self._stop_on_failure and self._failure is not None

    def _call(self, item: ItemT) -> T:
        """Run the callable for an item unless the batch has stopped.

        Args:
            item: The item.

        Returns:
            The return value of the callable.

        Raises:
            CancelledError: If the batch has stopped before the call started.
        """
        if self.stop.is_set():
            raise CancelledError
        try:
            return self._fn(item)
        except BaseException as e:
            self._record(e)
            raise

    def _settled(self, future: Future[T]) -> None:
        """Record the failure of a call whose future has been set, as its done callback.

        A done callback must not raise, which would skip the callbacks after
        it, such as the one that wakes an asyncio map, so an error of the
        ``counts`` predicate is the failure.

        Args:
            future: The future of the call.
        """
        try:
            self._record(self._outcome(future))
        except BaseException as e:
            self._latch(e)
        finally:
            with self._lock:
                self._remaining -= 1
                ended = not self._submitting and self._remaining == 0
            if ended:
                self._end()

    def _record(self, error: BaseException | None) -> None:
        """Record the first failure that counts, and stop the batch with it if requested.

        A call records its own failure in its thread before its future is
        set; the done callback of the future records those that the executor
        reports without running the call.

        Args:
            error: The failure of a call, or None if it succeeded.
        """
        if error is None:
            return
        if isinstance(error, Exception) and self._counts is not None and not self._counts(error):
            return
        self._latch(error)

    def _latch(self, error: BaseException) -> None:
        """Record a failure unless one was recorded, and stop the batch with it if requested.

        Args:
            error: The failure.
        """
        with self._lock:
            if self._failure is not None:
                return
            self._failure = error
            if self._stop_on_failure:
                self.stop.set()
        if self._stop_on_failure:
            self._end()

    def _end(self) -> None:
        """End the wait of the waiting thread, once."""
        with self._lock:
            if self._ended.done():
                return
            self._ended.set_result(None)


class S3Executor(metaclass=ABCMeta):
    """Abstract executor for parallel S3 operations.

    Defines the interface used by ``S3File`` and ``S3FileSystem`` for submitting
    work to run in parallel and for shutting down the executor when done.
    Both ``submit`` and ``shutdown`` mirror the ``concurrent.futures.Executor``
    interface so that ``as_completed()`` and ``Future.cancel()`` work unchanged.
    Each submitted callable runs with a copy of the context variables of its
    submitter, so that the stop scopes of :func:`~pyathena.util.stop_retries_on`
    reach the requests that it makes.
    """

    @abstractmethod
    def submit(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> Future[T]:
        """Submit a callable for execution and return a Future.

        An implementation runs the callable in a copy of the submitter's
        context, such as with ``contextvars.copy_context().run``, so that the
        stop scopes of the submitter reach it.
        """
        ...

    @abstractmethod
    def shutdown(self, wait: bool = True) -> None:
        """Shut down the executor, freeing any resources."""
        ...

    async def run(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Run a blocking call in the executor and wait for it.

        Cancelling the waiting task cancels a call that has not started, and
        stops the retries of the AWS API calls that a running call makes, as
        :func:`~pyathena.aio.util.run_in_thread` does; the call itself keeps
        running until it returns or raises.

        Args:
            fn: The blocking callable.
            *args: Positional arguments passed to the callable.
            **kwargs: Keyword arguments passed to the callable.

        Returns:
            The return value of the call.

        Raises:
            RuntimeError: If the executor has been shut down.
        """
        stop = threading.Event()
        # Orders the end of the call with a cancellation, after which work
        # that the call left running, such as the parts of a write in a
        # transaction, is not stopped.
        lock = threading.Lock()
        returned = False

        def call() -> T:
            """Run the call in a scope that the cancellation stops until it returns."""
            nonlocal returned
            try:
                with stop_retries_on(stop):
                    return fn(*args, **kwargs)
            finally:
                with lock:
                    returned = True

        try:
            return await asyncio.wrap_future(self.submit(call))
        except asyncio.CancelledError:
            with lock:
                if not returned:
                    stop.set()
            raise

    async def run_shielded(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Run a blocking call that a cancellation of the waiting task does not stop.

        The call runs to its end, even if it has not started when the waiting
        task is cancelled, and its retries are not stopped; its outcome is then
        dropped. See :meth:`wait_shielded`.

        Args:
            fn: The blocking callable.
            *args: Positional arguments passed to the callable.
            **kwargs: Keyword arguments passed to the callable.

        Returns:
            The return value of the call.

        Raises:
            RuntimeError: If the executor has been shut down.
        """
        return await self.wait_shielded(self.submit(fn, *args, **kwargs))

    async def wait_shielded(self, future: Future[T]) -> T:
        """Wait for a submitted call that a cancellation of the waiting task does not cancel.

        Unlike awaiting ``asyncio.wrap_future(future)``, a cancellation of
        the waiting task leaves the future to the caller, which can still
        wait for it or cancel it, and its outcome is retrieved, so that
        asyncio does not log a failure that ends after the cancellation as
        never retrieved.

        Args:
            future: The future of a call submitted to this executor.

        Returns:
            The return value of the call.
        """
        waiter = self.waiter(future)
        # Unlike shield(), which on Python 3.14 and later also logs the failure
        # of a future that ends after a cancellation, wait() leaves the future
        # alone when the waiting task is cancelled.
        await asyncio.wait([waiter])
        return waiter.result()

    @staticmethod
    def waiter(future: Future[T]) -> asyncio.Future[T]:
        """Wrap a future of an executor for asyncio, retrieving its outcome.

        The outcome belongs to the caller that awaits the wrapper; it is
        retrieved, so that asyncio does not log a failure as never retrieved
        when no one awaits it, such as after a cancellation.

        Args:
            future: The future.

        Returns:
            The asyncio future.
        """
        waiter = asyncio.wrap_future(future)
        waiter.add_done_callback(S3Executor._retrieve)
        return waiter

    @staticmethod
    def _retrieve(waiter: asyncio.Future[Any]) -> None:
        """Retrieve the outcome of a waiter, so that asyncio does not log it.

        Args:
            waiter: The asyncio future, which has ended.
        """
        if not waiter.cancelled():
            waiter.exception()

    def submit_to(self, future: Future[T], fn: Callable[..., T], *args: Any, **kwargs: Any) -> None:
        """Submit a callable whose outcome resolves a future of the caller.

        The caller creates the future before calling this method, so that it
        keeps a reference to the future even if an interrupt stops the
        scheduling midway, before :meth:`submit` returns. Cancelling the
        future keeps a callable that has not started from running; a started
        callable sets the future's result or exception. If the executor drops
        the callable before it starts, such as by cancelling it at its
        shutdown, the future is cancelled or gets the error that the
        executor reported.

        The arguments of a failed callable, such as the body of a part, are
        freed when it fails: its error keeps the frames that it was raised
        from, without their local variables. The errors that it was raised
        from keep theirs; :meth:`S3Core.call()
        <pyathena.filesystem.s3_core.S3Core.call>` clears those of the
        ``ClientError`` of a request.

        Args:
            future: A pending future that no executor has started.
            fn: The callable to run.
            *args: Positional arguments passed to the callable.
            **kwargs: Keyword arguments passed to the callable.
        """
        item = _S3WorkItem(future, fn, args, kwargs)
        item.link(future, self.submit(item.run))

    def map(
        self,
        fn: Callable[[ItemT], T],
        items: Iterable[ItemT],
        *,
        stop_on_failure: bool = True,
        counts: Callable[[BaseException], bool] | None = None,
    ) -> S3MapResult[T]:
        """Run a callable for each item in the executor and wait for the calls.

        Every item is submitted at once, and the executor applies its own
        limit, such as the ``max_workers`` of a scope. Each call runs in a
        scope of :func:`~pyathena.util.stop_retries_on` that the stop of the
        batch ends.

        With ``stop_on_failure``, the first failure that counts stops the
        batch: the calls that have not started are cancelled, the running
        ones make no further attempt, and the failure is raised once they
        have ended. An interrupt of the waiting thread, such as
        ``KeyboardInterrupt``, does the same whatever ``stop_on_failure`` is,
        and is raised once the running calls have ended. A further interrupt
        stops that wait; the running calls then end in the executor.

        A thread of a scope's pool that maps on that scope runs the calls
        itself. A thread of a bare :class:`S3ThreadPool` that maps on that
        pool waits for threads that it may hold, so a pool must not map on
        itself.

        Args:
            fn: The callable to run for each item.
            items: The items.
            stop_on_failure: Whether a failure that counts stops the batch.
            counts: Whether a failure counts, by default every one. A failure
                that does not count neither stops the batch nor becomes
                :attr:`S3MapResult.failure`.

        Returns:
            The outcomes of the calls.

        Raises:
            BaseException: The failure that stopped the batch.
            RuntimeError: If the executor has been shut down.
        """
        batch = _S3Batch(fn, stop_on_failure, counts)
        try:
            batch.submit(self, items)
            batch.wait()
            return batch.result()
        except BaseException as e:
            try:
                batch.drain()
            finally:
                batch.release(e)
            raise

    async def map_async(
        self,
        fn: Callable[[ItemT], T],
        items: Iterable[ItemT],
        *,
        stop_on_failure: bool = True,
        counts: Callable[[BaseException], bool] | None = None,
    ) -> S3MapResult[T]:
        """Run a callable for each item in the executor and await the calls.

        As :meth:`map`, with a cancellation of the waiting task in place of
        an interrupt. The task waits for the running calls also when the
        event loop shuts down with ``asyncio.run()``, which cancels it and
        runs the loop until it ends; a loop that is closed without
        cancelling it leaves the running calls to end in the executor.

        Args:
            fn: The callable to run for each item.
            items: The items.
            stop_on_failure: Whether a failure that counts stops the batch.
            counts: Whether a failure counts, by default every one.

        Returns:
            The outcomes of the calls.

        Raises:
            BaseException: The failure that stopped the batch.
            RuntimeError: If the executor has been shut down.
        """
        batch = _S3Batch(fn, stop_on_failure, counts)
        try:
            batch.submit(self, items)
            await batch.wait_async()
            return batch.result()
        except BaseException as e:
            try:
                await batch.drain_async()
            finally:
                batch.release(e)
            raise

    def __enter__(self) -> S3Executor:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.shutdown(wait=True)


# The pool, weakly referenced, whose thread this is.
_pool_thread = threading.local()


class S3ThreadPool(S3Executor):
    """A thread pool for the requests of the S3 filesystems.

    It runs at most ``max_workers`` calls at once in threads of its own, so
    that, unlike ``asyncio.to_thread()``, which runs a call in the default
    executor of the event loop, neither the size nor the load of that
    executor limits or blocks the calls of asyncio code. The calls are
    queued in the order they were submitted. The threads are created as the calls need
    them, and each call runs with a copy of the caller's context variables,
    as with ``asyncio.to_thread()``. In a forked child process, whose copy
    has no threads, the pool creates new ones, as it does after
    :meth:`release`. The pool can be shared by several threads and event
    loops.

    ``S3ThreadPoolExecutor`` and ``S3AioThreadPool`` are aliases of this
    class.

    Args:
        max_workers: The maximum number of calls that run at once.

    Raises:
        ValueError: If ``max_workers`` is not positive.
    """

    # The pools of this process, which a forked child resets.
    _instances: ClassVar[weakref.WeakSet[S3ThreadPool]] = weakref.WeakSet()
    # The executors that a forked child copied from its parent. They are
    # kept, since collecting one would put to a queue whose lock a thread of
    # the parent may have held.
    _forked_executors: ClassVar[list[ThreadPoolExecutor]] = []

    def __init__(self, max_workers: int | None = None) -> None:
        """Initialize the pool without starting any thread.

        Args:
            max_workers: The maximum number of calls that run at once. If
                None, the default of ``ThreadPoolExecutor``: ``min(32, CPUs +
                4)``.

        Raises:
            ValueError: If ``max_workers`` is not positive.
        """
        if max_workers is None:
            # As ThreadPoolExecutor, which counts the CPUs that the process
            # may use since Python 3.13.
            cpus = getattr(os, "process_cpu_count", os.cpu_count)()
            max_workers = min(32, (cpus or 1) + 4)
        if max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")
        self.max_workers = max_workers
        self._lock = threading.Lock()
        self._executor: ThreadPoolExecutor | None = None
        self._shutdown = False
        # The futures of the calls that were submitted and have not ended,
        # and whether release() waits for them to end.
        self._futures: set[Future[Any]] = set()
        self._releasing = False
        # A future keeps its callbacks after it has ended, so its callback
        # refers to the pool weakly, to let a pool whose futures are kept be
        # collected with its threads.
        pool = weakref.ref(self)

        def end_call(future: Future[Any]) -> None:
            """Count the end of a call if the pool still exists.

            Args:
                future: The future of the call, which has ended.
            """
            if (live := pool()) is not None:
                live._end_call(future)

        self._end_call_callback = end_call
        S3ThreadPool._instances.add(self)

    @override
    def submit(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> Future[T]:
        """Schedule a blocking call in a thread of the pool.

        As with ``ThreadPoolExecutor``, the returned future can be cancelled
        only before the call starts. The calls are queued in the order they
        were submitted, so that the calls submitted before a call that waits
        for them are taken by threads before it, and it does not wait for a
        thread that it holds.

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
        with self._lock:
            future = self._get_executor().submit(context.run, fn, *args, **kwargs)
            self._futures.add(future)
        future.add_done_callback(self._end_call_callback)
        return future

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

    def release(self) -> None:
        """Release the threads once no call runs or waits, keeping the pool usable.

        Until then, the calls, including those submitted after this method,
        run in the threads of the pool, at most ``max_workers`` at once. A
        call submitted after the threads have been released creates new ones.
        This method does not block.
        """
        with self._lock:
            self._releasing = True
            # An interrupt can keep submit() from adding the callback of a
            # call, whose future then ends without leaving the set until here.
            self._futures = {future for future in self._futures if not future.done()}
            executor = self._take_idle_executor()
        if executor is not None:
            executor.shutdown(wait=False)

    def _end_call(self, future: Future[Any]) -> None:
        """Count the end of a call, releasing the threads after the last one if requested.

        Args:
            future: The future of the call, which has ended.
        """
        with self._lock:
            # A future of the parent of a forked child is not in the set.
            self._futures.discard(future)
            executor = self._take_idle_executor()
        if executor is not None:
            executor.shutdown(wait=False)

    def _take_idle_executor(self) -> ThreadPoolExecutor | None:
        """Take the executor to shut down if a release waits and no call runs or waits.

        The caller holds the lock of the pool.

        Returns:
            The executor, or None if there is none to shut down yet.
        """
        if not self._releasing or self._futures:
            return None
        self._releasing = False
        executor, self._executor = self._executor, None
        return executor

    def owns_current_thread(self) -> bool:
        """Whether the calling thread is a thread of this pool.

        Returns:
            True in a thread that this pool started.
        """
        pool = getattr(_pool_thread, "pool", None)
        return pool is not None and pool() is self

    @classmethod
    def _reset_after_fork(cls) -> None:
        """Drop the threads and locks that a forked child copied from its parent.

        The threads of the parent do not exist in the child, and a lock that
        one of them held stays held, so each pool creates new ones.
        """
        for pool in list(cls._instances):
            if pool._executor is not None:
                cls._forked_executors.append(pool._executor)
            pool._lock = threading.Lock()
            pool._executor = None
            pool._futures = set()
            pool._releasing = False

    def _get_executor(self) -> ThreadPoolExecutor:
        """Return the thread pool of this process, creating it if needed.

        The caller holds the lock of the pool.

        Returns:
            The thread pool.

        Raises:
            RuntimeError: If the pool has been shut down.
        """
        if self._shutdown:
            raise RuntimeError("cannot schedule new futures after shutdown")
        if self._executor is None:
            pool = weakref.ref(self)
            self._executor = ThreadPoolExecutor(
                max_workers=self.max_workers,
                initializer=lambda: setattr(_pool_thread, "pool", pool),
            )
        return self._executor


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=S3ThreadPool._reset_after_fork)


class _S3PoolScope(S3Executor):
    """The calls of one file or operation in a pool that others share.

    At most ``max_workers`` calls of the scope are in the pool at once; the
    others wait in the scope, in the order they were submitted, and enter the
    pool as earlier ones end, from the thread of the call that ends. Each call
    runs with a copy of the context of its submitter. :meth:`shutdown` stops
    accepting calls and, with ``wait``, waits for those submitted, without
    shutting the pool down, so that a file or an operation drains its own
    requests while others keep using the pool.

    A call counts toward ``max_workers`` once the pool has accepted it, so
    that an interrupt of the submitter at any point leaves no place taken.
    A call whose submission an interrupt stopped after the pool accepted it
    may then run without counting, so that each such interrupt can let one
    more call run at once.

    Args:
        pool: The shared pool.
        max_workers: The maximum number of calls of the scope in the pool.

    Raises:
        ValueError: If ``max_workers`` is not positive.
    """

    def __init__(self, pool: S3ThreadPool, max_workers: int) -> None:
        """Initialize the scope on a shared pool.

        Args:
            pool: The shared pool.
            max_workers: The maximum number of calls of the scope in the pool.

        Raises:
            ValueError: If ``max_workers`` is not positive.
        """
        if max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")
        self.max_workers = max_workers
        self._pool = pool
        self._lock = threading.Lock()
        # The calls that were submitted and have not ended, by future.
        self._futures: dict[Future[Any], _S3ScopeCall] = {}
        # The futures of the calls that threads of the pool run themselves.
        self._inline: set[Future[Any]] = set()
        # The calls that wait for a place in the pool, and those that the
        # pool has accepted and that have not ended.
        self._waiting: deque[_S3ScopeCall] = deque()
        # The waiting calls that have not been cancelled, by future.
        self._waiting_calls: dict[Future[Any], _S3ScopeCall] = {}
        self._entered: set[_S3ScopeCall] = set()
        self._shutdown = False

    @override
    def submit(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> Future[T]:
        """Submit a call, which enters the pool once fewer than ``max_workers`` calls are there.

        As with ``ThreadPoolExecutor``, the returned future can be cancelled
        only before the call starts. An interrupt of the submission cancels
        the call unless it has started.

        Args:
            fn: The callable to run.
            *args: Positional arguments passed to the callable.
            **kwargs: Keyword arguments passed to the callable.

        Returns:
            The future of the call.

        Raises:
            RuntimeError: If the scope or the pool has been shut down.
        """
        future: Future[T] = Future()
        # The context is captured here, since a waiting call enters the pool
        # from the thread of the call that ends before it.
        call = _S3ScopeCall(
            contextvars.copy_context(), _S3WorkItem(future, fn, args, kwargs), future
        )
        if self._pool.owns_current_thread():
            # A thread of the pool, such as one in which garbage collection
            # closes a file, runs the call itself instead of waiting for a
            # place in the pool that it holds (T1). It does not count toward
            # max_workers, and a shutdown waits for it.
            with self._lock:
                if self._shutdown:
                    raise RuntimeError("cannot schedule new futures after shutdown")
                self._futures[future] = call
                self._inline.add(future)
            future.add_done_callback(self._untrack)
            call.context.run(call.item.run)
            return future
        try:
            with self._lock:
                if self._shutdown:
                    raise RuntimeError("cannot schedule new futures after shutdown")
                self._futures[future] = call
                # Behind any waiting call, so that the calls enter in order,
                # also when a place frees before a waiting call takes it.
                enter = not self._waiting_calls and len(self._entered) < self.max_workers
                if enter:
                    # Counted only once the pool has accepted it.
                    self._pool.submit(self._run, call)
                    self._entered.add(call)
                else:
                    self._waiting.append(call)
                    self._waiting_calls[future] = call
            future.add_done_callback(self._untrack)
        except BaseException:
            # A call that the pool has not taken, or that has not started,
            # never runs, and nothing waits for it; one that has started
            # runs to its end. The work item marks the cancelled future as
            # notified once, also when the pool or the scope runs it later.
            if future.cancel():
                call.item.run()
                with self._lock:
                    self._futures.pop(future, None)
            raise
        return future

    @override
    def shutdown(self, wait: bool = True) -> None:
        """Stop accepting calls and, with ``wait``, wait for those submitted.

        The pool keeps running, also for the calls of this scope that have
        not entered it yet.

        Args:
            wait: Whether to wait for the calls submitted through this scope.
        """
        with self._lock:
            self._shutdown = True
        pending = self.pending()
        if wait and pending:
            # Waits through done callbacks, which a cancellation runs at once,
            # not through futures.wait(), which waits for a cancelled future
            # until the pool reaches its call and marks it as notified. The
            # states are also checked every second, since an interrupt of a
            # cancellation can skip its callbacks.
            ended = threading.Event()
            for future in pending:
                future.add_done_callback(lambda _: ended.set())
            while not all(future.done() for future in pending):
                ended.wait(1.0)
                ended.clear()

    def pending(self) -> list[Future[Any]]:
        """Return the futures of the calls that were submitted and have not ended.

        A call that has neither entered the pool, nor waits for a place, nor
        started, is one whose submission interrupts stopped before its
        cleanup ended. It is cancelled instead, and a cancelled call is
        left out, so that nothing waits forever for a call that never runs.

        Returns:
            The futures, which an asyncio caller can wait for without
            blocking the event loop, such as with :meth:`wait_shielded`.
        """
        with self._lock:
            tracked = {call.future for call in self._entered}
            tracked.update(self._waiting_calls, self._inline)
            calls = list(self._futures.items())
        pending = []
        for future, call in calls:
            if future.cancelled():
                # Never runs, also if an interrupt stopped its notification.
                continue
            if future in tracked or not future.cancel():
                pending.append(future)
            else:
                # Marks the cancelled future as notified, so that wait()
                # counts it as done; the call does not run.
                call.item.run()
        return pending

    def _run(self, call: _S3ScopeCall) -> None:
        """Run a call in the pool, then let the next waiting calls enter it.

        The next calls enter the pool from this thread, before this call ends
        there, so that a release of the pool waits for the calls that wait
        here. If the pool takes no more calls, as when the interpreter exits,
        this thread runs the waiting calls itself.

        Args:
            call: The call.
        """
        try:
            call.context.run(call.item.run)
        finally:
            self._next(call)

    def _next(self, ended: _S3ScopeCall) -> None:
        """Free the place of a call that has ended, and fill the free places.

        Calls that were cancelled while they waited are settled without
        entering the pool.

        Args:
            ended: The call that has ended.
        """
        freed = False
        while True:
            with self._lock:
                if not freed:
                    # Freed under the lock that hands the place to the oldest
                    # waiting call, so that a new submitter cannot take it.
                    self._entered.discard(ended)
                    freed = True
                if not self._waiting or len(self._entered) >= self.max_workers:
                    return
                call = self._waiting.popleft()
                if self._waiting_calls.pop(call.future, None) is None:
                    # Cancelled while it waited, and already settled.
                    continue
                error: Exception | None = None
                try:
                    self._pool.submit(self._run, call)
                except RuntimeError:
                    # The pool takes no more calls, as when the interpreter
                    # exits: this thread runs the call, which a shutdown
                    # waits for.
                    run_inline = True
                    self._inline.add(call.future)
                except Exception as e:
                    # Such as a failure to start a thread: the call fails.
                    run_inline, error = False, e
                else:
                    run_inline = False
                    self._entered.add(call)
            if run_inline:
                call.context.run(call.item.run)
            elif error is not None:
                rejected: Future[None] = Future()
                rejected.set_running_or_notify_cancel()
                rejected.set_exception(error)
                call.item.settle(rejected)

    def _untrack(self, future: Future[Any]) -> None:
        """Stop tracking a call that has ended or was cancelled.

        A call cancelled while it waits leaves the queue at once, so that its
        arguments and context are not kept until a place frees.

        Args:
            future: The future of the call.
        """
        with self._lock:
            self._futures.pop(future, None)
            self._inline.discard(future)
            dropped = self._waiting_calls.pop(future, None) if future.cancelled() else None
            if dropped is not None and len(self._waiting) > 2 * len(self._waiting_calls) + 16:
                # Drops the entries of cancelled calls, so that calls submitted
                # and cancelled behind a long call do not accumulate.
                self._waiting = deque(c for c in self._waiting if c.future in self._waiting_calls)
        if dropped is not None:
            # Marks the future as notified and drops the arguments, without
            # running the call; the entry left in the queue is skipped.
            dropped.item.run()
            dropped.context = contextvars.Context()


class _S3ScopeCall:
    """A call of an :class:`_S3PoolScope`, with the context of its submitter.

    Args:
        context: The context of the submitter.
        item: The work item of the call.
        future: The future of the call.
    """

    __slots__ = ("context", "future", "item")

    def __init__(
        self, context: contextvars.Context, item: _S3WorkItem[Any], future: Future[Any]
    ) -> None:
        """Initialize the call.

        Args:
            context: The context of the submitter.
            item: The work item of the call.
            future: The future of the call.
        """
        self.context = context
        self.item = item
        self.future = future


class S3AioExecutor(S3ThreadPool):
    """A thread pool kept for compatibility, an :class:`S3ThreadPool`.

    It used to dispatch the calls through an asyncio event loop. The calls
    now run directly in the pool, so they need no running loop, and they run
    to their end when the loop stops.

    Args:
        loop: Ignored, with a ``DeprecationWarning`` if given.
        max_workers: The maximum number of calls that run at once.

    Raises:
        ValueError: If ``max_workers`` is not positive.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop | None = None,
        max_workers: int = (cpu_count() or 1) * 5,
    ) -> None:
        """Initialize the pool without starting any thread.

        Args:
            loop: Ignored, with a ``DeprecationWarning`` if given.
            max_workers: The maximum number of calls that run at once.

        Raises:
            ValueError: If ``max_workers`` is not positive.
        """
        if loop is not None:
            warnings.warn(
                "The loop argument of S3AioExecutor is ignored: the calls run in the pool "
                "without the event loop. S3AioExecutor is kept for compatibility; use "
                "S3ThreadPool instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        super().__init__(max_workers)


#: An alias of :class:`S3ThreadPool`, kept for compatibility.
S3ThreadPoolExecutor = S3ThreadPool
#: An alias of :class:`S3ThreadPool`, kept for compatibility.
S3AioThreadPool = S3ThreadPool
