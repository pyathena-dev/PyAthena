# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import asyncio
import contextlib
import contextvars
import gc
import sys
import threading
import time
import traceback
import weakref
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor, wait
from typing import Any, cast
from unittest import mock

import pytest
from botocore.exceptions import ClientError

from pyathena.filesystem.s3_executor import (
    S3AioExecutor,
    S3Executor,
    S3ThreadPool,
    _S3Batch,
    _S3PoolScope,
)
from pyathena.util import RetryConfig, override, retry_api_call, stop_retries_on
from tests.pyathena.util import (
    EVENT_TIMEOUT,
    OpcodeInterrupter,
    ThrottledOperation,
    await_bounded,
    call_bounded,
    forking,
    interrupt_each_opcode,
    overlapping_calls,
    poll_until,
    retried,
    run_in_forked_child,
    shutdown_bounded,
    sigint_when,
)


class TestS3ThreadPool:
    def test_submit_interrupted_at_each_opcode(self):
        # GH-1332 (P6, I2): an interrupt at any opcode of submit() leaves
        # nothing to wait for forever: the next call runs, and the shutdown
        # returns.
        def case(interrupt):
            pool = S3ThreadPool(1)
            hold = threading.Event()
            lock = threading.Lock()
            state = {"active": 0, "peak": 0}
            events = []

            overlap = threading.Condition(lock)

            def call(name, timeout):
                with overlap:
                    state["active"] += 1
                    state["peak"] = max(state["peak"], state["active"])
                    overlap.notify_all()
                    if name == "other":
                        # Waits for a third overlapping call, which only a
                        # pool that exceeds the bound starts.
                        overlap.wait_for(lambda: state["peak"] > 2, timeout=timeout)
                try:
                    if name == "call":
                        hold.wait(timeout)
                finally:
                    with overlap:
                        state["active"] -= 1
                events.append(name)

            try:
                # Running until the others have had the time to overlap it.
                future = interrupt(pool.submit, call, "call", EVENT_TIMEOUT)
                pool.release()
                others = [pool.submit(call, "other", 0.1) for _ in range(3)]
                # The others run beside the interrupted call only if the
                # release has left it untracked; otherwise they wait for it.
                wait(others, timeout=0.5)
                hold.set()
                for other in others:
                    other.result(EVENT_TIMEOUT)
                # At most one call more than max_workers runs at once (I2).
                assert state["peak"] <= 2
                shutdown_bounded(pool)
                if future is not None:
                    assert future.done()
                assert events.count("other") == 3
                assert events.count("call") <= 1
            finally:
                hold.set()
                pool.shutdown()

        assert interrupt_each_opcode(S3ThreadPool.submit, case) > 0

    def test_default_max_workers(self):
        # None takes the default of ThreadPoolExecutor, as documented.
        with ThreadPoolExecutor() as executor:
            assert S3ThreadPool().max_workers == executor._max_workers

    def test_run_beyond_default_executor(self):
        # GH-1159: the calls run in threads of the pool, so that max_workers,
        # not the default executor of the event loop, bounds them.
        call, state = overlapping_calls(4)
        pool = S3ThreadPool(4)

        async def main():
            asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(max_workers=1))
            await asyncio.gather(*[pool.run(call) for _ in range(6)])

        asyncio.run(main())
        pool.shutdown()

        assert state["peak"] == 4

    def test_run_context(self):
        # The call sees the context variables of the caller, as with
        # asyncio.to_thread().
        variable: contextvars.ContextVar[str] = contextvars.ContextVar("variable")
        pool = S3ThreadPool(1)

        async def main():
            variable.set("caller")
            return await pool.run(variable.get)

        assert asyncio.run(main()) == "caller"
        pool.shutdown()

    @pytest.mark.parametrize("shielded", [False, True])
    def test_run_cancelled_while_retrying(self, shielded):
        # GH-1203: cancelling the waiting task stops the retries of a running
        # call, unless it runs shielded. Without the stop, the backoff wait
        # would take at least EVENT_TIMEOUT seconds.
        operation = ThrottledOperation(block=True, failures=None if not shielded else 2)
        finished = threading.Event()
        pool = S3ThreadPool(1)

        def work():
            try:
                config = RetryConfig(attempt=3, multiplier=0 if shielded else EVENT_TIMEOUT)
                return retry_api_call(operation, config)
            finally:
                finished.set()

        async def main():
            run = pool.run_shielded if shielded else pool.run
            task = asyncio.ensure_future(run(work))
            await asyncio.to_thread(operation.started.wait, EVENT_TIMEOUT)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            operation.release.set()
            started = time.monotonic()
            assert await asyncio.to_thread(finished.wait, EVENT_TIMEOUT)
            return time.monotonic() - started

        try:
            assert asyncio.run(main()) < EVENT_TIMEOUT
        finally:
            operation.release.set()
            pool.shutdown()

        assert operation.calls == (3 if shielded else 1)

    def test_run_cancelled_after_return(self):
        # GH-1203: a cancellation that arrives after the call has returned does
        # not stop the work that the call left running, such as the parts of a
        # write in a transaction.
        operation = ThrottledOperation(block=True, failures=1)
        left = []
        go = threading.Event()
        pool = S3ThreadPool(1)
        submit = pool.submit

        def submit_and_cancel_after_return(fn, *args, **kwargs):
            future = submit(fn, *args, **kwargs)
            task = cast(asyncio.Task[None], asyncio.current_task())

            def cancel_after_return():
                # Runs once the task waits for the call, and blocks the event
                # loop until the call has returned, so that the task is
                # cancelled before it resumes.
                go.set()
                future.result(EVENT_TIMEOUT)
                task.cancel()

            asyncio.get_running_loop().call_soon(cancel_after_return)
            return future

        pool.submit = submit_and_cancel_after_return  # type: ignore[method-assign]

        def work():
            assert go.wait(EVENT_TIMEOUT)
            context = contextvars.copy_context()
            thread = threading.Thread(
                target=context.run,
                args=(retry_api_call, operation, RetryConfig(attempt=2, multiplier=0)),
            )
            thread.start()
            left.append(thread)

        async def main():
            task = asyncio.ensure_future(pool.run(work))
            with pytest.raises(asyncio.CancelledError):
                await task

        try:
            asyncio.run(main())
            operation.release.set()
            left[0].join(EVENT_TIMEOUT)
        finally:
            operation.release.set()
            pool.shutdown()

        assert operation.calls == 2

    @pytest.mark.parametrize("shielded", [False, True])
    def test_run_cancelled_before_start(self, shielded):
        # Cancelling the waiting task drops a call that has not started,
        # unless it runs shielded.
        events = []
        started = threading.Event()
        release = threading.Event()
        pool = S3ThreadPool(1)

        def work():
            started.set()
            release.wait(EVENT_TIMEOUT)

        running = pool.submit(work)
        submit = pool.submit

        async def main():
            submitted = asyncio.Event()

            def submit_and_notify(fn, *args, **kwargs):
                future = submit(fn, *args, **kwargs)
                submitted.set()
                return future

            pool.submit = submit_and_notify  # type: ignore[method-assign]
            run = pool.run_shielded if shielded else pool.run
            pending = asyncio.ensure_future(run(events.append, "pending finished"))
            assert await asyncio.to_thread(started.wait, EVENT_TIMEOUT)
            # The call waits for the only thread.
            await asyncio.wait_for(submitted.wait(), EVENT_TIMEOUT)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            release.set()
            await asyncio.wrap_future(running)

        asyncio.run(main())
        pool.shutdown()

        assert events == (["pending finished"] if shielded else [])

    def test_shutdown(self):
        # The calls submitted before the shutdown still run.
        events = []
        release = threading.Event()
        pool = S3ThreadPool(1)
        pool.submit(release.wait, EVENT_TIMEOUT)
        pending = pool.submit(events.append, "pending finished")
        pool.shutdown(wait=False)
        # The shutdown did not wait for the running call.
        assert not pending.done()
        with pytest.raises(RuntimeError):
            pool.submit(sum, [1, 2])
        release.set()
        pending.result(timeout=EVENT_TIMEOUT)

        assert events == ["pending finished"]

    def test_release_after_calls(self):
        # GH-1209: release() keeps the threads, and the max_workers bound,
        # until no call runs or waits, and then ends them.
        pool = S3ThreadPool(1)
        threads = []
        started, release = threading.Event(), threading.Event()

        def block():
            threads.append(threading.current_thread())
            started.set()
            release.wait(EVENT_TIMEOUT)

        first = pool.submit(block)
        assert started.wait(EVENT_TIMEOUT)
        pool.release()
        second = pool.submit(lambda: threads.append(threading.current_thread()))
        release.set()
        first.result(timeout=EVENT_TIMEOUT)
        second.result(timeout=EVENT_TIMEOUT)

        assert threads[1] is threads[0]
        threads[0].join(EVENT_TIMEOUT)
        assert not threads[0].is_alive()
        pool.shutdown()

    def test_kept_future_keeps_no_pool(self):
        # GH-1209: a future that the caller keeps after its call has ended does
        # not keep the pool, whose threads end once the pool is collected.
        pool = S3ThreadPool(1)
        threads = []
        release = threading.Event()

        def call():
            threads.append(threading.current_thread())
            release.wait(EVENT_TIMEOUT)

        # Pending while its callback is added, which the future then keeps.
        future = pool.submit(call)
        # Runs after the callback of the pool, which has then returned.
        ended = threading.Event()
        future.add_done_callback(lambda _: ended.set())
        release.set()
        assert ended.wait(EVENT_TIMEOUT)
        collected = weakref.ref(pool)
        del pool
        gc.collect()

        assert collected() is None
        threads[0].join(EVENT_TIMEOUT)
        assert not threads[0].is_alive()

    def test_release_after_interrupted_submit(self):
        # GH-1209: a call whose submit() an interrupt stopped after scheduling
        # it does not keep release() from ending the threads once it has ended.
        pool = S3ThreadPool(1)
        threads = []
        with (
            mock.patch.object(Future, "add_done_callback", side_effect=KeyboardInterrupt),
            pytest.raises(KeyboardInterrupt),
        ):
            pool.submit(lambda: threads.append(threading.current_thread()))
        # The calls start in order, so the interrupted one has ended.
        pool.submit(sum, [1, 2]).result(timeout=EVENT_TIMEOUT)
        pool.release()

        threads[0].join(EVENT_TIMEOUT)
        assert not threads[0].is_alive()
        pool.shutdown()

    @forking
    def test_fork(self):
        # A forked child, whose copy of the pool has no threads, creates new
        # ones, also if a thread of the parent held the lock of the pool.
        pool = S3ThreadPool(1)
        assert pool.submit(sum, [1, 2]).result(timeout=EVENT_TIMEOUT) == 3
        # No public call holds the lock across a fork; a thread of the parent
        # holds it while the main thread forks.
        held, release = threading.Event(), threading.Event()

        def hold_lock():
            with pool._lock:
                held.set()
                release.wait()

        holder = threading.Thread(target=hold_lock, daemon=True)
        holder.start()
        try:
            assert held.wait(EVENT_TIMEOUT)
            # A child whose pool waits for the threads of the parent would
            # not exit.
            code = run_in_forked_child(
                lambda: pool.submit(sum, [3, 4]).result(timeout=EVENT_TIMEOUT) == 7
            )
        finally:
            release.set()
            holder.join(EVENT_TIMEOUT)
            pool.shutdown()
        assert code == 0


class TestS3PoolScope:
    @pytest.mark.parametrize("busy", [False, True], ids=["entering", "waiting"])
    def test_submit_interrupted_at_each_opcode(self, busy):
        # GH-1332 (S1, S2, S5, S8, S9): an interrupt at any opcode of submit(),
        # for a call that enters the pool at once or waits for a place, leaves
        # no place taken and nothing to wait for forever: the next call runs,
        # the shutdown returns, and the interrupted call runs at most once.
        def case(interrupt):
            pool = S3ThreadPool(2)
            scope = _S3PoolScope(pool, 1)
            release = threading.Event()
            events = []
            try:
                if busy:
                    scope.submit(release.wait, EVENT_TIMEOUT)
                future = interrupt(scope.submit, events.append, "call")
                release.set()
                # A place left taken would keep the next call waiting forever.
                assert scope.submit(sum, [1, 2]).result(EVENT_TIMEOUT) == 3
                shutdown_bounded(scope)
                if future is not None:
                    assert future.done()
                if interrupt.fired and busy:
                    # A waiting call cannot have started, so it was cancelled.
                    assert events == []
                else:
                    assert events in ([], ["call"])
            finally:
                release.set()
                pool.shutdown()

        assert interrupt_each_opcode(_S3PoolScope.submit, case) > 0

    def test_submit_to_interrupted_at_each_opcode(self):
        # GH-1332 (I3): an interrupt at any opcode of submit_to() through a
        # scope leaves the caller's future cancellable by the caller, as the
        # write session does, after which nothing waits forever and the call
        # runs at most once.
        def case(interrupt):
            pool = S3ThreadPool(2)
            scope = _S3PoolScope(pool, 1)
            release = threading.Event()
            events = []
            future: Future[None] = Future()
            try:
                scope.submit(release.wait, EVENT_TIMEOUT)
                interrupt(scope.submit_to, future, events.append, "call")
                if interrupt.fired:
                    # As the write session does with its tracked request.
                    future.cancel()
                release.set()
                assert scope.submit(sum, [1, 2]).result(EVENT_TIMEOUT) == 3
                shutdown_bounded(scope)
                assert events == ([] if future.cancelled() else ["call"])
            finally:
                release.set()
                pool.shutdown()

        assert interrupt_each_opcode(S3Executor.submit_to, case) > 0

    def test_rejected_submit_interrupted_at_each_opcode(self):
        # GH-1332 (I1, S5): an interrupt at any point of a submission that the
        # pool rejects, including its cleanup, leaves nothing for the
        # shutdown of the scope to wait for forever.
        def case(interrupt):
            pool = S3ThreadPool(1)
            pool.shutdown()
            scope = _S3PoolScope(pool, 1)
            with contextlib.suppress(RuntimeError):
                interrupt(scope.submit, sum, [1, 2])
            shutdown_bounded(scope)

        assert interrupt_each_opcode(_S3PoolScope.submit, case) > 0

    @pytest.mark.skipif(sys.version_info < (3, 12), reason="Interleaving needs sys.monitoring.")
    def test_shutdown_during_hand_on_keeps_waiting_call(self):
        # GH-1332 (S7): a shutdown of the scope at any point of the hand-on
        # of a place keeps the waiting call, which then runs, also when the
        # pool takes no more calls and the ending call's thread runs it.
        def case(n):
            pool = S3ThreadPool(1)
            scope = _S3PoolScope(pool, 1)
            release = threading.Event()
            shutdowns = []
            errors = []

            def shut_down():
                # From another thread, as a close would; this thread may hold
                # the lock of the scope, so it waits only briefly.
                thread = threading.Thread(target=scope.shutdown, args=(False,), daemon=True)
                try:
                    thread.start()
                except BaseException as e:
                    errors.append(e)
                    raise
                shutdowns.append(thread)
                thread.join(0.05)

            interleave = OpcodeInterrupter(_S3PoolScope._next.__code__, n, action=shut_down)
            try:
                scope.submit(release.wait, EVENT_TIMEOUT)
                waiting = scope.submit(sum, [1, 2])
                pool.shutdown(wait=False)

                def end_first_call():
                    release.set()
                    return waiting.result(EVENT_TIMEOUT)

                assert interleave(end_first_call) == 3
                for thread in shutdowns:
                    thread.join(EVENT_TIMEOUT)
                assert errors == []
            finally:
                release.set()
                pool.shutdown()
            return interleave.fired

        n = 1
        while case(n):
            n += 1
            assert n <= 2000
        assert n > 1

    def test_shutdown_returns_once_calls_are_cancelled(self):
        # GH-1332 (S5, I1): a shutdown that waits for a call returns once the
        # call is cancelled, without waiting for the pool to reach it. A
        # wait for its notification by the pool, which an interrupt can stop,
        # used to keep the shutdown waiting.
        pool = S3ThreadPool(1)
        other = _S3PoolScope(pool, 1)
        scope = _S3PoolScope(pool, 1)
        release = threading.Event()
        try:
            # Longer than the waits below, so that the pool cannot reach the
            # cancelled call before the test ends.
            other.submit(release.wait, 3 * EVENT_TIMEOUT)
            # Accepted by the pool, behind the call of the other scope.
            future = scope.submit(sum, [1, 2])
            shut_down = threading.Event()
            thread = threading.Thread(
                target=lambda: (scope.shutdown(wait=True), shut_down.set()), daemon=True
            )
            thread.start()
            assert not shut_down.wait(0.1)
            assert future.cancel()
            assert shut_down.wait(EVENT_TIMEOUT)
            assert not release.is_set()
        finally:
            release.set()
            pool.shutdown()

    def test_shutdown_after_interrupted_cancel(self):
        # GH-1332 (S5, I1): an interrupt at any point of the cancellation of a
        # call that a shutdown waits for, including one that skips the
        # callbacks of the future, does not keep the shutdown waiting.
        def case(interrupt):
            pool = S3ThreadPool(1)
            other = _S3PoolScope(pool, 1)
            scope = _S3PoolScope(pool, 1)
            release = threading.Event()
            try:
                # Longer than the waits below, so that the pool cannot reach
                # the cancelled call before the test ends.
                other.submit(release.wait, 3 * EVENT_TIMEOUT)
                future = scope.submit(sum, [1, 2])
                waiting = threading.Event()
                add_done_callback = future.add_done_callback

                def watch(fn):
                    add_done_callback(fn)
                    waiting.set()

                future.add_done_callback = watch
                shut_down = threading.Event()
                thread = threading.Thread(
                    target=lambda: (scope.shutdown(wait=True), shut_down.set()), daemon=True
                )
                thread.start()
                assert waiting.wait(EVENT_TIMEOUT)
                interrupt(future.cancel)
                if not future.cancelled():
                    # Interrupted before it changed the state.
                    assert future.cancel()
                assert shut_down.wait(EVENT_TIMEOUT)
            finally:
                release.set()
                pool.shutdown()

        assert interrupt_each_opcode(Future.cancel, case) > 0

    def test_waiting_calls_enter_in_order(self):
        # GH-1332 (S3): the calls that wait for a place enter the pool in the
        # order they were submitted.
        pool = S3ThreadPool(2)
        scope = _S3PoolScope(pool, 1)
        release = threading.Event()
        order = []
        try:
            scope.submit(release.wait, EVENT_TIMEOUT)
            waiting = [scope.submit(order.append, name) for name in ("first", "second")]
            release.set()
            for future in waiting:
                future.result(EVENT_TIMEOUT)
            shutdown_bounded(scope)
        finally:
            release.set()
            pool.shutdown()
        assert order == ["first", "second"]

    def test_shutdown_waits_for_call_from_pool_thread(self):
        # GH-1332 (S5): the shutdown of a scope also waits for a call that a
        # thread of its pool runs itself (T1).
        pool = S3ThreadPool(2)
        scope = _S3PoolScope(pool, 1)
        started, release, ended = threading.Event(), threading.Event(), threading.Event()

        def call():
            started.set()
            release.wait(EVENT_TIMEOUT)
            ended.set()

        try:
            nested = pool.submit(lambda: scope.submit(call).result(EVENT_TIMEOUT))
            assert started.wait(EVENT_TIMEOUT)
            # Released from another thread while this one shuts the scope down.
            threading.Timer(0.1, release.set).start()
            scope.shutdown(wait=True)
            assert ended.is_set()
            nested.result(EVENT_TIMEOUT)
        finally:
            release.set()
            pool.shutdown()

    def test_cancelled_waiting_calls_do_not_accumulate(self):
        # GH-1332 (S8): calls submitted and cancelled while one long call
        # holds the only place are freed before that call ends.
        pool = S3ThreadPool(1)
        scope = _S3PoolScope(pool, 1)
        release = threading.Event()
        refs = []
        try:
            scope.submit(release.wait, EVENT_TIMEOUT)
            for _ in range(100):
                future = scope.submit(sum, [1, 2])
                assert future.cancel()
                refs.append(weakref.ref(future))
                del future
            gc.collect()
            assert sum(ref() is not None for ref in refs) < 50
        finally:
            release.set()
            pool.shutdown()

    def test_cancelled_waiting_call_frees_arguments(self):
        # GH-1332 (S8): a call cancelled while it waits leaves the queue at
        # once, so that its arguments are not kept until a place frees.
        class Body:
            pass

        pool = S3ThreadPool(1)
        scope = _S3PoolScope(pool, 1)
        release = threading.Event()
        body = Body()
        body_ref = weakref.ref(body)
        try:
            scope.submit(release.wait, EVENT_TIMEOUT)
            waiting = scope.submit(lambda b: None, body)
            del body
            assert waiting.cancel()
            gc.collect()
            assert body_ref() is None
        finally:
            release.set()
            shutdown_bounded(scope)
            pool.shutdown()

    @pytest.mark.skipif(
        threading.current_thread() is not threading.main_thread(),
        reason="SIGINT interrupts the main thread.",
    )
    def test_shutdown_interrupted_while_waiting(self):
        # GH-1332 (I1, S5): a real interrupt of a shutdown that waits for a
        # running call leaves the scope consistent: the call runs to its end,
        # and a later shutdown waits for it and returns.
        pool = S3ThreadPool(2)
        scope = _S3PoolScope(pool, 1)
        started, release = threading.Event(), threading.Event()

        def call():
            started.set()
            return release.wait(EVENT_TIMEOUT)

        try:
            running = scope.submit(call)
            assert started.wait(EVENT_TIMEOUT)
            (pending,) = scope.pending()
            # Sent once the shutdown waits for the call, which it does through
            # a done callback of the future.
            waiting = []
            add_done_callback = pending.add_done_callback

            def watch(fn):
                add_done_callback(fn)
                waiting.append(fn)

            pending.add_done_callback = watch
            with sigint_when(lambda: waiting), pytest.raises(KeyboardInterrupt):
                scope.shutdown(wait=True)
            assert not running.done()
            release.set()
            shutdown_bounded(scope)
            assert running.result(EVENT_TIMEOUT)
        finally:
            release.set()
            pool.shutdown()

    def test_submit_from_pool_thread(self):
        # GH-1332 (T1): a call submitted to a scope from a thread of its own
        # pool, as when garbage collection there closes a file, runs in that
        # thread instead of waiting for a place in the pool that it holds.
        pool = S3ThreadPool(1)
        scope = _S3PoolScope(pool, 1)
        try:
            nested = pool.submit(lambda: scope.submit(sum, [1, 2]).result(EVENT_TIMEOUT))
            assert nested.result(2 * EVENT_TIMEOUT) == 3
            shutdown_bounded(scope)
        finally:
            pool.shutdown()

    def test_waiting_call_rejected_by_pool(self):
        # GH-1332 (S5): a waiting call that the pool fails to take, such as
        # when a thread cannot start, fails instead of being lost, so that
        # the shutdown of the scope returns.
        pool = S3ThreadPool(2)
        scope = _S3PoolScope(pool, 1)
        release = threading.Event()
        submit = pool.submit

        def failing_submit(fn, *args, **kwargs):
            if threading.current_thread() is not threading.main_thread():
                raise OSError("can't start new thread")
            return submit(fn, *args, **kwargs)

        try:
            running = scope.submit(release.wait, EVENT_TIMEOUT)
            with mock.patch.object(pool, "submit", side_effect=failing_submit):
                waiting = scope.submit(sum, [1, 2])
                release.set()
                with pytest.raises(OSError, match="can't start new thread"):
                    waiting.result(EVENT_TIMEOUT)
            assert running.result(EVENT_TIMEOUT)
            shutdown_bounded(scope)
        finally:
            release.set()
            pool.shutdown()

    def test_window(self):
        # GH-1332: at most max_workers calls of a scope are in the shared
        # pool at once, as a file with its own pool of that size used to run.
        pool = S3ThreadPool(4)
        call, state = overlapping_calls(2)
        scope = _S3PoolScope(pool, 2)
        try:
            calls = [scope.submit(call) for _ in range(4)]
            wait(calls, timeout=EVENT_TIMEOUT)
        finally:
            shutdown_bounded(scope)
            pool.shutdown()
        assert all(c.done() for c in calls)
        assert state["peak"] == 2

    def test_waiting_call_context(self):
        # GH-1332: a call that waits for a place in the pool runs with the
        # context of its submitter, so that its stop scope reaches its
        # retries, although it enters the pool from the thread of the call
        # that ends before it.
        variable: contextvars.ContextVar[str] = contextvars.ContextVar("variable")
        stop = threading.Event()
        stop.set()
        operation = ThrottledOperation()
        release = threading.Event()

        def work():
            with pytest.raises(ClientError):
                retry_api_call(operation, RetryConfig(attempt=3, multiplier=0))
            return variable.get()

        pool = S3ThreadPool(2)
        scope = _S3PoolScope(pool, 1)
        try:
            blocking = scope.submit(release.wait, EVENT_TIMEOUT)
            variable.set("submitter")
            with stop_retries_on(stop):
                waiting = scope.submit(work)
            variable.set("other")
            release.set()
            assert waiting.result(EVENT_TIMEOUT) == "submitter"
            assert blocking.result(EVENT_TIMEOUT)
        finally:
            release.set()
            shutdown_bounded(scope)
            pool.shutdown()
        assert operation.calls == 1

    def test_shutdown_waits_for_own_calls(self):
        # GH-1332: shutting a scope down waits for its own calls, also those
        # that wait for a place, and not for those of another scope; the pool
        # keeps running for the other scopes.
        pool = S3ThreadPool(2)
        own_release, other_release = threading.Event(), threading.Event()
        own, other = _S3PoolScope(pool, 1), _S3PoolScope(pool, 1)
        events = []

        def slow_append(event):
            # Still running when a shutdown that does not wait returns.
            time.sleep(0.2)
            events.append(event)

        try:
            other_call = other.submit(other_release.wait, EVENT_TIMEOUT)
            own.submit(own_release.wait, EVENT_TIMEOUT)
            own.submit(slow_append, "waiting call")
            own_release.set()
            shutdown_bounded(own)
            assert events == ["waiting call"]
            assert not other_call.done()
            with pytest.raises(RuntimeError, match="after shutdown"):
                own.submit(sum, [1, 2])
            # The pool, whose other thread the other scope holds, still runs calls.
            assert pool.submit(sum, [1, 2]).result(EVENT_TIMEOUT) == 3
        finally:
            own_release.set()
            other_release.set()
            shutdown_bounded(other)
            pool.shutdown()
        assert other_call.result(EVENT_TIMEOUT)

    def test_cancel_waiting_call(self):
        # GH-1332: a call that waits for a place in the pool can be cancelled
        # through its future, and then never runs.
        pool = S3ThreadPool(2)
        scope = _S3PoolScope(pool, 1)
        release = threading.Event()
        events = []
        try:
            running = scope.submit(release.wait, EVENT_TIMEOUT)
            waiting = scope.submit(events.append, "waiting call")
            assert waiting.cancel()
            release.set()
            shutdown_bounded(scope)
        finally:
            release.set()
            pool.shutdown()
        assert running.result(EVENT_TIMEOUT)
        assert events == []

    def test_interrupted_submit(self):
        # GH-1332: an interrupt before the pool takes a call cancels the call,
        # so that the shutdown of the scope does not wait for it, and the
        # scope runs the next calls.
        pool = S3ThreadPool(1)
        scope = _S3PoolScope(pool, 1)
        try:
            with (
                mock.patch.object(pool, "submit", side_effect=KeyboardInterrupt),
                pytest.raises(KeyboardInterrupt),
            ):
                scope.submit(sum, [1, 2])
            assert scope.submit(sum, [3, 4]).result(EVENT_TIMEOUT) == 7
            shutdown_bounded(scope)
        finally:
            pool.shutdown()

    def test_interrupted_submit_after_acceptance(self):
        # GH-1332: an interrupt after the pool accepted a call that has not
        # started cancels the call, which then neither runs nor keeps its
        # place when the pool gets to it, so that the next calls still run.
        pool = S3ThreadPool(1)
        scope = _S3PoolScope(pool, 1)
        release = threading.Event()
        events = []

        class InterruptedSet(set):
            def add(self, element):
                super().add(element)
                # Once, after the pool has accepted the call.
                scope._entered = set(self)
                raise KeyboardInterrupt

        try:
            # Holds the only thread of the pool, so that the call does not start.
            holding = pool.submit(release.wait, EVENT_TIMEOUT)
            scope._entered = InterruptedSet()
            with pytest.raises(KeyboardInterrupt):
                scope.submit(events.append, "interrupted call")
            following = scope.submit(sum, [1, 2])
            release.set()
            assert holding.result(EVENT_TIMEOUT)
            assert following.result(EVENT_TIMEOUT) == 3
            shutdown_bounded(scope)
        finally:
            release.set()
            pool.shutdown()
        assert events == []

    def test_waiting_calls_run_after_pool_shutdown(self):
        # GH-1332: once the pool takes no more calls, as when the interpreter
        # exits, the calls that wait in a scope run in the thread of the call
        # that ends before them, as the calls queued in a pool of their own
        # used to drain, instead of failing.
        pool = S3ThreadPool(1)
        scope = _S3PoolScope(pool, 1)
        release = threading.Event()
        try:
            running = scope.submit(release.wait, EVENT_TIMEOUT)
            waiting = scope.submit(sum, [1, 2])
            pool.shutdown(wait=False)
            release.set()
            assert running.result(EVENT_TIMEOUT)
            assert waiting.result(EVENT_TIMEOUT) == 3
        finally:
            release.set()
            shutdown_bounded(scope)

    def test_release_waits_for_waiting_calls(self):
        # GH-1332: a release of the pool waits for the calls that wait in a
        # scope, so that it ends the threads that run them. A waiting call
        # used to enter the pool after its release had ended the threads,
        # and to start threads that nothing released.
        pool = S3ThreadPool(1)
        scope = _S3PoolScope(pool, 1)
        release = threading.Event()
        threads = []

        def record():
            threads.append(threading.current_thread())
            return release.wait(EVENT_TIMEOUT)

        try:
            calls = [scope.submit(record) for _ in range(2)]
            pool.release()
            release.set()
            assert all(c.result(EVENT_TIMEOUT) for c in calls)
            for thread in threads:
                thread.join(EVENT_TIMEOUT)
                assert not thread.is_alive()
        finally:
            release.set()
            shutdown_bounded(scope)
            pool.shutdown()


class TestS3AioExecutor:
    def test_loop_ignored(self):
        # GH-1332: the loop argument is ignored with a warning, and the calls
        # run in the pool without a running loop. They used to be dispatched
        # through the loop, which had to run.
        loop = asyncio.new_event_loop()
        try:
            with pytest.warns(DeprecationWarning, match="loop argument of S3AioExecutor"):
                executor = S3AioExecutor(loop=loop, max_workers=1)
            with executor:
                assert executor.submit(sum, [1, 2]).result(timeout=EVENT_TIMEOUT) == 3
        finally:
            loop.close()


class _DeferringExecutor(S3Executor):
    """Holds the submitted calls, which a test drops or fails before they run."""

    def __init__(self) -> None:
        self.submitted: list[Future[Any]] = []

    @override
    def submit(self, fn, *args, **kwargs):
        future: Future[Any] = Future()
        self.submitted.append(future)
        return future

    @override
    def shutdown(self, wait=True):
        pass


class TestS3Executor:
    def test_submit_context(self):
        # GH-1203: a callable runs with a copy of the context of its submitter,
        # so that the stop scope of the submitter reaches its retries.
        variable: contextvars.ContextVar[str] = contextvars.ContextVar("variable")
        stop = threading.Event()
        stop.set()
        operation = ThrottledOperation()

        def work():
            with pytest.raises(ClientError):
                retry_api_call(operation, RetryConfig(attempt=3, multiplier=0))
            return variable.get()

        variable.set("submitter")
        with S3ThreadPool(max_workers=1) as executor, stop_retries_on(stop):
            future: Future[str] = Future()
            executor.submit_to(future, work)
            assert future.result(EVENT_TIMEOUT) == "submitter"
        assert operation.calls == 1

    def test_submit_to(self):
        # The callable's outcome resolves the future of the caller.
        with S3ThreadPool(max_workers=1) as executor:
            succeeded: Future[int] = Future()
            failed: Future[int] = Future()
            executor.submit_to(succeeded, sum, [1, 2])
            executor.submit_to(failed, int, "x")
            wait([succeeded, failed], timeout=5)

        assert succeeded.result() == 3
        with pytest.raises(ValueError, match="invalid literal"):
            failed.result()

    def test_submit_to_failure_frees_arguments(self):
        # GH-1186: the arguments of a failed callable, such as the body of a
        # part, are freed when it fails, while the error keeps the frames of
        # its traceback. The traceback used to keep them, in a reference
        # cycle with the future until the garbage collector ran.
        class Body:
            pass

        def upload(body):
            raise PermissionError("Access Denied")

        body = Body()
        body_ref = weakref.ref(body)
        future: Future[None] = Future()
        gc.disable()
        try:
            with S3ThreadPool(max_workers=1) as executor:
                executor.submit_to(future, upload, body)
                del body
            error = future.exception()
            assert isinstance(error, PermissionError)
            assert traceback.extract_tb(error.__traceback__)[-1].name == "upload"
            assert body_ref() is None
        finally:
            gc.enable()

    def test_submit_to_cancelled_before_start(self):
        # Cancelling the future keeps a callable that has not started from running.
        events = []
        started = threading.Event()
        release = threading.Event()

        def work():
            started.set()
            release.wait(5)

        with S3ThreadPool(max_workers=1) as executor:
            executor.submit(work)
            pending: Future[None] = Future()
            executor.submit_to(pending, events.append, "pending finished")
            started.wait(5)
            assert pending.cancel()
            release.set()

        assert events == []
        assert pending.cancelled()

    def test_submit_to_dropped(self):
        # A callable that the executor drops before it starts is never run,
        # and the future of the caller is cancelled and settled, so that
        # wait() returns, instead of left pending.
        events = []
        executor = _DeferringExecutor()
        pending: Future[None] = Future()
        executor.submit_to(pending, events.append, "pending finished")
        (submitted,) = executor.submitted
        assert submitted.cancel()

        _, not_done = wait([pending], timeout=EVENT_TIMEOUT)
        assert not not_done
        assert pending.cancelled()
        assert events == []

    def test_submit_to_executor_failure(self):
        # An error that the executor reports before the callable starts is
        # set on the future of the caller.
        executor = _DeferringExecutor()
        future: Future[int] = Future()
        executor.submit_to(future, sum, [1, 2])
        (submitted,) = executor.submitted
        submitted.set_running_or_notify_cancel()
        submitted.set_exception(RuntimeError("cannot schedule new futures after shutdown"))

        with pytest.raises(RuntimeError, match="after shutdown"):
            future.result(timeout=EVENT_TIMEOUT)

    def test_submit_to_cancelled_executor_failure(self):
        # A future that the caller cancels before the executor reports the
        # error is marked as notified, so that wait() counts it as done.
        executor = _DeferringExecutor()
        future: Future[int] = Future()
        executor.submit_to(future, sum, [1, 2])
        (submitted,) = executor.submitted
        # Started, so that the cancellation of the future does not reach it.
        submitted.set_running_or_notify_cancel()
        assert future.cancel()
        submitted.set_exception(RuntimeError("cannot schedule new futures after shutdown"))

        _, not_done = wait([future], timeout=EVENT_TIMEOUT)
        assert not not_done
        assert future.cancelled()

    def test_submit_to_cancel_frees_waiting_call(self):
        # GH-1332 (S8): cancelling the future of a call that submit_to() put
        # in a scope's queue drops the call at once, so that its arguments are
        # not kept until a place frees.
        class Body:
            pass

        pool = S3ThreadPool(1)
        scope = _S3PoolScope(pool, 1)
        release = threading.Event()
        body = Body()
        body_ref = weakref.ref(body)
        try:
            scope.submit(release.wait, EVENT_TIMEOUT)
            future: Future[None] = Future()
            scope.submit_to(future, lambda b: None, body)
            del body
            assert future.cancel()
            gc.collect()
            assert body_ref() is None
        finally:
            release.set()
            shutdown_bounded(scope)
            pool.shutdown()

    def test_map(self):
        # GH-1332 (B1): the outcomes are in the order of the items, at most
        # the executor's limit at once.
        call, state = overlapping_calls(2)
        pool = S3ThreadPool(4)
        scope = _S3PoolScope(pool, 2)
        try:

            def square(i):
                call()
                return i * i

            result = call_bounded(scope.map, square, range(6))
            assert result.outcomes == [0, 1, 4, 9, 16, 25]
            assert result.failure is None
            assert state["peak"] == 2
        finally:
            shutdown_bounded(scope)
            pool.shutdown()

    def test_map_executor_without_limit(self):
        # GH-1332: an executor that has only submit() and shutdown(), as one
        # given to S3File(executor=), maps its items.
        class Executor(S3Executor):
            def __init__(self):
                self._pool = ThreadPoolExecutor(2)

            @override
            def submit(self, fn, *args, **kwargs):
                return self._pool.submit(contextvars.copy_context().run, fn, *args, **kwargs)

            @override
            def shutdown(self, wait=True):
                self._pool.shutdown(wait=wait)

        with Executor() as executor:
            assert call_bounded(executor.map, str, [1, 2]).outcomes == ["1", "2"]

    @pytest.mark.parametrize("rejection", ["failed", "cancelled"])
    def test_map_executor_rejection(self, rejection):
        # GH-1332: a call that the executor fails or cancels without running
        # it counts as a failure when its future is set: it stops the batch
        # before the next call starts, or is the failure of a batch without
        # stop_on_failure, instead of being returned as a value.
        class Rejecting(S3Executor):
            # Runs the other calls in the submitting thread, before submit()
            # returns, so that the call after the rejection starts before the
            # map waits.
            def __init__(self):
                self.calls = 0

            @override
            def submit(self, fn, *args, **kwargs):
                self.calls += 1
                future: Future[Any] = Future()
                if self.calls != 2:
                    future.set_running_or_notify_cancel()
                    future.set_result(contextvars.copy_context().run(fn, *args, **kwargs))
                    return future
                if rejection == "cancelled":
                    future.cancel()
                else:
                    future.set_running_or_notify_cancel()
                    future.set_exception(RuntimeError("cannot start a thread"))
                return future

            @override
            def shutdown(self, wait=True):
                pass

        expected = CancelledError if rejection == "cancelled" else RuntimeError
        ran = []

        def call(i):
            ran.append(i)
            return str(i)

        with Rejecting() as executor, pytest.raises(expected):
            call_bounded(executor.map, call, range(3))
        # The item submitted after the rejection does not run.
        assert 2 not in ran
        with Rejecting() as executor:
            result = call_bounded(executor.map, str, range(3), stop_on_failure=False)
        assert isinstance(result.failure, expected)
        assert isinstance(result.outcomes[1], expected)

    def test_map_failure_cancels_queued_calls(self):
        # GH-1332 (B2): a failure cancels the calls that have not started
        # without waiting for them to start, also when the executor would
        # never start them.
        class Holding(S3Executor):
            # Runs the first call, and holds the others until they are
            # cancelled.
            def __init__(self):
                self._pool = ThreadPoolExecutor(1)
                self.held: list[Future[Any]] = []
                self.calls = 0

            @override
            def submit(self, fn, *args, **kwargs):
                self.calls += 1
                if self.calls > 1:
                    self.held.append(Future())
                    return self.held[-1]
                return self._pool.submit(contextvars.copy_context().run, fn, *args, **kwargs)

            @override
            def shutdown(self, wait=True):
                self._pool.shutdown(wait=wait)

        def call(i):
            raise ValueError(i)

        with Holding() as executor:
            with pytest.raises(ValueError, match=r"^0$"):
                call_bounded(executor.map, call, range(2))
            assert all(future.cancelled() for future in executor.held)

    def test_map_deferred_cancellation_stops_batch(self):
        # GH-1332 (B2): a call that the executor cancels after the map began
        # waiting stops the batch, also while another call is held until it
        # is cancelled; a cancellation does not wake a wait for the first
        # exception.
        class Holding(S3Executor):
            def __init__(self):
                self.held: list[Future[Any]] = []

            @override
            def submit(self, fn, *args, **kwargs):
                self.held.append(Future())
                return self.held[-1]

            @override
            def shutdown(self, wait=True):
                pass

        executor = Holding()
        waiting = threading.Event()

        def entered_wait(futures, **kwargs):
            waiting.set()
            return wait(futures, **kwargs)

        def cancel():
            # Once the map waits.
            assert waiting.wait(EVENT_TIMEOUT)
            executor.held[0].cancel()

        threading.Thread(target=cancel, daemon=True).start()
        with (
            mock.patch("pyathena.filesystem.s3_executor.wait", entered_wait),
            pytest.raises(CancelledError),
        ):
            call_bounded(executor.map, str, range(2))
        assert executor.held[1].cancelled()

    def test_map_interrupted_while_ending_wait(self):
        # GH-1332 (I1): an interrupt at any opcode of the signal that ends the
        # wait, in a callback that records a failure, does not keep the map
        # waiting forever, also while another call is held until it is
        # cancelled.
        class Holding(S3Executor):
            # Rejects the first call in the submitting thread, and holds the
            # others until they are cancelled.
            def __init__(self):
                self.held: list[Future[Any]] = []

            @override
            def submit(self, fn, *args, **kwargs):
                future: Future[Any] = Future()
                if not self.held:
                    future.set_running_or_notify_cancel()
                    future.set_exception(RuntimeError("cannot start a thread"))
                self.held.append(future)
                return future

            @override
            def shutdown(self, wait=True):
                pass

        def case(interrupt):
            # The map raises the rejection, or the interrupt, which the
            # callback records as the failure or which propagates from it.
            with contextlib.suppress(RuntimeError):
                interrupt(Holding().map, str, range(2))

        assert call_bounded(interrupt_each_opcode, _S3Batch._end, case, timeout=120) > 0

    def test_map_records_failure_before_callbacks_run(self):
        # GH-1332: a future wakes its waiters before it runs its callbacks, so
        # the map records a failure that it sees, also before the callback
        # that would record it has run.
        release = threading.Event()

        class LateCallbacks(Future):
            def _invoke_callbacks(self):
                def invoke():
                    release.wait(EVENT_TIMEOUT)
                    Future._invoke_callbacks(self)

                threading.Thread(target=invoke, daemon=True).start()

        class Rejecting(S3Executor):
            @override
            def submit(self, fn, *args, **kwargs):
                future: Future[Any] = Future()
                future.set_running_or_notify_cancel()
                future.set_exception(RuntimeError("cannot start a thread"))
                return future

            @override
            def shutdown(self, wait=True):
                pass

        try:
            with (
                mock.patch("pyathena.filesystem.s3_executor.Future", LateCallbacks),
                pytest.raises(RuntimeError, match="cannot start"),
            ):
                call_bounded(Rejecting().map, str, range(1))
        finally:
            release.set()

    def test_map_interrupted_in_counts(self):
        # GH-1332 (B3): an interrupt of the waiting thread while it evaluates
        # the counts predicate is raised, also after a failure was recorded.
        pool = S3ThreadPool(1)
        scope = _S3PoolScope(pool, 1)
        waiting = []

        def counts(error):
            if threading.get_ident() in waiting:
                raise KeyboardInterrupt
            return True

        def call(i):
            raise ValueError(i)

        def run():
            waiting.append(threading.get_ident())
            return scope.map(call, range(1), stop_on_failure=False, counts=counts)

        try:
            with pytest.raises(KeyboardInterrupt):
                call_bounded(run)
        finally:
            shutdown_bounded(scope)
            pool.shutdown()

    @pytest.mark.asyncio
    async def test_map_async_deferred_cancellation_stops_batch(self):
        # GH-1332 (B2): as test_map_deferred_cancellation_stops_batch, for
        # map_async().
        class Holding(S3Executor):
            def __init__(self):
                self.held: list[Future[Any]] = []

            @override
            def submit(self, fn, *args, **kwargs):
                self.held.append(Future())
                return self.held[-1]

            @override
            def shutdown(self, wait=True):
                pass

        executor = Holding()
        waiting = threading.Event()
        waiter = S3Executor.waiter

        def watched_waiter(future):
            result = waiter(future)
            waiting.set()
            return result

        def cancel():
            # Once the map has wrapped the futures that it waits for.
            assert waiting.wait(EVENT_TIMEOUT)
            executor.held[0].cancel()

        threading.Thread(target=cancel, daemon=True).start()
        # The failure of the call, not a cancellation of the waiting task.
        with (
            mock.patch.object(S3Executor, "waiter", staticmethod(watched_waiter)),
            pytest.raises(CancelledError),
        ):
            await await_bounded(executor.map_async(str, range(2)))
        assert executor.held[1].cancelled()

    def test_map_failure_stops_batch(self):
        # GH-1332 (B2): a failure starts no further call, stops the retries
        # of the running one, whose backoff wait would take at least
        # EVENT_TIMEOUT seconds, and is raised once that call has ended. The
        # call that the failure frees the place for makes no request, also
        # when its thread takes it before the waiting thread resumes.
        operation = ThrottledOperation()
        throttled = retried(operation, RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT))
        ended = []
        pool = S3ThreadPool(2)
        scope = _S3PoolScope(pool, 2)

        def call(i):
            try:
                if i == 0:
                    return throttled()
                if i == 1:
                    assert operation.started.wait(EVENT_TIMEOUT)
                    assert waiting.wait(EVENT_TIMEOUT)
                    raise ValueError("failed")
                return i
            finally:
                ended.append(i)

        waiting = threading.Event()

        def late_wait(futures, **kwargs):
            # The waiting thread resumes after the thread of the failed call
            # has taken the next one.
            waiting.set()
            done = wait(futures, **kwargs)
            time.sleep(0.2)
            return done

        try:
            started = time.monotonic()
            with (
                mock.patch("pyathena.filesystem.s3_executor.wait", late_wait),
                pytest.raises(ValueError, match="failed"),
            ):
                call_bounded(scope.map, call, range(4))
            assert time.monotonic() - started < EVENT_TIMEOUT
            assert sorted(ended) == [0, 1]
            assert operation.calls == 1
        finally:
            shutdown_bounded(scope)
            pool.shutdown()

    def test_map_raises_first_failure_to_end(self):
        # GH-1332 (B2): the failure raised is the one that stopped the batch,
        # the first to end, not the first item that failed.
        pool = S3ThreadPool(2)
        scope = _S3PoolScope(pool, 2)
        recorded = threading.Event()
        record = _S3Batch._record

        def recording(batch, error):
            record(batch, error)
            if error is not None:
                recorded.set()

        def call(i):
            if i == 0:
                # Fails once the failure of the second item is recorded.
                assert recorded.wait(EVENT_TIMEOUT)
                raise ValueError("first item")
            raise KeyError("second item")

        try:
            with (
                mock.patch.object(_S3Batch, "_record", recording),
                pytest.raises(KeyError, match="second item"),
            ):
                call_bounded(scope.map, call, range(2))
        finally:
            shutdown_bounded(scope)
            pool.shutdown()

    def test_map_counts_failures(self):
        # GH-1332 (B5): without stop_on_failure, every call runs, and the
        # failure reported is the first that counts in the order they ended.
        pool = S3ThreadPool(3)
        scope = _S3PoolScope(pool, 3)
        recorded = {str(i): threading.Event() for i in range(3)}
        record = _S3Batch._record

        def recording(batch, error):
            record(batch, error)
            if error is not None:
                recorded[str(error)].set()

        def call(i):
            # Item 0 fails first and does not count; item 1 fails before item 2.
            if i == 1:
                assert recorded["0"].wait(EVENT_TIMEOUT)
            if i == 2:
                assert recorded["1"].wait(EVENT_TIMEOUT)
            raise (LookupError if i == 0 else ValueError)(i)

        try:
            with mock.patch.object(_S3Batch, "_record", recording):
                result = call_bounded(
                    scope.map,
                    call,
                    range(3),
                    stop_on_failure=False,
                    counts=lambda e: not isinstance(e, LookupError),
                )
            assert [type(e) for e in result.outcomes] == [LookupError, ValueError, ValueError]
            assert result.failure is result.outcomes[1]
        finally:
            shutdown_bounded(scope)
            pool.shutdown()

    def test_map_interrupted_without_stop_on_failure(self):
        # GH-1332 (B3): an interrupt of the wait cancels the calls that have
        # not started and stops the retries of the running one, whose backoff
        # wait would take at least EVENT_TIMEOUT seconds, also without
        # stop_on_failure, and is raised once that call has ended.
        operation = ThrottledOperation()
        throttled = retried(operation, RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT))
        ran = []
        waits = []
        pool = S3ThreadPool(1)
        scope = _S3PoolScope(pool, 1)

        def call(i):
            ran.append(i)
            return throttled()

        def interrupted_wait(futures, **kwargs):
            waits.append(futures)
            if len(waits) == 1:
                assert operation.started.wait(EVENT_TIMEOUT)
                raise KeyboardInterrupt
            return wait(futures, **kwargs)

        try:
            started = time.monotonic()
            with (
                mock.patch("pyathena.filesystem.s3_executor.wait", interrupted_wait),
                pytest.raises(KeyboardInterrupt),
            ):
                call_bounded(scope.map, call, range(2), stop_on_failure=False)
            assert time.monotonic() - started < EVENT_TIMEOUT
            assert ran == [0]
            assert operation.calls == 1
            assert scope.pending() == []
        finally:
            waits.clear()
            shutdown_bounded(scope)
            pool.shutdown()

    def test_map_interrupted_twice(self):
        # GH-1332 (B3, I1 amended): a second interrupt stops the wait for the
        # running calls, which then end in the executor.
        release = threading.Event()
        started = threading.Event()
        pool = S3ThreadPool(1)
        scope = _S3PoolScope(pool, 1)

        def call(i):
            started.set()
            release.wait(EVENT_TIMEOUT)
            return i

        def interrupted_wait(futures, **kwargs):
            assert started.wait(EVENT_TIMEOUT)
            raise KeyboardInterrupt

        try:
            with (
                mock.patch("pyathena.filesystem.s3_executor.wait", interrupted_wait),
                pytest.raises(KeyboardInterrupt) as info,
            ):
                call_bounded(scope.map, call, range(1))
            assert isinstance(info.value.__context__, KeyboardInterrupt)
            assert not release.is_set()
            assert len(scope.pending()) == 1
        finally:
            release.set()
            shutdown_bounded(scope)
            pool.shutdown()

    def test_map_result_frees_outcomes(self):
        # GH-1332 (F5): the outcomes of a map are freed once its result is
        # dropped, without the cyclic garbage collector, so that the bodies of
        # parallel reads are not kept until it runs.
        class Body:
            pass

        refs = []
        pool = S3ThreadPool(2)
        scope = _S3PoolScope(pool, 2)

        def call(i):
            body = Body()
            refs.append(weakref.ref(body))
            return body

        gc.disable()
        try:
            result = call_bounded(scope.map, call, range(2))
            assert len(result.outcomes) == 2
            del result
            # The threads leave the frames of the calls right after they end.
            poll_until(lambda: all(ref() is None for ref in refs), "an outcome was kept")
        finally:
            gc.enable()
            shutdown_bounded(scope)
            pool.shutdown()

    def test_map_failure_frees_outcomes(self):
        # GH-1332 (F5): the outcome of a call that ends while a failed map
        # waits for it is not kept by the traceback of the failure.
        class Body:
            pass

        refs = []
        failed = threading.Event()
        both_started = threading.Barrier(2, timeout=EVENT_TIMEOUT)
        pool = S3ThreadPool(2)
        scope = _S3PoolScope(pool, 2)

        def call(i):
            both_started.wait()
            if i == 0:
                failed.set()
                raise ValueError("failed")
            assert failed.wait(EVENT_TIMEOUT)
            # Ends after the failure, while the map waits for it.
            time.sleep(0.1)
            body = Body()
            refs.append(weakref.ref(body))
            return body

        try:
            with pytest.raises(ValueError, match="failed") as info:
                call_bounded(scope.map, call, range(2))
            gc.collect()
            assert len(refs) == 1
            assert refs[0]() is None
            assert info.value.__traceback__ is not None
        finally:
            shutdown_bounded(scope)
            pool.shutdown()

    def test_map_interrupted_at_each_opcode(self):
        # GH-1332 (B6, S1, S5, F5): an interrupt at any opcode of map() leaves
        # no call in the scope, every call that it started has ended when it
        # raises, and the outcomes are freed while the caller holds the
        # interrupt.
        class Body:
            pass

        def case(interrupt):
            pool = S3ThreadPool(2)
            scope = _S3PoolScope(pool, 2)
            running = []
            refs = []
            lock = threading.Lock()

            def call(i):
                with lock:
                    running.append(i)
                time.sleep(0.01)
                body = Body()
                with lock:
                    refs.append(weakref.ref(body))
                    running.remove(i)
                return body

            try:
                result = interrupt(scope.map, call, range(4))
                if not interrupt.fired:
                    assert len(result.outcomes) == 4
                    return
                assert interrupt.error.__traceback__ is not None
                assert running == []
                assert scope.pending() == []
                # The threads leave the frames of the calls right after they end.
                poll_until(lambda: all(ref() is None for ref in refs), "an outcome was kept")
            finally:
                shutdown_bounded(scope)
                pool.shutdown()

        # The tracer is installed in the thread that runs the case.
        assert call_bounded(interrupt_each_opcode, S3Executor.map, case, timeout=120) > 0

    @pytest.mark.asyncio
    async def test_map_async_cancelled(self):
        # GH-1332 (B3, B6): a cancellation of the waiting task cancels the
        # calls that have not started, stops the retries of the running one,
        # and is raised once that call has ended.
        operation = ThrottledOperation()
        throttled = retried(operation, RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT))
        ran = []
        ended = []
        pool = S3ThreadPool(1)
        scope = _S3PoolScope(pool, 1)

        def call(i):
            ran.append(i)
            try:
                return throttled()
            finally:
                ended.append(i)

        try:
            task = asyncio.ensure_future(scope.map_async(call, range(2)))
            assert await asyncio.to_thread(operation.started.wait, EVENT_TIMEOUT)
            started = time.monotonic()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, EVENT_TIMEOUT)
            assert time.monotonic() - started < EVENT_TIMEOUT
            assert ran == ended == [0]
            assert operation.calls == 1
        finally:
            await asyncio.to_thread(shutdown_bounded, scope)
            pool.shutdown()

    @pytest.mark.asyncio
    async def test_map_async_counts_error_in_callback(self):
        # GH-1332: an error of the counts predicate in the done callback that
        # records a failure the executor reports later, in another thread, is
        # the failure of the map, and does not escape the callback into that
        # thread, which would skip the callbacks after it.
        # Rejects once the map has wrapped the future of the call for asyncio.
        wrapped = threading.Event()
        rejected = threading.Event()
        escaped = []
        waiter = S3Executor.waiter

        def watched_waiter(future):
            result = waiter(future)
            wrapped.set()
            return result

        class Deferring(S3Executor):
            @override
            def submit(self, fn, *args, **kwargs):
                future: Future[Any] = Future()

                def reject():
                    assert wrapped.wait(EVENT_TIMEOUT)
                    future.set_running_or_notify_cancel()
                    try:
                        future.set_exception(RuntimeError("rejected"))
                    except BaseException as e:
                        escaped.append(e)
                    finally:
                        rejected.set()

                threading.Thread(target=reject, daemon=True).start()
                return future

            @override
            def shutdown(self, wait=True):
                pass

        class Abort(BaseException):
            # Not an Exception, which concurrent.futures would only log; not
            # SystemExit, which asyncio raises out of the event loop.
            pass

        def counts(error):
            raise Abort("predicate")

        with (
            mock.patch.object(S3Executor, "waiter", staticmethod(watched_waiter)),
            pytest.raises(Abort, match="predicate"),
        ):
            await await_bounded(Deferring().map_async(str, range(1), counts=counts))
        # Not raised in the thread that set the future either, once its
        # callbacks have returned.
        assert await asyncio.to_thread(rejected.wait, EVENT_TIMEOUT)
        assert escaped == []

    @pytest.mark.asyncio
    async def test_map_async_failures_are_retrieved(self):
        # GH-1332: the failures of the calls that a failure stopped, which end
        # while the map drains, are not logged as never retrieved.
        loop = asyncio.get_running_loop()
        logged = []
        previous = loop.get_exception_handler()
        loop.set_exception_handler(lambda loop, context: logged.append(context))
        pool = S3ThreadPool(2)
        scope = _S3PoolScope(pool, 2)
        both_started = threading.Barrier(2, timeout=EVENT_TIMEOUT)
        # The second call fails once the first has stopped its retries.
        throttled = retried(ThrottledOperation(), RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT))

        def call(i):
            both_started.wait()
            if i == 0:
                raise ValueError(i)
            return throttled()

        try:
            with pytest.raises(ValueError, match=r"^0$"):
                await scope.map_async(call, range(2))
            for _ in range(3):
                gc.collect()
                await asyncio.sleep(0)
            assert logged == []
        finally:
            loop.set_exception_handler(previous)
            await asyncio.to_thread(shutdown_bounded, scope)
            pool.shutdown()

    def test_map_async_at_loop_shutdown(self):
        # GH-1332 (B6): the end of asyncio.run() cancels a waiting map, which
        # waits for its running call before the loop closes.
        release = threading.Event()
        started = threading.Event()
        ended = []
        pool = S3ThreadPool(1)
        scope = _S3PoolScope(pool, 1)

        def call(i):
            started.set()
            release.wait(0.5)
            ended.append(i)

        tasks = []

        async def main():
            tasks.append(asyncio.ensure_future(scope.map_async(call, range(1))))
            await asyncio.to_thread(started.wait, EVENT_TIMEOUT)

        try:
            asyncio.run(main())
            assert ended == [0]
        finally:
            release.set()
            shutdown_bounded(scope)
            pool.shutdown()
