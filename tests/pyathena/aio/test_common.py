# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock, MagicMock

import pytest

from pyathena.aio.arrow.cursor import AioArrowCursor
from pyathena.aio.cursor import AioCursor, AioDictCursor
from pyathena.aio.pandas.cursor import AioPandasCursor
from pyathena.aio.polars.cursor import AioPolarsCursor
from pyathena.aio.s3fs.cursor import AioS3FSCursor
from pyathena.error import OperationalError, ProgrammingError
from pyathena.util import RetryConfig
from tests.pyathena.util import EVENT_TIMEOUT, ThrottledOperation, retried

_ALL_CURSOR_CLASSES = [
    AioCursor,
    AioDictCursor,
    AioArrowCursor,
    AioPandasCursor,
    AioPolarsCursor,
    AioS3FSCursor,
]

_WHOLE_RESULT_METHODS = {
    AioArrowCursor: ("as_arrow", "as_polars"),
    AioPolarsCursor: ("as_polars", "as_arrow"),
}


def _result_set_closing_after_loop_responds() -> tuple[MagicMock, list[bool]]:
    """Return a result set whose ``close()`` waits for a callback on the running loop.

    Each close schedules a callback on the event loop, waits for it, and records
    whether it ran. A close on the event loop's thread blocks the loop, so the
    callback cannot run and the record is False.

    Returns:
        The mock result set, which is open until closed, and the list of records.
    """
    loop = asyncio.get_running_loop()
    closes: list[bool] = []

    def close():
        responded = threading.Event()
        loop.call_soon_threadsafe(responded.set)
        closes.append(responded.wait(EVENT_TIMEOUT))

    result_set = MagicMock(is_closed=False)
    result_set.close.side_effect = close
    return result_set, closes


class TestWithAsyncFetch:
    @pytest.mark.parametrize("cursor_class", _ALL_CURSOR_CLASSES)
    def test_sync_iteration_raises(self, cursor_class):
        cursor = cursor_class(
            connection=MagicMock(), converter=None, formatter=None, retry_config=None
        )
        with pytest.raises(
            TypeError, match=rf"'{cursor_class.__name__}' object is not iterable; use 'async for'"
        ):
            iter(cursor)
        # GH-1207: next() called directly returned an un-awaited fetchone() coroutine.
        with pytest.raises(TypeError):
            next(cursor)

    @pytest.mark.parametrize(
        "cursor_class",
        [AioArrowCursor, AioPandasCursor, AioPolarsCursor, AioS3FSCursor],
    )
    async def test_fetch_runs_result_set_fetch(self, cursor_class):
        cursor = cursor_class(
            connection=MagicMock(), converter=None, formatter=None, retry_config=None
        )
        threads = []

        def fetch(rows):
            def record(*args):
                threads.append(threading.get_ident())
                return rows

            return record

        cursor.result_set = MagicMock()
        cursor.result_set.fetchone.side_effect = fetch((1,))
        cursor.result_set.fetchmany.side_effect = fetch([(2,), (3,)])
        cursor.result_set.fetchall.side_effect = fetch([(4,)])

        assert await cursor.fetchone() == (1,)
        assert await cursor.fetchmany(2) == [(2,), (3,)]
        assert await cursor.fetchall() == [(4,)]
        cursor.result_set.fetchmany.assert_called_once_with(2)
        # The synchronous fetches run off the event loop's thread.
        assert len(threads) == 3
        assert threading.get_ident() not in threads

    @pytest.mark.parametrize("cursor_class", _ALL_CURSOR_CLASSES)
    async def test_fetch_without_result_set(self, cursor_class):
        cursor = cursor_class(
            connection=MagicMock(), converter=None, formatter=None, retry_config=None
        )
        for fetch in (cursor.fetchone, cursor.fetchmany, cursor.fetchall):
            with pytest.raises(ProgrammingError, match=r"No result set\."):
                await fetch()
        # The Arrow and Polars cursors also convert the whole result.
        for name in _WHOLE_RESULT_METHODS.get(cursor_class, ()):
            with pytest.raises(ProgrammingError):
                getattr(cursor, name)()

    @pytest.mark.parametrize("cursor_class", _ALL_CURSOR_CLASSES)
    async def test_cancel_without_query(self, cursor_class):
        cursor = cursor_class(
            connection=MagicMock(), converter=None, formatter=None, retry_config=None
        )
        with pytest.raises(ProgrammingError):
            await cursor.cancel()

    @pytest.mark.parametrize(
        ("method", "args"), [("fetchone", ()), ("fetchmany", (2,)), ("fetchall", ())]
    )
    @pytest.mark.parametrize(
        "cursor_class",
        [AioArrowCursor, AioPandasCursor, AioPolarsCursor, AioS3FSCursor],
    )
    async def test_cancelled_fetch_detaches_result_set(self, cursor_class, method, args):
        """GH-1206: a cancelled fetch stops its retries and detaches the result set.

        The result set is closed in a thread once the fetch has ended in its thread,
        and later fetches raise ProgrammingError. Without the stop, the retries would
        wait at least EVENT_TIMEOUT seconds before the next attempt.
        """
        cursor = cursor_class(
            connection=MagicMock(), converter=None, formatter=None, retry_config=None
        )
        operation = ThrottledOperation(block=True)
        closes = []
        closed = threading.Event()

        def close():
            closes.append(threading.get_ident())
            closed.set()

        result_set = MagicMock(is_closed=False)
        result_set.close.side_effect = close
        getattr(result_set, method).side_effect = retried(
            operation, RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT)
        )
        cursor.result_set = result_set
        task = asyncio.create_task(getattr(cursor, method)(*args))
        try:
            assert await asyncio.to_thread(operation.started.wait, EVENT_TIMEOUT)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert cursor.result_set is None
            for fetch in (cursor.fetchone, cursor.fetchmany, cursor.fetchall):
                with pytest.raises(ProgrammingError, match=r"No result set\."):
                    await fetch()
            assert not closes
        finally:
            operation.release.set()
        assert await asyncio.to_thread(closed.wait, EVENT_TIMEOUT)
        assert operation.calls == 1
        assert closes != [threading.get_ident()]

    @pytest.mark.parametrize("cursor_class", _ALL_CURSOR_CLASSES)
    async def test_async_iteration(self, cursor_class):
        cursor = cursor_class(
            connection=MagicMock(), converter=None, formatter=None, retry_config=None
        )
        cursor.result_set = MagicMock()
        # AioCursor's result set fetches asynchronously; the others run it in a thread.
        fetchone = AsyncMock if cursor_class in (AioCursor, AioDictCursor) else MagicMock
        cursor.result_set.fetchone = fetchone(side_effect=[(1,), (2,), None])

        rows = []
        async for row in cursor:
            rows.append(row)
            # Stop rather than loop forever if iteration never ends.
            if len(rows) > 2:
                break
        assert rows == [(1,), (2,)]

    @pytest.mark.parametrize("cursor_class", _ALL_CURSOR_CLASSES)
    async def test_context_exit_closes_off_event_loop(self, cursor_class):
        result_set, closes = _result_set_closing_after_loop_responds()
        async with cursor_class(
            connection=MagicMock(), converter=None, formatter=None, retry_config=None
        ) as cursor:
            cursor.result_set = result_set

        # The event loop ran a callback while the close waited.
        assert closes == [True]

    @pytest.mark.parametrize("cursor_class", _ALL_CURSOR_CLASSES)
    async def test_execute_closes_previous_result_set_off_event_loop(self, cursor_class):
        cursor = cursor_class(
            connection=MagicMock(), converter=None, formatter=None, retry_config=None
        )
        cursor.result_set, closes = _result_set_closing_after_loop_responds()
        cursor._execute = AsyncMock(side_effect=OperationalError("stop"))

        with pytest.raises(OperationalError, match="stop"):
            await cursor.execute("SELECT 1")

        assert closes == [True]
        assert cursor.result_set is None

    @pytest.mark.parametrize("cursor_class", _ALL_CURSOR_CLASSES)
    async def test_execute_cancelled_during_close_leaves_cursor_reset(self, cursor_class):
        cursor = cursor_class(
            connection=MagicMock(), converter=None, formatter=None, retry_config=None
        )
        closing = threading.Event()
        released = threading.Event()
        result_set = MagicMock(is_closed=False)
        result_set.close.side_effect = lambda: (closing.set(), released.wait(EVENT_TIMEOUT))
        cursor.result_set = result_set
        cursor.query_id = "previous"
        cursor._execute = AsyncMock(side_effect=OperationalError("stop"))

        task = asyncio.create_task(cursor.execute("SELECT 1"))
        assert await asyncio.to_thread(closing.wait, EVENT_TIMEOUT)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # The cursor no longer refers to the result set that is still closing.
        assert cursor.result_set is None
        assert cursor.query_id is None
        assert cursor.rowcount == -1
        released.set()
        cursor._execute.assert_not_called()

    @pytest.mark.parametrize("cursor_class", _ALL_CURSOR_CLASSES)
    async def test_execute_cancelled_before_close_starts_still_closes(self, cursor_class):
        loop = asyncio.get_running_loop()
        # One busy worker keeps the close queued until the caller is cancelled.
        executor = ThreadPoolExecutor(max_workers=1)
        loop.set_default_executor(executor)
        busy = threading.Event()
        try:
            blocker = loop.run_in_executor(None, busy.wait, EVENT_TIMEOUT)
            cursor = cursor_class(
                connection=MagicMock(), converter=None, formatter=None, retry_config=None
            )
            closed = threading.Event()
            result_set = MagicMock(is_closed=False)
            result_set.close.side_effect = closed.set
            cursor.result_set = result_set
            cursor._execute = AsyncMock(side_effect=OperationalError("stop"))

            task = asyncio.create_task(cursor.execute("SELECT 1"))
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            busy.set()
            await blocker

            # The worker runs the queued close before this wait.
            assert await loop.run_in_executor(None, closed.wait, EVENT_TIMEOUT)
            assert cursor.result_set is None
        finally:
            busy.set()
            executor.shutdown(wait=True)

    @pytest.mark.parametrize("cursor_class", _ALL_CURSOR_CLASSES)
    async def test_executemany_closes_previous_result_set_off_event_loop(self, cursor_class):
        cursor = cursor_class(
            connection=MagicMock(), converter=None, formatter=None, retry_config=None
        )
        cursor.result_set, closes = _result_set_closing_after_loop_responds()

        await cursor.executemany("SELECT 1", [])

        assert closes == [True]
        assert cursor.result_set is None
        assert cursor.rowcount == 0

    @pytest.mark.parametrize("cursor_class", _ALL_CURSOR_CLASSES)
    async def test_executemany_unknown_rowcount(self, cursor_class):
        """A statement without an update count, such as a SELECT, makes the sum unknown.

        No result set is left to fetch.
        """
        cursor = cursor_class(
            connection=MagicMock(), converter=None, formatter=None, retry_config=None
        )
        rowcounts = iter([1, -1, 1])

        async def execute(operation, parameters, **kwargs):
            cursor.result_set = MagicMock(is_closed=True, rowcount=next(rowcounts))

        cursor.execute = execute
        await cursor.executemany("SELECT 1", [None, None, None])

        assert cursor.rowcount == -1
        assert cursor.result_set is None

    @pytest.mark.parametrize("cursor_class", _ALL_CURSOR_CLASSES)
    async def test_executemany_failure_closes_result_set_off_event_loop(self, cursor_class):
        cursor = cursor_class(
            connection=MagicMock(), converter=None, formatter=None, retry_config=None
        )
        result_set, closes = _result_set_closing_after_loop_responds()
        result_set.rowcount = 1

        async def execute(operation, parameters, **kwargs):
            cursor.query_id = "query_id"
            cursor.result_set = result_set

        def parameters():
            yield None
            raise ValueError("bad parameters")

        cursor.execute = execute
        with pytest.raises(ValueError, match="bad parameters"):
            await cursor.executemany("SELECT 1", parameters())

        assert closes == [True]
        assert cursor.result_set is None
        assert cursor.rowcount == -1
        # The query ID of the last execution is kept.
        assert cursor.query_id == "query_id"
