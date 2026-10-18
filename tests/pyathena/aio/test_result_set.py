# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
import asyncio
import threading
from unittest.mock import MagicMock

import pytest

from pyathena.aio.result_set import AthenaAioDictResultSet, AthenaAioResultSet
from pyathena.converter import DefaultTypeConverter
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.util import RetryConfig
from tests.pyathena.util import EVENT_TIMEOUT, get_query_results_responses


async def _create_result_set(result_set_class, pages=(("1", "2"),)):
    """Create a result set of a succeeded query from stubbed ``GetQueryResults`` pages.

    Args:
        result_set_class: The asyncio result set class to create.
        pages: The values of an integer column ``a``, one tuple per page, or an
            exception that the request for the page raises. The first page is fetched
            at creation, the others on demand.

    Returns:
        The result set.
    """
    connection = MagicMock()
    connection.client.get_query_results.side_effect = get_query_results_responses(*pages)
    query_execution = AthenaQueryExecution(
        {
            "QueryExecution": {
                "QueryExecutionId": "test_query_id",
                "Query": "SELECT a",
                "Status": {"State": AthenaQueryExecution.STATE_SUCCEEDED},
            }
        }
    )
    return await result_set_class.create(
        connection, DefaultTypeConverter(), query_execution, 2, RetryConfig()
    )


async def _assert_fetches_fail(result_set):
    """Assert that each fetch of a result set raises for an earlier failed fetch.

    Args:
        result_set: The asyncio result set.
    """
    for fetch in (result_set.fetchone, result_set.fetchmany, result_set.fetchall):
        with pytest.raises(ProgrammingError, match=r"cannot be fetched after a failed fetch\."):
            await fetch()


class TestAthenaAioResultSet:
    @pytest.mark.parametrize("result_set_class", [AthenaAioResultSet, AthenaAioDictResultSet])
    async def test_sync_iteration_raises(self, result_set_class):
        result_set = await _create_result_set(result_set_class)
        with pytest.raises(
            TypeError,
            match=rf"'{result_set_class.__name__}' object is not iterable; use 'async for'",
        ):
            iter(result_set)
        # GH-1207: next() called directly returned an un-awaited fetchone() coroutine.
        with pytest.raises(TypeError):
            next(result_set)

    @pytest.mark.parametrize(
        ("result_set_class", "expected"),
        [(AthenaAioResultSet, [(1,), (2,)]), (AthenaAioDictResultSet, [{"a": 1}, {"a": 2}])],
    )
    async def test_async_iteration(self, result_set_class, expected):
        result_set = await _create_result_set(result_set_class)
        assert [row async for row in result_set] == expected

    @pytest.mark.parametrize(
        ("result_set_class", "expected"),
        [
            (AthenaAioResultSet, [[(1,)], [(2,), (3,), (4,)], [(5,)]]),
            (AthenaAioDictResultSet, [[{"a": 1}], [{"a": 2}, {"a": 3}, {"a": 4}], [{"a": 5}]]),
        ],
    )
    async def test_fetchmany_across_pages(self, result_set_class, expected):
        result_set = await _create_result_set(result_set_class, [("1", "2"), ("3", "4"), ("5",)])
        assert await result_set.fetchmany(1) == expected[0]
        assert result_set.rownumber == 1
        assert await result_set.fetchmany(3) == expected[1]
        assert result_set.rownumber == 4
        assert await result_set.fetchmany() == expected[2]
        assert await result_set.fetchmany() == []
        assert result_set.rownumber == 5

    async def test_fetchall_across_pages(self):
        result_set = await _create_result_set(AthenaAioResultSet, [("1", "2"), ("3", "4"), ("5",)])
        assert await result_set.fetchone() == (1,)
        assert await result_set.fetchall() == [(2,), (3,), (4,), (5,)]
        assert result_set.rownumber == 5
        assert await result_set.fetchall() == []

    async def test_fetchmany_size_not_integer(self):
        """A size that is not an integer raises TypeError without consuming rows."""
        result_set = await _create_result_set(AthenaAioResultSet)
        with pytest.raises(TypeError):
            await result_set.fetchmany(1.5)
        assert await result_set.fetchall() == [(1,), (2,)]

    async def test_fetchone_across_pages(self):
        result_set = await _create_result_set(AthenaAioResultSet, [("1", "2"), ("3",)])
        assert [await result_set.fetchone() for _ in range(4)] == [(1,), (2,), (3,), None]
        assert result_set.rownumber == 3
        assert result_set.connection.client.get_query_results.call_count == 2

    @pytest.mark.parametrize(
        ("page", "expected"), [(RuntimeError("failed"), OperationalError), (("x",), ValueError)]
    )
    @pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall"])
    async def test_fetch_after_failed_fetch(self, method, page, expected):
        """GH-1225: after a fetch raises, later fetches raise without a request."""
        result_set = await _create_result_set(AthenaAioResultSet, [("1", "2"), page, ("3",)])
        assert await result_set.fetchmany(2) == [(1,), (2,)]
        with pytest.raises(expected):
            await getattr(result_set, method)()
        await _assert_fetches_fail(result_set)
        assert result_set.connection.client.get_query_results.call_count == 2

    @pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall"])
    async def test_fetch_after_cancelled_fetch(self, method):
        """GH-1225: after a fetch is cancelled during its page request, later fetches raise."""
        result_set = await _create_result_set(AthenaAioResultSet, [("1", "2"), ("3",)])
        assert await result_set.fetchmany(2) == [(1,), (2,)]
        requested, release = threading.Event(), threading.Event()
        responses = result_set.connection.client.get_query_results.side_effect

        def respond(**kwargs):
            requested.set()
            assert release.wait(EVENT_TIMEOUT)
            return next(responses)

        result_set.connection.client.get_query_results.side_effect = respond
        task = asyncio.create_task(getattr(result_set, method)())
        try:
            assert await asyncio.to_thread(requested.wait, EVENT_TIMEOUT)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await _assert_fetches_fail(result_set)
        finally:
            release.set()
        assert result_set.connection.client.get_query_results.call_count == 2

    async def test_fetch_after_close(self):
        """GH-1207: the fetches of a closed result set raise."""
        result_set = await _create_result_set(AthenaAioResultSet, [("1", "2"), ("3",)])
        result_set.close()
        for fetch in (result_set.fetchone, result_set.fetchmany, result_set.fetchall):
            with pytest.raises(ProgrammingError):
                await fetch()
        with pytest.raises(ProgrammingError):
            await anext(result_set)
        assert result_set.rownumber is None
