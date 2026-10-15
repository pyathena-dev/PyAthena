# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
from unittest.mock import MagicMock

import pytest

from pyathena.aio.result_set import AthenaAioDictResultSet, AthenaAioResultSet
from pyathena.converter import DefaultTypeConverter
from pyathena.error import OperationalError
from pyathena.model import AthenaQueryExecution
from pyathena.util import RetryConfig
from tests.pyathena.util import get_query_results_responses


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


class TestAthenaAioResultSet:
    @pytest.mark.parametrize("result_set_class", [AthenaAioResultSet, AthenaAioDictResultSet])
    async def test_sync_iteration_raises(self, result_set_class):
        result_set = await _create_result_set(result_set_class)
        with pytest.raises(
            TypeError,
            match=rf"'{result_set_class.__name__}' object is not iterable; use 'async for'",
        ):
            iter(result_set)

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

    @pytest.mark.parametrize(("method", "args"), [("fetchmany", (3,)), ("fetchall", ())])
    async def test_fetch_counts_rows_taken_before_error(self, method, args):
        """The rows taken before a failed page request are counted in rownumber."""
        result_set = await _create_result_set(
            AthenaAioResultSet, [("1", "2"), RuntimeError("failed")]
        )
        with pytest.raises(OperationalError):
            await getattr(result_set, method)(*args)
        assert result_set.rownumber == 2

    async def test_fetch_after_close(self):
        result_set = await _create_result_set(AthenaAioResultSet, [("1", "2"), ("3",)])
        result_set.close()
        assert await result_set.fetchone() is None
        assert await result_set.fetchmany() == []
        assert await result_set.fetchall() == []
        assert result_set.rownumber is None
