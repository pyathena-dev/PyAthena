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
from pyathena.model import AthenaQueryExecution
from pyathena.util import RetryConfig


async def _create_result_set(result_set_class, pages=(("1", "2"),)):
    """Create a result set of a succeeded query from stubbed ``GetQueryResults`` pages.

    Args:
        result_set_class: The asyncio result set class to create.
        pages: The values of an integer column ``a``, one tuple per page. The first
            page is fetched at creation, the others on demand.

    Returns:
        The result set.
    """
    responses = []
    for i, page in enumerate(pages):
        response = {
            "ResultSet": {"Rows": [{"Data": [{"VarCharValue": v}]} for v in page]},
        }
        if i == 0:
            response["ResultSet"]["ResultSetMetadata"] = {
                "ColumnInfo": [{"Name": "a", "Type": "integer"}]
            }
        if i + 1 < len(pages):
            response["NextToken"] = f"token{i + 1}"
        responses.append(response)
    connection = MagicMock()
    connection.client.get_query_results.side_effect = responses
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
