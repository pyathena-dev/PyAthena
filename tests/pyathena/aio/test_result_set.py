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


async def _create_result_set(result_set_class):
    """Create a result set of a succeeded query from a stubbed ``GetQueryResults``.

    Args:
        result_set_class: The asyncio result set class to create.

    Returns:
        The result set, holding the rows 1 and 2 of an integer column ``a``.
    """
    connection = MagicMock()
    connection.client.get_query_results.return_value = {
        "ResultSet": {
            "ResultSetMetadata": {"ColumnInfo": [{"Name": "a", "Type": "integer"}]},
            "Rows": [{"Data": [{"VarCharValue": "1"}]}, {"Data": [{"VarCharValue": "2"}]}],
        }
    }
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
        connection, DefaultTypeConverter(), query_execution, 1000, RetryConfig()
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
