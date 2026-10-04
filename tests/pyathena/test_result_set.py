# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
from unittest.mock import MagicMock, patch

import pytest

from pyathena.aio.common import AioBaseCursor, WithAsyncFetch
from pyathena.common import BaseCursor, CursorIterator
from pyathena.converter import DefaultTypeConverter
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet, WithFetch, WithResultSet
from pyathena.util import RetryConfig


def _page(values, next_token=None):
    response = {"ResultSet": {"Rows": [{"Data": [{"VarCharValue": v}]} for v in values]}}
    if next_token:
        response["NextToken"] = next_token
    return response


def _row(*values):
    return {"Data": [{} if v is None else {"VarCharValue": v} for v in values]}


class TestAthenaResultSet:
    def test_fetch_all_rows_skips_column_labels_only_on_first_page(self):
        """A later page can start with a data row equal to the column labels.

        No AWS calls; the GetQueryResults pages are mocked.
        """
        result_set = AthenaResultSet(
            connection=MagicMock(),
            converter=DefaultTypeConverter(),
            query_execution=MagicMock(state=AthenaQueryExecution.STATE_SUCCEEDED),
            arraysize=1,
            retry_config=RetryConfig(),
            _pre_fetch=False,
        )
        result_set._process_metadata(
            {"ResultSet": {"ResultSetMetadata": {"ColumnInfo": [{"Name": "a", "Type": "varchar"}]}}}
        )
        pages = [_page(["a", "1"], "token"), _page(["a", "2"])]
        with patch.object(result_set, "_get_query_results", side_effect=pages):
            assert result_set._fetch_all_rows() == [("1",), ("a",), ("2",)]

    def test_fetch_all_rows_as_csv(self):
        """The GetQueryResults rows are written as Athena writes a CSV result file.

        Only the first page starts with the column labels, so a later row equal to
        them is data. No AWS calls; GetQueryResults is mocked.
        """
        result_set = AthenaResultSet.__new__(AthenaResultSet)  # bypass __init__
        result_set._query_execution = None
        result_set._metadata = tuple(
            {"Name": name, "Type": "varchar", "Precision": 0, "Scale": 0, "Nullable": "UNKNOWN"}
            for name in ("v", 'w"')
        )
        pages = [
            {"ResultSet": {"Rows": [_row("v", 'w"'), _row('a,"b"\nc', None)]}, "NextToken": "t"},
            {"ResultSet": {"Rows": [_row("v", 'w"'), _row("", "1")]}},
        ]
        with patch.object(AthenaResultSet, "_get_query_results", side_effect=pages):
            data = result_set._fetch_all_rows_as_csv()
        assert data == b'"v","w"""\n"a,""b""\nc",\n"v","w"""\n"","1"\n'


class TestWithResultSet:
    def test_is_mixin(self):
        assert WithResultSet.__bases__ == (object,)

    @pytest.mark.parametrize(
        ("cursor_base", "base"),
        [(WithFetch, BaseCursor), (WithAsyncFetch, AioBaseCursor)],
    )
    def test_precedes_cursor_bases(self, cursor_base, base):
        # Listed first, so that its members take precedence over the cursor bases'.
        assert cursor_base.__bases__ == (WithResultSet, base, CursorIterator)
