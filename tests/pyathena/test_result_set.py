# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
import collections
import sys
from unittest.mock import MagicMock, patch

import pytest

from pyathena.aio.common import AioBaseCursor, WithAsyncFetch
from pyathena.common import BaseCursor, CursorIterator
from pyathena.converter import DefaultTypeConverter
from pyathena.error import OperationalError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet, WithFetch, WithResultSet
from pyathena.util import RetryConfig, override
from tests.pyathena.util import close_during_next_request, get_query_results_responses


def _page(values, next_token=None):
    response = {"ResultSet": {"Rows": [{"Data": [{"VarCharValue": v}]} for v in values]}}
    if next_token:
        response["NextToken"] = next_token
    return response


def _row(*values):
    return {"Data": [{} if v is None else {"VarCharValue": v} for v in values]}


def _fetching_result_set(*pages):
    """Create a result set of a succeeded query from stubbed ``GetQueryResults`` pages.

    No AWS calls; the first page is fetched at creation, the others on demand.

    Args:
        *pages: The values of an integer column ``a``, one tuple per page, or an
            exception that the request for the page raises.

    Returns:
        The result set.
    """
    connection = MagicMock()
    connection.client.get_query_results.side_effect = get_query_results_responses(*pages)
    return AthenaResultSet(
        connection=connection,
        converter=DefaultTypeConverter(),
        query_execution=MagicMock(state=AthenaQueryExecution.STATE_SUCCEEDED),
        arraysize=2,
        retry_config=RetryConfig(),
    )


class _ClosingDeque(collections.deque):
    """A buffer that closes its result set when it is read, as another thread may."""

    def __init__(self, rows, result_set):
        """Initialize the buffer.

        Args:
            rows: The buffered rows.
            result_set: The result set to close.
        """
        super().__init__(rows)
        self._result_set = result_set

    @override
    def popleft(self):
        row = super().popleft()
        self._result_set.close()
        return row

    @override
    def __iter__(self):
        self._result_set.close()
        return super().__iter__()


class TestAthenaResultSet:
    def test_fetchmany_across_pages(self):
        result_set = _fetching_result_set(("1", "2"), ("3", "4"), ("5",))
        assert result_set.fetchmany(1) == [(1,)]
        assert result_set.rownumber == 1
        assert result_set.fetchmany(3) == [(2,), (3,), (4,)]
        assert result_set.rownumber == 4
        assert result_set.fetchmany() == [(5,)]
        assert result_set.fetchmany() == []
        assert result_set.rownumber == 5
        assert result_set.connection.client.get_query_results.call_count == 3

    def test_fetchall_across_pages(self):
        result_set = _fetching_result_set(("1", "2"), ("3", "4"), ("5",))
        assert result_set.fetchone() == (1,)
        assert result_set.fetchall() == [(2,), (3,), (4,), (5,)]
        assert result_set.rownumber == 5
        assert result_set.fetchall() == []
        assert result_set.fetchone() is None

    def test_fetchmany_size_above_maxsize(self):
        result_set = _fetching_result_set(("1", "2"), ("3",))
        assert result_set.fetchmany(sys.maxsize + 1) == [(1,), (2,), (3,)]
        assert result_set.rownumber == 3

    def test_fetchmany_size_not_integer(self):
        """A size that is not an integer raises TypeError without consuming rows."""
        result_set = _fetching_result_set(("1", "2"))
        with pytest.raises(TypeError):
            result_set.fetchmany(1.5)
        assert result_set.fetchall() == [(1,), (2,)]

    @pytest.mark.parametrize(("method", "args"), [("fetchmany", (3,)), ("fetchall", ())])
    def test_fetch_counts_rows_taken_before_error(self, method, args):
        """The rows taken before a failed page request are counted in rownumber."""
        result_set = _fetching_result_set(("1", "2"), RuntimeError("failed"))
        with pytest.raises(OperationalError):
            getattr(result_set, method)(*args)
        assert result_set.rownumber == 2

    def test_fetch_after_close(self):
        result_set = _fetching_result_set(("1", "2"), ("3",))
        result_set.close()
        assert result_set.fetchmany() == []
        assert result_set.fetchall() == []
        assert result_set.rownumber is None

    @pytest.mark.parametrize(
        ("method", "args", "expected"),
        [("fetchmany", (2,), [(1,)]), ("fetchall", (), [(1,), (2,), (3,)])],
    )
    def test_fetch_with_close_during_take(self, method, args, expected):
        """A close() while the buffered rows are taken keeps the rows taken before it."""
        result_set = _fetching_result_set(("1", "2", "3"))
        result_set._rows = _ClosingDeque(result_set._rows, result_set)
        assert getattr(result_set, method)(*args) == expected
        assert result_set.rownumber is None

    @pytest.mark.parametrize(
        ("method", "args", "expected"),
        [("fetchone", (), None), ("fetchmany", (), []), ("fetchall", (), [])],
    )
    def test_fetch_with_close_during_request(self, method, args, expected):
        """A close() during a page request discards the page and fetches nothing more."""
        result_set = _fetching_result_set(("1", "2"), ("3", "4"), ("5",))
        assert result_set.fetchmany(2) == [(1,), (2,)]
        get_query_results = close_during_next_request(result_set)
        assert getattr(result_set, method)(*args) == expected
        assert result_set.fetchone() is None
        assert result_set.fetchmany() == []
        assert result_set.fetchall() == []
        assert result_set.rownumber is None
        assert get_query_results.call_count == 2

    def test_fetchone_across_pages(self):
        result_set = _fetching_result_set(("1", "2"), ("3",))
        assert [result_set.fetchone() for _ in range(4)] == [(1,), (2,), (3,), None]
        assert result_set.rownumber == 3
        assert result_set.connection.client.get_query_results.call_count == 2

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
