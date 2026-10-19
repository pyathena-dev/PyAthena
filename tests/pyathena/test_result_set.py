# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
import sys
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ReadTimeoutError

from pyathena.converter import DefaultTypeConverter
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import (
    AthenaResultSet,
    AthenaResultSetMetadataReader,
    AthenaUnloadResultSetMetadataReader,
)
from pyathena.util import RetryConfig
from tests.pyathena.util import (
    assert_fetches_fail,
    get_query_results_responses,
    query_results_response,
)


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


class TestAthenaResultSetMetadataReader:
    @pytest.mark.parametrize(
        ("state", "expected", "requests"),
        [
            (
                AthenaQueryExecution.STATE_SUCCEEDED,
                [("a", "integer", None, None, 3, 0, "UNKNOWN")],
                1,
            ),
            (AthenaQueryExecution.STATE_FAILED, None, 0),
        ],
    )
    def test_describe(self, state, expected, requests):
        """GH-1281: describe() reads a page of one row, or nothing for a failed query (no AWS)."""
        connection = MagicMock()
        connection.client.get_query_results.return_value = query_results_response(
            [("a", "integer")]
        )
        reader = AthenaResultSetMetadataReader(connection, MagicMock(state=state), RetryConfig())
        assert reader.describe() == expected
        assert connection.client.get_query_results.call_count == requests
        if requests:
            assert connection.client.get_query_results.call_args.kwargs["MaxResults"] == 1


class TestAthenaUnloadResultSetMetadataReader:
    def test_read_data_manifest_body_read_failure(self):
        """GH-1235: a failed data manifest read raises OperationalError and closes the body.

        No AWS calls; the S3 client is mocked.
        """
        body = MagicMock(read=MagicMock(side_effect=ReadTimeoutError(endpoint_url="s3")))
        connection = MagicMock()
        connection.s3_client.get_object.return_value = {"Body": body}
        reader = AthenaUnloadResultSetMetadataReader(
            connection,
            MagicMock(
                state=AthenaQueryExecution.STATE_SUCCEEDED,
                data_manifest_location="s3://bucket/path/query-id-manifest.csv",
            ),
            RetryConfig(),
        )
        with pytest.raises(OperationalError):
            reader._read_data_manifest()
        body.close.assert_called_once_with()


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

    @pytest.mark.parametrize(
        ("page", "expected"),
        [
            (RuntimeError("failed"), OperationalError),
            (KeyboardInterrupt(), KeyboardInterrupt),
            (("x",), ValueError),
        ],
    )
    @pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall"])
    def test_fetch_after_failed_fetch(self, method, page, expected):
        """GH-1225: after a fetch raises, later fetches raise without a request.

        The page request fails, is interrupted, or returns a value that fails to
        convert.
        """
        result_set = _fetching_result_set(("1", "2"), page, ("3",))
        assert result_set.fetchmany(2) == [(1,), (2,)]
        with pytest.raises(expected):
            getattr(result_set, method)()
        assert_fetches_fail(result_set)
        assert result_set.connection.client.get_query_results.call_count == 2

    def test_fetch_after_close(self):
        """GH-1207: the fetches of a closed result set raise without a request."""
        result_set = _fetching_result_set(("1", "2"), ("3",))
        client = result_set._connection.client
        result_set.close()
        for fetch in (result_set.fetchone, result_set.fetchmany, result_set.fetchall):
            with pytest.raises(ProgrammingError):
                fetch()
        with pytest.raises(ProgrammingError):
            next(result_set)
        assert result_set.rownumber is None
        assert client.get_query_results.call_count == 1

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
        pages = get_query_results_responses(["a", "1"], ["a", "2"])
        with patch.object(result_set, "_get_query_results", side_effect=pages):
            assert result_set._fetch_all_rows() == [("1",), ("a",), ("2",)]

    def test_fetch_all_rows_as_csv(self):
        """The GetQueryResults rows are written as Athena writes a CSV result file.

        Only the first page starts with the column labels, so a later row equal to
        them is data. No AWS calls; GetQueryResults is mocked.
        """
        result_set = AthenaResultSet.__new__(AthenaResultSet)  # bypass __init__
        result_set._connection = MagicMock()
        result_set._query_execution = None
        result_set._metadata_reader = AthenaResultSetMetadataReader(
            result_set._connection, MagicMock(substatement_type=None), RetryConfig()
        )
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
