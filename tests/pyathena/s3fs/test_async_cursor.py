# Copyright 2017 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import random
import time
from unittest.mock import MagicMock, patch

import pytest

from pyathena.error import ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.s3fs.async_cursor import AsyncS3FSCursor
from pyathena.s3fs.reader import AthenaCSVReader, EmptyStringAsNullCSVReader
from pyathena.s3fs.result_set import AthenaS3FSResultSet
from pyathena.util import RetryConfig
from tests import ENV
from tests.pyathena.conftest import connect
from tests.pyathena.util import (
    assert_close_stops_waiting_futures,
    assert_closes_result_sets_after_close,
    assert_description_reads_only_metadata,
)


class TestAsyncS3FSCursor:
    def test_arraysize(self, async_s3fs_cursor):
        async_s3fs_cursor.arraysize = 5
        query_id, future = async_s3fs_cursor.execute("SELECT a FROM many_rows ORDER BY a LIMIT 20")
        result_set = future.result()
        # The values check that the cursor passes its converter to the result set too.
        assert result_set.fetchmany() == [(i,) for i in range(5)]

    def test_arraysize_default(self, async_s3fs_cursor):
        assert async_s3fs_cursor.arraysize == AthenaS3FSResultSet.DEFAULT_FETCH_SIZE

    def test_invalid_arraysize(self, async_s3fs_cursor):
        async_s3fs_cursor.arraysize = 10000
        assert async_s3fs_cursor.arraysize == 10000
        with pytest.raises(ProgrammingError):
            async_s3fs_cursor.arraysize = -1

    def test_cancel(self, async_s3fs_cursor):
        query_id, future = async_s3fs_cursor.execute(
            """
            SELECT a.a * rand(), b.a * rand()
            FROM many_rows a
            CROSS JOIN many_rows b
            """
        )
        time.sleep(random.randint(5, 10))
        async_s3fs_cursor.cancel(query_id)
        result_set = future.result()
        assert result_set.state == AthenaQueryExecution.STATE_CANCELLED
        assert result_set.description is None
        assert result_set.fetchone() is None
        assert result_set.fetchmany() == []
        assert result_set.fetchall() == []

    def test_no_ops(self):
        conn = connect(schema_name=ENV.schema)
        cursor = conn.cursor(AsyncS3FSCursor)
        cursor.close()
        conn.close()

    def test_empty_result(self, async_s3fs_cursor):
        query_id, future = async_s3fs_cursor.execute("SELECT * FROM one_row WHERE 1 = 2")
        result_set = future.result()
        assert query_id
        assert result_set.rownumber == 0
        assert result_set.fetchone() is None
        assert result_set.fetchmany() == []
        assert result_set.fetchmany(10) == []
        assert result_set.fetchall() == []

    @pytest.mark.parametrize("execute_kwargs", [{}, {"csv_reader": AthenaCSVReader}])
    def test_read_options(self, execute_kwargs):
        """The cursor's read options reach the result set, and execute() overrides them.

        No AWS calls; the query and its result set are mocked.
        """
        cursor_kwargs = {"csv_reader": EmptyStringAsNullCSVReader}
        query_execution = MagicMock(state=AthenaQueryExecution.STATE_SUCCEEDED)
        with (
            AsyncS3FSCursor(
                connection=MagicMock(),
                converter=MagicMock(),
                formatter=MagicMock(),
                retry_config=RetryConfig(),
                **cursor_kwargs,
            ) as cursor,
            patch.object(AsyncS3FSCursor, "_execute", return_value="query_id"),
            patch.object(AsyncS3FSCursor, "_poll", return_value=query_execution),
            patch.object(cursor, "_result_set_class") as result_set_class,
        ):
            _, future = cursor.execute("SELECT 1", **execute_kwargs)
            future.result()
        kwargs = result_set_class.call_args.kwargs
        expected = {**cursor_kwargs, **execute_kwargs}
        assert {key: kwargs[key] for key in expected} == expected

    def test_close_closes_result_sets_after_close(self):
        """GH-1208: closing the cursor closes the result sets created after it (no AWS)."""
        assert_closes_result_sets_after_close(AsyncS3FSCursor)

    def test_close_stops_waiting_futures(self):
        """GH-1281: closing the cursor stops the futures that wait for a query (no AWS)."""
        assert_close_stops_waiting_futures(AsyncS3FSCursor)

    def test_description_reads_only_metadata(self):
        """GH-1281: description() reads only the metadata of a CSV result (no AWS)."""
        assert_description_reads_only_metadata(AsyncS3FSCursor)

    def test_closed(self, async_s3fs_cursor):
        """GH-1207: a closed cursor rejects execute()."""
        async_s3fs_cursor.close()

        with pytest.raises(ProgrammingError):
            async_s3fs_cursor.execute("SELECT * FROM one_row")
