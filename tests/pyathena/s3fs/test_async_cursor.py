# Copyright 2017 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import contextlib
import random
import time

import pytest

from pyathena.error import ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.s3fs.async_cursor import AsyncS3FSCursor
from pyathena.s3fs.result_set import AthenaS3FSResultSet
from tests import ENV
from tests.pyathena.conftest import connect
from tests.pyathena.expected import (
    ARRAY_JSON,
    MAP_JSON,
    PYTHON_VALUES,
    TIME_OF_TIMESTAMP,
    ExpectedResult,
)
from tests.pyathena.tables import ONE_ROW_COMPLEX


class TestAsyncS3FSCursor:
    def test_fetchone(self, async_s3fs_cursor):
        query_id, future = async_s3fs_cursor.execute("SELECT * FROM one_row")
        result_set = future.result()
        assert result_set.rownumber == 0
        assert result_set.fetchone() == (1,)
        assert result_set.rownumber == 1
        assert result_set.fetchone() is None

    def test_fetchmany(self, async_s3fs_cursor):
        query_id, future = async_s3fs_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        result_set = future.result()
        assert len(result_set.fetchmany(10)) == 10
        assert len(result_set.fetchmany(10)) == 5

    def test_fetchall(self, async_s3fs_cursor):
        query_id, future = async_s3fs_cursor.execute("SELECT * FROM one_row")
        result_set = future.result()
        assert result_set.fetchall() == [(1,)]

        query_id, future = async_s3fs_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        result_set = future.result()
        assert result_set.fetchall() == [(i,) for i in range(10000)]

    def test_arraysize(self, async_s3fs_cursor):
        async_s3fs_cursor.arraysize = 5
        query_id, future = async_s3fs_cursor.execute("SELECT * FROM many_rows LIMIT 20")
        result_set = future.result()
        assert len(result_set.fetchmany()) == 5

    def test_arraysize_default(self, async_s3fs_cursor):
        assert async_s3fs_cursor.arraysize == AthenaS3FSResultSet.DEFAULT_FETCH_SIZE

    def test_invalid_arraysize(self, async_s3fs_cursor):
        async_s3fs_cursor.arraysize = 10000
        assert async_s3fs_cursor.arraysize == 10000
        with pytest.raises(ProgrammingError):
            async_s3fs_cursor.arraysize = -1

    def test_complex(self, async_s3fs_cursor):
        expected = ExpectedResult(ONE_ROW_COMPLEX, casts=(TIME_OF_TIMESTAMP, ARRAY_JSON, MAP_JSON))
        query_id, future = async_s3fs_cursor.execute(expected.sql)
        result_set = future.result()
        assert result_set.description == expected.description()
        assert result_set.fetchall() == expected.rows(PYTHON_VALUES)

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

    def test_open_close(self):
        with (
            contextlib.closing(connect(schema_name=ENV.schema)) as conn,
            conn.cursor(AsyncS3FSCursor) as cursor,
        ):
            query_id, future = cursor.execute("SELECT * FROM one_row")
            result_set = future.result()
            assert result_set.fetchall() == [(1,)]

    def test_no_ops(self):
        conn = connect(schema_name=ENV.schema)
        cursor = conn.cursor(AsyncS3FSCursor)
        cursor.close()
        conn.close()

    def test_show_columns(self, async_s3fs_cursor):
        query_id, future = async_s3fs_cursor.execute("SHOW COLUMNS IN one_row")
        result_set = future.result()
        assert result_set.description == [("field", "string", None, None, 0, 0, "UNKNOWN")]
        assert result_set.fetchall() == [("number_of_rows      ",)]

    def test_empty_result(self, async_s3fs_cursor):
        query_id, future = async_s3fs_cursor.execute("SELECT * FROM one_row WHERE 1 = 2")
        result_set = future.result()
        assert query_id
        assert result_set.rownumber == 0
        assert result_set.fetchone() is None
        assert result_set.fetchmany() == []
        assert result_set.fetchmany(10) == []
        assert result_set.fetchall() == []
