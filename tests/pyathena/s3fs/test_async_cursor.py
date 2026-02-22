import contextlib
import random
import time
from datetime import datetime
from decimal import Decimal

import pytest

from pyathena.error import ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.s3fs.async_cursor import AsyncS3FSCursor
from pyathena.s3fs.result_set import AthenaS3FSResultSet
from tests import ENV
from tests.pyathena.conftest import connect


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
        query_id, future = async_s3fs_cursor.execute(
            """
            SELECT
              col_boolean
              ,col_tinyint
              ,col_smallint
              ,col_int
              ,col_bigint
              ,col_float
              ,col_double
              ,col_string
              ,col_varchar
              ,col_timestamp
              ,CAST(col_timestamp AS time) AS col_time
              ,col_date
              ,col_binary
              ,col_array
              ,CAST(col_array AS json) AS col_array_json
              ,col_map
              ,CAST(col_map AS json) AS col_map_json
              ,col_struct
              ,col_decimal
            FROM one_row_complex
            """
        )
        result_set = future.result()
        assert result_set.description == [
            ("col_boolean", "boolean", None, None, 0, 0, "UNKNOWN"),
            ("col_tinyint", "tinyint", None, None, 3, 0, "UNKNOWN"),
            ("col_smallint", "smallint", None, None, 5, 0, "UNKNOWN"),
            ("col_int", "integer", None, None, 10, 0, "UNKNOWN"),
            ("col_bigint", "bigint", None, None, 19, 0, "UNKNOWN"),
            ("col_float", "float", None, None, 17, 0, "UNKNOWN"),
            ("col_double", "double", None, None, 17, 0, "UNKNOWN"),
            ("col_string", "varchar", None, None, 2147483647, 0, "UNKNOWN"),
            ("col_varchar", "varchar", None, None, 10, 0, "UNKNOWN"),
            ("col_timestamp", "timestamp", None, None, 3, 0, "UNKNOWN"),
            ("col_time", "time", None, None, 3, 0, "UNKNOWN"),
            ("col_date", "date", None, None, 0, 0, "UNKNOWN"),
            ("col_binary", "varbinary", None, None, 1073741824, 0, "UNKNOWN"),
            ("col_array", "array", None, None, 0, 0, "UNKNOWN"),
            ("col_array_json", "json", None, None, 0, 0, "UNKNOWN"),
            ("col_map", "map", None, None, 0, 0, "UNKNOWN"),
            ("col_map_json", "json", None, None, 0, 0, "UNKNOWN"),
            ("col_struct", "row", None, None, 0, 0, "UNKNOWN"),
            ("col_decimal", "decimal", None, None, 10, 1, "UNKNOWN"),
        ]
        assert result_set.fetchall() == [
            (
                True,
                127,
                32767,
                2147483647,
                9223372036854775807,
                0.5,
                0.25,
                "a string",
                "varchar",
                datetime(2017, 1, 1, 0, 0, 0),
                datetime(2017, 1, 1, 0, 0, 0).time(),
                datetime(2017, 1, 2).date(),
                b"123",
                [1, 2],
                [1, 2],
                {"1": 2, "3": 4},
                {"1": 2, "3": 4},
                {"a": 1, "b": 2},
                Decimal("0.1"),
            )
        ]

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
