import contextlib
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal

import pytest

from pyathena.error import DatabaseError, ProgrammingError
from pyathena.s3fs.cursor import S3FSCursor
from pyathena.s3fs.reader import AthenaCSVReader, DefaultCSVReader
from pyathena.s3fs.result_set import AthenaS3FSResultSet
from tests import ENV
from tests.pyathena.conftest import connect


class TestS3FSCursor:
    def test_fetchone(self, s3fs_cursor):
        s3fs_cursor.execute("SELECT * FROM one_row")
        assert s3fs_cursor.rownumber == 0
        assert s3fs_cursor.fetchone() == (1,)
        assert s3fs_cursor.rownumber == 1
        assert s3fs_cursor.fetchone() is None

    def test_fetchmany(self, s3fs_cursor):
        s3fs_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        assert len(s3fs_cursor.fetchmany(10)) == 10
        assert len(s3fs_cursor.fetchmany(10)) == 5

    def test_fetchall(self, s3fs_cursor):
        s3fs_cursor.execute("SELECT * FROM one_row")
        assert s3fs_cursor.fetchall() == [(1,)]
        s3fs_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        assert s3fs_cursor.fetchall() == [(i,) for i in range(10000)]

    def test_iterator(self, s3fs_cursor):
        s3fs_cursor.execute("SELECT * FROM one_row")
        assert list(s3fs_cursor) == [(1,)]
        pytest.raises(StopIteration, s3fs_cursor.__next__)

    def test_arraysize(self, s3fs_cursor):
        s3fs_cursor.arraysize = 5
        s3fs_cursor.execute("SELECT * FROM many_rows LIMIT 20")
        assert len(s3fs_cursor.fetchmany()) == 5

    def test_arraysize_default(self, s3fs_cursor):
        assert s3fs_cursor.arraysize == AthenaS3FSResultSet.DEFAULT_FETCH_SIZE

    def test_invalid_arraysize(self, s3fs_cursor):
        s3fs_cursor.arraysize = 10000
        assert s3fs_cursor.arraysize == 10000
        with pytest.raises(ProgrammingError):
            s3fs_cursor.arraysize = -1

    def test_complex(self, s3fs_cursor):
        s3fs_cursor.execute(
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
        assert s3fs_cursor.description == [
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
        assert s3fs_cursor.fetchall() == [
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
                {"1": "2", "3": "4"},
                {"1": 2, "3": 4},
                {"a": "1", "b": "2"},
                Decimal("0.1"),
            )
        ]

    def test_cancel(self, s3fs_cursor):
        def cancel(c):
            time.sleep(random.randint(5, 10))
            c.cancel()

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(cancel, s3fs_cursor)
            pytest.raises(
                DatabaseError,
                lambda: s3fs_cursor.execute(
                    """
                    SELECT a.a * rand(), b.a * rand()
                    FROM many_rows a
                    CROSS JOIN many_rows b
                    """
                ),
            )

    def test_cancel_initial(self, s3fs_cursor):
        pytest.raises(ProgrammingError, s3fs_cursor.cancel)

    def test_open_close(self):
        with (
            contextlib.closing(connect(schema_name=ENV.schema)) as conn,
            conn.cursor(S3FSCursor) as cursor,
        ):
            cursor.execute("SELECT * FROM one_row")
            assert cursor.fetchall() == [(1,)]

    def test_no_ops(self):
        conn = connect(schema_name=ENV.schema)
        cursor = conn.cursor(S3FSCursor)
        cursor.close()
        conn.close()

    def test_show_columns(self, s3fs_cursor):
        s3fs_cursor.execute("SHOW COLUMNS IN one_row")
        assert s3fs_cursor.description == [("field", "string", None, None, 0, 0, "UNKNOWN")]
        assert s3fs_cursor.fetchall() == [("number_of_rows      ",)]

    def test_empty_result(self, s3fs_cursor):
        query_id = s3fs_cursor.execute("SELECT * FROM one_row WHERE 1 = 2").query_id
        assert query_id
        assert s3fs_cursor.rownumber == 0
        assert s3fs_cursor.fetchone() is None
        assert s3fs_cursor.fetchmany() == []
        assert s3fs_cursor.fetchmany(10) == []
        assert s3fs_cursor.fetchall() == []

    def test_query_id(self, s3fs_cursor):
        assert not s3fs_cursor.query_id
        s3fs_cursor.execute("SELECT * FROM one_row")
        assert s3fs_cursor.query_id is not None

    def test_description_without_execute(self, s3fs_cursor):
        assert s3fs_cursor.description is None

    def test_description_with_select(self, s3fs_cursor):
        s3fs_cursor.execute("SELECT * FROM one_row")
        assert s3fs_cursor.description == [
            ("number_of_rows", "integer", None, None, 10, 0, "UNKNOWN")
        ]

    def test_description_with_ctas(self, s3fs_cursor):
        table_name = (
            f"test_description_with_ctas_{''.join(random.choices(string.ascii_lowercase, k=10))}"
        )
        location = f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/"
        s3fs_cursor.execute(
            f"""
            CREATE TABLE {ENV.schema}.{table_name}
            WITH (
                format='PARQUET',
                external_location='{location}'
            ) AS SELECT a FROM many_rows LIMIT 1
            """
        )
        assert s3fs_cursor.description == [("rows", "bigint", None, None, 19, 0, "UNKNOWN")]
        # CTAS returns affected row count via rowcount, not via fetchone()
        assert s3fs_cursor.rowcount == 1
        assert s3fs_cursor.fetchone() is None

    def test_description_with_create_table(self, s3fs_cursor):
        table_name = (
            f"test_description_with_create_table_"
            f"{''.join(random.choices(string.ascii_lowercase, k=10))}"
        )
        s3fs_cursor.execute(
            f"""
            CREATE EXTERNAL TABLE {ENV.schema}.{table_name} (
                a INT
            ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
            STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION '{ENV.s3_staging_dir}'
            """
        )
        assert s3fs_cursor.description == []
        assert s3fs_cursor.fetchone() is None
        s3fs_cursor.execute(f"DROP TABLE {ENV.schema}.{table_name}")

    @pytest.mark.skip(reason="Requires insert_test table to exist in test environment")
    def test_executemany(self, s3fs_cursor):
        rows = [(1, "foo"), (2, "bar"), (3, "jim")]
        s3fs_cursor.executemany(
            f"INSERT INTO {ENV.schema}.insert_test (a, b) VALUES (%(a)s, %(b)s)",
            [{"a": row[0], "b": row[1]} for row in rows],
        )
        s3fs_cursor.execute(f"SELECT * FROM {ENV.schema}.insert_test ORDER BY a")
        assert s3fs_cursor.fetchall() == rows
        s3fs_cursor.execute(f"DELETE FROM {ENV.schema}.insert_test WHERE a IN (1, 2, 3)")

    def test_on_start_query_execution(self):
        callback_query_id = None

        def callback(query_id):
            nonlocal callback_query_id
            callback_query_id = query_id

        with (
            contextlib.closing(
                connect(
                    schema_name=ENV.schema,
                    cursor_class=S3FSCursor,
                    cursor_kwargs={"on_start_query_execution": callback},
                )
            ) as conn,
            conn.cursor() as cursor,
        ):
            cursor.execute("SELECT * FROM one_row")
            assert callback_query_id == cursor.query_id

    def test_on_start_query_execution_execute(self):
        callback_query_id = None

        def callback(query_id):
            nonlocal callback_query_id
            callback_query_id = query_id

        with (
            contextlib.closing(
                connect(
                    schema_name=ENV.schema,
                    cursor_class=S3FSCursor,
                )
            ) as conn,
            conn.cursor() as cursor,
        ):
            cursor.execute("SELECT * FROM one_row", on_start_query_execution=callback)
            assert callback_query_id == cursor.query_id

    def test_contain_tab_character(self, s3fs_cursor):
        """Test that tab characters in result data are handled correctly.

        S3FS cursor uses tab as delimiter for parsing Athena's CSV output.
        This test verifies that data containing tab characters is correctly
        parsed when Athena properly quotes such fields.
        """
        # Test with tab character in string using CHR(9)
        s3fs_cursor.execute("SELECT 'before' || CHR(9) || 'after' AS col_with_tab")
        result = s3fs_cursor.fetchone()
        assert result == ("before\tafter",)

        # Test with multiple columns where one contains tab
        s3fs_cursor.execute(
            """
            SELECT
                'normal' AS col1,
                'has' || CHR(9) || 'tab' AS col2,
                'also_normal' AS col3
            """
        )
        result = s3fs_cursor.fetchone()
        assert result == ("normal", "has\ttab", "also_normal")

        # Test with newline character as well
        s3fs_cursor.execute("SELECT 'line1' || CHR(10) || 'line2' AS col_with_newline")
        result = s3fs_cursor.fetchone()
        assert result == ("line1\nline2",)

        # Test with both tab and newline
        s3fs_cursor.execute(
            "SELECT 'a' || CHR(9) || 'b' || CHR(10) || 'c' AS col_with_special_chars"
        )
        result = s3fs_cursor.fetchone()
        assert result == ("a\tb\nc",)

    @pytest.mark.parametrize(
        "csv_reader_class",
        [DefaultCSVReader, AthenaCSVReader],
    )
    def test_basic_query_with_reader(self, csv_reader_class):
        """Both readers should work for basic queries."""
        with (
            contextlib.closing(
                connect(
                    schema_name=ENV.schema,
                    cursor_class=S3FSCursor,
                    cursor_kwargs={"csv_reader": csv_reader_class},
                )
            ) as conn,
            conn.cursor() as cursor,
        ):
            cursor.execute("SELECT * FROM one_row")
            result = cursor.fetchall()
            assert result == [(1,)]

    @pytest.mark.parametrize(
        "csv_reader_class",
        [DefaultCSVReader, AthenaCSVReader],
    )
    def test_multiple_columns_with_reader(self, csv_reader_class):
        """Test multiple columns work with both readers."""
        with (
            contextlib.closing(
                connect(
                    schema_name=ENV.schema,
                    cursor_class=S3FSCursor,
                    cursor_kwargs={"csv_reader": csv_reader_class},
                )
            ) as conn,
            conn.cursor() as cursor,
        ):
            cursor.execute(
                """
                SELECT
                    1 AS col_int,
                    'text' AS col_string,
                    1.5 AS col_double
                """
            )
            result = cursor.fetchone()
            assert result == (1, "text", 1.5)

    def test_null_with_default_reader(self):
        """DefaultCSVReader: NULL is returned as None."""
        with (
            contextlib.closing(
                connect(
                    schema_name=ENV.schema,
                    cursor_class=S3FSCursor,
                    cursor_kwargs={"csv_reader": DefaultCSVReader},
                )
            ) as conn,
            conn.cursor() as cursor,
        ):
            cursor.execute("SELECT NULL AS null_col")
            result = cursor.fetchone()
            assert result == (None,)

    def test_null_with_athena_reader(self):
        """AthenaCSVReader: NULL is returned as None."""
        with (
            contextlib.closing(
                connect(
                    schema_name=ENV.schema,
                    cursor_class=S3FSCursor,
                    cursor_kwargs={"csv_reader": AthenaCSVReader},
                )
            ) as conn,
            conn.cursor() as cursor,
        ):
            cursor.execute("SELECT NULL AS null_col")
            result = cursor.fetchone()
            assert result == (None,)

    def test_empty_string_with_default_reader(self):
        """DefaultCSVReader: Empty string becomes None (loses distinction)."""
        with (
            contextlib.closing(
                connect(
                    schema_name=ENV.schema,
                    cursor_class=S3FSCursor,
                    cursor_kwargs={"csv_reader": DefaultCSVReader},
                )
            ) as conn,
            conn.cursor() as cursor,
        ):
            cursor.execute("SELECT '' AS empty_col")
            result = cursor.fetchone()
            # DefaultCSVReader treats empty string same as NULL
            assert result == (None,)

    def test_empty_string_with_athena_reader(self):
        """AthenaCSVReader: Empty string is preserved as empty string."""
        with (
            contextlib.closing(
                connect(
                    schema_name=ENV.schema,
                    cursor_class=S3FSCursor,
                    cursor_kwargs={"csv_reader": AthenaCSVReader},
                )
            ) as conn,
            conn.cursor() as cursor,
        ):
            cursor.execute("SELECT '' AS empty_col")
            result = cursor.fetchone()
            # AthenaCSVReader preserves empty string as ''
            assert result == ("",)

    @pytest.mark.parametrize(
        ("csv_reader", "expected_empty"),
        [
            (DefaultCSVReader, None),  # DefaultCSVReader: empty string becomes None
            (AthenaCSVReader, ""),  # AthenaCSVReader: empty string is preserved
        ],
    )
    def test_null_vs_empty_string(self, csv_reader, expected_empty):
        """
        Test NULL vs empty string handling with different CSV readers.

        DefaultCSVReader: Both NULL and empty string become None.
        AthenaCSVReader: NULL is None, empty string is preserved as ''.

        See docs/null_handling.rst for details.
        """
        with (
            contextlib.closing(
                connect(
                    schema_name=ENV.schema,
                    cursor_class=S3FSCursor,
                    cursor_kwargs={"csv_reader": csv_reader},
                )
            ) as conn,
            conn.cursor() as cursor,
        ):
            cursor.execute("SELECT NULL AS null_col, '' AS empty_col")
            result = cursor.fetchone()
            # NULL is always None
            assert result[0] is None
            # Empty string behavior depends on reader
            assert result[1] == expected_empty

    def test_mixed_values_with_athena_reader(self):
        """AthenaCSVReader: Mixed NULL, empty string, and regular values."""
        with (
            contextlib.closing(
                connect(
                    schema_name=ENV.schema,
                    cursor_class=S3FSCursor,
                    cursor_kwargs={"csv_reader": AthenaCSVReader},
                )
            ) as conn,
            conn.cursor() as cursor,
        ):
            cursor.execute(
                """
                SELECT
                    NULL AS null_col,
                    '' AS empty_col,
                    'text' AS text_col,
                    NULL AS null_col2
                """
            )
            result = cursor.fetchone()
            assert result == (None, "", "text", None)

    @pytest.mark.parametrize(
        "csv_reader_class",
        [DefaultCSVReader, AthenaCSVReader],
    )
    def test_quoted_string_with_comma(self, csv_reader_class):
        """Both readers should handle strings containing commas."""
        with (
            contextlib.closing(
                connect(
                    schema_name=ENV.schema,
                    cursor_class=S3FSCursor,
                    cursor_kwargs={"csv_reader": csv_reader_class},
                )
            ) as conn,
            conn.cursor() as cursor,
        ):
            cursor.execute("SELECT 'a,b,c' AS col_with_commas")
            result = cursor.fetchone()
            assert result == ("a,b,c",)

    @pytest.mark.parametrize(
        "s3fs_cursor",
        [
            pytest.param({}, id="default"),
            pytest.param(
                {"work_group": ENV.managed_work_group, "s3_staging_dir": ""},
                id="managed",
                marks=pytest.mark.skipif(
                    not ENV.managed_work_group,
                    reason="AWS_ATHENA_MANAGED_WORKGROUP not set",
                ),
            ),
        ],
        indirect=["s3fs_cursor"],
    )
    def test_fetch_all_rows(self, s3fs_cursor):
        s3fs_cursor.execute("SELECT 1 AS col")
        assert s3fs_cursor.fetchall() == [(1,)]
