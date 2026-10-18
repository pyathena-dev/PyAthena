import contextlib
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from pyathena.converter import _to_default
from pyathena.error import DatabaseError, DataError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.s3fs.converter import DefaultS3FSTypeConverter
from pyathena.s3fs.cursor import S3FSCursor
from pyathena.s3fs.reader import AthenaCSVReader, EmptyStringAsNullCSVReader
from pyathena.s3fs.result_set import AthenaS3FSResultSet
from pyathena.util import RetryConfig
from tests import ENV
from tests.pyathena import temporal
from tests.pyathena.conftest import connect
from tests.pyathena.util import (
    AMBIGUOUS_TIME_ZONE_QUERY,
    CONVERTED_VALUES_QUERY,
    CONVERTED_VALUES_ROW,
    cached_file_systems,
)


def _s3fs_converter_with_json_text():
    converter = DefaultS3FSTypeConverter()
    converter.set("json", _to_default)
    return converter


class TestS3FSCursor:
    def test_result_set_file_system_not_cached(self, s3fs_cursor):
        # GH-978: the filesystem that read the results was kept in the fsspec
        # instance cache with the connection, so the connection was never freed.
        s3fs_cursor.execute("SELECT * FROM one_row")
        assert s3fs_cursor.fetchall() == [(1,)]
        assert not cached_file_systems(s3fs_cursor.connection)

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

    def test_temporal_types(self, s3fs_cursor):
        """Temporal values convert as the contract of #1212 defines."""
        row = s3fs_cursor.execute(temporal.query()).fetchone()
        assert row == temporal.fetched_row()
        assert temporal.zones(row) == temporal.zones(temporal.fetched_row())

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

    def test_executemany(self, s3fs_cursor, empty_table):
        rows = [(1, "foo"), (2, "bar"), (3, "jim o'rourke")]
        s3fs_cursor.executemany(
            f"INSERT INTO {empty_table} (a, b) VALUES (%(a)d, %(b)s)",
            [{"a": a, "b": b} for a, b in rows],
        )
        s3fs_cursor.execute(f"SELECT * FROM {empty_table}")
        assert sorted(s3fs_cursor.fetchall()) == rows

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

    @pytest.mark.parametrize(
        ("s3fs_cursor", "expected_empty"),
        [
            # EmptyStringAsNullCSVReader: empty string becomes None
            ({"cursor_kwargs": {"csv_reader": EmptyStringAsNullCSVReader}}, None),
            # AthenaCSVReader: empty string is preserved
            ({"cursor_kwargs": {"csv_reader": AthenaCSVReader}}, ""),
        ],
        indirect=["s3fs_cursor"],
    )
    def test_null_vs_empty_string(self, s3fs_cursor, expected_empty):
        """
        Test NULL vs empty string handling with different CSV readers.

        EmptyStringAsNullCSVReader: Both NULL and empty string become None.
        AthenaCSVReader: NULL is None, empty string is preserved as ''.
        Both readers read values with tabs, newlines, and commas, which Athena quotes.

        See docs/null_handling.md for details.
        """
        s3fs_cursor.execute(
            """
            SELECT
              NULL AS null_col
              ,'' AS empty_col
              ,'before' || CHR(9) || 'after' AS col_tab
              ,'line1' || CHR(10) || 'line2' AS col_newline
              ,'a' || CHR(9) || 'b' || CHR(10) || 'c' AS col_tab_newline
              ,'a,b,c' AS col_comma
            """
        )
        assert s3fs_cursor.fetchall() == [
            (None, expected_empty, "before\tafter", "line1\nline2", "a\tb\nc", "a,b,c")
        ]

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
        s3fs_cursor.execute(CONVERTED_VALUES_QUERY)
        assert s3fs_cursor.fetchall() == [CONVERTED_VALUES_ROW]

    def test_ambiguous_time_zone_value(self, s3fs_cursor):
        """A local time that its zone repeats raises DataError instead of choosing one."""
        s3fs_cursor.execute(AMBIGUOUS_TIME_ZONE_QUERY)
        with pytest.raises(DataError):
            s3fs_cursor.fetchall()

    @pytest.mark.parametrize(
        "s3fs_cursor",
        [
            pytest.param({"converter": _s3fs_converter_with_json_text()}, id="default"),
            pytest.param(
                {
                    "work_group": ENV.managed_work_group,
                    "s3_staging_dir": "",
                    "converter": _s3fs_converter_with_json_text(),
                },
                id="managed",
                marks=pytest.mark.skipif(
                    not ENV.managed_work_group,
                    reason="AWS_ATHENA_MANAGED_WORKGROUP not set",
                ),
            ),
        ],
        indirect=["s3fs_cursor"],
    )
    def test_fetch_all_rows_custom_converter(self, s3fs_cursor):
        s3fs_cursor.execute("SELECT 1 AS col, json_parse('{\"a\": 1}') AS col_json")
        assert s3fs_cursor.fetchall() == [(1, '{"a":1}')]

    @pytest.mark.parametrize("execute_kwargs", [{}, {"csv_reader": AthenaCSVReader}])
    def test_read_options(self, execute_kwargs):
        """The cursor's read options reach the result set, and execute() overrides them.

        No AWS calls; the query and its result set are mocked.
        """
        cursor_kwargs = {"csv_reader": EmptyStringAsNullCSVReader}
        query_execution = MagicMock(state=AthenaQueryExecution.STATE_SUCCEEDED)
        cursor = S3FSCursor(
            connection=MagicMock(),
            converter=MagicMock(),
            formatter=MagicMock(),
            retry_config=RetryConfig(),
            **cursor_kwargs,
        )
        with (
            patch.object(S3FSCursor, "_execute", return_value="query_id"),
            patch.object(S3FSCursor, "_poll", return_value=query_execution),
            patch("pyathena.s3fs.cursor.AthenaS3FSResultSet") as result_set_class,
        ):
            cursor.execute("SELECT 1", **execute_kwargs)
        kwargs = result_set_class.call_args.kwargs
        expected = {**cursor_kwargs, **execute_kwargs}
        assert {key: kwargs[key] for key in expected} == expected
