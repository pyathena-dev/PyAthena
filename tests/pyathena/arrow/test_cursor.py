# Copyright 2017 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import contextlib
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from pyathena.arrow.cursor import ArrowCursor
from pyathena.arrow.result_set import AthenaArrowResultSet
from pyathena.error import DatabaseError, ProgrammingError
from tests import ENV
from tests.pyathena.conftest import connect
from tests.pyathena.expected import (
    ARRAY_JSON,
    ARROW_TABLE_VALUES,
    ARROW_VALUES,
    MAP_JSON,
    TIME_OF_TIMESTAMP,
    UNLOAD_POLARS_VALUES,
    UNLOAD_VALUES,
    ExpectedResult,
    polars_type,
)
from tests.pyathena.tables import ONE_ROW_COMPLEX


class TestArrowCursor:
    def test_binary_null_vs_empty(self, arrow_cursor):
        query = """SELECT * FROM (VALUES
                    (1, CAST(NULL AS VARBINARY), 'null', CAST(NULL AS VARCHAR)),
                    (2, X'', 'empty', ''),
                    (3, X'00ff275c25', 'comma, quote" and' || chr(10) || 'newline', 'NULL')
                ) AS t(id, value, label, text_value) ORDER BY id"""
        arrow_cursor.execute(query)
        assert arrow_cursor.as_arrow().column("value").to_pylist() == [None, "", "00 ff 27 5c 25"]
        rows = arrow_cursor.fetchall()
        assert [row[:3] for row in rows] == [
            (1, None, "null"),
            (2, b"", "empty"),
            (3, b"\x00\xff'\\%", 'comma, quote" and\nnewline'),
        ]
        assert [row[3] for row in rows] == ["", "", "NULL"]

    def test_binary_single_null(self, arrow_cursor):
        arrow_cursor.execute("SELECT CAST(NULL AS VARBINARY) AS value")
        assert arrow_cursor.fetchall() == [(None,)]

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["arrow_cursor"],
    )
    def test_fetchone(self, arrow_cursor):
        arrow_cursor.execute("SELECT * FROM one_row")
        assert arrow_cursor.rownumber == 0
        assert arrow_cursor.fetchone() == (1,)
        assert arrow_cursor.rownumber == 1
        assert arrow_cursor.fetchone() is None

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["arrow_cursor"],
    )
    def test_fetchmany(self, arrow_cursor):
        arrow_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        assert len(arrow_cursor.fetchmany(10)) == 10
        assert len(arrow_cursor.fetchmany(10)) == 5

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["arrow_cursor"],
    )
    def test_fetchall(self, arrow_cursor):
        arrow_cursor.execute("SELECT * FROM one_row")
        assert arrow_cursor.fetchall() == [(1,)]
        arrow_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        if arrow_cursor._unload:
            assert sorted(arrow_cursor.fetchall()) == [(i,) for i in range(10000)]
        else:
            assert arrow_cursor.fetchall() == [(i,) for i in range(10000)]

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["arrow_cursor"],
    )
    def test_iterator(self, arrow_cursor):
        arrow_cursor.execute("SELECT * FROM one_row")
        assert list(arrow_cursor) == [(1,)]
        pytest.raises(StopIteration, arrow_cursor.__next__)

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["arrow_cursor"],
    )
    def test_arraysize(self, arrow_cursor):
        arrow_cursor.arraysize = 5
        arrow_cursor.execute("SELECT * FROM many_rows LIMIT 20")
        assert len(arrow_cursor.fetchmany()) == 5

    def test_arraysize_default(self, arrow_cursor):
        assert arrow_cursor.arraysize == AthenaArrowResultSet.DEFAULT_FETCH_SIZE

    def test_invalid_arraysize(self, arrow_cursor):
        arrow_cursor.arraysize = 10000
        assert arrow_cursor.arraysize == 10000
        with pytest.raises(ProgrammingError):
            arrow_cursor.arraysize = -1

    def test_complex(self, arrow_cursor):
        expected = ExpectedResult(ONE_ROW_COMPLEX, casts=(TIME_OF_TIMESTAMP, ARRAY_JSON, MAP_JSON))
        arrow_cursor.execute(expected.sql)
        assert arrow_cursor.description == expected.description()
        assert arrow_cursor.fetchall() == expected.rows(ARROW_VALUES)

    @pytest.mark.parametrize(
        "arrow_cursor",
        [
            {
                "cursor_kwargs": {"unload": True},
            },
        ],
        indirect=["arrow_cursor"],
    )
    def test_complex_unload(self, arrow_cursor):
        # NOT_SUPPORTED: Unsupported Hive type: time
        # NOT_SUPPORTED: Unsupported Hive type: json
        expected = ExpectedResult(ONE_ROW_COMPLEX)
        arrow_cursor.execute(expected.sql)
        assert arrow_cursor.description == expected.description(unload=True)
        assert arrow_cursor.fetchall() == expected.rows(UNLOAD_VALUES)

    def test_fetch_no_data(self, arrow_cursor):
        pytest.raises(ProgrammingError, arrow_cursor.fetchone)
        pytest.raises(ProgrammingError, arrow_cursor.fetchmany)
        pytest.raises(ProgrammingError, arrow_cursor.fetchall)
        pytest.raises(ProgrammingError, arrow_cursor.as_arrow)
        pytest.raises(ProgrammingError, arrow_cursor.as_polars)

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["arrow_cursor"],
    )
    def test_as_arrow(self, arrow_cursor):
        table = arrow_cursor.execute("SELECT * FROM one_row").as_arrow()
        assert table.shape[0] == 1
        assert table.shape[1] == 1
        assert list(zip(*table.to_pydict().values(), strict=False)) == [(1,)]

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["arrow_cursor"],
    )
    def test_many_as_arrow(self, arrow_cursor):
        table = arrow_cursor.execute("SELECT * FROM many_rows").as_arrow()
        assert table.shape[0] == 10000
        assert table.shape[1] == 1
        assert list(zip(*table.to_pydict().values(), strict=False)) == [(i,) for i in range(10000)]

    def test_complex_as_arrow(self, arrow_cursor):
        expected = ExpectedResult(ONE_ROW_COMPLEX, casts=(TIME_OF_TIMESTAMP, ARRAY_JSON, MAP_JSON))
        table = arrow_cursor.execute(expected.sql).as_arrow()
        assert table.schema == expected.arrow_schema()
        assert list(zip(*table.to_pydict().values(), strict=True)) == expected.rows(
            ARROW_TABLE_VALUES
        )

    @pytest.mark.parametrize(
        "arrow_cursor",
        [
            {
                "cursor_kwargs": {"unload": True},
            },
        ],
        indirect=["arrow_cursor"],
    )
    def test_complex_unload_as_arrow(self, arrow_cursor):
        # NOT_SUPPORTED: Unsupported Hive type: time
        # NOT_SUPPORTED: Unsupported Hive type: json
        expected = ExpectedResult(ONE_ROW_COMPLEX)
        table = arrow_cursor.execute(expected.sql).as_arrow()
        assert table.schema == expected.arrow_schema(unload=True)
        assert list(zip(*table.to_pydict().values(), strict=True)) == expected.rows(UNLOAD_VALUES)

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["arrow_cursor"],
    )
    def test_as_polars(self, arrow_cursor):
        df = arrow_cursor.execute("SELECT * FROM one_row").as_polars()
        assert df.height == 1
        assert df.width == 1
        assert df.to_dicts() == [{"number_of_rows": 1}]

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["arrow_cursor"],
    )
    def test_many_as_polars(self, arrow_cursor):
        df = arrow_cursor.execute("SELECT * FROM many_rows").as_polars()
        assert df.height == 10000
        assert df.width == 1
        assert df.to_dicts() == [{"a": i} for i in range(10000)]

    def test_complex_as_polars(self, arrow_cursor):
        expected = ExpectedResult(ONE_ROW_COMPLEX, casts=(TIME_OF_TIMESTAMP, ARRAY_JSON, MAP_JSON))
        df = arrow_cursor.execute(expected.sql).as_polars()
        assert list(zip(df.columns, df.dtypes, strict=True)) == [
            (f.name, polars_type(f.type)) for f in expected.arrow_schema()
        ]
        assert df.to_dicts() == expected.dicts(ARROW_TABLE_VALUES)

    @pytest.mark.parametrize(
        "arrow_cursor",
        [
            {
                "cursor_kwargs": {"unload": True},
            },
        ],
        indirect=["arrow_cursor"],
    )
    def test_complex_unload_as_polars(self, arrow_cursor):
        # NOT_SUPPORTED: Unsupported Hive type: time
        # NOT_SUPPORTED: Unsupported Hive type: json
        expected = ExpectedResult(ONE_ROW_COMPLEX)
        df = arrow_cursor.execute(expected.sql).as_polars()
        assert list(zip(df.columns, df.dtypes, strict=True)) == [
            (f.name, polars_type(f.type)) for f in expected.arrow_schema(unload=True)
        ]
        assert df.to_dicts() == expected.dicts(UNLOAD_POLARS_VALUES)

    def test_cancel(self, arrow_cursor):
        def cancel(c):
            time.sleep(random.randint(5, 10))
            c.cancel()

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(cancel, arrow_cursor)

            pytest.raises(
                DatabaseError,
                lambda: arrow_cursor.execute(
                    """
                    SELECT a.a * rand(), b.a * rand()
                    FROM many_rows a
                    CROSS JOIN many_rows b
                    """
                ),
            )

    def test_cancel_initial(self, arrow_cursor):
        pytest.raises(ProgrammingError, arrow_cursor.cancel)

    def test_open_close(self):
        with contextlib.closing(connect()) as conn, conn.cursor(ArrowCursor):
            pass

    def test_no_ops(self):
        conn = connect()
        cursor = conn.cursor(ArrowCursor)
        cursor.close()
        conn.close()

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["arrow_cursor"],
    )
    def test_show_columns(self, arrow_cursor):
        arrow_cursor.execute("SHOW COLUMNS IN one_row")
        assert arrow_cursor.description == [("field", "string", None, None, 0, 0, "UNKNOWN")]
        assert arrow_cursor.fetchall() == [("number_of_rows      ",)]

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["arrow_cursor"],
    )
    def test_empty_result(self, arrow_cursor):
        table = "test_arrow_cursor_empty_result_" + "".join(
            random.choices(string.ascii_lowercase + string.digits, k=10)
        )
        df = arrow_cursor.execute(
            f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS
            {ENV.schema}.{table} (number_of_rows INT)
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            LINES TERMINATED BY '\n' STORED AS TEXTFILE
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table}/'
            """
        ).as_arrow()
        assert df.shape[0] == 0
        assert df.shape[1] == 0

    @pytest.mark.parametrize(
        "arrow_cursor",
        [
            {
                "cursor_kwargs": {"unload": True},
            },
        ],
        indirect=["arrow_cursor"],
    )
    def test_empty_result_unload(self, arrow_cursor):
        table = arrow_cursor.execute(
            """
            SELECT * FROM one_row LIMIT 0
            """
        ).as_arrow()
        assert table.shape[0] == 0
        assert table.shape[1] == 0

    def test_ctas(self, arrow_cursor):
        table_name = f"test_ctas_arrow_{''.join(random.choices(string.ascii_lowercase, k=10))}"
        location = f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/"
        arrow_cursor.execute(
            f"""
            CREATE TABLE {ENV.schema}.{table_name}
            WITH (
                format='PARQUET',
                external_location='{location}'
            ) AS SELECT a FROM many_rows LIMIT 1
            """
        )
        assert arrow_cursor.description == [("rows", "bigint", None, None, 19, 0, "UNKNOWN")]
        # CTAS returns affected row count via rowcount, not via fetchone()
        assert arrow_cursor.rowcount == 1
        assert arrow_cursor.fetchone() is None

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["arrow_cursor"],
    )
    def test_executemany(self, arrow_cursor, empty_table):
        rows = [(1, "foo"), (2, "bar"), (3, "jim o'rourke")]
        arrow_cursor.executemany(
            f"INSERT INTO {empty_table} (a, b) VALUES (%(a)d, %(b)s)",
            [{"a": a, "b": b} for a, b in rows],
        )
        arrow_cursor.execute(f"SELECT * FROM {empty_table}")
        assert sorted(arrow_cursor.fetchall()) == list(rows)

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["arrow_cursor"],
    )
    def test_executemany_fetch(self, arrow_cursor):
        arrow_cursor.executemany("SELECT %(x)d AS x FROM one_row", [{"x": i} for i in range(1, 2)])
        # Operations that have result sets are not allowed with executemany.
        pytest.raises(ProgrammingError, arrow_cursor.fetchall)
        pytest.raises(ProgrammingError, arrow_cursor.fetchmany)
        pytest.raises(ProgrammingError, arrow_cursor.fetchone)
        pytest.raises(ProgrammingError, arrow_cursor.as_arrow)
        pytest.raises(ProgrammingError, arrow_cursor.as_polars)

    def test_iceberg_table(self, arrow_cursor):
        iceberg_table = "test_iceberg_table_arrow_cursor"
        arrow_cursor.execute(
            f"""
            CREATE TABLE {ENV.schema}.{iceberg_table} (
              id INT,
              col1 STRING
            )
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{iceberg_table}/'
            tblproperties('table_type'='ICEBERG')
            """
        )
        arrow_cursor.execute(
            f"""
            INSERT INTO {ENV.schema}.{iceberg_table} (id, col1)
            VALUES (1, 'test1'), (2, 'test2')
            """
        )
        arrow_cursor.execute(
            f"""
            SELECT COUNT(*) FROM {ENV.schema}.{iceberg_table}
            """
        )
        assert arrow_cursor.fetchall() == [(2,)]

        arrow_cursor.execute(
            f"""
            UPDATE {ENV.schema}.{iceberg_table}
            SET col1 = 'test1_update'
            WHERE id = 1
            """
        )
        arrow_cursor.execute(
            f"""
            SELECT col1
            FROM {ENV.schema}.{iceberg_table}
            WHERE id = 1
            """
        )
        assert arrow_cursor.fetchall() == [("test1_update",)]

        arrow_cursor.execute(
            f"""
            CREATE TABLE {ENV.schema}.{iceberg_table}_merge (
              id INT,
              col1 STRING
            )
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{iceberg_table}_merge/'
            tblproperties('table_type'='ICEBERG')
            """
        )
        arrow_cursor.execute(
            f"""
            INSERT INTO {ENV.schema}.{iceberg_table}_merge (id, col1)
            VALUES (1, 'foobar')
            """
        )
        arrow_cursor.execute(
            f"""
            MERGE INTO {ENV.schema}.{iceberg_table} AS t1
            USING {ENV.schema}.{iceberg_table}_merge AS t2
              ON t1.id = t2.id
            WHEN MATCHED
              THEN UPDATE SET col1 = t2.col1
            """
        )
        arrow_cursor.execute(
            f"""
            SELECT col1
            FROM {ENV.schema}.{iceberg_table}
            WHERE id = 1
            """
        )
        assert arrow_cursor.fetchall() == [("foobar",)]

        arrow_cursor.execute(
            f"""
            VACUUM {ENV.schema}.{iceberg_table}
            """
        )

        arrow_cursor.execute(
            f"""
            DELETE FROM {ENV.schema}.{iceberg_table}
            WHERE id = 2
            """
        )
        arrow_cursor.execute(
            f"""
            SELECT COUNT(*) FROM {ENV.schema}.{iceberg_table}
            """
        )
        assert arrow_cursor.fetchall() == [(1,)]

    def test_execute_with_callback(self, arrow_cursor):
        """Test that callback is invoked with query_id when on_start_query_execution is provided."""
        callback_results = []

        def test_callback(query_id: str):
            callback_results.append(query_id)

        arrow_cursor.execute("SELECT 1", on_start_query_execution=test_callback)

        assert len(callback_results) == 1
        assert callback_results[0] == arrow_cursor.query_id
        assert arrow_cursor.query_id is not None

    @pytest.mark.parametrize(
        "arrow_cursor",
        [
            {
                "cursor_kwargs": {
                    "connect_timeout": 10,
                    "request_timeout": 30,
                }
            }
        ],
        indirect=["arrow_cursor"],
    )
    def test_timeout_parameters(self, arrow_cursor):
        """Test that timeout parameters are correctly passed to ArrowCursor and result set."""
        # Verify timeout parameters are set on cursor
        assert arrow_cursor._connect_timeout == 10
        assert arrow_cursor._request_timeout == 30

        # Execute a simple query to create a result set
        arrow_cursor.execute("SELECT 1")

        # Verify timeout parameters are passed to result set
        assert arrow_cursor.result_set._connect_timeout == 10
        assert arrow_cursor.result_set._request_timeout == 30

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"connect_timeout": 5.5, "request_timeout": 15.5}}],
        indirect=["arrow_cursor"],
    )
    def test_timeout_parameters_float(self, arrow_cursor):
        """Test that timeout parameters accept float values."""
        # Verify float timeout parameters are set on cursor
        assert arrow_cursor._connect_timeout == 5.5
        assert arrow_cursor._request_timeout == 15.5

        # Execute a simple query to create a result set
        arrow_cursor.execute("SELECT 1")

        # Verify float timeout parameters are passed to result set
        assert arrow_cursor.result_set._connect_timeout == 5.5
        assert arrow_cursor.result_set._request_timeout == 15.5

    @pytest.mark.parametrize(
        "arrow_cursor",
        [
            {"cursor_kwargs": {"unload": False}},
            {"cursor_kwargs": {"unload": True}},
        ],
        indirect=["arrow_cursor"],
    )
    def test_null_vs_empty_string(self, arrow_cursor):
        """
        Test NULL vs empty string handling in ArrowCursor.

        Without unload (CSV): Cannot distinguish NULL from empty string (both become '').
        With unload (Parquet): Properly distinguishes NULL from empty string.

        See docs/null_handling.rst for details.
        """
        query = """
        SELECT * FROM (
            VALUES
                (1, '', 'empty_string'),
                (2, CAST(NULL AS VARCHAR), 'null_value'),
                (3, 'hello', 'normal_string'),
                (4, 'N/A', 'na_string'),
                (5, 'NULL', 'null_string_literal')
        ) AS t(id, value, description)
        ORDER BY id
        """
        table = arrow_cursor.execute(query).as_arrow()
        value_col = table.column("value")
        values = value_col.to_pylist()

        if arrow_cursor._unload:
            # With unload (Parquet): NULL and empty string are properly distinguished
            assert values[0] == ""  # Empty string
            assert values[1] is None  # NULL is None
            assert value_col.null_count == 1
        else:
            # Without unload (CSV): Both NULL and empty string become empty string
            assert values[0] == ""  # Empty string
            assert values[1] == ""  # NULL also becomes empty string
            assert value_col.null_count == 0

        # Normal strings are always preserved correctly
        assert values[2] == "hello"
        assert values[3] == "N/A"
        assert values[4] == "NULL"

    @pytest.mark.parametrize(
        "arrow_cursor",
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
        indirect=["arrow_cursor"],
    )
    def test_fetch_all_rows(self, arrow_cursor):
        arrow_cursor.execute("SELECT 1 AS col")
        assert arrow_cursor.fetchall() == [(1,)]
