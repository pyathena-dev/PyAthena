# -*- coding: utf-8 -*-
import contextlib
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal

import polars as pl
import pytest

from pyathena.error import DatabaseError, ProgrammingError
from pyathena.polars.cursor import PolarsCursor
from pyathena.polars.result_set import AthenaPolarsResultSet
from tests import ENV
from tests.pyathena.conftest import connect


class TestPolarsCursor:
    @pytest.mark.parametrize(
        "polars_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["polars_cursor"],
    )
    def test_fetchone(self, polars_cursor):
        polars_cursor.execute("SELECT * FROM one_row")
        assert polars_cursor.rownumber == 0
        assert polars_cursor.fetchone() == (1,)
        assert polars_cursor.rownumber == 1
        assert polars_cursor.fetchone() is None

    @pytest.mark.parametrize(
        "polars_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["polars_cursor"],
    )
    def test_fetchmany(self, polars_cursor):
        polars_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        assert len(polars_cursor.fetchmany(10)) == 10
        assert len(polars_cursor.fetchmany(10)) == 5

    @pytest.mark.parametrize(
        "polars_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["polars_cursor"],
    )
    def test_fetchall(self, polars_cursor):
        polars_cursor.execute("SELECT * FROM one_row")
        assert polars_cursor.fetchall() == [(1,)]
        polars_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        if polars_cursor._unload:
            assert sorted(polars_cursor.fetchall()) == [(i,) for i in range(10000)]
        else:
            assert polars_cursor.fetchall() == [(i,) for i in range(10000)]

    @pytest.mark.parametrize(
        "polars_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["polars_cursor"],
    )
    def test_iterator(self, polars_cursor):
        polars_cursor.execute("SELECT * FROM one_row")
        assert list(polars_cursor) == [(1,)]
        pytest.raises(StopIteration, polars_cursor.__next__)

    @pytest.mark.parametrize(
        "polars_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["polars_cursor"],
    )
    def test_arraysize(self, polars_cursor):
        polars_cursor.arraysize = 5
        polars_cursor.execute("SELECT * FROM many_rows LIMIT 20")
        assert len(polars_cursor.fetchmany()) == 5

    def test_arraysize_default(self, polars_cursor):
        assert polars_cursor.arraysize == AthenaPolarsResultSet.DEFAULT_FETCH_SIZE

    def test_invalid_arraysize(self, polars_cursor):
        polars_cursor.arraysize = 10000
        assert polars_cursor.arraysize == 10000
        with pytest.raises(ProgrammingError):
            polars_cursor.arraysize = -1

    def test_fetch_no_data(self, polars_cursor):
        pytest.raises(ProgrammingError, polars_cursor.fetchone)
        pytest.raises(ProgrammingError, polars_cursor.fetchmany)
        pytest.raises(ProgrammingError, polars_cursor.fetchall)
        pytest.raises(ProgrammingError, polars_cursor.as_polars)

    @pytest.mark.parametrize(
        "polars_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["polars_cursor"],
    )
    def test_as_polars(self, polars_cursor):
        df = polars_cursor.execute("SELECT * FROM one_row").as_polars()
        assert isinstance(df, pl.DataFrame)
        assert df.height == 1
        assert df.width == 1
        assert df.to_dicts() == [{"number_of_rows": 1}]

    @pytest.mark.parametrize(
        "polars_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["polars_cursor"],
    )
    def test_many_as_polars(self, polars_cursor):
        df = polars_cursor.execute("SELECT * FROM many_rows").as_polars()
        assert isinstance(df, pl.DataFrame)
        assert df.height == 10000
        assert df.width == 1

    def test_complex(self, polars_cursor):
        polars_cursor.execute(
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
              ,col_date
              ,col_decimal
            FROM one_row_complex
            """
        )
        assert polars_cursor.description == [
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
            ("col_date", "date", None, None, 0, 0, "UNKNOWN"),
            ("col_decimal", "decimal", None, None, 10, 1, "UNKNOWN"),
        ]
        assert polars_cursor.fetchall() == [
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
                datetime(2017, 1, 2).date(),
                Decimal("0.1"),
            )
        ]

    @pytest.mark.parametrize(
        "polars_cursor",
        [
            {
                "cursor_kwargs": {"unload": True},
            },
        ],
        indirect=["polars_cursor"],
    )
    def test_complex_unload(self, polars_cursor):
        polars_cursor.execute(
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
              ,col_date
              ,col_decimal
            FROM one_row_complex
            """
        )
        assert polars_cursor.description == [
            ("col_boolean", "boolean", None, None, 0, 0, "NULLABLE"),
            ("col_tinyint", "tinyint", None, None, 3, 0, "NULLABLE"),
            ("col_smallint", "smallint", None, None, 5, 0, "NULLABLE"),
            ("col_int", "integer", None, None, 10, 0, "NULLABLE"),
            ("col_bigint", "bigint", None, None, 19, 0, "NULLABLE"),
            ("col_float", "float", None, None, 17, 0, "NULLABLE"),
            ("col_double", "double", None, None, 17, 0, "NULLABLE"),
            ("col_string", "varchar", None, None, 2147483647, 0, "NULLABLE"),
            ("col_varchar", "varchar", None, None, 2147483647, 0, "NULLABLE"),
            ("col_timestamp", "timestamp", None, None, 3, 0, "NULLABLE"),
            ("col_date", "date", None, None, 0, 0, "NULLABLE"),
            ("col_decimal", "decimal", None, None, 10, 1, "NULLABLE"),
        ]
        assert polars_cursor.fetchall() == [
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
                datetime(2017, 1, 2).date(),
                Decimal("0.1"),
            )
        ]

    def test_complex_as_polars(self, polars_cursor):
        df = polars_cursor.execute(
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
              ,col_date
              ,col_decimal
            FROM one_row_complex
            """
        ).as_polars()
        assert isinstance(df, pl.DataFrame)
        assert (df.height, df.width) == (1, 12)
        assert df.schema == {
            "col_boolean": pl.Boolean,
            "col_tinyint": pl.Int8,
            "col_smallint": pl.Int16,
            "col_int": pl.Int32,
            "col_bigint": pl.Int64,
            "col_float": pl.Float32,
            "col_double": pl.Float64,
            "col_string": pl.String,
            "col_varchar": pl.String,
            "col_timestamp": pl.Datetime("us"),
            "col_date": pl.Date,
            "col_decimal": pl.Decimal(precision=10, scale=1),
        }
        assert df.row(0) == (
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
            datetime(2017, 1, 2).date(),
            Decimal("0.1"),
        )

    @pytest.mark.parametrize(
        "polars_cursor",
        [
            {
                "cursor_kwargs": {"unload": True},
            },
        ],
        indirect=["polars_cursor"],
    )
    def test_complex_unload_as_polars(self, polars_cursor):
        df = polars_cursor.execute(
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
              ,col_date
              ,col_decimal
            FROM one_row_complex
            """
        ).as_polars()
        assert isinstance(df, pl.DataFrame)
        assert (df.height, df.width) == (1, 12)
        assert df.row(0) == (
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
            datetime(2017, 1, 2).date(),
            Decimal("0.1"),
        )

    @pytest.mark.parametrize(
        "polars_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["polars_cursor"],
    )
    def test_as_arrow(self, polars_cursor):
        table = polars_cursor.execute("SELECT * FROM one_row").as_arrow()
        assert table.num_rows == 1
        assert table.num_columns == 1

    def test_cancel(self, polars_cursor):
        def cancel(c):
            time.sleep(random.randint(5, 10))
            c.cancel()

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(cancel, polars_cursor)

            pytest.raises(
                DatabaseError,
                lambda: polars_cursor.execute(
                    """
                    SELECT a.a * rand(), b.a * rand()
                    FROM many_rows a
                    CROSS JOIN many_rows b
                    """
                ),
            )

    def test_cancel_initial(self, polars_cursor):
        pytest.raises(ProgrammingError, polars_cursor.cancel)

    def test_open_close(self):
        with contextlib.closing(connect()) as conn, conn.cursor(PolarsCursor):
            pass

    def test_no_ops(self):
        conn = connect()
        cursor = conn.cursor(PolarsCursor)
        cursor.close()
        conn.close()

    @pytest.mark.parametrize(
        "polars_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["polars_cursor"],
    )
    def test_show_columns(self, polars_cursor):
        polars_cursor.execute("SHOW COLUMNS IN one_row")
        assert polars_cursor.description == [("field", "string", None, None, 0, 0, "UNKNOWN")]
        assert polars_cursor.fetchall() == [("number_of_rows      ",)]

    @pytest.mark.parametrize(
        "polars_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["polars_cursor"],
    )
    def test_empty_result(self, polars_cursor):
        table = "test_polars_cursor_empty_result_" + "".join(
            random.choices(string.ascii_lowercase + string.digits, k=10)
        )
        df = polars_cursor.execute(
            f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS
            {ENV.schema}.{table} (number_of_rows INT)
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            LINES TERMINATED BY '\n' STORED AS TEXTFILE
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table}/'
            """
        ).as_polars()
        assert df.height == 0
        assert df.width == 0

    @pytest.mark.parametrize(
        "polars_cursor",
        [
            {
                "cursor_kwargs": {"unload": True},
            },
        ],
        indirect=["polars_cursor"],
    )
    def test_empty_result_unload(self, polars_cursor):
        df = polars_cursor.execute(
            """
            SELECT * FROM one_row LIMIT 0
            """
        ).as_polars()
        assert df.height == 0
        assert df.width == 0

    @pytest.mark.parametrize(
        "polars_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["polars_cursor"],
    )
    def test_executemany(self, polars_cursor):
        rows = [(1, "foo"), (2, "bar"), (3, "jim o'rourke")]
        table_name = f"execute_many_polars{'_unload' if polars_cursor._unload else ''}"
        polars_cursor.executemany(
            f"INSERT INTO {table_name} (a, b) VALUES (%(a)d, %(b)s)",
            [{"a": a, "b": b} for a, b in rows],
        )
        polars_cursor.execute(f"SELECT * FROM {table_name}")
        assert sorted(polars_cursor.fetchall()) == list(rows)

    @pytest.mark.parametrize(
        "polars_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["polars_cursor"],
    )
    def test_executemany_fetch(self, polars_cursor):
        polars_cursor.executemany("SELECT %(x)d AS x FROM one_row", [{"x": i} for i in range(1, 2)])
        # Operations that have result sets are not allowed with executemany.
        pytest.raises(ProgrammingError, polars_cursor.fetchall)
        pytest.raises(ProgrammingError, polars_cursor.fetchmany)
        pytest.raises(ProgrammingError, polars_cursor.fetchone)
        pytest.raises(ProgrammingError, polars_cursor.as_polars)

    def test_execute_with_callback(self, polars_cursor):
        """Test that callback is invoked with query_id when on_start_query_execution is provided."""
        callback_results = []

        def test_callback(query_id: str):
            callback_results.append(query_id)

        polars_cursor.execute("SELECT 1", on_start_query_execution=test_callback)

        assert len(callback_results) == 1
        assert callback_results[0] == polars_cursor.query_id
        assert polars_cursor.query_id is not None

    def test_iter_chunks(self):
        """Test chunked iteration over query results."""
        with contextlib.closing(connect(schema_name=ENV.schema)) as conn:
            cursor = conn.cursor(PolarsCursor, chunksize=5)
            cursor.execute("SELECT * FROM many_rows LIMIT 15")
            chunks = list(cursor.iter_chunks())
            assert len(chunks) > 0
            total_rows = sum(chunk.height for chunk in chunks)
            assert total_rows == 15
            for chunk in chunks:
                assert isinstance(chunk, pl.DataFrame)

    def test_iter_chunks_without_chunksize(self, polars_cursor):
        """Test that iter_chunks works without chunksize, yielding entire DataFrame."""
        polars_cursor.execute("SELECT * FROM one_row")
        chunks = list(polars_cursor.iter_chunks())
        # Without chunksize, yields entire DataFrame as single chunk
        assert len(chunks) == 1
        assert isinstance(chunks[0], pl.DataFrame)
        assert chunks[0].height == 1

    def test_iter_chunks_many_rows(self):
        """Test chunked iteration with many rows."""
        with contextlib.closing(connect(schema_name=ENV.schema)) as conn:
            cursor = conn.cursor(PolarsCursor, chunksize=1000)
            cursor.execute("SELECT * FROM many_rows")
            chunks = list(cursor.iter_chunks())
            total_rows = sum(chunk.height for chunk in chunks)
            assert total_rows == 10000
            assert len(chunks) >= 10  # At least 10 chunks with chunksize=1000

    @pytest.mark.parametrize(
        "polars_cursor",
        [
            {
                "cursor_kwargs": {"unload": True, "chunksize": 5},
            },
        ],
        indirect=["polars_cursor"],
    )
    def test_iter_chunks_unload(self, polars_cursor):
        """Test chunked iteration with UNLOAD (Parquet)."""
        polars_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        chunks = list(polars_cursor.iter_chunks())
        assert len(chunks) > 0
        total_rows = sum(chunk.height for chunk in chunks)
        assert total_rows == 15
        for chunk in chunks:
            assert isinstance(chunk, pl.DataFrame)

    def test_iter_chunks_data_consistency(self):
        """Test that chunked and regular reading produce the same data."""
        with contextlib.closing(connect(schema_name=ENV.schema)) as conn:
            # Regular reading (no chunksize)
            regular_cursor = conn.cursor(PolarsCursor)
            regular_cursor.execute("SELECT * FROM many_rows LIMIT 100")
            regular_df = regular_cursor.as_polars()

            # Chunked reading
            chunked_cursor = conn.cursor(PolarsCursor, chunksize=25)
            chunked_cursor.execute("SELECT * FROM many_rows LIMIT 100")
            chunked_dfs = list(chunked_cursor.iter_chunks())

            # Combine chunks
            combined_df = pl.concat(chunked_dfs)

            # Should have the same data (sort for comparison)
            assert regular_df.sort("a").equals(combined_df.sort("a"))

            # Should have multiple chunks
            assert len(chunked_dfs) > 1

    def test_iter_chunks_chunk_sizes(self):
        """Test that chunks have correct sizes."""
        with contextlib.closing(connect(schema_name=ENV.schema)) as conn:
            cursor = conn.cursor(PolarsCursor, chunksize=10)
            cursor.execute("SELECT * FROM many_rows LIMIT 50")

            chunk_sizes = []
            total_rows = 0

            for chunk in cursor.iter_chunks():
                chunk_size = chunk.height
                chunk_sizes.append(chunk_size)
                total_rows += chunk_size

                # Each chunk should not exceed chunksize
                assert chunk_size <= 10

            # Should have processed all 50 rows
            assert total_rows == 50

            # Should have multiple chunks
            assert len(chunk_sizes) > 1

    def test_fetchone_with_chunksize(self):
        """Test that fetchone works correctly with chunksize enabled."""
        with contextlib.closing(connect(schema_name=ENV.schema)) as conn:
            cursor = conn.cursor(PolarsCursor, chunksize=5)
            cursor.execute("SELECT * FROM many_rows LIMIT 15")

            rows = []
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                rows.append(row)

            assert len(rows) == 15

    def test_fetchmany_with_chunksize(self):
        """Test that fetchmany works correctly with chunksize enabled."""
        with contextlib.closing(connect(schema_name=ENV.schema)) as conn:
            cursor = conn.cursor(PolarsCursor, chunksize=5)
            cursor.execute("SELECT * FROM many_rows LIMIT 15")

            batch1 = cursor.fetchmany(10)
            batch2 = cursor.fetchmany(10)

            assert len(batch1) == 10
            assert len(batch2) == 5

    def test_fetchall_with_chunksize(self):
        """Test that fetchall works correctly with chunksize enabled."""
        with contextlib.closing(connect(schema_name=ENV.schema)) as conn:
            cursor = conn.cursor(PolarsCursor, chunksize=5)
            cursor.execute("SELECT * FROM many_rows LIMIT 15")

            rows = cursor.fetchall()
            assert len(rows) == 15

    def test_iterator_with_chunksize(self):
        """Test that cursor iteration works correctly with chunksize enabled."""
        with contextlib.closing(connect(schema_name=ENV.schema)) as conn:
            cursor = conn.cursor(PolarsCursor, chunksize=5)
            cursor.execute("SELECT * FROM many_rows LIMIT 15")

            rows = list(cursor)
            assert len(rows) == 15

    @pytest.mark.parametrize(
        "polars_cursor",
        [
            {
                "cursor_kwargs": {"unload": True, "chunksize": 5},
            },
        ],
        indirect=["polars_cursor"],
    )
    def test_fetchone_with_chunksize_unload(self, polars_cursor):
        """Test that fetchone works correctly with chunksize and unload enabled."""
        polars_cursor.execute("SELECT * FROM many_rows LIMIT 15")

        rows = []
        while True:
            row = polars_cursor.fetchone()
            if row is None:
                break
            rows.append(row)

        assert len(rows) == 15

    @pytest.mark.parametrize(
        "polars_cursor",
        [
            {
                "cursor_kwargs": {"unload": True, "chunksize": 5},
            },
        ],
        indirect=["polars_cursor"],
    )
    def test_iterator_with_chunksize_unload(self, polars_cursor):
        """Test that cursor iteration works with chunksize and unload enabled."""
        polars_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        rows = list(polars_cursor)
        assert len(rows) == 15

    @pytest.mark.parametrize(
        "polars_cursor",
        [
            {"cursor_kwargs": {"unload": False}},
            {"cursor_kwargs": {"unload": True}},
        ],
        indirect=["polars_cursor"],
    )
    def test_null_vs_empty_string(self, polars_cursor):
        """
        Test NULL vs empty string handling in PolarsCursor.

        PolarsCursor can properly distinguish NULL from empty string in both CSV and Parquet modes.
        This is unique among file-based cursors because Polars' CSV parser correctly interprets
        unquoted empty values as NULL.

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
        df = polars_cursor.execute(query).as_polars()
        is_null = df["value"].is_null().to_list()
        values = df["value"].to_list()

        # Both CSV and Parquet modes properly distinguish NULL from empty string
        assert not is_null[0]  # Empty string is NOT null
        assert values[0] == ""  # Empty string is preserved

        assert is_null[1]  # NULL IS null
        assert values[1] is None  # NULL is None

        # Normal strings are preserved correctly
        assert values[2] == "hello"
        assert values[3] == "N/A"
        assert values[4] == "NULL"

    @pytest.mark.parametrize(
        "polars_cursor",
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
        indirect=["polars_cursor"],
    )
    def test_fetch_all_rows(self, polars_cursor):
        polars_cursor.execute("SELECT 1 AS col")
        assert polars_cursor.fetchall() == [(1,)]
