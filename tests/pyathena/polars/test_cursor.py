# Copyright 2017 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import contextlib
import logging
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from pyathena.error import DatabaseError, DataError, OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.polars.converter import DefaultPolarsTypeConverter
from pyathena.polars.cursor import PolarsCursor
from pyathena.polars.result_set import AthenaPolarsResultSet
from pyathena.util import RetryConfig
from tests import ENV
from tests.pyathena import temporal
from tests.pyathena.conftest import connect
from tests.pyathena.util import (
    AMBIGUOUS_TIME_ZONE_QUERY,
    CONVERTED_VALUES_QUERY,
    CONVERTED_VALUES_ROW,
    RESULT_FILE_VALUES_QUERY,
    cached_file_systems,
)

# Polars 2.0 reads INT96 at the time unit of the read schema; Polars 1.x at nanoseconds.
_POLARS_2 = int(pl.__version__.split(".")[0]) >= 2


class TestPolarsCursor:
    def test_result_set_file_system_not_cached(self, polars_cursor):
        # GH-978: the filesystem that read the CSV results was kept in the fsspec
        # instance cache with the connection, so the connection was never freed.
        polars_cursor.execute("SELECT * FROM one_row")
        assert polars_cursor.fetchall() == [(1,)]
        assert not cached_file_systems(polars_cursor.connection)

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

    def test_as_polars_with_read_kwargs(self, polars_cursor):
        """Read arguments given to execute() replace the ones the result set chooses."""
        df = polars_cursor.execute(
            "SELECT * FROM one_row",
            schema_overrides={"number_of_rows": pl.Utf8},
            storage_options={
                "connection": polars_cursor.connection,
                "skip_instance_cache": True,
            },
        ).as_polars()
        assert df.to_dicts() == [{"number_of_rows": "1"}]

    def test_storage_options_replace_defaults(self, polars_cursor):
        """storage_options given to execute() replace PyAthena's S3 settings for the read.

        A whole CSV result file is read through fsspec with PyAthena's filesystem, which
        the invalid credentials of the options fail.
        """
        with pytest.raises(OperationalError):
            polars_cursor.execute(
                "SELECT * FROM one_row",
                storage_options={
                    "key": "invalid",
                    "secret": "invalid",
                    "skip_instance_cache": True,
                },
            )

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

    @pytest.mark.parametrize(
        ("polars_cursor", "unload"),
        [
            ({"cursor_kwargs": {"unload": False}}, False),
            pytest.param(
                {"cursor_kwargs": {"unload": True}},
                True,
                marks=pytest.mark.skipif(not _POLARS_2, reason="Polars 1.x reads at nanoseconds."),
            ),
        ],
        indirect=["polars_cursor"],
        ids=["csv", "unload"],
    )
    def test_temporal_types(self, polars_cursor, unload):
        """Temporal values convert as the contract of #1212 defines.

        Types that Polars cannot represent stay Athena's text. Polars 1.x reads the
        UNLOAD timestamps at nanoseconds, which the years outside 1677-2262 of the
        query exceed; see test_unload_timestamps_polars_1 for its read.
        """
        types = {"timestamp": pl.Datetime("us"), "date": pl.Date()}
        columns = temporal.columns(unload)
        df = polars_cursor.execute(temporal.query(unload)).as_polars()
        assert dict(df.schema) == {c.name: types.get(c.athena_type, pl.String()) for c in columns}
        assert [df[c.name][0] for c in columns] == [
            c.value if c.athena_type in types else c.text for c in columns
        ]
        row = polars_cursor.fetchone()
        assert row == temporal.fetched_row(unload)
        assert temporal.zones(row) == temporal.zones(temporal.fetched_row(unload))

    @pytest.mark.parametrize("polars_cursor", [{"cursor_kwargs": {"chunksize": 1}}], indirect=True)
    def test_temporal_types_chunked(self, polars_cursor):
        """Chunks of a CSV result have the temporal types and values of a whole read (#1191).

        Timestamps with up to 12 fractional digits are truncated to microseconds.
        """
        types = {"timestamp": pl.Datetime("us"), "date": pl.Date()}
        columns = temporal.columns()
        chunks = list(polars_cursor.execute(temporal.query()).iter_chunks())
        assert len(chunks) == 1
        df = chunks[0]
        assert dict(df.schema) == {c.name: types.get(c.athena_type, pl.String()) for c in columns}
        assert [df[c.name][0] for c in columns] == [
            c.value if c.athena_type in types else c.text for c in columns
        ]

    @pytest.mark.parametrize(
        ("polars_cursor", "kwargs"),
        [
            pytest.param(
                {"cursor_kwargs": {"unload": True}},
                {},
                marks=pytest.mark.skipif(not _POLARS_2, reason="Polars 1.x reads at nanoseconds."),
            ),
            ({"cursor_kwargs": {"unload": True, "chunksize": 1}}, {}),
            pytest.param(
                {"cursor_kwargs": {"unload": True, "chunksize": 1}},
                {"low_memory": True},
                marks=pytest.mark.skipif(not _POLARS_2, reason="Polars 1.x scans at nanoseconds."),
            ),
        ],
        indirect=["polars_cursor"],
        ids=["whole", "chunks", "scanned-chunks"],
    )
    def test_unload_nested_timestamps(self, polars_cursor, kwargs):
        """UNLOAD timestamps, alone and inside arrays, rows, and maps, are microseconds."""
        polars_cursor.execute(temporal.nested_query(), **kwargs)
        df = pl.concat(polars_cursor.iter_chunks()).sort("id")
        us = pl.Datetime("us")
        entries = pl.List(pl.Struct({"key": pl.String, "value": us}))
        # Polars 2.0 reads a map as Map, and Polars 1.x as a List of key-value Structs.
        mp = pl.Map(pl.String, us) if _POLARS_2 else entries
        assert df.drop("id").schema == pl.Schema(
            {
                "ts": us,
                "arr": pl.List(us),
                "st": pl.Struct({"t": us}),
                "*": mp,
                "deep": pl.List(pl.Struct({"m": mp})),
            }
        )
        assert df["ts"].to_list() == temporal.NESTED_VALUES
        assert df["arr"].to_list() == [[v] for v in temporal.NESTED_VALUES]
        assert df["st"].to_list() == [{"t": v} for v in temporal.NESTED_VALUES]
        assert df.get_column("*").cast(entries).to_list() == [
            [{"key": "k", "value": v}] for v in temporal.NESTED_VALUES
        ]
        assert df.get_column("deep").cast(pl.List(pl.Struct({"m": entries}))).to_list() == [
            [{"m": [{"key": "k", "value": v}]}] for v in temporal.NESTED_VALUES
        ]

    @pytest.mark.skipif(_POLARS_2, reason="Polars 2.0 reads INT96 at microseconds.")
    @pytest.mark.parametrize(
        ("polars_cursor", "kwargs", "columns", "warned"),
        [
            ({"cursor_kwargs": {"unload": True}}, {"columns": ["ts"]}, ["ts"], True),
            (
                {"cursor_kwargs": {"unload": True, "chunksize": 1}},
                {"low_memory": True},
                ["id", "ts"],
                True,
            ),
            ({"cursor_kwargs": {"unload": True}}, {"columns": ["id"]}, ["id"], False),
            ({"cursor_kwargs": {"unload": True}}, {}, ["id", "ts"], True),
        ],
        indirect=["polars_cursor"],
        ids=["whole", "scanned-chunks", "no-timestamps", "whole-no-options"],
    )
    def test_unload_timestamps_polars_1(self, polars_cursor, caplog, kwargs, columns, warned):
        """Polars 1.x reads the timestamps itself at nanoseconds, and a warning is logged.

        The timestamps are cast to microseconds; the warning, which says that values
        outside 1677-09-21 to 2262-04-11 are wrong, is logged only when the read has
        timestamps.
        """
        query = "SELECT 1 AS id, TIMESTAMP '2020-01-02 03:04:05.123' AS ts"
        with caplog.at_level(logging.WARNING, logger="pyathena.polars.result_set"):
            polars_cursor.execute(query, **kwargs)
            df = pl.concat(polars_cursor.iter_chunks())
        assert df.columns == columns
        if "ts" in columns:
            assert df.schema["ts"] == pl.Datetime("us")
            assert df["ts"].to_list() == [datetime(2020, 1, 2, 3, 4, 5, 123000)]
        warnings = [
            r
            for r in caplog.records
            if r.name == "pyathena.polars.result_set" and r.levelno == logging.WARNING
        ]
        assert bool(warnings) is warned
        assert all("outside 1677-09-21 to 2262-04-11 are wrong" in r.getMessage() for r in warnings)

    @pytest.mark.skipif(_POLARS_2, reason="Polars 2.0 reads INT96 at microseconds.")
    @pytest.mark.parametrize(
        ("polars_cursor", "kwargs"),
        [
            ({"cursor_kwargs": {"unload": True}}, {}),
            ({"cursor_kwargs": {"unload": True, "chunksize": 1}}, {"low_memory": True}),
        ],
        indirect=["polars_cursor"],
        ids=["whole", "scanned-chunks"],
    )
    def test_unload_nested_timestamps_polars_1(self, polars_cursor, kwargs):
        """Polars 1.x casts the timestamps inside arrays, rows, and maps to microseconds too.

        The values are within 1677-2262, which Polars 1.x reads correctly; see
        test_unload_timestamps_polars_1 for the warning.
        """
        polars_cursor.execute(
            "SELECT id, ts, ARRAY[ts] AS arr, CAST(ROW(ts) AS ROW(t TIMESTAMP)) AS st, "
            "MAP(ARRAY['k'], ARRAY[ts]) AS mp "
            "FROM (VALUES (1, TIMESTAMP '2020-01-02 03:04:05.123'), "
            "(2, CAST(NULL AS TIMESTAMP))) AS t (id, ts)",
            **kwargs,
        )
        df = pl.concat(polars_cursor.iter_chunks()).sort("id")
        us = pl.Datetime("us")
        assert df.drop("id").schema == pl.Schema(
            {
                "ts": us,
                "arr": pl.List(us),
                "st": pl.Struct({"t": us}),
                "mp": pl.List(pl.Struct({"key": pl.String, "value": us})),
            }
        )
        values = [datetime(2020, 1, 2, 3, 4, 5, 123000), None]
        assert df["ts"].to_list() == values
        assert df["arr"].to_list() == [[v] for v in values]
        assert df["st"].to_list() == [{"t": v} for v in values]
        assert df["mp"].to_list() == [[{"key": "k", "value": v}] for v in values]

    @pytest.mark.skipif(not _POLARS_2, reason="Polars 1.x reads INT96 at nanoseconds.")
    @pytest.mark.parametrize(
        ("polars_cursor", "kwargs"),
        [
            ({"cursor_kwargs": {"unload": True}}, {}),
            ({"cursor_kwargs": {"unload": True, "chunksize": 1}}, {"low_memory": True}),
        ],
        indirect=["polars_cursor"],
        ids=["whole", "scanned-chunks"],
    )
    def test_unload_schema_option(self, polars_cursor, kwargs):
        """A schema given to execute() is used as given, a map as a List of Structs too."""
        schema = {"mp": pl.List(pl.Struct({"key": pl.String, "value": pl.Datetime("ms")}))}
        polars_cursor.execute(
            "SELECT MAP(ARRAY['k'], ARRAY[TIMESTAMP '2020-01-02 03:04:05.123']) AS mp",
            schema=schema,
            **kwargs,
        )
        df = pl.concat(polars_cursor.iter_chunks())
        assert df.schema == pl.Schema(schema)
        assert df["mp"].to_list() == [
            [{"key": "k", "value": datetime(2020, 1, 2, 3, 4, 5, 123000)}]
        ]

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

    def test_closed(self, polars_cursor):
        """GH-1207: a closed cursor rejects operations."""
        polars_cursor.execute("SELECT * FROM one_row")
        polars_cursor.close()

        with pytest.raises(ProgrammingError):
            polars_cursor.execute("SELECT * FROM one_row")
        with pytest.raises(ProgrammingError):
            polars_cursor.as_polars()
        with pytest.raises(ProgrammingError):
            polars_cursor.as_arrow()
        with pytest.raises(ProgrammingError):
            next(polars_cursor.iter_chunks())

    def test_execute_interrupted_in_start_callback(self, polars_cursor):
        """GH-1207: an interrupt in on_start_query_execution stops the query."""
        client = polars_cursor.connection.client
        reported = []

        def on_start_query_execution(query_id):
            reported.append(query_id)
            raise KeyboardInterrupt

        with (
            patch.object(client, "stop_query_execution", wraps=client.stop_query_execution) as stop,
            pytest.raises(KeyboardInterrupt),
        ):
            polars_cursor.execute(
                "SELECT * FROM one_row", on_start_query_execution=on_start_query_execution
            )

        stop.assert_called_once_with(QueryExecutionId=reported[0])

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
    def test_executemany(self, polars_cursor, empty_table):
        rows = [(1, "foo"), (2, "bar"), (3, "jim o'rourke")]
        polars_cursor.executemany(
            f"INSERT INTO {empty_table} (a, b) VALUES (%(a)d, %(b)s)",
            [{"a": a, "b": b} for a, b in rows],
        )
        polars_cursor.execute(f"SELECT * FROM {empty_table}")
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

    def test_whole_result_reused(self, polars_cursor):
        """Test that as_polars(), as_arrow(), and iter_chunks() do not consume the rows."""
        polars_cursor.execute("SELECT number FROM (VALUES (1), (2), (3)) AS t(number)")
        df = polars_cursor.as_polars()
        assert df["number"].to_list() == [1, 2, 3]
        assert polars_cursor.as_polars() is df
        assert polars_cursor.as_arrow().column("number").to_pylist() == [1, 2, 3]
        df[0, "number"] = 0

        assert polars_cursor.fetchone() == (1,)
        assert [chunk.height for chunk in polars_cursor.iter_chunks()] == [3]
        assert [chunk.height for chunk in polars_cursor.iter_chunks()] == [3]
        assert polars_cursor.as_polars() is df
        assert polars_cursor.fetchall() == [(2,), (3,)]

    @pytest.mark.parametrize(
        "polars_cursor", [{"cursor_kwargs": {"chunksize": 5}}], indirect=["polars_cursor"]
    )
    def test_close_stops_chunks(self, polars_cursor):
        """Test that closing the result set closes the chunk iterator it returned."""
        polars_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        result_set = polars_cursor.result_set
        chunks = result_set.iter_chunks()
        assert next(chunks).height == 5
        result_set.close()
        assert list(chunks) == []

    @pytest.mark.parametrize(
        ("polars_cursor", "chunksize", "options", "sizes"),
        [
            ({"cursor_kwargs": {"unload": True}}, 1, {}, [1, 1, 1]),
            ({"cursor_kwargs": {"unload": True}}, 2, {}, [2, 1]),
            ({"cursor_kwargs": {"unload": True}}, 10, {}, [3]),
            # Read options given to execute() keep Polars' own scan.
            ({"cursor_kwargs": {"unload": True}}, 2, {"low_memory": True}, [2, 1]),
        ],
        indirect=["polars_cursor"],
    )
    def test_unload_chunks(self, polars_cursor, chunksize, options, sizes):
        """Chunked UNLOAD results match the whole result."""
        query = """
            SELECT * FROM (
              VALUES (1, 1, true, 'a'), (2, NULL, NULL, NULL), (3, 3, false, 'c')
            ) AS t(k, i, b, s)
            """
        whole = polars_cursor.execute(query, **options).as_polars()
        chunks = list(polars_cursor.execute(query, chunksize=chunksize, **options).iter_chunks())
        assert [chunk.height for chunk in chunks] == sizes
        for chunk in chunks:
            assert chunk.schema == whole.schema
        assert pl.concat(chunks).sort("k").equals(whole.sort("k"))

    @pytest.mark.parametrize(
        "polars_cursor", [{"cursor_kwargs": {"unload": True}}], indirect=["polars_cursor"]
    )
    def test_unload_chunks_complex(self, polars_cursor):
        """Chunks of complex-typed UNLOAD results convert as the whole result does."""
        # NOT_SUPPORTED: Unsupported Hive type: time, json
        query = """
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
              ,col_binary
              ,col_array
              ,col_map
              ,col_struct
              ,col_decimal
            FROM one_row_complex
            """
        whole = polars_cursor.execute(query).as_polars()
        chunks = list(polars_cursor.execute(query, chunksize=1).iter_chunks())
        assert len(chunks) == 1
        assert chunks[0].schema == whole.schema
        assert chunks[0].equals(whole)

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

            # Should have multiple chunks, none above the chunk size
            assert len(chunked_dfs) > 1
            assert all(chunk.height <= 25 for chunk in chunked_dfs)

    @pytest.mark.parametrize(
        ("polars_cursor", "ordered"),
        [
            ({"cursor_kwargs": {"unload": False}}, True),
            # The rows of an UNLOAD result can be in any order of its files.
            ({"cursor_kwargs": {"unload": True}}, False),
        ],
        indirect=["polars_cursor"],
    )
    def test_fetch_across_chunks(self, polars_cursor, ordered):
        """The fetch methods continue across chunks and count the rows they return."""
        polars_cursor.execute(
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')) AS t(k, s)",
            chunksize=2,
        )
        rows = [polars_cursor.fetchone()]
        assert polars_cursor.rownumber == 1
        rows += polars_cursor.fetchmany(3)
        assert polars_cursor.rownumber == 4
        rows += polars_cursor.fetchall()
        assert polars_cursor.rownumber == 5
        assert (rows if ordered else sorted(rows)) == [
            (1, "a"),
            (2, "b"),
            (3, "c"),
            (4, "d"),
            (5, "e"),
        ]

    @pytest.mark.parametrize(
        ("polars_cursor", "query", "expected"),
        [
            (
                {"cursor_kwargs": {"unload": True, "chunksize": 5}},
                "SELECT 1 AS a, 'x' AS b",
                [(1, "x")],
            ),
            (
                {"cursor_kwargs": {"unload": True, "chunksize": 5}},
                "SELECT 1 AS a, 'x' AS b WHERE 1 = 0",
                [],
            ),
        ],
        indirect=["polars_cursor"],
    )
    def test_description_with_chunksize_unload(self, polars_cursor, query, expected):
        """Test that chunked UNLOAD results describe the Parquet columns."""
        polars_cursor.execute(query)
        description = [d[:2] for d in polars_cursor.description]
        assert description == ([("a", "integer"), ("b", "varchar")] if expected else [])
        assert polars_cursor.fetchall() == expected

    @pytest.mark.parametrize(
        "polars_cursor",
        [
            {"cursor_kwargs": {"unload": True}},
            {"cursor_kwargs": {"unload": True, "chunksize": 5}},
        ],
        indirect=["polars_cursor"],
    )
    def test_nested_type_description_unload(self, polars_cursor):
        """UNLOAD results describe nested Parquet columns by their outer type."""
        polars_cursor.execute(
            """
            SELECT
              ARRAY[1, 2] AS a
              ,CAST(ROW(ARRAY[1]) AS ROW(x ARRAY(INTEGER))) AS r
              ,MAP(ARRAY['k'], ARRAY[1]) AS m
            """
        )
        # Polars 1.x reads a Parquet map as a list of key-value structs.
        map_type = "map" if hasattr(pl, "Map") else "array"
        description = [d[:2] for d in polars_cursor.description]
        assert description == [("a", "array"), ("r", "row"), ("m", map_type)]

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
        polars_cursor.execute(
            "SELECT 1 AS col, CAST('12:34:56' AS TIME) AS col_time, X'0001' AS col_binary, "
            "json_parse('{\"a\": 1}') AS col_json, CAST('[1, 2]' AS JSON) AS col_json_string, "
            "CAST(NULL AS JSON) AS col_json_null "
            "UNION ALL SELECT 2, CAST('12:34:56' AS TIME), X'0001', json_parse('[1, \"x\"]'), "
            "json_parse('\"s\"'), NULL ORDER BY col"
        )
        assert polars_cursor.fetchall() == [
            (1, datetime(2017, 1, 1, 12, 34, 56).time(), b"\x00\x01", {"a": 1}, "[1, 2]", None),
            (2, datetime(2017, 1, 1, 12, 34, 56).time(), b"\x00\x01", [1, "x"], "s", None),
        ]
        assert polars_cursor.as_polars()["col_json"].dtype == pl.String

        polars_cursor.execute(CONVERTED_VALUES_QUERY)
        assert polars_cursor.fetchall() == [CONVERTED_VALUES_ROW]
        # A Polars Time has no time zone, so the DataFrame keeps the text.
        assert polars_cursor.as_polars()["col_time_tz"].to_list() == ["12:34:56.789+09:00"]
        assert polars_cursor.as_polars()["col_timestamp_tz"].to_list() == [
            "2024-02-29 23:59:58.123 +05:30"
        ]

    def test_ambiguous_time_zone_value(self, polars_cursor):
        """Fetch methods raise DataError for a local time that its zone repeats.

        The table keeps the text, as for every TIMESTAMP WITH TIME ZONE value.
        """
        polars_cursor.execute(AMBIGUOUS_TIME_ZONE_QUERY)
        with pytest.raises(DataError):
            polars_cursor.fetchall()

    @pytest.mark.skipif(not ENV.managed_work_group, reason="AWS_ATHENA_MANAGED_WORKGROUP not set")
    def test_managed_results_match_result_file(self):
        """Managed results have the types and values of the CSV result file.

        The cursor's converter applies to them, including a custom one.
        """
        results = []
        for kwargs in ({}, {"work_group": ENV.managed_work_group, "s3_staging_dir": ""}):
            converter = DefaultPolarsTypeConverter()
            converter.set("varchar", lambda value: value.upper() if value else value)
            with (
                contextlib.closing(connect(**kwargs)) as conn,
                conn.cursor(PolarsCursor, converter=converter) as cursor,
            ):
                cursor.execute(RESULT_FILE_VALUES_QUERY)
                results.append((cursor.as_polars(), cursor.fetchall()))
        (df, rows), (managed_df, managed_rows) = results
        assert managed_df.schema == df.schema
        assert managed_df.equals(df)
        assert managed_rows == rows
        assert rows[0][1] == 'A,"B"\nC'
        # Fractions finer than microseconds are truncated.
        assert (rows[0][5], rows[0][13]) == (datetime(2020, 1, 2, 3, 4, 5, 123456),) * 2

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
    def test_duplicate_column_names(self, polars_cursor):
        polars_cursor.execute(
            "SELECT 1 AS x, 'a' AS x, 'b' AS y, json_parse('[1]') AS j, json_parse('[2]') AS j, "
            "CAST('12:34:56' AS TIME) AS t, 2 AS t"
        )
        assert polars_cursor.fetchall() == [
            (
                1,
                "a",
                "b",
                [1],
                [2],
                datetime(2017, 1, 1, 12, 34, 56).time(),
                2,
            )
        ]
        assert polars_cursor.as_polars().columns == [
            "x",
            "x_duplicated_0",
            "y",
            "j",
            "j_duplicated_0",
            "t",
            "t_duplicated_0",
        ]

    @pytest.mark.parametrize("chunksize", [None, 1])
    def test_new_columns_renaming_first_columns(self, polars_cursor, chunksize):
        """The types stay with the columns when new_columns renames only the first ones."""
        polars_cursor.execute("SELECT '001' AS x, 2 AS y", new_columns=["z"], chunksize=chunksize)
        assert polars_cursor.fetchall() == [("001", 2)]

    def test_new_columns_with_schema_overrides(self, polars_cursor):
        """schema_overrides given with new_columns are keyed by the new names, as in Polars.

        Polars 2.0 rejects new_columns that do not name every column of the file.
        """
        polars_cursor.execute(
            "SELECT '001' AS x, 2 AS y", new_columns=["z", "w"], schema_overrides={"z": pl.String}
        )
        assert polars_cursor.fetchall() == [("001", 2)]

    def test_duplicate_column_names_new_columns(self, polars_cursor):
        """The column types follow the columns that new_columns renames."""
        polars_cursor.execute(
            "SELECT 1 AS x, 2 AS x, 'c' AS y", new_columns=["y", "x_duplicated_0", "x"]
        )
        assert polars_cursor.fetchall() == [(1, 2, "c")]
        assert polars_cursor.as_polars().columns == ["y", "x_duplicated_0", "x"]

    @pytest.mark.parametrize(
        "execute_kwargs",
        [{}, {"block_size": 2048, "cache_type": "none", "s3_max_workers": 3, "chunksize": 20}],
    )
    def test_read_options(self, execute_kwargs):
        """The cursor's read options reach the result set, and execute() overrides them.

        No AWS calls; the query and its result set are mocked.
        """
        cursor_kwargs = {
            "block_size": 1024,
            "cache_type": "bytes",
            "s3_max_workers": 2,
            "chunksize": 10,
        }
        cursor = PolarsCursor(
            connection=MagicMock(),
            converter=MagicMock(),
            formatter=MagicMock(),
            retry_config=RetryConfig(),
            **cursor_kwargs,
        )
        query_execution = MagicMock(state=AthenaQueryExecution.STATE_SUCCEEDED)
        with (
            patch.object(PolarsCursor, "_execute", return_value="query_id"),
            patch.object(PolarsCursor, "_poll", return_value=query_execution),
            patch("pyathena.polars.cursor.AthenaPolarsResultSet") as result_set_class,
        ):
            cursor.execute("SELECT 1", **execute_kwargs)
        kwargs = result_set_class.call_args.kwargs
        expected = {**cursor_kwargs, **execute_kwargs}
        expected["max_workers"] = expected.pop("s3_max_workers")
        assert {key: kwargs[key] for key in expected} == expected
