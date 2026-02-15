# -*- coding: utf-8 -*-
import contextlib
import math
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal
from unittest.mock import PropertyMock, patch

import numpy as np
import pandas as pd
import pytest

from pyathena.error import DatabaseError, ProgrammingError
from pyathena.pandas.cursor import PandasCursor
from pyathena.pandas.result_set import AthenaPandasResultSet, PandasDataFrameIterator
from tests import ENV
from tests.pyathena.conftest import connect


class TestPandasCursor:
    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine, chunksize",
        [
            ({"cursor_kwargs": {"unload": False}}, "auto", None),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000_000),
            ({"cursor_kwargs": {"unload": True}}, "auto", None),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow", None),
        ],
        indirect=["pandas_cursor"],
    )
    def test_fetchone(self, pandas_cursor, parquet_engine, chunksize):
        pandas_cursor.execute("SELECT * FROM one_row", engine=parquet_engine, chunksize=chunksize)
        assert pandas_cursor.rownumber == 0
        assert pandas_cursor.fetchone() == (1,)
        assert pandas_cursor.rownumber == 1
        assert pandas_cursor.fetchone() is None

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine, chunksize",
        [
            ({"cursor_kwargs": {"unload": False}}, "auto", None),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000_000),
            ({"cursor_kwargs": {"unload": True}}, "auto", None),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow", None),
        ],
        indirect=["pandas_cursor"],
    )
    def test_fetchmany(self, pandas_cursor, parquet_engine, chunksize):
        pandas_cursor.execute(
            "SELECT * FROM many_rows LIMIT 15",
            engine=parquet_engine,
            chunksize=chunksize,
        )
        assert len(pandas_cursor.fetchmany(10)) == 10
        assert len(pandas_cursor.fetchmany(10)) == 5

    @pytest.mark.parametrize(
        "pandas_cursor, chunksize",
        [
            ({}, None),
            ({}, 1_000),
            ({}, 1_000_000),
        ],
        indirect=["pandas_cursor"],
    )
    def test_get_chunk(self, pandas_cursor, chunksize):
        df = pandas_cursor.execute(
            "SELECT * FROM many_rows LIMIT 15",
            chunksize=chunksize,
        ).as_pandas()
        if chunksize:
            assert len(df.get_chunk(10)) == 10
            assert len(df.get_chunk(10)) == 5
            pytest.raises(StopIteration, lambda: df.get_chunk(10))
        else:
            assert len(df) == 15

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine, chunksize",
        [
            ({"cursor_kwargs": {"unload": False}}, "auto", None),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000_000),
            ({"cursor_kwargs": {"unload": True}}, "auto", None),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow", None),
        ],
        indirect=["pandas_cursor"],
    )
    def test_fetchall(self, pandas_cursor, parquet_engine, chunksize):
        pandas_cursor.execute("SELECT * FROM one_row", engine=parquet_engine, chunksize=chunksize)
        assert pandas_cursor.fetchall() == [(1,)]
        pandas_cursor.execute("SELECT a FROM many_rows ORDER BY a", engine=parquet_engine)
        assert pandas_cursor.fetchall() == [(i,) for i in range(10000)]

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine, chunksize",
        [
            ({"cursor_kwargs": {"unload": False}}, "auto", None),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000_000),
            ({"cursor_kwargs": {"unload": True}}, "auto", None),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow", None),
        ],
        indirect=["pandas_cursor"],
    )
    def test_iterator(self, pandas_cursor, parquet_engine, chunksize):
        pandas_cursor.execute("SELECT * FROM one_row", engine=parquet_engine, chunksize=chunksize)
        assert list(pandas_cursor) == [(1,)]
        pytest.raises(StopIteration, pandas_cursor.__next__)

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine, chunksize",
        [
            ({"cursor_kwargs": {"unload": False}}, "auto", None),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000_000),
            ({"cursor_kwargs": {"unload": True}}, "auto", None),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow", None),
        ],
        indirect=["pandas_cursor"],
    )
    def test_arraysize(self, pandas_cursor, parquet_engine, chunksize):
        pandas_cursor.arraysize = 5
        pandas_cursor.execute(
            "SELECT * FROM many_rows LIMIT 20",
            engine=parquet_engine,
            chunksize=chunksize,
        )
        assert len(pandas_cursor.fetchmany()) == 5

    def test_arraysize_default(self, pandas_cursor):
        assert pandas_cursor.arraysize == AthenaPandasResultSet.DEFAULT_FETCH_SIZE

    def test_invalid_arraysize(self, pandas_cursor):
        pandas_cursor.arraysize = 10000
        assert pandas_cursor.arraysize == 10000
        with pytest.raises(ProgrammingError):
            pandas_cursor.arraysize = -1

    @pytest.mark.parametrize(
        "pandas_cursor, chunksize",
        [
            ({}, None),
            ({}, 1_000),
            ({}, 1_000_000),
        ],
        indirect=["pandas_cursor"],
    )
    def test_complex(self, pandas_cursor, chunksize):
        pandas_cursor.execute(
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
            """,
            chunksize=chunksize,
        )
        assert pandas_cursor.description == [
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
        assert pandas_cursor.fetchall() == [
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
                pd.Timestamp(2017, 1, 1, 0, 0, 0),
                datetime(2017, 1, 1, 0, 0, 0).time(),
                pd.Timestamp(2017, 1, 2),
                b"123",
                "[1, 2]",
                [1, 2],
                "{1=2, 3=4}",
                {"1": 2, "3": 4},
                "{a=1, b=2}",
                Decimal("0.1"),
            )
        ]

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine",
        [
            ({"cursor_kwargs": {"unload": True}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow"),
        ],
        indirect=["pandas_cursor"],
    )
    def test_complex_unload_pyarrow(self, pandas_cursor, parquet_engine):
        # NOT_SUPPORTED: Unsupported Hive type: time, json
        pandas_cursor.execute(
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
              ,col_binary
              ,col_array
              ,col_map
              ,col_struct
              ,col_decimal
            FROM one_row_complex
            """,
            engine=parquet_engine,
        )
        assert pandas_cursor.description == [
            ("col_boolean", "boolean", None, None, 0, 0, "NULLABLE"),
            (
                "col_tinyint",
                "tinyint",
                None,
                None,
                3,
                0,
                "NULLABLE",
            ),
            (
                "col_smallint",
                "smallint",
                None,
                None,
                5,
                0,
                "NULLABLE",
            ),
            ("col_int", "integer", None, None, 10, 0, "NULLABLE"),
            ("col_bigint", "bigint", None, None, 19, 0, "NULLABLE"),
            ("col_float", "float", None, None, 17, 0, "NULLABLE"),
            ("col_double", "double", None, None, 17, 0, "NULLABLE"),
            ("col_string", "varchar", None, None, 2147483647, 0, "NULLABLE"),
            ("col_varchar", "varchar", None, None, 2147483647, 0, "NULLABLE"),
            ("col_timestamp", "timestamp", None, None, 3, 0, "NULLABLE"),
            ("col_date", "date", None, None, 0, 0, "NULLABLE"),
            ("col_binary", "varbinary", None, None, 1073741824, 0, "NULLABLE"),
            ("col_array", "array", None, None, 0, 0, "NULLABLE"),
            ("col_map", "map", None, None, 0, 0, "NULLABLE"),
            ("col_struct", "row", None, None, 0, 0, "NULLABLE"),
            ("col_decimal", "decimal", None, None, 10, 1, "NULLABLE"),
        ]
        rows = [
            (
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
                row[9],
                row[10],
                row[11],
                list(row[12]),
                row[13],
                row[14],
                row[15],
            )
            for row in pandas_cursor.fetchall()
        ]
        assert rows == [
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
                pd.Timestamp(2017, 1, 1, 0, 0, 0),
                datetime(2017, 1, 2).date(),
                b"123",
                # ValueError: The truth value of an array with more than one element is ambiguous.
                # Use a.any() or a.all()
                list(np.array([1, 2], dtype=np.int32)),
                [(1, 2), (3, 4)],
                {"a": 1, "b": 2},
                Decimal("0.1"),
            )
        ]

    def test_fetch_no_data(self, pandas_cursor):
        pytest.raises(ProgrammingError, pandas_cursor.fetchone)
        pytest.raises(ProgrammingError, pandas_cursor.fetchmany)
        pytest.raises(ProgrammingError, pandas_cursor.fetchall)
        pytest.raises(ProgrammingError, pandas_cursor.as_pandas)

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine, chunksize",
        [
            ({"cursor_kwargs": {"unload": False}}, "auto", None),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000_000),
            ({"cursor_kwargs": {"unload": True}}, "auto", None),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow", None),
        ],
        indirect=["pandas_cursor"],
    )
    def test_as_pandas(self, pandas_cursor, parquet_engine, chunksize):
        df = pandas_cursor.execute(
            "SELECT * FROM one_row", engine=parquet_engine, chunksize=chunksize
        ).as_pandas()
        if chunksize:
            df = pd.concat((d for d in df), ignore_index=True)
        assert df.shape[0] == 1
        assert df.shape[1] == 1
        assert [(row["number_of_rows"],) for _, row in df.iterrows()] == [(1,)]

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine, chunksize",
        [
            ({"cursor_kwargs": {"unload": False}}, "auto", None),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000_000),
            ({"cursor_kwargs": {"unload": True}}, "auto", None),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow", None),
        ],
        indirect=["pandas_cursor"],
    )
    def test_many_as_pandas(self, pandas_cursor, parquet_engine, chunksize):
        df = pandas_cursor.execute(
            "SELECT * FROM many_rows", engine=parquet_engine, chunksize=chunksize
        ).as_pandas()
        if chunksize:
            df = pd.concat((d for d in df), ignore_index=True)
        assert df.shape[0] == 10000
        assert df.shape[1] == 1
        assert [(row["a"],) for _, row in df.iterrows()] == [(i,) for i in range(10000)]

    @pytest.mark.parametrize(
        "pandas_cursor, chunksize",
        [
            ({}, None),
            ({}, 1_000),
            ({}, 1_000_000),
        ],
        indirect=["pandas_cursor"],
    )
    def test_complex_as_pandas(self, pandas_cursor, chunksize):
        df = pandas_cursor.execute(
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
            """,
            chunksize=chunksize,
        ).as_pandas()
        if chunksize:
            df = pd.concat((d for d in df), ignore_index=True)
        assert df.shape[0] == 1
        assert df.shape[1] == 19
        dtypes = (
            df["col_boolean"].dtype.type,
            df["col_tinyint"].dtype.type,
            df["col_smallint"].dtype.type,
            df["col_int"].dtype.type,
            df["col_bigint"].dtype.type,
            df["col_float"].dtype.type,
            df["col_double"].dtype.type,
            df["col_string"].dtype.type,
            df["col_varchar"].dtype.type,
            df["col_timestamp"].dtype.type,
            df["col_time"].dtype.type,
            df["col_date"].dtype.type,
            df["col_binary"].dtype.type,
            df["col_array"].dtype.type,
            df["col_array_json"].dtype.type,
            df["col_map"].dtype.type,
            df["col_map_json"].dtype.type,
            df["col_struct"].dtype.type,
            df["col_decimal"].dtype.type,
        )
        assert dtypes == (
            np.bool_,
            np.int64,
            np.int64,
            np.int64,
            np.int64,
            np.float64,
            np.float64,
            np.object_,
            np.object_,
            np.datetime64,
            np.object_,
            np.datetime64,
            np.object_,
            np.object_,
            np.object_,
            np.object_,
            np.object_,
            np.object_,
            np.object_,
        )
        rows = [
            (
                row["col_boolean"],
                row["col_tinyint"],
                row["col_smallint"],
                row["col_int"],
                row["col_bigint"],
                row["col_float"],
                row["col_double"],
                row["col_string"],
                row["col_varchar"],
                row["col_timestamp"],
                row["col_time"],
                row["col_date"],
                row["col_binary"],
                row["col_array"],
                row["col_array_json"],
                row["col_map"],
                row["col_map_json"],
                row["col_struct"],
                row["col_decimal"],
            )
            for _, row in df.iterrows()
        ]
        assert rows == [
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
                pd.Timestamp(2017, 1, 1, 0, 0, 0),
                datetime(2017, 1, 1, 0, 0, 0).time(),
                pd.Timestamp(2017, 1, 2),
                b"123",
                "[1, 2]",
                [1, 2],
                "{1=2, 3=4}",
                {"1": 2, "3": 4},
                "{a=1, b=2}",
                Decimal("0.1"),
            )
        ]

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine",
        [
            ({"cursor_kwargs": {"unload": True}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow"),
        ],
        indirect=["pandas_cursor"],
    )
    def test_complex_unload_as_pandas_pyarrow(self, pandas_cursor, parquet_engine):
        # NOT_SUPPORTED: Unsupported Hive type: time, json
        df = pandas_cursor.execute(
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
              ,col_binary
              ,col_array
              ,col_map
              ,col_struct
              ,col_decimal
            FROM one_row_complex
            """,
            engine=parquet_engine,
        ).as_pandas()
        assert df.shape[0] == 1
        assert df.shape[1] == 16
        dtypes = (
            df["col_boolean"].dtype.type,
            df["col_tinyint"].dtype.type,
            df["col_smallint"].dtype.type,
            df["col_int"].dtype.type,
            df["col_bigint"].dtype.type,
            df["col_float"].dtype.type,
            df["col_double"].dtype.type,
            df["col_string"].dtype.type,
            df["col_varchar"].dtype.type,
            df["col_timestamp"].dtype.type,
            df["col_date"].dtype.type,
            df["col_binary"].dtype.type,
            df["col_array"].dtype.type,
            df["col_map"].dtype.type,
            df["col_struct"].dtype.type,
            df["col_decimal"].dtype.type,
        )
        assert dtypes == (
            np.bool_,
            np.int8,
            np.int16,
            np.int32,
            np.int64,
            np.float32,
            np.float64,
            np.object_,
            np.object_,
            np.datetime64,
            np.object_,
            np.object_,
            np.object_,
            np.object_,
            np.object_,
            np.object_,
        )
        rows = [
            (
                row["col_boolean"],
                row["col_tinyint"],
                row["col_smallint"],
                row["col_int"],
                row["col_bigint"],
                row["col_float"],
                row["col_double"],
                row["col_string"],
                row["col_varchar"],
                row["col_timestamp"],
                row["col_date"],
                row["col_binary"],
                list(row["col_array"]),
                row["col_map"],
                row["col_struct"],
                row["col_decimal"],
            )
            for _, row in df.iterrows()
        ]
        assert rows == [
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
                pd.Timestamp(2017, 1, 1, 0, 0, 0),
                datetime(2017, 1, 2).date(),
                b"123",
                # ValueError: The truth value of an array with more than one element is ambiguous.
                # Use a.any() or a.all()
                list(np.array([1, 2], dtype=np.int32)),
                [(1, 2), (3, 4)],
                {"a": 1, "b": 2},
                Decimal("0.1"),
            )
        ]

    def test_cancel(self, pandas_cursor):
        def cancel(c):
            time.sleep(random.randint(5, 10))
            c.cancel()

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(cancel, pandas_cursor)

            pytest.raises(
                DatabaseError,
                lambda: pandas_cursor.execute(
                    """
                    SELECT a.a * rand(), b.a * rand()
                    FROM many_rows a
                    CROSS JOIN many_rows b
                    """
                ),
            )

    def test_cancel_initial(self, pandas_cursor):
        pytest.raises(ProgrammingError, pandas_cursor.cancel)

    def test_open_close(self):
        with contextlib.closing(connect()) as conn, conn.cursor(PandasCursor):
            pass

    def test_no_ops(self):
        conn = connect()
        cursor = conn.cursor(PandasCursor)
        cursor.close()
        conn.close()

    def test_get_optimal_csv_engine(self):
        """Test _get_optimal_csv_engine method behavior."""

        # Mock the parent class initialization
        with patch("pyathena.pandas.result_set.AthenaResultSet.__init__"):
            result_set = AthenaPandasResultSet.__new__(AthenaPandasResultSet)
            result_set._engine = "auto"
            result_set._chunksize = None  # No chunking by default

            # Small file should prefer C engine (compatibility-first approach)
            engine = result_set._get_optimal_csv_engine(1024)  # 1KB
            assert engine == "c"

            # Large file should prefer Python engine (avoid C parser int32 limits)
            engine = result_set._get_optimal_csv_engine(100 * 1024 * 1024)  # 100MB
            assert engine == "python"

    def test_auto_determine_chunksize(self):
        """Test _auto_determine_chunksize method behavior."""

        # Mock the parent class initialization
        with patch("pyathena.pandas.result_set.AthenaResultSet.__init__"):
            result_set = AthenaPandasResultSet.__new__(AthenaPandasResultSet)

            # Small file - no chunking
            chunksize = result_set._auto_determine_chunksize(10 * 1024 * 1024)  # 10MB
            assert chunksize is None

            # Medium file - 50K chunks
            chunksize = result_set._auto_determine_chunksize(150 * 1024 * 1024)  # 150MB
            assert chunksize == 50_000

            # Large file - 100K chunks
            chunksize = result_set._auto_determine_chunksize(300 * 1024 * 1024)  # 300MB
            assert chunksize == 100_000

    def test_chunksize_configuration_customization(self):
        """Test that users can customize chunksize configuration."""

        # Mock the parent class initialization
        with patch("pyathena.pandas.result_set.AthenaResultSet.__init__"):
            result_set = AthenaPandasResultSet.__new__(AthenaPandasResultSet)

            # Test default behavior
            chunksize = result_set._auto_determine_chunksize(200 * 1024 * 1024)  # 200MB
            assert chunksize == 100_000  # Default AUTO_CHUNK_SIZE_LARGE

            # Customize chunk size
            result_set.AUTO_CHUNK_SIZE_LARGE = 250_000
            chunksize = result_set._auto_determine_chunksize(200 * 1024 * 1024)  # 200MB
            assert chunksize == 250_000

            # Customize threshold
            result_set.LARGE_FILE_THRESHOLD_BYTES = 100 * 1024 * 1024  # 100MB
            chunksize = result_set._auto_determine_chunksize(80 * 1024 * 1024)  # 80MB
            assert chunksize is None  # Now below threshold

    def test_get_csv_engine_explicit_specification(self):
        """Test _get_csv_engine respects explicit engine specification."""

        # Mock the parent class initialization
        with patch("pyathena.pandas.result_set.AthenaResultSet.__init__"):
            result_set = AthenaPandasResultSet.__new__(AthenaPandasResultSet)
            result_set._chunksize = None  # Default values
            result_set._quoting = 1

            # Test C engine specification
            result_set._engine = "c"
            engine = result_set._get_csv_engine()
            assert engine == "c"

            # Test Python engine specification
            result_set._engine = "python"
            engine = result_set._get_csv_engine()
            assert engine == "python"

            # Test PyArrow specification (when available and compatible)
            result_set._engine = "pyarrow"
            with (
                patch.object(result_set, "_get_available_engine", return_value="pyarrow"),
                patch.object(
                    type(result_set), "converters", new_callable=PropertyMock, return_value={}
                ),
            ):
                engine = result_set._get_csv_engine()
                assert engine == "pyarrow"

            # Test PyArrow with incompatible chunksize (via parameter)
            with (
                patch.object(result_set, "_get_available_engine", return_value="pyarrow"),
                patch.object(
                    type(result_set), "converters", new_callable=PropertyMock, return_value={}
                ),
                patch.object(result_set, "_get_optimal_csv_engine", return_value="c") as mock_opt,
            ):
                engine = result_set._get_csv_engine(chunksize=1000)
                assert engine == "c"
                mock_opt.assert_called_once()

            # Test PyArrow with incompatible chunksize (via instance variable)
            result_set._chunksize = 1000
            with (
                patch.object(result_set, "_get_available_engine", return_value="pyarrow"),
                patch.object(
                    type(result_set), "converters", new_callable=PropertyMock, return_value={}
                ),
                patch.object(result_set, "_get_optimal_csv_engine", return_value="c") as mock_opt,
            ):
                engine = result_set._get_csv_engine()
                assert engine == "c"
                mock_opt.assert_called_once()

            # Test PyArrow with incompatible quoting
            result_set._chunksize = None
            result_set._quoting = 0  # Non-default quoting
            with (
                patch.object(result_set, "_get_available_engine", return_value="pyarrow"),
                patch.object(
                    type(result_set), "converters", new_callable=PropertyMock, return_value={}
                ),
                patch.object(result_set, "_get_optimal_csv_engine", return_value="c") as mock_opt,
            ):
                engine = result_set._get_csv_engine()
                assert engine == "c"
                mock_opt.assert_called_once()

            # Test PyArrow with incompatible converters
            result_set._quoting = 1  # Reset to default
            with (
                patch.object(result_set, "_get_available_engine", return_value="pyarrow"),
                patch.object(
                    type(result_set),
                    "converters",
                    new_callable=PropertyMock,
                    return_value={"col1": str},
                ),
                patch.object(result_set, "_get_optimal_csv_engine", return_value="c") as mock_opt,
            ):
                engine = result_set._get_csv_engine()
                assert engine == "c"
                mock_opt.assert_called_once()

            # Test PyArrow specification (when unavailable)
            with (
                patch.object(result_set, "_get_available_engine", side_effect=ImportError),
                patch.object(
                    type(result_set), "converters", new_callable=PropertyMock, return_value={}
                ),
                patch.object(result_set, "_get_optimal_csv_engine", return_value="c") as mock_opt,
            ):
                engine = result_set._get_csv_engine()
                assert engine == "c"
                mock_opt.assert_called_once()

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine, chunksize",
        [
            ({"cursor_kwargs": {"unload": False}}, "auto", None),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000_000),
            ({"cursor_kwargs": {"unload": True}}, "auto", None),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow", None),
        ],
        indirect=["pandas_cursor"],
    )
    def test_show_columns(self, pandas_cursor, parquet_engine, chunksize):
        pandas_cursor.execute("SHOW COLUMNS IN one_row", engine=parquet_engine, chunksize=chunksize)
        assert pandas_cursor.description == [("field", "string", None, None, 0, 0, "UNKNOWN")]
        assert pandas_cursor.fetchall() == [("number_of_rows      ",)]

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine, chunksize",
        [
            ({"cursor_kwargs": {"unload": False}}, "auto", None),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000_000),
            ({"cursor_kwargs": {"unload": True}}, "auto", None),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow", None),
        ],
        indirect=["pandas_cursor"],
    )
    def test_empty_result_ddl(self, pandas_cursor, parquet_engine, chunksize):
        table = "test_pandas_cursor_empty_result_" + "".join(
            random.choices(string.ascii_lowercase + string.digits, k=10)
        )
        df = pandas_cursor.execute(
            f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS
            {ENV.schema}.{table} (number_of_rows INT)
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            LINES TERMINATED BY '\n' STORED AS TEXTFILE
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table}/'
            """,
            engine=parquet_engine,
            chunksize=chunksize,
        ).as_pandas()
        if chunksize:
            df = pd.concat((d for d in df), ignore_index=True)
        assert df.shape[0] == 0
        assert df.shape[1] == 0

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine",
        [
            ({"cursor_kwargs": {"unload": True}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow"),
        ],
        indirect=["pandas_cursor"],
    )
    def test_empty_result_dml_unload(self, pandas_cursor, parquet_engine):
        df = pandas_cursor.execute(
            """
            SELECT * FROM one_row LIMIT 0
            """,
            engine=parquet_engine,
        ).as_pandas()
        assert df.shape[0] == 0
        assert df.shape[1] == 0

    def test_ctas(self, pandas_cursor):
        table_name = f"test_ctas_pandas_{''.join(random.choices(string.ascii_lowercase, k=10))}"
        location = f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/"
        pandas_cursor.execute(
            f"""
            CREATE TABLE {ENV.schema}.{table_name}
            WITH (
                format='PARQUET',
                external_location='{location}'
            ) AS SELECT a FROM many_rows LIMIT 1
            """
        )
        assert pandas_cursor.description == [("rows", "bigint", None, None, 19, 0, "UNKNOWN")]
        # CTAS returns affected row count via rowcount, not via fetchone()
        assert pandas_cursor.rowcount == 1
        assert pandas_cursor.fetchone() is None

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine",
        [
            ({"cursor_kwargs": {"unload": False}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow"),
        ],
        indirect=["pandas_cursor"],
    )
    def test_integer_na_values(self, pandas_cursor, parquet_engine):
        df = pandas_cursor.execute(
            """
            SELECT * FROM integer_na_values
            """,
            engine=parquet_engine,
        ).as_pandas()
        if pandas_cursor._unload:
            rows = [
                (
                    True if math.isnan(row["a"]) else row["a"],
                    True if math.isnan(row["b"]) else row["b"],
                )
                for _, row in df.iterrows()
            ]
            # If the UNLOAD option is enabled, it is converted to float for some reason.
            assert rows == [(1.0, 2.0), (1.0, True), (True, True)]
        else:
            rows = [(row["a"], row["b"]) for _, row in df.iterrows()]
            assert rows == [(1, 2), (1, pd.NA), (pd.NA, pd.NA)]

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine",
        [
            ({"cursor_kwargs": {"unload": False}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow"),
        ],
        indirect=["pandas_cursor"],
    )
    def test_float_na_values(self, pandas_cursor, parquet_engine):
        df = pandas_cursor.execute(
            """
            SELECT * FROM (VALUES (0.33), (NULL)) AS t (col)
            """,
            engine=parquet_engine,
        ).as_pandas()
        rows = [(row["col"],) for _, row in df.iterrows()]
        np.testing.assert_equal(rows, [(0.33,), (np.nan,)])

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine",
        [
            ({"cursor_kwargs": {"unload": False}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow"),
        ],
        indirect=["pandas_cursor"],
    )
    def test_boolean_na_values(self, pandas_cursor, parquet_engine):
        df = pandas_cursor.execute(
            """
            SELECT * FROM boolean_na_values
            """,
            engine=parquet_engine,
        ).as_pandas()
        rows = [(row["a"], row["b"]) for _, row in df.iterrows()]
        assert rows == [(True, False), (False, None), (None, None)]

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine",
        [
            ({"cursor_kwargs": {"unload": False}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow"),
        ],
        indirect=["pandas_cursor"],
    )
    def test_executemany(self, pandas_cursor, parquet_engine):
        rows = [(1, "foo"), (2, "bar"), (3, "jim o'rourke")]
        table_name = "execute_many_pandas" + (
            f"_unload_{parquet_engine}" if pandas_cursor._unload else ""
        )
        pandas_cursor.executemany(
            f"INSERT INTO {table_name} (a, b) VALUES (%(a)d, %(b)s)",
            [{"a": a, "b": b} for a, b in rows],
        )
        pandas_cursor.execute(f"SELECT * FROM {table_name}", engine=parquet_engine)
        assert sorted(pandas_cursor.fetchall()) == list(rows)

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine",
        [
            ({"cursor_kwargs": {"unload": False}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow"),
        ],
        indirect=["pandas_cursor"],
    )
    def test_executemany_fetch(self, pandas_cursor, parquet_engine):
        pandas_cursor.executemany("SELECT %(x)d AS x FROM one_row", [{"x": i} for i in range(1, 2)])
        # Operations that have result sets are not allowed with executemany.
        pytest.raises(ProgrammingError, pandas_cursor.fetchall)
        pytest.raises(ProgrammingError, pandas_cursor.fetchmany)
        pytest.raises(ProgrammingError, pandas_cursor.fetchone)
        pytest.raises(ProgrammingError, pandas_cursor.as_pandas)

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine",
        [
            ({"cursor_kwargs": {"unload": False}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow"),
        ],
        indirect=["pandas_cursor"],
    )
    def test_not_skip_blank_lines(self, pandas_cursor, parquet_engine):
        pandas_cursor.execute(
            """
            SELECT col FROM (VALUES (1), (NULL)) AS t (col)
            """,
            engine=parquet_engine,
        )
        assert len(pandas_cursor.fetchall()) == 2

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine",
        [
            ({"cursor_kwargs": {"unload": False}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow"),
        ],
        indirect=["pandas_cursor"],
    )
    def test_null_vs_empty_string(self, pandas_cursor, parquet_engine):
        # TODO https://github.com/pyathena-dev/PyAthena/issues/118
        query = """
        SELECT * FROM (VALUES ('', 'a'), ('N/A', 'a'), ('NULL', 'a'), (NULL, 'a'))
        AS t (col1, col2)
        """
        pandas_cursor.execute(query, engine=parquet_engine)
        if pandas_cursor._unload:
            # NULL and empty characters are correctly converted when the UNLOAD option is enabled.
            np.testing.assert_equal(
                pandas_cursor.fetchall(),
                [("", "a"), ("N/A", "a"), ("NULL", "a"), (None, "a")],
            )
        else:
            np.testing.assert_equal(
                pandas_cursor.fetchall(),
                [(np.nan, "a"), ("N/A", "a"), ("NULL", "a"), (np.nan, "a")],
            )
        pandas_cursor.execute(query, na_values=None, engine=parquet_engine)
        if pandas_cursor._unload:
            # NULL and empty characters are correctly converted when the UNLOAD option is enabled.
            assert pandas_cursor.fetchall() == [
                ("", "a"),
                ("N/A", "a"),
                ("NULL", "a"),
                (None, "a"),
            ]
        else:
            assert pandas_cursor.fetchall() == [
                ("", "a"),
                ("N/A", "a"),
                ("NULL", "a"),
                ("", "a"),
            ]

    @pytest.mark.parametrize(
        "pandas_cursor, parquet_engine",
        [
            ({"cursor_kwargs": {"unload": False}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "auto"),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow"),
        ],
        indirect=["pandas_cursor"],
    )
    def test_null_decimal_value(self, pandas_cursor, parquet_engine):
        pandas_cursor.execute("SELECT CAST(null AS DECIMAL) AS col_decimal", engine=parquet_engine)
        assert pandas_cursor.fetchall() == [(None,)]

    def test_iceberg_table(self, pandas_cursor):
        iceberg_table = "test_iceberg_table_pandas_cursor"
        pandas_cursor.execute(
            f"""
            CREATE TABLE {ENV.schema}.{iceberg_table} (
              id INT,
              col1 STRING
            )
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{iceberg_table}/'
            tblproperties('table_type'='ICEBERG')
            """
        )
        pandas_cursor.execute(
            f"""
            INSERT INTO {ENV.schema}.{iceberg_table} (id, col1)
            VALUES (1, 'test1'), (2, 'test2')
            """
        )
        pandas_cursor.execute(
            f"""
            SELECT COUNT(*) FROM {ENV.schema}.{iceberg_table}
            """
        )
        assert pandas_cursor.fetchall() == [(2,)]

        pandas_cursor.execute(
            f"""
            UPDATE {ENV.schema}.{iceberg_table}
            SET col1 = 'test1_update'
            WHERE id = 1
            """
        )
        pandas_cursor.execute(
            f"""
            SELECT col1
            FROM {ENV.schema}.{iceberg_table}
            WHERE id = 1
            """
        )
        assert pandas_cursor.fetchall() == [("test1_update",)]

        pandas_cursor.execute(
            f"""
            CREATE TABLE {ENV.schema}.{iceberg_table}_merge (
              id INT,
              col1 STRING
            )
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{iceberg_table}_merge/'
            tblproperties('table_type'='ICEBERG')
            """
        )
        pandas_cursor.execute(
            f"""
            INSERT INTO {ENV.schema}.{iceberg_table}_merge (id, col1)
            VALUES (1, 'foobar')
            """
        )
        pandas_cursor.execute(
            f"""
            MERGE INTO {ENV.schema}.{iceberg_table} AS t1
            USING {ENV.schema}.{iceberg_table}_merge AS t2
              ON t1.id = t2.id
            WHEN MATCHED
              THEN UPDATE SET col1 = t2.col1
            """
        )
        pandas_cursor.execute(
            f"""
            SELECT col1
            FROM {ENV.schema}.{iceberg_table}
            WHERE id = 1
            """
        )
        assert pandas_cursor.fetchall() == [("foobar",)]

        pandas_cursor.execute(
            f"""
            VACUUM {ENV.schema}.{iceberg_table}
            """
        )

        pandas_cursor.execute(
            f"""
            DELETE FROM {ENV.schema}.{iceberg_table}
            WHERE id = 2
            """
        )
        pandas_cursor.execute(
            f"""
            SELECT COUNT(*) FROM {ENV.schema}.{iceberg_table}
            """
        )
        assert pandas_cursor.fetchall() == [(1,)]

    def test_execute_with_callback(self, pandas_cursor):
        """Test that callback is invoked with query_id when on_start_query_execution is provided."""
        callback_results = []

        def test_callback(query_id: str):
            callback_results.append(query_id)

        pandas_cursor.execute("SELECT 1", on_start_query_execution=test_callback)

        assert len(callback_results) == 1
        assert callback_results[0] == pandas_cursor.query_id
        assert pandas_cursor.query_id is not None

    def test_pandas_cursor_iter_chunks_with_chunksize(self, pandas_cursor):
        """Test PandasCursor iter_chunks method with chunksize set."""
        cursor = pandas_cursor
        cursor._chunksize = 1000
        cursor.execute("SELECT number FROM (VALUES (1), (2), (3), (4), (5)) as t(number)")

        chunk_count = 0
        for chunk in cursor.iter_chunks():
            chunk_count += 1
            assert isinstance(chunk, pd.DataFrame)
            assert len(chunk) > 0

        assert chunk_count >= 1

    def test_pandas_cursor_auto_optimize_chunksize_enabled(self, pandas_cursor):
        """Test PandasCursor with auto_optimize_chunksize enabled."""
        cursor = pandas_cursor
        cursor._chunksize = None  # No explicit chunksize
        cursor._auto_optimize_chunksize = True
        cursor.execute("SELECT number FROM (VALUES (1), (2), (3), (4), (5)) as t(number)")

        # Should work without error (auto-optimization for small files may not trigger chunking)
        result = cursor.as_pandas()
        # Small test data likely won't trigger chunking, so expect DataFrame
        assert isinstance(result, (pd.DataFrame, PandasDataFrameIterator))

    def test_pandas_cursor_auto_optimize_chunksize_disabled(self, pandas_cursor):
        """Test PandasCursor with auto_optimize_chunksize disabled (default)."""
        cursor = pandas_cursor
        cursor._chunksize = None  # No explicit chunksize
        cursor._auto_optimize_chunksize = False  # Default behavior
        cursor.execute("SELECT number FROM (VALUES (1), (2), (3), (4), (5)) as t(number)")

        # Should return DataFrame (no chunking)
        result = cursor.as_pandas()
        assert isinstance(result, pd.DataFrame)

    def test_pandas_cursor_explicit_chunksize_overrides_auto_optimize(self, pandas_cursor):
        """Test that explicit chunksize takes precedence over auto-optimization."""
        cursor = pandas_cursor
        cursor._chunksize = 1000  # Explicit chunksize
        cursor._auto_optimize_chunksize = True  # Should be ignored
        cursor.execute("SELECT number FROM (VALUES (1), (2), (3), (4), (5)) as t(number)")

        # Should return iterator due to explicit chunksize
        result = cursor.as_pandas()
        assert isinstance(result, PandasDataFrameIterator)

    def test_pandas_cursor_iter_chunks_without_chunksize(self, pandas_cursor):
        """Test PandasCursor iter_chunks method without chunksize (single DataFrame)."""
        cursor = pandas_cursor
        cursor._chunksize = None
        cursor.execute("SELECT number FROM (VALUES (1), (2), (3), (4), (5)) as t(number)")

        chunk_count = 0
        for chunk in cursor.iter_chunks():
            chunk_count += 1
            assert isinstance(chunk, pd.DataFrame)
            assert len(chunk) > 0

        # Should yield exactly one chunk (the entire DataFrame)
        assert chunk_count == 1

    def test_pandas_cursor_chunked_vs_regular_same_data(self, pandas_cursor):
        """Test that chunked and regular reading produce the same data."""
        query = "SELECT * FROM many_rows LIMIT 100"  # Use a reasonable size for testing

        # Regular reading (no chunksize)
        regular_cursor = pandas_cursor
        regular_cursor._chunksize = None
        regular_cursor.execute(query)
        regular_df = regular_cursor.as_pandas()

        # Chunked reading - reuse the same cursor but set chunksize
        chunked_cursor = pandas_cursor
        chunked_cursor._chunksize = 25
        chunked_cursor.execute(query)
        chunked_dfs = list(chunked_cursor.iter_chunks())

        # Combine chunks
        combined_df = pd.concat(chunked_dfs, ignore_index=True)

        # Should have the same data
        pd.testing.assert_frame_equal(regular_df.sort_index(), combined_df.sort_index())

        # Should have multiple chunks
        assert len(chunked_dfs) > 1

        # Each chunk should be <= chunksize
        for chunk in chunked_dfs:
            assert len(chunk) <= 25

    def test_pandas_cursor_actual_chunking_behavior(self, pandas_cursor):
        """Test that chunking actually limits memory usage by checking chunk sizes."""
        # Use a query that returns more rows than chunksize
        cursor = pandas_cursor
        cursor._chunksize = 10
        cursor.execute("SELECT * FROM many_rows LIMIT 50")

        result = cursor.as_pandas()
        assert isinstance(result, PandasDataFrameIterator)

        chunk_sizes = []
        total_rows = 0

        for chunk in result:
            chunk_size = len(chunk)
            chunk_sizes.append(chunk_size)
            total_rows += chunk_size

            # Each chunk should not exceed chunksize (except possibly the last one)
            assert chunk_size <= 10

        # Should have processed all 50 rows
        assert total_rows == 50

        # Should have multiple chunks
        assert len(chunk_sizes) > 1

        # Most chunks should be exactly chunksize (10), last might be smaller
        # All but the last chunk should be exactly 10
        for size in chunk_sizes[:-1]:
            assert size == 10
        # Last chunk should be <= 10
        assert chunk_sizes[-1] <= 10

    def test_pandas_cursor_iter_chunks_consistency(self, pandas_cursor):
        """Test that iter_chunks() and direct iteration give same results."""
        cursor = pandas_cursor
        cursor._chunksize = 15
        cursor.execute("SELECT * FROM many_rows LIMIT 45")

        # Method 1: Using iter_chunks()
        chunks_via_method = list(cursor.iter_chunks())

        # Method 2: Direct iteration on as_pandas()
        cursor.execute("SELECT * FROM many_rows LIMIT 45")  # Re-execute
        chunks_via_direct = list(cursor.as_pandas())

        # Should have same number of chunks
        assert len(chunks_via_method) == len(chunks_via_direct)

        # Each corresponding chunk should be identical
        for chunk1, chunk2 in zip(chunks_via_method, chunks_via_direct, strict=False):
            pd.testing.assert_frame_equal(chunk1, chunk2)
