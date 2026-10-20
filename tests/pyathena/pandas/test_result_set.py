# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import csv
import inspect
import io
import re
import threading
from collections import defaultdict
from datetime import UTC, date, datetime, time
from unittest.mock import MagicMock, PropertyMock, patch
from zoneinfo import ZoneInfo

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from pandas.testing import assert_frame_equal
from pyarrow import csv as pyarrow_csv

from pyathena.error import DataError, OperationalError, ProgrammingError
from pyathena.filesystem.s3 import S3FileSystem
from pyathena.model import AthenaQueryExecution
from pyathena.pandas.converter import DefaultPandasTypeConverter
from pyathena.pandas.result_set import (
    AthenaPandasResultSet,
    AthenaPandasResultSetMetadataReader,
    PandasDataFrameIterator,
    _no_trunc_date,
    _read_csv_with_pyarrow,
)
from pyathena.util import RetryConfig, retry_api_call, stop_retries_on
from tests.pyathena import temporal
from tests.pyathena.util import (
    LocalBucketFileSystem,
    ThrottledOperation,
    assert_fetches_fail,
    query_results_response,
)

# A value that makes a result file larger than the smallest that the PyArrow engine reads.
_PADDING = "x" * 120
# A CSV result file of two rows.
_TEXT_CSV = '"v"\n"a"\n"b"\n'


# Temporal values as pandas reads them with PyAthena's parse_dates and converters.
_TIMESTAMP_TEXT = "2020-01-02 03:04:05.123456789"
_PARSED = pd.Timestamp(_TIMESTAMP_TEXT)
_DATE = pd.Timestamp("2020-01-02")
_TOKYO_TEXT = "2020-01-02 03:04:05.000 Asia/Tokyo"
_TOKYO = datetime(2020, 1, 2, 3, 4, 5, tzinfo=ZoneInfo("Asia/Tokyo"))
_TIME_TEXT = "03:04:05.123"
_TIME = time(3, 4, 5, 123000)


def _typed_values(rows):
    """Express the rows by the type and repr of each value, so that NaN and NA compare."""
    return [[(type(value), repr(value)) for value in row] for row in rows]


class TestPandasDataFrameIterator:
    @pytest.mark.parametrize(
        ("values", "dtype"),
        [
            ([1, 2, 3], "int64"),
            ([1.5, None, 3.0], "float64"),
            ([True, False, True], "bool"),
            ([[1], None, {"a": 1}], object),
            (["a", None, ""], "str"),
            (["a", None, ""], pd.StringDtype("python", na_value=np.nan)),
            (["a", None, ""], "string[pyarrow]"),
            (["a", None, ""], "string[python]"),
            ([1, None, 3], "Int64"),
            ([1.5, None, 3.0], "Float64"),
            ([True, None, False], "boolean"),
            (["2024-01-01 01:02:03", None, "2024-01-02"], "datetime64[ns]"),
            (["2024-01-01 01:02:03", None, "2024-01-02"], "datetime64[us, UTC]"),
            (["a", None, "a"], "category"),
            ([1, None, 3], pd.ArrowDtype(pa.int64())),
            (["a", None, ""], pd.ArrowDtype(pa.string())),
        ],
    )
    def test_itertuples_values(self, values, dtype):
        """The rows hold the values that DataFrame.itertuples() gives, with their types."""
        df = pd.DataFrame({"c": pd.Series(values, dtype=dtype), "n": [10, 11, 12]})
        expected = list(df.itertuples(index=False, name=None))

        rows = list(PandasDataFrameIterator(df, _no_trunc_date).itertuples())

        assert _typed_values(rows) == _typed_values(expected)

    def test_itertuples_large_chunk(self):
        """The rows of a chunk larger than a block of rows keep their order."""
        size = PandasDataFrameIterator._ROW_BLOCK_SIZE * 2 + 1
        df = pd.DataFrame(
            {"n": range(size), "s": pd.Series([f"v{i}" for i in range(size)], dtype="str")}
        )

        rows = list(PandasDataFrameIterator(df, _no_trunc_date).itertuples())

        assert rows == [(i, f"v{i}") for i in range(size)]

    def test_iterrows_continuous_row_numbers(self):
        """Row numbers continue across chunks."""
        chunks = [pd.DataFrame({"n": [1, 2]}), pd.DataFrame({"n": [3]}, index=[0])]

        rows = list(PandasDataFrameIterator(iter(chunks), _no_trunc_date).iterrows())

        assert rows == [(0, {"n": 1}), (1, {"n": 2}), (2, {"n": 3})]

    @pytest.mark.parametrize(
        ("csv", "read_csv_kwargs"),
        [
            ("id,kind\n10,a\n11,a\n12,b\n13,b\n14,c\n", {}),
            ("id,kind\n10,a\n11,b\n", {}),
            ("id,kind\n10,a\n11,a\n12,b\n13,b\n14,c\n", {"index_col": "id"}),
            ("id,kind\n10,c\n11,c\n12,a\n13,a\n14,b\n", {"dtype": {"kind": "category"}}),
            ("id,kind\n10,\n11,\n12,b\n13,a\n14,\n", {"dtype": {"kind": "category"}}),
            (
                "id,kind\n10,c\n11,c\n12,a\n13,a\n14,b\n",
                {"dtype": {"kind": pd.CategoricalDtype(ordered=True)}},
            ),
            (
                "id,kind\n10,c\n11,c\n12,a\n13,a\n14,b\n",
                {"index_col": "kind", "dtype": {"kind": "category"}},
            ),
        ],
        ids=[
            "default_index",
            "single_chunk",
            "index_col",
            "category",
            "category_null_chunk",
            "ordered_category",
            "category_index",
        ],
    )
    def test_as_pandas_matches_whole_read(self, csv, read_csv_kwargs):
        """Joining the chunks gives the DataFrame that reading the whole file gives."""
        expected = pd.read_csv(io.StringIO(csv), **read_csv_kwargs)
        reader = pd.read_csv(io.StringIO(csv), chunksize=2, **read_csv_kwargs)
        df_iter = PandasDataFrameIterator(reader, _no_trunc_date)

        pd.testing.assert_frame_equal(df_iter.as_pandas(), expected)

    def test_as_pandas_remaining_chunks(self):
        """After a chunk was read, the remaining chunks keep their row numbers."""
        reader = pd.read_csv(io.StringIO("n\n1\n2\n3\n4\n5\n"), chunksize=2)
        df_iter = PandasDataFrameIterator(reader, _no_trunc_date)
        next(df_iter)

        df = df_iter.as_pandas()
        assert df["n"].tolist() == [3, 4, 5]
        assert df.index.tolist() == [2, 3, 4]
        assert df_iter.as_pandas().empty

    @pytest.mark.parametrize(
        "read",
        [PandasDataFrameIterator.__next__, PandasDataFrameIterator.get_chunk],
        ids=["next", "get_chunk"],
    )
    @pytest.mark.parametrize("error", [OperationalError("failed"), KeyboardInterrupt()])
    def test_read_after_failed_read(self, read, error):
        """GH-1225: after a read raises, the iterator is closed and later reads raise."""

        def chunks():
            yield pd.DataFrame({"n": [1]})
            raise error

        csv_stream = io.StringIO()
        reader = chunks()
        df_iter = PandasDataFrameIterator(reader, _no_trunc_date, csv_stream)
        assert read(df_iter)["n"].tolist() == [1]
        with pytest.raises(type(error)):
            read(df_iter)
        assert inspect.getgeneratorstate(reader) == inspect.GEN_CLOSED
        assert csv_stream.closed
        for later in (next, PandasDataFrameIterator.get_chunk):
            with pytest.raises(ProgrammingError, match=r"cannot be read after a failed read\."):
                later(df_iter)


class TestAthenaPandasResultSet:
    def test_fetch_across_chunks(self):
        """The fetch methods continue across chunks, reading a chunk when its rows are needed."""
        read = []

        def chunks():
            for start in (0, 4, 8):
                read.append(start)
                stop = min(start + 4, 10)
                yield pd.DataFrame(
                    {
                        "n": range(start, stop),
                        "s": [f"v{i}" for i in range(start, stop)],
                    },
                    index=range(start, stop),
                )

        result_set = _fetching_result_set(chunks(), arraysize=3)
        assert result_set.rownumber == 0
        assert result_set.fetchone() == (0, "v0")
        assert result_set.rownumber == 1
        assert result_set.fetchmany(3) == [(1, "v1"), (2, "v2"), (3, "v3")]
        assert result_set.rownumber == 4
        assert read == [0]
        assert result_set.fetchmany(5) == [(i, f"v{i}") for i in range(4, 9)]
        assert result_set.rownumber == 9
        assert read == [0, 4, 8]
        assert result_set.fetchmany() == [(9, "v9")]
        assert result_set.rownumber == 10
        assert result_set.fetchmany() == []
        assert result_set.fetchall() == []
        assert result_set.fetchone() is None
        assert result_set.rownumber == 10

    @pytest.mark.parametrize(
        ("seconds", "method"),
        [
            ([0, 10**13, 0], "fetchone"),
            ([0, 10**13, 0], "fetchmany"),
            ([0, 10**13, 0], "fetchall"),
            ([0, 0, 10**13], "fetchmany"),
            ([0, 0, 10**13], "fetchall"),
        ],
    )
    def test_fetch_after_conversion_error(self, seconds, method):
        """GH-1225: after a value fails to convert, the fetch methods raise."""
        # A timestamp that does not fit a Python datetime fails while its row is built.
        timestamps = pa.array(seconds, type=pa.timestamp("s"))
        chunks = [
            pd.DataFrame(
                {"n": [0, 1, 2], "t": pd.Series(timestamps, dtype=pd.ArrowDtype(timestamps.type))}
            ),
            pd.DataFrame({"n": [3]}),
        ]
        result_set = _fetching_result_set(iter(chunks), arraysize=3)
        assert result_set.fetchone() == (0, pd.Timestamp(0, unit="s"))
        with pytest.raises(OverflowError):
            getattr(result_set, method)()
        assert_fetches_fail(result_set)

    def test_fetchone_interrupted_while_counting(self):
        """GH-1225: an interrupt after fetchone() took its row breaks the result set."""
        result_set = _fetching_result_set(pd.DataFrame({"n": [0, 1]}), arraysize=3)
        with (
            patch.object(result_set, "_count_rows", side_effect=KeyboardInterrupt),
            pytest.raises(KeyboardInterrupt),
        ):
            result_set.fetchone()
        assert_fetches_fail(result_set)

    def test_fetch_after_failed_chunk_read(self):
        """GH-1225: after iter_chunks() fails to read a chunk, the fetch methods raise."""

        def chunks():
            yield pd.DataFrame({"n": [0]})
            raise OperationalError("failed")

        result_set = _fetching_result_set(chunks(), arraysize=3)
        with pytest.raises(OperationalError):
            list(result_set.iter_chunks())
        with pytest.raises(ProgrammingError, match=r"cannot be read after a failed read\."):
            result_set.fetchone()
        assert_fetches_fail(result_set)

    def test_fetch_after_chunks_run_out(self):
        """The rows of a chunk that a fetch started stay after iter_chunks() runs out."""
        size = PandasDataFrameIterator._ROW_BLOCK_SIZE * 2
        result_set = _fetching_result_set(iter([pd.DataFrame({"n": range(size)})]), arraysize=3)
        assert result_set.fetchone() == (0,)
        assert list(result_set.iter_chunks()) == []
        assert result_set.fetchall() == [(i,) for i in range(1, size)]
        assert result_set.rownumber == size

    def test_fetchmany_size_above_maxsize(self):
        result_set = _fetching_result_set(pd.DataFrame({"n": [1, 2]}), arraysize=3)
        assert result_set.fetchmany(10**100) == [(1,), (2,)]
        assert result_set.rownumber == 2

    def test_fetchmany_size_not_integer(self):
        """A size that is not an integer raises TypeError without consuming rows."""
        result_set = _fetching_result_set(pd.DataFrame({"n": [1, 2]}), arraysize=3)
        with pytest.raises(TypeError):
            result_set.fetchmany(1.5)
        assert result_set.fetchall() == [(1,), (2,)]

    def test_fetch_after_close(self):
        """GH-1207: the fetches and DataFrame methods of a closed result set raise."""
        result_set = _fetching_result_set(pd.DataFrame({"n": [1, 2]}), arraysize=3)
        result_set.close()
        for fetch in (
            result_set.fetchone,
            result_set.fetchmany,
            result_set.fetchall,
            result_set.as_pandas,
            result_set.iter_chunks,
        ):
            with pytest.raises(ProgrammingError):
                fetch()
        assert result_set.rownumber is None

    @pytest.mark.parametrize("infer_string", [True, False])
    @pytest.mark.parametrize("read_csv_kwargs", [{}, {"keep_default_na": True}])
    @pytest.mark.parametrize(
        ("data", "names", "expected_names"),
        [
            ("001\n007\n1e3\n" * 10, ["tab_name"], ["001", "007", "1e3"] * 10),
            (
                "001                 \n007                 \n1e3                 \n" * 10,
                ["field"],
                ["001                 ", "007                 ", "1e3                 "] * 10,
            ),
            (
                (
                    "001                 \tint                 \t\n"
                    "007                 \tint                 \t\n"
                    "1e3                 \tint                 \t\n"
                )
                * 10,
                ["col_name", "data_type", "comment"],
                ["001                 ", "007                 ", "1e3                 "] * 10,
            ),
        ],
        ids=["show_tables", "show_columns", "describe"],
    )
    def test_read_csv_ddl_preserves_numeric_looking_names(
        self, tmp_path, data, names, expected_names, read_csv_kwargs, infer_string
    ):
        """A DDL result read with engine="pyarrow" keeps numeric-looking names and their padding.

        The result file is closed after the read.
        """
        assert len(data) >= AthenaPandasResultSet.PYARROW_MIN_FILE_SIZE_BYTES
        types = [(name, "string") for name in names]
        fs = LocalBucketFileSystem(tmp_path)
        with pd.option_context("future.infer_string", infer_string):
            df = _local_result_set(
                fs, types, data, name="result.txt", engine="pyarrow", **read_csv_kwargs
            ).as_pandas()
        assert all(file.closed for _, file in fs.opened)
        assert df.columns.tolist() == names
        assert df.iloc[:, 0].tolist() == expected_names

    @pytest.mark.parametrize(
        ("columns", "rows", "read_options", "expected", "sizes"),
        [
            (
                [("a", "integer"), ("s", "varchar")],
                [("1", _PADDING), ("2", _PADDING)],
                {"chunksize": 1},
                [(1, _PADDING), (2, _PADDING)],
                [1, 1],
            ),
            (
                [("s", "varchar")],
                [(_PADDING,)],
                {"quoting": csv.QUOTE_NONE},
                [(f'"{_PADDING}"',)],
                [1],
            ),
            (
                [("x", "integer"), ("x", "varchar")],
                [("1", f"007{_PADDING}")],
                {},
                [(1, f"007{_PADDING}")],
                [1],
            ),
            (
                [("b", "varbinary"), ("s", "varchar")],
                [("00 ff", _PADDING)],
                {},
                [(b"\x00\xff", _PADDING)],
                [1],
            ),
        ],
        ids=["chunksize", "quoting", "duplicate_names", "converters"],
    )
    def test_pyarrow_engine_incompatible_options(
        self, tmp_path, columns, rows, read_options, expected, sizes
    ):
        """engine="pyarrow" reads with the C engine what the PyArrow engine cannot read.

        That is a result in chunks, with another quoting, with columns of the same name,
        or with converters.
        """
        text = io.StringIO(newline="")
        writer = csv.writer(text, quoting=csv.QUOTE_ALL, lineterminator="\n")
        writer.writerows([[name for name, _ in columns], *rows])
        result_set = _local_result_set(
            LocalBucketFileSystem(tmp_path),
            columns,
            text.getvalue(),
            engine="pyarrow",
            **read_options,
        )
        chunks = list(result_set.iter_chunks())
        assert [len(chunk) for chunk in chunks] == sizes
        assert [tuple(row) for chunk in chunks for row in chunk.itertuples(index=False)] == expected

    @pytest.mark.parametrize(
        "values",
        [
            pytest.param(["007", "1e3", "10", None], id="numeric_looking"),
            pytest.param(["a", "line1\nline2", "nan", None], id="text"),
        ],
    )
    def test_read_csv_with_pyarrow_engine_values(self, tmp_path, values):
        """engine="pyarrow" keeps the text of varchar values, numeric-looking and multiline."""
        data = '"v","p"\n' + "".join(
            f'{"" if v is None else chr(34) + v + chr(34)},"{_PADDING}"\n' for v in values
        )
        result_set = _local_result_set(
            LocalBucketFileSystem(tmp_path),
            [("v", "varchar"), ("p", "varchar")],
            data,
            engine="pyarrow",
        )
        assert [None if pd.isna(v) else v for v, _ in result_set.fetchall()] == values

    def test_read_small_csv_with_pyarrow_engine(self, tmp_path):
        """A CSV result smaller than PYARROW_MIN_FILE_SIZE_BYTES is read with the C engine.

        The C engine reads timestamps at microseconds.
        """
        data = '"d","t"\n"2024-01-01","2024-01-01 00:00:00.000"\n'
        assert len(data) < AthenaPandasResultSet.PYARROW_MIN_FILE_SIZE_BYTES
        types = [("d", "date"), ("t", "timestamp")]
        df = _local_result_set(
            LocalBucketFileSystem(tmp_path), types, data, engine="pyarrow"
        ).as_pandas()
        assert df.to_dict("list") == {
            "d": [pd.Timestamp("2024-01-01")],
            "t": [pd.Timestamp("2024-01-01 00:00:00")],
        }
        assert df["t"].dtype == "datetime64[us]"

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            pytest.param({}, ["NA", "null", "x", None, "y", "z", "w"], id="default"),
            pytest.param(
                {"keep_default_na": True},
                [None, None, "x", None, "y", "z", "w"],
                id="keep_default_na",
            ),
            pytest.param(
                {"on_bad_lines": "skip"}, ["NA", "null", None, "y", "z", "w"], id="on_bad_lines"
            ),
            pytest.param(
                {"dtype": "str"}, ["NA", "null", "x", None, "y", "z", "w"], id="single_dtype"
            ),
            pytest.param({"parse_dates": ("d",)}, OperationalError, id="parse_dates_tuple"),
            pytest.param(
                {"na_values": ("", "NA")}, [None, "null", "x", None, "y", "z", "w"], id="na_values"
            ),
        ],
    )
    def test_read_csv_with_pyarrow_engine_options(self, tmp_path, kwargs, expected):
        """With engine="pyarrow", options that PyAthena's own read does not reproduce reach pandas.

        pandas applies them, or rejects the ones that it does not accept.
        """
        rows = ["NA", "null", "x", "", "y", "z", "w"]
        dates = [f"2024-01-0{i + 1}" for i in range(len(rows))]
        lines = [f'"{v}","{d}"' for v, d in zip(rows, dates, strict=True)]
        if "on_bad_lines" in kwargs:
            # The row of "x" has a field too many.
            lines[2] += ',"extra"'
            del dates[2]
        data = '"v","d"\n' + "".join(f"{line}\n" for line in lines)
        assert len(data) >= AthenaPandasResultSet.PYARROW_MIN_FILE_SIZE_BYTES
        types = [("v", "varchar"), ("d", "date")]
        fs = LocalBucketFileSystem(tmp_path)
        if expected is OperationalError:
            with pytest.raises(OperationalError):
                _local_result_set(fs, types, data, engine="pyarrow", **kwargs)
            return
        df = _local_result_set(fs, types, data, engine="pyarrow", **kwargs).as_pandas()
        assert [None if pd.isna(v) else v for v in df["v"]] == expected
        if "dtype" in kwargs:
            assert df["d"].tolist() == dates
        else:
            assert df["d"].tolist() == [pd.Timestamp(d) for d in dates]

    @pytest.mark.parametrize(
        "kwargs",
        [{}, {"chunksize": 1}, {"filters": [("i", ">", 10)]}],
        ids=["whole", "chunks", "whole-without-matching-rows"],
    )
    def test_describe_unload(self, tmp_path, kwargs):
        """GH-1281: the metadata reader describes an UNLOAD result as the result set does.

        A result with data files is described by their columns, also when the read
        options drop every row.
        """
        fs = LocalBucketFileSystem(tmp_path)
        tables = [pa.table({"i": pa.array([1, 2], pa.int64())}), pa.table({"i": [3]})]
        expected = [("i", "bigint", None, None, 19, 0, "NULLABLE")]
        assert _local_result_set(fs, tables=tables, **kwargs).description == expected
        assert _local_result_set(fs, tables=tables, describe=True, **kwargs) == expected

    def test_describe_unload_chunks_reads_first_footer(self, tmp_path):
        """GH-1281: describe() of an UNLOAD result read in chunks opens only its first file.

        The metadata reader reads the footer of the first file, without listing the
        unload location.
        """
        fs = LocalBucketFileSystem(tmp_path)
        tables = [pa.table({"i": [1]}), pa.table({"i": [2]}), pa.table({"i": [3]})]
        description = _local_result_set(fs, tables=tables, describe=True, chunksize=1)
        assert description == [("i", "bigint", None, None, 19, 0, "NULLABLE")]
        assert len(fs.opened) == 1

    def test_iter_parquet_chunks_without_rows(self, tmp_path):
        """GH-1281: an UNLOAD result read in chunks whose files hold no rows keeps its columns.

        Its DataFrame has the columns that describe it, as a result read whole does.
        """
        fs = LocalBucketFileSystem(tmp_path)
        tables = [pa.table({"i": pa.array([], pa.int64()), "t": pa.array([], pa.timestamp("ns"))})]
        result_set = _local_result_set(
            fs,
            tables=tables,
            # As Athena writes timestamps.
            write_options={"use_deprecated_int96_timestamps": True},
            chunksize=1,
        )
        assert result_set.description == [
            ("i", "bigint", None, None, 19, 0, "NULLABLE"),
            ("t", "timestamp", None, None, 3, 0, "NULLABLE"),
        ]
        chunks = list(result_set.iter_chunks())
        assert [chunk.columns.tolist() for chunk in chunks] == [["i", "t"]]
        # INT96 timestamps at microseconds, as the chunks of files with rows have them.
        assert str(chunks[0].dtypes["t"]) == "datetime64[us]"
        assert result_set.fetchall() == []

    def test_iter_parquet_chunks_null_in_later_file(self, tmp_path):
        """Top-level integer and boolean columns with NULL anywhere convert alike in every chunk.

        The result is described by the columns of its files, read from the files of the data
        manifest without listing the unload location, which credentials may not allow.
        """

        class UnlistableFileSystem(LocalBucketFileSystem):
            def ls(self, path, detail=True, **kwargs):
                raise PermissionError(path)

            def find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs):
                raise PermissionError(path)

        result_set = _local_result_set(
            UnlistableFileSystem(tmp_path),
            tables=[
                pa.table({"i": pa.array([1, 2], pa.int64()), "b": [True, False]}),
                pa.table({"i": pa.array([None, 4], pa.int64()), "b": [None, True]}),
            ],
            chunksize=1,
        )
        chunks = list(result_set.iter_chunks())
        whole = pd.read_parquet(tmp_path / "bucket" / "unload")
        assert result_set.description == [
            ("i", "bigint", None, None, 19, 0, "NULLABLE"),
            ("b", "boolean", None, None, 0, 0, "NULLABLE"),
        ]
        assert [len(chunk) for chunk in chunks] == [1, 1, 1, 1]
        for chunk in chunks:
            assert chunk.dtypes.tolist() == whole.dtypes.tolist()
        # The chunks are indexed from their first row, as the whole result is.
        assert_frame_equal(pd.concat(chunks), whole, check_exact=True)

    def test_read_parquet_footers_stopped(self, tmp_path):
        """GH-1202: the footer reads run in the stop scope of their caller.

        A cancelled ``create_in_thread()`` of AioPandasCursor stops their retries too.
        """
        fs = LocalBucketFileSystem(tmp_path)
        stop = threading.Event()
        stop.set()
        operations = []
        write = fs.open

        def open_(path, mode="rb", **kwargs):
            if "w" in mode:
                return write(path, mode, **kwargs)
            # A read of the file throttles, as an S3 request does.
            operation = ThrottledOperation()
            operations.append(operation)
            return retry_api_call(operation, RetryConfig(attempt=3, multiplier=0))

        with (
            patch.object(fs, "open", side_effect=open_),
            stop_retries_on(stop),
            pytest.raises(OperationalError) as error,
        ):
            _local_result_set(fs, tables=[pa.table({"i": [1]}), pa.table({"i": [2]})], chunksize=1)
        # The error of the stopped attempt is raised.
        assert isinstance(error.value.__cause__, ClientError)
        assert error.value.__cause__.response["Error"]["Code"] == "ThrottlingException"
        assert operations
        assert [operation.calls for operation in operations] == [1] * len(operations)

    @pytest.mark.parametrize("engine", ["c", "pyarrow"])
    def test_as_pandas_parquet_chunks_engine(self, tmp_path, engine):
        """Chunks of an UNLOAD result need the pyarrow engine, as the whole result does."""
        fs = LocalBucketFileSystem(tmp_path)
        tables = [pa.table({"i": [1]})]
        if engine == "c":
            with pytest.raises(ProgrammingError):
                _local_result_set(fs, tables=tables, chunksize=1, engine=engine)
            return
        result_set = _local_result_set(fs, tables=tables, chunksize=1, engine=engine)
        assert result_set.fetchall() == [(1,)]

    @pytest.mark.parametrize(
        ("kwargs", "sizes", "columns"),
        [
            pytest.param({}, [1, 1, 1], ["a", "s"], id="default"),
            pytest.param({"columns": ["a"]}, [1, 1, 1], ["a"], id="columns"),
            pytest.param({"dtype_backend": "pyarrow"}, [1, 1, 1], ["a", "s"], id="dtype_backend"),
            pytest.param({"columns": []}, [3], [], id="no_columns"),
            # Other options read the result whole, where pandas applies or rejects them.
            pytest.param({"filters": [("a", "=", 1)]}, [1], ["a", "s"], id="filters"),
            pytest.param({"columns": ["s.x"]}, [3], ["x"], id="nested_column"),
            pytest.param({"columns": ["a", "a"]}, [3], ["a", "a"], id="repeated_columns"),
            pytest.param({"dtype_backend": None}, None, None, id="invalid_dtype_backend"),
        ],
    )
    def test_unload_chunks_with_read_options(self, tmp_path, kwargs, sizes, columns):
        """Only chunksize with supported read options reads an UNLOAD result in chunks."""
        tables = [
            pa.table({"a": [1, 2], "s": [{"x": 1}, {"x": 2}]}),
            pa.table({"a": [3], "s": [{"x": 3}]}),
        ]
        fs = LocalBucketFileSystem(tmp_path)
        if sizes is None:
            with pytest.raises(OperationalError):
                _local_result_set(fs, tables=tables, chunksize=1, **kwargs)
            return
        chunks = list(_local_result_set(fs, tables=tables, chunksize=1, **kwargs).iter_chunks())
        assert [len(chunk) for chunk in chunks] == sizes
        assert [chunk.columns.tolist() for chunk in chunks] == [columns] * len(sizes)
        # The chunks have the dtypes of the backend.
        backend = kwargs.get("dtype_backend")
        assert all(
            isinstance(dtype, pd.ArrowDtype) is (backend == "pyarrow")
            for chunk in chunks
            for dtype in chunk.dtypes
        )

    def test_unload_repeated_columns(self, tmp_path):
        """Rows of an UNLOAD result read with a column repeated have the column twice."""
        result_set = _local_result_set(
            LocalBucketFileSystem(tmp_path),
            tables=[pa.table({"a": [1, 2], "b": ["x", "y"]})],
            columns=["a", "a"],
        )
        assert result_set.fetchall() == [(1, 1), (2, 2)]

    def test_unload_user_filesystem(self, tmp_path):
        """A filesystem given to execute() reads the UNLOAD result instead of PyAthena's."""
        fs = LocalBucketFileSystem(tmp_path / "pyathena")
        user_fs = LocalBucketFileSystem(tmp_path / "user")
        _write_unload_tables(user_fs, [pa.table({"a": [2]})])
        result_set = _local_result_set(fs, tables=[pa.table({"a": [1]})], filesystem=user_fs)
        assert result_set.fetchall() == [(2,)]

    @pytest.mark.parametrize(
        ("option", "value"),
        [("storage_options", {}), ("storage_options", None), ("filesystem", None)],
        ids=["storage_options", "storage_options-none", "filesystem-none"],
    )
    def test_unload_filesystem_options(self, tmp_path, option, value):
        """storage_options, even None, or a None filesystem given to execute() replace PyAthena's.

        pandas opens the S3 files itself, so the read is observed.
        """
        fs = LocalBucketFileSystem(tmp_path / "pyathena")
        tables = [pa.table({"a": [1]})]
        with patch("pandas.read_parquet", return_value=pd.DataFrame({"a": [2]})) as read_parquet:
            assert _local_result_set(fs, tables=tables, **{option: value}).fetchall() == [(2,)]
        # The S3 location, which pandas opens with the given options instead of PyAthena's.
        assert read_parquet.call_args.args[0] == "s3://bucket/unload/"
        assert read_parquet.call_args.kwargs[option] == value
        assert read_parquet.call_args.kwargs.get("filesystem") is None

    @pytest.mark.parametrize(
        ("data", "types", "chunksize", "kwargs", "prefetch"),
        [
            pytest.param(_TEXT_CSV, [("v", "varchar")], None, {}, True, id="text"),
            pytest.param(_TEXT_CSV, [("v", "varchar")], 1, {}, True, id="text-chunks"),
            pytest.param('"b"\n"00 ff"\n', [("b", "varbinary")], None, {}, True, id="binary"),
            pytest.param(
                _TEXT_CSV,
                [("v", "varchar")],
                None,
                {"block_size": 5 * 2**20},
                False,
                id="block_size",
            ),
            pytest.param(
                _TEXT_CSV, [("v", "varchar")], None, {"cache_type": "bytes"}, False, id="cache_type"
            ),
        ],
    )
    def test_csv_open_options(self, tmp_path, data, types, chunksize, kwargs, prefetch):
        """A CSV result is read in 32 MiB blocks with the background cache, keeping two blocks.

        With block_size or cache_type, the file is opened with the filesystem's settings.
        """
        fs = LocalBucketFileSystem(tmp_path)
        result_set = _local_result_set(fs, types, data, chunksize=chunksize, **kwargs)
        assert len(result_set.fetchall()) == len(data.splitlines()) - 1
        [(options, _)] = fs.opened
        if prefetch:
            assert options["cache_type"] == "background"
            assert options["block_size"] == 32 * 2**20
            assert options["cache_options"] == {"maxblocks": 2}
        else:
            assert "cache_type" not in options
            assert "block_size" not in options

    @pytest.mark.parametrize(
        ("kwargs", "read_in_blocks"),
        [
            pytest.param({"block_size": 2**20}, True, id="block_size"),
            pytest.param({"cache_type": "none"}, False, id="cache_type"),
        ],
    )
    def test_csv_read_options_reach_file_system(self, kwargs, read_in_blocks):
        """block_size and cache_type set how PyAthena's filesystem reads the CSV result file.

        The file is read from S3 in blocks of block_size, or, with the "none" cache, only
        in the ranges that pandas reads, never the whole file at once as the filesystem's
        default block would. No AWS calls; the S3 client serves the file from memory.
        """
        data = b'"v"\n' + (b'"' + b"x" * 1000 + b'"\n') * 3000
        s3_object = _InMemoryS3Object(data)
        connection = MagicMock(retry_config=RetryConfig())
        connection.s3_client = boto3.session.Session().client(
            "s3", region_name="us-east-1", aws_access_key_id="key", aws_secret_access_key="secret"
        )
        connection.client.get_query_results.return_value = query_results_response(
            [("v", "varchar")]
        )
        with (
            patch.object(
                connection.s3_client,
                "head_object",
                side_effect=lambda **params: s3_object.answer("HeadObject", params),
            ),
            patch.object(
                connection.s3_client,
                "get_object",
                side_effect=lambda **params: s3_object.answer("GetObject", params),
            ),
        ):
            result_set = AthenaPandasResultSet(
                connection=connection,
                converter=DefaultPandasTypeConverter(),
                query_execution=AthenaQueryExecution(
                    {
                        "QueryExecution": {
                            "QueryExecutionId": "query_id",
                            "Query": "SELECT 1",
                            "Status": {"State": AthenaQueryExecution.STATE_SUCCEEDED},
                            "ResultConfiguration": {"OutputLocation": "s3://bucket/result.csv"},
                        }
                    }
                ),
                arraysize=1000,
                retry_config=RetryConfig(),
                engine="c",
                **kwargs,
            )
        assert len(result_set.as_pandas()) == 3000
        sizes = [end - start for start, end in s3_object.ranges]
        if read_in_blocks:
            # The parts that the filesystem's workers fetch are blocks of block_size.
            assert max(sizes) == kwargs["block_size"]
        else:
            assert len(data) < S3FileSystem.DEFAULT_BLOCK_SIZE
            assert max(sizes) < len(data)

    @pytest.mark.filterwarnings("error::pandas.errors.DtypeWarning")
    @pytest.mark.parametrize(
        ("json_value", "expected"), [("9007199254740993", 9007199254740993), ("true", True)]
    )
    @pytest.mark.parametrize(
        ("engine", "kwargs", "access"),
        [
            pytest.param("c", {}, "whole", id="c"),
            pytest.param("auto", {}, "whole", id="auto"),
            pytest.param("pyarrow", {}, "whole", id="pyarrow-fallback"),
            pytest.param("python", {}, "whole", id="python"),
            pytest.param("c", {"chunksize": 400005}, "iteration", id="iteration"),
            pytest.param("c", {"chunksize": 400010}, "get_chunk", id="get-chunk"),
            pytest.param("c", {"index_col": "j"}, "whole", id="index"),
            pytest.param("c", {"usecols": ["j"]}, "whole", id="usecols"),
        ],
    )
    def test_read_csv_json_null_without_dtype_warning(
        self, tmp_path, json_value, expected, engine, kwargs, access
    ):
        """JSON NULLs in a later parser block keep exact values without a warning."""
        data = "n,j\n" + f"1,{json_value}\n" * 400000 + "2,\n" * 10
        result = _read_local_csv(
            tmp_path, data, {"n": "integer", "j": "json"}, engine=engine, **kwargs
        )
        if access != "whole":
            with result:
                first = result.get_chunk(400005) if access == "get_chunk" else next(result)
                assert len(first) == 400005
                df = pd.concat([first, *result])
        else:
            df = result
        values = df.index if kwargs.get("index_col") == "j" else df["j"]
        assert len(df) == 400010
        assert values.dtype == object
        assert type(values[0]) is type(expected)
        assert (values[:400000] == expected).all()
        assert values[-10:].tolist() == [None] * 10

    @pytest.mark.filterwarnings("error::pandas.errors.DtypeWarning")
    @pytest.mark.parametrize("kwargs", [{}, {"low_memory": True}])
    def test_read_csv_json_numbers_in_a_parser_block_without_null(self, tmp_path, kwargs):
        """JSON integers keep their type when their parser block has a float but no NULL."""
        data = "n,j\n1,1.5\n" + "1,7\n" * 400000 + "2,\n" * 10
        df = _read_local_csv(tmp_path, data, {"n": "integer", "j": "json"}, **kwargs)
        values = df["j"]
        assert values.dtype == object
        assert values[0] == 1.5
        assert type(values[1]) is int
        assert (values[1:400001] == 7).all()
        assert values[-10:].tolist() == [None] * 10

    @pytest.mark.filterwarnings("error::pandas.errors.DtypeWarning")
    @pytest.mark.parametrize("engine", ["c", "python"])
    @pytest.mark.parametrize(
        ("text", "dtype", "values"),
        [
            pytest.param("2\n3,4", "int64", [2, 4], id="integers"),
            pytest.param("2\n3,4.5", "float64", [2.0, 4.5], id="numbers"),
            pytest.param("true\n3,false", "bool", [True, False], id="booleans"),
            pytest.param('"""a"""\n3,"""b"""', "str", ["a", "b"], id="strings"),
            pytest.param('"{""a"": 1}"\n3,[1]', object, [{"a": 1}, [1]], id="containers"),
        ],
    )
    def test_read_csv_json_without_null(self, tmp_path, engine, text, dtype, values):
        """JSON without NULL gets the dtype that pandas infers for a converter's values."""
        df = _read_local_csv(
            tmp_path, f"n,j\n1,{text}\n", {"n": "integer", "j": "json"}, engine=engine
        )
        assert df["j"].dtype == dtype
        assert df["j"].tolist() == values

    @pytest.mark.filterwarnings("ignore:Both a converter and dtype:pandas.errors.ParserWarning")
    @pytest.mark.parametrize("engine", ["c", "python"])
    @pytest.mark.parametrize(
        ("dtype", "n_dtype", "m_dtype"),
        [
            pytest.param({"n": "int32"}, "int32", "int64", id="mapping"),
            pytest.param(
                defaultdict(lambda: "str", {"n": "int32"}), "int32", "str", id="defaultdict"
            ),
            pytest.param("str", "str", "str", id="scalar"),
            pytest.param({"n": "int32", "j": "Int64"}, "int32", "int64", id="json-column"),
        ],
    )
    def test_read_csv_json_with_dtype(self, tmp_path, engine, dtype, n_dtype, m_dtype):
        """A dtype given to execute() replaces PyAthena's and does not apply to json columns."""
        df = _read_local_csv(
            tmp_path,
            "n,m,j\n1,1,2\n3,4,\n",
            {"n": "integer", "m": "integer", "j": "json"},
            engine=engine,
            dtype=dtype,
        )
        assert df["n"].dtype == n_dtype
        assert df["m"].dtype == m_dtype
        assert df["j"].tolist() == [2, None]

    @pytest.mark.filterwarnings("ignore:Both a converter and dtype:pandas.errors.ParserWarning")
    @pytest.mark.parametrize(
        ("kwargs", "expected", "dtype"),
        [
            pytest.param({}, {"x": [1, 3], "x.1": [2, 4]}, "Int64", id="labels"),
            pytest.param({"dtype": {}}, {"x": [1, 3], "x.1": [2, 4]}, "int64", id="dtype"),
            pytest.param(
                {"dtype": {"x": "str"}},
                {"x": [1, 3], "x.1": ["002", "004"]},
                "str",
                id="dtype-of-json-column",
            ),
            pytest.param({"skiprows": [1]}, {"x": [3], "x.1": [4]}, "Int64", id="without-labels"),
        ],
    )
    def test_read_csv_json_column_with_the_same_name_as_another_column(
        self, tmp_path, kwargs, expected, dtype
    ):
        """A column renamed from the name of a json column is read with its own options."""
        df = _read_local_csv(
            tmp_path, "x,x\n1,002\n3,004\n", [("x", "json"), ("x", "integer")], **kwargs
        )
        assert df.to_dict("list") == expected
        assert df["x.1"].dtype == dtype

    @pytest.mark.parametrize("engine", ["c", "python"])
    @pytest.mark.parametrize("kwargs", [{}, {"index_col": "j"}])
    def test_read_csv_json_parse_dates(self, tmp_path, engine, kwargs):
        """Dates that pandas parses from a json column with parse_dates are kept."""
        df = _read_local_csv(
            tmp_path,
            "n,j\n1,20240101\n",
            {"n": "integer", "j": "json"},
            engine=engine,
            parse_dates=["j"],
            **kwargs,
        )
        values = df.index if kwargs else df["j"]
        assert values.tolist() == [pd.Timestamp("2024-01-01")]

    @pytest.mark.parametrize("engine", ["c", "python"])
    def test_read_csv_single_json_column_with_null(self, tmp_path, engine):
        """NULL in a result with only a json column, an empty line, is None."""
        df = _read_local_csv(tmp_path, "j\n1\n\n", {"j": "json"}, engine=engine)
        assert df["j"].tolist() == [1, None]

    @pytest.mark.parametrize("chunksize", [None, 1])
    def test_read_csv_json_conversion_error(self, tmp_path, chunksize):
        """A json value that the converter cannot convert raises OperationalError.

        A chunked read raises when it reads the chunk.
        """

        def read():
            result = _read_local_csv(
                tmp_path, "n,j\n1,{\n", {"n": "integer", "j": "json"}, chunksize=chunksize
            )
            if chunksize:
                with result:
                    next(result)

        with pytest.raises(OperationalError):
            read()

    @pytest.mark.parametrize(("converters", "expected"), [({}, ["[1]"]), ({"j": len}, [3])])
    def test_read_csv_json_custom_converters(self, tmp_path, converters, expected):
        """Converters given to execute() replace the json conversion."""
        df = _read_local_csv(
            tmp_path, 'n,j\n1,"[1]"\n', {"n": "integer", "j": "json"}, converters=converters
        )
        assert df["j"].tolist() == expected

    @pytest.mark.parametrize(
        ("kwargs", "reader"),
        [
            pytest.param({"engine": "c"}, "c", id="c"),
            pytest.param({"engine": "python"}, "python", id="python"),
            pytest.param({"engine": "pyarrow"}, "pyathena-pyarrow", id="pyarrow"),
            # An option that PyAthena's PyArrow reader does not take: pandas reads the file.
            pytest.param(
                {"engine": "pyarrow", "encoding": "utf-8"}, "pyarrow", id="pandas-pyarrow"
            ),
            pytest.param({"chunksize": 1}, "c", id="chunks"),
        ],
    )
    @pytest.mark.parametrize("infer_string", [True, False])
    def test_temporal_types(self, tmp_path, kwargs, reader, infer_string):
        """Temporal columns have the dtype of their Athena type in every CSV read path.

        Fractions beyond microseconds are truncated, and aware values are held in UTC.
        The fetch methods return datetime, date, and time objects, and None for NULL.
        With ``future.infer_string``, the text is read as pyarrow-backed strings, and
        without it, as objects. The PyArrow engine reads results without the ``time
        with time zone`` columns, whose converter it does not take.
        """
        columns = temporal.columns()
        if reader.endswith("pyarrow"):
            columns = [c for c in columns if c.athena_type != "time with time zone"]
        with (
            pd.option_context("future.infer_string", infer_string),
            patch("pyarrow.csv.read_csv", wraps=pyarrow_csv.read_csv) as pyarrow_read,
            patch("pandas.read_csv", wraps=pd.read_csv) as pandas_read,
        ):
            self._check_temporal_types(tmp_path, columns, kwargs)
        # pandas' PyArrow engine also reads with pyarrow.csv.
        readers = {call.kwargs["engine"] for call in pandas_read.call_args_list} or (
            {"pyathena-pyarrow"} if pyarrow_read.called else set()
        )
        assert readers == {reader}

    @staticmethod
    def _check_temporal_types(tmp_path, columns, kwargs):
        """Check the dtypes, values, and fetched values of temporal corpus columns.

        Args:
            tmp_path: The directory of the result files.
            columns: The columns of ``tests/pyathena/temporal.py`` in the result.
            kwargs: The result set's options.
        """
        data = _temporal_csv(columns)
        assert len(data) >= AthenaPandasResultSet.PYARROW_MIN_FILE_SIZE_BYTES
        types = [(c.name, c.athena_type) for c in columns]
        df = _local_result_set(LocalBucketFileSystem(tmp_path / "df"), types, data, **kwargs)
        df = pd.concat(list(df.iter_chunks()))
        assert {name: str(dtype) for name, dtype in df.dtypes.items()} == {
            c.name: _TEMPORAL_DTYPES[c.athena_type] for c in columns
        }
        assert [None if pd.isna(v) else v for v in df.iloc[0]] == [
            None
            if c.value is None
            else pd.Timestamp(c.value).tz_convert("UTC")
            if c.athena_type == "timestamp with time zone"
            else pd.Timestamp(c.value)
            if c.athena_type in ("timestamp", "date")
            else c.value
            for c in columns
        ]
        result_set = _local_result_set(
            LocalBucketFileSystem(tmp_path / "rows"), types, data, **kwargs
        )
        row = result_set.fetchone()
        expected = tuple(c.value for c in columns)
        assert row == expected
        assert [type(v) for v in row] == [type(v) for v in expected]

    @pytest.mark.parametrize("infer_string", [True, False])
    @pytest.mark.parametrize("chunksize", [None, 2])
    def test_temporal_values_in_one_column(self, tmp_path, chunksize, infer_string):
        """The dtype of a temporal column does not depend on its values or chunks.

        A column holds 0 to 12 fractional digits, years 1 and 9999, and NULL, and a time
        zone column holds names and offsets.
        """
        data = (
            '"ts","tstz"\n'
            '"2020-01-02 03:04:05","2020-01-02 03:04:05 UTC"\n'
            '"0001-01-01 00:00:00.123456789012","2020-01-02 03:04:05.123 Asia/Tokyo"\n'
            ",\n"
            '"9999-12-31 23:59:59.999999999999","2020-01-02 03:04:05.123456789 +05:30"\n'
        )
        with pd.option_context("future.infer_string", infer_string):
            result_set = _local_result_set(
                LocalBucketFileSystem(tmp_path),
                [("ts", "timestamp"), ("tstz", "timestamp with time zone")],
                data,
                chunksize=chunksize,
            )
            chunks = list(result_set.iter_chunks())
        assert {(str(c["ts"].dtype), str(c["tstz"].dtype)) for c in chunks} == {
            ("datetime64[us]", "datetime64[us, UTC]")
        }
        df = pd.concat(chunks)
        assert df["ts"].tolist()[:2] + df["ts"].tolist()[3:] == [
            pd.Timestamp(2020, 1, 2, 3, 4, 5),
            pd.Timestamp(1, 1, 1, 0, 0, 0, 123456),
            pd.Timestamp(9999, 12, 31, 23, 59, 59, 999999),
        ]
        assert df["tstz"].tolist()[:2] + df["tstz"].tolist()[3:] == [
            pd.Timestamp(2020, 1, 2, 3, 4, 5, tz="UTC"),
            pd.Timestamp(2020, 1, 1, 18, 4, 5, 123000, tz="UTC"),
            pd.Timestamp(2020, 1, 1, 21, 34, 5, 123456, tz="UTC"),
        ]
        assert pd.isna(df["ts"].iloc[2])
        assert pd.isna(df["tstz"].iloc[2])

    # pandas parses the time column without a format.
    @pytest.mark.filterwarnings("ignore:Could not infer format")
    @pytest.mark.parametrize(
        "kwargs",
        [
            pytest.param({}, id="default"),
            # pandas parses the time column, which then becomes time objects.
            pytest.param({"dtype": {}}, id="pandas_parses"),
        ],
    )
    @pytest.mark.parametrize("chunksize", [None, 1])
    def test_time_index(self, tmp_path, kwargs, chunksize):
        """A time column that index_col reads as the index holds time objects, as a column does."""
        result_set = _local_result_set(
            LocalBucketFileSystem(tmp_path),
            [("tm", "time"), ("n", "integer")],
            f'"tm","n"\n"{_TIME_TEXT}","1"\n',
            chunksize=chunksize,
            index_col="tm",
            **kwargs,
        )
        df = pd.concat(list(result_set.iter_chunks()))
        assert df.index.tolist() == [_TIME]

    @pytest.mark.parametrize("chunksize", [None, 1])
    def test_unload_temporal_types(self, tmp_path, chunksize):
        """UNLOAD dates are datetime64[us], as CSV dates are, and fetch returns date objects."""
        tables = [
            pa.table(
                {
                    "ts": pa.array([datetime(1, 1, 1, 0, 0, 0, 123000), None], pa.timestamp("us")),
                    "d": pa.array([date(9999, 12, 31), None], pa.date32()),
                }
            )
        ]
        df = _local_result_set(
            LocalBucketFileSystem(tmp_path / "df"), tables=tables, chunksize=chunksize
        )
        df = pd.concat(list(df.iter_chunks()))
        assert df.dtypes.tolist() == ["datetime64[us]", "datetime64[us]"]
        result_set = _local_result_set(
            LocalBucketFileSystem(tmp_path / "rows"), tables=tables, chunksize=chunksize
        )
        rows = result_set.fetchall()
        assert rows == [(datetime(1, 1, 1, 0, 0, 0, 123000), date(9999, 12, 31)), (None, None)]
        assert [type(v) for v in rows[0]] == [datetime, date]

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"dtype_backend": "pyarrow"},
            {"coerce_int96_timestamp_unit": "ms"},
        ],
    )
    def test_unload_temporal_options(self, tmp_path, kwargs):
        """With read options that set the dtypes of UNLOAD dates, they are kept as given."""
        tables = [pa.table({"d": pa.array([date(2020, 1, 2)], pa.date32())})]
        result_set = _local_result_set(LocalBucketFileSystem(tmp_path), tables=tables, **kwargs)
        assert result_set.as_pandas()["d"].dtype != "datetime64[us]"

    @pytest.mark.parametrize(
        "kwargs",
        [
            pytest.param({}, id="default"),
            # With dtype given, pandas converts each value as it reads.
            pytest.param({"dtype": {}}, id="pandas_converts"),
            # skiprows leaves the column labels to pandas.
            pytest.param({"dtype": {}, "skiprows": 0}, id="pandas_converts_unlabeled"),
        ],
    )
    @pytest.mark.parametrize("chunksize", [None, 1])
    @pytest.mark.parametrize(
        ("value", "reason"),
        [
            ("2021-11-07 01:30:00.000 America/New_York", "is ambiguous"),
            ("2021-03-14 02:30:00.000 America/New_York", "does not exist"),
            ("2021-03-14 02:30:00.000 Mars/Base", "Unknown time zone"),
        ],
        ids=["ambiguous", "nonexistent", "unknown_zone"],
    )
    def test_time_zone_errors(self, tmp_path, kwargs, chunksize, value, reason):
        """A value without one instant raises DataError naming the column (#1217).

        A whole read raises in the constructor, and a chunked read when it reads the
        chunk with the value, also when pandas converts the values as it reads.
        """
        data = f'"tstz"\n"2020-01-02 03:04:05.000 UTC"\n"{value}"\n'

        def read():
            result_set = _local_result_set(
                LocalBucketFileSystem(tmp_path),
                [("tstz", "timestamp with time zone")],
                data,
                chunksize=chunksize,
                **kwargs,
            )
            return list(result_set.iter_chunks())

        with pytest.raises(DataError) as error:
            read()
        assert str(error.value).startswith("Column 'tstz': ")
        assert reason in str(error.value)

    @pytest.mark.filterwarnings("ignore:Both a converter and dtype:pandas.errors.ParserWarning")
    @pytest.mark.parametrize("case", ["mapping", "converters", "invalid_converters"])
    def test_read_error(self, tmp_path, case):
        """A whole read raises an error other than the time zone conversion's as OperationalError.

        This includes a DataError from another conversion function, such as one that the
        converter maps, which pandas applies with the time zone conversion when dtype
        is given.
        """

        def reject(value):
            raise DataError("invalid number")

        converter = DefaultPandasTypeConverter()
        if case == "mapping":
            converter.set("varchar", reject)
        kwargs = {
            "mapping": {"dtype": {}},
            "converters": {"converters": {"n": reject}},
            "invalid_converters": {"converters": [reject]},
        }[case]
        with pytest.raises(OperationalError):
            _local_result_set(
                LocalBucketFileSystem(tmp_path),
                [("tstz", "timestamp with time zone"), ("n", "varchar")],
                '"tstz","n"\n"2020-01-02 03:04:05.000 UTC","bad"\n',
                converter=converter,
                **kwargs,
            )

    @pytest.mark.parametrize(
        ("kwargs", "converter_types", "expected"),
        [
            # parse_dates given to execute() replaces PyAthena's: pandas parses the column.
            pytest.param(
                {"parse_dates": ["ts"]},
                None,
                pd.Timestamp("2020-01-02 03:04:05.123456789"),
                id="parse_dates",
            ),
            # pandas parses the dates of PyAthena's parse_dates after a dtype or converter
            # that names the column, without PyAthena's truncation.
            pytest.param(
                {"dtype": {"ts": "str"}},
                None,
                pd.Timestamp("2020-01-02 03:04:05.123456789"),
                id="dtype",
            ),
            # pandas keeps a value that parse_dates cannot parse as it is.
            pytest.param(
                {"converters": {"ts": lambda value: "converted"}},
                None,
                "converted",
                id="converters",
            ),
            pytest.param({}, {"timestamp": lambda value: "converted"}, "converted", id="converter"),
        ],
    )
    def test_temporal_options_take_precedence(self, tmp_path, kwargs, converter_types, expected):
        """Read options and conversion functions that name a temporal column decide its values.

        The fetch methods return the values as pandas holds them.
        """
        converter = DefaultPandasTypeConverter()
        for type_, function in (converter_types or {}).items():
            converter.set(type_, function)
        result_set = _local_result_set(
            LocalBucketFileSystem(tmp_path),
            [("ts", "timestamp")],
            '"ts"\n"2020-01-02 03:04:05.123456789"\n',
            converter=converter,
            **kwargs,
        )
        assert result_set.fetchone() == (expected,)
        assert type(result_set.as_pandas()["ts"].iloc[0]) is type(expected)

    def test_time_zone_before_first_transition(self, tmp_path):
        """Local times before a zone's first transition convert by the zone's offset then.

        pandas reports them as nonexistent; the zone's local mean time gives their
        instant. The other values of their zones keep the zones' later offsets.
        """
        data = (
            '"tstz"\n'
            '"1500-01-01 00:00:00.000 America/New_York"\n'
            '"2020-06-01 12:00:00.000 America/New_York"\n'
            '"0001-01-01 00:00:00.000 Asia/Tokyo"\n'
            '"2020-01-02 03:04:05.000 Asia/Tokyo"\n'
        )
        result_set = _local_result_set(
            LocalBucketFileSystem(tmp_path), [("tstz", "timestamp with time zone")], data
        )
        df = result_set.as_pandas()
        assert str(df["tstz"].dtype) == "datetime64[us, UTC]"
        values = df["tstz"].dt.tz_localize(None).to_numpy()
        # America/New_York's local mean time is -04:56:02 and Asia/Tokyo's +09:18:59.
        expected = [
            "1500-01-01T04:56:02",
            "2020-06-01T16:00:00",
            "0000-12-31T14:41:01",
            "2020-01-01T18:04:05",
        ]
        np.testing.assert_array_equal(values, np.array(expected, dtype="datetime64[us]"))

    @pytest.mark.parametrize("infer_string", [True, False])
    @pytest.mark.parametrize("type_", ["timestamp", "date", "timestamp with time zone", "time"])
    def test_temporal_conversion_error(self, tmp_path, type_, infer_string):
        """A value that fails to convert raises DataError naming the column."""
        value = {
            "timestamp": "2020-13-01 00:00:00.000",
            "date": "2020-13-01",
            "timestamp with time zone": "2020-13-01 00:00:00.000 UTC",
            "time": "25:00:00.000",
        }[type_]
        with (
            pd.option_context("future.infer_string", infer_string),
            pytest.raises(DataError) as error,
        ):
            _local_result_set(LocalBucketFileSystem(tmp_path), [("x", type_)], f'"x"\n"{value}"\n')
        assert str(error.value).startswith("Column 'x': ")

    @pytest.mark.filterwarnings("ignore:Both a converter and dtype:pandas.errors.ParserWarning")
    @pytest.mark.parametrize(
        "kwargs",
        [
            pytest.param({"converters": {"s": str.upper}}, id="converters"),
            pytest.param({"dtype": {"tstz": "str"}}, id="dtype_entry"),
            pytest.param({"dtype": "str"}, id="dtype"),
        ],
    )
    def test_time_zone_results_do_not_depend_on_engine(self, tmp_path, kwargs):
        """engine="pyarrow" gives the values of the C engine with options for the columns."""
        data = '"tstz","s"\n' + '"2020-01-02 03:04:05.000 Asia/Tokyo","a"\n' * 5
        types = [("tstz", "timestamp with time zone"), ("s", "varchar")]
        frames, rows = [], []
        for engine in ("c", "pyarrow"):
            result_set = _local_result_set(
                LocalBucketFileSystem(tmp_path / engine), types, data, engine=engine, **kwargs
            )
            frames.append(result_set.as_pandas())
            rows.append(result_set.fetchall())
        assert_frame_equal(frames[1], frames[0])
        assert rows[1] == rows[0]

    @pytest.mark.parametrize("engine", ["c", "pyarrow"])
    @pytest.mark.parametrize("key", ["parse_dates", "dtype", "converters"])
    def test_temporal_options_none(self, tmp_path, engine, key):
        """parse_dates, dtype, or converters of None, their default, is as if not given.

        With dtype of None, pyarrow parses the dates and the times without a fraction.
        """
        data = '"d","tm","n"\n' + '"2020-01-02","03:04:05","1"\n' * 10
        assert len(data) >= AthenaPandasResultSet.PYARROW_MIN_FILE_SIZE_BYTES
        result_set = _local_result_set(
            LocalBucketFileSystem(tmp_path),
            [("d", "date"), ("tm", "time"), ("n", "integer")],
            data,
            engine=engine,
            **{key: None},
        )
        assert str(result_set.as_pandas()["d"].dtype) == "datetime64[us]"
        assert result_set.fetchone() == (date(2020, 1, 2), time(3, 4, 5), 1)

    # pandas parses the time column without a format.
    @pytest.mark.filterwarnings("ignore:Could not infer format")
    @pytest.mark.parametrize("engine", ["c", "python", "pyarrow"])
    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            # PyAthena's parse_dates and converters read the temporal columns.
            pytest.param({"dtype": {"n": "Int64"}}, (_PARSED, _DATE, _TOKYO, _TIME, 1), id="dtype"),
            # The converters replace the timestamp with time zone conversion.
            pytest.param(
                {"converters": {}}, (_PARSED, _DATE, _TOKYO_TEXT, _TIME, 1), id="converters"
            ),
            # parse_dates replaces PyAthena's, and the columns it does not name stay text.
            pytest.param(
                {"parse_dates": ["ts"]},
                (_PARSED, "2020-01-02", _TOKYO, _TIME_TEXT, 1),
                id="parse_dates",
            ),
        ],
    )
    def test_temporal_options_read_by_pandas(self, tmp_path, engine, kwargs, expected):
        """With parse_dates, dtype, or converters, pandas reads every temporal column.

        PyAthena's own options read them as before its conversion, and the fetch
        methods return the values as pandas holds them, with any engine.
        """
        data = '"ts","d","tstz","tm","n"\n' + (
            f'"{_TIMESTAMP_TEXT}","2020-01-02","{_TOKYO_TEXT}","{_TIME_TEXT}","1"\n' * 2
        )
        assert len(data) >= AthenaPandasResultSet.PYARROW_MIN_FILE_SIZE_BYTES
        result_set = _local_result_set(
            LocalBucketFileSystem(tmp_path),
            [
                ("ts", "timestamp"),
                ("d", "date"),
                ("tstz", "timestamp with time zone"),
                ("tm", "time"),
                ("n", "integer"),
            ],
            data,
            engine=engine,
            **kwargs,
        )
        row = result_set.fetchone()
        assert row == expected
        # pandas keeps the zone of the time zone values.
        assert [v.utcoffset() for v in row if isinstance(v, datetime) and v.tzinfo] == [
            v.utcoffset() for v in expected if isinstance(v, datetime) and v.tzinfo
        ]

    @pytest.mark.parametrize(
        "kwargs",
        [
            pytest.param({"dtype": {"n": "Int64"}}, id="dtype"),
            # pyarrow reads the dates of a column that parse_dates does not name as date
            # objects, as pandas' PyArrow engine does.
            pytest.param({"parse_dates": ["ts", "d"]}, id="parse_dates"),
            # converters of None, their default, do not rule out the PyArrow engine.
            pytest.param({"parse_dates": ["ts", "d"], "converters": None}, id="converters_none"),
        ],
    )
    def test_temporal_options_read_by_pandas_with_pyarrow(self, tmp_path, kwargs):
        """The PyArrow engine reads temporal columns given these options as the C engine."""
        data = '"ts","d","n"\n' + f'"{_TIMESTAMP_TEXT}","2020-01-02","1"\n' * 3
        types = [("ts", "timestamp"), ("d", "date"), ("n", "integer")]
        frames, rows = [], []
        with patch("pyarrow.csv.read_csv", wraps=pyarrow_csv.read_csv) as pyarrow_read:
            for engine in ("c", "pyarrow"):
                result_set = _local_result_set(
                    LocalBucketFileSystem(tmp_path / engine), types, data, engine=engine, **kwargs
                )
                frames.append(result_set.as_pandas())
                rows.append(result_set.fetchall())
        assert pyarrow_read.called
        assert_frame_equal(frames[1], frames[0])
        assert rows[1] == rows[0]

    def test_custom_time_zone_converter(self, tmp_path):
        """A conversion function that the converter maps to timestamp with time zone wins."""
        converter = DefaultPandasTypeConverter()
        converter.set("timestamp with time zone", lambda value: value and value.lower())
        result_set = _local_result_set(
            LocalBucketFileSystem(tmp_path),
            [("tstz", "timestamp with time zone")],
            '"tstz"\n"2020-01-02 03:04:05.000 UTC"\n',
            converter=converter,
        )
        assert result_set.fetchone() == ("2020-01-02 03:04:05.000 utc",)


# A CSV result of Athena with each type the PyArrow engine reads, then a row of NULLs.
_TYPES_CSV = (
    '"ti","si","i","bi","r","d","c","v","ml","arr","m","rw","dt","ts","ts6","tm","iv","nul","u",'
    '"empty","na"\n'
    '"1","2","3","4","1.5","2.25","ab ","plain","multi\nline ""q"", x","[1, 2]","{k=1}",'
    '"{a=1, b=x}","2024-02-29","2024-02-29 23:59:58.123","2024-02-29 23:59:58.123456",'
    '"12:34:56.789","2 00:00:00.000",,"589f6631-9c50-4f58-a121-e2608a04fc64","","NA"\n'
    ",,,,,,,,,,,,,,,,,,,,\n"
)
_TYPES = {
    "ti": "tinyint",
    "si": "smallint",
    "i": "integer",
    "bi": "bigint",
    "r": "float",
    "d": "double",
    "c": "char",
    "v": "varchar",
    "ml": "varchar",
    "arr": "array",
    "m": "map",
    "rw": "row",
    "dt": "date",
    "ts": "timestamp",
    "ts6": "timestamp",
    "tm": "time",
    "iv": "interval day to second",
    "nul": "unknown",
    "u": "uuid",
    "empty": "varchar",
    "na": "varchar",
}


def _pyarrow_read_csv_kwargs(types, tab_separated=False, dtype_overrides=None, **kwargs):
    """Build the pandas.read_csv() options with AthenaPandasResultSet._get_csv_read_options().

    Args:
        types: The Athena types of the result columns, keyed by column name.
        tab_separated: Whether the result is a tab-separated ``.txt`` file.
        dtype_overrides: Entries to add to or replace in the default dtype mapping.
            A ``dtype`` in kwargs instead replaces the entire mapping.
        **kwargs: The pandas.read_csv() options given to ``execute()``.

    Returns:
        The options for the PyArrow engine.
    """
    result_set = AthenaPandasResultSet.__new__(AthenaPandasResultSet)
    result_set._converter = DefaultPandasTypeConverter()
    result_set._keep_default_na = False
    result_set._na_values = ("",)
    result_set._quoting = 1
    result_set._kwargs = kwargs
    description = [(name, type_, None, None, 0, 0, "UNKNOWN") for name, type_ in types.items()]
    location = f"s3://bucket/result.{'txt' if tab_separated else 'csv'}"
    with (
        patch.object(
            AthenaPandasResultSet,
            "description",
            new_callable=PropertyMock,
            return_value=description,
        ),
        patch.object(
            AthenaPandasResultSet,
            "output_location",
            new_callable=PropertyMock,
            return_value=location,
        ),
    ):
        if dtype_overrides is not None:
            result_set._kwargs["dtype"] = {**result_set.dtypes, **dtype_overrides}
        assert result_set._reads_csv_with_pyarrow()
        return result_set._get_csv_read_options("pyarrow", None)


def _read_local_csv(tmp_path, data, types, engine="c", chunksize=None, **kwargs):
    """Read CSV text as the result file of a result set built by its constructor.

    Args:
        tmp_path: The directory that holds the result file.
        data: The text of the result file.
        types: The Athena types of the result columns, keyed by name, or the
            (name, type) pairs of columns with the same name.
        engine: The CSV engine of the result set.
        chunksize: The chunk size of the result set.
        **kwargs: The read options given to ``execute()``.

    Returns:
        The DataFrame of the result, or its chunk iterator if ``chunksize`` is set.
    """
    pairs = list(types.items()) if isinstance(types, dict) else types
    result_set = _local_result_set(
        LocalBucketFileSystem(tmp_path), pairs, data, engine=engine, chunksize=chunksize, **kwargs
    )
    return result_set.as_pandas()


def _write_unload_tables(fs, tables, **write_options):
    """Write the Parquet files of an UNLOAD result under ``s3://bucket/unload/``.

    Args:
        fs: The filesystem of the files.
        tables: The tables of the files.
        **write_options: Options of ``pyarrow.parquet.write_table()``.

    Returns:
        The data manifest of the files.
    """
    fs.makedirs("bucket/unload", exist_ok=True)
    manifest = [f"s3://bucket/unload/{index}.parquet" for index in range(len(tables))]
    for path, table in zip(manifest, tables, strict=True):
        with fs.open(path, "wb") as file:
            pq.write_table(table, file, **write_options)
    fs.opened.clear()
    return manifest


class _InMemoryS3Object:
    """One S3 object served from memory to a boto3 client, with the ranges it reads.

    Attributes:
        ranges: The (start, end) byte ranges of the GetObject requests, end exclusive.
    """

    def __init__(self, data):
        """Initialize the object.

        Args:
            data: The bytes of the object.
        """
        self.data = data
        self.ranges = []

    def answer(self, operation, params):
        """Answer a request of the client as S3 would for this object.

        Args:
            operation: The operation name.
            params: The request parameters.

        Returns:
            The parsed response.
        """
        if operation == "HeadObject":
            return {
                "ContentLength": len(self.data),
                "ETag": '"etag"',
                "LastModified": datetime.now(UTC),
            }
        assert operation == "GetObject", operation
        assert (params["Bucket"], params["Key"]) == ("bucket", "result.csv"), params
        # Only the closed ranges that PyAthena's filesystem requests are answered.
        match = re.fullmatch(r"bytes=(\d+)-(\d+)", params["Range"])
        assert match, params["Range"]
        start, end = int(match[1]), int(match[2]) + 1
        assert start < len(self.data), params["Range"]
        self.ranges.append((start, end))
        body = self.data[start:end]
        return {
            "Body": StreamingBody(io.BytesIO(body), len(body)),
            "ContentLength": len(body),
            "ETag": '"etag"',
        }


def _local_result_set(
    fs,
    types=(),
    data=None,
    tables=None,
    name="result.csv",
    converter=None,
    describe=False,
    write_options=None,
    **kwargs,
):
    """Create a result set of a succeeded query over local result files, without AWS.

    The CSV ``data`` is the result file ``s3://bucket/<name>``; ``tables`` are the files
    of an UNLOAD result, which the connection's S3 client lists in the data manifest.
    ``fs`` holds the files and stands in for PyAthena's filesystem, ``types`` lists the
    (name, Athena type) pairs of the GetQueryResults metadata, ``converter`` replaces
    the default converter, ``write_options`` are the options of
    ``pyarrow.parquet.write_table()`` for ``tables``, and ``kwargs`` are the result
    set's options. With
    ``describe``, ``AthenaPandasResultSetMetadataReader.describe()`` returns the descriptions
    instead.
    """
    unload = tables is not None
    location = f"s3://bucket/{name}"
    if unload:
        manifest = _write_unload_tables(fs, tables, **(write_options or {}))
        length = 1
    else:
        manifest = []
        fs.makedirs("bucket", exist_ok=True)
        fs.pipe_file(location, data.encode())
        length = len(data.encode())
    query_execution = AthenaQueryExecution(
        {
            "QueryExecution": {
                "QueryExecutionId": "query_id",
                "Query": "UNLOAD (SELECT 1) TO 's3://bucket/unload/'" if unload else "SELECT 1",
                "Status": {"State": AthenaQueryExecution.STATE_SUCCEEDED},
                "ResultConfiguration": {"OutputLocation": location},
                "Statistics": {"DataManifestLocation": "s3://bucket/manifest.csv"},
            }
        }
    )
    connection = MagicMock()
    connection.s3_client.get_object.side_effect = lambda **_: {
        "Body": io.BytesIO("\n".join(manifest).encode())
    }
    connection.s3_client.meta.config.max_pool_connections = 2
    connection.s3_client.head_object.return_value = {"ContentLength": length}
    connection.client.get_query_results.return_value = query_results_response(types)
    with patch("pyathena.pandas.result_set.S3FileSystem", return_value=fs):
        if describe:
            return AthenaPandasResultSetMetadataReader(
                connection, query_execution, RetryConfig(), unload=unload, **kwargs
            ).describe()
        return AthenaPandasResultSet(
            connection=connection,
            converter=converter or DefaultPandasTypeConverter(),
            query_execution=query_execution,
            arraysize=1000,
            retry_config=RetryConfig(),
            unload=unload,
            **kwargs,
        )


def _temporal_csv(columns):
    """Write the temporal corpus as Athena writes a CSV result file: quoted text, NULL empty.

    Args:
        columns: The columns of ``tests/pyathena/temporal.py``.

    Returns:
        The text of the result file, with one row.
    """

    def text(column):
        if column.value is None:
            return ""
        # The text that Athena writes is the column's text, or else its SQL literal.
        value = column.text or re.search(r"'([^']*)'", column.sql)[1]
        return f'"{value}"'

    header = ",".join(f'"{c.name}"' for c in columns)
    return f"{header}\n{','.join(text(c) for c in columns)}\n"


# The dtypes of the temporal columns of a PandasCursor result (#1212).
_TEMPORAL_DTYPES = {
    "timestamp": "datetime64[us]",
    "date": "datetime64[us]",
    "timestamp with time zone": "datetime64[us, UTC]",
    "time": "object",
    "time with time zone": "object",
}


def _string_series(values, infer_string):
    """Express the expected string dtype for the active pandas option."""
    return pd.Series(values, dtype="str" if infer_string else object)


def _types_frame(infer_string, parse_time=True, temporal_text=False):
    """Build the typed literal expectation for the all-types CSV.

    With ``temporal_text``, the date, timestamp, and time columns are pyarrow strings,
    as PyAthena reads them for the PyArrow engine with its default options before
    converting them.
    """
    missing = float("nan")
    return pd.DataFrame(
        {
            "ti": pd.Series([1, None], dtype="Int64"),
            "si": pd.Series([2, None], dtype="Int64"),
            "i": pd.Series([3, None], dtype="Int64"),
            "bi": pd.Series([4, None], dtype="Int64"),
            "r": pd.Series([1.5, missing], dtype="float64"),
            "d": pd.Series([2.25, missing], dtype="float64"),
            "c": _string_series(["ab ", missing], infer_string),
            "v": _string_series(["plain", missing], infer_string),
            "ml": _string_series(['multi\nline "q", x', missing], infer_string),
            "arr": _string_series(["[1, 2]", missing], infer_string),
            "m": _string_series(["{k=1}", missing], infer_string),
            "rw": _string_series(["{a=1, b=x}", missing], infer_string),
            "dt": (
                pd.Series(["2024-02-29", None], dtype="string[pyarrow]")
                if temporal_text
                else pd.Series([pd.Timestamp("2024-02-29"), pd.NaT], dtype="datetime64[us]")
            ),
            "ts": (
                pd.Series(["2024-02-29 23:59:58.123", None], dtype="string[pyarrow]")
                if temporal_text
                else pd.Series(
                    [pd.Timestamp("2024-02-29 23:59:58.123"), pd.NaT], dtype="datetime64[ns]"
                )
            ),
            "ts6": (
                pd.Series(["2024-02-29 23:59:58.123456", None], dtype="string[pyarrow]")
                if temporal_text
                else pd.Series(
                    [pd.Timestamp("2024-02-29 23:59:58.123456"), pd.NaT], dtype="datetime64[ns]"
                )
            ),
            # pandas supplies today's date when it parses a time without a date.
            "tm": (
                pd.Series(["12:34:56.789", None], dtype="string[pyarrow]")
                if temporal_text
                else pd.Series(pd.to_datetime(["12:34:56.789", None]))
                if parse_time
                else _string_series(["12:34:56.789", None], infer_string)
            ),
            "iv": _string_series(["2 00:00:00.000", None], infer_string),
            "nul": pd.Series([missing, missing], dtype="float64"),
            "u": _string_series(["589f6631-9c50-4f58-a121-e2608a04fc64", None], infer_string),
            "empty": _string_series([missing, missing], infer_string),
            "na": _string_series(["NA", missing], infer_string),
        }
    )


@pytest.mark.filterwarnings("ignore:Could not infer format")
@pytest.mark.parametrize("infer_string", [True, False])
@pytest.mark.parametrize(
    ("data", "types", "read_options", "expected_frame", "pandas_columns"),
    [
        pytest.param(
            _TYPES_CSV,
            _TYPES,
            {},
            # The date, timestamp, and time columns are read as text and converted
            # afterwards.
            lambda infer: _types_frame(infer, temporal_text=True),
            [0, 1, 2, 3, 4, 5, 15, 16, 17, 18],
            id="types",
        ),
        pytest.param(
            _TYPES_CSV,
            _TYPES,
            {"dtype_overrides": {"ti": "float32", "v": "category", "missing": "int64"}},
            lambda infer: _types_frame(infer).astype({"ti": "float32", "v": "category"}),
            [0, 1, 2, 3, 4, 5, 7, 12, 13, 14, 15, 16, 17, 18],
            id="dtype",
        ),
        pytest.param(
            _TYPES_CSV,
            _TYPES,
            {"parse_dates": [12, "ts"]},
            lambda infer: _types_frame(infer, parse_time=False),
            [0, 1, 2, 3, 4, 5, 12, 13, 14, 15, 16, 17, 18],
            id="parse_dates",
        ),
        pytest.param(
            _TYPES_CSV,
            _TYPES,
            {"dtype_overrides": {"dt": "string"}},
            lambda infer: _types_frame(infer).assign(
                dt=pd.Series(["2024-02-29", None], dtype="string")
            ),
            [0, 1, 2, 3, 4, 5, 12, 13, 14, 15, 16, 17, 18],
            id="dtype_of_date_column",
        ),
        pytest.param(
            '"x","d"\n"1","2024-01-01"\n,\n',
            {"x": "integer", "d": "date"},
            {"dtype": {"x": None}},
            lambda infer: pd.DataFrame(
                {
                    "x": [1.0, float("nan")],
                    "d": pd.Series([pd.Timestamp("2024-01-01"), pd.NaT], dtype="datetime64[us]"),
                }
            ),
            [0, 1],
            id="dtype_none",
        ),
        pytest.param(
            '"x","x","d"\n"1","2","2024-01-01"\n,,\n',
            {"x": "integer", "d": "date"},
            {},
            lambda infer: pd.concat(
                [
                    pd.Series([1, None], name="x", dtype="Int64"),
                    pd.Series([2, None], name="x", dtype="Int64"),
                    pd.Series(["2024-01-01", None], name="d", dtype="string[pyarrow]"),
                ],
                axis=1,
            ),
            [0, 1],
            id="duplicate_names",
        ),
        pytest.param(
            '"v","n"\n"1","1"\n,\n"nan","3"\n"007","4"\n"1e3","5"\n',
            {"v": "varchar", "n": "integer"},
            {},
            lambda infer: pd.DataFrame(
                {
                    "v": _string_series(["1", float("nan"), "nan", "007", "1e3"], infer),
                    "n": pd.Series([1, None, 3, 4, 5], dtype="Int64"),
                }
            ),
            [1],
            id="numeric_looking_strings",
        ),
        pytest.param(
            '"v","w","x"\n"007","a","1"\n,,"2"\n',
            {"x": "integer"},
            {
                "dtype": {
                    "v": pd.ArrowDtype(pa.string()),
                    "w": pd.ArrowDtype(pa.large_string()),
                    "x": pd.Int64Dtype(),
                }
            },
            lambda infer: pd.DataFrame(
                {
                    "v": pd.Series(["007", None], dtype=pd.ArrowDtype(pa.string())),
                    "w": pd.Series(["a", None], dtype=pd.ArrowDtype(pa.large_string())),
                    "x": pd.Series([1, 2], dtype="Int64"),
                }
            ),
            [2],
            id="arrow_string_dtypes",
        ),
        pytest.param(
            "001\t2\t003\n004\t5\t\n",
            {"v": "varchar"},
            {"tab_separated": True},
            lambda infer: pd.DataFrame(
                {"0": [1, 4], "1": [2, 5], "v": _string_series(["3", float("nan")], infer)}
            ),
            [0, 1],
            id="tab_separated_numeric_fields",
        ),
        pytest.param(
            '"v","n"\n"007","1"\n',
            {"v": "varchar", "n": "integer"},
            {"dtype": {"v": str, 0: str}},
            lambda infer: pd.DataFrame({"v": _string_series(["007"], infer), "n": [1]}),
            [1],
            id="dtype_position_key",
        ),
        pytest.param(
            '"v"\n"plain"\n"2024-01-01"\n\n',
            {"v": "varchar"},
            {"parse_dates": ["v"]},
            lambda infer: pd.DataFrame(
                {"v": _string_series(["plain", "2024-01-01", float("nan")], infer)}
            ),
            [],
            id="unparsed_dates",
        ),
        pytest.param(
            '"v"\n"2024-01-01"\n\n',
            {"v": "varchar"},
            {"parse_dates": ["v"]},
            # The string dtype applies again after parse_dates, and NULL stays missing.
            lambda infer: pd.DataFrame({"v": _string_series(["2024-01-01", float("nan")], infer)}),
            [],
            id="parsed_dates",
        ),
        pytest.param(
            "id    \tint    \t    \nname  \tstring \t    \n",
            {"col_name": "varchar"},
            {"tab_separated": True},
            lambda infer: pd.DataFrame(
                {
                    "0": _string_series(["id    ", "name  "], infer),
                    "1": _string_series(["int    ", "string "], infer),
                    "col_name": _string_series(["    ", "    "], infer),
                }
            ),
            [0, 1, 2],
            id="tab_separated_extra_fields",
        ),
        pytest.param(
            "x\t1\t2024-01-01\n\t\t\ny y\t3\t2024-01-02\n",
            {"a": "varchar", "b": "bigint", "c": "date"},
            {"tab_separated": True},
            lambda infer: pd.DataFrame(
                {
                    "a": _string_series(["x", float("nan"), "y y"], infer),
                    "b": pd.Series([1, None, 3], dtype="Int64"),
                    "c": pd.Series(["2024-01-01", None, "2024-01-02"], dtype="string[pyarrow]"),
                }
            ),
            [1],
            id="tab_separated",
        ),
    ],
)
def test_read_csv_with_pyarrow_matches_pandas(
    data, types, read_options, expected_frame, pandas_columns, infer_string
):
    """CSV results match literal expectations and pandas where their contracts agree."""
    with pd.option_context("future.infer_string", infer_string):
        read_csv_kwargs = _pyarrow_read_csv_kwargs(types, **read_options)
        expected = expected_frame(infer_string)
        actual = _read_csv_with_pyarrow(io.BytesIO(data.encode()), read_csv_kwargs)
        assert_frame_equal(actual, expected, check_exact=True)
        # Explicit positions retain duplicate names and headerless-column parity.
        # Mapped string columns use PyAthena's preservation contract instead.
        # pandas 3.0.0's PyArrow engine reads strings as the str dtype even with
        # future.infer_string off, which pandas 3.0.1 fixed.
        if pandas_columns and (infer_string or pd.__version__ != "3.0.0"):
            reference = pd.read_csv(
                io.BytesIO(data.encode()),
                **{**read_csv_kwargs, "dtype": dict(read_csv_kwargs["dtype"])},
            )
            assert_frame_equal(
                actual.iloc[:, pandas_columns], reference.iloc[:, pandas_columns], check_exact=True
            )


def test_read_csv_with_pyarrow_ignores_unused_dtype_entries():
    # As with pandas' PyArrow engine, a dtype entry for a column that is not in
    # the result is not validated.
    read_csv_kwargs = _pyarrow_read_csv_kwargs(
        {"v": "varchar"},
        dtype={"v": str, "unused": "not-a-dtype", "unsupported": "decimal128(10, 2)[pyarrow]"},
    )
    data = b'"v"\n"007"\n'
    actual = _read_csv_with_pyarrow(
        io.BytesIO(data), {**read_csv_kwargs, "dtype": dict(read_csv_kwargs["dtype"])}
    )
    assert actual.columns.tolist() == ["v"]
    assert actual["v"].tolist() == ["007"]


def test_read_csv_with_pyarrow_multiline_values_across_blocks():
    # The 2.4 MB file spans several 1 MiB pyarrow read blocks, and its values
    # contain newlines and quotes.
    rows = [(i, f'{"x" * 600}\n"{"y" * 598}"' if i % 2 else "z") for i in range(4000)]
    buffer = io.StringIO()
    writer = csv.writer(buffer, quoting=csv.QUOTE_ALL, lineterminator="\n")
    writer.writerow(["id", "v"])
    writer.writerows(rows)
    df = _read_csv_with_pyarrow(
        io.BytesIO(buffer.getvalue().encode()),
        _pyarrow_read_csv_kwargs({"id": "integer", "v": "varchar"}),
    )
    assert df["id"].tolist() == [i for i, _ in rows]
    assert df["v"].tolist() == [v for _, v in rows]


def _fetching_result_set(result, arraysize):
    """Create a result set of a succeeded query whose read gives ``result``, without AWS.

    ``result`` is a DataFrame or an iterator of DataFrame chunks, as ``_as_pandas()``
    returns them.
    """
    query_execution = MagicMock(
        state=AthenaQueryExecution.STATE_SUCCEEDED, output_location="s3://bucket/result.csv"
    )
    with (
        patch.object(AthenaPandasResultSet, "_load_metadata"),
        patch("pyathena.pandas.result_set.S3FileSystem"),
        patch.object(AthenaPandasResultSet, "_as_pandas", return_value=result),
        patch.object(
            AthenaPandasResultSet, "is_unload", new_callable=PropertyMock, return_value=True
        ),
    ):
        return AthenaPandasResultSet(
            connection=MagicMock(),
            converter=DefaultPandasTypeConverter(),
            query_execution=query_execution,
            arraysize=arraysize,
            retry_config=RetryConfig(),
        )
