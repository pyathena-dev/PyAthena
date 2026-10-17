# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import csv
import inspect
import io
import threading
from collections import defaultdict
from unittest.mock import MagicMock, PropertyMock, patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fsspec.implementations.local import LocalFileSystem
from pandas.testing import assert_frame_equal

from pyathena.arrow.util import to_column_info
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.pandas.converter import DefaultPandasTypeConverter
from pyathena.pandas.result_set import (
    AthenaPandasResultSet,
    PandasDataFrameIterator,
    _no_trunc_date,
    _read_csv_with_pyarrow,
)
from pyathena.util import RetryConfig, retry_api_call, stop_retries_on
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
        result_set = _fetching_result_set(pd.DataFrame({"n": [1, 2]}), arraysize=3)
        result_set.close()
        assert result_set.fetchone() is None
        assert result_set.fetchmany() == []
        assert result_set.fetchall() == []
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

    def test_read_csv_with_pyarrow_engine_values(self, tmp_path):
        """engine="pyarrow" keeps the text of varchar values, numeric-looking and multiline."""
        values = ["007", "1e3", "nan", "line1\nline2", None, _PADDING]
        data = '"v","n"\n' + "".join(
            f'{"" if v is None else chr(34) + v + chr(34)},"{i}"\n' for i, v in enumerate(values)
        )
        assert len(data) >= AthenaPandasResultSet.PYARROW_MIN_FILE_SIZE_BYTES
        result_set = _local_result_set(
            LocalBucketFileSystem(tmp_path),
            [("v", "varchar"), ("n", "integer")],
            data,
            engine="pyarrow",
        )
        rows = result_set.fetchall()
        assert [(None if pd.isna(v) else v, n) for v, n in rows] == [
            (v, i) for i, v in enumerate(values)
        ]

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

    def test_iter_parquet_chunks_null_in_later_file(self, tmp_path):
        """Top-level integer and boolean columns with NULL anywhere convert alike in every chunk."""
        chunks, _ = _read_unload_result(
            tmp_path,
            [
                pa.table({"i": pa.array([1, 2], pa.int64()), "b": [True, False]}),
                pa.table({"i": pa.array([None, 4], pa.int64()), "b": [None, True]}),
            ],
            chunksize=1,
        )
        whole = pd.read_parquet(tmp_path)
        assert [len(chunk) for chunk in chunks] == [1, 1, 1, 1]
        for chunk in chunks:
            assert chunk.dtypes.tolist() == whole.dtypes.tolist()
        assert_frame_equal(pd.concat(chunks), whole, check_exact=True)

    def test_read_parquet_footers_stopped(self, tmp_path):
        """GH-1202: the footer reads run in the stop scope of their caller.

        A cancelled ``create_in_thread()`` of AioPandasCursor stops their retries too.
        """
        result_set, paths = _unload_result_set(
            tmp_path, [pa.table({"i": [1]}), pa.table({"i": [2]})], chunksize=1
        )
        stop = threading.Event()
        stop.set()
        operations = []

        def read_parquet_footer(path):
            operation = ThrottledOperation()
            operations.append(operation)
            retry_api_call(operation, RetryConfig(attempt=3, multiplier=0))

        with (
            patch.object(AthenaPandasResultSet, "_read_data_manifest", return_value=paths),
            patch.object(
                AthenaPandasResultSet, "_read_parquet_footer", side_effect=read_parquet_footer
            ),
            stop_retries_on(stop),
            pytest.raises(OperationalError, match="ThrottlingException"),
        ):
            result_set._read_parquet_footers()
        assert operations
        assert [operation.calls for operation in operations] == [1] * len(operations)

    def test_as_pandas_parquet_chunks_engine(self, tmp_path):
        """Chunks of an UNLOAD result need the pyarrow engine, as the whole result does."""
        result_set, paths = _unload_result_set(tmp_path, [pa.table({"i": [1]})], chunksize=1)
        result_set._engine = "c"
        with (
            patch.object(AthenaPandasResultSet, "_read_data_manifest", return_value=paths),
            patch.object(
                AthenaPandasResultSet, "is_unload", new_callable=PropertyMock, return_value=True
            ),
            pytest.raises(ProgrammingError, match="Engine must be `pyarrow`"),
        ):
            result_set._as_pandas()

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

        With block_size or cache_type, the file is opened with the filesystem's settings,
        which are created with them.
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
            assert fs.created_with["default_block_size"] == kwargs.get("block_size")
            assert fs.created_with["default_cache_type"] == kwargs.get("cache_type")

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
        self, json_value, expected, engine, kwargs, access
    ):
        """JSON NULLs in a later parser block keep exact values without a warning."""
        data = "n,j\n" + f"1,{json_value}\n" * 400000 + "2,\n" * 10
        result = _read_csv_result(data, {"n": "integer", "j": "json"}, engine=engine, **kwargs)
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
    def test_read_csv_json_numbers_in_a_parser_block_without_null(self, kwargs):
        """JSON integers keep their type when their parser block has a float but no NULL."""
        data = "n,j\n1,1.5\n" + "1,7\n" * 400000 + "2,\n" * 10
        df = _read_csv_result(data, {"n": "integer", "j": "json"}, **kwargs)
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
    def test_read_csv_json_without_null(self, engine, text, dtype, values):
        """JSON without NULL gets the dtype that pandas infers for a converter's values."""
        df = _read_csv_result(f"n,j\n1,{text}\n", {"n": "integer", "j": "json"}, engine=engine)
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
    def test_read_csv_json_with_dtype(self, engine, dtype, n_dtype, m_dtype):
        """A dtype given to execute() replaces PyAthena's and does not apply to json columns."""
        df = _read_csv_result(
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
        self, kwargs, expected, dtype
    ):
        """A column renamed from the name of a json column is read with its own options."""
        df = _read_csv_result("x,x\n1,002\n3,004\n", [("x", "json"), ("x", "integer")], **kwargs)
        assert df.to_dict("list") == expected
        assert df["x.1"].dtype == dtype

    @pytest.mark.parametrize("engine", ["c", "python"])
    @pytest.mark.parametrize("kwargs", [{}, {"index_col": "j"}])
    def test_read_csv_json_parse_dates(self, engine, kwargs):
        """Dates that pandas parses from a json column with parse_dates are kept."""
        df = _read_csv_result(
            "n,j\n1,20240101\n",
            {"n": "integer", "j": "json"},
            engine=engine,
            parse_dates=["j"],
            **kwargs,
        )
        values = df.index if kwargs else df["j"]
        assert values.tolist() == [pd.Timestamp("2024-01-01")]

    @pytest.mark.parametrize("engine", ["c", "python"])
    def test_read_csv_single_json_column_with_null(self, engine):
        """NULL in a result with only a json column, an empty line, is None."""
        df = _read_csv_result("j\n1\n\n", {"j": "json"}, engine=engine)
        assert df["j"].tolist() == [1, None]

    @pytest.mark.parametrize("chunksize", [None, 1])
    def test_read_csv_json_conversion_error(self, chunksize):
        """A json value that the converter cannot convert raises OperationalError.

        A chunked read raises when it reads the chunk.
        """

        def read():
            result = _read_csv_result(
                "n,j\n1,{\n", {"n": "integer", "j": "json"}, chunksize=chunksize
            )
            if chunksize:
                with result:
                    next(result)

        with pytest.raises(OperationalError):
            read()

    @pytest.mark.parametrize(("converters", "expected"), [({}, ["[1]"]), ({"j": len}, [3])])
    def test_read_csv_json_custom_converters(self, converters, expected):
        """Converters given to execute() replace the json conversion."""
        df = _read_csv_result(
            'n,j\n1,"[1]"\n', {"n": "integer", "j": "json"}, converters=converters
        )
        assert df["j"].tolist() == expected


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


def _unload_result_set(tmp_path, tables, chunksize, **kwargs):
    """Write Parquet files and create a result set of an UNLOAD result in them, without AWS.

    Returns:
        The result set, and the paths of the files in the order of the data manifest.
    """
    tmp_path.mkdir(parents=True, exist_ok=True)
    paths = []
    for index, table in enumerate(tables):
        path = tmp_path / f"{index}.parquet"
        pq.write_table(table, path)
        paths.append(str(path))
    result_set = AthenaPandasResultSet.__new__(AthenaPandasResultSet)  # bypass __init__
    result_set._fs = LocalFileSystem()
    result_set._engine = "auto"
    result_set._chunksize = chunksize
    result_set._cache_type = None
    result_set._connection = MagicMock()
    result_set._connection.s3_client.meta.config.max_pool_connections = 2
    result_set._kwargs = kwargs
    result_set._unload_location = None
    result_set._parquet_null_dtypes = {}
    return result_set, paths


def _read_unload_result(tmp_path, tables, chunksize, **kwargs):
    """Read Parquet files in chunks through the result set, without AWS.

    The column metadata must be what the dataset of the files reports, read without
    listing the unload location.

    Returns:
        The chunks, and the whole result read by pandas.read_parquet() with the same
        options.
    """
    result_set, paths = _unload_result_set(tmp_path, tables, chunksize, **kwargs)
    with (
        patch.object(AthenaPandasResultSet, "_read_data_manifest", return_value=paths),
        patch.object(
            AthenaPandasResultSet, "is_unload", new_callable=PropertyMock, return_value=True
        ),
        patch.object(AthenaPandasResultSet, "_read_parquet_schema") as read_parquet_schema,
    ):
        reader = result_set._as_pandas()
    read_parquet_schema.assert_not_called()
    dataset = pq.ParquetDataset(str(tmp_path), filesystem=LocalFileSystem())
    assert result_set._metadata == to_column_info(dataset.schema)
    chunks = list(reader)
    whole = pd.concat([pd.read_parquet(path, **kwargs) for path in paths], ignore_index=True)
    return chunks, whole


def _read_csv_result(data, types, engine="c", chunksize=None, **kwargs):
    """Read an in-memory CSV through the result set and the real pandas parser.

    ``types`` maps column names to Athena types, or lists (name, type) pairs for
    columns with the same name.
    """
    result_set = AthenaPandasResultSet.__new__(AthenaPandasResultSet)
    result_set._converter = DefaultPandasTypeConverter()
    result_set._keep_default_na = False
    result_set._na_values = ("",)
    result_set._quoting = 1
    result_set._engine = engine
    result_set._chunksize = chunksize
    result_set._auto_optimize_chunksize = False
    result_set._kwargs = kwargs
    result_set._time_columns = []
    result_set._csv_stream = None
    result_set._block_size = None
    result_set._cache_type = None
    result_set._fs = MagicMock()
    # Binary columns are read through a text stream.
    result_set._fs.open.side_effect = lambda path, mode="rb", **_: (
        io.StringIO(data, newline="") if "t" in mode else io.BytesIO(data.encode())
    )
    pairs = types.items() if isinstance(types, dict) else types
    description = [(name, type_, None, None, 0, 0, "UNKNOWN") for name, type_ in pairs]
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
            return_value="s3://bucket/result.csv",
        ),
        patch.object(
            AthenaPandasResultSet,
            "substatement_type",
            new_callable=PropertyMock,
            return_value=None,
        ),
        patch.object(AthenaPandasResultSet, "_get_content_length", return_value=len(data)),
    ):
        result = result_set._read_csv()
    if isinstance(result, pd.DataFrame):
        return result_set._finish_csv_frame(result)
    return PandasDataFrameIterator(result, result_set._finish_csv_frame, result_set._csv_stream)


def _write_unload_tables(fs, tables):
    """Write the Parquet files of an UNLOAD result under ``s3://bucket/unload/``.

    Returns:
        The data manifest of the files.
    """
    fs.makedirs("bucket/unload", exist_ok=True)
    manifest = [f"s3://bucket/unload/{index}.parquet" for index in range(len(tables))]
    for path, table in zip(manifest, tables, strict=True):
        with fs.open(path, "wb") as file:
            pq.write_table(table, file)
    fs.opened.clear()
    return manifest


def _local_result_set(fs, types=(), data=None, tables=None, name="result.csv", **kwargs):
    """Create a result set of a succeeded query over local result files, without AWS.

    The CSV ``data`` is the result file ``s3://bucket/<name>``; ``tables`` are the files
    of an UNLOAD result, which the connection's S3 client lists in the data manifest.
    ``fs`` holds the files and stands in for PyAthena's filesystem, ``types`` lists the
    (name, Athena type) pairs of the GetQueryResults metadata, and ``kwargs`` are the
    result set's options.
    """
    unload = tables is not None
    location = f"s3://bucket/{name}"
    if unload:
        manifest = _write_unload_tables(fs, tables)
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
    with patch("pyathena.pandas.result_set.S3FileSystem", return_value=fs) as file_system:
        result_set = AthenaPandasResultSet(
            connection=connection,
            converter=DefaultPandasTypeConverter(),
            query_execution=query_execution,
            arraysize=1000,
            retry_config=RetryConfig(),
            unload=unload,
            **kwargs,
        )
    # The options that PyAthena's filesystem was created with.
    fs.created_with = file_system.call_args.kwargs
    return result_set


def _string_series(values, infer_string):
    """Express the expected string dtype for the active pandas option."""
    return pd.Series(values, dtype="str" if infer_string else object)


def _types_frame(infer_string, parse_time=True):
    """Build the typed literal expectation for the all-types CSV."""
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
            "dt": pd.Series([pd.Timestamp("2024-02-29"), pd.NaT], dtype="datetime64[us]"),
            "ts": pd.Series(
                [pd.Timestamp("2024-02-29 23:59:58.123"), pd.NaT], dtype="datetime64[ns]"
            ),
            "ts6": pd.Series(
                [pd.Timestamp("2024-02-29 23:59:58.123456"), pd.NaT], dtype="datetime64[ns]"
            ),
            # pandas supplies today's date when it parses a time without a date.
            "tm": (
                pd.Series(pd.to_datetime(["12:34:56.789", None]))
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
            _types_frame,
            [0, 1, 2, 3, 4, 5, 12, 13, 14, 15, 16, 17, 18],
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
                    pd.Series(
                        [pd.Timestamp("2024-01-01"), pd.NaT], name="d", dtype="datetime64[us]"
                    ),
                ],
                axis=1,
            ),
            [0, 1, 2],
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
                    "c": pd.Series(
                        [pd.Timestamp("2024-01-01"), pd.NaT, pd.Timestamp("2024-01-02")],
                        dtype="datetime64[us]",
                    ),
                }
            ),
            [1, 2],
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
        if pandas_columns:
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
        patch.object(AthenaPandasResultSet, "_pre_fetch"),
        patch.object(AthenaPandasResultSet, "_create_s3_file_system"),
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
