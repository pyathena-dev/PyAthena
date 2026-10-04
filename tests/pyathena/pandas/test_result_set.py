# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import csv
import io
from unittest.mock import MagicMock, PropertyMock, patch

import pandas as pd
import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal

from pyathena.pandas.converter import DefaultPandasTypeConverter
from pyathena.pandas.result_set import (
    AthenaPandasResultSet,
    PandasDataFrameIterator,
    _no_trunc_date,
    _read_csv_with_pyarrow,
)


class TestPandasDataFrameIterator:
    @pytest.mark.parametrize(
        ("csv", "read_csv_kwargs"),
        [
            ("id,kind\n10,a\n11,a\n12,b\n13,b\n14,c\n", {}),
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

    def test_as_pandas_single_dataframe(self):
        """A DataFrame that was read at once is returned as is."""
        df = pd.DataFrame({"n": [1, 2]})
        df_iter = PandasDataFrameIterator(df, _no_trunc_date)

        assert df_iter.as_pandas() is df


_FS = MagicMock(name="pyathena_fs")
_USER_FS = MagicMock(name="user_fs")


class TestAthenaPandasResultSet:
    @pytest.mark.parametrize("engine", ["auto", "c", "python", "pyarrow"])
    @pytest.mark.parametrize(
        ("output_location", "file_size_bytes", "pyarrow_engine"),
        [
            ("s3://bucket/result.txt", None, "c"),
            ("s3://bucket/result.txt", 99, "c"),
            ("s3://bucket/result.txt", 100, "c"),
            ("s3://bucket/result.txt", 101, "c"),
            ("s3://bucket/result.csv", None, "pyarrow"),
            ("s3://bucket/result.csv", 99, "c"),
            ("s3://bucket/result.csv", 100, "pyarrow"),
            ("s3://bucket/result.csv", 101, "pyarrow"),
            (None, None, "pyarrow"),
        ],
    )
    def test_get_csv_engine_result_format(
        self, engine, output_location, file_size_bytes, pyarrow_engine
    ):
        """PyArrow falls back for DDL text while C and Python choices are preserved."""
        result_set = AthenaPandasResultSet.__new__(AthenaPandasResultSet)
        result_set._query_execution = MagicMock(output_location=output_location)
        result_set._metadata = None
        result_set._converter = DefaultPandasTypeConverter()
        result_set._engine = engine
        result_set._chunksize = None
        result_set._quoting = csv.QUOTE_ALL
        result_set._kwargs = {}

        expected = {"auto": "c", "c": "c", "python": "python", "pyarrow": pyarrow_engine}[engine]
        assert result_set._get_csv_engine(file_size_bytes) == expected

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
        self, data, names, expected_names, read_csv_kwargs, infer_string
    ):
        """Reading DDL results preserves numeric-looking names and their padding."""
        data = data.encode()
        assert len(data) >= AthenaPandasResultSet.PYARROW_MIN_FILE_SIZE_BYTES

        result_set = AthenaPandasResultSet.__new__(AthenaPandasResultSet)
        result_set._query_execution = MagicMock(
            output_location="s3://bucket/result.txt", substatement_type=None
        )
        result_set._converter = DefaultPandasTypeConverter()
        result_set._engine = "pyarrow"
        result_set._chunksize = None
        result_set._auto_optimize_chunksize = False
        result_set._quoting = csv.QUOTE_ALL
        result_set._keep_default_na = False
        result_set._na_values = ("",)
        result_set._kwargs = read_csv_kwargs
        result_set._fs = MagicMock()
        result_set._fs.open.return_value = stream = io.BytesIO(data)
        description = [(name, "string", None, None, 0, 0, "UNKNOWN") for name in names]

        with (
            pd.option_context("future.infer_string", infer_string),
            patch.object(
                AthenaPandasResultSet,
                "description",
                new_callable=PropertyMock,
                return_value=description,
            ),
            patch.object(result_set, "_get_content_length", return_value=len(data)),
            patch("pandas.read_csv", wraps=pd.read_csv) as read_csv,
        ):
            expected = pd.read_csv(io.BytesIO(data), **result_set._get_csv_read_options("c", None))
            read_csv.reset_mock()
            actual = result_set._read_csv()

        assert_frame_equal(actual, expected, check_exact=True)
        assert actual.iloc[:, 0].tolist() == expected_names
        read_csv.assert_called_once()
        assert read_csv.call_args.kwargs["engine"] == "c"
        assert stream.closed

    @pytest.mark.parametrize(
        ("execute_kwargs", "path", "filesystem_kwargs"),
        [
            ({}, "bucket/unload/", {"filesystem": _FS}),
            ({"filesystem": _USER_FS}, "bucket/unload/", {"filesystem": _USER_FS}),
            ({"filesystem": None}, "s3://bucket/unload/", {"filesystem": None}),
            ({"storage_options": {"anon": True}}, "s3://bucket/unload/", {}),
            ({"storage_options": None}, "s3://bucket/unload/", {}),
        ],
        ids=["default", "filesystem", "filesystem-none", "storage-options", "storage-options-none"],
    )
    def test_read_parquet_filesystem(self, execute_kwargs, path, filesystem_kwargs):
        """filesystem or storage_options given to execute() replace PyAthena's filesystem.

        No AWS calls; the manifest and pandas.read_parquet are mocked.
        """
        result_set = AthenaPandasResultSet.__new__(AthenaPandasResultSet)  # bypass __init__
        result_set._unload_location = None
        result_set._engine = "pyarrow"
        result_set._fs = _FS
        result_set._kwargs = dict(execute_kwargs)
        with (
            patch.object(
                AthenaPandasResultSet,
                "_read_data_manifest",
                return_value=["s3://bucket/unload/0.parquet"],
            ),
            patch("pandas.read_parquet") as read_parquet,
        ):
            result_set._read_parquet("pyarrow")
        assert read_parquet.call_args.args == (path,)
        assert read_parquet.call_args.kwargs == {
            "engine": "pyarrow",
            "use_threads": True,
            **execute_kwargs,
            **filesystem_kwargs,
        }


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


def _is_string_dtype(value):
    """Return whether a dtype mapping value is a string dtype, ignoring invalid values."""
    try:
        dtype = pd.api.types.pandas_dtype(value)
    except TypeError:
        return False
    return isinstance(dtype, pd.StringDtype) or dtype.kind == "U"


def _pyarrow_read_csv_kwargs(types, tab_separated=False, **kwargs):
    """Build the pandas.read_csv() options with AthenaPandasResultSet._get_csv_read_options().

    Args:
        types: The Athena types of the result columns, keyed by column name.
        tab_separated: Whether the result is a tab-separated ``.txt`` file.
        **kwargs: The pandas.read_csv() options given to ``execute()``.

    Returns:
        The options for the PyArrow engine.
    """
    with patch("pyathena.pandas.result_set.AthenaResultSet.__init__", return_value=None):
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
        assert result_set._reads_csv_with_pyarrow()
        return result_set._get_csv_read_options("pyarrow", None)


def _read_csv_result(data, types, engine="c", chunksize=None, **kwargs):
    """Read an in-memory CSV through the result set and the real pandas parser."""
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
    result_set._fs = MagicMock()
    result_set._fs.open.return_value = io.BytesIO(data.encode())
    description = [(name, type_, None, None, 0, 0, "UNKNOWN") for name, type_ in types.items()]
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
        patch("pandas.read_csv", wraps=pd.read_csv) as read_csv,
        patch(
            "pyathena.pandas.result_set._read_csv_with_pyarrow",
            wraps=_read_csv_with_pyarrow,
        ) as read_pyarrow,
    ):
        result = result_set._read_csv()
    options = read_pyarrow.call_args.args[1] if read_pyarrow.called else read_csv.call_args.kwargs
    if isinstance(result, pd.DataFrame):
        return result_set._finish_csv_frame(result), options
    return PandasDataFrameIterator(
        result, result_set._finish_csv_frame, result_set._csv_stream
    ), options


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
        pytest.param("c", {"chunksize": 400005}, "iteration", id="iteration"),
        pytest.param("c", {"chunksize": 400010}, "get_chunk", id="get-chunk"),
        pytest.param("c", {"index_col": "j"}, "whole", id="index"),
        pytest.param("c", {"usecols": ["j"]}, "whole", id="usecols"),
    ],
)
def test_read_csv_json_null_without_dtype_warning(json_value, expected, engine, kwargs, access):
    """JSON NULLs in a later parser block keep exact values without a warning."""
    data = "n,j\n" + f"1,{json_value}\n" * 400000 + "2,\n" * 10
    result, options = _read_csv_result(data, {"n": "integer", "j": "json"}, engine=engine, **kwargs)
    if access != "whole":
        with result:
            first = result.get_chunk(400005) if access == "get_chunk" else next(result)
            assert len(first) == 400005
            df = pd.concat([first, *result])
    else:
        df = result
    values = df.index if kwargs.get("index_col") == "j" else df["j"]
    assert options["engine"] == "c"
    assert options["low_memory"] is False
    assert len(df) == 400010
    assert values.dtype == object
    assert type(values[0]) is type(expected)
    assert (values[:400000] == expected).all()
    assert values[-10:].tolist() == [None] * 10


def test_read_csv_json_explicit_low_memory_true():
    """An explicit low_memory=True keeps pandas' internal block parsing and warning."""
    data = "n,j\n" + "1,9007199254740993\n" * 400000 + "2,\n" * 10
    with pytest.warns(pd.errors.DtypeWarning, match="have mixed types"):
        df, options = _read_csv_result(data, {"n": "integer", "j": "json"}, low_memory=True)
    assert options["low_memory"] is True
    assert df["j"].iloc[0] == 9007199254740993
    assert df["j"].iloc[-10:].tolist() == [None] * 10


@pytest.mark.filterwarnings("error::pandas.errors.DtypeWarning")
@pytest.mark.parametrize("low_memory", [None, True, False])
def test_read_csv_json_without_null(low_memory):
    """JSON without NULL keeps its inferred dtype with default or explicit low_memory."""
    kwargs = {} if low_memory is None else {"low_memory": low_memory}
    df, options = _read_csv_result("n,j\n1,2\n3,4\n", {"n": "integer", "j": "json"}, **kwargs)
    assert options["low_memory"] is (False if low_memory is None else low_memory)
    assert df["j"].dtype == "int64"
    assert df["j"].tolist() == [2, 4]


@pytest.mark.parametrize(
    ("types", "engine", "kwargs"),
    [
        pytest.param({"n": "integer", "j": "integer"}, "c", {}, id="no-json"),
        pytest.param({"n": "integer", "j": "json"}, "c", {"usecols": ["n"]}, id="json-excluded"),
        pytest.param(
            {"n": "integer", "j": "json"}, "c", {"converters": {}}, id="custom-converters"
        ),
        pytest.param(
            {"n": "integer", "j": "json"},
            "c",
            {"converters": {"j": int}},
            id="custom-json-converter",
        ),
        pytest.param({"n": "integer", "j": "json"}, "python", {}, id="python"),
        pytest.param({"n": "integer", "j": "integer"}, "pyarrow", {}, id="pyarrow"),
    ],
)
def test_read_csv_without_json_c_converter_keeps_low_memory_default(types, engine, kwargs):
    """Other columns, converters, and engines do not get a low_memory option."""
    df, options = _read_csv_result("n,j\n" + "1,2\n" * 30, types, engine=engine, **kwargs)
    assert options["engine"] == engine
    assert "low_memory" not in options
    assert len(df) == 30


@pytest.mark.filterwarnings("ignore:Could not infer format")
@pytest.mark.parametrize("infer_string", [True, False])
@pytest.mark.parametrize(
    ("data", "read_csv_kwargs"),
    [
        (_TYPES_CSV, _pyarrow_read_csv_kwargs(_TYPES)),
        (
            _TYPES_CSV,
            _pyarrow_read_csv_kwargs(
                _TYPES,
                dtype={
                    **_pyarrow_read_csv_kwargs(_TYPES)["dtype"],
                    "ti": "float32",
                    "v": "category",
                    "missing": "int64",
                },
            ),
        ),
        (_TYPES_CSV, _pyarrow_read_csv_kwargs(_TYPES, parse_dates=[12, "ts"])),
        (
            _TYPES_CSV,
            _pyarrow_read_csv_kwargs(
                _TYPES, dtype={**_pyarrow_read_csv_kwargs(_TYPES)["dtype"], "dt": "string"}
            ),
        ),
        (
            '"x","d"\n"1","2024-01-01"\n,\n',
            _pyarrow_read_csv_kwargs({"x": "integer", "d": "date"}, dtype={"x": None}),
        ),
        (
            '"x","x","d"\n"1","2","2024-01-01"\n,,\n',
            _pyarrow_read_csv_kwargs({"x": "integer", "d": "date"}),
        ),
        (
            '"v","n"\n"1","1"\n,\n"nan","3"\n"007","4"\n"1e3","5"\n',
            _pyarrow_read_csv_kwargs({"v": "varchar", "n": "integer"}),
        ),
        (
            '"v","w","x"\n"007","a","1"\n,,"2"\n',
            _pyarrow_read_csv_kwargs(
                {"x": "integer"},
                dtype={
                    "v": pd.ArrowDtype(pa.string()),
                    "w": pd.ArrowDtype(pa.large_string()),
                    "x": pd.Int64Dtype(),
                },
            ),
        ),
        (
            "001\t2\t003\n004\t5\t\n",
            _pyarrow_read_csv_kwargs({"v": "varchar"}, True),
        ),
        (
            '"v","n"\n"007","1"\n',
            _pyarrow_read_csv_kwargs(
                {"v": "varchar", "n": "integer"},
                dtype={**_pyarrow_read_csv_kwargs({"v": "varchar"})["dtype"], 0: str},
            ),
        ),
        (
            '"v"\n"plain"\n"2024-01-01"\n\n',
            _pyarrow_read_csv_kwargs({"v": "varchar"}, parse_dates=["v"]),
        ),
        (
            "id    \tint    \t    \nname  \tstring \t    \n",
            _pyarrow_read_csv_kwargs({"col_name": "varchar"}, True),
        ),
        (
            "x\t1\t2024-01-01\n\t\t\ny y\t3\t2024-01-02\n",
            _pyarrow_read_csv_kwargs({"a": "varchar", "b": "bigint", "c": "date"}, True),
        ),
    ],
    ids=[
        "types",
        "dtype",
        "parse_dates",
        "dtype_of_date_column",
        "dtype_none",
        "duplicate_names",
        "numeric_looking_strings",
        "arrow_string_dtypes",
        "tab_separated_numeric_fields",
        "dtype_position_key",
        "unparsed_dates",
        "tab_separated_extra_fields",
        "tab_separated",
    ],
)
def test_read_csv_with_pyarrow_matches_pandas(data, read_csv_kwargs, infer_string):
    # Without values that cross a read block, the result is the one of
    # pandas.read_csv(engine="pyarrow"), except that the columns with a string
    # dtype have the values of pandas' C engine, and in a header-less file, its
    # missing values. Where the C engine parses a parse_dates column despite its
    # string dtype, the PyArrow engine's applying the dtype again is kept.
    with pd.option_context("future.infer_string", infer_string):
        expected = pd.read_csv(
            io.BytesIO(data.encode()),
            **{**read_csv_kwargs, "dtype": dict(read_csv_kwargs["dtype"])},
        )
        c_engine = pd.read_csv(
            io.BytesIO(data.encode()),
            **{**read_csv_kwargs, "engine": "c", "dtype": dict(read_csv_kwargs["dtype"])},
        )
        string_columns = {
            column for column, value in read_csv_kwargs["dtype"].items() if _is_string_dtype(value)
        }
        for index, column in enumerate(expected.columns):
            if column not in string_columns or c_engine[column].dtype.kind == "M":
                continue
            if read_csv_kwargs["header"] is None:
                # Header-less fields keep the inferred types, and only their missing
                # values follow the C engine, which makes extra fields the index.
                missing = c_engine[column].isna().to_numpy()
                expected.isetitem(index, expected.iloc[:, index].mask(missing, float("nan")))
            else:
                expected.isetitem(index, c_engine[column].array)
        actual = _read_csv_with_pyarrow(
            io.BytesIO(data.encode()),
            {**read_csv_kwargs, "dtype": dict(read_csv_kwargs["dtype"])},
        )
    assert_frame_equal(actual, expected, check_exact=True)


@pytest.mark.parametrize("infer_string", [True, False])
def test_read_csv_with_pyarrow_string_dtype_after_dates(infer_string):
    # A string dtype applies again after parse_dates, as with pandas' PyArrow
    # engine, and keeps NULL missing.
    with pd.option_context("future.infer_string", infer_string):
        df = _read_csv_with_pyarrow(
            io.BytesIO(b'"v"\n"2024-01-01"\n\n'),
            _pyarrow_read_csv_kwargs({"v": "varchar"}, parse_dates=["v"]),
        )
    assert df["v"].tolist()[0] == "2024-01-01"
    assert pd.isna(df["v"].tolist()[1])


def test_read_csv_with_pyarrow_ignores_unused_dtype_entries():
    # As with pandas' PyArrow engine, a dtype entry for a column that is not in
    # the result is not validated.
    read_csv_kwargs = _pyarrow_read_csv_kwargs(
        {"v": "varchar"},
        dtype={"v": str, "unused": "not-a-dtype", "unsupported": "decimal128(10, 2)[pyarrow]"},
    )
    data = b'"v"\n"007"\n'
    expected = pd.read_csv(io.BytesIO(data), **{**read_csv_kwargs, "dtype": {"v": str}})
    actual = _read_csv_with_pyarrow(
        io.BytesIO(data), {**read_csv_kwargs, "dtype": dict(read_csv_kwargs["dtype"])}
    )
    assert actual.columns.tolist() == expected.columns.tolist() == ["v"]
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
