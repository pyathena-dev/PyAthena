# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import csv
import io
from unittest.mock import MagicMock, patch

import pandas as pd
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


def _pyarrow_read_csv_kwargs(types, tab_separated=False, **kwargs):
    """Build the pandas.read_csv() options that PandasCursor passes to the PyArrow engine."""
    converter = DefaultPandasTypeConverter()
    return {
        "sep": "\t" if tab_separated else ",",
        "header": None if tab_separated else 0,
        "names": list(types) if tab_separated else None,
        "dtype": {
            name: dtype
            for name, type_ in types.items()
            if (dtype := converter.get_dtype(type_, 0, 0)) is not None
        },
        "parse_dates": [
            name for name, type_ in types.items() if type_ in ("date", "time", "timestamp")
        ],
        "skip_blank_lines": False,
        "keep_default_na": False,
        "na_values": ("",),
        **kwargs,
    }


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
        "tab_separated",
    ],
)
def test_read_csv_with_pyarrow_matches_pandas(data, read_csv_kwargs, infer_string):
    # Without values that cross a read block, the result is the one of
    # pandas.read_csv(engine="pyarrow").
    with pd.option_context("future.infer_string", infer_string):
        expected = pd.read_csv(
            io.BytesIO(data.encode()),
            engine="pyarrow",
            **{**read_csv_kwargs, "dtype": dict(read_csv_kwargs["dtype"])},
        )
        actual = _read_csv_with_pyarrow(
            io.BytesIO(data.encode()),
            {**read_csv_kwargs, "dtype": dict(read_csv_kwargs["dtype"])},
        )
    assert_frame_equal(actual, expected, check_exact=True)


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
