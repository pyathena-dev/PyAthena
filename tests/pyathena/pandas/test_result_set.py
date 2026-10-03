# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import io
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pyathena.pandas.result_set import (
    AthenaPandasResultSet,
    PandasDataFrameIterator,
    _no_trunc_date,
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
