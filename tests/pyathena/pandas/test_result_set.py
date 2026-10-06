# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import csv
import io
from unittest.mock import MagicMock

import pandas as pd
import pytest

from pyathena.pandas.converter import DefaultPandasTypeConverter
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
