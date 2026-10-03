# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from unittest.mock import PropertyMock, patch

import polars as pl
import pytest

from pyathena.error import OperationalError
from pyathena.polars.result_set import AthenaPolarsResultSet, PolarsDataFrameIterator

_ROWS_BEFORE_FAILURE = 300_000


def _chunked_result_set() -> AthenaPolarsResultSet:
    """Create a chunked result set without calling Athena.

    Returns:
        An AthenaPolarsResultSet with only the attributes used by the chunk readers.
    """
    result_set = AthenaPolarsResultSet.__new__(AthenaPolarsResultSet)  # bypass __init__
    result_set._chunksize = 10_000
    result_set._kwargs = {}
    return result_set


class TestAthenaPolarsResultSet:
    def test_iter_csv_chunks_raises_when_read_fails_partway(self, tmp_path):
        """A CSV read that fails partway through the data raises instead of ending early."""
        path = tmp_path / "result.csv"
        path.write_text(
            "a\n" + "".join(f"{i}\n" for i in range(_ROWS_BEFORE_FAILURE)) + "not-a-number\n"
        )
        result_set = _chunked_result_set()
        with (
            patch.object(
                AthenaPolarsResultSet,
                "output_location",
                new_callable=PropertyMock,
                return_value=str(path),
            ),
            patch.object(
                AthenaPolarsResultSet,
                "dtypes",
                new_callable=PropertyMock,
                return_value={"a": pl.Int64},
            ),
            patch.object(
                AthenaPolarsResultSet,
                "_parquet_storage_options",
                new_callable=PropertyMock,
                return_value={},
            ),
            patch.object(AthenaPolarsResultSet, "_is_csv_readable", return_value=True),
            pytest.raises(OperationalError, match="not-a-number"),
        ):
            list(result_set._iter_csv_chunks())

    def test_iter_parquet_chunks_raises_when_read_fails_partway(self, tmp_path):
        """A Parquet read that fails partway through the data raises instead of ending early."""
        pl.DataFrame({"a": range(_ROWS_BEFORE_FAILURE)}).write_parquet(
            tmp_path / "0.parquet", row_group_size=10_000
        )
        valid = (tmp_path / "0.parquet").read_bytes()
        # Keep the footer magic but corrupt the metadata so the second file fails to read.
        (tmp_path / "1.parquet").write_bytes(valid[: len(valid) // 2] + b"\x00" * 64 + valid[-8:])
        result_set = _chunked_result_set()
        result_set._unload_location = f"{tmp_path}/"
        with (
            patch.object(
                AthenaPolarsResultSet,
                "_parquet_storage_options",
                new_callable=PropertyMock,
                return_value={},
            ),
            pytest.raises(OperationalError),
        ):
            list(result_set._iter_parquet_chunks())

    @pytest.mark.parametrize("reader", ["_read_csv", "_iter_csv_chunks"])
    def test_csv_read_kwargs_replace_defaults(self, tmp_path, reader):
        """Read arguments given to execute() replace the ones the result set chooses."""
        path = tmp_path / "result.csv"
        path.write_text("1;x\n2;y\n")
        result_set = _chunked_result_set()
        result_set._kwargs = {
            "separator": ";",
            "has_header": False,
            "schema_overrides": {"column_1": pl.Utf8},
        }
        with (
            patch.object(
                AthenaPolarsResultSet,
                "output_location",
                new_callable=PropertyMock,
                return_value=str(path),
            ),
            patch.object(
                AthenaPolarsResultSet,
                "dtypes",
                new_callable=PropertyMock,
                return_value={"1;x": pl.Int64},
            ),
            patch.object(
                AthenaPolarsResultSet,
                "_csv_storage_options",
                new_callable=PropertyMock,
                return_value={},
            ),
            patch.object(
                AthenaPolarsResultSet,
                "_parquet_storage_options",
                new_callable=PropertyMock,
                return_value={},
            ),
            patch.object(AthenaPolarsResultSet, "_is_csv_readable", return_value=True),
        ):
            result = getattr(result_set, reader)()
            df = result if isinstance(result, pl.DataFrame) else pl.concat(list(result))
        assert df.to_dict(as_series=False) == {"column_1": ["1", "2"], "column_2": ["x", "y"]}

    @pytest.mark.parametrize(
        ("reader", "function"),
        [
            ("_read_csv", "read_csv"),
            ("_iter_csv_chunks", "scan_csv"),
            ("_read_parquet", "read_parquet"),
            ("_iter_parquet_chunks", "scan_parquet"),
            ("_read_parquet_schema", "scan_parquet"),
        ],
    )
    def test_storage_options_replace_defaults(self, reader, function):
        """storage_options given to execute() replace PyAthena's without computing them."""
        result_set = _chunked_result_set()
        result_set._unload_location = "s3://bucket/unload/"
        result_set._kwargs = {"storage_options": {"anon": True}}
        with (
            patch.object(
                AthenaPolarsResultSet,
                "output_location",
                new_callable=PropertyMock,
                return_value="s3://bucket/result.csv",
            ),
            patch.object(
                AthenaPolarsResultSet, "dtypes", new_callable=PropertyMock, return_value={}
            ),
            patch.object(
                AthenaPolarsResultSet,
                "_csv_storage_options",
                new_callable=PropertyMock,
                side_effect=AssertionError("replaced storage options were computed"),
            ),
            patch.object(
                AthenaPolarsResultSet,
                "_parquet_storage_options",
                new_callable=PropertyMock,
                side_effect=AssertionError("replaced storage options were computed"),
            ),
            patch.object(AthenaPolarsResultSet, "_is_csv_readable", return_value=True),
            patch.object(AthenaPolarsResultSet, "_prepare_parquet_location", return_value=True),
            patch(f"polars.{function}") as read,
            patch("pyathena.polars.result_set.to_column_info"),
        ):
            result = getattr(result_set, reader)()
            if not isinstance(result, (pl.DataFrame, tuple)):
                list(result)
        assert read.call_args.kwargs["storage_options"] == {"anon": True}


class TestPolarsDataFrameIterator:
    @pytest.mark.parametrize(
        "reader",
        [pl.DataFrame({"a": [1, 2]}), (df for df in [pl.DataFrame({"a": [1]})] * 2)],
        ids=["dataframe", "generator"],
    )
    def test_close_stops_iteration(self, reader):
        """A closed iterator yields nothing for either reader kind."""
        df_iter = PolarsDataFrameIterator(reader, {}, ["a"])
        df_iter.close()
        assert list(df_iter) == []
