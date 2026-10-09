# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import builtins
from datetime import datetime
from unittest.mock import PropertyMock, patch

import polars as pl
import pytest

from pyathena.error import OperationalError
from pyathena.polars.converter import DefaultPolarsTypeConverter
from pyathena.polars.result_set import (
    AthenaPolarsResultSet,
    PolarsDataFrameIterator,
    validate_execute_kwargs,
)

_ROWS_BEFORE_FAILURE = 300_000


def _chunked_result_set() -> AthenaPolarsResultSet:
    """Create a chunked result set without calling Athena.

    Returns:
        An AthenaPolarsResultSet with only the attributes used by the chunk readers.
    """
    result_set = AthenaPolarsResultSet.__new__(AthenaPolarsResultSet)  # bypass __init__
    result_set._chunksize = 10_000
    result_set._kwargs = {}
    result_set._metadata = None
    return result_set


class TestAthenaPolarsResultSet:
    def test_managed_csv_accepts_eager_options_with_chunksize(self):
        result_set = _chunked_result_set()
        result_set._query_execution = None
        result_set._converter = DefaultPolarsTypeConverter()
        result_set._metadata = tuple(
            {"Name": name, "Type": dtype, "Precision": 0, "Scale": 0, "Nullable": "UNKNOWN"}
            for name, dtype in (("a", "integer"), ("b", "varchar"))
        )
        result_set._kwargs = {
            "columns": ["a"],
            "n_threads": 1,
            "use_pyarrow": False,
            "batch_size": 1024,
        }
        with (
            patch.object(
                AthenaPolarsResultSet, "_fetch_all_rows_as_csv", return_value=b"a,b\n1,x\n2,y\n"
            ),
            patch.object(
                AthenaPolarsResultSet,
                "_csv_storage_options",
                new_callable=PropertyMock,
                return_value={},
            ),
        ):
            frame = result_set._read_csv()
        assert frame.to_dict(as_series=False) == {"a": [1, 2]}

    @pytest.mark.parametrize("reader", ["_read_csv", "_create_dataframe_iterator"])
    def test_csv_rejects_options_for_other_actual_reader(self, reader):
        result_set = _chunked_result_set()
        result_set._query_execution = None
        result_set._unload = False
        key = "include_file_paths" if reader == "_read_csv" else "columns"
        result_set._kwargs = {key: "a"}
        for _ in range(2):
            with pytest.raises(TypeError, match=f"unexpected keyword argument '{key}'"):
                (
                    result_set._read_csv()
                    if reader == "_read_csv"
                    else result_set._create_dataframe_iterator()
                )

    @pytest.mark.parametrize("reader", ["_read_csv", "_iter_csv_chunks"])
    @pytest.mark.parametrize("rename", [False, True])
    def test_csv_dtypes_alias_replaces_inferred_schema(self, tmp_path, reader, rename):
        path = tmp_path / "result.csv"
        path.write_text("a\n001\n002\n")
        result_set = _chunked_result_set()
        name = "z" if rename else "a"
        result_set._kwargs = {"dtypes": {name: pl.String}}
        if rename:
            result_set._kwargs["new_columns"] = [name]
        with (
            patch.object(
                AthenaPolarsResultSet,
                "output_location",
                new_callable=PropertyMock,
                return_value=str(path),
            ),
            patch.object(
                AthenaPolarsResultSet,
                "_csv_dtypes",
                new_callable=PropertyMock,
                return_value={"a": pl.Int64},
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
            pytest.warns(DeprecationWarning, match="dtypes"),
        ):
            frame = (
                result_set._read_csv()
                if reader == "_read_csv"
                else pl.concat(list(result_set._iter_csv_chunks()))
            )
        assert frame.to_dict(as_series=False) == {name: ["001", "002"]}

    @pytest.mark.parametrize("reader", ["_read_csv", "_iter_csv_chunks"])
    def test_csv_keeps_deprecated_row_index_options(self, tmp_path, reader):
        path = tmp_path / "result.csv"
        path.write_text("a\n1\n2\n")
        result_set = _chunked_result_set()
        result_set._kwargs = {"row_count_name": "row_number", "row_count_offset": 2}
        with (
            patch.object(
                AthenaPolarsResultSet,
                "output_location",
                new_callable=PropertyMock,
                return_value=str(path),
            ),
            patch.object(
                AthenaPolarsResultSet,
                "_csv_dtypes",
                new_callable=PropertyMock,
                return_value={"a": pl.Int64},
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
            pytest.warns(DeprecationWarning, match="row_count_(name|offset)"),
        ):
            frame = (
                result_set._read_csv()
                if reader == "_read_csv"
                else pl.concat(list(result_set._iter_csv_chunks()))
            )
        assert frame.to_dict(as_series=False) == {"row_number": [2, 3], "a": [1, 2]}

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
                "_csv_dtypes",
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
                "_csv_dtypes",
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

    @pytest.mark.parametrize("reader", ["_read_csv", "_iter_csv_chunks"])
    def test_txt_new_columns_with_schema_overrides(self, tmp_path, reader):
        """schema_overrides given with new_columns reach Polars with them for a .txt file."""
        path = tmp_path / "result.txt"
        path.write_text("001\tx\n")
        result_set = _chunked_result_set()
        result_set._kwargs = {"new_columns": ["z"], "schema_overrides": {"z": pl.Utf8}}
        with (
            patch.object(
                AthenaPolarsResultSet,
                "output_location",
                new_callable=PropertyMock,
                return_value=str(path),
            ),
            patch.object(AthenaPolarsResultSet, "_get_column_names", return_value=["a", "b"]),
            patch.object(
                AthenaPolarsResultSet, "_csv_dtypes", new_callable=PropertyMock, return_value={}
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
        assert df.to_dict(as_series=False) == {"z": ["001"], "b": ["x"]}

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
                AthenaPolarsResultSet, "_csv_dtypes", new_callable=PropertyMock, return_value={}
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
            patch(f"polars.{function}", autospec=True) as read,
            patch("pyathena.polars.result_set.to_column_info"),
        ):
            result = getattr(result_set, reader)()
            if not isinstance(result, (pl.DataFrame, tuple)):
                list(result)
        assert read.call_args.kwargs["storage_options"] == {"anon": True}

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            ({}, {"t": [datetime(2020, 1, 2, 3, 4, 5, 123456), None]}),
            ({"dtypes": {"t": pl.Datetime("ms")}}, OperationalError),
            ({"schema_overrides": {"t": pl.Datetime("ms")}}, OperationalError),
            ({"dtypes": {"v": pl.Int64}}, OperationalError),
            ({"schema_overrides": {"v": pl.Int64}}, OperationalError),
            (
                {"dtypes": {"t2": pl.Datetime("us")}, "new_columns": ["t2", "v2"]},
                OperationalError,
            ),
            (
                {"schema_overrides": {"t2": pl.Datetime("us")}, "new_columns": ["t2", "v2"]},
                OperationalError,
            ),
            ({"columns": ["v"]}, {}),
            ({"new_columns": ["t2", "v2"]}, {"t2": [datetime(2020, 1, 2, 3, 4, 5, 123456), None]}),
            ({"with_column_names": lambda names: [n.upper() for n in names]}, TypeError),
        ],
    )
    def test_read_csv_truncates_timestamps(self, kwargs, expected):
        """Timestamps that fail to parse are read again as text and truncated.

        ``with_column_names`` is a scan-only option and is rejected by the eager reader.
        The ``schema_overrides`` option and its ``dtypes`` alias disable timestamp
        retries and preserve reader errors.
        No AWS calls; the GetQueryResults rows are mocked.
        """
        result_set = AthenaPolarsResultSet.__new__(AthenaPolarsResultSet)  # bypass __init__
        result_set._query_execution = None
        result_set._converter = DefaultPolarsTypeConverter()
        result_set._kwargs = kwargs
        result_set._metadata = tuple(
            {"Name": n, "Type": t, "Precision": 3, "Scale": 0, "Nullable": "UNKNOWN"}
            for n, t in (("t", "timestamp"), ("v", "varchar"))
        )
        data = b'"t","v"\n"2020-01-02 03:04:05.123456789012","x"\n,"y"\n'
        with (
            patch.object(AthenaPolarsResultSet, "_fetch_all_rows_as_csv", return_value=data),
            patch.object(
                AthenaPolarsResultSet,
                "_csv_storage_options",
                new_callable=PropertyMock,
                return_value={},
            ),
        ):
            if expected is OperationalError:
                with pytest.raises(OperationalError, match="could not parse"):
                    result_set._read_csv()
                return
            if expected is TypeError:
                with pytest.raises(
                    TypeError, match="unexpected keyword argument 'with_column_names'"
                ):
                    result_set._read_csv()
                return
            df = result_set._read_csv()
        for name, values in expected.items():
            assert df.schema[name] == pl.Datetime("us")
            assert df[name].to_list() == values
        if not expected:
            assert df.columns == ["v"]


class TestPolarsDataFrameIterator:
    @pytest.mark.parametrize(
        "reader",
        ["dataframe", "generator"],
        ids=["dataframe", "generator"],
    )
    def test_close_stops_iteration(self, reader):
        """A closed iterator yields nothing for either reader kind."""
        reader = (
            pl.DataFrame({"a": [1, 2]})
            if reader == "dataframe"
            else (pl.DataFrame({"a": [1]}) for _ in range(2))
        )
        df_iter = PolarsDataFrameIterator(reader, {}, ["a"])
        df_iter.close()
        assert list(df_iter) == []


@pytest.mark.parametrize("unload", [False, True])
@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"chunksize": 10},
        {"chunksize": 10, "block_size": 64, "cache_type": "bytes", "max_workers": 2},
    ],
)
def test_cursor_settings_skip_reader_imports(kwargs, unload):
    with (
        patch("builtins.__import__", wraps=builtins.__import__) as load_module,
        patch("pyathena.polars.result_set.keyword_parameters") as reader_parameters,
    ):
        validate_execute_kwargs("Cursor.execute", kwargs, unload, None)
    assert not any(
        call.args and call.args[0] in {"polars", "pyarrow.parquet"}
        for call in load_module.call_args_list
    )
    reader_parameters.assert_not_called()


def test_reader_replacement_updates_keyword_validation():
    def first_reader(source, *, old_option=None):
        pass

    def second_reader(source, *, new_option=None):
        pass

    with patch.object(pl, "read_csv", first_reader):
        validate_execute_kwargs("Cursor.execute", {"old_option": True}, False, None)
    with patch.object(pl, "read_csv", second_reader):
        validate_execute_kwargs(
            "Cursor.execute", {"chunksize": 10, "new_option": True}, False, None
        )
        with pytest.raises(TypeError, match="unexpected keyword argument 'old_option'"):
            validate_execute_kwargs(
                "Cursor.execute", {"chunksize": 10, "old_option": True}, False, None
            )
