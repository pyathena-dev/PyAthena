# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from collections.abc import Iterator
from io import BytesIO
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from pyathena.error import OperationalError
from pyathena.filesystem.s3 import S3FileSystem
from pyathena.filesystem.s3_async import AioS3FileSystem
from pyathena.model import AthenaQueryExecution
from pyathena.s3fs.converter import DefaultS3FSTypeConverter
from pyathena.s3fs.reader import AthenaCSVReader, EmptyStringAsNullCSVReader
from pyathena.s3fs.result_set import AthenaS3FSResultSet
from pyathena.util import RetryConfig


class _TupleCSVReader:
    """An independent implementation that returns tuples rather than lists."""

    def __init__(self, file_obj: Any, delimiter: str = ",") -> None:
        self._reader = AthenaCSVReader(file_obj, delimiter=delimiter)

    def __iter__(self) -> Iterator[tuple[str | None, ...]]:
        return self

    def __next__(self) -> tuple[str | None, ...]:
        return tuple(next(self._reader))

    def close(self) -> None:
        self._reader.close()


def _result_set(data, csv_reader, suffix=".csv", column_types=None, type_hints=None, stream=None):
    stream = stream or BytesIO(data.encode())
    filesystem = MagicMock()
    filesystem._open.return_value = stream
    connection = MagicMock()
    connection.s3_client.head_object.return_value = {"ContentLength": len(data.encode())}
    response = {
        "ResultSet": {
            "ResultSetMetadata": {
                "ColumnInfo": [
                    {"Name": name, "Type": col_type}
                    for name, col_type in zip(
                        ("null_col", "empty_col", "value_col"),
                        column_types or ("varchar", "varchar", "varchar"),
                        strict=True,
                    )
                ]
            },
            "Rows": [],
        }
    }
    with patch.object(AthenaS3FSResultSet, "_get_query_results", return_value=response):
        result_set = AthenaS3FSResultSet(
            connection=connection,
            converter=DefaultS3FSTypeConverter(),
            query_execution=MagicMock(
                state=AthenaQueryExecution.STATE_SUCCEEDED,
                output_location=f"s3://bucket/query{suffix}",
                substatement_type="SELECT",
            ),
            arraysize=1,
            retry_config=RetryConfig(),
            csv_reader=csv_reader,
            filesystem_class=MagicMock(return_value=filesystem),
            result_set_type_hints=type_hints,
        )
    return result_set, stream


class _UserS3FileSystem(S3FileSystem):
    """A user's subclass, whose own cache settings apply."""


_PREFETCH_OPTIONS = {
    "block_size": 32 * 2**20,
    "cache_type": "background",
    "cache_options": {"maxblocks": 2},
}


class _FailingCSVReader(_TupleCSVReader):
    """A reader that fails to read the header row."""

    def __next__(self) -> tuple[str | None, ...]:
        raise ValueError("unreadable header")


class TestAthenaS3FSResultSet:
    @pytest.mark.parametrize(
        ("block_size", "filesystem_class", "expected"),
        [
            (None, S3FileSystem, _PREFETCH_OPTIONS),
            (None, AioS3FileSystem, _PREFETCH_OPTIONS),
            (5 * 2**20, S3FileSystem, {}),
            (None, _UserS3FileSystem, {}),
        ],
        ids=["default", "aio", "block_size", "subclass"],
    )
    def test_get_csv_open_options(self, block_size, filesystem_class, expected):
        """PyAthena's filesystems prefetch the CSV result unless block_size is given."""
        result_set = AthenaS3FSResultSet.__new__(AthenaS3FSResultSet)
        result_set._block_size = block_size
        result_set._fs = filesystem_class.__new__(filesystem_class)
        assert result_set._get_csv_open_options() == expected

    def test_reader_closes_at_end_of_results(self):
        """The file is closed once every row has been read, before close()."""
        result_set, stream = _result_set("null_col,empty_col,value_col\n,,a\n", None)
        assert result_set.fetchall() == [(None, None, "a")]
        assert stream.closed
        assert result_set.fetchone() is None
        result_set.close()

    def test_fetch_across_batches(self):
        """The buffer is refilled one batch of arraysize rows at a time."""
        result_set, _ = _result_set("null_col,empty_col,value_col\n,,a\n,,b\n,,c\n", None)
        assert result_set.fetchmany(2) == [(None, None, "a"), (None, None, "b")]
        assert result_set.fetchone() == (None, None, "c")
        assert result_set.fetchmany() == []
        assert result_set.fetchall() == []
        assert result_set.fetchone() is None
        assert result_set.rownumber == 3
        result_set.close()

    def test_reader_failure_closes_file(self):
        """A failure to read the header row closes the file."""
        data = "null_col,empty_col,value_col\n"
        stream = BytesIO(data.encode())
        with pytest.raises(OperationalError, match="unreadable header"):
            _result_set(data, _FailingCSVReader, stream=stream)
        assert stream.closed

    @pytest.mark.parametrize(
        ("csv_reader", "expected_empty"),
        [
            (None, ""),
            (AthenaCSVReader, ""),
            (EmptyStringAsNullCSVReader, None),
            (_TupleCSVReader, ""),
        ],
    )
    @pytest.mark.parametrize(
        ("suffix", "data"),
        [
            (".csv", 'null_col,empty_col,value_col\n,"",text\nnext,"",last\n'),
            (".txt", '\t""\ttext\nnext\t""\tlast\n'),
        ],
    )
    def test_reader_null_conversion(self, csv_reader, expected_empty, suffix, data):
        result_set, stream = _result_set(data, csv_reader, suffix=suffix)
        with result_set:
            assert result_set.fetchmany() == [(None, expected_empty, "text")]
            assert result_set.fetchall() == [("next", expected_empty, "last")]
            assert result_set.fetchone() is None
            assert result_set.rownumber == 2
        assert stream.closed

    @pytest.mark.parametrize(
        ("csv_reader", "expected_empty"),
        [
            (AthenaCSVReader, ""),
            (EmptyStringAsNullCSVReader, None),
            (_TupleCSVReader, ""),
        ],
    )
    def test_reader_null_conversion_with_type_hints(self, csv_reader, expected_empty):
        result_set, stream = _result_set(
            'null_col,empty_col,value_col\n,"","[1, 2]"\n,"",\n',
            csv_reader,
            column_types=("varchar", "varchar", "array"),
            type_hints={"value_col": "array(integer)"},
        )
        with result_set:
            assert result_set.fetchall() == [
                (None, expected_empty, [1, 2]),
                (None, expected_empty, None),
            ]
        assert stream.closed

    @pytest.mark.parametrize("csv_reader", [AthenaCSVReader, EmptyStringAsNullCSVReader])
    def test_managed_results_preserve_api_values(self, csv_reader):
        response = {
            "ResultSet": {
                "ResultSetMetadata": {"ColumnInfo": [{"Name": "value", "Type": "varchar"}]},
                "Rows": [
                    {"Data": [{"VarCharValue": "value"}]},
                    {"Data": [{"VarCharValue": ""}]},
                    {"Data": [{}]},
                ],
            }
        }
        with (
            patch.object(AthenaS3FSResultSet, "_get_query_results", return_value=response),
            AthenaS3FSResultSet(
                connection=MagicMock(),
                converter=DefaultS3FSTypeConverter(),
                query_execution=MagicMock(
                    state=AthenaQueryExecution.STATE_SUCCEEDED, output_location=None
                ),
                arraysize=1,
                retry_config=RetryConfig(),
                csv_reader=csv_reader,
                filesystem_class=MagicMock(),
            ) as result_set,
        ):
            assert result_set.fetchall() == [("",), (None,)]
