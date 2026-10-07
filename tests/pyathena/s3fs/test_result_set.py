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

from pyathena.model import AthenaQueryExecution
from pyathena.s3fs.converter import DefaultS3FSTypeConverter
from pyathena.s3fs.reader import AthenaCSVReader, EmptyStringAsNullCSVReader
from pyathena.s3fs.result_set import AthenaS3FSResultSet
from pyathena.util import RetryConfig, override


class _InheritedEmptyStringAsNullCSVReader(EmptyStringAsNullCSVReader):
    pass


class _NullifyingAthenaCSVReader(AthenaCSVReader):
    @property
    @override
    def empty_strings_as_null(self) -> bool:
        return True


class _TupleCSVReader:
    """An independent implementation that returns tuples rather than lists."""

    empty_strings_as_null = False

    def __init__(self, file_obj: Any, delimiter: str = ",") -> None:
        self._reader = AthenaCSVReader(file_obj, delimiter=delimiter)

    def __iter__(self) -> Iterator[tuple[str | None, ...]]:
        return self

    def __next__(self) -> tuple[str | None, ...]:
        return tuple(next(self._reader))

    def close(self) -> None:
        self._reader.close()


class _NullifyingTupleCSVReader(_TupleCSVReader):
    empty_strings_as_null = True


def _result_set(data, csv_reader, suffix=".csv", column_types=None, type_hints=None):
    stream = BytesIO(data.encode())
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


@pytest.mark.parametrize(
    ("csv_reader", "expected_empty"),
    [
        (None, ""),
        (AthenaCSVReader, ""),
        (EmptyStringAsNullCSVReader, None),
        (_InheritedEmptyStringAsNullCSVReader, None),
        (_NullifyingAthenaCSVReader, None),
        (_TupleCSVReader, ""),
        (_NullifyingTupleCSVReader, None),
    ],
)
@pytest.mark.parametrize(
    ("suffix", "data"),
    [
        (".csv", 'null_col,empty_col,value_col\n,"",text\nnext,"",last\n'),
        (".txt", '\t""\ttext\nnext\t""\tlast\n'),
    ],
)
def test_reader_null_conversion(csv_reader, expected_empty, suffix, data):
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
        (_InheritedEmptyStringAsNullCSVReader, None),
        (_NullifyingAthenaCSVReader, None),
        (_TupleCSVReader, ""),
        (_NullifyingTupleCSVReader, None),
    ],
)
def test_reader_null_conversion_with_type_hints(csv_reader, expected_empty):
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
def test_managed_results_preserve_api_values(csv_reader):
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
