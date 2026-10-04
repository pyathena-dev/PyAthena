# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
from datetime import datetime
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from pyathena.arrow.converter import DefaultArrowTypeConverter
from pyathena.arrow.result_set import AthenaArrowResultSet, _to_timestamp
from pyathena.model import AthenaQueryExecution
from pyathena.util import RetryConfig


@pytest.mark.parametrize(
    ("unit", "microseconds"),
    [
        ("s", [0, 0, 0, 0, 0]),
        ("ms", [0, 123000, 123000, 123000, 123000]),
        ("us", [0, 123000, 123456, 123456, 123456]),
    ],
)
def test_to_timestamp(unit, microseconds):
    """Timestamp text with up to 12 fractional digits is truncated to the unit.

    NULL can be null or an empty string, depending on the CSV read options.
    """
    column = pa.chunked_array(
        [
            pa.array(
                [
                    "2020-01-02 03:04:05",
                    "2020-01-02 03:04:05.123",
                    "2020-01-02 03:04:05.123456",
                    "2020-01-02 03:04:05.123456789",
                    "2020-01-02 03:04:05.123456789012",
                    "0001-01-01 00:00:00.000",
                    "",
                    None,
                ],
                pa.string(),
            )
        ]
    )
    values = _to_timestamp(column, pa.timestamp(unit))
    assert values.type == pa.timestamp(unit)
    assert values.to_pylist() == [
        *(datetime(2020, 1, 2, 3, 4, 5, us) for us in microseconds),
        datetime(1, 1, 1),
        None,
        None,
    ]


class TestAthenaArrowResultSet:
    def test_fetch_after_close(self):
        """No AWS calls; the query execution and the filesystem are mocked."""
        with patch.object(AthenaArrowResultSet, "_create_s3_file_system"):
            result_set = AthenaArrowResultSet(
                connection=MagicMock(),
                converter=DefaultArrowTypeConverter(),
                query_execution=MagicMock(state=AthenaQueryExecution.STATE_FAILED),
                arraysize=1,
                retry_config=RetryConfig(),
            )
        result_set.close()
        assert result_set.fetchone() is None
        assert result_set.fetchmany() == []
        assert result_set.fetchall() == []

    def test_read_csv_timestamps_with_the_same_name(self):
        """Timestamp columns are converted by position, also with names that repeat.

        No AWS calls; the GetQueryResults rows are mocked.
        """
        result_set = AthenaArrowResultSet.__new__(AthenaArrowResultSet)  # bypass __init__
        result_set._query_execution = None
        result_set._converter = DefaultArrowTypeConverter()
        result_set._block_size = AthenaArrowResultSet.DEFAULT_BLOCK_SIZE
        result_set._metadata = tuple(
            {"Name": n, "Type": t, "Precision": 3, "Scale": 0, "Nullable": "UNKNOWN"}
            for n, t in (("x", "varchar"), ("x", "timestamp"), ("y", "timestamp"))
        )
        data = b'"x","x","y"\n"a","2020-01-02 03:04:05.123456789","2020-01-02 03:04:05.1"\n"b",,\n'
        with patch.object(AthenaArrowResultSet, "_fetch_all_rows_as_csv", return_value=data):
            table = result_set._read_csv()
        assert table.column_names == ["x", "x", "y"]
        assert table.schema.types == [pa.string(), pa.timestamp("us"), pa.timestamp("us")]
        rows = list(zip(*(column.to_pylist() for column in table.columns), strict=True))
        assert rows == [
            ("a", datetime(2020, 1, 2, 3, 4, 5, 123456), datetime(2020, 1, 2, 3, 4, 5, 100000)),
            ("b", None, None),
        ]
