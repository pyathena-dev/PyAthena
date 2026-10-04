# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from datetime import datetime

import pyarrow as pa
import pytest

from pyathena.arrow.converter import DefaultArrowUnloadTypeConverter, _to_timestamp


class TestDefaultArrowUnloadTypeConverter:
    def test_convert_delegates_to_default(self):
        """convert() dispatches through the default converter instead of returning None."""
        converter = DefaultArrowUnloadTypeConverter()
        assert converter.convert("varchar", "hello") == "hello"


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
