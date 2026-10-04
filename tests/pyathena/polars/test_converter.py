# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from datetime import datetime

import polars as pl
import pytest

from pyathena.polars.converter import DefaultPolarsUnloadTypeConverter, _to_datetimes


class TestDefaultPolarsUnloadTypeConverter:
    def test_convert_delegates_to_default(self):
        """convert() dispatches through the default converter instead of returning None."""
        converter = DefaultPolarsUnloadTypeConverter()
        assert converter.convert("varchar", "hello") == "hello"


@pytest.mark.parametrize(
    ("dtype", "microseconds"),
    [
        (pl.Datetime, [0, 123000, 123456, 123456, 123456]),
        (pl.Datetime("ms"), [0, 123000, 123000, 123000, 123000]),
        (pl.Datetime("us"), [0, 123000, 123456, 123456, 123456]),
    ],
)
def test_to_datetimes(dtype, microseconds):
    """Timestamp text with up to 12 fractional digits is truncated to the time unit.

    NULL can be null or an empty string, depending on the read options. A column that
    the DataFrame does not have, such as one not selected, is skipped.
    """
    df = pl.DataFrame(
        {
            "t": [
                "2020-01-02 03:04:05",
                "2020-01-02 03:04:05.123",
                "2020-01-02 03:04:05.123456",
                "2020-01-02 03:04:05.123456789",
                "2020-01-02 03:04:05.123456789012",
                "0001-01-01 00:00:00.000",
                "",
                None,
            ]
        }
    )
    result = _to_datetimes(df, {"t": dtype, "missing": dtype})
    assert result.schema["t"] == dtype
    assert result["t"].to_list() == [
        *(datetime(2020, 1, 2, 3, 4, 5, us) for us in microseconds),
        datetime(1, 1, 1),
        None,
        None,
    ]
