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


@pytest.mark.parametrize("lazy", [False, True], ids=["dataframe", "lazyframe"])
@pytest.mark.parametrize(
    ("dtype", "microseconds"),
    [
        (pl.Datetime, [0, 123000, 123456, 123456, 123456]),
        (pl.Datetime("ms"), [0, 123000, 123000, 123000, 123000]),
        (pl.Datetime("us"), [0, 123000, 123456, 123456, 123456]),
    ],
)
def test_to_datetimes(dtype, microseconds, lazy):
    """Timestamp text with up to 12 fractional digits is truncated to the time unit.

    NULL can be null or an empty string, depending on the read options. A column that
    the frame does not have, such as one not selected, is skipped. A LazyFrame is
    converted in its query plan.
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
    result = _to_datetimes(df.lazy() if lazy else df, {"t": dtype, "missing": dtype})
    if lazy:
        assert isinstance(result, pl.LazyFrame)
        result = result.collect()
    assert result.schema["t"] == dtype
    assert result["t"].to_list() == [
        *(datetime(2020, 1, 2, 3, 4, 5, us) for us in microseconds),
        datetime(1, 1, 1),
        None,
        None,
    ]


def test_to_datetimes_not_strict():
    """Text that is not a timestamp becomes null when the conversion is not strict."""
    df = pl.DataFrame({"t": ["2020-01-02 03:04:05.123456789012", "invalid"]})
    with pytest.raises(pl.exceptions.InvalidOperationError):
        _to_datetimes(df, {"t": pl.Datetime})
    result = _to_datetimes(df, {"t": pl.Datetime}, strict=False)
    assert result["t"].to_list() == [datetime(2020, 1, 2, 3, 4, 5, 123456), None]


@pytest.mark.parametrize(
    ("dtype", "nanoseconds"),
    [
        (pl.Datetime("ms"), [123000000, 123000000, 999000000]),
        (pl.Datetime("us"), [123000000, 123456000, 999999000]),
    ],
)
def test_to_datetimes_years_outside_four_digits(dtype, nanoseconds):
    """The fraction is truncated to the time unit whatever the width of the year.

    Athena writes a year after 9999 with a sign and five or more digits, and a year
    before 0001 with a sign.
    """
    df = pl.DataFrame(
        {
            "t": [
                "+11020-01-02 03:04:05.123",
                "+11020-01-02 03:04:05.123456789012",
                "-0001-01-02 03:04:05.999999999999",
            ]
        }
    )
    result = _to_datetimes(df, {"t": dtype})
    assert result.schema["t"] == dtype
    assert result["t"].dt.year().to_list() == [11020, 11020, -1]
    assert result["t"].dt.nanosecond().to_list() == nanoseconds
