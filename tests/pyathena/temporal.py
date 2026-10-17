# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
"""A query of Athena temporal values and what each cursor returns for them (#1212).

Each column is one format. The fetch methods return ``value``, fractional digits
beyond six truncated. ArrowCursor and PolarsCursor keep the types that they cannot
represent as Athena's text, ``text``.
"""

from datetime import date, datetime, time, timedelta, timezone
from typing import Any, NamedTuple
from zoneinfo import ZoneInfo


class Column(NamedTuple):
    name: str
    sql: str
    athena_type: str
    value: Any
    text: str | None = None


_UTC = ZoneInfo("UTC")
_PLUS_0530 = timezone(timedelta(hours=5, minutes=30))

COLUMNS = [
    Column(
        "ts_p0",
        "CAST('2020-01-02 03:04:05' AS TIMESTAMP(0))",
        "timestamp",
        datetime(2020, 1, 2, 3, 4, 5),
    ),
    Column(
        "ts_p3",
        "TIMESTAMP '2020-01-02 03:04:05.123'",
        "timestamp",
        datetime(2020, 1, 2, 3, 4, 5, 123000),
    ),
    Column(
        "ts_p6",
        "CAST('2020-01-02 03:04:05.123456' AS TIMESTAMP(6))",
        "timestamp",
        datetime(2020, 1, 2, 3, 4, 5, 123456),
    ),
    Column(
        "ts_p9",
        "CAST('2020-01-02 03:04:05.123456789' AS TIMESTAMP(9))",
        "timestamp",
        datetime(2020, 1, 2, 3, 4, 5, 123456),
    ),
    Column(
        "ts_p12",
        "CAST('2020-01-02 03:04:05.123456789012' AS TIMESTAMP(12))",
        "timestamp",
        datetime(2020, 1, 2, 3, 4, 5, 123456),
    ),
    Column(
        "ts_before_epoch",
        "CAST('1969-12-31 23:59:59.987654321' AS TIMESTAMP(9))",
        "timestamp",
        datetime(1969, 12, 31, 23, 59, 59, 987654),
    ),
    Column("ts_min", "TIMESTAMP '0001-01-01 00:00:00.000'", "timestamp", datetime(1, 1, 1)),
    Column(
        "ts_1600",
        "TIMESTAMP '1600-01-02 03:04:05.123'",
        "timestamp",
        datetime(1600, 1, 2, 3, 4, 5, 123000),
    ),
    Column(
        "ts_2300",
        "TIMESTAMP '2300-01-02 03:04:05.123'",
        "timestamp",
        datetime(2300, 1, 2, 3, 4, 5, 123000),
    ),
    Column(
        "ts_max",
        "CAST('9999-12-31 23:59:59.999999999999' AS TIMESTAMP(12))",
        "timestamp",
        datetime(9999, 12, 31, 23, 59, 59, 999999),
    ),
    Column("ts_null", "CAST(NULL AS TIMESTAMP(3))", "timestamp", None),
    Column("date", "DATE '2020-01-02'", "date", date(2020, 1, 2)),
    Column("date_min", "DATE '0001-01-01'", "date", date(1, 1, 1)),
    Column("date_max", "DATE '9999-12-31'", "date", date(9999, 12, 31)),
    Column("date_null", "CAST(NULL AS DATE)", "date", None),
    Column(
        "tstz_utc",
        "TIMESTAMP '2020-01-02 03:04:05.123 UTC'",
        "timestamp with time zone",
        datetime(2020, 1, 2, 3, 4, 5, 123000, tzinfo=_UTC),
        "2020-01-02 03:04:05.123 UTC",
    ),
    Column(
        "tstz_zone",
        "TIMESTAMP '2020-01-02 03:04:05.123 Asia/Tokyo'",
        "timestamp with time zone",
        datetime(2020, 1, 2, 3, 4, 5, 123000, tzinfo=ZoneInfo("Asia/Tokyo")),
        "2020-01-02 03:04:05.123 Asia/Tokyo",
    ),
    Column(
        "tstz_offset",
        "TIMESTAMP '2020-01-02 03:04:05.123 +05:30'",
        "timestamp with time zone",
        datetime(2020, 1, 2, 3, 4, 5, 123000, tzinfo=_PLUS_0530),
        "2020-01-02 03:04:05.123 +05:30",
    ),
    Column(
        "tstz_p12",
        "CAST('2020-01-02 03:04:05.123456789012 UTC' AS TIMESTAMP(12) WITH TIME ZONE)",
        "timestamp with time zone",
        datetime(2020, 1, 2, 3, 4, 5, 123456, tzinfo=_UTC),
        "2020-01-02 03:04:05.123456789012 UTC",
    ),
    Column(
        "tstz_min",
        "CAST('0001-01-01 00:00:00.000 UTC' AS TIMESTAMP(3) WITH TIME ZONE)",
        "timestamp with time zone",
        datetime(1, 1, 1, tzinfo=_UTC),
        "0001-01-01 00:00:00.000 UTC",
    ),
    Column(
        "tstz_max",
        "CAST('9999-12-31 23:59:59.999 UTC' AS TIMESTAMP(3) WITH TIME ZONE)",
        "timestamp with time zone",
        datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=_UTC),
        "9999-12-31 23:59:59.999 UTC",
    ),
    Column(
        "tstz_null", "CAST(NULL AS TIMESTAMP(3) WITH TIME ZONE)", "timestamp with time zone", None
    ),
    Column("time_p3", "TIME '03:04:05.123'", "time", time(3, 4, 5, 123000), "03:04:05.123"),
    Column(
        "time_p12",
        "CAST('03:04:05.123456789012' AS TIME(12))",
        "time",
        time(3, 4, 5, 123456),
        "03:04:05.123456789012",
    ),
    Column("time_null", "CAST(NULL AS TIME(3))", "time", None),
    Column(
        "time_tz",
        "TIME '03:04:05.123 +05:30'",
        "time with time zone",
        time(3, 4, 5, 123000, tzinfo=_PLUS_0530),
        "03:04:05.123+05:30",
    ),
    Column(
        "time_tz_p12",
        "CAST('03:04:05.123456789012 +05:30' AS TIME(12) WITH TIME ZONE)",
        "time with time zone",
        time(3, 4, 5, 123456, tzinfo=_PLUS_0530),
        "03:04:05.123456789012+05:30",
    ),
    Column("time_tz_null", "CAST(NULL AS TIME(3) WITH TIME ZONE)", "time with time zone", None),
]

# UNLOAD writes only TIMESTAMP(3) and date of these types.
UNLOAD_COLUMNS = [
    *(c for c in COLUMNS if c.name in ("ts_p3", "ts_min", "ts_1600", "ts_2300", "ts_null")),
    Column(
        "ts_max",
        "TIMESTAMP '9999-12-31 23:59:59.999'",
        "timestamp",
        datetime(9999, 12, 31, 23, 59, 59, 999000),
    ),
    *(c for c in COLUMNS if c.athena_type == "date"),
]


def columns(unload: bool = False) -> list[Column]:
    """The columns of the query, or of its UNLOAD form."""
    return UNLOAD_COLUMNS if unload else COLUMNS


def query(unload: bool = False) -> str:
    """The SELECT query of the columns."""
    return "SELECT " + ", ".join(f"{c.sql} AS {c.name}" for c in columns(unload))


def fetched_row(unload: bool = False) -> tuple[Any, ...]:
    """The row that the fetch methods return."""
    return tuple(c.value for c in columns(unload))


def zones(row: tuple[Any, ...]) -> list[Any]:
    """The time zone of each aware value: a fixed-offset timezone or a ZoneInfo."""
    return [v.tzinfo for v in row if isinstance(v, datetime | time) and v.tzinfo is not None]
