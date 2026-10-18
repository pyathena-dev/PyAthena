<!--
Copyright 2026 The PyAthena authors

Licensed under the MIT License.
See LICENSE or https://opensource.org/licenses/MIT.

SPDX-License-Identifier: MIT
-->

(temporal_types)=

# Temporal Types

This section documents how each cursor returns the Athena temporal types:
`timestamp`, `timestamp with time zone`, `date`, `time`, and `time with time zone`.
The asynchronous and aio variants of each cursor return the same types as the cursor.
`SparkCursor` and its variants are not covered.

## Types by Cursor

| Athena type | Fetch methods | `PandasCursor` | `ArrowCursor` | `PolarsCursor` |
| --- | --- | --- | --- | --- |
| `timestamp` | naive `datetime` | `datetime64[us]` | `timestamp[us]` | `Datetime("us")` |
| `timestamp with time zone` | aware `datetime` | `datetime64[us, UTC]` | `string` | `String` |
| `date` | `date` | `datetime64[us]` | `date32` | `Date` |
| `time` | naive `time` | `object` (`time`) | `string` | `String` |
| `time with time zone` | aware `time` | `object` (aware `time`) | `string` | `String` |

The fetch methods are `fetchone()`, `fetchmany()`, `fetchall()`, and iteration of the cursor.
They return the types in the "Fetch methods" column in every cursor, including `Cursor`, `DictCursor`, and `S3FSCursor`.
`PandasCursor` returns `timestamp with time zone` values as aware `datetime` objects in UTC, as its DataFrame holds them.

The type of a column depends only on its Athena type and the cursor.
It is the same for every value, for whole and chunked reads, for results read with `unload=True`, and for {ref}`managed query result storage <managed-query-result-storage>`.
Options given to `execute()` and custom converters can change it, as each cursor's page describes:
{ref}`pandas-cursor`, {ref}`arrow-cursor`, and {ref}`polars-cursor`.

`ArrowCursor` and `PolarsCursor` keep `timestamp with time zone`, `time`, and `time with time zone` columns as Athena's text, such as `2020-01-02 03:04:05.123 Asia/Tokyo` and `03:04:05.123+05:30`.
Their fetch methods convert these values to `datetime` and `time` objects.

## Supported Years

Typed values cover years 1 to 9999.
PyAthena does not check for other years.
For them, the result is whatever Python, pandas, pyarrow, or Polars returns or raises.

`PandasCursor` holds `timestamp with time zone` values in UTC, so for them the range applies to the year in UTC.
For example, the fetch methods raise an error for `0001-01-01 00:00:00.000 Asia/Tokyo`, which is in year 0 in UTC.

## Precision

Microseconds are the precision of every typed value.
Fractional digits beyond six are truncated, not rounded, including before 1970:
the `timestamp(9)` value `1969-12-31 23:59:59.987654321` is read as `datetime(1969, 12, 31, 23, 59, 59, 987654)`.
Columns kept as text keep every fractional digit.

`cursor.description` reports precision 3 for every `timestamp` column, whatever precision the column has.

## NULL

SQL NULL is `None` in the fetch methods.
In `PandasCursor` DataFrames, it is `NaT` in the `datetime64` columns and `None` in the `object` columns.
In Arrow Tables and Polars DataFrames, it is `null`, including the columns kept as text.
See {ref}`null_handling` for the other types.

## Time Zones

A `timestamp with time zone` value has a UTC offset, such as `+05:30`, or a time zone name, such as `UTC` or `Asia/Tokyo`.
The fetch methods of the cursors other than `PandasCursor` return a value with an offset in a fixed-offset `datetime.timezone`, and a value with a zone name in `zoneinfo.ZoneInfo`.
`time with time zone` values always have an offset.

Athena writes a value with a zone name as the local time in that zone.
A local time that a change of the zone's UTC offset, such as daylight saving time, repeats or skips does not identify one instant.
For such a value, and for a zone name that Python's time zone database does not know, the cursors raise `DataError` naming the column when they convert the value.
Some reads convert values before the fetch methods are called, for example `execute()` for the first page of `Cursor` and `DictCursor` results and the chunk reads of `PandasCursor`, so the error is raised there.

```text
DataError: Column 'col': '2020-11-01 01:30:00.000 America/New_York' is ambiguous: the local time occurs twice in America/New_York. Convert the value to UTC in SQL, for example with AT TIME ZONE 'UTC'.
```

Python looks up zone names in the system time zone database first and then in the `tzdata` package, which PyAthena requires at version 2024.2 or later.
For an unknown zone name, update the `tzdata` package or the system time zone database.

Athena's time zone rules can differ from Python's for some zones and years.
For such a value, the instant that PyAthena returns differs from the one Athena holds, and PyAthena cannot detect it.
To read the exact instant, convert the value in SQL:

```sql
SELECT
  col_timestamp_tz AT TIME ZONE 'UTC' AS col_utc,
  to_unixtime(col_timestamp_tz) AS col_epoch_seconds
FROM your_table
```

Values with a zone name come from SQL expressions, such as `AT TIME ZONE`, `at_timezone()`, `with_timezone()`, and `from_unixtime()` with a zone.
Iceberg `timestamp with time zone` columns read back in UTC.

## UNLOAD

With `unload=True`, Athena writes the result as Parquet, which supports only some temporal types:

- `timestamp` columns must be `TIMESTAMP(3)`. Other precisions fail with `NOT_SUPPORTED: Incorrect timestamp precision`.
- `time`, `time with time zone`, and `timestamp with time zone` columns fail with `NOT_SUPPORTED: Unsupported Hive type`.
- `date` columns are supported.

Cast such columns in the query:

```sql
SELECT
  CAST(col_timestamp_6 AS TIMESTAMP(3)) AS col_timestamp,
  CAST(col_timestamp_tz AT TIME ZONE 'UTC' AS TIMESTAMP(3)) AS col_timestamp_utc,
  CAST(col_time AS VARCHAR) AS col_time
FROM your_table
```

Timestamps are read at microseconds, including those inside arrays, rows, and maps, so years outside 1677 to 2262 are read correctly.
For Polars 1.x, see {ref}`polars-unload-options`.
