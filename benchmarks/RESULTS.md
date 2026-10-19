<!--
Copyright 2026 The PyAthena authors

Licensed under the MIT License.
See LICENSE or https://opensource.org/licenses/MIT.

SPDX-License-Identifier: MIT
-->

# Benchmark results

Each entry in `history/` records one run of the [benchmark harness](README.md), named by the run date and the short commit hash.
An entry lists the environment, the measured selection, the run record, findings, limits, and the result tables.
Compare entries only when they used the same instance type, architecture, and scales, because measurements depend on the host.

| Date | Commit | PyAthena | Hosts | Purchase | Scales | Entry |
| --- | --- | --- | --- | --- | --- | --- |
| 2026-10-11 | `8a69ce03` | 4.0.0 | `r7g.2xlarge` (arm64), fleet of 12 | Spot | medium, large, xlarge | [2026-10-11-8a69ce03](history/2026-10-11-8a69ce03.md) |
| 2026-10-13 | `437276e1` | 4.1.0 release commit | `r7g.2xlarge` (arm64), fleet of 8 | Spot | medium, large, xlarge | [2026-10-13-437276e1](history/2026-10-13-437276e1.md) |
| 2026-10-13 | `437276e1` | 4.1.0 release commit, filesystem suite | `c8gn.2xlarge` (arm64), one host | Spot | small, medium, large, xlarge (objects) | [2026-10-13-437276e1-filesystem](history/2026-10-13-437276e1-filesystem.md) |
| 2026-10-19 | `9e99caa8` | 4.2.0 release commit | `r7g.2xlarge` (arm64), fleet of 10 | Spot | medium, large, xlarge, xxlarge | [2026-10-19-9e99caa8](history/2026-10-19-9e99caa8.md) |
| 2026-10-19 | `9e99caa8` | 4.2.0 release commit, filesystem suite | `c8gn.2xlarge` (arm64), one host | Spot | small, medium, large, xlarge (objects) | [2026-10-19-9e99caa8-filesystem](history/2026-10-19-9e99caa8-filesystem.md) |

## Recommendations

These recommendations come from the runs of `9e99caa8` on `r7g.2xlarge` and `c8gn.2xlarge` hosts, and, where a row says so, from the 4.0.0 run; the entries give the measured cases and their limits.

| Need | Use | Measured |
| --- | --- | --- |
| A DataFrame or Table of a large result | `unload=True` with ArrowCursor, PolarsCursor, or PandasCursor | 10 million flat rows in 18.2 to 19.8 seconds through UNLOAD, against 55.2 to 84.2 seconds through CSV; nested rows in 33.8 to 36.4 seconds with ArrowCursor, PolarsCursor, or PandasCursor with pyarrow dtypes, against 211.3 to 280.3 seconds through CSV |
| A pandas DataFrame with nested columns | PandasCursor with `unload=True` and `dtype_backend="pyarrow"` | 10 million nested rows in 34.2 seconds and 5,820 MiB, against 88.0 seconds and 19,498 MiB with the default dtypes |
| Bounded memory, including results larger than memory | `chunksize` with PandasCursor or PolarsCursor, with CSV or UNLOAD results | 100 million flat rows, about 29 GB of CSV, at 462 to 1,051 MiB with 100,000-row chunks; 10 million rows at 362 to 917 MiB with 10,000-row chunks, against 3,765 to 19,498 MiB without chunks; larger chunks use more memory |
| A whole CSV result in PolarsCursor | Polars 2.0, or `chunksize`; with Polars 1.x, the read holds a copy of the result file (#1307) | 10 million flat rows peaked at 7,547 MiB with Polars 2.0.0 and 9,641 MiB with Polars 1.44.2 |
| Python rows of a large result | ArrowCursor with `unload=True`, or S3FSCursor for constant memory | GetQueryResults paging through Cursor or DictCursor took 217 to 225 seconds for 1 million rows; ArrowCursor with `unload=True` returned 10 million flat rows in 27 seconds in the 4.0.0 run, and S3FSCursor in 349 seconds at 323 MiB, and 100 million rows in 3,387 seconds at 327 MiB |
| `pandas.read_sql_query()` | `awsathena+arrow`, `awsathena+polars`, or `awsathena+pandas` with `unload=true`; for bounded memory, `awsathena+polars` or `awsathena+pandas` with `connect_args={"cursor_kwargs": {"unload": True, "chunksize": 100000}}`, which replaces the cursor options of the URL, and `chunksize=100000` in `read_sql_query()` | 1 million flat rows in 6.7 to 7.0 seconds, against 227 seconds through the default `awsathena+rest`; 10 million flat rows in 48.7 to 50.9 seconds, and at 1,032 to 1,054 MiB with chunks through UNLOAD with the pandas and Polars drivers |
| A Wrangler-like pandas read | PandasCursor | 10 million flat rows in 19.8 seconds through UNLOAD and 84.2 seconds through CSV, against 23.4 and 108.9 seconds for Wrangler |
| Asynchronous code | The native asyncio cursors, or the synchronous and ThreadPool APIs offloaded with `asyncio.to_thread()` or an executor; synchronous calls block the event loop while they poll | Single-query timings of the three APIs differed by a median of 1.7% in the 4.0.0 run |
| S3 transfers in asyncio applications | `AioS3FileSystem`; PyAthena keeps `S3FileSystem` registered for fsspec's `s3` and `s3a` protocols, and making `AioS3FileSystem` the default is to be revisited when boto3 supports asyncio natively, because it still runs the blocking boto3 calls in threads | With 40 workers on 8 vCPUs, `AioS3FileSystem` wrote 1 GiB and 6 GiB objects at 0.97 to 1.00 times and copied 6 GiB at 1.01 to 1.05 times the throughput of `S3FileSystem`, and read 1 GiB at 1.00 times; with 8 workers, it copied 6 GiB at 1.36 to 1.49 times (#1159) |
