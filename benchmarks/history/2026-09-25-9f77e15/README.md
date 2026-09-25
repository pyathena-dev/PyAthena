<!--
Copyright 2026 The PyAthena authors

Licensed under the MIT License.
See LICENSE or https://opensource.org/licenses/MIT.

SPDX-License-Identifier: MIT
-->

# Benchmark run: 2026-09-25, commit 9f77e15

This run measured PyAthena at commit [`9f77e151e8677230fd3f98c796f3b676663bc206`](https://github.com/pyathena-dev/PyAthena/tree/9f77e151e8677230fd3f98c796f3b676663bc206) (`v3.36.0` plus 31 commits), using the benchmark harness from the same commit.
Commits after 9f77e15 change CSV result reading for the Pandas and Arrow cursors, shared cursor code, and the benchmark project layout; these results do not describe those later revisions.
To reproduce the procedure, follow the [README at 9f77e15](https://github.com/pyathena-dev/PyAthena/blob/9f77e151e8677230fd3f98c796f3b676663bc206/benchmarks/README.md).

[`summary.csv`](summary.csv) contains the unmodified report rows from all eight output directories listed below.
Raw trial records, events, and manifests are not published because they contain account-specific identifiers.

## Environment

| Item | Value |
| --- | --- |
| Host | EC2 `r7i.2xlarge` (Intel Xeon Platinum 8488C, 8 vCPUs, 64 GiB), encrypted 100 GiB gp3 root volume |
| Operating system | Amazon Linux 2023.12.20260918, kernel 6.18.48, AMI `ami-075d448db8fb256af` |
| Region and workgroup | `us-west-2`, existing workgroup `pyathena`, Athena engine version 3 |
| Python and uv | CPython 3.12.14, uv 0.12.5 |
| Library versions | PyAthena 3.36.1.dev31+g9f77e151e, boto3/botocore 1.43.98, pandas 3.0.6, pyarrow 25.0.1, polars 1.44.2, awswrangler 3.17.1, numpy 2.5.3, fsspec 2026.9.0 |
| Benchmark lockfile SHA-256 | `eecc18e389a17b1f870101374a8b9f514de3160d9b1f8bba8b5cc5066c15a0d1` |
| Measurement window | 2026-09-25 10:28–12:02 UTC |

The CloudFormation stack was deployed from the template at 9f77e15 on 2026-09-19, stopped, and restarted for this run.
The Git checkout on the host was clean, and the lockfile and configuration hashes matched the values recorded at deployment.
Before each output directory, no integration CI workflow was running and no query had been submitted to the account's Athena workgroups for at least 120 seconds.
Checks about once per minute during the measurements found no running integration CI workflow and no queries in the account's other Athena workgroups.

## Configuration and inputs

The configuration differed from the default `config.toml` only in `concurrency = [1, 10]` and `timeout_seconds = 600.0`.
Every case ran one discarded warmup and five measured trials in separate child processes.
The Athena polling interval was 1.0 second, RSS was sampled every 50 ms, and the event-loop heartbeat interval was 10 ms.
The asyncio executor had 32 workers.

Two snapshots of `pyathena_benchmark.pypi_file_downloads` (`download_date = '2026-09-17'`) were prepared once and reused by every comparison at their scale:

| Scale | Rows | CTAS data scanned |
| --- | ---: | ---: |
| small | 10,000 | 88.6 MB |
| medium | 100,000 | 1.83 GB |

Both snapshots were deleted after the run.
A later run selects different rows because preparation uses `LIMIT` without ordering.

## Scope

| Output directory | Suite | Scale | Selection | Trials |
| --- | --- | --- | --- | ---: |
| `single-small-cursor-r2` | single | small | Cursor, CSV, sync/thread/aio, arraysize 100 and 1000 | 36 |
| `native-small-flat` | single | small | Pandas, Arrow, Polars, Wrangler; sync, CSV, native output | 60 |
| `native-small-nested` | single | small | As above, nested shape | 42 + 3 unsupported |
| `init-small` | init | small | Arrow, CSV and UNLOAD, direct and `to_thread` construction | 24 |
| `concurrent-small` | concurrent | small | Cursor, thread and aio, concurrency 1 and 10 | 24 |
| `single-medium-cursor` | single | medium | Same selection as `single-small-cursor-r2` | 36 |
| `native-medium-flat` | single | medium | Same selection as `native-small-flat` | 60 |
| `native-medium-nested` | single | medium | Same selection as `native-small-nested` | 42 + 3 unsupported |

Trial counts include warmups.
Every attempted trial succeeded, and every measured query returned the expected row count.
The only unsupported cases are Wrangler CSV with the nested shape, which the harness classifies before execution.
An earlier `single-small-cursor` run overlapped with another Athena workload and is excluded; `single-small-cursor-r2` repeats it without overlap.

Not measured in this run: the large and xlarge scales, concurrency 50 and 100, DictCursor, S3FSCursor, UNLOAD and CTAS transports in the single suite, thread and aio APIs for DataFrame cursors, and the initialization suite for families other than Arrow.

## Results

Times are medians over all measured queries: five per case, or 50 at concurrency 10.
RSS increase is the median over the five measured trials; peak RSS and peak threads are maxima across them, and loop-lag columns summarize all heartbeat samples.
p95 values are interpolated from these few samples and are descriptive only.
`Total` is execute plus full result consumption, including Athena execution and 1-second polling.
`Client result` starts at the observed Athena completion and ends after consumption.
RSS increase is measured from a per-trial baseline of 151–153 MiB, which includes importing all DataFrame backends.
Peak threads include SDK and native library threads.
See [measurement semantics](https://github.com/pyathena-dev/PyAthena/blob/9f77e151e8677230fd3f98c796f3b676663bc206/benchmarks/README.md#measurement-semantics) for the definitions.

### Row retrieval through the Cursor API (CSV, flat)

| Scale | API | arraysize | Total (s) | p95 (s) | Client result (s) | Athena total (ms) | RSS increase (MiB) | Peak threads |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small | sync | 100 | 11.29 | 11.67 | 10.17 | 718 | 20 | 12 |
| small | thread | 100 | 11.41 | 11.55 | 10.28 | 655 | 26 | 14 |
| small | aio | 100 | 11.57 | 11.64 | 10.45 | 995 | 20 | 14 |
| small | sync | 1000 | 3.48 | 4.45 | 2.28 | 686 | 28 | 12 |
| small | thread | 1000 | 3.35 | 4.34 | 2.22 | 685 | 36 | 14 |
| small | aio | 1000 | 3.33 | 3.36 | 2.21 | 797 | 30 | 13 |
| medium | sync | 100 | 110.09 | 110.83 | 107.97 | 1499 | 20 | 12 |
| medium | thread | 100 | 107.40 | 107.79 | 105.27 | 1352 | 27 | 14 |
| medium | aio | 100 | 108.12 | 109.07 | 105.99 | 1254 | 23 | 21 |
| medium | sync | 1000 | 23.28 | 24.14 | 21.15 | 1407 | 29 | 12 |
| medium | thread | 1000 | 23.41 | 24.09 | 20.97 | 1221 | 37 | 14 |
| medium | aio | 1000 | 23.19 | 24.67 | 21.29 | 1100 | 31 | 14 |

### Native DataFrame and Table output (sync, CSV)

`—` in the chunksize column means the full result was read at once.

| Scale | Shape | Family | chunksize | Total (s) | p95 (s) | Client result (s) | RSS increase (MiB) | Peak RSS (MiB) | Peak threads |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small | flat | pandas | — | 1.43 | 1.45 | 0.32 | 36 | 195 | 12 |
| small | flat | pandas | 10000 | 1.42 | 1.43 | 0.30 | 31 | 194 | 13 |
| small | flat | pandas | 100000 | 1.42 | 1.44 | 0.31 | 31 | 189 | 12 |
| small | flat | arrow | — | 1.42 | 1.43 | 0.30 | 84 | 237 | 23 |
| small | flat | polars | — | 1.41 | 2.20 | 0.28 | 32 | 184 | 22 |
| small | flat | polars | 10000 | 1.53 | 1.55 | 0.41 | 53 | 207 | 40 |
| small | flat | polars | 100000 | 1.51 | 1.52 | 0.41 | 52 | 207 | 40 |
| small | flat | wrangler | — | 1.48 | 1.49 | 0.23 | 30 | 183 | 12 |
| small | flat | wrangler | 10000 | 1.46 | 1.47 | 0.23 | 31 | 194 | 12 |
| small | flat | wrangler | 100000 | 1.47 | 1.48 | 0.23 | 33 | 200 | 12 |
| small | nested | pandas | — | 1.45 | 2.26 | 0.33 | 47 | 200 | 14 |
| small | nested | pandas | 10000 | 1.44 | 2.29 | 0.33 | 51 | 204 | 14 |
| small | nested | pandas | 100000 | 1.45 | 2.24 | 0.33 | 51 | 205 | 14 |
| small | nested | arrow | — | 1.45 | 2.23 | 0.31 | 74 | 231 | 23 |
| small | nested | polars | — | 1.41 | 2.23 | 0.30 | 40 | 192 | 22 |
| small | nested | polars | 10000 | 1.54 | 1.57 | 0.43 | 59 | 213 | 40 |
| small | nested | polars | 100000 | 1.54 | 1.56 | 0.43 | 60 | 213 | 40 |
| small | nested | wrangler | — | unsupported | — | — | — | — | — |
| small | nested | wrangler | 10000 | unsupported | — | — | — | — | — |
| small | nested | wrangler | 100000 | unsupported | — | — | — | — | — |
| medium | flat | pandas | — | 2.96 | 3.01 | 0.84 | 134 | 294 | 14 |
| medium | flat | pandas | 10000 | 3.00 | 3.02 | 0.85 | 81 | 240 | 14 |
| medium | flat | pandas | 100000 | 3.00 | 3.00 | 0.86 | 143 | 299 | 14 |
| medium | flat | arrow | — | 2.69 | 2.69 | 0.55 | 175 | 336 | 23 |
| medium | flat | polars | — | 2.52 | 2.69 | 0.37 | 95 | 255 | 28 |
| medium | flat | polars | 10000 | 2.79 | 2.81 | 0.65 | 115 | 272 | 41 |
| medium | flat | polars | 100000 | 2.82 | 2.83 | 0.67 | 128 | 283 | 40 |
| medium | flat | wrangler | — | 2.93 | 2.96 | 0.66 | 147 | 308 | 12 |
| medium | flat | wrangler | 10000 | 2.99 | 3.01 | 0.72 | 74 | 243 | 12 |
| medium | flat | wrangler | 100000 | 2.97 | 3.79 | 0.70 | 139 | 305 | 12 |
| medium | nested | pandas | — | 3.36 | 4.39 | 1.23 | 208 | 382 | 14 |
| medium | nested | pandas | 10000 | 3.40 | 3.58 | 1.25 | 135 | 292 | 14 |
| medium | nested | pandas | 100000 | 4.37 | 4.56 | 1.24 | 210 | 388 | 14 |
| medium | nested | arrow | — | 4.01 | 4.80 | 0.84 | 263 | 420 | 23 |
| medium | nested | polars | — | 2.64 | 3.56 | 0.42 | 145 | 327 | 33 |
| medium | nested | polars | 10000 | 3.85 | 3.88 | 0.72 | 143 | 297 | 41 |
| medium | nested | polars | 100000 | 3.86 | 4.74 | 0.73 | 174 | 333 | 40 |
| medium | nested | wrangler | — | unsupported | — | — | — | — | — |
| medium | nested | wrangler | 10000 | unsupported | — | — | — | — | — |
| medium | nested | wrangler | 100000 | unsupported | — | — | — | — | — |

### ResultSet initialization (Arrow, small, flat)

Each construction reads the same stored query output.
`Init` excludes the subsequent row-count validation; RSS includes it.

| Transport | Construction | Init (s) | Loop lag p95 (ms) | Loop lag max (ms) | RSS increase (MiB) | Peak threads |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| CSV | direct | 0.29 | 289.6 | 290.5 | 82 | 22 |
| CSV | to_thread | 0.29 | 12.3 | 34.3 | 88 | 23 |
| UNLOAD | direct | 0.34 | 370.5 | 376.9 | 73 | 22 |
| UNLOAD | to_thread | 0.36 | 5.6 | 34.0 | 80 | 24 |

### Concurrent queries (Cursor, CSV, flat, small, arraysize 1000)

A batch of concurrent queries shares one process, so RSS and thread counts describe the batch.

| API | Concurrency | Per-query total (s) | p95 (s) | Queries/s | Loop lag p95 (ms) | Loop lag max (ms) | RSS increase (MiB) | Peak threads |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| aio | 1 | 3.23 | 3.28 | 0.31 | 1.2 | 61.1 | 30 | 13 |
| thread | 1 | 3.29 | 3.36 | 0.30 | 1.7 | 47.7 | 36 | 14 |
| aio | 10 | 4.06 | 4.50 | 2.14 | 32.2 | 106.3 | 98 | 34 |
| thread | 10 | 4.04 | 4.32 | 2.33 | 28.0 | 87.6 | 111 | 32 |

## Observations

These observations apply to 10,000- and 100,000-row results in this environment.

- Reading native output from the CSV result file took 1.4–1.5 seconds at 10,000 rows and 2.5–4.4 seconds at 100,000 rows.
  Row retrieval through the Cursor API with the default arraysize of 1000 took 3.3–3.5 and 23.2–23.4 seconds for the same scales.
- With arraysize 100, Cursor API retrieval took 3.2–3.5 times as long as with arraysize 1000 at 10,000 rows and 4.6–4.7 times as long at 100,000 rows.
- Athena execution took 0.6–1.0 seconds at 10,000 rows and 1.1–2.4 seconds at 100,000 rows in every case, so client-side retrieval accounts for most of the difference between cases.
  At 10,000 rows, the 1-second polling interval is a large part of the native-output total.
- The synchronous, ThreadPool, and native asyncio Cursor APIs differed in end-to-end time by at most 0.3 seconds at 10,000 rows and 2.7 seconds (2.5%) at 100,000 rows with arraysize 100.
  At concurrency 10, the ThreadPool and native asyncio APIs differed by 0.02 seconds per query.
- Constructing an Arrow result set directly on the event loop blocked it for 0.29–0.38 seconds at 10,000 rows.
  Constructing it with `asyncio.to_thread()`, as `AioArrowCursor` does, took the same time and kept the event-loop lag p95 at 6–12 ms.
- The Arrow cursor had the largest RSS increase among the full-result native readers: 175 MiB (flat) and 263 MiB (nested) at 100,000 rows.
  Polars without a chunksize had the smallest: 95 MiB and 145 MiB.
- A chunksize of 10,000 reduced the RSS increase at 100,000 rows for pandas (134 to 81 MiB flat, 208 to 135 MiB nested) and Wrangler (147 to 74 MiB flat), without changing the total time by more than 0.1 seconds.
  A chunksize of 100,000 covers the whole medium result; its RSS increase was within 9 MiB of reading the full result.
  Polars chunk iteration reduced the RSS increase by at most 2 MiB at these scales, used 40–41 threads, and took 0.1–1.2 seconds longer than reading the full result.

## Limits

Five measured trials per case do not support population estimates or small differences.
The results cover one instance type, one region, one dataset, and results of at most 100,000 rows; they do not predict behavior for larger results or higher concurrency.
Cross-library comparisons measure each library's native output, not a common Python representation.
