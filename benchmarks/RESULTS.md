<!--
Copyright 2026 The PyAthena authors

Licensed under the MIT License.
See LICENSE or https://opensource.org/licenses/MIT.

SPDX-License-Identifier: MIT
-->

# Benchmark results

Each recorded run measures one PyAthena commit with the benchmark harness from that commit.
Runs are identified by date and commit; a run can fall between releases.
Each entry under [`history/`](history/) records the environment, configuration, scope, result tables, observations, and the report's `summary.csv`.
Published entries are not revised; a new measurement adds a new entry.

## Recorded runs

| Date | Commit | Nearest release | Environment | Scales | Suites | Details |
| --- | --- | --- | --- | --- | --- | --- |
| 2026-09-25 | [`9f77e15`](https://github.com/pyathena-dev/PyAthena/tree/9f77e151e8677230fd3f98c796f3b676663bc206) | v3.36.0 + 31 commits | `r7i.2xlarge`, `us-west-2` | 10,000 and 100,000 rows | single, init, concurrent (1 and 10) | [2026-09-25-9f77e15](history/2026-09-25-9f77e15/README.md) |

## Latest findings

The following statements summarize the [2026-09-25 run](history/2026-09-25-9f77e15/README.md) and apply only to results of 10,000 and 100,000 rows in that environment.

- Native output from the Pandas, Arrow, and Polars cursors, and from AWS Wrangler, took 1.4–4.4 seconds end to end.
  Row retrieval through the Cursor API took 3.3–23.4 seconds with the default arraysize of 1000, and 3.2–4.7 times as long with arraysize 100.
- The synchronous, ThreadPool, and native asyncio Cursor APIs had similar end-to-end times, both for single queries and at a concurrency of 10.
- Constructing an Arrow result set on the event loop blocked the loop for 0.29–0.38 seconds at 10,000 rows; `asyncio.to_thread()` construction, which `AioArrowCursor` uses, kept the event-loop lag p95 at 6–12 ms.
- Among full-result native readers at 100,000 rows, Arrow had the largest RSS increase and Polars the smallest.
  A chunksize smaller than the result reduced the Pandas and Wrangler RSS increase; Polars chunk iteration reduced it by at most 2 MiB.

## Comparing runs

Compare runs only for the same scale, shape, transport, and output representation.
Snapshots from different preparations contain different rows, and runs on different instance types, regions, or library versions are not directly comparable.
Five measured trials per case do not resolve small differences.
