<!--
Copyright 2026 The PyAthena authors

Licensed under the MIT License.
See LICENSE or https://opensource.org/licenses/MIT.

SPDX-License-Identifier: MIT
-->

# S3 filesystem benchmark of the PyAthena 4.2.0 release commit on 2026-10-19

This entry records the [filesystem suite](../README.md#filesystem-suite) at commit [`9e99caa8`](https://github.com/pyathena-dev/PyAthena/tree/9e99caa84857f8d29abe63435461fc412e040c43/benchmarks), which is tagged `v4.2.0`, for #1159.
It repeats the selection of the [4.1.0 run](2026-10-13-437276e1-filesystem.md), after #1184 gave the asyncio paths of `AioS3FileSystem` thread pools sized by `max_workers` instead of the event loop's default executor, and #1165 bounded the parts in flight of `S3File` writes.
The cursor run of the same commit is a [separate entry](2026-10-19-9e99caa8.md).

## Environment

| Item | Value |
| --- | --- |
| Commit | `9e99caa84857f8d29abe63435461fc412e040c43`, with a clean checkout |
| Lockfile SHA-256 | `0715fe352738dc098f28c35ed3ff597ac9949387c6aa93595f728ce9f929eac7`, the same as the commit's `uv.lock` |
| Python | 3.12.14 |
| Libraries | boto3 1.43.102, fsspec 2026.9.0 |
| Host | one `c8gn.2xlarge` (arm64, 8 vCPUs, 16 GiB, 25 Gbps baseline network bandwidth), Spot, `us-west-2b`, Amazon Linux 2023 (`ami-0957e38de3db270cc`), kernel 6.18.54 |
| Objects | zero bytes in the stack's scratch bucket in `us-west-2`: 64 MiB (small), 256 MiB (medium), 1 GiB (large), and 6 GiB (xlarge) |
| Block size | 8 MiB for range requests, parts, and copied parts |

## Selection

The suite ran as in the 4.1.0 run: 16 jobs, five repetitions, no warmups.

- **small, medium, large**: reads and writes.
- **xlarge**: writes and copies.

Each operation ran with the sync and aio APIs, `max_workers` 8 and 40, and connection pools (`max_pool_connections`) of 10 and 40.
The sync API calls the filesystem on the trial's thread; the aio API runs reads and writes in `asyncio.to_thread()`, as `AioS3FSCursor` does, so that a file's parallel requests go through `S3AioExecutor` to the file's pool of `max_workers` threads, and awaits the copy coroutine on the trial's event loop, whose part copies run in the filesystem's pool of `max_workers` threads.

## Run record

The queue `fs-1` was published at 07:40 UTC, and its 16 jobs finished on the one host by 08:03 UTC without an interruption; the host then retired itself.
Each of the 64 cases recorded five successful trials and no failure.
The suite does not query Athena; the stack of the [cursor run](2026-10-19-9e99caa8.md) was deployed at the same time and published its queue after this suite had finished.

## Findings

1. **With 40 workers, aio writes and copies ran at the sync throughput** (#1184).
   For the 6 GiB object, sync wrote 1,050 to 1,074 MiB/s and aio 1,047 to 1,052 MiB/s (0.98 to 1.00 times sync), against 0.75 to 0.77 times in the 4.1.0 run; for the 1 GiB object, 0.97 to 0.99 times, against 0.81 to 0.83.
   Sync copied the 6 GiB object at 1,835 to 1,879 MiB/s and aio at 1,889 to 1,925 MiB/s (1.01 to 1.05 times), against 521 to 540 MiB/s (0.30 to 0.35 times) in the 4.1.0 run.
   With the 1 GiB and 6 GiB objects, the aio processes peaked at 52 to 53 threads, against 24 to 25 in the 4.1.0 run and 51 for sync, so their thread counts follow `max_workers`.
2. **Reads and the 8-worker cases did not regress.**
   For the 1 GiB object, aio read at 0.96 to 1.06 times sync; with 8 workers, aio wrote at 0.95 to 1.02 times sync from 256 MiB up, and copied at 1.36 to 1.49 times, against 1.33 to 1.46 in the 4.1.0 run.
   The 64 MiB transfers ran at 0.74 to 1.29 times sync, without a consistent order among the settings, as in the 4.1.0 run.
3. **Writes no longer held the whole object in memory** (#1165).
   Writing the 6 GiB object peaked at 690 to 699 MiB with 40 workers and 302 to 305 MiB with 8 workers, with either API, against 6,005 to 6,313 MiB in the 4.1.0 run; the 1 GiB object peaked at 688 to 691 and 302 to 304 MiB, against 1,245 to 1,376 MiB.
4. **Sync transfers of large objects ran faster than in the 4.1.0 run.**
   Sync wrote the 6 GiB object at 1,050 to 1,074 MiB/s with 40 workers and 553 to 560 MiB/s with 8 workers, against 861 to 864 and 470 to 487 MiB/s, and copied it with 40 workers at 1,835 to 1,879 MiB/s, against 1,495 to 1,781 MiB/s.
   The cause of these differences was not investigated.

For #1159 and #685, the aio paths now match the sync paths for writes and copies, and `S3FileSystem` stays the filesystem that PyAthena registers for fsspec's `s3` and `s3a` protocols.
`AioS3FileSystem` is recommended for asyncio applications, and making it the default is to be revisited when boto3 supports asyncio natively, because `AioS3FileSystem` still runs the blocking boto3 calls in threads.

## Limits

- One host and five trials per case; one `c8gn.2xlarge` host in one Availability Zone.
- The objects hold zero bytes; S3 does not compress them, but other content was not measured.
- Reads of the 6 GiB object did not run, and copies only ran at 6 GiB.
- Comparisons with the 4.1.0 run use the same instance type and selection, but different hosts and days.

## Results

Median throughput is the object size divided by the median transfer time, in MiB per second; threads are the peak thread count of the trial process, and RSS its peak sampled resident memory, including the harness's common imports.

| Scale | Operation | Workers | Pool | sync (MiB/s) | aio (MiB/s) | aio / sync | sync threads | aio threads | sync peak RSS (MiB) | aio peak RSS (MiB) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small (64 MiB) | read | 8 | 10 | 295 | 253 | 0.86 | 19 | 21 | 332 | 322 |
| small (64 MiB) | read | 8 | 40 | 321 | 239 | 0.74 | 19 | 21 | 328 | 328 |
| small (64 MiB) | read | 40 | 10 | 308 | 230 | 0.75 | 19 | 21 | 330 | 344 |
| small (64 MiB) | read | 40 | 40 | 283 | 266 | 0.94 | 19 | 21 | 321 | 347 |
| small (64 MiB) | write | 8 | 10 | 131 | 168 | 1.29 | 19 | 21 | 285 | 287 |
| small (64 MiB) | write | 8 | 40 | 171 | 145 | 0.85 | 19 | 21 | 285 | 287 |
| small (64 MiB) | write | 40 | 10 | 162 | 170 | 1.05 | 19 | 21 | 285 | 287 |
| small (64 MiB) | write | 40 | 40 | 170 | 148 | 0.87 | 19 | 21 | 285 | 287 |
| medium (256 MiB) | read | 8 | 10 | 423 | 425 | 1.01 | 19 | 21 | 658 | 650 |
| medium (256 MiB) | read | 8 | 40 | 413 | 414 | 1.00 | 19 | 21 | 655 | 638 |
| medium (256 MiB) | read | 40 | 10 | 512 | 511 | 1.00 | 43 | 45 | 689 | 847 |
| medium (256 MiB) | read | 40 | 40 | 514 | 507 | 0.99 | 43 | 45 | 813 | 764 |
| medium (256 MiB) | write | 8 | 10 | 348 | 329 | 0.95 | 19 | 21 | 301 | 304 |
| medium (256 MiB) | write | 8 | 40 | 343 | 327 | 0.95 | 19 | 21 | 302 | 303 |
| medium (256 MiB) | write | 40 | 10 | 408 | 452 | 1.11 | 43 | 45 | 575 | 577 |
| medium (256 MiB) | write | 40 | 40 | 473 | 452 | 0.96 | 43 | 45 | 575 | 577 |
| large (1 GiB) | read | 8 | 10 | 438 | 465 | 1.06 | 19 | 21 | 2,220 | 2,207 |
| large (1 GiB) | read | 8 | 40 | 468 | 452 | 0.96 | 19 | 21 | 2,182 | 2,184 |
| large (1 GiB) | read | 40 | 10 | 602 | 600 | 1.00 | 51 | 53 | 2,423 | 2,369 |
| large (1 GiB) | read | 40 | 40 | 592 | 592 | 1.00 | 51 | 53 | 2,240 | 2,222 |
| large (1 GiB) | write | 8 | 10 | 496 | 506 | 1.02 | 19 | 21 | 302 | 304 |
| large (1 GiB) | write | 8 | 40 | 512 | 494 | 0.97 | 19 | 21 | 302 | 304 |
| large (1 GiB) | write | 40 | 10 | 831 | 804 | 0.97 | 51 | 53 | 688 | 691 |
| large (1 GiB) | write | 40 | 40 | 823 | 815 | 0.99 | 51 | 53 | 688 | 691 |
| xlarge (6 GiB) | write | 8 | 10 | 553 | 561 | 1.01 | 19 | 21 | 302 | 305 |
| xlarge (6 GiB) | write | 8 | 40 | 560 | 566 | 1.01 | 19 | 21 | 303 | 305 |
| xlarge (6 GiB) | write | 40 | 10 | 1,050 | 1,052 | 1.00 | 51 | 53 | 690 | 699 |
| xlarge (6 GiB) | write | 40 | 40 | 1,074 | 1,047 | 0.98 | 51 | 53 | 690 | 692 |
| xlarge (6 GiB) | copy | 8 | 10 | 285 | 425 | 1.49 | 19 | 20 | 191 | 192 |
| xlarge (6 GiB) | copy | 8 | 40 | 299 | 407 | 1.36 | 19 | 20 | 191 | 192 |
| xlarge (6 GiB) | copy | 40 | 10 | 1,835 | 1,925 | 1.05 | 51 | 52 | 217 | 218 |
| xlarge (6 GiB) | copy | 40 | 40 | 1,879 | 1,889 | 1.01 | 51 | 52 | 217 | 218 |

### Compared with 4.1.0

The same cases in the [4.1.0 run](2026-10-13-437276e1-filesystem.md).

| Scale | Operation | Workers | Pool | sync 4.1.0 (MiB/s) | sync (MiB/s) | aio 4.1.0 (MiB/s) | aio (MiB/s) | aio / sync 4.1.0 | aio / sync | aio threads 4.1.0 | aio threads |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small (64 MiB) | read | 8 | 10 | 215 | 295 | 304 | 253 | 1.41 | 0.86 | 21 | 21 |
| small (64 MiB) | read | 8 | 40 | 260 | 321 | 309 | 239 | 1.19 | 0.74 | 21 | 21 |
| small (64 MiB) | read | 40 | 10 | 270 | 308 | 312 | 230 | 1.16 | 0.75 | 21 | 21 |
| small (64 MiB) | read | 40 | 40 | 274 | 283 | 202 | 266 | 0.74 | 0.94 | 21 | 21 |
| small (64 MiB) | write | 8 | 10 | 162 | 131 | 175 | 168 | 1.08 | 1.29 | 21 | 21 |
| small (64 MiB) | write | 8 | 40 | 172 | 171 | 167 | 145 | 0.97 | 0.85 | 21 | 21 |
| small (64 MiB) | write | 40 | 10 | 146 | 162 | 163 | 170 | 1.12 | 1.05 | 21 | 21 |
| small (64 MiB) | write | 40 | 40 | 180 | 170 | 162 | 148 | 0.90 | 0.87 | 21 | 21 |
| medium (256 MiB) | read | 8 | 10 | 382 | 423 | 418 | 425 | 1.09 | 1.01 | 22 | 21 |
| medium (256 MiB) | read | 8 | 40 | 359 | 413 | 412 | 414 | 1.15 | 1.00 | 23 | 21 |
| medium (256 MiB) | read | 40 | 10 | 488 | 512 | 457 | 511 | 0.94 | 1.00 | 25 | 45 |
| medium (256 MiB) | read | 40 | 40 | 509 | 514 | 468 | 507 | 0.92 | 0.99 | 25 | 45 |
| medium (256 MiB) | write | 8 | 10 | 302 | 348 | 329 | 329 | 1.09 | 0.95 | 22 | 21 |
| medium (256 MiB) | write | 8 | 40 | 321 | 343 | 346 | 327 | 1.08 | 0.95 | 22 | 21 |
| medium (256 MiB) | write | 40 | 10 | 453 | 408 | 407 | 452 | 0.90 | 1.11 | 25 | 45 |
| medium (256 MiB) | write | 40 | 40 | 448 | 473 | 421 | 452 | 0.94 | 0.96 | 25 | 45 |
| large (1 GiB) | read | 8 | 10 | 432 | 438 | 448 | 465 | 1.04 | 1.06 | 23 | 21 |
| large (1 GiB) | read | 8 | 40 | 453 | 468 | 449 | 452 | 0.99 | 0.96 | 24 | 21 |
| large (1 GiB) | read | 40 | 10 | 550 | 602 | 581 | 600 | 1.06 | 1.00 | 25 | 53 |
| large (1 GiB) | read | 40 | 40 | 565 | 592 | 590 | 592 | 1.04 | 1.00 | 25 | 53 |
| large (1 GiB) | write | 8 | 10 | 448 | 496 | 439 | 506 | 0.98 | 1.02 | 25 | 21 |
| large (1 GiB) | write | 8 | 40 | 423 | 512 | 433 | 494 | 1.03 | 0.97 | 25 | 21 |
| large (1 GiB) | write | 40 | 10 | 718 | 831 | 593 | 804 | 0.83 | 0.97 | 25 | 53 |
| large (1 GiB) | write | 40 | 40 | 712 | 823 | 575 | 815 | 0.81 | 0.99 | 25 | 53 |
| xlarge (6 GiB) | write | 8 | 10 | 470 | 553 | 497 | 561 | 1.06 | 1.01 | 25 | 21 |
| xlarge (6 GiB) | write | 8 | 40 | 487 | 560 | 485 | 566 | 1.00 | 1.01 | 25 | 21 |
| xlarge (6 GiB) | write | 40 | 10 | 861 | 1,050 | 647 | 1,052 | 0.75 | 1.00 | 25 | 53 |
| xlarge (6 GiB) | write | 40 | 40 | 864 | 1,074 | 665 | 1,047 | 0.77 | 0.98 | 25 | 53 |
| xlarge (6 GiB) | copy | 8 | 10 | 267 | 285 | 390 | 425 | 1.46 | 1.49 | 24 | 20 |
| xlarge (6 GiB) | copy | 8 | 40 | 274 | 299 | 366 | 407 | 1.33 | 1.36 | 24 | 20 |
| xlarge (6 GiB) | copy | 40 | 10 | 1,495 | 1,835 | 521 | 1,925 | 0.35 | 1.05 | 24 | 52 |
| xlarge (6 GiB) | copy | 40 | 40 | 1,781 | 1,879 | 540 | 1,889 | 0.30 | 1.01 | 24 | 52 |
