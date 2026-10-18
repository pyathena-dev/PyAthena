(aio)=

# Native Asyncio Cursors

PyAthena provides native asyncio cursor implementations under `pyathena.aio`.
These cursors use `asyncio.sleep` for polling and `asyncio.to_thread` for boto3 calls,
keeping the event loop free. Concurrency comes from asyncio tasks rather than a
`ThreadPoolExecutor` owned by the cursor; the boto3 calls run on the event loop's default executor.

## Why native asyncio?

PyAthena has two families of async cursors:

| | AsyncCursor | AioCursor |
|---|---|---|
| **Concurrency model** | `concurrent.futures.ThreadPoolExecutor` | Native `asyncio` (`await` / `async for`) |
| **Event loop** | Blocks a thread per query | Non-blocking |
| **Connection** | `connect()` (sync) | `aio_connect()` (async) |
| **execute()** returns | `(query_id, Future)` | Awaitable cursor (self) |
| **Fetch methods** | Sync (via `Future.result()`) | `await cursor.fetchone()` for streaming cursors |
| **Iteration** | `for row in result_set` | `async for row in cursor` |
| **Context manager** | `with conn.cursor() as cursor` | `async with conn.cursor() as cursor` |
| **Best for** | Adding concurrency to sync code | Async frameworks (FastAPI, aiohttp, etc.) |

Choose `AioCursor` when your application already uses `asyncio` (e.g., web frameworks,
async pipelines). Choose `AsyncCursor` when you want simple parallel query execution
from synchronous code.

(aio-connection)=

## Connection

Use the `aio_connect()` function to create an async connection.
It returns an `AioConnection` that produces `AioCursor` instances by default.

```python
from pyathena import aio_connect

conn = await aio_connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                      region_name="us-west-2")
```

The connection supports the async context manager protocol:

```python
from pyathena import aio_connect

async with await aio_connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor()
    await cursor.execute("SELECT 1")
    print(await cursor.fetchone())
```

(aio-cursor)=

## AioCursor

AioCursor is a native asyncio cursor that uses `await` for query execution and result fetching.
It follows the DB API 2.0 interface adapted for async usage.

```python
from pyathena import aio_connect
from pyathena.aio.cursor import AioCursor

async with await aio_connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor()
    await cursor.execute("SELECT * FROM many_rows")
    print(await cursor.fetchone())
    print(await cursor.fetchmany(10))
    print(await cursor.fetchall())
```

The cursor supports the `async with` context manager:

```python
from pyathena import aio_connect

async with await aio_connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT * FROM many_rows")
        rows = await cursor.fetchall()
```

Leaving the block closes the cursor in a thread without blocking the event loop, and `execute()` and `executemany()` close the previous result the same way.
Calling `cursor.close()` directly closes it on the calling thread.

You can iterate over results with `async for`:

```python
from pyathena import aio_connect

async with await aio_connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT * FROM many_rows")
        async for row in cursor:
            print(row)
```

Execution information of the query can also be retrieved:

```python
from pyathena import aio_connect

async with await aio_connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT * FROM many_rows")
        print(cursor.state)
        print(cursor.state_change_reason)
        print(cursor.completion_date_time)
        print(cursor.submission_date_time)
        print(cursor.data_scanned_in_bytes)
        print(cursor.engine_execution_time_in_millis)
        print(cursor.query_queue_time_in_millis)
        print(cursor.total_execution_time_in_millis)
        print(cursor.query_planning_time_in_millis)
        print(cursor.service_processing_time_in_millis)
        print(cursor.output_location)
```

To cancel a running query:

```python
from pyathena import aio_connect

async with await aio_connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT * FROM many_rows")
        await cursor.cancel()
```

(aio-task-cancellation)=

### Task cancellation

With `kill_on_interrupt` enabled, which is the default, cancelling the task while `execute()` waits for the query
requests cancellation of the query, waits until it reaches a terminal state, and then raises `asyncio.CancelledError`.
Cancellation is a best-effort request, so the query can still end as `SUCCEEDED` or `FAILED`.
The `query_id` property keeps the ID of the cancelled query.
If the cancellation request or that wait fails, `asyncio.CancelledError` is raised with the error as its cause.
Cancelling the task while `execute()` is still starting the query first waits for the start request to finish,
and then cancels the query it started in the same way.
If the task is cancelled before `execute()` begins the request, the request is never sent.
Cancelling the task again during the cancellation request or these waits raises `asyncio.CancelledError` immediately, and the query can keep running.
With `kill_on_interrupt=False`, `asyncio.CancelledError` is raised immediately, and a query that has already started keeps running.

With `kill_on_interrupt` enabled, a timeout from `asyncio.wait_for()` while `execute()` starts or waits for the query therefore cancels it and raises `asyncio.TimeoutError`.
If the timeout expires while `execute()` is still looking up a cached result, no query is started and `query_id` is `None`.

```python
import asyncio

from pyathena import aio_connect

async with await aio_connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    async with conn.cursor() as cursor:
        try:
            await asyncio.wait_for(cursor.execute("SELECT * FROM many_rows"), timeout=60)
        except asyncio.TimeoutError:
            print(f"Query timed out: {cursor.query_id}")
```

Cancelling a task also stops the retries of the Athena API requests that the asyncio cursors make in a thread for it, such as the status polling of `execute()` and the page requests of `AioCursor`.
No further attempt is made, and the wait before the next attempt ends.
A request that has already been sent still runs to its end in its thread.
The requests that stop a query or a calculation and the request that terminates a Spark session keep their retries.
The start request of `execute()` with `kill_on_interrupt` enabled keeps its retries after a cancellation, and stops them when the task is cancelled again.
After a fetch is cancelled, later fetches of its result raise `ProgrammingError`.
The fetch methods of `AioPandasCursor`, `AioArrowCursor`, `AioPolarsCursor`, and `AioS3FSCursor` run in a thread.
A cancellation skips a fetch that has not started, and a started fetch keeps running in its thread without the retries of its requests.
The cursor drops the result, which is closed once its fetch is no longer running.

(aio-dict-cursor)=

## AioDictCursor

AioDictCursor is an AioCursor that returns rows as dictionaries with column names as keys.

```python
from pyathena import aio_connect
from pyathena.aio.cursor import AioDictCursor

async with await aio_connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioDictCursor)
    await cursor.execute("SELECT * FROM many_rows LIMIT 10")
    async for row in cursor:
        print(row["a"])
```

If you want to change the dictionary type (e.g., use OrderedDict):

```python
from collections import OrderedDict
from pyathena import aio_connect
from pyathena.aio.cursor import AioDictCursor

async with await aio_connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioDictCursor, dict_type=OrderedDict)
    await cursor.execute("SELECT * FROM many_rows LIMIT 10")
    async for row in cursor:
        print(row)
```

## Specialized Aio Cursors

Native asyncio versions are available for all cursor types:

| Cursor | Module | Result format |
|--------|--------|---------------|
| {ref}`AioPandasCursor <aio-pandas-cursor>` | `pyathena.aio.pandas.cursor` | pandas DataFrame |
| {ref}`AioArrowCursor <aio-arrow-cursor>` | `pyathena.aio.arrow.cursor` | pyarrow Table |
| {ref}`AioPolarsCursor <aio-polars-cursor>` | `pyathena.aio.polars.cursor` | polars DataFrame |
| {ref}`AioS3FSCursor <aio-s3fs-cursor>` | `pyathena.aio.s3fs.cursor` | Row tuples (lightweight) |
| {ref}`AioSparkCursor <aio-spark-cursor>` | `pyathena.aio.spark.cursor` | PySpark execution |

### Fetch behavior

All aio cursors use `await` for fetch operations, so fetching does not block the event loop.

- `AioCursor` and `AioDictCursor` page through `GetQueryResults` as rows are fetched.
- `AioPandasCursor`, `AioArrowCursor`, and `AioPolarsCursor` download the result file (CSV or
  Parquet) inside `execute()`, wrapped in `asyncio.to_thread()`. When CSV results are read in
  chunks (`chunksize` for pandas and Polars, or a chunk size chosen by `auto_optimize_chunksize`
  for pandas), or with `chunksize` on Polars UNLOAD results, fetch calls read S3 lazily instead.
  The fetch methods also run in a thread.
- `AioS3FSCursor` streams rows from the result file in S3 as they are fetched.

With managed query result storage, where the query has no S3 output location, the pandas, Arrow,
Polars, and S3FS cursors instead read every row through `GetQueryResults` inside `execute()`.

```python
await cursor.execute("SELECT * FROM many_rows")
row = await cursor.fetchone()
rows = await cursor.fetchall()

await cursor.execute("SELECT * FROM many_rows")
df = cursor.as_pandas()  # In-memory conversion, no await needed
```

The `as_pandas()`, `as_arrow()`, and `as_polars()` convenience methods are synchronous.
When `execute()` has loaded the whole result, they return that data.
When the result is read in chunks, they read S3 on the calling thread and block the event loop.
With `chunksize` on CSV results, `as_pandas()` returns an iterator that reads each chunk as it is iterated, and
`as_polars()` reads every remaining chunk.
With a chunk size chosen by `auto_optimize_chunksize`, `as_pandas()` reads every remaining chunk.

See each cursor's documentation page for detailed usage examples.

(aio-s3-filesystem)=

## AioS3FileSystem

`AioS3FileSystem` is a native asyncio filesystem interface for Amazon S3, built on
fsspec's `AsyncFileSystem`. It provides the same functionality as `S3FileSystem`, and
its coroutines run the blocking boto3 calls in threads that PyAthena owns, sized by
`max_workers`, instead of the default executor of the event loop.

| | S3FileSystem | AioS3FileSystem |
|---|---|---|
| **Blocking calls** | Caller's thread | Thread pool of the filesystem |
| **Parallelism** | `ThreadPoolExecutor` | `asyncio.gather` on a thread pool |
| **File handles** | `S3File` with thread pool | `AioS3File` with `S3AioExecutor`; `AioS3StreamedFile` from `open_async()` |
| **Bulk delete** | Thread pool per batch | `asyncio.gather` per batch |
| **Multipart copy** | Thread pool per part | Asyncio task per part |
| **Best for** | Synchronous applications | Async frameworks (FastAPI, aiohttp, etc.) |

### Threads

The threads of `AioS3FileSystem` belong to `S3AioThreadPool` instances, not to the
default executor of the event loop, so `loop.set_default_executor()` neither limits nor
blocks them:

- The filesystem has a pool of `max_workers` threads for the blocking calls of its
  coroutines, such as `_info()`, `_cat_file()`, the requests of `_rm()`, and the part
  copies of multipart copies. A multipart copy runs at most the `max_workers` of the
  call of its part copies at once, and the copies together run at most the
  `max_workers` of the filesystem.
- Each file opened with `open()` or `open_async()` has a pool of its own, sized by the
  `max_workers` of the call.

The pools create threads as the calls need them, and each call runs with the caller's
context variables, as with `asyncio.to_thread()`.
In a forked child process, a pool creates new threads.

Cancelling a coroutine of the filesystem also stops the retries of the S3 requests that
its call makes in these threads, including the part uploads and range reads that the
call runs in parallel: no further attempt is made, and the wait before the next attempt
ends. A request that has already been sent still runs to its end in its thread. The
requests that run to their end after a cancellation keep their retries: the commit of a
file, the CompleteMultipartUpload request of a multipart copy, and the aborts of
multipart uploads.

### Executor strategy

`S3FileSystem` and `S3File` use a pluggable executor abstraction (`S3Executor`) for
parallel operations. Two implementations are provided:

- `S3ThreadPoolExecutor` — wraps `ThreadPoolExecutor` (default for sync usage)
- `S3AioExecutor` — dispatches work via `asyncio.run_coroutine_threadsafe` to an
  `S3AioThreadPool` of its own

`AioS3FileSystem` automatically uses `S3AioExecutor` for the file handles of `open()`,
so multipart uploads and parallel range reads are dispatched through the event loop to
a pool of `max_workers` threads of each file.
An instance created with `asynchronous=True` has no event loop of its own, so its file handles use `S3ThreadPoolExecutor`.

### Async file handles

`open_async()` returns an `AioS3StreamedFile`, whose `read()`, `write()`, and `close()`
are coroutines. It supports the `rb`, `wb`, `ab`, and `xb` modes:

```python
from pyathena.filesystem.s3_async import AioS3FileSystem

fs = AioS3FileSystem(asynchronous=True)

async with await fs.open_async("s3://my-bucket/data/file.bin", "wb") as f:
    await f.write(data)

async with await fs.open_async("s3://my-bucket/data/file.bin", "rb") as f:
    data = await f.read()
```

- A read splits the requested range into ranges of the block size, and reads them in
  parallel. When a range fails, the reads that have not finished are cancelled, and the
  running ones make no further attempt. The file has no read cache.
- A write uploads each full block as a part of a multipart upload, with at most
  `max_workers` parts, including the parts copied from the existing object in an
  append, in flight. A write that would upload another part awaits one of them, so the
  event loop keeps running. A part that has failed by the next write, or before the
  write uploads another part, stops the write with its error, and the multipart upload
  is aborted. A part that fails after the last write raises its error when the file is
  closed. The commit cancels the parts that have not started as soon as one fails, and
  waits for the running ones, which make no further attempt, before it aborts the upload
  and raises the error.
- Closing the file commits the written data, as `S3File` does.
- Leaving an `async with` block with an exception, or cancelling a write, closes the file
  without committing it, and aborts the multipart upload after the running parts, which
  make no further attempt, finish.
  The abort completes even if the task is cancelled again.
- Once closing or the final flush has begun the commit, the commit completes even if
  the task that closes or flushes the file is cancelled, and leaving an `async with`
  block with an exception afterward does not abort it. A commit that fails aborts the
  upload; leaving the block after a cancelled final flush while that abort runs waits
  for it.
- A file opened for writing must be closed with `await f.close()` or an `async with`
  block. A file that is garbage collected before it is closed or its commit has begun
  is not committed: it emits a `ResourceWarning`, and its multipart upload is aborted.
  A synchronous `with` block raises `TypeError`.
- `open_async()` raises `NotImplementedError` for the write modes inside a transaction,
  and for `autocommit=False`, because the file commits when it is closed.

### Usage with AioS3FSCursor

`AioS3FSCursor` automatically uses `AioS3FileSystem` internally. No additional
configuration is needed:

```python
from pyathena import aio_connect
from pyathena.aio.s3fs.cursor import AioS3FSCursor

async with await aio_connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioS3FSCursor)
    await cursor.execute("SELECT * FROM many_rows")
    async for row in cursor:
        print(row)
```

### Standalone usage

`AioS3FileSystem` can also be used directly for S3 operations:

```python
from pyathena.filesystem.s3_async import AioS3FileSystem

# Async context
fs = AioS3FileSystem(asynchronous=True)

files = await fs._ls("s3://my-bucket/data/")
data = await fs._cat_file("s3://my-bucket/data/file.csv")
await fs._rm("s3://my-bucket/data/old/", recursive=True)
```

fsspec also generates synchronous wrappers such as `ls()`.
Call them on an instance created without `asynchronous=True`; each call blocks the caller until it completes:

```python
from pyathena.filesystem.s3_async import AioS3FileSystem

fs = AioS3FileSystem()
files = fs.ls("s3://my-bucket/data/")
```
