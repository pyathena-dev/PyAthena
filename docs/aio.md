(aio)=

# Native Asyncio Cursors

PyAthena provides native asyncio cursor implementations under `pyathena.aio`.
These cursors use `asyncio.sleep` for polling and `asyncio.to_thread` for boto3 calls,
keeping the event loop free without relying on thread pools for concurrency.

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

All aio cursors use `await` for fetch operations. The S3 download (CSV or Parquet)
happens inside `execute()`, wrapped in `asyncio.to_thread()`. Fetch methods are also
wrapped in `asyncio.to_thread()` to ensure the event loop is never blocked — this is
especially important when `chunksize` is set, as fetch calls trigger lazy S3 reads.

```python
await cursor.execute("SELECT * FROM many_rows")
row = await cursor.fetchone()
rows = await cursor.fetchall()
df = cursor.as_pandas()  # In-memory conversion, no await needed
```

The `as_pandas()`, `as_arrow()`, and `as_polars()` convenience methods operate on
already-loaded data and remain synchronous.

See each cursor's documentation page for detailed usage examples.
