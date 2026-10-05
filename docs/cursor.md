# Cursor

(cursor-workers)=

## Worker settings in 4.0.0

The thread-pool cursors use `max_workers` for their pool of tasks that wait for queries and collect results.
Query submission happens before the task is queued, so this setting does not cap the number of queries running in Athena.
The pandas, Arrow, and Polars cursors use `s3_max_workers` for the workers of each PyAthena S3 file reader.
It does not set a shared limit across queries or change the parsing library's CPU thread pool.

| Cursor | `max_workers` | `s3_max_workers` default |
|--------|---------------|--------------------------|
| `PandasCursor`, `PolarsCursor`, `AioPandasCursor`, `AioPolarsCursor` | No query pool | `(cpu_count() or 1) * 5` |
| `AsyncPandasCursor`, `AsyncPolarsCursor` | Query task pool | `(cpu_count() or 1) * 5` |
| `ArrowCursor`, `AioArrowCursor` | No query pool | `None` (native PyArrow S3 filesystem) |
| `AsyncArrowCursor` | Query task pool | `None` (native PyArrow S3 filesystem) |

Pass `s3_max_workers` to `execute()` to override the setting for one query.
The next call without an override uses the cursor's setting again.
For Arrow, a positive integer selects PyAthena's S3 filesystem through PyArrow's fsspec adapter for both CSV and UNLOAD results.
An explicit `None` selects the native PyArrow filesystem for that query.
On the PyAthena path, Arrow's `request_timeout` sets the boto3 read timeout; `connect_timeout` sets the connection timeout.
The connection's S3 configuration supplies defaults for unspecified timeouts.

```python
from pyathena.pandas.async_cursor import AsyncPandasCursor

cursor = connection.cursor(AsyncPandasCursor, max_workers=4, s3_max_workers=2)
query_id, future = cursor.execute("SELECT 1", s3_max_workers=3)
result = future.result()
```

The S3 setting applies when PyAthena's S3 filesystem reads the results.
User-provided pandas `filesystem` or `storage_options` can replace that filesystem and its settings.
Polars uses PyAthena's filesystem for CSV results; its native Parquet reader uses its own concurrency settings.

### Migrate the former S3 argument

This is a breaking API change in 4.0.0.
Replace `max_workers=N` with `s3_max_workers=N` in synchronous and native asyncio pandas/Polars cursor constructors, and in pandas/Polars `execute()` calls.
Keep `max_workers` in thread-pool cursor constructors to size the query task pool.
For `AsyncPolarsCursor`, specify both arguments to preserve the former behavior of controlling both pools with one value.
`AsyncPandasCursor` now also accepts an S3 worker default in its constructor.
The former S3 keyword raises `TypeError`; nonpositive S3 worker values raise `ValueError` before a query starts.
The `max_workers` names on result sets, S3 filesystems, and pandas upload utilities retain their existing meanings.

(default_cursor)=

## DefaultCursor

See {ref}`usage`.

(dict-cursor)=

## DictCursor

DictCursor retrieve the query execution result as a dictionary type with column names and values.

You can use the DictCursor by specifying the `cursor_class`
with the connect method or connection object.

```python
from pyathena import connect
from pyathena.cursor import DictCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=DictCursor).cursor()
```

```python
from pyathena.connection import Connection
from pyathena.cursor import DictCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2",
                    cursor_class=DictCursor).cursor()
```

It can also be used by specifying the cursor class when calling the connection object's cursor method.

```python
from pyathena import connect
from pyathena.cursor import DictCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor(DictCursor)
```

```python
from pyathena.connection import Connection
from pyathena.cursor import DictCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2").cursor(DictCursor)
```

The basic usage is the same as the Cursor.

```python
from pyathena.connection import Connection
from pyathena.cursor import DictCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2").cursor(DictCursor)
cursor.execute("SELECT * FROM many_rows LIMIT 10")
for row in cursor:
    print(row["a"])
```

If you want to change the dictionary type (e.g., use OrderedDict), you can specify like the following.

```python
from collections import OrderedDict
from pyathena import connect
from pyathena.cursor import DictCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=DictCursor).cursor(dict_type=OrderedDict)
```

```python
from collections import OrderedDict
from pyathena import connect
from pyathena.cursor import DictCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor(cursor=DictCursor, dict_type=OrderedDict)
```

(async-cursor)=

## AsyncCursor

AsyncCursor is a simple implementation using the concurrent.futures package.
This cursor does not follow the [DB API 2.0 (PEP 249)](https://www.python.org/dev/peps/pep-0249/).

You can use the AsyncCursor by specifying the `cursor_class`
with the connect method or connection object.

```python
from pyathena import connect
from pyathena.async_cursor import AsyncCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncCursor).cursor()
```

```python
from pyathena.connection import Connection
from pyathena.async_cursor import AsyncCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2",
                    cursor_class=AsyncCursor).cursor()
```

It can also be used by specifying the cursor class when calling the connection object's cursor method.

```python
from pyathena import connect
from pyathena.async_cursor import AsyncCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor(AsyncCursor)
```

```python
from pyathena.connection import Connection
from pyathena.async_cursor import AsyncCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2").cursor(AsyncCursor)
```

The default number of workers is 5 or cpu number * 5.
If you want to change the number of workers you can specify like the following.

```python
from pyathena import connect
from pyathena.async_cursor import AsyncCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncCursor).cursor(max_workers=10)
```

The execute method of the AsyncCursor returns the tuple of the query ID and the [future object](https://docs.python.org/3/library/concurrent.futures.html#future-objects).

```python
from pyathena import connect
from pyathena.async_cursor import AsyncCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncCursor).cursor()

query_id, future = cursor.execute("SELECT * FROM many_rows")
```

The return value of the [future object](https://docs.python.org/3/library/concurrent.futures.html#future-objects) is an `AthenaResultSet` object.
This object has an interface that can fetch and iterate query results similar to synchronous cursors.
It also has information on the result of query execution.

```python
from pyathena import connect
from pyathena.async_cursor import AsyncCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncCursor).cursor()
query_id, future = cursor.execute("SELECT * FROM many_rows")
result_set = future.result()
print(result_set.state)
print(result_set.state_change_reason)
print(result_set.completion_date_time)
print(result_set.submission_date_time)
print(result_set.data_scanned_in_bytes)
print(result_set.engine_execution_time_in_millis)
print(result_set.query_queue_time_in_millis)
print(result_set.total_execution_time_in_millis)
print(result_set.query_planning_time_in_millis)
print(result_set.service_processing_time_in_millis)
print(result_set.output_location)
print(result_set.description)
for row in result_set:
    print(row)
```

```python
from pyathena import connect
from pyathena.async_cursor import AsyncCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncCursor).cursor()
query_id, future = cursor.execute("SELECT * FROM many_rows")
result_set = future.result()
print(result_set.fetchall())
```

A query ID is required to cancel a query with the AsyncCursor.

```python
from pyathena import connect
from pyathena.async_cursor import AsyncCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncCursor).cursor()
query_id, future = cursor.execute("SELECT * FROM many_rows")
cursor.cancel(query_id)
```

NOTE: The cancel method of the [future object](https://docs.python.org/3/library/concurrent.futures.html#future-objects) does not cancel the query.

(async-dict-cursor)=

## AsyncDictCursor

AsyncDictCursor is an AsyncCursor that can retrieve the query execution result
as a dictionary type with column names and values.

You can use the AsyncDictCursor by specifying the `cursor_class`
with the connect method or connection object.

```python
from pyathena import connect
from pyathena.async_cursor import AsyncDictCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncDictCursor).cursor()
```

```python
from pyathena.connection import Connection
from pyathena.async_cursor import AsyncDictCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2",
                    cursor_class=AsyncDictCursor).cursor()
```

It can also be used by specifying the cursor class when calling the connection object's cursor method.

```python
from pyathena import connect
from pyathena.async_cursor import AsyncDictCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor(AsyncDictCursor)
```

```python
from pyathena.connection import Connection
from pyathena.async_cursor import AsyncDictCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2").cursor(AsyncDictCursor)
```

The basic usage is the same as the AsyncCursor.

```python
from pyathena.connection import Connection
from pyathena.async_cursor import AsyncDictCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2").cursor(AsyncDictCursor)
query_id, future = cursor.execute("SELECT * FROM many_rows LIMIT 10")
result_set = future.result()
for row in result_set:
    print(row["a"])
```

If you want to change the dictionary type (e.g., use OrderedDict), you can specify like the following.

```python
from collections import OrderedDict
from pyathena import connect
from pyathena.async_cursor import AsyncDictCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncDictCursor).cursor(dict_type=OrderedDict)
```

```python
from collections import OrderedDict
from pyathena import connect
from pyathena.async_cursor import AsyncDictCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor(cursor=AsyncDictCursor, dict_type=OrderedDict)
```

## AioCursor

See {ref}`aio-cursor`.

## AioDictCursor

See {ref}`aio-dict-cursor`.

## PandasCursor

See {ref}`pandas-cursor`.

## AsyncPandasCursor

See {ref}`async-pandas-cursor`.

## AioPandasCursor

See {ref}`aio-pandas-cursor`.

## ArrowCursor

See {ref}`arrow-cursor`.

## AsyncArrowCursor

See {ref}`async-arrow-cursor`.

## AioArrowCursor

See {ref}`aio-arrow-cursor`.

## PolarsCursor

See {ref}`polars-cursor`.

## AsyncPolarsCursor

See {ref}`async-polars-cursor`.

## AioPolarsCursor

See {ref}`aio-polars-cursor`.

## S3FSCursor

See {ref}`s3fs-cursor`.

## AsyncS3FSCursor

See {ref}`async-s3fs-cursor`.

## AioS3FSCursor

See {ref}`aio-s3fs-cursor`.

## SparkCursor

See {ref}`spark-cursor`.

## AsyncSparkCursor

See {ref}`async-spark-cursor`.

## AioSparkCursor

See {ref}`aio-spark-cursor`.

For detailed API documentation of all cursor classes and their methods,
see the {ref}`api` section.
