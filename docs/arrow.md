(arrow)=

# Arrow

(arrow-cursor)=

## ArrowCursor

ArrowCursor directly handles the CSV file of the query execution result output to S3.
This cursor is to download the CSV file after executing the query, and then loaded into [pyarrow.Table object](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html).
Performance is better than fetching data with Cursor.

You can use the ArrowCursor by specifying the `cursor_class`
with the connect method or connection object.

```python
from pyathena import connect
from pyathena.arrow.cursor import ArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=ArrowCursor).cursor()
```

```python
from pyathena.connection import Connection
from pyathena.arrow.cursor import ArrowCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2",
                    cursor_class=ArrowCursor).cursor()
```

It can also be used by specifying the cursor class when calling the connection object's cursor method.

```python
from pyathena import connect
from pyathena.arrow.cursor import ArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor(ArrowCursor)
```

```python
from pyathena.connection import Connection
from pyathena.arrow.cursor import ArrowCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2").cursor(ArrowCursor)
```

The as_arrow method returns a [pyarrow.Table object](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html).

```python
from pyathena import connect
from pyathena.arrow.cursor import ArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=ArrowCursor).cursor()

table = cursor.execute("SELECT * FROM many_rows").as_arrow()
print(table)
print(table.column_names)
print(table.columns)
print(table.nbytes)
print(table.num_columns)
print(table.num_rows)
print(table.schema)
print(table.shape)
```

Support fetch and iterate query results.

```python
from pyathena import connect
from pyathena.arrow.cursor import ArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=ArrowCursor).cursor()

cursor.execute("SELECT * FROM many_rows")
print(cursor.fetchone())
print(cursor.fetchmany())
print(cursor.fetchall())
```

```python
from pyathena import connect
from pyathena.arrow.cursor import ArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=ArrowCursor).cursor()

cursor.execute("SELECT * FROM many_rows")
for row in cursor:
    print(row)
```

Execution information of the query can also be retrieved.

```python
from pyathena import connect
from pyathena.arrow.cursor import ArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=ArrowCursor).cursor()

cursor.execute("SELECT * FROM many_rows")
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

If you want to customize the [pyarrow.Table object](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html) types, create a converter class like this:

```python
import pyarrow as pa
from pyathena.arrow.converter import _to_date
from pyathena.converter import Converter

class CustomArrowTypeConverter(Converter):
    def __init__(self) -> None:
        super().__init__(
            mappings={
                "date": _to_date,
            },
            types={
                "boolean": pa.bool_(),
                "tinyint": pa.int8(),
                "smallint": pa.int16(),
                "integer": pa.int32(),
                "bigint": pa.int64(),
                "float": pa.float32(),
                "real": pa.float64(),
                "double": pa.float64(),
                "char": pa.string(),
                "varchar": pa.string(),
                "string": pa.string(),
                "timestamp": pa.timestamp("ms"),
                "date": pa.timestamp("ms"),
                "time": pa.string(),
                "varbinary": pa.string(),
                "array": pa.string(),
                "map": pa.string(),
                "row": pa.string(),
                "decimal": pa.string(),
                "json": pa.string(),
            },
        )

def convert(self, type_, value):
    converter = self.get(type_)
    return converter(value)
```

`types` is used to explicitly specify the Arrow type when reading CSV files.
`mappings` is used as a conversion method when fetching data from a cursor object.

Then you simply specify an instance of this class in the convertes argument when creating a connection or cursor.

```python
from pyathena import connect
from pyathena.arrow.cursor import ArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor(ArrowCursor, converter=CustomArrowTypeConverter())
```

```python
from pyathena import connect
from pyathena.arrow.cursor import ArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 converter=CustomArrowTypeConverter()).cursor(ArrowCursor)
```

If the unload option is enabled, the Parquet file itself has a schema, so the conversion is done to the Arrow type according to that schema,
and the `types` setting of the Converter class is not used.

(arrow-unload-options)=

### Unload options

ArrowCursor supports the unload option. When this option is enabled,
queries with SELECT statements are automatically converted to unload statements and executed to Athena,
and the results are output in Parquet format (Snappy compressed) to `s3_staging_dir`.
The cursor reads the output Parquet file directly.

The output of query results with the unload statement is faster than normal query execution.
In addition, the output Parquet file is split and can be read faster than a CSV file.
We recommend trying this option if you are concerned about the time it takes to execute the query and retrieve the results.

However, unload has some limitations. Please refer to the [official unload documentation](https://docs.aws.amazon.com/athena/latest/ug/unload.html) for more information on limitations.
As per the limitations of the official documentation, the results of unload will be written to multiple files in parallel,
and the contents of each file will be in sort order, but the relative order of the files to each other will not be sorted.
Note that specifying ORDER BY with this option enabled does not guarantee the sort order of the data.

The unload option can be enabled by specifying it in the `cursor_kwargs` argument of the connect method or as an argument to the cursor method.

```python
from pyathena import connect
from pyathena.arrow.cursor import ArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=ArrowCursor,
                 cursor_kwargs={
                     "unload": True
                 }).cursor()
```

```python
from pyathena import connect
from pyathena.arrow.cursor import ArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=ArrowCursor).cursor(unload=True)
```

SQLAlchemy allows this option to be specified in the connection string.

```text
awsathena+arrow://:@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}&unload=true...
```

If a `NOT_SUPPORTED` occurs, a type not supported by unload is included in the result of the SELECT.
Try converting to another type, such as `SELECT CAST(1 AS VARCHAR) AS name`.

```text
pyathena.error.OperationalError: NOT_SUPPORTED: Unsupported Hive type: time
```

In most cases of `SYNTAX_ERROR`, you forgot to alias the column in the SELECT result.
Try adding an alias to the SELECTed column, such as `SELECT 1 AS name`.

```text
pyathena.error.OperationalError: SYNTAX_ERROR: line 1:1: Column name not specified at position 1
```

### S3 Timeout Configuration

ArrowCursor supports configuring S3 connection and request timeouts through `connect_timeout` and `request_timeout` parameters.
These parameters are particularly useful when experiencing timeout errors due to:

- Role assumption with AWS STS (cross-account access)
- High network latency between your environment and S3
- Connecting from regions far from the S3 bucket

By default, PyArrow uses AWS SDK default timeouts (typically 1 second for connection, 3 seconds for requests).
You can increase these values to accommodate slower authentication or network conditions.

```python
from pyathena import connect
from pyathena.arrow.cursor import ArrowCursor

# Configure higher timeouts for role assumption scenarios
cursor = connect(
    s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
    region_name="us-west-2",
    cursor_class=ArrowCursor,
    cursor_kwargs={
        "connect_timeout": 10.0,  # Socket connection timeout in seconds
        "request_timeout": 30.0   # Request timeout in seconds
    }
).cursor()
```

```python
from pyathena import connect
from pyathena.arrow.cursor import ArrowCursor

cursor = connect(
    s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
    region_name="us-west-2"
).cursor(ArrowCursor, connect_timeout=10.0, request_timeout=30.0)
```

The timeout parameters accept float values in seconds and apply to all S3 operations performed by the cursor,
including HeadObject and GetObject operations when retrieving query results.

```{note}
These timeout parameters require PyArrow >= 10.0.0, which added support for configuring S3FileSystem timeouts.
```

(async-arrow-cursor)=

## AsyncArrowCursor

AsyncArrowCursor is an AsyncCursor that can handle [pyarrow.Table object](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html).
This cursor directly handles the CSV of query results output to S3 in the same way as ArrowCursor.

You can use the AsyncArrowCursor by specifying the `cursor_class`
with the connect method or connection object.

```python
from pyathena import connect
from pyathena.arrow.async_cursor import AsyncArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncArrowCursor).cursor()
```

```python
from pyathena.connection import Connection
from pyathena.arrow.async_cursor import AsyncArrowCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2",
                    cursor_class=AsyncArrowCursor).cursor()
```

It can also be used by specifying the cursor class when calling the connection object's cursor method.

```python
from pyathena import connect
from pyathena.arrow.async_cursor import AsyncArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor(AsyncArrowCursor)
```

```python
from pyathena.connection import Connection
from pyathena.arrow.async_cursor import AsyncArrowCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2").cursor(AsyncArrowCursor)
```

The default number of workers is 5 or cpu number * 5.
If you want to change the number of workers you can specify like the following.

```python
from pyathena import connect
from pyathena.arrow.async_cursor import AsyncArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncArrowCursor).cursor(max_workers=10)
```

The execute method of the AsyncArrowCursor returns the tuple of the query ID and the [future object](https://docs.python.org/3/library/concurrent.futures.html#future-objects).

```python
from pyathena import connect
from pyathena.arrow.async_cursor import AsyncArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncArrowCursor).cursor()

query_id, future = cursor.execute("SELECT * FROM many_rows")
```

The return value of the [future object](https://docs.python.org/3/library/concurrent.futures.html#future-objects) is an `AthenaArrowResultSet` object.
This object has an interface similar to `AthenaResultSetObject`.

```python
from pyathena import connect
from pyathena.arrow.async_cursor import AsyncArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncArrowCursor).cursor()

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
from pyathena.arrow.async_cursor import AsyncArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncArrowCursor).cursor()

query_id, future = cursor.execute("SELECT * FROM many_rows")
result_set = future.result()
print(result_set.fetchall())
```

This object also has an as_arrow method that returns a [pyarrow.Table object](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html) similar to the ArrowCursor.

```python
from pyathena import connect
from pyathena.arrow.async_cursor import AsyncArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncArrowCursor).cursor()

query_id, future = cursor.execute("SELECT * FROM many_rows")
result_set = future.result()
table = result_set.as_arrow()
print(table)
print(table.column_names)
print(table.columns)
print(table.nbytes)
print(table.num_columns)
print(table.num_rows)
print(table.schema)
print(table.shape)
```

As with AsyncArrowCursor, you need a query ID to cancel a query.

```python
from pyathena import connect
from pyathena.arrow.async_cursor import AsyncArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncArrowCursor).cursor()

query_id, future = cursor.execute("SELECT * FROM many_rows")
cursor.cancel(query_id)
```

As with AsyncArrowCursor, the UNLOAD option is also available.

```python
from pyathena import connect
from pyathena.arrow.async_cursor import AsyncArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncArrowCursor,
                 cursor_kwargs={
                     "unload": True
                 }).cursor()
```

```python
from pyathena import connect
from pyathena.arrow.cursor import AsyncArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncArrowCursor).cursor(unload=True)
```

AsyncArrowCursor also supports S3 timeout configuration using the same `connect_timeout` and `request_timeout` parameters as ArrowCursor.

```python
from pyathena import connect
from pyathena.arrow.async_cursor import AsyncArrowCursor

cursor = connect(
    s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
    region_name="us-west-2"
).cursor(AsyncArrowCursor, connect_timeout=10.0, request_timeout=30.0)
```

(aio-arrow-cursor)=

## AioArrowCursor

AioArrowCursor is a native asyncio cursor that returns results as Apache Arrow Tables.
Unlike AsyncArrowCursor which uses `concurrent.futures`, this cursor uses
`asyncio.to_thread()` for both result set creation and fetch operations,
keeping the event loop free.

```python
from pyathena import aconnect
from pyathena.aio.arrow.cursor import AioArrowCursor

async with await aconnect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioArrowCursor)
    table = (await cursor.execute("SELECT * FROM many_rows")).as_arrow()
    print(table)
    print(table.column_names)
    print(table.num_rows)
    print(table.schema)
```

Support fetch and iterate query results:

```python
from pyathena import aconnect
from pyathena.aio.arrow.cursor import AioArrowCursor

async with await aconnect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioArrowCursor)
    await cursor.execute("SELECT * FROM many_rows")
    print(await cursor.fetchone())
    print(await cursor.fetchmany())
    print(await cursor.fetchall())
```

```python
from pyathena import aconnect
from pyathena.aio.arrow.cursor import AioArrowCursor

async with await aconnect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioArrowCursor)
    await cursor.execute("SELECT * FROM many_rows")
    async for row in cursor:
        print(row)
```

The `as_polars()` method converts the result to a Polars DataFrame:

```python
from pyathena import aconnect
from pyathena.aio.arrow.cursor import AioArrowCursor

async with await aconnect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioArrowCursor)
    await cursor.execute("SELECT * FROM many_rows")
    df = cursor.as_polars()
```

The unload option is also available:

```python
from pyathena import aconnect
from pyathena.aio.arrow.cursor import AioArrowCursor

async with await aconnect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioArrowCursor, unload=True)
    await cursor.execute("SELECT * FROM many_rows")
    table = cursor.as_arrow()
```

AioArrowCursor also supports S3 timeout configuration:

```python
from pyathena import aconnect
from pyathena.aio.arrow.cursor import AioArrowCursor

async with await aconnect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioArrowCursor, connect_timeout=10.0, request_timeout=30.0)
    await cursor.execute("SELECT * FROM many_rows")
```
