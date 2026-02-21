(polars)=

# Polars

(polars-cursor)=

## PolarsCursor

PolarsCursor directly handles the CSV file of the query execution result output to S3.
This cursor downloads the CSV file after executing the query and loads it into a [polars.DataFrame object](https://docs.pola.rs/api/python/stable/reference/dataframe/index.html).
Performance is better than fetching data with Cursor.

PolarsCursor uses [Polars](https://pola.rs/) native reading capabilities (`pl.read_csv`, `pl.read_parquet`) and
does not require PyArrow as a dependency. PyAthena's own S3FileSystem (fsspec compatible)
is used for S3 access, so s3fs is also not required.

You can use the PolarsCursor by specifying the `cursor_class`
with the connect method or connection object.

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PolarsCursor).cursor()
```

```python
from pyathena.connection import Connection
from pyathena.polars.cursor import PolarsCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2",
                    cursor_class=PolarsCursor).cursor()
```

It can also be used by specifying the cursor class when calling the connection object's cursor method.

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor(PolarsCursor)
```

```python
from pyathena.connection import Connection
from pyathena.polars.cursor import PolarsCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2").cursor(PolarsCursor)
```

The as_polars method returns a [polars.DataFrame object](https://docs.pola.rs/api/python/stable/reference/dataframe/index.html).

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PolarsCursor).cursor()

df = cursor.execute("SELECT * FROM many_rows").as_polars()
print(df.describe())
print(df.head())
print(df.height)  # Number of rows
print(df.width)   # Number of columns
print(df.columns) # Column names
```

Support fetch and iterate query results.

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PolarsCursor).cursor()

cursor.execute("SELECT * FROM many_rows")
print(cursor.fetchone())
print(cursor.fetchmany())
print(cursor.fetchall())
```

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PolarsCursor).cursor()

cursor.execute("SELECT * FROM many_rows")
for row in cursor:
    print(row)
```

Execution information of the query can also be retrieved.

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PolarsCursor).cursor()

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

### Arrow Interoperability

PolarsCursor can convert results to Apache Arrow format if PyArrow is installed.

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PolarsCursor).cursor()

# Convert to Arrow Table (requires pyarrow)
table = cursor.execute("SELECT * FROM many_rows").as_arrow()
print(table.num_rows)
print(table.num_columns)
print(table.schema)
```

If you want to customize the polars.DataFrame dtypes, create a converter class like this:

```python
import polars as pl
from pyathena.converter import Converter

class CustomPolarsTypeConverter(Converter):

    def __init__(self):
        super().__init__(
            mappings=None,
            types={
                "boolean": pl.Boolean,
                "tinyint": pl.Int8,
                "smallint": pl.Int16,
                "integer": pl.Int32,
                "bigint": pl.Int64,
                "float": pl.Float32,
                "real": pl.Float64,
                "double": pl.Float64,
                "decimal": pl.String,
                "char": pl.String,
                "varchar": pl.String,
                "string": pl.String,
                "timestamp": pl.Datetime,
                "date": pl.Date,
                "time": pl.Time,
                "varbinary": pl.String,
                "array": pl.String,
                "map": pl.String,
                "row": pl.String,
                "json": pl.String,
            }
        )

    def convert(self, type_, value):
        # Not used in PolarsCursor.
        pass
```

Then you simply specify an instance of this class in the converter argument when creating a connection or cursor.

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor(PolarsCursor, converter=CustomPolarsTypeConverter())
```

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 converter=CustomPolarsTypeConverter()).cursor(PolarsCursor)
```

If the unload option is enabled, the Parquet file itself has a schema, so the conversion is done to the dtypes according to that schema,
and the `types` setting of the Converter class is not used.

### Unload Options

PolarsCursor supports the unload option, as does {ref}`arrow-cursor`.

See {ref}`arrow-unload-options` for more information.

The unload option can be enabled by specifying it in the `cursor_kwargs` argument of the connect method or as an argument to the cursor method.

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PolarsCursor,
                 cursor_kwargs={
                     "unload": True
                 }).cursor()
```

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PolarsCursor).cursor(unload=True)
```

SQLAlchemy allows this option to be specified in the connection string.

```text
awsathena+polars://:@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}&unload=true...
```

NOTE: PolarsCursor handles the CSV file on memory. Pay attention to the memory capacity.

### Chunksize Options

PolarsCursor supports memory-efficient chunked processing of large query results
using Polars' native lazy evaluation APIs. This allows processing datasets that
are too large to fit in memory.

The chunksize option can be enabled by specifying an integer value in the `cursor_kwargs`
argument of the connect method or as an argument to the cursor method.

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PolarsCursor,
                 cursor_kwargs={
                     "chunksize": 50_000
                 }).cursor()
```

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PolarsCursor).cursor(chunksize=50_000)
```

When the chunksize option is enabled, data is loaded lazily in chunks. This applies
to all data access methods:

**Standard DB-API fetch methods** - `fetchone()` and `fetchmany()` load data
chunk by chunk as needed, keeping memory usage bounded:

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PolarsCursor).cursor(chunksize=50_000)

cursor.execute("SELECT * FROM large_table")
# Data is loaded in 50,000 row chunks as you iterate
for row in cursor:
    process_row(row)
```

**iter_chunks() method** - Use this when you want to process data as Polars DataFrames
in chunks, which is more efficient for batch processing:

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PolarsCursor).cursor(chunksize=50_000)

cursor.execute("SELECT * FROM large_table")
for chunk in cursor.iter_chunks():
    # Process each chunk - chunk is a polars.DataFrame
    processed = chunk.group_by('category').agg(pl.sum('value'))
    print(f"Processed chunk with {chunk.height} rows")
```

This method uses Polars' `scan_csv()` and `scan_parquet()` with `collect_batches()`
for efficient lazy evaluation, minimizing memory usage when processing large datasets.

The chunked iteration also works with the unload option:

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PolarsCursor).cursor(chunksize=100_000, unload=True)

cursor.execute("SELECT * FROM huge_table")
for chunk in cursor.iter_chunks():
    # Process Parquet data in chunks
    process_chunk(chunk)
```

When the chunksize option is used, the object returned by the `as_polars` method is a `PolarsDataFrameIterator` object.
This object provides the same chunked iteration interface and can be used in the same way:

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PolarsCursor).cursor(chunksize=50_000)
df_iter = cursor.execute("SELECT * FROM many_rows").as_polars()
for df in df_iter:
    print(df.describe())
    print(df.head())
```

The `PolarsDataFrameIterator` also has an `as_polars()` method that collects all chunks into a single DataFrame:

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PolarsCursor).cursor(chunksize=50_000)
df_iter = cursor.execute("SELECT * FROM many_rows").as_polars()
df = df_iter.as_polars()  # Collect all chunks into a single DataFrame
```

This is equivalent to using [polars.concat](https://docs.pola.rs/api/python/stable/reference/api/polars.concat.html):

```python
import polars as pl
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PolarsCursor).cursor(chunksize=50_000)
df_iter = cursor.execute("SELECT * FROM many_rows").as_polars()
df = pl.concat(list(df_iter))
```

(async-polars-cursor)=

## AsyncPolarsCursor

AsyncPolarsCursor is an AsyncCursor that can handle [polars.DataFrame object](https://docs.pola.rs/api/python/stable/reference/dataframe/index.html).
This cursor directly handles the CSV of query results output to S3 in the same way as PolarsCursor.

You can use the AsyncPolarsCursor by specifying the `cursor_class`
with the connect method or connection object.

```python
from pyathena import connect
from pyathena.polars.async_cursor import AsyncPolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPolarsCursor).cursor()
```

```python
from pyathena.connection import Connection
from pyathena.polars.async_cursor import AsyncPolarsCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2",
                    cursor_class=AsyncPolarsCursor).cursor()
```

It can also be used by specifying the cursor class when calling the connection object's cursor method.

```python
from pyathena import connect
from pyathena.polars.async_cursor import AsyncPolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor(AsyncPolarsCursor)
```

```python
from pyathena.connection import Connection
from pyathena.polars.async_cursor import AsyncPolarsCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2").cursor(AsyncPolarsCursor)
```

The default number of workers is 5 or cpu number * 5.
If you want to change the number of workers you can specify like the following.

```python
from pyathena import connect
from pyathena.polars.async_cursor import AsyncPolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPolarsCursor).cursor(max_workers=10)
```

The execute method of the AsyncPolarsCursor returns the tuple of the query ID and the [future object](https://docs.python.org/3/library/concurrent.futures.html#future-objects).

```python
from pyathena import connect
from pyathena.polars.async_cursor import AsyncPolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPolarsCursor).cursor()

query_id, future = cursor.execute("SELECT * FROM many_rows")
```

The return value of the [future object](https://docs.python.org/3/library/concurrent.futures.html#future-objects) is an `AthenaPolarsResultSet` object.
This object has an interface similar to `AthenaResultSetObject`.

```python
from pyathena import connect
from pyathena.polars.async_cursor import AsyncPolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPolarsCursor).cursor()

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
from pyathena.polars.async_cursor import AsyncPolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPolarsCursor).cursor()

query_id, future = cursor.execute("SELECT * FROM many_rows")
result_set = future.result()
print(result_set.fetchall())
```

This object also has an as_polars method that returns a [polars.DataFrame object](https://docs.pola.rs/api/python/stable/reference/dataframe/index.html) similar to the PolarsCursor.

```python
from pyathena import connect
from pyathena.polars.async_cursor import AsyncPolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPolarsCursor).cursor()

query_id, future = cursor.execute("SELECT * FROM many_rows")
result_set = future.result()
df = result_set.as_polars()
print(df.describe())
print(df.head())
```

As with AsyncPolarsCursor, you need a query ID to cancel a query.

```python
from pyathena import connect
from pyathena.polars.async_cursor import AsyncPolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPolarsCursor).cursor()

query_id, future = cursor.execute("SELECT * FROM many_rows")
cursor.cancel(query_id)
```

As with AsyncPolarsCursor, the unload option is also available.

```python
from pyathena import connect
from pyathena.polars.async_cursor import AsyncPolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPolarsCursor,
                 cursor_kwargs={
                     "unload": True
                 }).cursor()
```

```python
from pyathena import connect
from pyathena.polars.async_cursor import AsyncPolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPolarsCursor).cursor(unload=True)
```

As with PolarsCursor, the chunksize option is also available for memory-efficient processing.
When chunksize is specified, data is loaded lazily in chunks for both standard fetch methods
and `iter_chunks()`.

```python
from pyathena import connect
from pyathena.polars.async_cursor import AsyncPolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPolarsCursor).cursor(chunksize=50_000)

query_id, future = cursor.execute("SELECT * FROM large_table")
result_set = future.result()

# Standard iteration - data loaded in chunks
for row in result_set:
    process_row(row)
```

```python
from pyathena import connect
from pyathena.polars.async_cursor import AsyncPolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPolarsCursor).cursor(chunksize=50_000)

query_id, future = cursor.execute("SELECT * FROM large_table")
result_set = future.result()

# Process as DataFrame chunks
for chunk in result_set.iter_chunks():
    process_chunk(chunk)
```

(aio-polars-cursor)=

## AioPolarsCursor

AioPolarsCursor is a native asyncio cursor that returns results as Polars DataFrames.
Unlike AsyncPolarsCursor which uses `concurrent.futures`, this cursor uses
`asyncio.to_thread()` for both result set creation and fetch operations,
keeping the event loop free.

```python
from pyathena import aconnect
from pyathena.aio.polars.cursor import AioPolarsCursor

async with await aconnect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioPolarsCursor)
    await cursor.execute("SELECT * FROM many_rows")
    df = cursor.as_polars()
    print(df.describe())
    print(df.head())
```

Support fetch and iterate query results:

```python
from pyathena import aconnect
from pyathena.aio.polars.cursor import AioPolarsCursor

async with await aconnect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioPolarsCursor)
    await cursor.execute("SELECT * FROM many_rows")
    print(await cursor.fetchone())
    print(await cursor.fetchmany())
    print(await cursor.fetchall())
```

```python
from pyathena import aconnect
from pyathena.aio.polars.cursor import AioPolarsCursor

async with await aconnect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioPolarsCursor)
    await cursor.execute("SELECT * FROM many_rows")
    async for row in cursor:
        print(row)
```

The `as_arrow()` method converts the result to an Apache Arrow Table:

```python
from pyathena import aconnect
from pyathena.aio.polars.cursor import AioPolarsCursor

async with await aconnect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioPolarsCursor)
    await cursor.execute("SELECT * FROM many_rows")
    table = cursor.as_arrow()
```

The unload option is also available:

```python
from pyathena import aconnect
from pyathena.aio.polars.cursor import AioPolarsCursor

async with await aconnect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioPolarsCursor, unload=True)
    await cursor.execute("SELECT * FROM many_rows")
    df = cursor.as_polars()
```

