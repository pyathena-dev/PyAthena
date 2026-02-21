(pandas)=

# Pandas

(as-dataframe)=

## As DataFrame

You can use the [pandas.read_sql_query](https://pandas.pydata.org/docs/reference/api/pandas.read_sql_query.html) to handle the query results as a [pandas.DataFrame object](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html).

```python
from pyathena import connect
import pandas as pd

conn = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
               region_name="us-west-2")
df = pd.read_sql_query("SELECT * FROM many_rows", conn)
print(df.head())
```

NOTE: [Poor performance when using pandas.read_sql #222](https://github.com/pyathena-dev/PyAthena/issues/222)

The `pyathena.pandas.util` package also has helper methods.

```python
from pyathena import connect
from pyathena.pandas.util import as_pandas

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
cursor.execute("SELECT * FROM many_rows")
df = as_pandas(cursor)
print(df.describe())
```

If you want to use the query results output to S3 directly, you can use [PandasCursor](#pandas-cursor).
This cursor fetches query results faster than the default cursor. (See [benchmark results](https://github.com/pyathena-dev/PyAthena/tree/master/benchmarks).)

(to-sql)=

## To SQL

You can use [pandas.DataFrame.to_sql](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html) to write records stored in DataFrame to Amazon Athena.
[pandas.DataFrame.to_sql](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html) uses {ref}`sqlalchemy`, so you need to install it.

```python
import pandas as pd
from sqlalchemy import create_engine

conn_str = "awsathena+rest://:@athena.{region_name}.amazonaws.com:443/"\
           "{schema_name}?s3_staging_dir={s3_staging_dir}&location={location}&compression=snappy"
engine = create_engine(conn_str.format(
    region_name="us-west-2",
    schema_name="YOUR_SCHEMA",
    s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
    location="s3://YOUR_S3_BUCKET/path/to/"))

df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
df.to_sql("YOUR_TABLE", engine, schema="YOUR_SCHEMA", index=False, if_exists="replace", method="multi")
```

The location of the Amazon S3 table is specified by the `location` parameter in the connection string.
If `location` is not specified, `s3_staging_dir` parameter will be used. The following rules apply.

```text
s3://{location or s3_staging_dir}/{schema}/{table}/
```

The file format, row format, and compression settings are specified in the connection string.

The `pyathena.pandas.util` package also has helper methods.

```python
import pandas as pd
from pyathena import connect
from pyathena.pandas.util import to_sql

conn = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
               region_name="us-west-2")
df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
to_sql(df, "YOUR_TABLE", conn, "s3://YOUR_S3_BUCKET/path/to/",
       schema="YOUR_SCHEMA", index=False, if_exists="replace")
```

This helper method supports partitioning.

```python
import pandas as pd
from datetime import date
from pyathena import connect
from pyathena.pandas.util import to_sql

conn = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
               region_name="us-west-2")
df = pd.DataFrame({
    "a": [1, 2, 3, 4, 5],
    "dt": [
        date(2020, 1, 1), date(2020, 1, 1), date(2020, 1, 1),
        date(2020, 1, 2),
        date(2020, 1, 3)
    ],
})
to_sql(df, "YOUR_TABLE", conn, "s3://YOUR_S3_BUCKET/path/to/",
       schema="YOUR_SCHEMA", partitions=["dt"])

cursor = conn.cursor()
cursor.execute("SHOW PARTITIONS YOUR_TABLE")
print(cursor.fetchall())
```

Conversion to Parquet and upload to S3 use [ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor) by default.
It is also possible to use [ProcessPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#processpoolexecutor).

```python
import pandas as pd
from concurrent.futures.process import ProcessPoolExecutor
from pyathena import connect
from pyathena.pandas.util import to_sql

conn = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
               region_name="us-west-2")
df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
to_sql(df, "YOUR_TABLE", conn, "s3://YOUR_S3_BUCKET/path/to/",
       schema="YOUR_SCHEMA", index=False, if_exists="replace",
       chunksize=1, executor_class=ProcessPoolExecutor, max_workers=5)
```

(pandas-cursor)=

## PandasCursor

PandasCursor directly handles the CSV file of the query execution result output to S3.
This cursor is to download the CSV file after executing the query, and then loaded into [pandas.DataFrame object](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html).
Performance is better than fetching data with Cursor.

You can use the PandasCursor by specifying the `cursor_class`
with the connect method or connection object.

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor()
```

```python
from pyathena.connection import Connection
from pyathena.pandas.cursor import PandasCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2",
                    cursor_class=PandasCursor).cursor()
```

It can also be used by specifying the cursor class when calling the connection object's cursor method.

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor(PandasCursor)
```

```python
from pyathena.connection import Connection
from pyathena.pandas.cursor import PandasCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2").cursor(PandasCursor)
```

The as_pandas method returns a [pandas.DataFrame object](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html).

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor()

df = cursor.execute("SELECT * FROM many_rows").as_pandas()
print(df.describe())
print(df.head())
```

Support fetch and iterate query results.

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor()

cursor.execute("SELECT * FROM many_rows")
print(cursor.fetchone())
print(cursor.fetchmany())
print(cursor.fetchall())
```

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor()

cursor.execute("SELECT * FROM many_rows")
for row in cursor:
    print(row)
```

The DATE and TIMESTAMP of Athena's data type are returned as [pandas.Timestamp](https://pandas.pydata.org/docs/reference/api/pandas.Timestamp.html) type.

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor()

cursor.execute("SELECT col_timestamp FROM one_row_complex")
print(type(cursor.fetchone()[0]))  # <class 'pandas._libs.tslibs.timestamps.Timestamp'>
```

Execution information of the query can also be retrieved.

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor()

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

If you want to customize the pandas.Dataframe object dtypes and converters, create a converter class like this:

```python
from pyathena.converter import Converter

class CustomPandasTypeConverter(Converter):

    def __init__(self):
        super().__init__(
            mappings=None,
            types={
                "boolean": object,
                "tinyint": float,
                "smallint": float,
                "integer": float,
                "bigint": float,
                "float": float,
                "real": float,
                "double": float,
                "decimal": float,
                "char": str,
                "varchar": str,
                "array": str,
                "map": str,
                "row": str,
                "varbinary": str,
                "json": str,
            }
        )

    def convert(self, type_, value):
        # Not used in PandasCursor.
        pass
```

Specify the combination of converter functions in the mappings argument and the dtypes combination in the types argument.

Then you simply specify an instance of this class in the convertes argument when creating a connection or cursor.

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor(PandasCursor, converter=CustomPandasTypeConverter())
```

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 converter=CustomPandasTypeConverter()).cursor(PandasCursor)
```

If the unload option is enabled, the Parquet file itself has a schema, so the conversion is done to the dtypes according to that schema,
and the `mappings` and `types` settings of the Converter class are not used.

If you want to change the NaN behavior of pandas.Dataframe,
you can do so by using the `keep_default_na`, `na_values` and `quoting` arguments of the cursor object's execute method.

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor()
df = cursor.execute("SELECT * FROM many_rows",
                    keep_default_na=False,
                    na_values=[""]).as_pandas()
```

NOTE: PandasCursor handles the CSV file on memory. Pay attention to the memory capacity.

### Chunksize options

The Pandas cursor can read the CSV file for each specified number of rows by using the chunksize option.
This option should reduce memory usage.

The chunksize option can be enabled by specifying an integer value in the `cursor_kwargs` argument of the connect method or as an argument to the cursor method.

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor,
                 cursor_kwargs={
                     "chunksize": 1_000_000
                 }).cursor()
```

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor(chunksize=1_000_000)
```

It can also be specified in the execution method when executing the query.

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor()
cursor.execute("SELECT * FROM many_rows", chunksize=1_000_000)
```

SQLAlchemy allows this option to be specified in the connection string.

```text
awsathena+pandas://:@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}&chunksize=1000000...
```

When this option is used, the object returned by the as_pandas method is a `PandasDataFrameIterator` object.
This object has exactly the same interface as the `TextFileReader` object and can be handled in the same way.

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor()
df_iter = cursor.execute("SELECT * FROM many_rows", chunksize=1_000_000).as_pandas()
for df in df_iter:
    print(df.describe())
    print(df.head())
```

**Memory-efficient iteration with iter_chunks()**

PandasCursor provides an `iter_chunks()` method for convenient chunked processing:

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor()

# Process large dataset in chunks
cursor.execute("SELECT * FROM large_table", chunksize=50_000)
for chunk in cursor.iter_chunks():
    # Process each chunk
    processed = chunk.groupby('category').sum()
    # Memory can be freed after each chunk
    del chunk
```

The `PandasDataFrameIterator` also has an `as_pandas()` method that collects all chunks into a single DataFrame:

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor()
df_iter = cursor.execute("SELECT * FROM many_rows", chunksize=1_000_000).as_pandas()
df = df_iter.as_pandas()  # Collect all chunks into a single DataFrame
```

This is equivalent to using [pandas.concat](https://pandas.pydata.org/docs/reference/api/pandas.concat.html):

```python
import pandas
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor()
df_iter = cursor.execute("SELECT * FROM many_rows", chunksize=1_000_000).as_pandas()
df = pandas.concat((df for df in df_iter), ignore_index=True)
```

You can use the `get_chunk` method to retrieve a [pandas.DataFrame object](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) for each specified number of rows.
When all rows have been read, calling the `get_chunk` method will raise `StopIteration`.

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor()
df_iter = cursor.execute("SELECT * FROM many_rows LIMIT 15", chunksize=1_000_000).as_pandas()
df_iter.get_chunk(10)
df_iter.get_chunk(10)
df_iter.get_chunk(10)  # raise StopIteration
```

**Auto-optimization of chunksize**

PandasCursor can automatically determine optimal chunksize based on result file size when enabled:

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

# Enable auto-optimization (chunksize will be determined automatically for large files)
cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor(auto_optimize_chunksize=True)

# For large files, chunksize will be automatically set based on file size
cursor.execute("SELECT * FROM very_large_table")
for chunk in cursor.iter_chunks():
    process_chunk(chunk)
```

**Priority of chunksize settings:**

1. **Explicit chunksize** (highest priority): Always respected
2. **auto_optimize_chunksize=True**: Automatic determination for large files
3. **auto_optimize_chunksize=False** (default): No chunking, load entire DataFrame

```python
# Explicit chunksize always takes precedence
cursor = connection.cursor(PandasCursor, chunksize=50_000, auto_optimize_chunksize=True)
# Will use chunksize=50_000, auto-optimization is ignored

# Auto-optimization only when chunksize is not specified
cursor = connection.cursor(PandasCursor, auto_optimize_chunksize=True)
# Will determine chunksize automatically for large files

# Default behavior - no chunking
cursor = connection.cursor(PandasCursor)
# Will load entire DataFrame regardless of file size
```

You can customize the automatic chunksize determination by modifying class attributes:

```python
from pyathena.pandas.result_set import AthenaPandasResultSet

# Customize thresholds and chunk sizes for your use case
AthenaPandasResultSet.LARGE_FILE_THRESHOLD_BYTES = 100 * 1024 * 1024  # 100MB
AthenaPandasResultSet.AUTO_CHUNK_SIZE_LARGE = 200_000  # Larger chunks
AthenaPandasResultSet.AUTO_CHUNK_SIZE_MEDIUM = 100_000
```

**Performance tuning options**

PandasCursor accepts additional pandas.read_csv() options for performance optimization:

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor()

# High-performance reading with PyArrow engine
cursor.execute("SELECT * FROM large_table",
               engine="pyarrow",
               chunksize=100_000,
               use_threads=True)

# Memory-conscious reading with Python engine
cursor.execute("SELECT * FROM huge_table",
               engine="python",
               chunksize=25_000,
               low_memory=True)

# Fine-tuned C engine with custom buffer
cursor.execute("SELECT * FROM data_table",
               engine="c",
               chunksize=50_000,
               buffer_lines=100_000)

# Custom data types for better performance
cursor.execute("SELECT * FROM typed_table",
               dtype={'col1': 'int64', 'col2': 'float32'},
               parse_dates=['timestamp_col'])
```

Common performance options:

- `engine`: CSV parsing engine ('c', 'python', 'pyarrow')
- `use_threads`: Enable threading for PyArrow engine
- `low_memory`: Use low memory mode for Python engine
- `buffer_lines`: Buffer size for C engine
- `dtype`: Explicit column data types
- `parse_dates`: Columns to parse as dates

### Unload options

PandasCursor also supports the unload option, as does {ref}`arrow-cursor`.

See {ref}`arrow-unload-options` for more information.

The unload option can be enabled by specifying it in the `cursor_kwargs` argument of the connect method or as an argument to the cursor method.

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor,
                 cursor_kwargs={
                     "unload": True
                 }).cursor()
```

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor(unload=True)
```

SQLAlchemy allows this option to be specified in the connection string.

```text
awsathena+pandas://:@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}&unload=true...
```

(async-pandas-cursor)=

## AsyncPandasCursor

AsyncPandasCursor is an AsyncCursor that can handle [pandas.DataFrame object](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html).
This cursor directly handles the CSV of query results output to S3 in the same way as PandasCursor.

You can use the AsyncPandasCursor by specifying the `cursor_class`
with the connect method or connection object.

```python
from pyathena import connect
from pyathena.pandas.async_cursor import AsyncPandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPandasCursor).cursor()
```

```python
from pyathena.connection import Connection
from pyathena.pandas.async_cursor import AsyncPandasCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2",
                    cursor_class=AsyncPandasCursor).cursor()
```

It can also be used by specifying the cursor class when calling the connection object's cursor method.

```python
from pyathena import connect
from pyathena.pandas.async_cursor import AsyncPandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor(AsyncPandasCursor)
```

```python
from pyathena.connection import Connection
from pyathena.pandas.async_cursor import AsyncPandasCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2").cursor(AsyncPandasCursor)
```

The default number of workers is 5 or cpu number * 5.
If you want to change the number of workers you can specify like the following.

```python
from pyathena import connect
from pyathena.pandas.async_cursor import AsyncPandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPandasCursor).cursor(max_workers=10)
```

The execute method of the AsyncPandasCursor returns the tuple of the query ID and the [future object](https://docs.python.org/3/library/concurrent.futures.html#future-objects).

```python
from pyathena import connect
from pyathena.pandas.async_cursor import AsyncPandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPandasCursor).cursor()

query_id, future = cursor.execute("SELECT * FROM many_rows")
```

The return value of the [future object](https://docs.python.org/3/library/concurrent.futures.html#future-objects) is an `AthenaPandasResultSet` object.
This object has an interface similar to `AthenaResultSetObject`.

```python
from pyathena import connect
from pyathena.pandas.async_cursor import AsyncPandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPandasCursor).cursor()

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
from pyathena.pandas.async_cursor import AsyncPandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPandasCursor).cursor()

query_id, future = cursor.execute("SELECT * FROM many_rows")
result_set = future.result()
print(result_set.fetchall())
```

This object also has an as_pandas method that returns a [pandas.DataFrame object](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) similar to the PandasCursor.

```python
from pyathena import connect
from pyathena.pandas.async_cursor import AsyncPandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPandasCursor).cursor()

query_id, future = cursor.execute("SELECT * FROM many_rows")
result_set = future.result()
df = result_set.as_pandas()
print(df.describe())
print(df.head())
```

The DATE and TIMESTAMP of Athena's data type are returned as [pandas.Timestamp](https://pandas.pydata.org/docs/reference/api/pandas.Timestamp.html) type.

```python
from pyathena import connect
from pyathena.pandas.async_cursor import AsyncPandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPandasCursor).cursor()

query_id, future = cursor.execute("SELECT col_timestamp FROM one_row_complex")
result_set = future.result()
print(type(result_set.fetchone()[0]))  # <class 'pandas._libs.tslibs.timestamps.Timestamp'>
```

As with AsyncPandasCursor, you need a query ID to cancel a query.

```python
from pyathena import connect
from pyathena.pandas.async_cursor import AsyncPandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPandasCursor).cursor()

query_id, future = cursor.execute("SELECT * FROM many_rows")
cursor.cancel(query_id)
```

As with AsyncPandasCursor, the unload option is also available.

```python
from pyathena import connect
from pyathena.pandas.async_cursor import AsyncPandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPandasCursor,
                 cursor_kwargs={
                     "unload": True
                 }).cursor()
```

```python
from pyathena import connect
from pyathena.pandas.cursor import AsyncPandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncPandasCursor).cursor(unload=True)
```

(aio-pandas-cursor)=

## AioPandasCursor

AioPandasCursor is a native asyncio cursor that returns results as pandas DataFrames.
Unlike AsyncPandasCursor which uses `concurrent.futures`, this cursor uses
`asyncio.to_thread()` for result set creation, keeping the event loop free.

The S3 download (CSV or Parquet) happens inside `execute()`, wrapped in `asyncio.to_thread()`.
By the time `execute()` returns, all data is already loaded into memory.
Therefore fetch methods and `as_pandas()` are synchronous and do not need `await`.

```python
from pyathena import aconnect
from pyathena.aio.pandas.cursor import AioPandasCursor

async with await aconnect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioPandasCursor)
    await cursor.execute("SELECT * FROM many_rows")
    df = cursor.as_pandas()
    print(df.describe())
    print(df.head())
```

Support fetch and iterate query results:

```python
from pyathena import aconnect
from pyathena.aio.pandas.cursor import AioPandasCursor

async with await aconnect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioPandasCursor)
    await cursor.execute("SELECT * FROM many_rows")
    print(cursor.fetchone())
    print(cursor.fetchmany())
    print(cursor.fetchall())
```

```python
from pyathena import aconnect
from pyathena.aio.pandas.cursor import AioPandasCursor

async with await aconnect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioPandasCursor)
    await cursor.execute("SELECT * FROM many_rows")
    async for row in cursor:
        print(row)
```

The unload option is also available:

```python
from pyathena import aconnect
from pyathena.aio.pandas.cursor import AioPandasCursor

async with await aconnect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioPandasCursor, unload=True)
    await cursor.execute("SELECT * FROM many_rows")
    df = cursor.as_pandas()
```

```{note}
When using AioPandasCursor with the `chunksize` option, `execute()` creates a lazy
`TextFileReader` instead of loading all data at once. Subsequent iteration via
`as_pandas()`, `fetchone()`, or `async for` triggers chunk-by-chunk S3 reads that
are not wrapped in `asyncio.to_thread()` and will block the event loop.
```
