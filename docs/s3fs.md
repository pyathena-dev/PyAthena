(s3fs)=

# S3FS

(s3fs-cursor)=

## S3FSCursor

S3FSCursor is a lightweight cursor that directly handles the CSV file of the query execution result output to S3.
Unlike ArrowCursor or PandasCursor, this cursor does not require pandas or pyarrow dependencies,
making it ideal for environments where installing these libraries is not desirable.

**Key features:**

- No pandas or pyarrow dependencies required
- Lightweight CSV parsing (custom parser or Python's built-in `csv` module)
- Lower memory footprint for simple query results
- Full DB API 2.0 compatibility

You can use the S3FSCursor by specifying the `cursor_class`
with the connect method or connection object.

```python
from pyathena import connect
from pyathena.s3fs.cursor import S3FSCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=S3FSCursor).cursor()
```

```python
from pyathena.connection import Connection
from pyathena.s3fs.cursor import S3FSCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2",
                    cursor_class=S3FSCursor).cursor()
```

It can also be used by specifying the cursor class when calling the connection object's cursor method.

```python
from pyathena import connect
from pyathena.s3fs.cursor import S3FSCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor(S3FSCursor)
```

```python
from pyathena.connection import Connection
from pyathena.s3fs.cursor import S3FSCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2").cursor(S3FSCursor)
```

Support fetch and iterate query results.

```python
from pyathena import connect
from pyathena.s3fs.cursor import S3FSCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=S3FSCursor).cursor()

cursor.execute("SELECT * FROM many_rows")
print(cursor.fetchone())
print(cursor.fetchmany())
print(cursor.fetchall())
```

```python
from pyathena import connect
from pyathena.s3fs.cursor import S3FSCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=S3FSCursor).cursor()

cursor.execute("SELECT * FROM many_rows")
for row in cursor:
    print(row)
```

Execution information of the query can also be retrieved.

```python
from pyathena import connect
from pyathena.s3fs.cursor import S3FSCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=S3FSCursor).cursor()

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

### Type conversion

S3FSCursor converts Athena data types to Python types using the built-in converter.
The following type mappings are used:

| Athena Type | Python Type |
| --- | --- |
| boolean | bool |
| tinyint, smallint, integer, bigint | int |
| float, double, real | float |
| decimal | decimal.Decimal |
| char, varchar, string | str |
| date | datetime.date |
| timestamp | datetime.datetime |
| timestamp with time zone | datetime.datetime (timezone-aware) |
| time | datetime.time |
| time with time zone | datetime.time (timezone-aware) |
| varbinary | bytes |
| array, map, row (struct) | Parsed into Python list/dict (see {ref}`usage-type-hints` for the types of nested values); values too complex to parse are returned as the original string |
| json | Parsed JSON value (dict, list, or scalar) |

If you want to customize type conversion, create a converter class like this:

```python
from typing import Any

from pyathena.s3fs.converter import DefaultS3FSTypeConverter

class CustomS3FSTypeConverter(DefaultS3FSTypeConverter):
    def __init__(self) -> None:
        super().__init__()
        # Override specific type mappings
        self._mappings["custom_type"] = self._convert_custom

    def _convert_custom(self, value: str) -> Any:
        # Your custom conversion logic
        return value.upper()
```

Then specify an instance of this class in the converter argument when creating a cursor.

```python
from pyathena import connect
from pyathena.s3fs.cursor import S3FSCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor(S3FSCursor, converter=CustomS3FSTypeConverter())
```

### CSV reader options

S3FSCursor supports pluggable CSV reader implementations to control how NULL values and empty strings
are handled. Two readers are provided:

- `AthenaCSVReader` (default): Custom parser that distinguishes between NULL and empty string
- `EmptyStringAsNullCSVReader`: Uses Python's built-in `csv` module; both NULL and empty string become `None` in cursor results

**Default behavior (AthenaCSVReader):**

By default, `AthenaCSVReader` is used, which correctly distinguishes between NULL
values and empty strings in query results.

```python
from pyathena import connect
from pyathena.s3fs.cursor import S3FSCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=S3FSCursor).cursor()

cursor.execute("SELECT NULL AS null_col, '' AS empty_col")
row = cursor.fetchone()
print(row)  # (None, '')  - NULL is None, empty string is ''
```

**Treating empty strings as NULL:**

Use `EmptyStringAsNullCSVReader` when empty strings should be treated as NULL.
It parses result files with Python's `csv` module and returns `None` for both NULL and quoted empty fields.

```python
from pyathena import connect
from pyathena.s3fs.cursor import S3FSCursor
from pyathena.s3fs.reader import EmptyStringAsNullCSVReader

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=S3FSCursor,
                 cursor_kwargs={"csv_reader": EmptyStringAsNullCSVReader}).cursor()

cursor.execute("SELECT NULL AS null_col, '' AS empty_col")
row = cursor.fetchone()
print(row)  # (None, None)  - Both NULL and empty string become None
```

**Comparison of CSV readers:**

| Reader | Implementation | NULL value | Empty string |
| --- | --- | --- | --- |
| AthenaCSVReader (default) | Custom parser | None | '' (empty string) |
| EmptyStringAsNullCSVReader | Python csv module | None | None |

**Why the difference?**

Athena's CSV output format distinguishes between NULL values and empty strings:

- NULL: unquoted empty field (e.g., `a,,b` -> the middle field is NULL)
- Empty string: quoted empty field (e.g., `a,"",b` -> the middle field is an empty string)

Python's standard `csv` module parses both cases as empty strings, losing this distinction.
The `AthenaCSVReader` implements a custom parser that preserves the difference.

### CSV reader rename in PyAthena 4.0

PyAthena 4.0 renames `DefaultCSVReader` to `EmptyStringAsNullCSVReader` and removes the old name.
Update imports and `csv_reader` arguments to use the new name:

```python
from pyathena.s3fs.reader import EmptyStringAsNullCSVReader

cursor = connection.cursor(S3FSCursor, csv_reader=EmptyStringAsNullCSVReader)
```

`AthenaCSVReader` remains the default.
Iterating over `EmptyStringAsNullCSVReader` directly now returns `None` for empty fields instead of empty strings.
Cursor results are unchanged.

### Custom CSV readers

Custom reader classes must satisfy the `pyathena.s3fs.reader.CSVReader` protocol; inheriting from it is optional.
Pass the class through `csv_reader` in the cursor constructor or an individual `execute()` call.

- Accept a text stream as `file_obj` and a `delimiter` keyword argument in the constructor.
- Implement `__iter__()` and `__next__()` to yield sequences of strings or `None`, and raise `StopIteration` at the end of the stream.
  The cursor converts each value by its column type and treats `None` as NULL.
- Implement `close()` to close the underlying stream.

The reader is used only for result files; managed query results are read through the Athena API.

### Limitations

S3FSCursor has some limitations compared to ArrowCursor or PandasCursor:

- **No UNLOAD support**: S3FSCursor reads CSV results directly and does not support the UNLOAD option
  that outputs results in Parquet format.
- **Sequential reading**: Results are read row by row from the CSV file, which may be slower
  for very large result sets compared to columnar formats.
- **No DataFrame conversion**: There is no `as_pandas()` or `as_arrow()` method.
  Use PandasCursor or ArrowCursor if you need DataFrame operations.

### When to use S3FSCursor

S3FSCursor is recommended when:

- You want to minimize dependencies (no pandas/pyarrow required)
- You're working in a constrained environment (e.g., AWS Lambda with size limits)
- You only need simple row-by-row result processing
- Memory efficiency is important and results don't need columnar operations

For large-scale data processing or analytical workloads, consider using ArrowCursor or PandasCursor instead.

(async-s3fs-cursor)=

## AsyncS3FSCursor

AsyncS3FSCursor is an AsyncCursor that uses the same lightweight CSV parsing as S3FSCursor.
This cursor is useful when you need to execute queries asynchronously without pandas or pyarrow dependencies.

You can use the AsyncS3FSCursor by specifying the `cursor_class`
with the connect method or connection object.

```python
from pyathena import connect
from pyathena.s3fs.async_cursor import AsyncS3FSCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncS3FSCursor).cursor()
```

```python
from pyathena.connection import Connection
from pyathena.s3fs.async_cursor import AsyncS3FSCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2",
                    cursor_class=AsyncS3FSCursor).cursor()
```

It can also be used by specifying the cursor class when calling the connection object's cursor method.

```python
from pyathena import connect
from pyathena.s3fs.async_cursor import AsyncS3FSCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor(AsyncS3FSCursor)
```

```python
from pyathena.connection import Connection
from pyathena.s3fs.async_cursor import AsyncS3FSCursor

cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                    region_name="us-west-2").cursor(AsyncS3FSCursor)
```

The default number of workers is 5 or cpu number * 5.
If you want to change the number of workers you can specify like the following.

```python
from pyathena import connect
from pyathena.s3fs.async_cursor import AsyncS3FSCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncS3FSCursor).cursor(max_workers=10)
```

The execute method of the AsyncS3FSCursor returns the tuple of the query ID and the [future object](https://docs.python.org/3/library/concurrent.futures.html#future-objects).

```python
from pyathena import connect
from pyathena.s3fs.async_cursor import AsyncS3FSCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncS3FSCursor).cursor()

query_id, future = cursor.execute("SELECT * FROM many_rows")
```

The return value of the [future object](https://docs.python.org/3/library/concurrent.futures.html#future-objects) is an `AthenaS3FSResultSet` object.
This object has an interface similar to `AthenaResultSet`.

```python
from pyathena import connect
from pyathena.s3fs.async_cursor import AsyncS3FSCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncS3FSCursor).cursor()

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
from pyathena.s3fs.async_cursor import AsyncS3FSCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncS3FSCursor).cursor()

query_id, future = cursor.execute("SELECT * FROM many_rows")
result_set = future.result()
print(result_set.fetchall())
```

As with AsyncCursor, you need a query ID to cancel a query.

```python
from pyathena import connect
from pyathena.s3fs.async_cursor import AsyncS3FSCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=AsyncS3FSCursor).cursor()

query_id, future = cursor.execute("SELECT * FROM many_rows")
cursor.cancel(query_id)
```

(aio-s3fs-cursor)=

## AioS3FSCursor

AioS3FSCursor is a native asyncio cursor that uses the same lightweight CSV parsing as S3FSCursor.
Unlike AsyncS3FSCursor which uses `concurrent.futures`, this cursor uses
`asyncio.to_thread()` for both result set creation and fetch operations,
keeping the event loop free.

Since `AthenaS3FSResultSet` lazily streams rows from S3 via a CSV reader,
fetch methods are async and require `await`.

```python
from pyathena import aio_connect
from pyathena.aio.s3fs.cursor import AioS3FSCursor

async with await aio_connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioS3FSCursor)
    await cursor.execute("SELECT * FROM many_rows")
    print(await cursor.fetchone())
    print(await cursor.fetchmany(10))
    print(await cursor.fetchall())
```

Async iteration is supported:

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

Execution information of the query can also be retrieved:

```python
from pyathena import aio_connect
from pyathena.aio.s3fs.cursor import AioS3FSCursor

async with await aio_connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                          region_name="us-west-2") as conn:
    cursor = conn.cursor(AioS3FSCursor)
    await cursor.execute("SELECT * FROM many_rows")
    print(cursor.state)
    print(cursor.data_scanned_in_bytes)
    print(cursor.output_location)
```
