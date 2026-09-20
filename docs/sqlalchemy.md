(sqlalchemy)=

# SQLAlchemy

Install SQLAlchemy with `pip install "SQLAlchemy>=1.0.0"` or `pip install PyAthena[sqlalchemy]`.
Supported SQLAlchemy is 1.0.0 or higher.

For async support (`create_async_engine`), install with `pip install PyAthena[aiosqlalchemy]`
(requires SQLAlchemy 2.0+).

## Basic usage

### Sync

```python
from sqlalchemy import func, select
from sqlalchemy.engine import create_engine
from sqlalchemy.sql.schema import Table, MetaData

conn_str = "awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/"\
           "{schema_name}?s3_staging_dir={s3_staging_dir}"
engine = create_engine(conn_str.format(
    aws_access_key_id="YOUR_ACCESS_KEY_ID",
    aws_secret_access_key="YOUR_SECRET_ACCESS_KEY",
    region_name="us-west-2",
    schema_name="default",
    s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/"))
with engine.connect() as connection:
    many_rows = Table("many_rows", MetaData(), autoload_with=connection)
    result = connection.execute(select(func.count()).select_from(many_rows))
    print(result.scalar())
```

### Async

```python
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

conn_str = "awsathena+aiorest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/"\
           "{schema_name}?s3_staging_dir={s3_staging_dir}"
engine = create_async_engine(conn_str.format(
    aws_access_key_id="YOUR_ACCESS_KEY_ID",
    aws_secret_access_key="YOUR_SECRET_ACCESS_KEY",
    region_name="us-west-2",
    schema_name="default",
    s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/"))

async def main():
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT * FROM many_rows"))
        print(result.fetchall())
    await engine.dispose()
```

SQLAlchemy's reflection API (`Table(..., autoload_with=)`, `inspect()`) is synchronous
internally, so it cannot be called directly on an async connection. Use `run_sync()` to
bridge the gap:

```python
from sqlalchemy.sql.schema import Table, MetaData

async with engine.connect() as connection:
    # Table reflection
    table = await connection.run_sync(
        lambda sync_conn: Table("my_table", MetaData(), autoload_with=sync_conn)
    )

    # Schema inspection
    import sqlalchemy
    schemas = await connection.run_sync(
        lambda sync_conn: sqlalchemy.inspect(sync_conn).get_schema_names()
    )
```

## Reflection and identifiers

The dialect reflects schemas, tables, views, columns, comments, and Athena table options.
Athena does not support primary key, foreign key, unique, or index constraints.
Its `CREATE TABLE` syntax does not enforce `nullable=False`, and reflected columns report `nullable=True` and `autoincrement=False`.
Iceberg strings do not preserve `CHAR` or `VARCHAR` length constraints; `CHAR` is not a supported Iceberg type.
See the [Iceberg data type documentation](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-supported-data-types.html).
Hive preserves explicit `CHAR(n)` and `VARCHAR(n)` lengths; SQLAlchemy's generic `String` compiles to unbounded `STRING`.
Table comments returned by Athena metadata can have whitespace and line breaks normalized.
Glue rejects line breaks in Hive column comments.
Athena does not persist the table-level `COMMENT` when creating an Iceberg table, so its reflected table comment is `None`; column comments are preserved.

SQLAlchemy's Inspector caches reflection results, including both positive and negative `has_table()` results.
After creating or dropping a table, use a new Inspector or call `inspector.clear_cache()` (SQLAlchemy 2.0+) before inspecting it again.
The dialect does not cache direct `has_table()` calls without an `info_cache`.

Table listings include column and table metadata.
The dialect reuses these positive results within the same Inspector, so subsequent column, comment, and table-option reflection does not fetch each listed table again.
Individual table lookups also share their metadata across these reflection methods.
Metadata cache keys use the effective cursor catalog and resolved schema, so omitting the default schema and passing its name explicitly reuse the same metadata and table listing.
Table-name case variants share metadata in `AwsDataCatalog`; other catalogs retain case-sensitive cache keys.
A later listing preserves metadata already fetched for a table.
`clear_cache()` also discards this metadata; an absent entry in a listing is not cached as proof that a table does not exist.

Table-metadata lookups propagate throttling and permission errors rather than reporting missing tables.
`has_table()` propagates permission failures, including access denied by Lake Formation, instead of returning or caching `False`.
For failed metadata requests, only recognized `EntityNotFoundException` responses establish absence; unrecognized errors are propagated rather than guessed to mean a missing table.
Column reflection and `has_table()` do not retry a throttled table-metadata request; when Athena reports throttling, they read `information_schema.columns` instead, executed without query result reuse, and log a warning.
Other error codes listed in the connection's `RetryConfig.exceptions` are still retried on that path, except `MetadataException` itself, which carries wrapped throttling; list the wrapped Glue codes instead.
A `retry_config` in `cursor_kwargs` replaces that policy entirely, including its throttling retries, which then run before the fallback.
The fallback maps unbounded `varchar` to SQLAlchemy `String`, matching Hive `STRING` reflection from the metadata API, and preserves explicit `VARCHAR(n)` and `CHAR(n)` lengths.
Partition columns are marked from the `extra_info` column.
This fallback does not populate the table-metadata cache, and table comments and table options still come from the metadata API with the configured retries, so they propagate the throttling error.
Athena applies its metadata API rate limits per account, and they are not listed in Service Quotas.
PyAthena's API retries use exponential backoff with uniform jitter; `RetryConfig` documents the default attempt count and waits.
PyAthena recognizes Glue error codes in Athena's `MetadataException` service-error envelope and applies `RetryConfig.exceptions` to those codes.
`RetryConfig` accepts one exception-name string or an iterable and captures the names as a tuple at construction.
Changes to the original input list or iterator no longer change the stored policy; construct a new `RetryConfig` when changing the retry policy.
SDK retries and PyAthena retries are separate layers, so increasing both attempt limits can multiply requests and waiting time.
Adaptive SDK retries regulate the request rate of one client; they do not coordinate separate processes.
For highly concurrent reflection or `checkfirst` DDL, bound the concurrency of metadata requests and consider `botocore.config.Config(retries={"mode": "adaptive"})` on the connection.

Use SQLAlchemy's identifier quoting for reserved words or names beginning with an underscore.
The dialect uses backticks for table DDL and double quotes for DML.
Quoted names still follow Athena's naming rules: Athena lowercases identifiers, and table names containing spaces or embedded quotes are not supported.
The dialect declares a maximum identifier length of 255 characters to SQLAlchemy.
For table names longer than 128 characters, reflection uses Athena's `ListTableMetadata` API because [`GetTableMetadata`](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetTableMetadata.html) rejects those names.
Both APIs limit database names to 128 characters, so reflection is limited to schemas within that length.
AWS also specifies a maximum of 255 UTF-8 bytes for database, table, and column names, so the character limit alone does not validate multibyte names.
See [Athena's naming rules](https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html) and [CREATE TABLE restrictions](https://docs.aws.amazon.com/athena/latest/ug/create-table.html).

## Connection string

The connection string has the following format:

```text
awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}&...
```

If you do not specify `aws_access_key_id` and `aws_secret_access_key` using instance profile or boto3 configuration file:

```text
awsathena+rest://:@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}&...
```

For async, replace the driver portion (e.g. `+rest` with `+aiorest`):

```text
awsathena+aiorest://:@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}&...
```

## Dialect & driver

### Sync

| Dialect   | Driver | Schema           | Cursor                 |
|-----------|--------|------------------|------------------------|
| awsathena |        | awsathena        | DefaultCursor          |
| awsathena | rest   | awsathena+rest   | DefaultCursor          |
| awsathena | pandas | awsathena+pandas | {ref}`pandas-cursor`   |
| awsathena | arrow  | awsathena+arrow  | {ref}`arrow-cursor`    |
| awsathena | polars | awsathena+polars | {ref}`polars-cursor`   |
| awsathena | s3fs   | awsathena+s3fs   | {ref}`s3fs-cursor`     |

### Async

Requires `pip install PyAthena[aiosqlalchemy]` (SQLAlchemy 2.0+).

| Dialect   | Driver    | Schema              | Cursor                       |
|-----------|-----------|---------------------|------------------------------|
| awsathena | aiorest   | awsathena+aiorest   | DefaultCursor (async)        |
| awsathena | aiopandas | awsathena+aiopandas | {ref}`pandas-cursor` (async) |
| awsathena | aioarrow  | awsathena+aioarrow  | {ref}`arrow-cursor` (async)  |
| awsathena | aiopolars | awsathena+aiopolars | {ref}`polars-cursor` (async) |
| awsathena | aios3fs   | awsathena+aios3fs   | {ref}`s3fs-cursor` (async)   |

## Dialect options

### Table options

location
: Type: `str`

  Description: Specifies the location of the underlying data in the Amazon S3 from which the table is created.

  value: s3://bucket/path/to/

  Example:

  ```python
  Table("some_table", metadata, ..., awsathena_location="s3://bucket/path/to/")
  ```

compression
: Type: `str`

  Description: Specifies the compression format.

  Value:

- BZIP2
- DEFLATE
- GZIP
- LZ4
- LZO
- SNAPPY
- ZLIB
- ZSTD
- NONE|UNCOMPRESSED

  Example:

  ```python
  Table("some_table", metadata, ..., awsathena_compression="SNAPPY")
  ```

row_format
: Type: `str`

  Description: Specifies the row format of the table and its underlying source data if applicable.

  Value:

- [DELIMITED FIELDS TERMINATED BY char [ESCAPED BY char]]
- [DELIMITED COLLECTION ITEMS TERMINATED BY char]
- [MAP KEYS TERMINATED BY char]
- [LINES TERMINATED BY char]
- [NULL DEFINED AS char]
- SERDE 'serde_name'

  Example:

  ```python
  Table("some_table", metadata, ..., awsathena_row_format="SERDE 'org.openx.data.jsonserde.JsonSerDe'")
  ```

file_format
: Type: `str`

  Description: Specifies the file format for table data.

  Value:

- SEQUENCEFILE
- TEXTFILE
- RCFILE
- ORC
- PARQUET
- AVRO
- ION
- INPUTFORMAT input_format_classname OUTPUTFORMAT output_format_classname

  Example:

  ```python
  Table("some_table", metadata, ..., awsathena_file_format="PARQUET")
  Table("some_table", metadata, ..., awsathena_file_format="INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'")
  ```

serdeproperties
: Type: `dict[str, str]`

  Description: Specifies one or more custom properties allowed in SerDe.

  Value:

  ```python
  { "property_name": "property_value", "property_name": "property_value", ... }
  ```

  Example:

  ```python
  Table("some_table", metadata, ..., awsathena_serdeproperties={
      "separatorChar": ",", "escapeChar": "\\\\"
  })
  ```

tblproperties
: Type: `dict[str, str]`

  Description: Specifies custom metadata key-value pairs for the table definition in addition to predefined table properties.

  Value:

  ```python
  { "property_name": "property_value", "property_name": "property_value", ... }
  ```

  Example:

  ```python
  Table("some_table", metadata, ..., awsathena_tblproperties={
      "projection.enabled": "true",
      "projection.dt.type": "date",
      "projection.dt.range": "NOW-1YEARS,NOW",
      "projection.dt.format": "yyyy-MM-dd",
  })
  ```

bucket_count
: Type: `int`

  Description: The number of buckets for bucketing your data.

  Value: Integer value greater than or equal to 0

  Example:

  ```python
  Table("some_table", metadata, ..., awsathena_bucket_count=5)
  ```

All table options can also be configured with the connection string as follows:

```text
awsathena+rest://:@athena.us-west-2.amazonaws.com:443/default?s3_staging_dir=s3%3A%2F%2Fbucket%2Fpath%2Fto%2F&location=s3%3A%2F%2Fbucket%2Fpath%2Fto%2F&file_format=parquet&compression=snappy&...
```

`serdeproperties` and `tblproperties` must be converted to strings in the `'key'='value','key'='value'` format and url encoded.
If single quotes are included, escape them with a backslash.

For example, if you configure a projection setting `'projection.enabled'='true','projection.dt.type'='date','projection.dt.range'='NOW-1YEARS,NOW','projection.dt.format'= 'yyyy-MM-dd'` in tblproperties, it would look like this

```text
awsathena+rest://:@athena.us-west-2.amazonaws.com:443/default?s3_staging_dir=s3%3A%2F%2Fbucket%2Fpath%2Fto%2F&tblproperties=%27projection.enabled%27%3D%27true%27%2C%27projection.dt.type%27%3D%27date%27%2C%27projection.dt.range%27%3D%27NOW-1YEARS%2CNOW%27%2C%27projection.dt.format%27%3D+%27yyyy-MM-dd%27
```

### Column options

partition
: Type: `bool`

  Description: Specifies a key for partitioning data.

  Value: True / False

  Example:

  ```python
  Column("some_column", types.String, ..., awsathena_partition=True)
  ```

partition_transform
: Type: `str`

  Description: Specifies a partition transform function for partitioning data.
  Only has an effect for ICEBERG tables and when partition is set to true for the column.

  Value:

- year
- month
- day
- hour
- bucket
- truncate

  Example:

  ```python
  Column("some_column", types.Date, ..., awsathena_partition=True, awsathena_partition_transform='year')
  ```

partition_transform_bucket_count
: Type: `int`

  Description: Used for N in the bucket partition transform function, partitions by hashed value mod N buckets.
  Only has an effect for ICEBERG tables and when partition is set to true and
  when the partition transform is set to 'bucket' for the column.

  Value: Integer value greater than or equal to 0

  Example:

  ```python
  Column("some_column", types.String, ..., awsathena_partition=True, awsathena_partition_transform='bucket', awsathena_partition_transform_bucket_count=5)
  ```

partition_transform_truncate_length
: Type: `int`

  Description: Used for L in the truncate partition transform function, partitions by value truncated to L.
  Only has an effect for ICEBERG tables and when partition is set to true and
  when the partition transform is set to 'truncate' for the column.

  Value: Integer value greater than or equal to 0

  Example:

  ```python
  Column("some_column", types.String, ..., awsathena_partition=True, awsathena_partition_transform='truncate', awsathena_partition_transform_truncate_length=5)
  ```

cluster
: Type: `bool`

  Description: Divides the data in the specified column into data subsets called buckets, with or without partitioning.

  Value: True / False

  Example:

  ```python
  Column("some_column", types.String, ..., awsathena_cluster=True)
  ```

To configure column options from the connection string, specify the column name as a comma-separated string.
The options partition_transform, partition_transform_bucket_count, partition_transform_truncate_length are not supported
to be configured from the connection string.

```text
awsathena+rest://:@athena.us-west-2.amazonaws.com:443/default?partition=column1%2Ccolumn2&cluster=column1%2Ccolumn2&...
```

If you want to limit the column options to specific table names only, specify the table and column names connected by dots as a comma-separated string.

```text
awsathena+rest://:@athena.us-west-2.amazonaws.com:443/default?partition=table1.column1%2Ctable1.column2&cluster=table2.column1%2Ctable2.column2&...
```

## Amazon S3 Tables

[Amazon S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html) are Iceberg-backed
tables stored in a dedicated table bucket. Once the table bucket is integrated with the AWS analytics services,
Athena registers it as a catalog named `s3tablescatalog/<table-bucket>`, with the S3 Tables namespace as the
database.

Athena does not accept a three-part `catalog.namespace.table` identifier in DDL, so select the catalog on the
**connection** with `catalog_name` and use the namespace as the table `schema`. Because S3 Tables use managed
storage, the dialect omits the `LOCATION`, `ROW FORMAT`, and `STORED AS` clauses automatically. Tables must
declare `awsathena_tblproperties={"table_type": "ICEBERG"}` (S3 Tables support only Iceberg), and
`awsathena_location` must not be set — the dialect raises a compile-time error otherwise.

```python
engine = create_engine(
    "awsathena+rest://athena.us-west-2.amazonaws.com:443/my_namespace"
    "?s3_staging_dir=s3://my-bucket/athena-results/"
    "&catalog_name=s3tablescatalog/my-table-bucket"
)

table = Table(
    "some_table",
    MetaData(schema="my_namespace"),
    Column("id", types.Integer),
    Column("dt", types.Date, awsathena_partition=True, awsathena_partition_transform="day"),
    awsathena_tblproperties={"table_type": "ICEBERG"},
)
with engine.connect() as conn:
    table.create(bind=conn)
```

which builds the following statement:

```sql
CREATE TABLE my_namespace.some_table (
  id INT,
  dt DATE
)
PARTITIONED BY (
  day(dt)
)
TBLPROPERTIES (
  'table_type' = 'ICEBERG'
)
```

All Iceberg partition transforms (`year`, `month`, `day`, `hour`, `bucket`, `truncate`) are supported, the same as
for other Iceberg tables. CTAS (`CREATE TABLE ... AS SELECT`) is not modeled as a SQLAlchemy construct; issue it as
raw SQL via `text()`. Managed Iceberg CTAS requires `is_external = false`:

```python
conn.execute(text(
    'CREATE TABLE "my_namespace"."some_table" '
    "WITH (table_type = 'ICEBERG', is_external = false) AS SELECT 1 AS id"
))
```

## Temporal/Time-travel with Iceberg

Athena supports time-travel queries on Iceberg tables by either a version_id or a timestamp. The `FOR TIMESTAMP AS OF`
clause is used to query the table as it existed at the specified timestamp. To build a time travel query by timestamp,
use `with_hint(table_name, "FOR TIMESTAMP AS OF timestamp")` after the table name in the SELECT statement, as in the
following example.

```python
    select(table.c).with_hint(table_name, "FOR TIMESTAMP AS OF '2024-03-17 10:00:00'")
```

which will build a statement that outputs the following:

```sql
    SELECT * FROM table_name FOR TIMESTAMP AS OF '2024-03-17 10:00:00'
```

To build a time travel query by version_id, use `with_hint(table_name, "FOR VERSION AS OF version_id")` after the table
name. Note: the version_id is also know as a snapshot_id can be retrieved by querying the `table_name$snapshots`
or `table_name$history` metadata. Again the hint goes after the select statement as in the following example.

```python
    select(table.c).with_hint(table_name, "FOR VERSION AS OF 949530903748831860")
```

```sql
    SELECT * FROM table_name FOR VERSION AS OF 949530903748831860
```

(sqlalchemy-query-execution-callback)=

## Query Execution Callback

PyAthena provides callback support for SQLAlchemy applications to get immediate access to query IDs
after the `start_query_execution` API call, enabling query monitoring and cancellation capabilities.

### Connection-level callback

You can set a default callback for all queries through an engine's connection parameters:

```python
from sqlalchemy import create_engine, text

def query_callback(query_id):
    print(f"SQLAlchemy query started: {query_id}")
    # Store query_id for monitoring or cancellation

conn_str = "awsathena+rest://:@athena.us-west-2.amazonaws.com:443/default?s3_staging_dir=s3://YOUR_S3_BUCKET/path/to/"
engine = create_engine(
    conn_str,
    connect_args={"on_start_query_execution": query_callback}
)

with engine.connect() as connection:
    result = connection.execute(text("SELECT * FROM many_rows"))
    # query_callback will be invoked before query execution
```

### Execution options callback

SQLAlchemy applications can use `execution_options` to specify callbacks for individual queries:

```python
from sqlalchemy import create_engine, text

def specific_callback(query_id):
    print(f"Specific query callback: {query_id}")

conn_str = "awsathena+rest://:@athena.us-west-2.amazonaws.com:443/default?s3_staging_dir=s3://YOUR_S3_BUCKET/path/to/"
engine = create_engine(conn_str)

with engine.connect() as connection:
    result = connection.execute(
        text("SELECT * FROM many_rows").execution_options(
            on_start_query_execution=specific_callback
        )
    )
```

### Query timeout management with SQLAlchemy

A practical example for managing long-running analytical queries with timeout:

```python
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from sqlalchemy import create_engine, text

def run_analytics_with_timeout():
    """Run analytics query with automatic timeout and cancellation."""

    query_info = {'query_id': None, 'connection': None}

    def track_query_start(query_id):
        query_info['query_id'] = query_id
        print(f"Analytics query started: {query_id}")

    def timeout_monitor(timeout_minutes):
        """Cancel query after timeout period."""
        time.sleep(timeout_minutes * 60)
        if query_info['query_id'] and query_info['connection']:
            try:
                # Cancel via raw connection's cursor
                cursor = query_info['connection'].connection.cursor()
                cursor.cancel()
                print(f"Query {query_info['query_id']} cancelled after {timeout_minutes}min timeout")
            except Exception as e:
                print(f"Cancellation attempt failed: {e}")

    conn_str = "awsathena+rest://:@athena.us-west-2.amazonaws.com:443/default?s3_staging_dir=s3://YOUR_S3_BUCKET/path/to/"
    engine = create_engine(
        conn_str,
        connect_args={"on_start_query_execution": track_query_start}
    )

    # Complex data processing query
    analytics_query = text("""
    WITH monthly_cohorts AS (
        SELECT
            date_trunc('month', first_purchase_date) as cohort_month,
            user_id,
            date_trunc('month', purchase_date) as purchase_month,
            revenue
        FROM user_purchases
        WHERE first_purchase_date >= current_date - interval '2' year
    ),
    cohort_data AS (
        SELECT
            cohort_month,
            purchase_month,
            COUNT(DISTINCT user_id) as users,
            SUM(revenue) as total_revenue,
            date_diff('month', cohort_month, purchase_month) as month_number
        FROM monthly_cohorts
        GROUP BY cohort_month, purchase_month
    )
    SELECT
        cohort_month,
        month_number,
        users,
        total_revenue,
        ROUND(users * 100.0 / FIRST_VALUE(users) OVER (
            PARTITION BY cohort_month ORDER BY month_number
        ), 2) as retention_rate
    FROM cohort_data
    WHERE month_number <= 12
    ORDER BY cohort_month, month_number
    """)

    with ThreadPoolExecutor(max_workers=1) as executor:
        with engine.connect() as connection:
            query_info['connection'] = connection

            # Start timeout monitor (15 minutes for complex analytics)
            timeout_future = executor.submit(timeout_monitor, 15)

            try:
                print("Starting cohort analysis (15-minute timeout)...")
                result = connection.execute(analytics_query)

                # Process results
                rows = result.fetchall()
                print(f"Cohort analysis completed: {len(rows)} data points")

                # Show sample results
                for i, row in enumerate(rows[:5]):  # First 5 rows
                    print(f"  Cohort {row.cohort_month}: Month {row.month_number}, "
                          f"{row.users} users, {row.retention_rate}% retention")

                if len(rows) > 5:
                    print(f"  ... and {len(rows) - 5} more rows")

            except Exception as e:
                print(f"Analytics query failed or was cancelled: {e}")
            finally:
                # Clean up
                query_info['connection'] = None
                try:
                    timeout_future.result(timeout=1)
                except TimeoutError:
                    pass  # Timeout monitor still running

# Run the analytics example
run_analytics_with_timeout()
```

### Multiple callbacks

When both connection-level and execution_options callbacks are specified,
both callbacks will be invoked:

```python
from sqlalchemy import create_engine, text

def connection_callback(query_id):
    print(f"Connection callback: {query_id}")
    # Global monitoring for all queries

def execution_callback(query_id):
    print(f"Execution callback: {query_id}")
    # Specific handling for this query

conn_str = "awsathena+rest://:@athena.us-west-2.amazonaws.com:443/default?s3_staging_dir=s3://YOUR_S3_BUCKET/path/to/"
engine = create_engine(
    conn_str,
    connect_args={"on_start_query_execution": connection_callback}
)

with engine.connect() as connection:
    # This will invoke both connection_callback and execution_callback
    result = connection.execute(
        text("SELECT 1").execution_options(
            on_start_query_execution=execution_callback
        )
    )
```

### Supported SQLAlchemy dialects

The `on_start_query_execution` callback is supported by all PyAthena SQLAlchemy dialects:

- `awsathena` and `awsathena+rest` (default cursor)
- `awsathena+pandas` (pandas cursor)
- `awsathena+arrow` (arrow cursor)
- `awsathena+polars` (polars cursor)
- `awsathena+s3fs` (S3FS cursor)

Usage with different dialects:

```python
# With pandas dialect
engine_pandas = create_engine(
    "awsathena+pandas://:@athena.us-west-2.amazonaws.com:443/default?s3_staging_dir=s3://YOUR_S3_BUCKET/path/to/",
    connect_args={"on_start_query_execution": query_callback}
)

# With arrow dialect
engine_arrow = create_engine(
    "awsathena+arrow://:@athena.us-west-2.amazonaws.com:443/default?s3_staging_dir=s3://YOUR_S3_BUCKET/path/to/",
    connect_args={"on_start_query_execution": query_callback}
)
```

## Complex data types

### STRUCT type support

PyAthena provides comprehensive support for Amazon Athena's STRUCT (also known as ROW) data types, enabling you to work with complex nested data structures in your Python applications.

#### Basic usage

```python
from sqlalchemy import Column, String, Integer, Table, MetaData
from pyathena.sqlalchemy.types import AthenaStruct

# Define a table with STRUCT columns
users = Table('users', metadata,
    Column('id', Integer),
    Column('profile', AthenaStruct(
        ('name', String),
        ('age', Integer),
        ('email', String)
    )),
    Column('settings', AthenaStruct(
        ('theme', String),
        ('notifications', AthenaStruct(
            ('email', String),
            ('push', String)
        ))
    ))
)
```

This generates the following SQL structure:

```sql
CREATE TABLE users (
    id INTEGER,
    profile ROW(name STRING, age INTEGER, email STRING),
    settings ROW(theme STRING, notifications ROW(email STRING, push STRING))
)
```

#### Querying STRUCT data

PyAthena automatically converts STRUCT data between different formats:

```python
from sqlalchemy import create_engine, select

# Query STRUCT data using ROW constructor
result = connection.execute(
    select().from_statement(
        text("SELECT ROW('John Doe', 30, 'john@example.com') as profile")
    )
).fetchone()

# Access STRUCT fields as dictionary
profile = result.profile  # {"0": "John Doe", "1": 30, "2": "john@example.com"}
```

#### Named STRUCT fields

For better readability, use JSON casting to get named fields:

```python
# Using CAST AS JSON for named field access
result = connection.execute(
    select().from_statement(
        text("SELECT CAST(ROW('John', 30) AS JSON) as user_data")
    )
).fetchone()

# Parse JSON result
import json
user_data = json.loads(result.user_data)  # ["John", 30]
```

#### Data format support

PyAthena supports multiple STRUCT data formats:

**Athena Native Format:**

```python
# Input: "{name=John, age=30}"
# Output: {"name": "John", "age": 30}
```

**JSON Format (Recommended):**

```python
# Input: '{"name": "John", "age": 30}'
# Output: {"name": "John", "age": 30}
```

**Unnamed STRUCT Format:**

```python
# Input: "{Alice, 25}"
# Output: {"0": "Alice", "1": 25}
```

#### Performance considerations

- **JSON Format**: Recommended for complex nested structures
- **Native Format**: Optimized for simple key-value pairs
- **Smart Detection**: PyAthena automatically detects the format to avoid unnecessary parsing overhead

#### Best practices

1. **Use JSON casting** for complex nested structures:

   ```sql
   SELECT CAST(complex_struct AS JSON) FROM table_name
   ```

2. **Define clear field types** in AthenaStruct definitions:

   ```python
   AthenaStruct(
       ('user_id', Integer),
       ('profile', AthenaStruct(
           ('name', String),
           ('preferences', AthenaStruct(
               ('theme', String),
               ('language', String)
           ))
       ))
   )
   ```

3. **Handle NULL values** appropriately in your application logic:

   ```python
   if result.struct_column is not None:
       # Process struct data
       field_value = result.struct_column.get('field_name')
   ```

#### Migration from RAW strings

**Before (raw string handling):**

```python
result = cursor.execute("SELECT struct_column FROM table").fetchone()
raw_data = result[0]  # "{\"name\": \"John\", \"age\": 30}"
import json
parsed_data = json.loads(raw_data)
```

**After (automatic conversion):**

```python
result = cursor.execute("SELECT struct_column FROM table").fetchone()
struct_data = result[0]  # {"name": "John", "age": 30} - automatically converted
name = struct_data['name']  # Direct access
```

### MAP type support

PyAthena provides comprehensive support for Amazon Athena's MAP data types, enabling you to work with key-value data structures in your Python applications.

#### Basic usage

```python
from sqlalchemy import Column, String, Integer, Table, MetaData
from pyathena.sqlalchemy.types import AthenaMap

# Define a table with MAP columns
products = Table('products', metadata,
    Column('id', Integer),
    Column('attributes', AthenaMap(String, String)),
    Column('metrics', AthenaMap(String, Integer)),
    Column('categories', AthenaMap(Integer, String))
)
```

This generates the following SQL structure:

```sql
CREATE TABLE products (
    id INTEGER,
    attributes MAP<STRING, STRING>,
    metrics MAP<STRING, INTEGER>,
    categories MAP<INTEGER, STRING>
)
```

#### Querying MAP data

PyAthena automatically converts MAP data between different formats:

```python
from sqlalchemy import create_engine, select

# Query MAP data using MAP constructor
result = connection.execute(
    select().from_statement(
        text("SELECT MAP(ARRAY['name', 'category'], ARRAY['Laptop', 'Electronics']) as product_info")
    )
).fetchone()

# Access MAP data as dictionary
product_info = result.product_info  # {"name": "Laptop", "category": "Electronics"}
```

#### Advanced MAP operations

For complex MAP operations, use JSON casting:

```python
# Using CAST AS JSON for complex MAP operations
result = connection.execute(
    select().from_statement(
        text("SELECT CAST(MAP(ARRAY['price', 'rating'], ARRAY['999', '4.5']) AS JSON) as data")
    )
).fetchone()

# Parse JSON result
import json
data = json.loads(result.data)  # {"price": "999", "rating": "4.5"}
```

#### Data format support

PyAthena supports multiple MAP data formats:

**Athena Native Format:**

```python
# Input: "{name=Laptop, category=Electronics}"
# Output: {"name": "Laptop", "category": "Electronics"}
```

**JSON Format (Recommended):**

```python
# Input: '{"name": "Laptop", "category": "Electronics"}'
# Output: {"name": "Laptop", "category": "Electronics"}
```

#### Performance considerations

- **JSON Format**: Recommended for complex nested structures
- **Native Format**: Optimized for simple key-value pairs
- **Smart Detection**: PyAthena automatically detects the format to avoid unnecessary parsing overhead

#### Best practices

1. **Use JSON casting** for complex nested structures:

   ```sql
   SELECT CAST(complex_map AS JSON) FROM table_name
   ```

2. **Define clear key-value types** in AthenaMap definitions:

   ```python
   AthenaMap(String, Integer)  # String keys, Integer values
   AthenaMap(Integer, AthenaStruct(...))  # Integer keys, STRUCT values
   ```

3. **Handle NULL values** appropriately in your application logic:

   ```python
   if result.map_column is not None:
       # Process map data
       value = result.map_column.get('key_name')
   ```

#### Migration from RAW strings

**Before (raw string handling):**

```python
result = cursor.execute("SELECT map_column FROM table").fetchone()
raw_data = result[0]  # "{\"key1\": \"value1\", \"key2\": \"value2\"}"
import json
parsed_data = json.loads(raw_data)
```

**After (automatic conversion):**

```python
result = cursor.execute("SELECT map_column FROM table").fetchone()
map_data = result[0]  # {"key1": "value1", "key2": "value2"} - automatically converted
value = map_data['key1']  # Direct access
```

### ARRAY type support

PyAthena supports SQLAlchemy's `ARRAY` type and the dialect-specific `AthenaArray` type.
Both store native Athena arrays and return typed Python collections.
Reflected ARRAY columns use `AthenaArray` and preserve their element types, including nested arrays, maps, rows, decimal precision, and string length where Athena retains it.

```python
from sqlalchemy import ARRAY, Column, Integer, MetaData, String, Table, select

metadata = MetaData()
events = Table(
    "events",
    metadata,
    Column("id", Integer),
    Column("numbers", ARRAY(Integer)),
    Column("labels", ARRAY(String, dimensions=2)),
)

connection.execute(
    events.insert(),
    {"id": 1, "numbers": [1, None, 3], "labels": [["one", "two"], ["a,b", "001"]]},
)
row = connection.execute(select(events.c.numbers, events.c.labels)).one()
assert row.numbers == [1, None, 3]
assert row.labels == [["one", "two"], ["a,b", "001"]]
```

Use `dimensions=N` for a fixed number of dimensions with the standard SQLAlchemy type.
Without it, the table column has one dimension.
The existing `AthenaArray(AthenaArray(Integer))` spelling also remains supported; do not combine nested ARRAY types with `dimensions`.
`AthenaArray()` still defaults to string elements, and the uppercase `pyathena.sqlalchemy.types.ARRAY` alias remains available.
Set `as_tuple=True` to return tuples at each array dimension instead of lists.

Bound parameters, multiple parameter sets, and SQLAlchemy literals use Athena's `ARRAY[...]` constructors.
An empty list represents an empty array, `None` represents SQL NULL, and individual elements can also be NULL.
Ordinary DB API list and tuple parameters retain their existing `IN (...)` formatting.
For decimal binds, specify `Numeric(precision, scale)`; a precision-free `Numeric()` raises a compilation error because Athena's bare DECIMAL cast would round fractional values to scale zero.
Athena `Float` uses 32-bit REAL values; use `Double` for 64-bit floating-point elements.

Typed SQLAlchemy SELECT expressions use a JSON transport projection to preserve nested values and strings containing commas, quotes, whitespace, or the word `null`.
Scalar leaves for supported Athena table types are decoded according to the declared type, preserving decimal precision, dates, timestamps, and binary values.
SQL predicates and intermediate subqueries still operate on native arrays.
Arrays with unknown (`NullType`) elements keep the cursor's native conversion instead of using typed transport.
For ordered typed ARRAY results, use SQLAlchemy column expressions.
Textual ORDER BY clauses may name selected columns (including comma-separated names and direction/null placement); other textual expressions raise a compilation error to prevent ordering serialized values or referring to columns outside their scope.
String label references such as `.order_by("id")` also resolve FROM-table columns using SQLAlchemy's normal rules.
For DISTINCT and compound queries, ordering expressions must refer to selected columns.
Ordered, DISTINCT, and compound ARRAY queries require explicit SELECT columns: use SQLAlchemy column expressions or `literal_column()` instead of `text()` projections, and `select(table)` instead of a wildcard.
Literal SQL expressions need an explicit label, for example `literal_column("cardinality(items)").label("size")`.
This avoids dropping unnamed columns or exposing internal ordering columns when the result is wrapped.
An outer `TypeDecorator` retains its result processor as well as native ARRAY ordering.
Raw `text()` queries and direct DB API queries retain the cursor's existing conversion behavior described below; they do not receive this projection automatically.

Compared with earlier releases, reflected ARRAY columns are no longer reported as `String`.
ARRAY DDL now renders integer elements as `INT` and row elements as `STRUCT<...>`, which Athena requires for nested DDL types.
Code that inspects reflected types or compares compiled SQL strings should account for these changes.

#### Basic Usage

```python
from sqlalchemy import Column, String, Integer, Table, MetaData
from pyathena.sqlalchemy.types import AthenaArray

# Define a table with ARRAY columns
orders = Table('orders', metadata,
    Column('id', Integer),
    Column('item_ids', AthenaArray(Integer)),
    Column('tags', AthenaArray(String)),
    Column('categories', AthenaArray(String))
)
```

This creates a table definition equivalent to:

```sql
CREATE TABLE orders (
    id INTEGER,
    item_ids ARRAY<INT>,
    tags ARRAY<STRING>,
    categories ARRAY<STRING>
)
```

#### Querying ARRAY data

PyAthena automatically converts ARRAY data between different formats:

```python
from sqlalchemy import text

# Query ARRAY data using ARRAY constructor
result = connection.execute(
    text("SELECT ARRAY[1, 2, 3, 4, 5] as item_ids")
).fetchone()

# Access ARRAY data as Python list
item_ids = result.item_ids  # [1, 2, 3, 4, 5]
```

#### Complex ARRAY operations

For arrays containing complex data types:

```python
# Arrays with STRUCT elements
result = connection.execute(
    text("SELECT ARRAY[ROW('Alice', 25), ROW('Bob', 30)] as users")
).fetchone()

users = result.users
# Use typed SQLAlchemy expressions when you need declared ROW field types.

# Using CAST AS JSON for complex ARRAY operations
result = connection.execute(
    text("SELECT CAST(ARRAY[1, 2, 3] AS JSON) as data")
).fetchone()

# Parse JSON result
import json
if isinstance(result.data, str):
    array_data = json.loads(result.data)  # [1, 2, 3]
else:
    array_data = result.data  # Already converted to list
```

#### Data format support

PyAthena supports multiple ARRAY data formats:

**Athena Native Format:**

```python
# Input: '[1, 2, 3]'
# Output: [1, 2, 3]

# Input: '[apple, banana, cherry]'
# Output: ["apple", "banana", "cherry"]
```

**JSON Format:**

```python
# Input: '[1, 2, 3]'
# Output: [1, 2, 3]

# Input: '["apple", "banana", "cherry"]'
# Output: ["apple", "banana", "cherry"]
```

**Complex Nested Arrays:**

```python
# Input: '[{name=John, age=30}, {name=Jane, age=25}]'
# Output: [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]
```

#### Type definitions

AthenaArray supports various item types:

```python
from pyathena.sqlalchemy.types import AthenaArray, AthenaStruct, AthenaMap

# Simple arrays
AthenaArray(String)      # ARRAY<STRING>
AthenaArray(Integer)     # ARRAY<INT>

# Arrays of complex types
AthenaArray(AthenaStruct(...))  # ARRAY<STRUCT<...>>
AthenaArray(AthenaMap(...))     # ARRAY<MAP<...>>

# Nested arrays
AthenaArray(AthenaArray(Integer))  # ARRAY<ARRAY<INT>>
```

#### Best practices

1. **Use appropriate item types** in AthenaArray definitions:

   ```python
   AthenaArray(Integer)  # For numeric arrays
   AthenaArray(String)   # For string arrays
   AthenaArray(AthenaStruct(...))  # For arrays of structs
   ```

2. **Use CAST AS JSON** for complex array operations:

   ```sql
   SELECT CAST(complex_array AS JSON) FROM table_name
   ```

3. **Handle NULL values** appropriately in your application logic:

   ```python
   if result.array_column is not None:
       # Process array data
       first_item = result.array_column[0] if result.array_column else None
   ```

#### Migration from RAW strings

**Before (raw string handling):**

```python
result = cursor.execute("SELECT array_column FROM table").fetchone()
raw_data = result[0]  # "[1, 2, 3]"
import json
parsed_data = json.loads(raw_data)
```

**After (automatic conversion):**

```python
result = cursor.execute("SELECT array_column FROM table").fetchone()
array_data = result[0]  # [1, 2, 3] - automatically converted
first_item = array_data[0]  # Direct access
```

### JSON type support

PyAthena provides support for Amazon Athena's JSON data type, enabling you to work with JSON data in your SQLAlchemy applications. The JSON type is primarily used with Data Manipulation Language (DML) operations in Athena.

#### Basic usage

```python
from sqlalchemy import Column, Integer, Table, MetaData
from sqlalchemy.types import JSON

# Define a table with JSON column
events = Table('events', metadata,
    Column('id', Integer),
    Column('metadata', JSON),
    Column('config', JSON)
)
```

#### Querying JSON data

When querying JSON data, PyAthena automatically parses JSON strings into Python dictionaries:

```python
from sqlalchemy import select, literal_column
from sqlalchemy.sql import type_coerce
from sqlalchemy.types import JSON

# Query with explicit type coercion
result = connection.execute(
    select(
        type_coerce(
            literal_column('CAST(\'{"name": "test", "value": 123}\' AS JSON)'),
            JSON
        ).label("json_col")
    )
).fetchone()

# Result is automatically parsed as a dictionary
print(result.json_col)  # {"name": "test", "value": 123}
print(type(result.json_col))  # <class 'dict'>
```

#### Important limitations

Athena's JSON type support has specific limitations:

- **JSON objects are fully supported** - Objects with key-value pairs work correctly
- **Top-level JSON arrays are not supported** - Direct CAST of arrays like `[1, 2, 3]` will fail
- **Arrays within objects are supported** - JSON objects can contain arrays as property values
- **DML only** - JSON type is supported for SELECT queries but not in CREATE TABLE statements

```python
# Supported: JSON object with nested array
result = connection.execute(
    select(
        type_coerce(
            literal_column('CAST(\'{"items": [1, 2, 3]}\' AS JSON)'),
            JSON
        ).label("json_col")
    )
).fetchone()
print(result.json_col)  # {"items": [1, 2, 3]}

# Not supported: Top-level array
# This will raise InvalidRequestException
# CAST('[1, 2, 3]' AS JSON)
```

#### Best practices

1. **Use with SELECT queries** - JSON type works best for querying existing data
2. **Handle nested structures** - Objects with nested arrays and objects are fully supported
3. **Explicit type coercion** - Use `type_coerce()` when working with literal JSON values
4. **Error handling** - Be prepared to handle `InvalidRequestException` for unsupported operations
