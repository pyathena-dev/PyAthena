(usage)=

# Usage

## Basic usage

```python
from pyathena import connect

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
cursor.execute("SELECT * FROM one_row")
print(cursor.description)
print(cursor.fetchall())
```

## Cursor iteration

```python
from pyathena import connect

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
cursor.execute("SELECT * FROM many_rows LIMIT 10")
for row in cursor:
    print(row)
```

## Query with parameters

Supported [DB API paramstyle](https://www.python.org/dev/peps/pep-0249/#paramstyle) is only `PyFormat`.
`PyFormat` only supports [named placeholders](https://pyformat.info/#named_placeholders) with old `%` operator style and parameters specify dictionary format.

```python
from pyathena import connect

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
cursor.execute("""
               SELECT col_string FROM one_row_complex
               WHERE col_string = %(param)s
               """,
               {"param": "a string"})
print(cursor.fetchall())
```

if `%` character is contained in your query, it must be escaped with `%%` like the following:

```sql
SELECT col_string FROM one_row_complex
WHERE col_string = %(param)s OR col_string LIKE 'a%%'
```

### Use parameterized queries

If you want to use Athena's parameterized queries, you can do so by changing the `paramstyle` to `qmark` as follows.

```python
from pyathena import connect

pyathena.paramstyle = "qmark"
cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
cursor.execute("""
               SELECT col_string FROM one_row_complex
               WHERE col_string = ?
               """,
               ["'a string'"])
print(cursor.fetchall())
```

You can also specify the `paramstyle` using the execute method when executing a query.

```python
from pyathena import connect

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
cursor.execute("""
               SELECT col_string FROM one_row_complex
               WHERE col_string = ?
               """,
               ["'a string'"],
               paramstyle="qmark")
print(cursor.fetchall())
```

You can find more information about the [considerations and limitations of parameterized queries](https://docs.aws.amazon.com/athena/latest/ug/querying-with-prepared-statements.html) in the official documentation.

## Quickly re-run queries

### Result reuse configuration

Athena engine version 3 allows you to [reuse the results of previous queries](https://docs.aws.amazon.com/athena/latest/ug/reusing-query-results.html).

It is available by specifying the arguments `result_reuse_enable` and `result_reuse_minutes` in the connection object.

```python
from pyathena import connect

conn = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
               region_name="us-west-2",
               work_group="YOUR_WORK_GROUP",
               result_reuse_enable=True,
               result_reuse_minutes=60)
```

You can also specify `result_reuse_enable` and `result_reuse_minutes` when executing a query.

```python
from pyathena import connect

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
cursor.execute("SELECT * FROM one_row",
               work_group="YOUR_WORK_GROUP",
               result_reuse_enable=True,
               result_reuse_minutes=60)
```

If the following error occurs, please use a workgroup configured with Athena engine version 3.

```text
pyathena.error.DatabaseError: An error occurred (InvalidRequestException) when calling the StartQueryExecution operation: This functionality is not enabled in the selected engine version. Please check the engine version settings or contact AWS support for further assistance.
```

If for some reason you cannot use the reuse feature of Athena engine version 3, please use the {ref}`Cache configuration <cache-configuration>` implemented by PyAthena.

(cache-configuration)=

### Cache configuration

**Please use the Result reuse configuration.**

You can attempt to re-use the results from a previously executed query to help save time and money in the cases where your underlying data isn't changing.
Set the `cache_size` or `cache_expiration_time` parameter of `cursor.execute()` to a number larger than 0 to enable caching.

```python
from pyathena import connect

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
cursor.execute("SELECT * FROM one_row")  # run once
print(cursor.query_id)
cursor.execute("SELECT * FROM one_row", cache_size=10)  # re-use earlier results
print(cursor.query_id)  # You should expect to see the same Query ID
```

The unit of `expiration_time` is seconds. To use the results of queries executed up to one hour ago, specify like the following.

```python
from pyathena import connect

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
cursor.execute("SELECT * FROM one_row", cache_expiration_time=3600)  # Use queries executed within 1 hour as cache.
```

If `cache_size` is not specified, the value of `sys.maxsize` will be automatically set and all query results executed up to one hour ago will be checked.
Therefore, it is recommended to specify `cache_expiration_time` together with `cache_size` like the following.

```python
from pyathena import connect

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
cursor.execute("SELECT * FROM one_row", cache_size=100, cache_expiration_time=3600)  # Use the last 100 queries within 1 hour as cache.
```

Results will only be re-used if the query strings match *exactly*,
and the query was a DML statement (the assumption being that you always want to re-run queries like `CREATE TABLE` and `DROP TABLE`).

The S3 staging directory is not checked, so it's possible that the location of the results is not in your provided `s3_staging_dir`.

(query-execution-callback)=

## Query execution callback

PyAthena provides a callback mechanism that allows you to get immediate access to the query ID
as soon as the `start_query_execution` API call is made, before waiting for query completion.
This is useful for monitoring, logging, or cancelling long-running queries from another thread.

The `on_start_query_execution` callback can be configured at both the connection level and
the execute level. When both are set, both callbacks will be invoked.

### Connection-level callback

You can set a default callback for all queries executed through a connection:

```python
from pyathena import connect

def query_callback(query_id):
    print(f"Query started with ID: {query_id}")
    # You can use query_id for monitoring or cancellation

cursor = connect(
    s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
    region_name="us-west-2",
    on_start_query_execution=query_callback
).cursor()

cursor.execute("SELECT * FROM many_rows")  # Callback will be invoked
```

### Execute-level callback

You can also specify a callback for individual query executions:

```python
from pyathena import connect

def specific_callback(query_id):
    print(f"Specific query started: {query_id}")

cursor = connect(
    s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
    region_name="us-west-2"
).cursor()

cursor.execute(
    "SELECT * FROM many_rows",
    on_start_query_execution=specific_callback
)
```

### Query cancellation example

A common use case is to cancel long-running analytical queries after a timeout:

```python
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pyathena import connect

def cancel_long_running_query():
    """Example: Cancel a complex analytical query after 10 minutes."""

    def track_query_start(query_id):
        print(f"Long-running analysis started: {query_id}")
        return query_id

    def monitor_and_cancel(cursor, timeout_minutes):
        """Monitor query and cancel if it exceeds timeout."""
        time.sleep(timeout_minutes * 60)  # Convert to seconds
        try:
            cursor.cancel()
            print(f"Query cancelled after {timeout_minutes} minutes timeout")
        except Exception as e:
            print(f"Cancellation failed: {e}")

    cursor = connect(
        s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
        region_name="us-west-2",
        on_start_query_execution=track_query_start
    ).cursor()

    # Complex analytical query that might run for a long time
    long_query = """
    WITH daily_metrics AS (
        SELECT
            date_trunc('day', timestamp_col) as day,
            user_id,
            COUNT(*) as events,
            AVG(duration) as avg_duration
        FROM large_events_table
        WHERE timestamp_col >= current_date - interval '1' year
        GROUP BY 1, 2
    ),
    user_segments AS (
        SELECT
            user_id,
            CASE
                WHEN AVG(events) > 100 THEN 'high_activity'
                WHEN AVG(events) > 10 THEN 'medium_activity'
                ELSE 'low_activity'
            END as segment
        FROM daily_metrics
        GROUP BY user_id
    )
    SELECT
        segment,
        COUNT(DISTINCT user_id) as users,
        AVG(events) as avg_daily_events
    FROM daily_metrics dm
    JOIN user_segments us ON dm.user_id = us.user_id
    GROUP BY segment
    ORDER BY avg_daily_events DESC
    """

    # Use ThreadPoolExecutor for timeout management
    with ThreadPoolExecutor(max_workers=1) as executor:
        # Start timeout monitor (cancel after 10 minutes)
        timeout_future = executor.submit(monitor_and_cancel, cursor, 10)

        try:
            print("Starting complex analytical query (10-minute timeout)...")
            cursor.execute(long_query)

            # Process results
            results = cursor.fetchall()
            print(f"Analysis completed successfully: {len(results)} segments found")
            for row in results:
                print(f"  {row[0]}: {row[1]} users, {row[2]:.1f} avg events")

        except Exception as e:
            print(f"Query failed or was cancelled: {e}")
        finally:
            # Clean up timeout monitor
            try:
                timeout_future.result(timeout=1)
            except TimeoutError:
                pass  # Monitor is still running, which is fine

# Run the example
cancel_long_running_query()
```

### Multiple callbacks

When both connection-level and execute-level callbacks are specified,
both callbacks will be invoked:

```python
from pyathena import connect

def connection_callback(query_id):
    print(f"Connection callback: {query_id}")
    # Log to monitoring system

def execute_callback(query_id):
    print(f"Execute callback: {query_id}")
    # Store for cancellation if needed

cursor = connect(
    s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
    region_name="us-west-2",
    on_start_query_execution=connection_callback
).cursor()

# This will invoke both connection_callback and execute_callback
cursor.execute(
    "SELECT 1",
    on_start_query_execution=execute_callback
)
```

### Supported cursor types

The `on_start_query_execution` callback is supported by the following cursor types:

* `Cursor` (default cursor)
* `DictCursor`
* `ArrowCursor`
* `PandasCursor`
* `PolarsCursor`
* `S3FSCursor`

Note: `AsyncCursor` and its variants do not support this callback as they already
return the query ID immediately through their different execution model.

## Environment variables

Support [Boto3 environment variables](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-environment-variables).

### Additional environment variables

AWS_ATHENA_S3_STAGING_DIR
: The S3 location where Athena automatically stores the query results and metadata information. Required if you have not set up workgroups. Not required if a workgroup has been set up.

AWS_ATHENA_WORK_GROUP
: The setting of the workgroup to execute the query.

## Credentials

Support [Boto3 credentials](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html).

### Examples

#### Passing credentials as parameters

```python
from pyathena import connect

cursor = connect(aws_access_key_id="YOUR_ACCESS_KEY_ID",
                 aws_secret_access_key="YOUR_SECRET_ACCESS_KEY",
                 s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
```

```python
from pyathena import connect

cursor = connect(aws_access_key_id="YOUR_ACCESS_KEY_ID",
                 aws_secret_access_key="YOUR_SECRET_ACCESS_KEY",
                 aws_session_token="YOUR_SESSION_TOKEN",
                 s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
```

#### Multi-factor authentication

You will be prompted to enter the MFA code.
The program execution will be blocked until the MFA code is entered.

```python
from pyathena import connect

cursor = connect(duration_seconds=3600,
                 serial_number="arn:aws:iam::ACCOUNT_NUMBER_WITHOUT_HYPHENS:mfa/MFA_DEVICE_ID",
                 s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
```

#### Shared credentials file

The shared credentials file has a default location of ~/.aws/credentials.

If you use the default profile, there is no need to specify credential information.

```python
from pyathena import connect

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
```

You can also specify a profile other than the default.

```python
from pyathena import connect

cursor = connect(profile_name="YOUR_PROFILE_NAME",
                 s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
```

#### Assume role provider

```python
from pyathena import connect

cursor = connect(role_arn="YOUR_ASSUME_ROLE_ARN",
                 role_session_name="PyAthena-session",
                 duration_seconds=3600,
                 s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
```

#### Assume role provider with MFA

You will be prompted to enter the MFA code.
The program execution will be blocked until the MFA code is entered.

```python
from pyathena import connect

cursor = connect(role_arn="YOUR_ASSUME_ROLE_ARN",
                 role_session_name="PyAthena-session",
                 duration_seconds=3600,
                 serial_number="arn:aws:iam::ACCOUNT_NUMBER_WITHOUT_HYPHENS:mfa/MFA_DEVICE_ID",
                 s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
```

#### Instance profiles

No need to specify credential information.

```python
from pyathena import connect

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
```
