(spark)=

# Spark

(spark-cursor)=

## SparkCursor

SparkCursor can run Spark applications in Athena.
This cursor does not follow the [DB API 2.0 (PEP 249)](https://www.python.org/dev/peps/pep-0249/). It does not support result set iteration.

To use this cursor, you must [create a Spark enabled workgroup in Athena](https://docs.aws.amazon.com/athena/latest/ug/notebooks-spark-getting-started.html) and use that workgroup.

You can use the SparkCursor by specifying the `cursor_class`
with the connect method or connection object.

```python
from pyathena import connect
from pyathena.spark.cursor import SparkCursor

cursor = connect(region_name="us-west-2",
                 work_group="YOUR_SPARK_WORKGROUP",
                 cursor_class=SparkCursor).cursor()
```

```python
from pyathena.connection import Connection
from pyathena.spark.cursor import SparkCursor

cursor = Connection(region_name="us-west-2",
                    work_group="YOUR_SPARK_WORKGROUP",
                    cursor_class=SparkCursor).cursor()
```

It can also be used by specifying the cursor class when calling the connection object's cursor method.

```python
from pyathena import connect
from pyathena.spark.cursor import SparkCursor

cursor = connect(region_name="us-west-2"
                 work_group="YOUR_SPARK_WORKGROUP").cursor(SparkCursor)
```

```python
from pyathena.connection import Connection
from pyathena.spark.cursor import SparkCursor

cursor = Connection(region_name="us-west-2"
                    work_group="YOUR_SPARK_WORKGROUP").cursor(SparkCursor)
```

This cursor allows you to send PySpark code blocks and use Spark DataFrame and SQL.

### Session lifecycle

If session_id is not specified as an argument when creating a Spark cursor, it will start a new session;
if session_id is specified, it will check if the session is idle.

The session idle timeout minutes can be specified with the `session_idle_timeout_minutes` argument when creating
the cursor and the engine DPU and Spark properties can also be specified with the `engine_configuration` argument.

```python
from pyathena import connect
from pyathena.spark.cursor import SparkCursor

conn = connect(work_group="YOUR_SPARK_WORKGROUP",
               cursor_class=SparkCursor)
cursor = conn.cursor(session_idle_timeout_minutes=60,
                     engine_configuration={
                         "CoordinatorDpuSize": 1,
                         "MaxConcurrentDpus": 20,
                         "DefaultExecutorDpuSize": 1,
                         "AdditionalConfigs": {"string": "string"},
                         "SparkProperties": {"string": "string"},
                     })
```

The session is not terminated until the close method of the cursor is called.
You can use the context manager to automatically call the close method.

```python
from pyathena import connect
from pyathena.spark.cursor import SparkCursor

conn = connect(work_group="YOUR_SPARK_WORKGROUP",
               cursor_class=SparkCursor)
with conn.cursor() as cursor:
    cursor.execute("...")
    ...
```

### Spark DataFrames

The Spark DataFrames code in the sample notebook that can be enabled
when creating a workgroup can be executed as follows:

```python
import textwrap
from pyathena import connect
from pyathena.spark.cursor import SparkCursor

conn = connect(work_group="YOUR_SPARK_WORKGROUP", cursor_class=SparkCursor)
with conn.cursor() as cursor:
    cursor.execute(
        textwrap.dedent(
            """
            file_name = "s3://athena-examples-us-east-1/notebooks/yellow_tripdata_2016-01.parquet"

            taxi_df = (spark.read.format("parquet")
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .load(file_name))
            """
        )
    )

    cursor.execute(
        textwrap.dedent(
            """
            taxi1_df=taxi_df.groupBy("VendorID", "passenger_count").count()
            taxi1_df.show()
            """
        )
    )
    print(cursor.get_std_out())

    cursor.execute(
        textwrap.dedent(
            """
            taxi1_df.coalesce(1).write.mode('overwrite').csv("s3://YOUR_S3_BUCKET/select_taxi")
            print("Write to s3 " + "complete")
            """
        )
    )
    print(cursor.get_std_out())
```

The standard output and standard error of a spark application can be retrieved
with the `get_std_out()` and `get_std_error()` methods in the cursor class.

### Spark SQL

The Spark SQL code in the sample notebook can be executed as follows:

```python
import textwrap
from pyathena import connect
from pyathena.spark.cursor import SparkCursor

conn = connect(work_group="YOUR_SPARK_WORKGROUP", cursor_class=SparkCursor)
with conn.cursor() as cursor:
    cursor.execute(
        textwrap.dedent(
            """
            file_name = "s3://athena-examples-us-east-1/notebooks/yellow_tripdata_2016-01.parquet"

            taxi_df = (spark.read.format("parquet")
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .load(file_name))
            taxi_df.createOrReplaceTempView("taxis")

            sqlDF = spark.sql("SELECT DOLocationID, sum(total_amount) as sum_total_amount FROM taxis where DOLocationID < 25 GRoup by DOLocationID ORDER BY DOLocationID")
            sqlDF.show(50)
            """
        )
    )
    print(cursor.get_std_out())

    cursor.execute(
        textwrap.dedent(
            """
            spark.sql("create database if not exists spark_demo_database")
            spark.sql("show databases").show()
            """
        )
    )
    print(cursor.get_std_out())

    cursor.execute(
        textwrap.dedent(
            """
            spark.sql("use spark_demo_database")
            taxi1_df=taxi_df.groupBy("VendorID", "passenger_count").count()
            taxi1_df.write.mode("overwrite").format("parquet").option("path","s3://YOUR_S3_BUCKET/select_taxi").saveAsTable("select_taxi_table")
            print("Create new table" + " complete")
            """
        )
    )
    print(cursor.get_std_out())

    cursor.execute(
        textwrap.dedent(
            """
            spark.sql("show tables").show()
            """
        )
    )
    print(cursor.get_std_out())

    cursor.execute(
        textwrap.dedent(
            """
            spark.sql("select * from select_taxi_table").show()
            """
        )
    )
    print(cursor.get_std_out())
```

(async-spark-cursor)=

## AsyncSparkCursor

AsyncSparkCursor is an AsyncCursor that can handle Spark applications.

You can use the AsyncSparkCursor by specifying the `cursor_class`
with the connect method or connection object.

```python
from pyathena import connect
from pyathena.spark.async_cursor import AsyncSparkCursor

cursor = connect(region_name="us-west-2",
                 work_group="YOUR_SPARK_WORKGROUP",
                 cursor_class=AsyncSparkCursor).cursor()
```

```python
from pyathena.connection import Connection
from pyathena.spark.async_cursor import AsyncSparkCursor

cursor = Connection(region_name="us-west-2",
                    work_group="YOUR_SPARK_WORKGROUP",
                    cursor_class=AsyncSparkCursor).cursor()
```

It can also be used by specifying the cursor class when calling the connection object's cursor method.

```python
from pyathena import connect
from pyathena.spark.async_cursor import AsyncSparkCursor

cursor = connect(region_name="us-west-2"
                 work_group="YOUR_SPARK_WORKGROUP").cursor(AsyncSparkCursor)
```

```python
from pyathena.connection import Connection
from pyathena.spark.async_cursor import AsyncSparkCursor

cursor = Connection(region_name="us-west-2"
                    work_group="YOUR_SPARK_WORKGROUP").cursor(AsyncSparkCursor)
```

The default number of workers is 5 or cpu number * 5.
If you want to change the number of workers you can specify like the following.

```python
from pyathena import connect
from pyathena.spark.async_cursor import AsyncSparkCursor

cursor = connect(region_name="us-west-2",
                 work_group="YOUR_SPARK_WORKGROUP",
                 cursor_class=AsyncSparkCursor).cursor(max_workers=10)
```

The execute method of the AsyncSparkCursor returns the tuple of the calculation ID and the [future object](https://docs.python.org/3/library/concurrent.futures.html#future-objects).

```python
from pyathena import connect
from pyathena.spark.async_cursor import AsyncSparkCursor

conn = connect(work_group="YOUR_SPARK_WORKGROUP", cursor_class=AsyncSparkCursor)
with conn.cursor() as cursor:
    calculation_id, future = cursor.execute("""spark.sql("SELECT * FROM many_rows")""")
```

The return value of the [future object](https://docs.python.org/3/library/concurrent.futures.html#future-objects) is an `AthenaCalculationExecution` object.

```python
from pyathena import connect
from pyathena.spark.async_cursor import AsyncSparkCursor

conn = connect(work_group="YOUR_SPARK_WORKGROUP", cursor_class=AsyncSparkCursor)
with conn.cursor() as cursor:
    calculation_id, future = cursor.execute("""spark.sql("SELECT * FROM many_rows")""")
    calculation_execution = future.result()
    print(calculation_execution.session_id)
    print(calculation_execution.calculation_id)
    print(calculation_execution.description)
    print(calculation_execution.working_directory)
    print(calculation_execution.state)
    print(calculation_execution.state_change_reason)
    print(calculation_execution.submission_date_time)
    print(calculation_execution.completion_date_time)
    print(calculation_execution.dpu_execution_in_millis)
    print(calculation_execution.progress)
    print(calculation_execution.std_out_s3_uri)
    print(calculation_execution.std_error_s3_uri)
    print(calculation_execution.result_s3_uri)
    print(calculation_execution.result_type)
```

Standard output and standard error can be retrieved by passing this object to the cursor class.

```python
from pyathena import connect
from pyathena.spark.async_cursor import AsyncSparkCursor

conn = connect(work_group="YOUR_SPARK_WORKGROUP", cursor_class=AsyncSparkCursor)
with conn.cursor() as cursor:
    calculation_id, future = cursor.execute("""spark.sql("SELECT * FROM many_rows")""")
    calculation_execution = future.result()
    print(cursor.get_std_out(calculation_execution).result())
    print(cursor.get_std_error(calculation_execution).result())
```

As with AsyncSparkCursor, you need a calculation ID to cancel a calculation.

```python
from pyathena import connect
from pyathena.spark.async_cursor import AsyncSparkCursor

conn = connect(work_group="YOUR_SPARK_WORKGROUP", cursor_class=AsyncSparkCursor)
with conn.cursor() as cursor:
    calculation_id, future = cursor.execute("""spark.sql("SELECT * FROM many_rows")""")
    cursor.cancel(calculation_id)  # The cancel method future object returns nothing.
    # It is better not to get the result of cursor execution.
    # Because it will be blocked until the session is terminated.
    # future.result()
```

NOTE: Currently it appears that the calculation is not canceled unless the session is terminated.
