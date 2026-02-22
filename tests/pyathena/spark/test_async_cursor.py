import textwrap
import time
from random import randint

from pyathena.model import AthenaCalculationExecutionStatus
from tests import ENV


class TestAsyncSparkCursor:
    def test_spark_dataframe(self, async_spark_cursor):
        query_id, future = async_spark_cursor.execute(
            textwrap.dedent(
                f"""
                df = spark.read.format("csv") \\
                    .option("header", "true") \\
                    .option("inferSchema", "true") \\
                    .load("{ENV.s3_staging_dir}{ENV.schema}/spark_group_by/spark_group_by.csv")
                """
            ),
            description="test description",
        )
        calculation_execution = future.result()
        assert calculation_execution.session_id
        assert query_id == calculation_execution.calculation_id
        assert calculation_execution.description == "test description"
        assert calculation_execution.working_directory
        assert calculation_execution.state == AthenaCalculationExecutionStatus.STATE_COMPLETED
        assert calculation_execution.state_change_reason is None
        assert calculation_execution.submission_date_time
        assert calculation_execution.completion_date_time
        assert calculation_execution.dpu_execution_in_millis
        assert calculation_execution.progress
        assert calculation_execution.std_out_s3_uri
        assert calculation_execution.std_error_s3_uri
        assert calculation_execution.result_s3_uri
        assert calculation_execution.result_type

        query_id, future = async_spark_cursor.execute(
            textwrap.dedent(
                """
                from pyspark.sql.functions import sum
                df_count = df.groupBy("name").agg(sum("count").alias("sum"))
                df_count.show()
                """
            )
        )
        calculation_execution = future.result()
        std_out = async_spark_cursor.get_std_out(calculation_execution).result()
        assert (
            std_out
            == textwrap.dedent(
                """
                +----+---+
                |name|sum|
                +----+---+
                | bar|  5|
                | foo|  5|
                +----+---+
                """
            ).strip()
        )

    def test_spark_sql(self, async_spark_cursor):
        query_id, future = async_spark_cursor.execute(
            textwrap.dedent(
                f"""
                spark.sql("SELECT * FROM {ENV.schema}.one_row").show()
                """
            )
        )
        calculation_execution = future.result()
        std_out = async_spark_cursor.get_std_out(calculation_execution).result()
        assert (
            std_out
            == textwrap.dedent(
                """
                +--------------+
                |number_of_rows|
                +--------------+
                |             1|
                +--------------+
                """
            ).strip()
        )

    def test_failed(self, async_spark_cursor):
        query_id, future = async_spark_cursor.execute(
            textwrap.dedent(
                """
                foobar
                """
            )
        )
        calculation_execution = future.result()
        assert calculation_execution.state == AthenaCalculationExecutionStatus.STATE_FAILED
        std_error = async_spark_cursor.get_std_error(calculation_execution).result()
        assert (
            std_error
            == textwrap.dedent(
                """
                File "<stdin>", line 2, in <module>
                NameError: name 'foobar' is not defined
                """
            ).strip()
        )

    def test_cancel(self, async_spark_cursor):
        query_id, future = async_spark_cursor.execute(
            textwrap.dedent(
                """
                import time
                time.sleep(60)
                """
            )
        )
        time.sleep(randint(5, 10))
        async_spark_cursor.cancel(query_id).result()

        # TODO: Calculation execution is not canceled unless session is terminated
        async_spark_cursor.close()

        calculation_execution = future.result()
        assert calculation_execution.state == AthenaCalculationExecutionStatus.STATE_CANCELED
