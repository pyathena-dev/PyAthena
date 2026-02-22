import textwrap
import time
from concurrent.futures import ThreadPoolExecutor
from random import randint

import pytest

from pyathena import DatabaseError, OperationalError
from pyathena.model import AthenaCalculationExecutionStatus
from tests import ENV


class TestSparkCursor:
    def test_spark_dataframe(self, spark_cursor):
        spark_cursor.execute(
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
        assert spark_cursor.calculation_execution
        assert spark_cursor.session_id
        assert spark_cursor.calculation_id
        assert spark_cursor.description == "test description"
        assert spark_cursor.working_directory
        assert spark_cursor.state == AthenaCalculationExecutionStatus.STATE_COMPLETED
        assert spark_cursor.state_change_reason is None
        assert spark_cursor.submission_date_time
        assert spark_cursor.completion_date_time
        assert spark_cursor.dpu_execution_in_millis
        assert spark_cursor.progress
        assert spark_cursor.std_out_s3_uri
        assert spark_cursor.std_error_s3_uri
        assert spark_cursor.result_s3_uri
        assert spark_cursor.result_type

        spark_cursor.execute(
            textwrap.dedent(
                """
                from pyspark.sql.functions import sum
                df_count = df.groupBy("name").agg(sum("count").alias("sum"))
                df_count.show()
                """
            )
        )
        assert (
            spark_cursor.get_std_out()
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

        spark_cursor.execute(
            textwrap.dedent(
                f"""
                df_count.write.mode('overwrite') \\
                    .format("parquet") \\
                    .option("path", "{ENV.s3_staging_dir}{ENV.schema}/spark/group_by") \\
                    .saveAsTable("{ENV.schema}.spark_group_by")
                """
            )
        )

    @pytest.mark.dependency(depends="test_spark_dataframe")
    def test_spark_sql(self, spark_cursor):
        spark_cursor.execute(
            textwrap.dedent(
                f"""
                spark.sql("SELECT * FROM {ENV.schema}.one_row").show()
                """
            )
        )
        assert (
            spark_cursor.get_std_out()
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

        spark_cursor.execute(
            textwrap.dedent(
                f"""
                spark.sql("DROP TABLE IF EXISTS {ENV.schema}.spark_group_by")
                """
            )
        )

    def test_failed(self, spark_cursor):
        with pytest.raises(OperationalError):
            spark_cursor.execute(
                textwrap.dedent(
                    """
                    foobar
                    """
                )
            )
        assert spark_cursor.state == AthenaCalculationExecutionStatus.STATE_FAILED
        assert (
            spark_cursor.get_std_error()
            == textwrap.dedent(
                """
                File "<stdin>", line 2, in <module>
                NameError: name 'foobar' is not defined
                """
            ).strip()
        )

    def test_cancel(self, spark_cursor):
        def cancel(c):
            time.sleep(randint(5, 10))
            c.cancel()

            # TODO: Calculation execution is not canceled unless session is terminated
            c.close()

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(cancel, spark_cursor)

            pytest.raises(
                DatabaseError,
                lambda: spark_cursor.execute(
                    textwrap.dedent(
                        """
                        import time
                        time.sleep(60)
                        """
                    )
                ),
            )
