# -*- coding: utf-8 -*-
import textwrap

import pytest

from pyathena.error import DatabaseError, OperationalError
from pyathena.model import AthenaCalculationExecutionStatus
from tests import ENV


class TestAioSparkCursor:
    async def test_spark_dataframe(self, aio_spark_cursor):
        await aio_spark_cursor.execute(
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
        assert aio_spark_cursor.calculation_execution
        assert aio_spark_cursor.session_id
        assert aio_spark_cursor.calculation_id
        assert aio_spark_cursor.description == "test description"
        assert aio_spark_cursor.working_directory
        assert aio_spark_cursor.state == AthenaCalculationExecutionStatus.STATE_COMPLETED
        assert aio_spark_cursor.state_change_reason is None
        assert aio_spark_cursor.submission_date_time
        assert aio_spark_cursor.completion_date_time
        assert aio_spark_cursor.dpu_execution_in_millis
        assert aio_spark_cursor.progress
        assert aio_spark_cursor.std_out_s3_uri
        assert aio_spark_cursor.std_error_s3_uri
        assert aio_spark_cursor.result_s3_uri
        assert aio_spark_cursor.result_type

        await aio_spark_cursor.execute(
            textwrap.dedent(
                """
                from pyspark.sql.functions import sum
                df_count = df.groupBy("name").agg(sum("count").alias("sum"))
                df_count.show()
                """
            )
        )
        assert (
            await aio_spark_cursor.get_std_out()
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

        await aio_spark_cursor.execute(
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
    async def test_spark_sql(self, aio_spark_cursor):
        await aio_spark_cursor.execute(
            textwrap.dedent(
                f"""
                spark.sql("SELECT * FROM {ENV.schema}.one_row").show()
                """
            )
        )
        assert (
            await aio_spark_cursor.get_std_out()
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

        await aio_spark_cursor.execute(
            textwrap.dedent(
                f"""
                spark.sql("DROP TABLE IF EXISTS {ENV.schema}.spark_group_by")
                """
            )
        )

    async def test_failed(self, aio_spark_cursor):
        with pytest.raises(OperationalError):
            await aio_spark_cursor.execute(
                textwrap.dedent(
                    """
                    foobar
                    """
                )
            )
        assert aio_spark_cursor.state == AthenaCalculationExecutionStatus.STATE_FAILED
        assert (
            await aio_spark_cursor.get_std_error()
            == textwrap.dedent(
                """
                File "<stdin>", line 2, in <module>
                NameError: name 'foobar' is not defined
                """
            ).strip()
        )

    async def test_cancel(self, aio_spark_cursor):
        import asyncio

        async def cancel_after_delay(c):
            await asyncio.sleep(5)
            await c.cancel()
            await c.close()

        task = asyncio.create_task(cancel_after_delay(aio_spark_cursor))

        with pytest.raises(DatabaseError):
            await aio_spark_cursor.execute(
                textwrap.dedent(
                    """
                    import time
                    time.sleep(60)
                    """
                )
            )

        await task
