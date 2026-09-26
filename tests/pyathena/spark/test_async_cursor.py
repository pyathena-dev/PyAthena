# Copyright 2024 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import textwrap
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from random import randint
from unittest.mock import MagicMock

import pytest

from pyathena import OperationalError
from pyathena.model import AthenaCalculationExecutionStatus
from pyathena.spark.async_cursor import AsyncSparkCursor
from tests import ENV

_TIMEOUT = 10


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

    @staticmethod
    def _cursor_with_submitted_work():
        """Build a cursor whose executor holds one running and one queued future.

        Returns:
            A tuple of the cursor, the event that releases the running future,
            the running future, and the queued future.
        """
        cursor = AsyncSparkCursor.__new__(AsyncSparkCursor)  # bypass __init__ to avoid AWS calls
        cursor._executor = ThreadPoolExecutor(max_workers=1)
        started = threading.Event()
        release = threading.Event()

        def run():
            started.set()
            release.wait(_TIMEOUT)
            return "running"

        running = cursor._executor.submit(run)
        queued = cursor._executor.submit(lambda: "queued")
        assert started.wait(_TIMEOUT)
        return cursor, release, running, queued

    @pytest.mark.parametrize("fails", [False, True])
    def test_close_wait_shuts_down_executor(self, fails):
        cursor, release, running, queued = self._cursor_with_submitted_work()

        def terminate():
            # Let the running future finish so that shutdown(wait=True) returns.
            release.set()
            if fails:
                raise OperationalError("termination failed")

        cursor._terminate_session = MagicMock(side_effect=terminate)
        if fails:
            with pytest.raises(OperationalError, match="termination failed"):
                cursor.close(wait=True)
        else:
            cursor.close(wait=True)

        cursor._terminate_session.assert_called_once_with()
        assert running.done()
        assert running.result() == "running"
        assert queued.done()
        assert queued.result() == "queued"
        with pytest.raises(RuntimeError):
            cursor._executor.submit(lambda: None)

    @pytest.mark.parametrize("fails", [False, True])
    def test_close_no_wait_shuts_down_executor(self, fails):
        cursor, release, running, queued = self._cursor_with_submitted_work()
        cursor._terminate_session = MagicMock(
            side_effect=OperationalError("termination failed") if fails else None
        )
        try:
            if fails:
                with pytest.raises(OperationalError, match="termination failed"):
                    cursor.close(wait=False)
            else:
                cursor.close(wait=False)

            cursor._terminate_session.assert_called_once_with()
            assert not running.done()
            with pytest.raises(RuntimeError):
                cursor._executor.submit(lambda: None)
        finally:
            release.set()
        assert running.result(_TIMEOUT) == "running"
        assert queued.result(_TIMEOUT) == "queued"

    def test_close_retries_termination_after_failure(self):
        cursor = AsyncSparkCursor.__new__(AsyncSparkCursor)  # bypass __init__ to avoid AWS calls
        cursor._terminate_session = MagicMock(
            side_effect=[OperationalError("termination failed"), None]
        )
        cursor._executor = ThreadPoolExecutor(max_workers=1)

        with pytest.raises(OperationalError, match="termination failed"):
            cursor.close()
        cursor.close()

        assert cursor._terminate_session.call_count == 2
