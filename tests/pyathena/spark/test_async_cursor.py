# Copyright 2024 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import textwrap
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

import pytest

from pyathena import OperationalError
from pyathena.model import AthenaCalculationExecutionStatus, AthenaSessionStatus
from pyathena.spark.async_cursor import AsyncSparkCursor
from tests import ENV
from tests.pyathena.util import (
    CANCELABLE_SPARK_JOB,
    wait_for_spark_job,
    wait_for_spark_session_state,
)

# Bounds how long the executor test task blocks when nothing releases it.
_TIMEOUT = 10


def _owned_session_cursor() -> AsyncSparkCursor:
    """An AsyncSparkCursor that started its own session, without AWS calls.

    Returns:
        The cursor, whose ``close()`` terminates the session.
    """
    cursor = AsyncSparkCursor.__new__(AsyncSparkCursor)  # bypass __init__ to avoid AWS calls
    cursor._owns_session = True
    cursor._terminate_session_on_close = None
    cursor._session_terminated = False
    return cursor


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
        query_id, future = async_spark_cursor.execute(CANCELABLE_SPARK_JOB)
        wait_for_spark_job(async_spark_cursor.connection.client, query_id)
        async_spark_cursor.cancel(query_id).result()
        calculation_execution = future.result()
        assert calculation_execution.state == AthenaCalculationExecutionStatus.STATE_CANCELED

        # Canceling a calculation leaves the session usable.
        query_id, future = async_spark_cursor.execute("print(1)")
        calculation_execution = future.result()
        assert calculation_execution.state == AthenaCalculationExecutionStatus.STATE_COMPLETED
        assert async_spark_cursor.get_std_out(calculation_execution).result() == "1"
        # Canceling a completed calculation does not change its state.
        async_spark_cursor.cancel(query_id).result()
        assert (
            async_spark_cursor.calculation_execution(query_id).result().state
            == AthenaCalculationExecutionStatus.STATE_COMPLETED
        )

    def test_session_ownership(self, async_spark_cursor):
        client = async_spark_cursor.connection.client
        session_id = async_spark_cursor.session_id
        with async_spark_cursor.connection.cursor(
            AsyncSparkCursor, session_id=session_id
        ) as borrower:
            _, future = borrower.execute("print(1)")
            calculation_execution = future.result()
            assert borrower.get_std_out(calculation_execution).result() == "1"

        # Closing a cursor that was given the session leaves the session running.
        _, future = async_spark_cursor.execute("print(2)")
        calculation_execution = future.result()
        assert async_spark_cursor.get_std_out(calculation_execution).result() == "2"

        # Closing the cursor that started the session terminates it.
        async_spark_cursor.close()
        wait_for_spark_session_state(client, session_id, AthenaSessionStatus.STATE_TERMINATED)

    @staticmethod
    def _cursor_with_submitted_work():
        """Build a cursor whose executor holds one running and one queued future.

        Returns:
            A tuple of the cursor, the event that releases the running future,
            the running future, and the queued future.
        """
        cursor = _owned_session_cursor()
        cursor._executor = MagicMock(wraps=ThreadPoolExecutor(max_workers=1))
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
        cursor._executor.shutdown.assert_called_once_with(wait=True)
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
            cursor._executor.shutdown.assert_called_once_with(wait=False)
            with pytest.raises(RuntimeError):
                cursor._executor.submit(lambda: None)
        finally:
            release.set()
        assert running.result(_TIMEOUT) == "running"
        assert queued.result(_TIMEOUT) == "queued"

    def test_close_retries_termination_after_failure(self):
        cursor = _owned_session_cursor()
        cursor._terminate_session = MagicMock(
            side_effect=[OperationalError("termination failed"), None]
        )
        cursor._executor = ThreadPoolExecutor(max_workers=1)

        with pytest.raises(OperationalError, match="termination failed"):
            cursor.close()
        cursor.close()

        assert cursor._terminate_session.call_count == 2
