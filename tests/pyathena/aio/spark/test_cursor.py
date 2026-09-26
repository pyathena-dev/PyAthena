# Copyright 2024 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import asyncio
import textwrap
from unittest.mock import AsyncMock, MagicMock

import pytest

from pyathena.aio.spark.cursor import AioSparkCursor
from pyathena.error import NotSupportedError, OperationalError
from pyathena.model import AthenaCalculationExecutionStatus
from tests import ENV
from tests.pyathena.aio.conftest import _aio_connect
from tests.pyathena.util import CANCELABLE_SPARK_JOB, wait_for_spark_job


def _offline_cursor(kill_on_interrupt, final_state):
    """An AioSparkCursor whose first status request blocks until the task is cancelled.

    Args:
        kill_on_interrupt: Whether the cursor cancels the calculation on cancellation.
        final_state: The state of the calculation after cancellation.

    Returns:
        The cursor, the mock of its cancellation request, and an event set when the
        first status request starts.
    """
    polling = asyncio.Event()

    async def get_status(query_id):
        if not polling.is_set():
            polling.set()
            await asyncio.Event().wait()
        return MagicMock(state=final_state)

    cursor = AioSparkCursor.__new__(AioSparkCursor)  # bypass __init__ to avoid AWS calls
    cursor._session_id = "session_id"
    cursor._poll_interval = 0
    cursor._kill_on_interrupt = kill_on_interrupt
    cursor._on_poll = None
    cursor._calculation_execution = None
    cursor._calculate = AsyncMock(return_value="calculation_id")
    cursor._get_calculation_execution_status = get_status
    cursor._get_calculation_execution = AsyncMock(return_value=MagicMock(state=final_state))
    cancel = cursor._cancel = AsyncMock()
    return cursor, cancel, polling


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
        async def wait_for_job(previous_calculation_id=None):
            for _ in range(60):
                if aio_spark_cursor.calculation_id not in (None, previous_calculation_id):
                    break
                await asyncio.sleep(1)
            await asyncio.to_thread(
                wait_for_spark_job,
                aio_spark_cursor.connection.client,
                aio_spark_cursor.calculation_id,
            )

        task = asyncio.create_task(aio_spark_cursor.execute(CANCELABLE_SPARK_JOB))
        await wait_for_job()
        await aio_spark_cursor.cancel()
        with pytest.raises(OperationalError):
            await task
        assert aio_spark_cursor.state == AthenaCalculationExecutionStatus.STATE_CANCELED

        # Cancelling the task cancels the calculation and re-raises the cancellation.
        canceled_calculation_id = aio_spark_cursor.calculation_id
        task = asyncio.create_task(aio_spark_cursor.execute(CANCELABLE_SPARK_JOB))
        await wait_for_job(canceled_calculation_id)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert aio_spark_cursor.state == AthenaCalculationExecutionStatus.STATE_CANCELED

        # Canceling a calculation leaves the session usable.
        await aio_spark_cursor.execute("print(1)")
        assert await aio_spark_cursor.get_std_out() == "1"
        # Canceling a completed calculation does not change its state.
        await aio_spark_cursor.cancel()
        assert aio_spark_cursor.state == AthenaCalculationExecutionStatus.STATE_COMPLETED

    @pytest.mark.parametrize(
        "final_state",
        [
            AthenaCalculationExecutionStatus.STATE_CANCELED,
            AthenaCalculationExecutionStatus.STATE_COMPLETED,
        ],
    )
    async def test_execute_kill_on_interrupt(self, final_state):
        """Task cancellation cancels the calculation, waits for it, and is re-raised (no AWS)."""
        cursor, cancel, polling = _offline_cursor(kill_on_interrupt=True, final_state=final_state)
        task = asyncio.create_task(cursor.execute("code"))
        await polling.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert task.cancelled()
        cancel.assert_awaited_once_with("calculation_id")
        assert cursor.state == final_state

    @pytest.mark.parametrize("failing", ["cancel", "wait"])
    async def test_execute_kill_on_interrupt_failure(self, failing):
        """A failure to cancel or wait becomes the cause of the cancellation (no AWS)."""
        error = OperationalError("failed")
        cursor, cancel, _ = _offline_cursor(
            kill_on_interrupt=True,
            final_state=AthenaCalculationExecutionStatus.STATE_COMPLETED,
        )
        # Raise the cancellation from the first status request directly, so that the
        # test receives the re-raised exception itself rather than one made by a task.
        cursor._get_calculation_execution_status = AsyncMock(
            side_effect=[asyncio.CancelledError(), error]
        )
        if failing == "cancel":
            cancel.side_effect = error
        with pytest.raises(asyncio.CancelledError) as exc_info:
            await cursor.execute("code")

        assert exc_info.value.__cause__ is error
        cancel.assert_awaited_once_with("calculation_id")
        assert cursor.calculation_execution is None

    async def test_execute_cancellation_without_kill_on_interrupt(self):
        """Without kill_on_interrupt, task cancellation propagates without cancel (no AWS)."""
        cursor, cancel, polling = _offline_cursor(
            kill_on_interrupt=False,
            final_state=AthenaCalculationExecutionStatus.STATE_COMPLETED,
        )
        task = asyncio.create_task(cursor.execute("code"))
        await polling.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert task.cancelled()
        cancel.assert_not_awaited()
        assert cursor.calculation_execution is None

    async def test_executemany(self, aio_spark_cursor):
        with pytest.raises(NotSupportedError):
            await aio_spark_cursor.executemany("SELECT 1", [])

    async def test_context_manager(self):
        conn = await _aio_connect(
            schema_name=ENV.schema,
            cursor_class=AioSparkCursor,
            work_group=ENV.spark_work_group,
        )
        cursor = await asyncio.to_thread(conn.cursor)
        async with cursor:
            await cursor.execute("print('hello')")
            assert await cursor.get_std_out() == "hello"
