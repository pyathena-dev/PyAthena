# Copyright 2024 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import textwrap
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

import pytest

from pyathena import OperationalError
from pyathena.model import AthenaCalculationExecutionStatus
from pyathena.spark.cursor import SparkCursor
from tests import ENV
from tests.pyathena.util import CANCELABLE_SPARK_JOB, wait_for_spark_job


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
            for _ in range(60):
                if c.calculation_id:
                    break
                time.sleep(1)
            wait_for_spark_job(c.connection.client, c.calculation_id)
            c.cancel()

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(cancel, spark_cursor)
            with pytest.raises(OperationalError):
                spark_cursor.execute(CANCELABLE_SPARK_JOB)
            future.result()
        assert spark_cursor.state == AthenaCalculationExecutionStatus.STATE_CANCELED

        # Canceling a calculation leaves the session usable.
        spark_cursor.execute("print(1)")
        assert spark_cursor.get_std_out() == "1"
        # Canceling a completed calculation does not change its state.
        spark_cursor.cancel()
        assert spark_cursor.state == AthenaCalculationExecutionStatus.STATE_COMPLETED

    @pytest.mark.parametrize(
        "final_state",
        [
            AthenaCalculationExecutionStatus.STATE_CANCELED,
            AthenaCalculationExecutionStatus.STATE_COMPLETED,
        ],
    )
    def test_execute_kill_on_interrupt(self, final_state):
        """An interrupt cancels the calculation, waits for it, and is re-raised (no AWS)."""
        final_execution = MagicMock(state=final_state)
        cursor = SparkCursor.__new__(SparkCursor)  # bypass __init__ to avoid AWS calls
        cursor._session_id = "session_id"
        cursor._poll_interval = 0
        cursor._kill_on_interrupt = True
        cursor._on_poll = None
        cursor._calculation_execution = None

        with (
            patch.object(SparkCursor, "_calculate", return_value="calculation_id"),
            patch.object(
                SparkCursor,
                "_get_calculation_execution_status",
                side_effect=[
                    KeyboardInterrupt(),
                    MagicMock(state=AthenaCalculationExecutionStatus.STATE_RUNNING),
                    MagicMock(state=final_state),
                ],
            ),
            patch.object(SparkCursor, "_get_calculation_execution", return_value=final_execution),
            patch.object(SparkCursor, "_cancel") as cancel,
            pytest.raises(KeyboardInterrupt),
        ):
            cursor.execute("code")

        cancel.assert_called_once_with("calculation_id")
        assert cursor.calculation_execution is final_execution
        assert cursor.state == final_state

    def test_execute_interrupt_without_kill_on_interrupt(self):
        """Without kill_on_interrupt, an interrupt propagates without cancellation (no AWS)."""
        cursor = SparkCursor.__new__(SparkCursor)  # bypass __init__ to avoid AWS calls
        cursor._session_id = "session_id"
        cursor._poll_interval = 0
        cursor._kill_on_interrupt = False
        cursor._on_poll = None
        cursor._calculation_execution = None

        with (
            patch.object(SparkCursor, "_calculate", return_value="calculation_id"),
            patch.object(
                SparkCursor, "_get_calculation_execution_status", side_effect=KeyboardInterrupt()
            ),
            patch.object(SparkCursor, "_cancel") as cancel,
            pytest.raises(KeyboardInterrupt),
        ):
            cursor.execute("code")

        cancel.assert_not_called()
        assert cursor.calculation_execution is None


def test_spark_on_poll_invoked_each_iteration():
    """on_poll fires once per Spark calculation poll iteration with the status (no AWS)."""
    states = [
        AthenaCalculationExecutionStatus.STATE_CREATING,
        AthenaCalculationExecutionStatus.STATE_RUNNING,
        AthenaCalculationExecutionStatus.STATE_COMPLETED,
    ]
    statuses = [MagicMock(state=state) for state in states]
    final_execution = MagicMock()
    received = []

    cursor = SparkCursor.__new__(SparkCursor)  # bypass __init__ to avoid AWS calls
    cursor._poll_interval = 0
    cursor._kill_on_interrupt = False
    cursor._on_poll = received.append

    with (
        patch.object(SparkCursor, "_get_calculation_execution_status", side_effect=statuses),
        patch.object(SparkCursor, "_get_calculation_execution", return_value=final_execution),
    ):
        result = cursor._poll("calculation_id")

    assert [status.state for status in received] == states
    assert result is final_execution
