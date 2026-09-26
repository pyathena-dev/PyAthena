# Copyright 2024 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from __future__ import annotations

import logging
from typing import Any, cast

from pyathena import OperationalError, ProgrammingError
from pyathena.model import AthenaCalculationExecution, AthenaCalculationExecutionStatus
from pyathena.spark.common import SparkBaseCursor, WithCalculationExecution

_logger = logging.getLogger(__name__)


class SparkCursor(SparkBaseCursor, WithCalculationExecution):
    """Cursor for executing PySpark code on Amazon Athena for Apache Spark.

    This cursor allows you to execute PySpark code directly on Athena's managed
    Spark environment. It's designed for big data processing, ETL operations,
    and machine learning workloads that require Spark's distributed computing
    capabilities.

    The cursor manages Spark sessions automatically and provides an interface
    similar to other PyAthena cursors but optimized for Spark calculations
    rather than SQL queries.

    Attributes:
        session_id: The Athena Spark session ID.
        description: Optional description for the Spark session.
        engine_configuration: Spark engine configuration settings.
        calculation_id: ID of the current calculation being executed.

    Example:
        >>> from pyathena.spark.cursor import SparkCursor
        >>> cursor = connection.cursor(SparkCursor)
        >>>
        >>> # Execute PySpark code
        >>> spark_code = '''
        ... df = spark.read.table("my_database.my_table")
        ... result = df.groupBy("category").count()
        ... result.show()
        ... '''
        >>> cursor.execute(spark_code)
        >>> result = cursor.fetchall()

        # Configure Spark session
        >>> cursor = connection.cursor(
        ...     SparkCursor,
        ...     engine_configuration={
        ...         'CoordinatorDpuSize': 1,
        ...         'MaxConcurrentDpus': 20,
        ...         'DefaultExecutorDpuSize': 1
        ...     }
        ... )

    Note:
        Requires an Athena workgroup configured for Spark calculations.
        Spark sessions have associated costs and idle timeout settings.
    """

    def __init__(
        self,
        session_id: str | None = None,
        description: str | None = None,
        engine_configuration: dict[str, Any] | None = None,
        notebook_version: str | None = None,
        session_idle_timeout_minutes: int | None = None,
        terminate_session_on_close: bool | None = None,
        **kwargs,
    ) -> None:
        """Initialize the cursor and start or attach to a Spark session.

        Args:
            session_id: ID of an existing session to use. If omitted, a new
                session is started.
            description: Description of a new session.
            engine_configuration: Engine configuration of a new session.
            notebook_version: Notebook version of a new session.
            session_idle_timeout_minutes: Idle timeout of a new session in minutes.
            terminate_session_on_close: Whether ``close()`` terminates the session.
                If None, only a session started by this cursor is terminated;
                a session supplied with ``session_id`` is left running.
            **kwargs: Arguments passed to ``SparkBaseCursor``.

        Raises:
            OperationalError: If the supplied session does not exist, or the
                session cannot be started or does not become idle.
        """
        super().__init__(
            session_id=session_id,
            description=description,
            engine_configuration=engine_configuration,
            notebook_version=notebook_version,
            session_idle_timeout_minutes=session_idle_timeout_minutes,
            terminate_session_on_close=terminate_session_on_close,
            **kwargs,
        )

    @property
    def calculation_execution(self) -> AthenaCalculationExecution | None:
        return self._calculation_execution

    def get_std_out(self) -> str | None:
        """Get the standard output from the Spark calculation execution.

        Retrieves and returns the contents of the standard output generated
        during the Spark calculation execution, if available.

        Returns:
            The standard output as a string, or None if no output is available
            or the calculation has not been executed.
        """
        if not self._calculation_execution or not self._calculation_execution.std_out_s3_uri:
            return None
        return self._read_s3_file_as_text(self._calculation_execution.std_out_s3_uri)

    def get_std_error(self) -> str | None:
        """Get the standard error from the Spark calculation execution.

        Retrieves and returns the contents of the standard error generated
        during the Spark calculation execution, if available. This is useful
        for debugging failed or problematic Spark operations.

        Returns:
            The standard error as a string, or None if no error output is available
            or the calculation has not been executed.
        """
        if not self._calculation_execution or not self._calculation_execution.std_error_s3_uri:
            return None
        return self._read_s3_file_as_text(self._calculation_execution.std_error_s3_uri)

    def execute(
        self,
        operation: str,
        parameters: dict[str, Any] | list[str] | None = None,
        session_id: str | None = None,
        description: str | None = None,
        client_request_token: str | None = None,
        work_group: str | None = None,
        **kwargs,
    ) -> SparkCursor:
        self._calculation_id = self._calculate(
            session_id=session_id if session_id else self._session_id,
            code_block=operation,
            description=description,
            client_request_token=client_request_token,
        )
        self._calculation_execution = cast(
            AthenaCalculationExecution, self._poll(self._calculation_id)
        )
        if self._calculation_execution.state != AthenaCalculationExecutionStatus.STATE_COMPLETED:
            std_error = self.get_std_error()
            raise OperationalError(std_error)
        return self

    def cancel(self) -> None:
        if not self.calculation_id:
            raise ProgrammingError("CalculationExecutionId is none or empty.")
        self._cancel(self.calculation_id)
