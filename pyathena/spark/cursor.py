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
        **kwargs,
    ) -> None:
        super().__init__(
            session_id=session_id,
            description=description,
            engine_configuration=engine_configuration,
            notebook_version=notebook_version,
            session_idle_timeout_minutes=session_idle_timeout_minutes,
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
