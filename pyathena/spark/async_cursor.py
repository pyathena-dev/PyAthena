import logging
from concurrent.futures import Future, ThreadPoolExecutor
from multiprocessing import cpu_count
from typing import TYPE_CHECKING, Any, cast

from pyathena.model import AthenaCalculationExecution
from pyathena.spark.common import SparkBaseCursor

if TYPE_CHECKING:
    from pyathena.model import AthenaQueryExecution

_logger = logging.getLogger(__name__)


class AsyncSparkCursor(SparkBaseCursor):
    """Asynchronous cursor for executing PySpark code on Amazon Athena for Apache Spark.

    This cursor provides asynchronous execution of PySpark code on Athena's managed
    Spark environment. It's designed for non-blocking big data processing, ETL
    operations, and machine learning workloads that require Spark's distributed
    computing capabilities without blocking the main thread.

    Features:
        - Asynchronous PySpark code execution with concurrent futures
        - Non-blocking query submission and result polling
        - Managed Spark sessions with configurable resources
        - Access to standard output and error streams asynchronously
        - Automatic session lifecycle management
        - Thread pool executor for concurrent operations

    Attributes:
        max_workers: Maximum number of worker threads for async operations.
        session_id: The Athena Spark session ID.
        engine_configuration: Spark engine configuration settings.

    Example:
        >>> from pyathena.spark.async_cursor import AsyncSparkCursor
        >>>
        >>> cursor = connection.cursor(
        ...     AsyncSparkCursor,
        ...     engine_configuration={
        ...         'CoordinatorDpuSize': 1,
        ...         'MaxConcurrentDpus': 20
        ...     }
        ... )
        >>>
        >>> # Execute PySpark code asynchronously
        >>> spark_code = '''
        ... df = spark.read.table("my_database.my_table")
        ... result = df.groupBy("category").count()
        ... result.show()
        ... '''
        >>> calculation_id, future = cursor.execute(spark_code)
        >>>
        >>> # Get result when ready
        >>> calc_execution = future.result()
        >>> stdout_future = cursor.get_std_out(calc_execution)
        >>> if stdout_future:
        ...     output = stdout_future.result()
        ...     print(output)

    Note:
        Requires an Athena workgroup configured for Spark calculations.
        Spark sessions have associated costs and idle timeout settings.
        The cursor manages a thread pool for asynchronous operations.
    """

    def __init__(
        self,
        session_id: str | None = None,
        description: str | None = None,
        engine_configuration: dict[str, Any] | None = None,
        notebook_version: str | None = None,
        session_idle_timeout_minutes: int | None = None,
        max_workers: int = (cpu_count() or 1) * 5,
        **kwargs,
    ):
        super().__init__(
            session_id=session_id,
            description=description,
            engine_configuration=engine_configuration,
            notebook_version=notebook_version,
            session_idle_timeout_minutes=session_idle_timeout_minutes,
            **kwargs,
        )
        self._max_workers = max_workers
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    def close(self, wait: bool = False) -> None:
        super().close()
        self._executor.shutdown(wait=wait)

    def calculation_execution(self, query_id: str) -> "Future[AthenaCalculationExecution]":
        return self._executor.submit(self._get_calculation_execution, query_id)

    def get_std_out(
        self, calculation_execution: AthenaCalculationExecution
    ) -> "Future[str] | None":
        if not calculation_execution.std_out_s3_uri:
            return None
        return self._executor.submit(
            self._read_s3_file_as_text, calculation_execution.std_out_s3_uri
        )

    def get_std_error(
        self, calculation_execution: AthenaCalculationExecution
    ) -> "Future[str] | None":
        if not calculation_execution.std_error_s3_uri:
            return None
        return self._executor.submit(
            self._read_s3_file_as_text, calculation_execution.std_error_s3_uri
        )

    def poll(self, query_id: str) -> "Future[AthenaCalculationExecution]":
        return cast(
            "Future[AthenaCalculationExecution]", self._executor.submit(self._poll, query_id)
        )

    def execute(
        self,
        operation: str,
        parameters: dict[str, Any] | list[str] | None = None,
        session_id: str | None = None,
        description: str | None = None,
        client_request_token: str | None = None,
        work_group: str | None = None,
        **kwargs,
    ) -> tuple[str, "Future[AthenaQueryExecution | AthenaCalculationExecution]"]:
        calculation_id = self._calculate(
            session_id=session_id if session_id else self._session_id,
            code_block=operation,
            description=description,
            client_request_token=client_request_token,
        )
        return calculation_id, self._executor.submit(self._poll, calculation_id)

    def cancel(self, query_id: str) -> "Future[None]":
        return self._executor.submit(self._cancel, query_id)
