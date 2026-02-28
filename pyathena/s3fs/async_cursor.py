from __future__ import annotations

import logging
from concurrent.futures import Future
from multiprocessing import cpu_count
from typing import Any, cast

from pyathena.async_cursor import AsyncCursor
from pyathena.common import CursorIterator
from pyathena.error import ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.s3fs.converter import DefaultS3FSTypeConverter
from pyathena.s3fs.result_set import AthenaS3FSResultSet, CSVReaderType

_logger = logging.getLogger(__name__)


class AsyncS3FSCursor(AsyncCursor):
    """Asynchronous cursor that reads CSV results via S3FileSystem.

    This cursor extends AsyncCursor to provide asynchronous query execution
    with results read via PyAthena's S3FileSystem.
    It's a lightweight alternative when pandas/pyarrow are not needed.

    Features:
        - Asynchronous query execution with concurrent futures
        - Lightweight CSV parsing via pluggable readers
        - Uses PyAthena's S3FileSystem for S3 access
        - No external dependencies beyond boto3
        - Memory-efficient streaming for large datasets

    Attributes:
        arraysize: Number of rows to fetch per batch (configurable).

    Example:
        >>> from pyathena.s3fs.async_cursor import AsyncS3FSCursor
        >>>
        >>> cursor = connection.cursor(AsyncS3FSCursor)
        >>> query_id, future = cursor.execute("SELECT * FROM my_table")
        >>>
        >>> # Get result when ready
        >>> result_set = future.result()
        >>> rows = result_set.fetchall()

    Note:
        This cursor does not require pandas or pyarrow.
    """

    def __init__(
        self,
        s3_staging_dir: str | None = None,
        schema_name: str | None = None,
        catalog_name: str | None = None,
        work_group: str | None = None,
        poll_interval: float = 1,
        encryption_option: str | None = None,
        kms_key: str | None = None,
        kill_on_interrupt: bool = True,
        max_workers: int = (cpu_count() or 1) * 5,
        arraysize: int = CursorIterator.DEFAULT_FETCH_SIZE,
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
        csv_reader: CSVReaderType | None = None,
        **kwargs,
    ) -> None:
        """Initialize an AsyncS3FSCursor.

        Args:
            s3_staging_dir: S3 location for query results.
            schema_name: Default schema name.
            catalog_name: Default catalog name.
            work_group: Athena workgroup name.
            poll_interval: Query status polling interval in seconds.
            encryption_option: S3 encryption option (SSE_S3, SSE_KMS, CSE_KMS).
            kms_key: KMS key ARN for encryption.
            kill_on_interrupt: Cancel running query on keyboard interrupt.
            max_workers: Maximum number of workers for concurrent execution.
            arraysize: Number of rows to fetch per batch.
            result_reuse_enable: Enable Athena query result reuse.
            result_reuse_minutes: Minutes to reuse cached results.
            csv_reader: CSV reader class to use for parsing results.
                Use AthenaCSVReader (default) to distinguish between NULL
                (unquoted empty) and empty string (quoted empty "").
                Use DefaultCSVReader for backward compatibility where empty
                strings are treated as NULL.
            **kwargs: Additional connection parameters.

        Example:
            >>> cursor = connection.cursor(AsyncS3FSCursor)
            >>> query_id, future = cursor.execute("SELECT * FROM my_table")
        """
        super().__init__(
            s3_staging_dir=s3_staging_dir,
            schema_name=schema_name,
            catalog_name=catalog_name,
            work_group=work_group,
            poll_interval=poll_interval,
            encryption_option=encryption_option,
            kms_key=kms_key,
            kill_on_interrupt=kill_on_interrupt,
            max_workers=max_workers,
            arraysize=arraysize,
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            **kwargs,
        )
        self._csv_reader = csv_reader

    @staticmethod
    def get_default_converter(
        unload: bool = False,
    ) -> DefaultS3FSTypeConverter:
        """Get the default type converter for S3FS cursor.

        Args:
            unload: Unused. S3FS cursor does not support UNLOAD operations.

        Returns:
            DefaultS3FSTypeConverter instance.
        """
        return DefaultS3FSTypeConverter()

    @property
    def arraysize(self) -> int:
        """Get the number of rows to fetch at a time."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        """Set the number of rows to fetch at a time.

        Args:
            value: Number of rows (must be positive).

        Raises:
            ProgrammingError: If value is not positive.
        """
        if value <= 0:
            raise ProgrammingError("arraysize must be a positive integer value.")
        self._arraysize = value

    def _collect_result_set(
        self,
        query_id: str,
        result_set_type_hints: dict[str | int, str] | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> AthenaS3FSResultSet:
        """Collect result set after query execution.

        Args:
            query_id: The Athena query execution ID.
            result_set_type_hints: Optional dictionary mapping column names to
                Athena DDL type signatures for precise type conversion within
                complex types.
            kwargs: Additional keyword arguments for result set.

        Returns:
            AthenaS3FSResultSet containing the query results.
        """
        if kwargs is None:
            kwargs = {}
        query_execution = cast(AthenaQueryExecution, self._poll(query_id))
        return AthenaS3FSResultSet(
            connection=self._connection,
            converter=self._converter,
            query_execution=query_execution,
            arraysize=self._arraysize,
            retry_config=self._retry_config,
            csv_reader=self._csv_reader,
            result_set_type_hints=result_set_type_hints,
            **kwargs,
        )

    def execute(
        self,
        operation: str,
        parameters: dict[str, Any] | list[str] | None = None,
        work_group: str | None = None,
        s3_staging_dir: str | None = None,
        cache_size: int | None = 0,
        cache_expiration_time: int | None = 0,
        result_reuse_enable: bool | None = None,
        result_reuse_minutes: int | None = None,
        paramstyle: str | None = None,
        result_set_type_hints: dict[str | int, str] | None = None,
        **kwargs,
    ) -> tuple[str, Future[AthenaS3FSResultSet | Any]]:
        """Execute a SQL query asynchronously.

        Submits the query to Athena and returns immediately with a query ID
        and a Future that will contain the result set when complete.

        Args:
            operation: SQL query string to execute.
            parameters: Query parameters for parameterized queries.
            work_group: Athena workgroup to use for this query.
            s3_staging_dir: S3 location for query results.
            cache_size: Number of queries to check for result caching.
            cache_expiration_time: Cache expiration time in seconds.
            result_reuse_enable: Enable Athena result reuse for this query.
            result_reuse_minutes: Minutes to reuse cached results.
            paramstyle: Parameter style ('qmark' or 'pyformat').
            result_set_type_hints: Optional dictionary mapping column names to
                Athena DDL type signatures for precise type conversion within
                complex types.
            **kwargs: Additional execution parameters.

        Returns:
            Tuple of (query_id, Future[AthenaS3FSResultSet]).

        Example:
            >>> query_id, future = cursor.execute("SELECT * FROM my_table")
            >>> result_set = future.result()
            >>> rows = result_set.fetchall()
        """
        query_id = self._execute(
            operation,
            parameters=parameters,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
            cache_expiration_time=cache_expiration_time,
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            paramstyle=paramstyle,
        )
        return (
            query_id,
            self._executor.submit(
                self._collect_result_set,
                query_id,
                result_set_type_hints,
                kwargs,
            ),
        )
