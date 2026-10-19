"""Asynchronous cursor that returns Athena query results as Apache Arrow tables."""

from __future__ import annotations

import logging
from concurrent.futures import Future
from multiprocessing import cpu_count
from typing import Any, cast

from pyathena import ProgrammingError
from pyathena.arrow.converter import (
    DefaultArrowTypeConverter,
    DefaultArrowUnloadTypeConverter,
)
from pyathena.arrow.result_set import (
    AthenaArrowResultSet,
)
from pyathena.async_cursor import AsyncCursor
from pyathena.common import CursorIterator
from pyathena.model import AthenaQueryExecution
from pyathena.options import ExecuteOptions
from pyathena.util import override

_logger = logging.getLogger(__name__)


class AsyncArrowCursor(AsyncCursor):
    """Asynchronous cursor that returns results in Apache Arrow format.

    This cursor extends AsyncCursor to provide asynchronous query execution
    with results returned as Apache Arrow Tables or RecordBatches. It's optimized
    for high-performance analytics workloads and interoperability with the
    Apache Arrow ecosystem.

    Features:
        - Asynchronous query execution with concurrent futures
        - Apache Arrow columnar data format for high performance
        - Memory-efficient processing of large datasets
        - Support for UNLOAD operations with Parquet output
        - Integration with pandas, Polars, and other Arrow-compatible libraries

    Attributes:
        arraysize: Number of rows to fetch per batch (configurable).

    Example:
        >>> from pyathena.arrow.async_cursor import AsyncArrowCursor
        >>>
        >>> cursor = connection.cursor(AsyncArrowCursor, unload=True)
        >>> query_id, future = cursor.execute("SELECT * FROM large_table")
        >>>
        >>> # Get result when ready
        >>> result_set = future.result()
        >>> arrow_table = result_set.as_arrow()
        >>>
        >>> # Convert to pandas if needed
        >>> df = arrow_table.to_pandas()
        >>>
        >>> # Convert to Polars if needed (requires polars)
        >>> polars_df = result_set.as_polars()

    Note:
        Requires pyarrow to be installed. UNLOAD operations generate
        Parquet files in S3 for optimal Arrow compatibility. For Polars
        interoperability, polars must be installed separately.
    """

    def __init__(
        self,
        *,
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
        unload: bool = False,
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
        connect_timeout: float | None = None,
        request_timeout: float | None = None,
        **kwargs,
    ) -> None:
        """Initialize an AsyncArrowCursor.

        Args:
            s3_staging_dir: S3 location for query results.
            schema_name: Default schema name.
            catalog_name: Default catalog name.
            work_group: Athena workgroup name.
            poll_interval: Query status polling interval in seconds.
            encryption_option: S3 encryption option (SSE_S3, SSE_KMS, CSE_KMS).
            kms_key: KMS key ARN for encryption.
            kill_on_interrupt: Cancel running query on keyboard interrupt.
            max_workers: Size of the cursor thread pool for waiting and collecting results.
            arraysize: Number of rows to fetch per batch.
            unload: Enable UNLOAD for high-performance Parquet output.
            result_reuse_enable: Enable Athena query result reuse.
            result_reuse_minutes: Minutes to reuse cached results.
            connect_timeout: Socket connection timeout in seconds for S3 operations.
                Defaults to AWS SDK default (typically 1 second) if not specified.
            request_timeout: Request timeout in seconds for S3 operations.
                Defaults to AWS SDK default (typically 3 seconds) if not specified.
                Increase this value if you experience timeout errors when using
                role assumption with STS or have high latency to S3.
            **kwargs: Additional connection parameters.

        Example:
            >>> # Use higher timeouts for role assumption scenarios
            >>> cursor = connection.cursor(
            ...     AsyncArrowCursor,
            ...     connect_timeout=10.0,
            ...     request_timeout=30.0
            ... )
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
        self._result_set_class: type[AthenaArrowResultSet] = AthenaArrowResultSet
        self._unload = unload
        self._connect_timeout = connect_timeout
        self._request_timeout = request_timeout

    @staticmethod
    @override
    def get_default_converter(
        unload: bool = False,
    ) -> DefaultArrowTypeConverter | DefaultArrowUnloadTypeConverter | Any:
        if unload:
            return DefaultArrowUnloadTypeConverter()
        return DefaultArrowTypeConverter()

    @property  # type: ignore[explicit-override]  # python/mypy#15900
    @override
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value <= 0:
            raise ProgrammingError("arraysize must be a positive integer value.")
        self._arraysize = value

    @override
    def _metadata_reader_arguments(
        self,
        query_execution: AthenaQueryExecution,
        unload_location: str | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Get the arguments that create the Arrow metadata reader of a query's result set.

        Args:
            query_execution: The query execution in a terminal state.
            unload_location: The S3 location of the ``UNLOAD`` output.
            kwargs: Options of ``execute()``, from which the reader options are
                taken; the reader takes the timeouts.

        Returns:
            The keyword arguments for ``AthenaArrowResultSetMetadataReader``.
        """
        if kwargs is None:
            kwargs = {}
        return dict(
            **super()._metadata_reader_arguments(query_execution),
            unload=self._unload,
            unload_location=unload_location,
            connect_timeout=kwargs.pop("connect_timeout", self._connect_timeout),
            request_timeout=kwargs.pop("request_timeout", self._request_timeout),
        )

    @override
    def _result_set_arguments(
        self,
        query_execution: AthenaQueryExecution,
        result_set_type_hints: dict[str | int, str] | None = None,
        unload_location: str | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Get the arguments that create the Arrow result set of a query.

        They are the metadata reader arguments and the options of the rows.

        Args:
            query_execution: The query execution in a terminal state.
            result_set_type_hints: Athena type signatures for complex-type columns,
                keyed by column name (case-insensitive) or zero-based column index.
            unload_location: The S3 location of the ``UNLOAD`` output.
            kwargs: Options of ``execute()`` for the result set.

        Returns:
            The keyword arguments for ``AthenaArrowResultSet``.
        """
        if kwargs is None:
            kwargs = {}
        return dict(
            **self._metadata_reader_arguments(query_execution, unload_location, kwargs),
            converter=self._converter,
            arraysize=self._arraysize,
            result_set_type_hints=result_set_type_hints,
            # The options that the reader did not take.
            **kwargs,
        )

    @override
    def _collect_result_set(
        self,
        query_id: str,
        result_set_type_hints: dict[str | int, str] | None = None,
        unload_location: str | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> AthenaArrowResultSet:
        """Wait for the query, then create its Arrow result set.

        Args:
            query_id: The Athena query execution ID.
            result_set_type_hints: Athena type signatures for complex-type columns,
                keyed by column name (case-insensitive) or zero-based column index.
            unload_location: The S3 location of the ``UNLOAD`` output.
            kwargs: Options of ``execute()`` for the result set.

        Returns:
            The result set.

        Raises:
            ProgrammingError: If the cursor closes before the query finishes.
            OperationalError: If a status request or reading the result fails.
        """
        query_execution = cast(AthenaQueryExecution, self._poll(query_id))
        return self._result_set_class(
            **self._result_set_arguments(
                query_execution, result_set_type_hints, unload_location, kwargs
            )
        )

    @override
    def execute(
        self,
        operation: str,
        parameters: dict[str, Any] | list[str] | None = None,
        *,
        work_group: str | None = None,
        s3_staging_dir: str | None = None,
        cache_size: int | None = None,
        cache_expiration_time: int | None = None,
        result_reuse_enable: bool | None = None,
        result_reuse_minutes: int | None = None,
        paramstyle: str | None = None,
        result_set_type_hints: dict[str | int, str] | None = None,
        options: ExecuteOptions | None = None,
        **kwargs,
    ) -> tuple[str, Future[AthenaArrowResultSet | Any]]:
        """Execute a SQL query asynchronously and return results as Arrow Tables.

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
            result_set_type_hints: Athena type signatures for complex-type columns,
                keyed by column name (case-insensitive) or zero-based column index.
            options: Shared execution options as an
                :class:`~pyathena.options.ExecuteOptions` instance. Individual
                keyword arguments take precedence over ``options`` fields.
            **kwargs: Result-set overrides: ``block_size``, ``connect_timeout``,
                and ``request_timeout``. Unknown names raise TypeError.
                ``block_size`` sets the read block size for this query, and
                ``connect_timeout`` and ``request_timeout`` override the cursor's values.

        Returns:
            Tuple of (query_id, future) where future resolves to AthenaArrowResultSet.

        Raises:
            ProgrammingError: If the cursor is closed.
        """
        self._ensure_open()
        self._validate_execute_kwargs(kwargs, ("block_size", "connect_timeout", "request_timeout"))
        options = ExecuteOptions.resolve(
            options,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
            cache_expiration_time=cache_expiration_time,
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            paramstyle=paramstyle,
            result_set_type_hints=result_set_type_hints,
        )
        operation, unload_location = self._prepare_unload(operation, options.s3_staging_dir)
        query_id = self._execute(
            operation,
            parameters=parameters,
            options=options,
        )
        return (
            query_id,
            self._submit_result_set(
                self._collect_result_set,
                query_id,
                options.result_set_type_hints,
                unload_location,
                kwargs,
            ),
        )
