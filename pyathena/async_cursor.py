"""Thread-pool cursors that run Athena queries concurrently and return futures."""

from __future__ import annotations

import logging
from concurrent.futures import Future
from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing import cpu_count
from typing import Any, cast

from pyathena.common import BaseCursor, CursorIterator
from pyathena.error import NotSupportedError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.options import ExecuteOptions
from pyathena.result_set import AthenaDictResultSet, AthenaResultSet
from pyathena.util import override

_logger = logging.getLogger(__name__)


class AsyncCursor(BaseCursor):
    """Asynchronous cursor for non-blocking Athena query execution.

    This cursor allows multiple queries to be executed concurrently without
    blocking the main thread. It's useful for applications that need to execute
    multiple queries in parallel or perform other work while queries are running.

    The cursor maintains a thread pool for executing queries asynchronously and
    provides methods to check query status and retrieve results when ready.

    Attributes:
        arraysize: Default number of rows that fetchmany() returns on the result
            sets this cursor creates.

    Example:
        >>> cursor = connection.cursor(AsyncCursor)
        >>>
        >>> # Execute multiple queries concurrently
        >>> query_id1, future1 = cursor.execute("SELECT COUNT(*) FROM table1")
        >>> query_id2, future2 = cursor.execute("SELECT COUNT(*) FROM table2")
        >>> query_id3, future3 = cursor.execute("SELECT COUNT(*) FROM table3")
        >>>
        >>> # Check if queries are done and get results
        >>> if future1.done():
        ...     result1 = future1.result().fetchall()
        >>>
        >>> # Wait for all to complete
        >>> results = [f.result().fetchall() for f in [future1, future2, future3]]

    Note:
        Each execute() call returns a ``(query_id, future)`` tuple. The future
        resolves to the result set, which provides the column descriptions and
        the fetch methods. ``description(query_id)`` also returns the column
        descriptions as a Future.
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
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
        **kwargs,
    ) -> None:
        """Initialize an AsyncCursor.

        Args:
            s3_staging_dir: S3 location for query results.
            schema_name: Default schema name.
            catalog_name: Default catalog name.
            work_group: Athena workgroup name.
            poll_interval: Query status polling interval in seconds.
            encryption_option: S3 encryption option (SSE_S3, SSE_KMS, CSE_KMS).
            kms_key: KMS key for encryption.
            kill_on_interrupt: Cancel a query whose start in ``execute()`` is interrupted by
                ``KeyboardInterrupt``. Waiting runs on worker threads, which do not
                receive the interrupt.
            max_workers: Size of the cursor thread pool for waiting and collecting results.
            arraysize: Default number of rows per ``fetchmany()`` call of the result
                sets the cursor creates.
            result_reuse_enable: Enable Athena query result reuse.
            result_reuse_minutes: Maximum age in minutes of a reused result.
            **kwargs: Arguments forwarded to ``BaseCursor.__init__``, such as
                ``connection``, ``converter``, ``formatter``, and ``retry_config``.

        Raises:
            ProgrammingError: If ``arraysize`` is not between 1 and
                ``CursorIterator.DEFAULT_FETCH_SIZE``.
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
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            **kwargs,
        )
        self.arraysize = arraysize
        self._max_workers = max_workers
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._result_set_class = AthenaResultSet
        self._result_set_kwargs: dict[str, Any] = {}

    @property
    def arraysize(self) -> int:
        """The default number of rows per ``fetchmany()`` call of the result sets."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value <= 0 or value > CursorIterator.DEFAULT_FETCH_SIZE:
            raise ProgrammingError(
                "MaxResults is more than maximum allowed length "
                f"{CursorIterator.DEFAULT_FETCH_SIZE}."
            )
        self._arraysize = value

    @override
    def close(self, wait: bool = False) -> None:
        self._executor.shutdown(wait=wait)

    def _description(
        self, query_id: str
    ) -> list[tuple[str, str, None, None, int, int, str]] | None:
        result_set = self._collect_result_set(query_id)
        return result_set.description

    def description(
        self, query_id: str
    ) -> Future[list[tuple[str, str, None, None, int, int, str]] | None]:
        """Get the column descriptions of a query's result set asynchronously.

        The future waits for the query to finish before it reads the result set.

        Args:
            query_id: The Athena query execution ID.

        Returns:
            Future object containing the DB API 2.0 column descriptions, or None.
        """
        return self._executor.submit(self._description, query_id)

    def query_execution(self, query_id: str) -> Future[AthenaQueryExecution]:
        """Get query execution details asynchronously.

        Retrieves the current execution status and metadata for a query.
        This is useful for monitoring query progress without blocking.

        Args:
            query_id: The Athena query execution ID.

        Returns:
            Future object containing AthenaQueryExecution with query details.
        """
        return self._executor.submit(self._get_query_execution, query_id)

    def poll(self, query_id: str) -> Future[AthenaQueryExecution]:
        """Poll for query completion asynchronously.

        Waits for the query to complete (succeed, fail, or be cancelled) and
        returns the final execution status. This method blocks until completion
        but runs the polling in a background thread.

        Args:
            query_id: The Athena query execution ID to poll.

        Returns:
            Future object containing the final AthenaQueryExecution status.

        Note:
            This method performs polling internally, so it will take time proportional
            to your query execution duration.
        """
        return cast("Future[AthenaQueryExecution]", self._executor.submit(self._poll, query_id))

    def _collect_result_set(
        self,
        query_id: str,
        result_set_type_hints: dict[str | int, str] | None = None,
    ) -> AthenaResultSet:
        query_execution = cast(AthenaQueryExecution, self._poll(query_id))
        return self._result_set_class(
            connection=self._connection,
            converter=self._converter,
            query_execution=query_execution,
            arraysize=self._arraysize,
            retry_config=self._retry_config,
            result_set_type_hints=result_set_type_hints,
            **self._result_set_kwargs,
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
    ) -> tuple[str, Future[AthenaResultSet | Any]]:
        """Execute a SQL query asynchronously.

        Starts query execution on Amazon Athena and returns immediately without
        waiting for completion. The query runs in the background while your
        application can continue with other work.

        Args:
            operation: SQL query string to execute.
            parameters: Query parameters (optional).
            work_group: Athena workgroup to use (optional).
            s3_staging_dir: S3 location for query results (optional).
            cache_size: Number of queries to check for result caching (optional).
            cache_expiration_time: Cache expiration time in seconds (optional).
            result_reuse_enable: Enable result reuse for identical queries (optional).
            result_reuse_minutes: Result reuse duration in minutes (optional).
            paramstyle: Parameter style to use (optional).
            result_set_type_hints: Athena type signatures for complex-type columns,
                keyed by column name (case-insensitive) or zero-based column index.
            options: Shared execution options as an
                :class:`~pyathena.options.ExecuteOptions` instance. Individual
                keyword arguments take precedence over ``options`` fields.
            **kwargs: Unknown keyword arguments raise TypeError.

        Returns:
            Tuple of (query_id, future) where:
            - query_id: Athena query execution ID for tracking
            - future: Future object for result retrieval

        Example:
            >>> query_id, future = cursor.execute("SELECT * FROM large_table")
            >>> print(f"Query started: {query_id}")
            >>> # Do other work while query runs...
            >>> result_set = future.result()  # Wait for completion
        """
        self._validate_execute_kwargs(kwargs)
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
        query_id = self._execute(
            operation,
            parameters=parameters,
            options=options,
        )
        return query_id, self._executor.submit(
            self._collect_result_set, query_id, options.result_set_type_hints
        )

    @override
    def executemany(
        self,
        operation: str,
        seq_of_parameters: list[dict[str, Any] | list[str] | None],
        **kwargs,
    ) -> None:
        """Execute multiple queries asynchronously (not supported).

        This method is not supported for asynchronous cursors because managing
        multiple concurrent queries would be complex and resource-intensive.

        Args:
            operation: SQL query string.
            seq_of_parameters: Sequence of parameter sets.
            **kwargs: Additional arguments.

        Raises:
            NotSupportedError: Always raised as this operation is not supported.

        Note:
            For bulk operations, consider using execute() with parameterized
            queries or batch processing patterns instead.
        """
        raise NotSupportedError

    def cancel(self, query_id: str) -> Future[None]:
        """Cancel a running query asynchronously.

        Submits a cancellation request for the specified query. The cancellation
        itself runs asynchronously in the background.

        Args:
            query_id: The Athena query execution ID to cancel.

        Returns:
            Future object that completes when the cancellation request finishes.

        Example:
            >>> query_id, future = cursor.execute("SELECT * FROM huge_table")
            >>> # Later, cancel the query
            >>> cancel_future = cursor.cancel(query_id)
            >>> cancel_future.result()  # Wait for cancellation to complete
        """
        return self._executor.submit(self._cancel, query_id)


class AsyncDictCursor(AsyncCursor):
    """Asynchronous cursor that returns query results as dictionaries.

    Combines the asynchronous execution capabilities of AsyncCursor with
    the dictionary-based result format of DictCursor. Results are returned
    as dictionaries where column names are keys, making it easier to access
    column values by name rather than position.

    Example:
        >>> cursor = connection.cursor(AsyncDictCursor)
        >>> query_id, future = cursor.execute("SELECT id, name, email FROM users")
        >>> result_set = future.result()
        >>> row = result_set.fetchone()
        >>> print(f"User: {row['name']} ({row['email']})")
    """

    def __init__(self, *, dict_type: type[Any] | None = None, **kwargs) -> None:
        """Initialize an AsyncDictCursor.

        Args:
            dict_type: The type used to build each row of this cursor's result
                sets. If None, the result set class's ``dict_type`` is used.
            **kwargs: Arguments forwarded to ``AsyncCursor.__init__``.
        """
        super().__init__(**kwargs)
        self._result_set_class = AthenaDictResultSet
        if dict_type is not None:
            self._result_set_kwargs = {"dict_type": dict_type}
