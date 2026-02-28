from __future__ import annotations

import logging
from concurrent.futures import Future
from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing import cpu_count
from typing import Any, cast

from pyathena.common import BaseCursor, CursorIterator
from pyathena.error import NotSupportedError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaDictResultSet, AthenaResultSet

_logger = logging.getLogger(__name__)


class AsyncCursor(BaseCursor):
    """Asynchronous cursor for non-blocking Athena query execution.

    This cursor allows multiple queries to be executed concurrently without
    blocking the main thread. It's useful for applications that need to execute
    multiple queries in parallel or perform other work while queries are running.

    The cursor maintains a thread pool for executing queries asynchronously and
    provides methods to check query status and retrieve results when ready.

    Attributes:
        description: Sequence of column descriptions for the last query.
        rowcount: Number of rows affected by the last query (-1 for SELECT queries).
        arraysize: Default number of rows to fetch with fetchmany().
        max_workers: Maximum number of worker threads for concurrent execution.

    Example:
        >>> cursor = connection.cursor(AsyncCursor)
        >>>
        >>> # Execute multiple queries concurrently
        >>> future1 = cursor.execute("SELECT COUNT(*) FROM table1")
        >>> future2 = cursor.execute("SELECT COUNT(*) FROM table2")
        >>> future3 = cursor.execute("SELECT COUNT(*) FROM table3")
        >>>
        >>> # Check if queries are done and get results
        >>> if future1.done():
        ...     result1 = future1.result().fetchall()
        >>>
        >>> # Wait for all to complete
        >>> results = [f.result().fetchall() for f in [future1, future2, future3]]

    Note:
        Each execute() call returns a Future object that can be used to
        check completion status and retrieve results.
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
        **kwargs,
    ) -> None:
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
        self._max_workers = max_workers
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._arraysize = arraysize
        self._result_set_class = AthenaResultSet

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value <= 0 or value > CursorIterator.DEFAULT_FETCH_SIZE:
            raise ProgrammingError(
                "MaxResults is more than maximum allowed length "
                f"{CursorIterator.DEFAULT_FETCH_SIZE}."
            )
        self._arraysize = value

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
        result_set_type_hints: dict[str, str] | None = None,
    ) -> AthenaResultSet:
        query_execution = cast(AthenaQueryExecution, self._poll(query_id))
        return self._result_set_class(
            connection=self._connection,
            converter=self._converter,
            query_execution=query_execution,
            arraysize=self._arraysize,
            retry_config=self._retry_config,
            result_set_type_hints=result_set_type_hints,
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
        result_set_type_hints: dict[str, str] | None = None,
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
            cache_size: Query result cache size in MB (optional).
            cache_expiration_time: Cache expiration time in seconds (optional).
            result_reuse_enable: Enable result reuse for identical queries (optional).
            result_reuse_minutes: Result reuse duration in minutes (optional).
            paramstyle: Parameter style to use (optional).
            result_set_type_hints: Optional dictionary mapping column names to
                Athena DDL type signatures for precise type conversion within
                complex types.
            **kwargs: Additional execution parameters.

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
        return query_id, self._executor.submit(
            self._collect_result_set, query_id, result_set_type_hints
        )

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
        >>> future = cursor.execute("SELECT id, name, email FROM users")
        >>> result_cursor = future.result()
        >>> row = result_cursor.fetchone()
        >>> print(f"User: {row['name']} ({row['email']})")
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._result_set_class = AthenaDictResultSet
        if "dict_type" in kwargs:
            AthenaDictResultSet.dict_type = kwargs["dict_type"]
