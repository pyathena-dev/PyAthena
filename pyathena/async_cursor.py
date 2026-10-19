"""Thread-pool cursors that run Athena queries concurrently and return futures."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from concurrent.futures import Future
from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing import cpu_count
from typing import Any, TypeVar, cast

from pyathena.common import BaseCursor, CursorIterator
from pyathena.error import NotSupportedError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.options import ExecuteOptions
from pyathena.result_set import AthenaDictResultSet, AthenaResultSet
from pyathena.util import override

_logger = logging.getLogger(__name__)

_ResultSetT = TypeVar("_ResultSetT", bound=AthenaResultSet)


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
        descriptions as a Future. A result set whose future resolved before
        the cursor closed belongs to the caller. A future that still waits for
        its query when the cursor closes raises ``ProgrammingError``, also if
        its last status check finds the query finished, and one that is
        building its result set resolves with a closed result set (see
        :meth:`close`).
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
        # Set by close(), to end the waits between status requests.
        self._closed_event = threading.Event()
        self._result_set_class: type[AthenaResultSet] = AthenaResultSet
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
        """Close the cursor and shut down its executor.

        Later operations on the cursor raise ``ProgrammingError``. The futures
        of ``execute()``, ``description()``, and ``poll()`` that still wait
        for their query raise ``ProgrammingError`` at their next status check,
        also when that check finds the query finished; the query itself keeps
        running. A future of ``execute()`` that is building its result set
        resolves with a closed result set, and one that is just finishing can
        resolve with either an open or a closed one. A result set whose future resolved before the
        close is left to the caller. The futures of ``cancel()`` and
        ``query_execution()`` still run.

        Args:
            wait: Whether to wait for submitted futures to finish before returning.
        """
        self._closed = True
        self._closed_event.set()
        self._executor.shutdown(wait=wait)

    @override
    def _poll_until_terminal(self, query_id: str) -> AthenaQueryExecution:
        """Poll a query execution until it reaches a terminal state, unless the cursor closes.

        The cursor is checked before the first request, after each wait (see
        :meth:`_wait_poll_interval`), and after the terminal state, so that a
        future stops waiting, and builds no result set, once the cursor has
        closed.

        Args:
            query_id: The query execution ID.

        Returns:
            The query execution in a terminal state.

        Raises:
            ProgrammingError: If the cursor closes first.
            OperationalError: If a status request fails.
        """
        self._ensure_open()
        query_execution = cast(AthenaQueryExecution, super()._poll_until_terminal(query_id))
        self._ensure_open()
        return query_execution

    @override
    def _wait_poll_interval(self) -> None:
        """Wait ``poll_interval`` seconds, or until the cursor closes.

        Raises:
            ProgrammingError: If the cursor has closed.
        """
        self._closed_event.wait(self._poll_interval)
        self._ensure_open()

    def _submit_result_set(
        self, fn: Callable[..., _ResultSetT], /, *args: Any
    ) -> Future[_ResultSetT]:
        """Create a result set in the executor, closing it if the cursor has closed.

        After creating the result set, the worker closes it if the cursor has
        closed by then, before the future resolves.

        Args:
            fn: The function that creates the result set.
            *args: Positional arguments for ``fn``.

        Returns:
            The future of the result set.
        """

        def create() -> _ResultSetT:
            """Create the result set, and close it if the cursor has closed.

            Returns:
                The result set.
            """
            result_set = fn(*args)
            if self._closed:
                try:
                    result_set.close()
                except Exception:
                    _logger.exception(f"Failed to close {result_set!r} of a closed cursor.")
            return result_set

        return self._executor.submit(create)

    def _description(
        self, query_id: str
    ) -> list[tuple[str, str, None, None, int, int, str]] | None:
        """Wait for the query, then read the column descriptions of its result.

        The metadata reader class of the result set class reads them, from the
        arguments of :meth:`_metadata_reader_arguments`, as the result set that
        ``execute()`` creates reads its metadata. No result set is created, and no
        row is read.

        Args:
            query_id: The Athena query execution ID.

        Returns:
            The DB API 2.0 column descriptions, or None.

        Raises:
            ProgrammingError: If the cursor closes before the query finishes.
            OperationalError: If a status request or reading the metadata fails.
        """
        query_execution = cast(AthenaQueryExecution, self._poll(query_id))
        reader_class = self._result_set_class._metadata_reader_class
        return reader_class(**self._metadata_reader_arguments(query_execution)).describe()

    def description(
        self, query_id: str
    ) -> Future[list[tuple[str, str, None, None, int, int, str]] | None]:
        """Get the column descriptions of a query's result set asynchronously.

        The future waits for the query to finish, then reads only the result set
        metadata with the metadata reader of the cursor: a ``GetQueryResults``
        page of one row, or for an UNLOAD result of the pandas, Arrow, and Polars
        cursors, the schema of its Parquet files. The descriptions are those of
        the result set of ``execute()`` with the options of the cursor, not those
        given to ``execute()``. If the cursor closes first, the future raises
        ``ProgrammingError``.

        Args:
            query_id: The Athena query execution ID.

        Returns:
            Future object containing the DB API 2.0 column descriptions, or None.

        Raises:
            ProgrammingError: If the cursor is closed.
        """
        self._ensure_open()
        return self._executor.submit(self._description, query_id)

    def query_execution(self, query_id: str) -> Future[AthenaQueryExecution]:
        """Get query execution details asynchronously.

        Retrieves the current execution status and metadata for a query.
        This is useful for monitoring query progress without blocking.

        Args:
            query_id: The Athena query execution ID.

        Returns:
            Future object containing AthenaQueryExecution with query details.

        Raises:
            ProgrammingError: If the cursor is closed.
        """
        self._ensure_open()
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
            to your query execution duration. If the cursor closes first, the future
            raises ``ProgrammingError``.

        Raises:
            ProgrammingError: If the cursor is closed.
        """
        self._ensure_open()
        return cast("Future[AthenaQueryExecution]", self._executor.submit(self._poll, query_id))

    def _metadata_reader_arguments(self, query_execution: AthenaQueryExecution) -> dict[str, Any]:
        """Get the arguments that create the metadata reader of a query's result set.

        ``_description()`` passes them to the metadata reader class. The result
        set arguments include them (see :meth:`_result_set_arguments`), so that
        the result set reads its metadata with the same options. A variant
        cursor adds the options of its reader, taken from the options of
        ``execute()``.

        Args:
            query_execution: The query execution in a terminal state.

        Returns:
            The keyword arguments.
        """
        return {
            "connection": self._connection,
            "query_execution": query_execution,
            "retry_config": self._retry_config,
        }

    def _result_set_arguments(
        self,
        query_execution: AthenaQueryExecution,
        result_set_type_hints: dict[str | int, str] | None = None,
    ) -> dict[str, Any]:
        """Get the arguments that create the result set of a query.

        ``_collect_result_set()`` passes them to the result set class. They are
        the metadata reader arguments and the options of the rows. A variant
        cursor overrides both methods. Both build the arguments with ``dict()``
        calls, so that an option of ``execute()`` that names one of them raises
        ``TypeError``, as a duplicate keyword argument does.

        Args:
            query_execution: The query execution in a terminal state.
            result_set_type_hints: Athena type signatures for complex-type columns,
                keyed by column name (case-insensitive) or zero-based column index.

        Returns:
            The keyword arguments.
        """
        return dict(
            **self._metadata_reader_arguments(query_execution),
            converter=self._converter,
            arraysize=self._arraysize,
            result_set_type_hints=result_set_type_hints,
            **self._result_set_kwargs,
        )

    def _collect_result_set(
        self,
        query_id: str,
        result_set_type_hints: dict[str | int, str] | None = None,
    ) -> AthenaResultSet:
        """Wait for the query, then create its result set.

        Args:
            query_id: The Athena query execution ID.
            result_set_type_hints: Athena type signatures for complex-type columns,
                keyed by column name (case-insensitive) or zero-based column index.

        Returns:
            The result set.

        Raises:
            ProgrammingError: If the cursor closes before the query finishes.
            OperationalError: If a status request or reading the result fails.
        """
        query_execution = cast(AthenaQueryExecution, self._poll(query_id))
        return self._result_set_class(
            **self._result_set_arguments(query_execution, result_set_type_hints)
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
            - future: Future object for result retrieval, which raises
              ``ProgrammingError`` if the cursor closes before the query finishes
              (see :meth:`close`)

        Raises:
            ProgrammingError: If the cursor is closed.

        Example:
            >>> query_id, future = cursor.execute("SELECT * FROM large_table")
            >>> print(f"Query started: {query_id}")
            >>> # Do other work while query runs...
            >>> result_set = future.result()  # Wait for completion
        """
        self._ensure_open()
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
        return query_id, self._submit_result_set(
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
            ProgrammingError: If the cursor is closed.

        Note:
            For bulk operations, consider using execute() with parameterized
            queries or batch processing patterns instead.
        """
        self._ensure_open()
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

        Raises:
            ProgrammingError: If the cursor is closed.
        """
        self._ensure_open()
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
