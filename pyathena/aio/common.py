"""Asyncio base cursor and the fetch mixin shared by the asyncio SQL cursors."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, NoReturn, TypeVar, cast

from botocore.exceptions import BotoCoreError, ClientError

from pyathena.aio.util import async_retry_api_call
from pyathena.common import BaseCursor, CursorIterator
from pyathena.error import DatabaseError, OperationalError, ProgrammingError
from pyathena.glue import GlueMetadataClient
from pyathena.model import AthenaDatabase, AthenaQueryExecution, AthenaTableMetadata
from pyathena.options import ExecuteOptions
from pyathena.result_set import AthenaResultSet, WithResultSet
from pyathena.util import _is_throttling_error, override

_logger = logging.getLogger(__name__)

_T = TypeVar("_T")


class AioBaseCursor(BaseCursor):
    """Async base cursor that overrides I/O methods with async equivalents.

    Reuses ``BaseCursor.__init__``, all ``_build_*`` methods, and constants.
    Only the methods that perform network I/O or blocking sleep are overridden
    to use ``asyncio.to_thread`` / ``asyncio.sleep``.
    """

    @override
    async def _execute(  # type: ignore[override]
        self,
        operation: str,
        parameters: dict[str, Any] | list[str] | None = None,
        work_group: str | None = None,
        s3_staging_dir: str | None = None,
        cache_size: int | None = None,
        cache_expiration_time: int | None = None,
        result_reuse_enable: bool | None = None,
        result_reuse_minutes: int | None = None,
        paramstyle: str | None = None,
        options: ExecuteOptions | None = None,
    ) -> str:
        """Start a query execution, or find a previous one to reuse.

        The individual keyword arguments override the ``options`` field of the
        same name unless None. A query with execution parameters (``qmark``)
        always starts a new execution.

        Args:
            operation: SQL query string.
            parameters: Query parameters.
            work_group: Athena work group.
            s3_staging_dir: S3 location for query results.
            cache_size: Number of recent executions to search for a reusable result.
            cache_expiration_time: Maximum age of a reusable result in seconds.
            result_reuse_enable: Whether to enable Athena result reuse.
            result_reuse_minutes: Maximum age of an Athena-reused result in minutes.
            paramstyle: Parameter style ('qmark' or 'pyformat').
            options: The execution options.

        Returns:
            The query execution ID.

        Raises:
            asyncio.CancelledError: If the task is cancelled while starting the
                query; see ``_start_execution()``.
            ProgrammingError: If the formatter rejects the query or its parameters.
            DatabaseError: If the ``StartQueryExecution`` request fails.
        """
        # The individual keyword arguments are retained for backward compatibility
        # with external callers that predate ExecuteOptions, mirroring
        # BaseCursor._execute().
        options = ExecuteOptions.resolve(
            options,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
            cache_expiration_time=cache_expiration_time,
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            paramstyle=paramstyle,
        )
        query, request = self._build_execute_request(operation, parameters, options)
        query_id = None
        # Athena does not return the ExecutionParameters of earlier executions,
        # so the cache cannot tell which parameters an execution ran with (#941).
        if not request.get("ExecutionParameters"):
            query_id = await self._find_previous_query_id(
                query,
                options.work_group,
                cache_size=options.cache_size,
                cache_expiration_time=options.cache_expiration_time,
            )
        if query_id is None:
            query_id = await self._start_execution(lambda: self._start_query_execution(request))
        return query_id

    @override
    async def _start_query_execution(self, request: dict[str, Any]) -> str:  # type: ignore[override]
        """Send a ``StartQueryExecution`` request.

        Args:
            request: The request parameters.

        Returns:
            The query execution ID.

        Raises:
            DatabaseError: If the request fails.
        """
        try:
            response = await async_retry_api_call(
                self._connection.client.start_query_execution,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to execute query.")
            raise DatabaseError(*e.args) from e
        return cast(str, response.get("QueryExecutionId"))

    @override
    async def _start_execution(  # type: ignore[override]
        self, start: Callable[[], Coroutine[Any, Any, str]]
    ) -> str:
        """Send a start request so that task cancellation stops the execution it starts.

        With ``kill_on_interrupt`` enabled, the request runs in a task shielded
        from task cancellation. On cancellation, the request is abandoned if that
        task has not begun it by then; it is never sent, and the cancellation
        propagates. Otherwise the cursor waits for the request to finish, records
        the execution ID with ``_set_interrupted_execution_id()``, requests
        cancellation with ``_cancel_and_wait()``, and re-raises
        ``asyncio.CancelledError``. Another cancellation during that wait
        propagates at once.

        Args:
            start: Returns a coroutine that sends the start request and returns the
                execution ID.

        Returns:
            The execution ID.

        Raises:
            asyncio.CancelledError: If the task is cancelled while starting. A
                failure to start, cancel, or wait for the execution becomes its
                ``__cause__``.
            DatabaseError: If the request fails.
        """
        if not self._kill_on_interrupt:
            return await start()

        caller = asyncio.current_task()
        cancel_requests = caller.cancelling() if caller else 0

        async def run() -> str | None:
            # Begin the request only if the caller has not been cancelled since.
            if caller and caller.cancelling() > cancel_requests:
                return None
            return await start()

        task = asyncio.ensure_future(run())
        try:
            return cast(str, await asyncio.shield(task))
        except asyncio.CancelledError as cancellation:
            try:
                execution_id = await task
                if execution_id is None:
                    # The task did not begin the request, so it was never sent.
                    raise cancellation
                _logger.warning("Query canceled by user.")
                self._set_interrupted_execution_id(execution_id)
                await self._cancel_and_wait(execution_id)
            except Exception as e:
                raise cancellation from e
            raise

    @override
    async def _get_query_execution(self, query_id: str) -> AthenaQueryExecution:  # type: ignore[override]
        """Get a query execution with ``GetQueryExecution``.

        Args:
            query_id: The query execution ID.

        Returns:
            The query execution.

        Raises:
            OperationalError: If the request fails.
        """
        request: dict[str, Any] = {"QueryExecutionId": query_id}
        try:
            response = await async_retry_api_call(
                self._connection.client.get_query_execution,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to get query execution.")
            raise OperationalError(*e.args) from e
        else:
            return AthenaQueryExecution(response)

    @override
    async def _poll_until_terminal(self, query_id: str) -> AthenaQueryExecution:  # type: ignore[override]
        """Poll a query execution until it reaches a terminal state.

        Calls ``on_poll`` with every status and awaits ``poll_interval`` seconds
        between requests.

        Args:
            query_id: The query execution ID.

        Returns:
            The query execution in a terminal state.

        Raises:
            OperationalError: If a status request fails.
        """
        while True:
            query_execution = await self._get_query_execution(query_id)
            if self._on_poll:
                self._on_poll(query_execution)
            if query_execution.state in AthenaQueryExecution.TERMINAL_STATES:
                return query_execution
            await asyncio.sleep(self._poll_interval)

    @override
    async def _poll(self, query_id: str) -> AthenaQueryExecution:  # type: ignore[override]
        """Wait for a query execution to reach a terminal state.

        On task cancellation with ``kill_on_interrupt`` enabled, requests
        cancellation with ``_cancel_and_wait()`` and re-raises
        ``asyncio.CancelledError``. Cancellation is a best-effort request, so the
        query can still end as ``SUCCEEDED`` or ``FAILED`` instead of ``CANCELLED``.

        Args:
            query_id: The query execution ID.

        Returns:
            The query execution in a terminal state.

        Raises:
            asyncio.CancelledError: If the task is cancelled while waiting. A failure
                to cancel or wait for the query becomes its ``__cause__``.
            OperationalError: If a status request fails.
        """
        try:
            return await self._poll_until_terminal(query_id)
        except asyncio.CancelledError as cancellation:
            if not self._kill_on_interrupt:
                raise
            _logger.warning("Query canceled by user.")
            try:
                await self._cancel_and_wait(query_id)
            except Exception as e:
                raise cancellation from e
            raise

    @override
    async def _cancel_and_wait(self, query_id: str) -> None:  # type: ignore[override]
        """Request cancellation of a query and wait for a terminal state.

        Args:
            query_id: The query execution ID.

        Raises:
            OperationalError: If the cancellation or a status request fails.
        """
        await self._cancel(query_id)
        await self._poll_until_terminal(query_id)

    @override
    async def _cancel(self, query_id: str) -> None:  # type: ignore[override]
        """Stop a query execution with ``StopQueryExecution``.

        Args:
            query_id: The query execution ID.

        Raises:
            OperationalError: If the request fails.
        """
        request: dict[str, Any] = {"QueryExecutionId": query_id}
        try:
            await async_retry_api_call(
                self._connection.client.stop_query_execution,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to cancel query.")
            raise OperationalError(*e.args) from e

    @override
    async def _batch_get_query_execution(  # type: ignore[override]
        self, query_ids: list[str]
    ) -> list[AthenaQueryExecution]:
        try:
            response = await async_retry_api_call(
                self.connection._client.batch_get_query_execution,
                config=self._retry_config,
                logger=_logger,
                QueryExecutionIds=query_ids,
            )
        except Exception as e:
            _logger.exception("Failed to batch get query execution.")
            raise OperationalError(*e.args) from e
        else:
            return [
                AthenaQueryExecution({"QueryExecution": r})
                for r in response.get("QueryExecutions", [])
            ]

    @override
    async def _list_query_executions(  # type: ignore[override]
        self,
        work_group: str | None = None,
        next_token: str | None = None,
        max_results: int | None = None,
    ) -> tuple[str | None, list[AthenaQueryExecution]]:
        request = self._build_list_query_executions_request(
            work_group=work_group, next_token=next_token, max_results=max_results
        )
        try:
            response = await async_retry_api_call(
                self.connection._client.list_query_executions,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to list query executions.")
            raise OperationalError(*e.args) from e
        else:
            next_token = response.get("NextToken")
            query_ids = response.get("QueryExecutionIds")
            if not query_ids:
                return next_token, []
            return next_token, await self._batch_get_query_execution(query_ids)

    @override
    async def _find_previous_query_id(  # type: ignore[override]
        self,
        query: str,
        work_group: str | None,
        cache_size: int = 0,
        cache_expiration_time: int = 0,
    ) -> str | None:
        """Find a previous execution of a query whose result can be reused.

        Searches the work group's recent executions page by page. A failed
        search is logged and treated as a cache miss.

        Args:
            query: The query string.
            work_group: The work group to search, or None for the cursor's.
            cache_size: The number of recent executions to search, or 0.
            cache_expiration_time: The maximum age of a reused result in
                seconds, or 0 for no limit.

        Returns:
            The query ID of the latest reusable execution, or None.
        """
        cache_size, expiration_time = self._cache_search_limits(cache_size, cache_expiration_time)
        query_id = None
        try:
            next_token = None
            while cache_size > 0:
                max_results = min(cache_size, self.LIST_QUERY_EXECUTIONS_MAX_RESULTS)
                cache_size -= max_results
                next_token, query_executions = await self._list_query_executions(
                    work_group, next_token=next_token, max_results=max_results
                )
                query_id, expired = self._match_previous_query(
                    query, query_executions, expiration_time
                )
                if query_id or expired or next_token is None:
                    break
        except Exception:
            _logger.warning("Failed to check the cache. Moving on without cache.", exc_info=True)
        return query_id

    async def _async_with_glue_fallback(
        self,
        catalog_name: str | None,
        athena_request: Callable[[Callable[[BaseException], bool] | None, bool], Awaitable[_T]],
        glue_request: Callable[[GlueMetadataClient, str], _T],
        description: str,
        logging_: bool = True,
        absence_is_final: bool = False,
    ) -> _T:
        """Async counterpart of ``BaseCursor._with_glue_fallback``.

        The Glue client is built and the Glue request sent in a worker thread,
        as the Athena requests are.

        Args:
            catalog_name: The requested catalog, or None for the cursor's catalog.
            athena_request: Sends the Athena request; receives the predicate
                that stops its retries (or None) and whether to log a failure.
            glue_request: Sends the Glue request; receives the connection's
                ``GlueMetadataClient`` and the catalog name.
            description: What the request does, for log messages.
            logging_: Whether to log a failed request.
            absence_is_final: Whether Glue's ``EntityNotFoundException`` answers
                the request.

        Returns:
            The result of the Athena or the Glue request.

        Raises:
            OperationalError: If the request fails.
        """
        glue_catalog = self._glue_catalog_name(catalog_name)
        if glue_catalog is None:
            return await athena_request(None, logging_)
        try:
            return await athena_request(_is_throttling_error, False)
        except OperationalError as e:
            if not _is_throttling_error(e.__cause__ or e):
                if logging_:
                    _logger.exception(f"Failed to {description}.")
                raise
        _logger.warning(f"Request to {description} was throttled; reading it from Glue.")
        try:
            return await asyncio.to_thread(glue_request, self._connection._glue, glue_catalog)
        except (BotoCoreError, ClientError) as e:
            self._glue_request_failed(e, description, absence_is_final)
        return await athena_request(None, logging_)

    @override
    async def _list_databases(  # type: ignore[override]
        self,
        catalog_name: str | None,
        next_token: str | None = None,
        max_results: int | None = None,
        logging_: bool = True,
        stop_on: Callable[[BaseException], bool] | None = None,
    ) -> tuple[str | None, list[AthenaDatabase]]:
        """List one page of the catalog's databases with ``ListDatabases``.

        Args:
            catalog_name: The catalog, or None for the cursor's catalog.
            next_token: The token of the page to read.
            max_results: The page size.
            logging_: Whether to log a failed request.
            stop_on: Stops the retries at an exception it accepts; used for
                the first attempt of the Glue fallback.

        Returns:
            The next page's token, or None, and the page's databases.

        Raises:
            OperationalError: If the request fails.
        """
        request = self._build_list_databases_request(
            catalog_name=catalog_name,
            next_token=next_token,
            max_results=max_results,
        )
        try:
            response = await async_retry_api_call(
                self.connection._client.list_databases,
                config=self._retry_config,
                logger=_logger,
                stop_on=stop_on,
                **request,
            )
        except Exception as e:
            if logging_:
                _logger.exception("Failed to list databases.")
            raise OperationalError(*e.args) from e
        else:
            return response.get("NextToken"), [
                AthenaDatabase({"Database": r}) for r in response.get("DatabaseList", [])
            ]

    @override
    async def list_databases(  # type: ignore[override]
        self,
        catalog_name: str | None,
        max_results: int | None = None,
    ) -> list[AthenaDatabase]:
        # Pages already read are kept, so a retried request resumes after them.
        """List the catalog's databases.

        In ``AwsDataCatalog`` and S3 Tables catalogs, a throttled request is
        answered from the AWS Glue Data Catalog; see ``glue_metadata_fallback``.

        Args:
            catalog_name: The catalog, or None for the cursor's catalog.
            max_results: The page size of each request.

        Returns:
            The catalog's databases.

        Raises:
            OperationalError: If the request fails.
        """
        databases: list[AthenaDatabase] = []
        next_token = None

        async def athena_request(
            stop_on: Callable[[BaseException], bool] | None, logging_: bool
        ) -> list[AthenaDatabase]:
            nonlocal next_token
            while True:
                next_token, response = await self._list_databases(
                    catalog_name=catalog_name,
                    next_token=next_token,
                    max_results=max_results,
                    logging_=logging_,
                    stop_on=stop_on,
                )
                databases.extend(response)
                if not next_token:
                    return databases

        return await self._async_with_glue_fallback(
            catalog_name, athena_request, GlueMetadataClient.list_databases, "list databases"
        )

    @override
    async def _get_table_metadata(  # type: ignore[override]
        self,
        table_name: str,
        catalog_name: str | None = None,
        schema_name: str | None = None,
        logging_: bool = True,
        stop_on: Callable[[BaseException], bool] | None = None,
    ) -> AthenaTableMetadata:
        """Get one table's metadata with ``GetTableMetadata``.

        Args:
            table_name: The table name.
            catalog_name: The catalog, or None for the cursor's catalog.
            schema_name: The database, or None for the cursor's schema.
            logging_: Whether to log a failed request.
            stop_on: Stops the retries at an exception it accepts; used for
                the first attempt of the Glue fallback.

        Returns:
            The table's metadata.

        Raises:
            OperationalError: If the request fails.
        """
        request = self._build_get_table_metadata_request(
            table_name=table_name,
            catalog_name=catalog_name,
            schema_name=schema_name,
        )
        try:
            response = await async_retry_api_call(
                self._connection.client.get_table_metadata,
                config=self._retry_config,
                logger=_logger,
                stop_on=stop_on,
                **request,
            )
        except Exception as e:
            if logging_:
                _logger.exception("Failed to get table metadata.")
            raise OperationalError(*e.args) from e
        else:
            return AthenaTableMetadata(response)

    @override
    async def get_table_metadata(  # type: ignore[override]
        self,
        table_name: str,
        catalog_name: str | None = None,
        schema_name: str | None = None,
        logging_: bool = True,
    ) -> AthenaTableMetadata:
        """Get one table's metadata.

        In ``AwsDataCatalog`` and S3 Tables catalogs, a throttled request is
        answered from the AWS Glue Data Catalog; see ``glue_metadata_fallback``.

        Args:
            table_name: The table name.
            catalog_name: The catalog, or None for the cursor's catalog.
            schema_name: The database, or None for the cursor's schema.
            logging_: Whether to log a failed request.

        Returns:
            The table's metadata.

        Raises:
            OperationalError: If the request fails, including when the table does
                not exist.
        """
        schema_name = schema_name if schema_name else self._schema_name
        return await self._async_with_glue_fallback(
            catalog_name,
            lambda stop_on, logging_: self._get_table_metadata(
                table_name=table_name,
                catalog_name=catalog_name,
                schema_name=schema_name,
                logging_=logging_,
                stop_on=stop_on,
            ),
            lambda glue, catalog: glue.get_table(catalog, schema_name, table_name),
            "get table metadata",
            logging_=logging_,
            absence_is_final=True,
        )

    @override
    async def _list_table_metadata(  # type: ignore[override]
        self,
        catalog_name: str | None = None,
        schema_name: str | None = None,
        expression: str | None = None,
        next_token: str | None = None,
        max_results: int | None = None,
        logging_: bool = True,
        stop_on: Callable[[BaseException], bool] | None = None,
    ) -> tuple[str | None, list[AthenaTableMetadata]]:
        """List one page of a database's table metadata with ``ListTableMetadata``.

        Args:
            catalog_name: The catalog, or None for the cursor's catalog.
            schema_name: The database, or None for the cursor's schema.
            expression: A table name pattern.
            next_token: The token of the page to read.
            max_results: The page size.
            logging_: Whether to log a failed request.
            stop_on: Stops the retries at an exception it accepts; used for
                the first attempt of the Glue fallback.

        Returns:
            The next page's token, or None, and the page's table metadata.

        Raises:
            OperationalError: If the request fails.
        """
        request = self._build_list_table_metadata_request(
            catalog_name=catalog_name,
            schema_name=schema_name,
            expression=expression,
            next_token=next_token,
            max_results=max_results,
        )
        try:
            response = await async_retry_api_call(
                self.connection._client.list_table_metadata,
                config=self._retry_config,
                logger=_logger,
                stop_on=stop_on,
                **request,
            )
        except Exception as e:
            if logging_:
                _logger.exception("Failed to list table metadata.")
            raise OperationalError(*e.args) from e
        else:
            return response.get("NextToken"), [
                AthenaTableMetadata({"TableMetadata": r})
                for r in response.get("TableMetadataList", [])
            ]

    @override
    async def list_table_metadata(  # type: ignore[override]
        self,
        catalog_name: str | None = None,
        schema_name: str | None = None,
        expression: str | None = None,
        max_results: int | None = None,
        logging_: bool = True,
    ) -> list[AthenaTableMetadata]:
        """List a database's table metadata.

        In ``AwsDataCatalog`` and S3 Tables catalogs, a throttled request is
        answered from the AWS Glue Data Catalog; see ``glue_metadata_fallback``.

        Args:
            catalog_name: The catalog, or None for the cursor's catalog.
            schema_name: The database, or None for the cursor's schema.
            expression: A table name pattern.
            max_results: The page size of each request.
            logging_: Whether to log a failed request.

        Returns:
            The metadata of the database's tables.

        Raises:
            OperationalError: If the request fails.
        """
        schema_name = schema_name if schema_name else self._schema_name
        # Pages already read are kept, so a retried request resumes after them.
        metadata: list[AthenaTableMetadata] = []
        next_token = None

        async def athena_request(
            stop_on: Callable[[BaseException], bool] | None, logging_: bool
        ) -> list[AthenaTableMetadata]:
            nonlocal next_token
            while True:
                next_token, response = await self._list_table_metadata(
                    catalog_name=catalog_name,
                    schema_name=schema_name,
                    expression=expression,
                    next_token=next_token,
                    max_results=max_results,
                    logging_=logging_,
                    stop_on=stop_on,
                )
                metadata.extend(response)
                if not next_token:
                    return metadata

        return await self._async_with_glue_fallback(
            catalog_name,
            athena_request,
            lambda glue, catalog: glue.list_tables(catalog, schema_name, expression),
            "list table metadata",
            logging_=logging_,
        )


class WithAsyncFetch(WithResultSet, AioBaseCursor, CursorIterator):
    """Base class of the asyncio SQL cursors.

    Combines ``WithResultSet`` with ``AioBaseCursor`` and ``CursorIterator``,
    and provides async fetch, ``executemany``, and ``cancel``, async
    iteration, and the async context manager protocol. The fetch methods run
    the result set's synchronous fetch with ``asyncio.to_thread``; a subclass
    whose result set fetches asynchronously overrides them. Synchronous
    iteration raises ``TypeError``.

    Subclasses override ``execute()`` and optionally ``__init__`` and
    format-specific helpers.
    """

    @override
    async def executemany(  # type: ignore[override]
        self,
        operation: str,
        seq_of_parameters: list[dict[str, Any] | list[str] | None],
        **kwargs,
    ) -> None:
        """Execute a SQL query multiple times with different parameters.

        On success, ``rowcount`` is the sum of the affected row counts, or
        -1 if any execution has an unknown count. An empty parameter list
        sets it to 0. On failure or cancellation, it is -1; earlier executions
        are not rolled back. Result sets are discarded.

        On failure, ``query_id`` retains the current query ID when available.
        If parameter iteration fails, this can identify the last successful
        execution.

        Args:
            operation: SQL query string to execute.
            seq_of_parameters: Sequence of parameter sets, one per execution.
            **kwargs: Additional keyword arguments passed to each ``execute()``.
        """
        self._reset_state()
        rowcount = 0
        try:
            for parameters in seq_of_parameters:
                await self.execute(operation, parameters, **kwargs)
                count = self.rowcount
                rowcount = rowcount + count if rowcount >= 0 and count >= 0 else -1
        except BaseException:
            # Keep the query ID available for diagnostics and explicit cancellation.
            self.close()
            self.result_set = None
            raise
        self._reset_state()
        self._rowcount = rowcount

    async def cancel(self) -> None:
        """Cancel the currently executing query.

        Raises:
            ProgrammingError: If no query is currently executing.
        """
        if not self.query_id:
            raise ProgrammingError("QueryExecutionId is none or empty.")
        await self._cancel(self.query_id)

    @override
    async def fetchone(
        self,
    ) -> tuple[Any | None, ...] | dict[Any, Any | None] | None:
        """Fetch the next row of the result set.

        Wraps the synchronous fetch in ``asyncio.to_thread`` to avoid
        blocking the event loop.

        Returns:
            A tuple representing the next row, or None if no more rows.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaResultSet, self.result_set)
        return await asyncio.to_thread(result_set.fetchone)

    @override
    async def fetchmany(
        self, size: int | None = None
    ) -> list[tuple[Any | None, ...] | dict[Any, Any | None]]:
        """Fetch multiple rows from the result set.

        Wraps the synchronous fetch in ``asyncio.to_thread`` to avoid
        blocking the event loop.

        Args:
            size: Maximum number of rows to fetch. If None or not positive,
                ``arraysize`` is used.

        Returns:
            List of tuples representing the fetched rows.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaResultSet, self.result_set)
        return await asyncio.to_thread(result_set.fetchmany, size)

    @override
    async def fetchall(
        self,
    ) -> list[tuple[Any | None, ...] | dict[Any, Any | None]]:
        """Fetch all remaining rows from the result set.

        Wraps the synchronous fetch in ``asyncio.to_thread`` to avoid
        blocking the event loop.

        Returns:
            List of tuples representing all remaining rows.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaResultSet, self.result_set)
        return await asyncio.to_thread(result_set.fetchall)

    @override
    def __iter__(self) -> NoReturn:
        """Reject synchronous iteration; use ``async for`` instead.

        Raises:
            TypeError: Always, because the fetch methods are coroutines.
        """
        raise TypeError(f"'{type(self).__name__}' object is not iterable; use 'async for' instead.")

    def __aiter__(self):
        return self

    async def __anext__(self):
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
