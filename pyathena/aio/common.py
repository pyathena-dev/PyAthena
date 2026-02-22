from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from pyathena.aio.util import async_retry_api_call
from pyathena.common import BaseCursor, CursorIterator
from pyathena.error import DatabaseError, OperationalError, ProgrammingError
from pyathena.model import AthenaDatabase, AthenaQueryExecution, AthenaTableMetadata
from pyathena.result_set import AthenaResultSet, WithResultSet

_logger = logging.getLogger(__name__)


class AioBaseCursor(BaseCursor):
    """Async base cursor that overrides I/O methods with async equivalents.

    Reuses ``BaseCursor.__init__``, all ``_build_*`` methods, and constants.
    Only the methods that perform network I/O or blocking sleep are overridden
    to use ``asyncio.to_thread`` / ``asyncio.sleep``.
    """

    async def _execute(  # type: ignore[override]
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
    ) -> str:
        query, execution_parameters = self._prepare_query(operation, parameters, paramstyle)

        request = self._build_start_query_execution_request(
            query=query,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            execution_parameters=execution_parameters,
        )
        query_id = await self._find_previous_query_id(
            query,
            work_group,
            cache_size=cache_size if cache_size else 0,
            cache_expiration_time=cache_expiration_time if cache_expiration_time else 0,
        )
        if query_id is None:
            try:
                response = await async_retry_api_call(
                    self._connection.client.start_query_execution,
                    config=self._retry_config,
                    logger=_logger,
                    **request,
                )
                query_id = response.get("QueryExecutionId")
            except Exception as e:
                _logger.exception("Failed to execute query.")
                raise DatabaseError(*e.args) from e
        return query_id

    async def _get_query_execution(self, query_id: str) -> AthenaQueryExecution:  # type: ignore[override]
        request = {"QueryExecutionId": query_id}
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

    async def __poll(self, query_id: str) -> AthenaQueryExecution:
        while True:
            query_execution = await self._get_query_execution(query_id)
            if query_execution.state in [
                AthenaQueryExecution.STATE_SUCCEEDED,
                AthenaQueryExecution.STATE_FAILED,
                AthenaQueryExecution.STATE_CANCELLED,
            ]:
                return query_execution
            await asyncio.sleep(self._poll_interval)

    async def _poll(self, query_id: str) -> AthenaQueryExecution:  # type: ignore[override]
        try:
            query_execution = await self.__poll(query_id)
        except asyncio.CancelledError:
            if self._kill_on_interrupt:
                _logger.warning("Query canceled by user.")
                await self._cancel(query_id)
                query_execution = await self.__poll(query_id)
            else:
                raise
        return query_execution

    async def _cancel(self, query_id: str) -> None:  # type: ignore[override]
        request = {"QueryExecutionId": query_id}
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

    async def _find_previous_query_id(  # type: ignore[override]
        self,
        query: str,
        work_group: str | None,
        cache_size: int = 0,
        cache_expiration_time: int = 0,
    ) -> str | None:
        query_id = None
        if cache_size == 0 and cache_expiration_time > 0:
            cache_size = sys.maxsize
        if cache_expiration_time > 0:
            expiration_time = datetime.now(timezone.utc) - timedelta(seconds=cache_expiration_time)
        else:
            expiration_time = datetime.now(timezone.utc)
        try:
            next_token = None
            while cache_size > 0:
                max_results = min(cache_size, self.LIST_QUERY_EXECUTIONS_MAX_RESULTS)
                cache_size -= max_results
                next_token, query_executions = await self._list_query_executions(
                    work_group, next_token=next_token, max_results=max_results
                )
                for execution in sorted(
                    (
                        e
                        for e in query_executions
                        if e.state == AthenaQueryExecution.STATE_SUCCEEDED
                        and e.statement_type == AthenaQueryExecution.STATEMENT_TYPE_DML
                    ),
                    key=lambda e: e.completion_date_time,  # type: ignore[arg-type, return-value]
                    reverse=True,
                ):
                    if (
                        cache_expiration_time > 0
                        and execution.completion_date_time
                        and execution.completion_date_time.astimezone(timezone.utc)
                        < expiration_time
                    ):
                        next_token = None
                        break
                    if execution.query == query:
                        query_id = execution.query_id
                        break
                if query_id or next_token is None:
                    break
        except Exception:
            _logger.warning("Failed to check the cache. Moving on without cache.", exc_info=True)
        return query_id

    async def _list_databases(  # type: ignore[override]
        self,
        catalog_name: str | None,
        next_token: str | None = None,
        max_results: int | None = None,
    ) -> tuple[str | None, list[AthenaDatabase]]:
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
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to list databases.")
            raise OperationalError(*e.args) from e
        else:
            return response.get("NextToken"), [
                AthenaDatabase({"Database": r}) for r in response.get("DatabaseList", [])
            ]

    async def list_databases(  # type: ignore[override]
        self,
        catalog_name: str | None,
        max_results: int | None = None,
    ) -> list[AthenaDatabase]:
        databases: list[AthenaDatabase] = []
        next_token = None
        while True:
            next_token, response = await self._list_databases(
                catalog_name=catalog_name,
                next_token=next_token,
                max_results=max_results,
            )
            databases.extend(response)
            if not next_token:
                break
        return databases

    async def _get_table_metadata(  # type: ignore[override]
        self,
        table_name: str,
        catalog_name: str | None = None,
        schema_name: str | None = None,
        logging_: bool = True,
    ) -> AthenaTableMetadata:
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
                **request,
            )
        except Exception as e:
            if logging_:
                _logger.exception("Failed to get table metadata.")
            raise OperationalError(*e.args) from e
        else:
            return AthenaTableMetadata(response)

    async def get_table_metadata(  # type: ignore[override]
        self,
        table_name: str,
        catalog_name: str | None = None,
        schema_name: str | None = None,
        logging_: bool = True,
    ) -> AthenaTableMetadata:
        return await self._get_table_metadata(
            table_name=table_name,
            catalog_name=catalog_name,
            schema_name=schema_name,
            logging_=logging_,
        )

    async def _list_table_metadata(  # type: ignore[override]
        self,
        catalog_name: str | None = None,
        schema_name: str | None = None,
        expression: str | None = None,
        next_token: str | None = None,
        max_results: int | None = None,
    ) -> tuple[str | None, list[AthenaTableMetadata]]:
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
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to list table metadata.")
            raise OperationalError(*e.args) from e
        else:
            return response.get("NextToken"), [
                AthenaTableMetadata({"TableMetadata": r})
                for r in response.get("TableMetadataList", [])
            ]

    async def list_table_metadata(  # type: ignore[override]
        self,
        catalog_name: str | None = None,
        schema_name: str | None = None,
        expression: str | None = None,
        max_results: int | None = None,
    ) -> list[AthenaTableMetadata]:
        metadata: list[AthenaTableMetadata] = []
        next_token = None
        while True:
            next_token, response = await self._list_table_metadata(
                catalog_name=catalog_name,
                schema_name=schema_name,
                expression=expression,
                next_token=next_token,
                max_results=max_results,
            )
            metadata.extend(response)
            if not next_token:
                break
        return metadata


class WithAsyncFetch(AioBaseCursor, CursorIterator, WithResultSet):
    """Mixin providing shared fetch, lifecycle, and async protocol for SQL cursors.

    Provides properties (``arraysize``, ``result_set``, ``query_id``,
    ``rownumber``, ``rowcount``), lifecycle methods (``close``, ``executemany``,
    ``cancel``), default sync fetch (for cursors whose result sets load all
    data eagerly in ``__init__``), and the async iteration protocol.

    Subclasses override ``execute()`` and optionally ``__init__`` and
    format-specific helpers.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._query_id: str | None = None
        self._result_set: AthenaResultSet | None = None

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value <= 0:
            raise ProgrammingError("arraysize must be a positive integer value.")
        self._arraysize = value

    @property  # type: ignore[override]
    def result_set(self) -> AthenaResultSet | None:
        return self._result_set

    @result_set.setter
    def result_set(self, val) -> None:
        self._result_set = val

    @property
    def query_id(self) -> str | None:
        return self._query_id

    @query_id.setter
    def query_id(self, val) -> None:
        self._query_id = val

    @property
    def rownumber(self) -> int | None:
        return self.result_set.rownumber if self.result_set else None

    @property
    def rowcount(self) -> int:
        return self.result_set.rowcount if self.result_set else -1

    def close(self) -> None:
        """Close the cursor and release associated resources."""
        if self.result_set and not self.result_set.is_closed:
            self.result_set.close()

    async def executemany(  # type: ignore[override]
        self,
        operation: str,
        seq_of_parameters: list[dict[str, Any] | list[str] | None],
        **kwargs,
    ) -> None:
        """Execute a SQL query multiple times with different parameters.

        Args:
            operation: SQL query string to execute.
            seq_of_parameters: Sequence of parameter sets, one per execution.
            **kwargs: Additional keyword arguments passed to each ``execute()``.
        """
        for parameters in seq_of_parameters:
            await self.execute(operation, parameters, **kwargs)
        # Operations that have result sets are not allowed with executemany.
        self._reset_state()

    async def cancel(self) -> None:
        """Cancel the currently executing query.

        Raises:
            ProgrammingError: If no query is currently executing.
        """
        if not self.query_id:
            raise ProgrammingError("QueryExecutionId is none or empty.")
        await self._cancel(self.query_id)

    def fetchone(
        self,
    ) -> tuple[Any | None, ...] | dict[Any, Any | None] | None:
        """Fetch the next row of the result set.

        Returns:
            A tuple representing the next row, or None if no more rows.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.fetchone()

    def fetchmany(
        self, size: int | None = None
    ) -> list[tuple[Any | None, ...] | dict[Any, Any | None]]:
        """Fetch multiple rows from the result set.

        Args:
            size: Maximum number of rows to fetch. Defaults to arraysize.

        Returns:
            List of tuples representing the fetched rows.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.fetchmany(size)

    def fetchall(
        self,
    ) -> list[tuple[Any | None, ...] | dict[Any, Any | None]]:
        """Fetch all remaining rows from the result set.

        Returns:
            List of tuples representing all remaining rows.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.fetchall()

    def __aiter__(self):
        return self

    async def __anext__(self):
        row = self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
