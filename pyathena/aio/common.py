# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Union, cast

import pyathena
from pyathena.aio.util import async_retry_api_call
from pyathena.common import BaseCursor
from pyathena.error import DatabaseError, OperationalError
from pyathena.model import AthenaQueryExecution

_logger = logging.getLogger(__name__)  # type: ignore


class AioBaseCursor(BaseCursor):
    """Async base cursor that overrides I/O methods with async equivalents.

    Reuses ``BaseCursor.__init__``, all ``_build_*`` methods, and constants.
    Only the methods that perform network I/O or blocking sleep are overridden
    to use ``asyncio.to_thread`` / ``asyncio.sleep``.
    """

    async def _execute(  # type: ignore[override]
        self,
        operation: str,
        parameters: Optional[Union[Dict[str, Any], List[str]]] = None,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        cache_size: Optional[int] = 0,
        cache_expiration_time: Optional[int] = 0,
        result_reuse_enable: Optional[bool] = None,
        result_reuse_minutes: Optional[int] = None,
        paramstyle: Optional[str] = None,
    ) -> str:
        if pyathena.paramstyle == "qmark" or paramstyle == "qmark":
            query = operation
            execution_parameters = cast(Optional[List[str]], parameters)
        else:
            query = self._formatter.format(operation, cast(Optional[Dict[str, Any]], parameters))
            execution_parameters = None
        _logger.debug(query)

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
        self, query_ids: List[str]
    ) -> List[AthenaQueryExecution]:
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
        work_group: Optional[str] = None,
        next_token: Optional[str] = None,
        max_results: Optional[int] = None,
    ) -> Tuple[Optional[str], List[AthenaQueryExecution]]:
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
        work_group: Optional[str],
        cache_size: int = 0,
        cache_expiration_time: int = 0,
    ) -> Optional[str]:
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
                    key=lambda e: e.completion_date_time,  # type: ignore
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
