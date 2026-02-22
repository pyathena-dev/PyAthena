from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)

from pyathena.aio.util import async_retry_api_call
from pyathena.converter import Converter
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaDictResultSet, AthenaResultSet
from pyathena.util import RetryConfig

if TYPE_CHECKING:
    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)


class AthenaAioResultSet(AthenaResultSet):
    """Async result set that provides async fetch methods.

    Skips the synchronous ``_pre_fetch`` by passing ``_pre_fetch=False`` to
    the parent ``__init__`` and provides an ``async create()`` classmethod
    factory instead.
    """

    def __init__(
        self,
        connection: Connection[Any],
        converter: Converter,
        query_execution: AthenaQueryExecution,
        arraysize: int,
        retry_config: RetryConfig,
    ) -> None:
        super().__init__(
            connection=connection,
            converter=converter,
            query_execution=query_execution,
            arraysize=arraysize,
            retry_config=retry_config,
            _pre_fetch=False,
        )

    @classmethod
    async def create(
        cls,
        connection: Connection[Any],
        converter: Converter,
        query_execution: AthenaQueryExecution,
        arraysize: int,
        retry_config: RetryConfig,
    ) -> AthenaAioResultSet:
        """Async factory method.

        Creates an ``AthenaAioResultSet`` and awaits the initial data fetch.

        Args:
            connection: The database connection.
            converter: Type converter for result values.
            query_execution: Query execution metadata.
            arraysize: Number of rows to fetch per request.
            retry_config: Retry configuration for API calls.

        Returns:
            A fully initialized ``AthenaAioResultSet``.
        """
        result_set = cls(connection, converter, query_execution, arraysize, retry_config)
        if result_set.state == AthenaQueryExecution.STATE_SUCCEEDED:
            await result_set._async_pre_fetch()
        return result_set

    async def __async_get_query_results(
        self, max_results: int, next_token: str | None = None
    ) -> dict[str, Any]:
        if not self.query_id:
            raise ProgrammingError("QueryExecutionId is none or empty.")
        if self.state != AthenaQueryExecution.STATE_SUCCEEDED:
            raise ProgrammingError("QueryExecutionState is not SUCCEEDED.")
        if self.is_closed:
            raise ProgrammingError("AthenaAioResultSet is closed.")
        request: dict[str, Any] = {
            "QueryExecutionId": self.query_id,
            "MaxResults": max_results,
        }
        if next_token:
            request["NextToken"] = next_token
        try:
            response = await async_retry_api_call(
                self.connection.client.get_query_results,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to fetch result set.")
            raise OperationalError(*e.args) from e
        else:
            return cast(dict[str, Any], response)

    async def __async_fetch(self, next_token: str | None = None) -> dict[str, Any]:
        return await self.__async_get_query_results(self._arraysize, next_token)

    async def _async_fetch(self) -> None:
        if not self._next_token:
            raise ProgrammingError("NextToken is none or empty.")
        response = await self.__async_fetch(self._next_token)
        rows, self._next_token = self._parse_result_rows(response)
        self._process_rows(rows)

    async def _async_pre_fetch(self) -> None:
        response = await self.__async_fetch()
        self._process_metadata(response)
        self._process_update_count(response)
        rows, self._next_token = self._parse_result_rows(response)
        offset = 1 if rows and self._is_first_row_column_labels(rows) else 0
        self._process_rows(rows, offset)

    async def fetchone(  # type: ignore[override]
        self,
    ) -> tuple[Any | None, ...] | dict[Any, Any | None] | None:
        """Fetch the next row of the result set.

        Automatically fetches the next page from Athena when the current
        page is exhausted and more pages are available.

        Returns:
            A tuple representing the next row, or None if no more rows.
        """
        if not self._rows and self._next_token:
            await self._async_fetch()
        if not self._rows:
            return None
        if self._rownumber is None:
            self._rownumber = 0
        self._rownumber += 1
        return self._rows.popleft()

    async def fetchmany(  # type: ignore[override]
        self, size: int | None = None
    ) -> list[tuple[Any | None, ...] | dict[Any, Any | None]]:
        """Fetch multiple rows from the result set.

        Args:
            size: Maximum number of rows to fetch. If None, uses arraysize.

        Returns:
            List of row tuples. May contain fewer rows than requested if
            fewer are available.
        """
        if not size or size <= 0:
            size = self._arraysize
        rows = []
        for _ in range(size):
            row = await self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    async def fetchall(  # type: ignore[override]
        self,
    ) -> list[tuple[Any | None, ...] | dict[Any, Any | None]]:
        """Fetch all remaining rows from the result set.

        Returns:
            List of all remaining row tuples.
        """
        rows = []
        while True:
            row = await self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def __aiter__(self):
        return self

    async def __anext__(self):
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row


class AthenaAioDictResultSet(AthenaDictResultSet, AthenaAioResultSet):
    """Async result set that returns rows as dictionaries.

    Inherits ``_get_rows`` from ``AthenaDictResultSet`` and async fetch
    methods from ``AthenaAioResultSet`` via multiple inheritance.
    """
