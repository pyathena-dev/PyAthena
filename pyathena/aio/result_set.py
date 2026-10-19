"""Asyncio result sets that fetch Athena query results with ``GetQueryResults``."""

from __future__ import annotations

import logging
import operator
from typing import (
    TYPE_CHECKING,
    Any,
    NoReturn,
    cast,
)

from pyathena.aio.util import async_retry_api_call
from pyathena.converter import Converter
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import (
    _CLOSED_MESSAGE,
    _FETCH_FAILED_MESSAGE,
    AthenaDictResultSet,
    AthenaResultSet,
)
from pyathena.util import RetryConfig, override

if TYPE_CHECKING:
    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)


class AthenaAioResultSet(AthenaResultSet):
    """Async result set that provides async fetch methods.

    Skips the synchronous ``_pre_fetch`` by passing ``_pre_fetch=False`` to
    the parent ``__init__`` and provides an ``async create()`` classmethod
    factory instead. Synchronous iteration raises ``TypeError``; use
    ``async for`` instead.
    """

    def __init__(
        self,
        connection: Connection[Any],
        converter: Converter,
        query_execution: AthenaQueryExecution,
        arraysize: int,
        retry_config: RetryConfig,
        result_set_type_hints: dict[str | int, str] | None = None,
    ) -> None:
        """Initialize the result set without fetching rows; ``create()`` fetches the first page.

        Args:
            connection: The connection that ran the query.
            converter: The converter for result values.
            query_execution: The query execution whose results to read.
            arraysize: The number of rows per ``GetQueryResults`` page and the default
                ``fetchmany()`` size.
            retry_config: The retry configuration for API calls.
            result_set_type_hints: Athena type signatures for complex-type columns,
                keyed by column name (case-insensitive) or zero-based column index.

        Raises:
            ProgrammingError: If ``query_execution`` is not given.
        """
        super().__init__(
            connection=connection,
            converter=converter,
            query_execution=query_execution,
            arraysize=arraysize,
            retry_config=retry_config,
            _pre_fetch=False,
            result_set_type_hints=result_set_type_hints,
        )

    @classmethod
    async def create(
        cls,
        connection: Connection[Any],
        converter: Converter,
        query_execution: AthenaQueryExecution,
        arraysize: int,
        retry_config: RetryConfig,
        result_set_type_hints: dict[str | int, str] | None = None,
        **kwargs: Any,
    ) -> AthenaAioResultSet:
        """Async factory method.

        Creates an ``AthenaAioResultSet`` and awaits the initial data fetch.

        Args:
            connection: The database connection.
            converter: Type converter for result values.
            query_execution: Query execution metadata.
            arraysize: Number of rows to fetch per request.
            retry_config: Retry configuration for API calls.
            result_set_type_hints: Athena type signatures for complex-type columns,
                keyed by column name (case-insensitive) or zero-based column index.
            **kwargs: Additional arguments passed to the constructor of ``cls``,
                such as ``dict_type`` for ``AthenaAioDictResultSet``.

        Returns:
            A fully initialized ``AthenaAioResultSet``.
        """
        result_set = cls(
            connection,
            converter,
            query_execution,
            arraysize,
            retry_config,
            result_set_type_hints=result_set_type_hints,
            **kwargs,
        )
        if result_set.state == AthenaQueryExecution.STATE_SUCCEEDED:
            await result_set._async_pre_fetch()
        return result_set

    async def _async_get_query_results(
        self, max_results: int, next_token: str | None = None
    ) -> dict[str, Any]:
        """Get a page of query results with ``GetQueryResults``.

        Args:
            max_results: The maximum number of rows in the page.
            next_token: The token of the page to get; the first page if None.

        Returns:
            The ``GetQueryResults`` response.

        Raises:
            ProgrammingError: If the query ID is missing, the query has not
                succeeded, or the result set is closed.
            OperationalError: If the request fails.
        """
        if self.is_closed:
            raise ProgrammingError("AthenaAioResultSet is closed.")
        request = self._reader.build_get_query_results_request(max_results, next_token)
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

    async def _async_fetch(self) -> None:
        """Fetch the next page of rows, if any, into the buffer.

        Nothing is fetched when no page remains, as after ``close()``.

        Raises:
            ProgrammingError: If the next page is requested and the query ID is
                missing or the query has not succeeded.
            OperationalError: If the request fails.
        """
        if not self._next_token:
            return
        response = await self._async_get_query_results(self._arraysize, self._next_token)
        rows, self._next_token = self._parse_result_rows(response)
        self._process_rows(rows)

    async def _async_pre_fetch(self) -> None:
        """Fetch the first page of rows along with the result metadata.

        Raises:
            ProgrammingError: If the query ID is missing, the query has not
                succeeded, or the result set is closed.
            OperationalError: If the request fails.
        """
        response = await self._async_get_query_results(self._arraysize)
        self._process_metadata(response)
        self._process_update_count(response)
        rows, self._next_token = self._parse_result_rows(response)
        offset = 1 if rows and self._is_first_row_column_labels(rows) else 0
        self._process_rows(rows, offset)

    @override
    async def fetchone(  # type: ignore[override]
        self,
    ) -> tuple[Any | None, ...] | dict[Any, Any | None] | None:
        """Fetch the next row of the result set.

        Automatically fetches the next page from Athena when the current
        page is exhausted and more pages are available.

        Returns:
            The next row (a tuple, or a dict for ``AthenaAioDictResultSet``), or
            None if no more rows.

        Raises:
            ProgrammingError: If the result set is closed, or an earlier fetch
                failed or was cancelled.
        """
        # As is_closed does, inline on the per-row path.
        if self._connection is None:
            raise ProgrammingError(_CLOSED_MESSAGE)
        if self._fetch_failed:
            raise ProgrammingError(_FETCH_FAILED_MESSAGE)
        try:
            if not self._rows:
                await self._async_fetch()
                if not self._rows:
                    return None
            row = self._rows.popleft()
            # As _count_rows() does, inline on the per-row path.
            rownumber = self._rownumber
            if rownumber is not None:
                self._rownumber = rownumber + 1
        except BaseException:
            self._fetch_failed = True
            raise
        return row

    @override
    async def fetchmany(  # type: ignore[override]
        self, size: int | None = None
    ) -> list[tuple[Any | None, ...] | dict[Any, Any | None]]:
        """Fetch multiple rows from the result set.

        Args:
            size: Maximum number of rows to fetch. If None or not positive,
                ``arraysize`` is used.

        Returns:
            The rows, fewer than ``size`` when the result is exhausted.

        Raises:
            TypeError: If ``size`` is not an integer and not replaced with ``arraysize``.
            ProgrammingError: If the result set is closed, or an earlier fetch
                failed or was cancelled.
        """
        if not size or size <= 0:
            size = self._arraysize
        size = operator.index(size)
        self._ensure_open()
        if self._fetch_failed:
            raise ProgrammingError(_FETCH_FAILED_MESSAGE)
        try:
            return await self._async_take_rows(size)
        except BaseException:
            self._fetch_failed = True
            raise

    @override
    async def fetchall(  # type: ignore[override]
        self,
    ) -> list[tuple[Any | None, ...] | dict[Any, Any | None]]:
        """Fetch all remaining rows from the result set.

        Returns:
            The remaining rows.

        Raises:
            ProgrammingError: If the result set is closed, or an earlier fetch
                failed or was cancelled.
        """
        self._ensure_open()
        if self._fetch_failed:
            raise ProgrammingError(_FETCH_FAILED_MESSAGE)
        try:
            return await self._async_take_rows(None)
        except BaseException:
            self._fetch_failed = True
            raise

    async def _async_take_rows(
        self, size: int | None
    ) -> list[tuple[Any | None, ...] | dict[Any, Any | None]]:
        """Take the next rows and count them in ``rownumber``.

        The buffered rows are taken in bulk, and ``_async_fetch()`` refills the buffer
        when it is empty.

        Args:
            size: The maximum number of rows, or None for all remaining rows.

        Returns:
            The rows, fewer than ``size`` when the result is exhausted.
        """
        rows: list[tuple[Any | None, ...] | dict[Any, Any | None]] = []
        while size is None or len(rows) < size:
            if not self._rows:
                await self._async_fetch()
                if not self._rows:
                    break
            self._take_buffered_rows(rows, size)
        return rows

    @override
    def __iter__(self) -> NoReturn:
        """Reject synchronous iteration; use ``async for`` instead.

        Raises:
            TypeError: Always, because the fetch methods are coroutines.
        """
        raise TypeError(f"'{type(self).__name__}' object is not iterable; use 'async for' instead.")

    @override
    def __next__(self) -> NoReturn:
        """Reject synchronous iteration with ``next()``; use ``async for`` instead.

        Raises:
            TypeError: Always, because the fetch methods are coroutines.
        """
        raise TypeError(
            f"'{type(self).__name__}' object is not an iterator; use 'async for' instead."
        )

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
