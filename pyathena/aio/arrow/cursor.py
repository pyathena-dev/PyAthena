# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union, cast

from pyathena.aio.common import WithAsyncFetch
from pyathena.arrow.converter import (
    DefaultArrowTypeConverter,
    DefaultArrowUnloadTypeConverter,
)
from pyathena.arrow.result_set import AthenaArrowResultSet
from pyathena.common import CursorIterator
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution

if TYPE_CHECKING:
    import polars as pl
    from pyarrow import Table

_logger = logging.getLogger(__name__)  # type: ignore


class AioArrowCursor(WithAsyncFetch):
    """Native asyncio cursor that returns results as Apache Arrow Tables.

    Uses ``asyncio.to_thread()`` for both result set creation and fetch
    operations, keeping the event loop free.

    Example:
        >>> async with await pyathena.aio_connect(...) as conn:
        ...     cursor = conn.cursor(AioArrowCursor)
        ...     await cursor.execute("SELECT * FROM my_table")
        ...     table = cursor.as_arrow()
    """

    def __init__(
        self,
        s3_staging_dir: Optional[str] = None,
        schema_name: Optional[str] = None,
        catalog_name: Optional[str] = None,
        work_group: Optional[str] = None,
        poll_interval: float = 1,
        encryption_option: Optional[str] = None,
        kms_key: Optional[str] = None,
        kill_on_interrupt: bool = True,
        unload: bool = False,
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
        connect_timeout: Optional[float] = None,
        request_timeout: Optional[float] = None,
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
        self._unload = unload
        self._connect_timeout = connect_timeout
        self._request_timeout = request_timeout
        self._result_set: Optional[AthenaArrowResultSet] = None

    @staticmethod
    def get_default_converter(
        unload: bool = False,
    ) -> Union[DefaultArrowTypeConverter, DefaultArrowUnloadTypeConverter, Any]:
        if unload:
            return DefaultArrowUnloadTypeConverter()
        return DefaultArrowTypeConverter()

    async def execute(  # type: ignore[override]
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
        **kwargs,
    ) -> "AioArrowCursor":
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
            **kwargs: Additional execution parameters.

        Returns:
            Self reference for method chaining.
        """
        self._reset_state()
        operation, unload_location = self._prepare_unload(operation, s3_staging_dir)
        self.query_id = await self._execute(
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

        query_execution = await self._poll(self.query_id)
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self.result_set = await asyncio.to_thread(
                AthenaArrowResultSet,
                connection=self._connection,
                converter=self._converter,
                query_execution=query_execution,
                arraysize=self.arraysize,
                retry_config=self._retry_config,
                unload=self._unload,
                unload_location=unload_location,
                connect_timeout=self._connect_timeout,
                request_timeout=self._request_timeout,
                **kwargs,
            )
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self

    async def fetchone(  # type: ignore[override]
        self,
    ) -> Optional[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
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
        result_set = cast(AthenaArrowResultSet, self.result_set)
        return await asyncio.to_thread(result_set.fetchone)

    async def fetchmany(  # type: ignore[override]
        self, size: Optional[int] = None
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch multiple rows from the result set.

        Wraps the synchronous fetch in ``asyncio.to_thread`` to avoid
        blocking the event loop.

        Args:
            size: Maximum number of rows to fetch. Defaults to arraysize.

        Returns:
            List of tuples representing the fetched rows.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaArrowResultSet, self.result_set)
        return await asyncio.to_thread(result_set.fetchmany, size)

    async def fetchall(  # type: ignore[override]
        self,
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
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
        result_set = cast(AthenaArrowResultSet, self.result_set)
        return await asyncio.to_thread(result_set.fetchall)

    async def __anext__(self):
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    def as_arrow(self) -> "Table":
        """Return query results as an Apache Arrow Table.

        Returns:
            Apache Arrow Table containing all query results.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaArrowResultSet, self.result_set)
        return result_set.as_arrow()

    def as_polars(self) -> "pl.DataFrame":
        """Return query results as a Polars DataFrame.

        Returns:
            Polars DataFrame containing all query results.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaArrowResultSet, self.result_set)
        return result_set.as_polars()
