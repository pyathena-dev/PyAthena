# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Union, cast

from pyathena.aio.common import AioBaseCursor
from pyathena.arrow.converter import (
    DefaultArrowTypeConverter,
    DefaultArrowUnloadTypeConverter,
)
from pyathena.arrow.result_set import AthenaArrowResultSet
from pyathena.common import CursorIterator
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaCompression, AthenaFileFormat, AthenaQueryExecution
from pyathena.result_set import WithResultSet

if TYPE_CHECKING:
    import polars as pl
    from pyarrow import Table

_logger = logging.getLogger(__name__)  # type: ignore


class AioArrowCursor(AioBaseCursor, CursorIterator, WithResultSet):
    """Native asyncio cursor that returns results as Apache Arrow Tables.

    Uses ``asyncio.to_thread()`` to create the result set off the event loop.
    Since ``AthenaArrowResultSet`` loads all data in ``__init__`` (via S3),
    fetch methods are synchronous (in-memory only) and do not need to be async.

    Example:
        >>> async with await pyathena.aconnect(...) as conn:
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
        on_start_query_execution: Optional[Callable[[str], None]] = None,
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
        self._on_start_query_execution = on_start_query_execution
        self._connect_timeout = connect_timeout
        self._request_timeout = request_timeout
        self._query_id: Optional[str] = None
        self._result_set: Optional[AthenaArrowResultSet] = None

    @staticmethod
    def get_default_converter(
        unload: bool = False,
    ) -> Union[DefaultArrowTypeConverter, DefaultArrowUnloadTypeConverter, Any]:
        if unload:
            return DefaultArrowUnloadTypeConverter()
        return DefaultArrowTypeConverter()

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value <= 0:
            raise ProgrammingError("arraysize must be a positive integer value.")
        self._arraysize = value

    @property  # type: ignore
    def result_set(self) -> Optional[AthenaArrowResultSet]:
        return self._result_set

    @result_set.setter
    def result_set(self, val) -> None:
        self._result_set = val

    @property
    def query_id(self) -> Optional[str]:
        return self._query_id

    @query_id.setter
    def query_id(self, val) -> None:
        self._query_id = val

    @property
    def rownumber(self) -> Optional[int]:
        return self.result_set.rownumber if self.result_set else None

    @property
    def rowcount(self) -> int:
        return self.result_set.rowcount if self.result_set else -1

    def close(self) -> None:
        """Close the cursor and release associated resources."""
        if self.result_set and not self.result_set.is_closed:
            self.result_set.close()

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
        on_start_query_execution: Optional[Callable[[str], None]] = None,
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
            on_start_query_execution: Callback called when query starts.
            **kwargs: Additional execution parameters.

        Returns:
            Self reference for method chaining.
        """
        self._reset_state()
        if self._unload:
            s3_staging_dir = s3_staging_dir if s3_staging_dir else self._s3_staging_dir
            if not s3_staging_dir:
                raise ProgrammingError("If the unload option is used, s3_staging_dir is required.")
            operation, unload_location = self._formatter.wrap_unload(
                operation,
                s3_staging_dir=s3_staging_dir,
                format_=AthenaFileFormat.FILE_FORMAT_PARQUET,
                compression=AthenaCompression.COMPRESSION_SNAPPY,
            )
        else:
            unload_location = None
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

        if self._on_start_query_execution:
            self._on_start_query_execution(self.query_id)
        if on_start_query_execution:
            on_start_query_execution(self.query_id)
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

    async def executemany(  # type: ignore[override]
        self,
        operation: str,
        seq_of_parameters: List[Optional[Union[Dict[str, Any], List[str]]]],
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
    ) -> Optional[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch the next row of the result set.

        Returns:
            A tuple representing the next row, or None if no more rows.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaArrowResultSet, self.result_set)
        return result_set.fetchone()

    def fetchmany(
        self, size: Optional[int] = None
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
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
        result_set = cast(AthenaArrowResultSet, self.result_set)
        return result_set.fetchmany(size)

    def fetchall(
        self,
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch all remaining rows from the result set.

        Returns:
            List of tuples representing all remaining rows.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaArrowResultSet, self.result_set)
        return result_set.fetchall()

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
