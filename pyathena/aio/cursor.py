# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Union, cast

from pyathena.aio.base import AioCursorBase
from pyathena.aio.result_set import AthenaAioDictResultSet, AthenaAioResultSet
from pyathena.common import CursorIterator
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution

_logger = logging.getLogger(__name__)  # type: ignore


class AioCursor(AioCursorBase):
    """Native asyncio cursor for Amazon Athena.

    Unlike ``AsyncCursor`` (which uses ``ThreadPoolExecutor``), this cursor
    uses ``asyncio.sleep`` for polling and ``asyncio.to_thread`` for boto3
    calls, keeping the event loop free.

    Example:
        >>> async with AioConnection.create(...) as conn:
        ...     async with conn.cursor() as cursor:
        ...         await cursor.execute("SELECT * FROM my_table")
        ...         rows = await cursor.fetchall()
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
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
        on_start_query_execution: Optional[Callable[[str], None]] = None,
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
            on_start_query_execution=on_start_query_execution,
            **kwargs,
        )
        self._result_set: Optional[AthenaAioResultSet] = None
        self._result_set_class = AthenaAioResultSet

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value <= 0 or value > self.DEFAULT_FETCH_SIZE:
            raise ProgrammingError(
                f"MaxResults is more than maximum allowed length {self.DEFAULT_FETCH_SIZE}."
            )
        self._arraysize = value

    async def execute(  # type: ignore[override]
        self,
        operation: str,
        parameters: Optional[Union[Dict[str, Any], List[str]]] = None,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        cache_size: int = 0,
        cache_expiration_time: int = 0,
        result_reuse_enable: Optional[bool] = None,
        result_reuse_minutes: Optional[int] = None,
        paramstyle: Optional[str] = None,
        on_start_query_execution: Optional[Callable[[str], None]] = None,
        **kwargs,
    ) -> "AioCursor":
        """Execute a SQL query asynchronously.

        Args:
            operation: SQL query string to execute.
            parameters: Query parameters (optional).
            work_group: Athena workgroup to use (optional).
            s3_staging_dir: S3 location for query results (optional).
            cache_size: Query result cache size (optional).
            cache_expiration_time: Cache expiration time in seconds (optional).
            result_reuse_enable: Enable result reuse (optional).
            result_reuse_minutes: Result reuse duration in minutes (optional).
            paramstyle: Parameter style to use (optional).
            on_start_query_execution: Callback invoked with query_id after submission.
            **kwargs: Additional execution parameters.

        Returns:
            Self reference for method chaining.
        """
        self._reset_state()
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
            self.result_set = await self._result_set_class.create(
                self._connection,
                self._converter,
                query_execution,
                self.arraysize,
                self._retry_config,
            )
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self

    async def fetchone(  # type: ignore[override]
        self,
    ) -> Optional[Union[Any, Dict[Any, Optional[Any]]]]:
        """Fetch the next row of a query result set.

        Returns:
            A tuple representing the next row, or None if no more rows.

        Raises:
            ProgrammingError: If called before executing a query that
                returns results.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaAioResultSet, self.result_set)
        return await result_set.fetchone()

    async def fetchmany(  # type: ignore[override]
        self, size: Optional[int] = None
    ) -> List[Union[Any, Dict[Any, Optional[Any]]]]:
        """Fetch multiple rows from a query result set.

        Args:
            size: Maximum number of rows to fetch. If None, uses arraysize.

        Returns:
            List of tuples representing the fetched rows.

        Raises:
            ProgrammingError: If called before executing a query that
                returns results.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaAioResultSet, self.result_set)
        return await result_set.fetchmany(size)

    async def fetchall(  # type: ignore[override]
        self,
    ) -> List[Union[Any, Dict[Any, Optional[Any]]]]:
        """Fetch all remaining rows from a query result set.

        Returns:
            List of tuples representing all remaining rows in the result set.

        Raises:
            ProgrammingError: If called before executing a query that
                returns results.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaAioResultSet, self.result_set)
        return await result_set.fetchall()

    async def __anext__(self):
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row


class AioDictCursor(AioCursor):
    """Native asyncio cursor that returns rows as dictionaries.

    Example:
        >>> async with AioConnection.create(...) as conn:
        ...     cursor = conn.cursor(AioDictCursor)
        ...     await cursor.execute("SELECT id, name FROM users")
        ...     row = await cursor.fetchone()
        ...     print(row["name"])
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._result_set_class = AthenaAioDictResultSet
        if "dict_type" in kwargs:
            AthenaAioDictResultSet.dict_type = kwargs["dict_type"]
