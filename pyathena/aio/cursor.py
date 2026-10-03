# Copyright 2017 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Native asyncio cursors that return rows as tuples or dictionaries."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, cast

from pyathena.aio.common import WithAsyncFetch
from pyathena.aio.result_set import AthenaAioDictResultSet, AthenaAioResultSet
from pyathena.common import CursorIterator
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.options import ExecuteOptions
from pyathena.util import override

_logger = logging.getLogger(__name__)


class AioCursor(WithAsyncFetch):
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
        s3_staging_dir: str | None = None,
        schema_name: str | None = None,
        catalog_name: str | None = None,
        work_group: str | None = None,
        poll_interval: float = 1,
        encryption_option: str | None = None,
        kms_key: str | None = None,
        kill_on_interrupt: bool = True,
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
        **kwargs,
    ) -> None:
        """Initialize an AioCursor.

        Args:
            s3_staging_dir: S3 location for query results.
            schema_name: Default schema name.
            catalog_name: Default catalog name.
            work_group: Athena workgroup name.
            poll_interval: Query status polling interval in seconds.
            encryption_option: S3 encryption option (SSE_S3, SSE_KMS, CSE_KMS).
            kms_key: KMS key for encryption.
            kill_on_interrupt: Stop the running query when polling is cancelled
                with ``asyncio.CancelledError``.
            result_reuse_enable: Enable Athena query result reuse.
            result_reuse_minutes: Maximum age in minutes of a reused result.
            **kwargs: Arguments forwarded to ``WithResultSet.__init__`` and
                ``AioBaseCursor.__init__``, such as ``arraysize``, ``connection``,
                ``converter``, ``formatter``, and ``retry_config``.
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
        self._result_set: AthenaAioResultSet | None = None
        self._result_set_class = AthenaAioResultSet

    @property  # type: ignore[explicit-override]  # python/mypy#15900
    @override
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value <= 0 or value > self.DEFAULT_FETCH_SIZE:
            raise ProgrammingError(
                f"MaxResults is more than maximum allowed length {self.DEFAULT_FETCH_SIZE}."
            )
        self._arraysize = value

    @override
    async def execute(
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
        on_start_query_execution: Callable[[str], None] | None = None,
        result_set_type_hints: dict[str | int, str] | None = None,
        *,
        options: ExecuteOptions | None = None,
        **kwargs,
    ) -> AioCursor:
        """Execute a SQL query asynchronously.

        Args:
            operation: SQL query string to execute.
            parameters: Query parameters (optional).
            work_group: Athena workgroup to use (optional).
            s3_staging_dir: S3 location for query results (optional).
            cache_size: Number of queries to check for result caching (optional).
            cache_expiration_time: Cache expiration time in seconds (optional).
            result_reuse_enable: Enable result reuse (optional).
            result_reuse_minutes: Result reuse duration in minutes (optional).
            paramstyle: Parameter style to use (optional).
            on_start_query_execution: Callback invoked with the query ID before ``execute()``
                waits for the query: after the ``StartQueryExecution`` call, or after a
                reusable query ID is found through ``cache_size``.
            result_set_type_hints: Optional dictionary mapping column names to
                Athena DDL type signatures for precise type conversion within
                complex types.
            options: Shared execution options as an
                :class:`~pyathena.options.ExecuteOptions` instance. Individual
                keyword arguments take precedence over ``options`` fields.
            **kwargs: Additional execution parameters.

        Returns:
            Self reference for method chaining.
        """
        self._reset_state()
        options = ExecuteOptions.resolve(
            options,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
            cache_expiration_time=cache_expiration_time,
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            paramstyle=paramstyle,
            on_start_query_execution=on_start_query_execution,
            result_set_type_hints=result_set_type_hints,
        )
        self.query_id = await self._execute(
            operation,
            parameters=parameters,
            options=options,
        )

        # Call user callbacks immediately after start_query_execution
        self._call_on_start_query_execution(self.query_id, options)

        query_execution = await self._poll(self.query_id)
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self.result_set = await self._result_set_class.create(
                self._connection,
                self._converter,
                query_execution,
                self.arraysize,
                self._retry_config,
                result_set_type_hints=options.result_set_type_hints,
            )
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self

    @override
    async def fetchone(
        self,
    ) -> Any | dict[Any, Any | None] | None:
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

    @override
    async def fetchmany(self, size: int | None = None) -> list[Any | dict[Any, Any | None]]:
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

    @override
    async def fetchall(
        self,
    ) -> list[Any | dict[Any, Any | None]]:
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
        """Initialize an AioDictCursor.

        Args:
            **kwargs: Arguments forwarded to ``AioCursor.__init__``. If they include
                ``dict_type``, it is also assigned to the class attribute
                ``AthenaAioDictResultSet.dict_type``, the type used to build each row.
        """
        super().__init__(**kwargs)
        self._result_set_class = AthenaAioDictResultSet
        if "dict_type" in kwargs:
            AthenaAioDictResultSet.dict_type = kwargs["dict_type"]
