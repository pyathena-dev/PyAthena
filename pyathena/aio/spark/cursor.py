# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union, cast

from pyathena.aio.spark.common import AioSparkBaseCursor
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaCalculationExecution, AthenaCalculationExecutionStatus
from pyathena.spark.common import WithCalculationExecution

_logger = logging.getLogger(__name__)  # type: ignore


class AioSparkCursor(AioSparkBaseCursor, WithCalculationExecution):
    """Native asyncio cursor for executing PySpark code on Athena.

    Since ``SparkBaseCursor.__init__`` performs I/O (session management),
    cursor creation must be wrapped in ``asyncio.to_thread``::

        cursor = await asyncio.to_thread(conn.cursor)

    Example:
        >>> import asyncio
        >>> async with await pyathena.aconnect(
        ...     work_group="spark-workgroup",
        ...     cursor_class=AioSparkCursor,
        ... ) as conn:
        ...     cursor = await asyncio.to_thread(conn.cursor)
        ...     await cursor.execute("spark.sql('SELECT 1').show()")
        ...     print(await cursor.get_std_out())
    """

    def __init__(
        self,
        session_id: Optional[str] = None,
        description: Optional[str] = None,
        engine_configuration: Optional[Dict[str, Any]] = None,
        notebook_version: Optional[str] = None,
        session_idle_timeout_minutes: Optional[int] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            session_id=session_id,
            description=description,
            engine_configuration=engine_configuration,
            notebook_version=notebook_version,
            session_idle_timeout_minutes=session_idle_timeout_minutes,
            **kwargs,
        )

    @property
    def calculation_execution(self) -> Optional[AthenaCalculationExecution]:
        return self._calculation_execution

    async def get_std_out(self) -> Optional[str]:
        """Get the standard output from the Spark calculation execution.

        Returns:
            The standard output as a string, or None if no output is available.
        """
        if not self._calculation_execution or not self._calculation_execution.std_out_s3_uri:
            return None
        return await self._read_s3_file_as_text(self._calculation_execution.std_out_s3_uri)

    async def get_std_error(self) -> Optional[str]:
        """Get the standard error from the Spark calculation execution.

        Returns:
            The standard error as a string, or None if no error output is available.
        """
        if not self._calculation_execution or not self._calculation_execution.std_error_s3_uri:
            return None
        return await self._read_s3_file_as_text(self._calculation_execution.std_error_s3_uri)

    async def execute(  # type: ignore[override]
        self,
        operation: str,
        parameters: Optional[Union[Dict[str, Any], List[str]]] = None,
        session_id: Optional[str] = None,
        description: Optional[str] = None,
        client_request_token: Optional[str] = None,
        work_group: Optional[str] = None,
        **kwargs,
    ) -> "AioSparkCursor":
        """Execute PySpark code asynchronously.

        Args:
            operation: PySpark code to execute.
            parameters: Unused, kept for API compatibility.
            session_id: Spark session ID override.
            description: Calculation description.
            client_request_token: Idempotency token.
            work_group: Unused, kept for API compatibility.
            **kwargs: Additional parameters.

        Returns:
            Self reference for method chaining.
        """
        self._calculation_id = await self._calculate(
            session_id=session_id if session_id else self._session_id,
            code_block=operation,
            description=description,
            client_request_token=client_request_token,
        )
        self._calculation_execution = cast(
            AthenaCalculationExecution, await self._poll(self._calculation_id)
        )
        if self._calculation_execution.state != AthenaCalculationExecutionStatus.STATE_COMPLETED:
            std_error = await self.get_std_error()
            raise OperationalError(std_error)
        return self

    async def cancel(self) -> None:
        """Cancel the currently running calculation.

        Raises:
            ProgrammingError: If no calculation is running.
        """
        if not self.calculation_id:
            raise ProgrammingError("CalculationExecutionId is none or empty.")
        await self._cancel(self.calculation_id)

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
