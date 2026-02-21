# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, Union, cast

from pyathena.aio.util import async_retry_api_call
from pyathena.error import DatabaseError, NotSupportedError, OperationalError, ProgrammingError
from pyathena.model import (
    AthenaCalculationExecution,
    AthenaCalculationExecutionStatus,
    AthenaQueryExecution,
)
from pyathena.spark.common import SparkBaseCursor, WithCalculationExecution
from pyathena.util import parse_output_location

_logger = logging.getLogger(__name__)  # type: ignore


class AioSparkCursor(SparkBaseCursor, WithCalculationExecution):
    """Native asyncio cursor for executing PySpark code on Athena.

    Overrides post-init I/O methods of ``SparkBaseCursor`` with async
    equivalents.  Session management (``_exists_session``,
    ``_start_session``, etc.) stays synchronous because ``__init__``
    runs inside ``asyncio.to_thread``.

    Since ``SparkBaseCursor.__init__`` performs I/O (session management),
    cursor creation must be wrapped in ``asyncio.to_thread``::

        cursor = await asyncio.to_thread(conn.cursor)

    Example:
        >>> import asyncio
        >>> async with await pyathena.aio_connect(
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

    # --- async overrides of SparkBaseCursor I/O methods ---

    async def _get_calculation_execution_status(  # type: ignore[override]
        self, query_id: str
    ) -> AthenaCalculationExecutionStatus:
        request: Dict[str, Any] = {"CalculationExecutionId": query_id}
        try:
            response = await async_retry_api_call(
                self._connection.client.get_calculation_execution_status,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to get calculation execution status.")
            raise OperationalError(*e.args) from e
        else:
            return AthenaCalculationExecutionStatus(response)

    async def _get_calculation_execution(  # type: ignore[override]
        self, query_id: str
    ) -> AthenaCalculationExecution:
        request: Dict[str, Any] = {"CalculationExecutionId": query_id}
        try:
            response = await async_retry_api_call(
                self._connection.client.get_calculation_execution,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to get calculation execution.")
            raise OperationalError(*e.args) from e
        else:
            return AthenaCalculationExecution(response)

    async def _calculate(  # type: ignore[override]
        self,
        session_id: str,
        code_block: str,
        description: Optional[str] = None,
        client_request_token: Optional[str] = None,
    ) -> str:
        request = self._build_start_calculation_execution_request(
            session_id=session_id,
            code_block=code_block,
            description=description,
            client_request_token=client_request_token,
        )
        try:
            response = await async_retry_api_call(
                self._connection.client.start_calculation_execution,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
            calculation_id = response.get("CalculationExecutionId")
        except Exception as e:
            _logger.exception("Failed to execute calculation.")
            raise DatabaseError(*e.args) from e
        return cast(str, calculation_id)

    async def __poll(
        self, query_id: str
    ) -> Union[AthenaQueryExecution, AthenaCalculationExecution]:
        while True:
            calculation_status = await self._get_calculation_execution_status(query_id)
            if calculation_status.state in [
                AthenaCalculationExecutionStatus.STATE_COMPLETED,
                AthenaCalculationExecutionStatus.STATE_FAILED,
                AthenaCalculationExecutionStatus.STATE_CANCELED,
            ]:
                return await self._get_calculation_execution(query_id)
            await asyncio.sleep(self._poll_interval)

    async def _poll(  # type: ignore[override]
        self, query_id: str
    ) -> Union[AthenaQueryExecution, AthenaCalculationExecution]:
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
        request: Dict[str, Any] = {"CalculationExecutionId": query_id}
        try:
            await async_retry_api_call(
                self._connection.client.stop_calculation_execution,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to cancel calculation.")
            raise OperationalError(*e.args) from e

    async def _terminate_session(self) -> None:  # type: ignore[override]
        request: Dict[str, Any] = {"SessionId": self._session_id}
        try:
            await async_retry_api_call(
                self._connection.client.terminate_session,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to terminate session.")
            raise OperationalError(*e.args) from e

    async def _read_s3_file_as_text(self, uri) -> str:  # type: ignore[override]
        bucket, key = parse_output_location(uri)
        response = await async_retry_api_call(
            self._client.get_object,
            config=self._retry_config,
            logger=_logger,
            Bucket=bucket,
            Key=key,
        )
        return cast(str, response["Body"].read().decode("utf-8").strip())

    # --- public API ---

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

    async def close(self) -> None:  # type: ignore[override]
        """Close the cursor by terminating the Spark session."""
        await self._terminate_session()

    async def executemany(  # type: ignore[override]
        self,
        operation: str,
        seq_of_parameters: List[Optional[Union[Dict[str, Any], List[str]]]],
        **kwargs,
    ) -> None:
        raise NotSupportedError

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
