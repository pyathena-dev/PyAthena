# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, Union, cast

from pyathena.aio.util import async_retry_api_call
from pyathena.error import NotSupportedError, OperationalError
from pyathena.model import (
    AthenaCalculationExecution,
    AthenaCalculationExecutionStatus,
    AthenaQueryExecution,
)
from pyathena.spark.common import SparkBaseCursor
from pyathena.util import parse_output_location

_logger = logging.getLogger(__name__)  # type: ignore


class AioSparkBaseCursor(SparkBaseCursor):
    """Async base cursor for Spark calculations on Athena.

    Overrides post-init I/O methods with async equivalents.  Session
    management (``_exists_session``, ``_start_session``, etc.) stays
    synchronous because ``__init__`` runs inside ``asyncio.to_thread``.
    """

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
            raise OperationalError(*e.args) from e
        return cast(str, calculation_id)

    async def __poll(  # type: ignore[override]
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
        response = await asyncio.to_thread(
            self._client.get_object,
            Bucket=bucket,
            Key=key,
        )
        return cast(str, response["Body"].read().decode("utf-8").strip())

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
