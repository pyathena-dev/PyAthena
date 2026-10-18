# Copyright 2017 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Native asyncio cursor that runs PySpark code in an Athena for Apache Spark session."""

from __future__ import annotations

import asyncio
import functools
import logging
import uuid
from typing import TYPE_CHECKING, Any, cast

from pyathena.aio.util import async_retry_api_call, create_in_thread, run_in_thread
from pyathena.error import DatabaseError, NotSupportedError, OperationalError, ProgrammingError
from pyathena.model import (
    AthenaCalculationExecution,
    AthenaCalculationExecutionStatus,
    AthenaQueryExecution,
)
from pyathena.spark.common import SparkBaseCursor, WithCalculationExecution
from pyathena.util import override

if TYPE_CHECKING:
    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)


class AioSparkCursor(SparkBaseCursor, WithCalculationExecution):
    """Native asyncio cursor for executing PySpark code on Athena.

    Overrides post-init I/O methods of ``SparkBaseCursor`` with async
    equivalents.  Session management (``_exists_session``,
    ``_start_session``, etc.) stays synchronous because ``__init__``
    runs in a thread.

    Since ``SparkBaseCursor.__init__`` performs I/O (session management),
    create the cursor with :meth:`create`, which runs it in a thread::

        cursor = await AioSparkCursor.create(conn)

    Example:
        >>> async with await pyathena.aio_connect(
        ...     work_group="spark-workgroup",
        ...     cursor_class=AioSparkCursor,
        ... ) as conn:
        ...     cursor = await AioSparkCursor.create(conn)
        ...     await cursor.execute("spark.sql('SELECT 1').show()")
        ...     print(await cursor.get_std_out())
    """

    @classmethod
    async def create(cls, connection: Connection[Any], **kwargs: Any) -> AioSparkCursor:
        """Create a cursor with ``connection.cursor()`` in a thread.

        The cursor starts or attaches to its Spark session as ``__init__``
        describes, without blocking the event loop. If the caller is
        cancelled, a cursor built after the cancellation terminates the
        session as :meth:`close` would, also if the caller is cancelled
        again (see :func:`~pyathena.aio.util.create_in_thread`).

        Args:
            connection: The connection of the cursor.
            **kwargs: Arguments for ``connection.cursor()``, which override
                the connection's ``cursor_kwargs``.

        Returns:
            The cursor.

        Raises:
            OperationalError: If the supplied session does not exist, or the
                session cannot be started or does not become idle.
        """
        return await create_in_thread(
            functools.partial(connection.cursor, cls, **kwargs),
            # The blocking close of the base class, for a cursor that nobody awaits.
            release=SparkBaseCursor.close,
        )

    @property
    @override
    def calculation_execution(self) -> AthenaCalculationExecution | None:
        return self._calculation_execution

    # --- async overrides of SparkBaseCursor I/O methods ---

    @override
    async def _get_calculation_execution_status(  # type: ignore[override]
        self, query_id: str
    ) -> AthenaCalculationExecutionStatus:
        request: dict[str, Any] = {"CalculationExecutionId": query_id}
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

    @override
    async def _get_calculation_execution(  # type: ignore[override]
        self, query_id: str
    ) -> AthenaCalculationExecution:
        request: dict[str, Any] = {"CalculationExecutionId": query_id}
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

    @override
    async def _calculate(  # type: ignore[override]
        self,
        session_id: str,
        code_block: str,
        description: str | None = None,
        client_request_token: str | None = None,
    ) -> str:
        """Start a calculation execution with ``StartCalculationExecution``.

        Without ``client_request_token``, a generated token is sent, so that a
        retried request returns the calculation an earlier attempt started instead
        of starting another one.

        With ``kill_on_interrupt`` enabled, the request runs in a task shielded
        from task cancellation. On cancellation, the request is abandoned if that
        task has not begun it by then; it is never sent, and the cancellation
        propagates. Otherwise the cursor waits for the request to finish, requests
        cancellation of the calculation it started, waits for a terminal state,
        stores the calculation ID and execution on the cursor, and re-raises
        ``asyncio.CancelledError``. Another cancellation during that wait
        propagates at once.

        Args:
            session_id: The session ID.
            code_block: The code to run.
            description: The calculation description.
            client_request_token: The idempotency token of the request.

        Returns:
            The calculation execution ID.

        Raises:
            asyncio.CancelledError: If the task is cancelled while starting the
                calculation. A failure to start, cancel, or wait for the
                calculation becomes its ``__cause__``.
            DatabaseError: If the request fails.
        """
        request = self._build_start_calculation_execution_request(
            session_id=session_id,
            code_block=code_block,
            description=description,
            client_request_token=client_request_token or str(uuid.uuid4()),
        )
        if not self._kill_on_interrupt:
            return await self._start_calculation_execution(request)

        caller = asyncio.current_task()
        cancel_requests = caller.cancelling() if caller else 0

        async def run() -> str | None:
            # Begin the request only if the caller has not been cancelled since.
            if caller and caller.cancelling() > cancel_requests:
                return None
            return await self._start_calculation_execution(request)

        start = asyncio.ensure_future(run())
        try:
            return cast(str, await asyncio.shield(start))
        except asyncio.CancelledError as cancellation:
            try:
                calculation_id = await start
                if calculation_id is None:
                    # The task did not begin the request, so it was never sent.
                    raise cancellation
                _logger.warning("Query canceled by user.")
                self._calculation_id = calculation_id
                await self._cancel_and_wait(calculation_id)
            except Exception as e:
                raise cancellation from e
            raise

    @override
    async def _start_calculation_execution(  # type: ignore[override]
        self, request: dict[str, Any]
    ) -> str:
        """Send a ``StartCalculationExecution`` request.

        Args:
            request: The request parameters.

        Returns:
            The calculation execution ID.

        Raises:
            DatabaseError: If the request fails.
        """
        try:
            response = await async_retry_api_call(
                self._connection.client.start_calculation_execution,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to execute calculation.")
            raise DatabaseError(*e.args) from e
        return cast(str, response.get("CalculationExecutionId"))

    @override
    async def _poll_until_terminal(  # type: ignore[override]
        self, query_id: str
    ) -> AthenaQueryExecution | AthenaCalculationExecution:
        """Poll a calculation execution until it reaches a terminal state.

        Calls ``on_poll`` with every status and awaits ``poll_interval`` seconds
        between requests.

        Args:
            query_id: The calculation execution ID.

        Returns:
            The calculation execution in a terminal state.

        Raises:
            OperationalError: If a status request fails.
        """
        while True:
            calculation_status = await self._get_calculation_execution_status(query_id)
            if self._on_poll:
                self._on_poll(calculation_status)
            if calculation_status.state in AthenaCalculationExecutionStatus.TERMINAL_STATES:
                return await self._get_calculation_execution(query_id)
            await asyncio.sleep(self._poll_interval)

    @override
    async def _poll(  # type: ignore[override]
        self, query_id: str
    ) -> AthenaQueryExecution | AthenaCalculationExecution:
        """Wait for a calculation execution to reach a terminal state.

        On task cancellation with ``kill_on_interrupt`` enabled, requests
        cancellation, waits for the calculation to reach a terminal state, stores
        it as the cursor's calculation execution, and re-raises
        ``asyncio.CancelledError``.
        Cancellation is a best-effort request, so the terminal state can be
        ``COMPLETED`` or ``FAILED`` instead of ``CANCELED``.

        Args:
            query_id: The calculation execution ID.

        Returns:
            The calculation execution in a terminal state.

        Raises:
            asyncio.CancelledError: If the task is cancelled while waiting. A failure
                to cancel or wait for the calculation becomes its ``__cause__``.
            OperationalError: If a status request fails.
        """
        try:
            return await self._poll_until_terminal(query_id)
        except asyncio.CancelledError as cancellation:
            if not self._kill_on_interrupt:
                raise
            _logger.warning("Query canceled by user.")
            try:
                await self._cancel_and_wait(query_id)
            except Exception as e:
                raise cancellation from e
            raise

    @override
    async def _cancel_and_wait(self, calculation_id: str) -> None:  # type: ignore[override]
        """Request cancellation and store the calculation's terminal state.

        Args:
            calculation_id: The calculation execution ID.

        Raises:
            OperationalError: If the cancellation or a status request fails.
        """
        await self._cancel(calculation_id)
        self._calculation_execution = cast(
            AthenaCalculationExecution, await self._poll_until_terminal(calculation_id)
        )

    @override
    async def _cancel(self, query_id: str) -> None:  # type: ignore[override]
        request: dict[str, Any] = {"CalculationExecutionId": query_id}
        try:
            await async_retry_api_call(
                self._connection.client.stop_calculation_execution,
                config=self._retry_config,
                logger=_logger,
                stoppable=False,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to cancel calculation.")
            raise OperationalError(*e.args) from e

    async def _terminate_session(self) -> None:
        """Terminate the cursor's Spark session with ``TerminateSession``.

        The asyncio counterpart of ``_terminate_session_by_id()``, which the
        blocking close uses. Cancellation does not stop its retries.

        Raises:
            OperationalError: If the request fails.
        """
        request: dict[str, Any] = {"SessionId": self._session_id}
        try:
            await async_retry_api_call(
                self._connection.client.terminate_session,
                config=self._retry_config,
                logger=_logger,
                stoppable=False,
                **request,
            )
        except Exception as e:
            _logger.exception(f"Failed to terminate session: {self._session_id}.")
            raise OperationalError(*e.args) from e

    @override
    async def _read_s3_file_as_text(self, uri: str) -> str:  # type: ignore[override]
        return await run_in_thread(super()._read_s3_file_as_text, uri)

    # --- public API ---

    async def get_std_out(self) -> str | None:
        """Get the standard output from the Spark calculation execution.

        Returns:
            The standard output as a string, or None if no output is available.

        Raises:
            ProgrammingError: If the cursor is closed.
            OperationalError: If reading the file from S3 fails.
        """
        self._ensure_open()
        if not self._calculation_execution or not self._calculation_execution.std_out_s3_uri:
            return None
        return await self._read_s3_file_as_text(self._calculation_execution.std_out_s3_uri)

    async def get_std_error(self) -> str | None:
        """Get the standard error from the Spark calculation execution.

        Returns:
            The standard error as a string, or None if no error output is available.

        Raises:
            ProgrammingError: If the cursor is closed.
            OperationalError: If reading the file from S3 fails.
        """
        self._ensure_open()
        if not self._calculation_execution or not self._calculation_execution.std_error_s3_uri:
            return None
        return await self._read_s3_file_as_text(self._calculation_execution.std_error_s3_uri)

    @override
    async def execute(
        self,
        operation: str,
        parameters: dict[str, Any] | list[str] | None = None,
        *,
        session_id: str | None = None,
        description: str | None = None,
        client_request_token: str | None = None,
        **kwargs,
    ) -> AioSparkCursor:
        """Execute PySpark code asynchronously.

        Args:
            operation: PySpark code to execute.
            parameters: Must be None; Spark calculations do not support parameters.
            session_id: Spark session ID override.
            description: Calculation description.
            client_request_token: Idempotency token.
            **kwargs: Unknown keyword arguments raise TypeError.

        Returns:
            Self reference for method chaining.

        Raises:
            TypeError: If an unknown keyword argument is given.
            NotSupportedError: If ``parameters`` is not None.
            OperationalError: If the calculation does not complete successfully.
            ProgrammingError: If the cursor is closed.
        """
        self._ensure_open()
        self._validate_execute_kwargs(kwargs)
        if parameters is not None:
            raise NotSupportedError("Spark cursors do not support parameters.")
        # A failure below must not leave the previous calculation on the cursor.
        self._calculation_id = None
        self._calculation_execution = None
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
            ProgrammingError: If the cursor is closed or no calculation is running.
        """
        self._ensure_open()
        if not self.calculation_id:
            raise ProgrammingError("CalculationExecutionId is none or empty.")
        await self._cancel(self.calculation_id)

    @override
    async def close(self) -> None:  # type: ignore[override]
        """Close the cursor, terminating its Spark session if configured to.

        See ``terminate_session_on_close``. After a successful termination,
        further calls do not terminate the session again; after a failed one,
        calling this method again retries it.

        Later operations on the cursor raise ``ProgrammingError``.

        Raises:
            OperationalError: If terminating the session fails.
        """
        self._closed = True
        if self._terminate_session_on_close:
            await self._terminate_session()
            # Terminated; later calls do nothing.
            self._terminate_session_on_close = False

    @override
    async def executemany(  # type: ignore[override]
        self,
        operation: str,
        seq_of_parameters: list[dict[str, Any] | list[str] | None],
        **kwargs,
    ) -> None:
        """Reject ``executemany()``, which Spark cursors do not support.

        Args:
            operation: The code to run.
            seq_of_parameters: Sequence of parameter sets.
            **kwargs: Additional arguments.

        Raises:
            NotSupportedError: If the cursor is open.
            ProgrammingError: If the cursor is closed.
        """
        self._ensure_open()
        raise NotSupportedError

    def __aiter__(self):
        return self

    async def __anext__(self):
        self._ensure_open()
        raise StopAsyncIteration

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
