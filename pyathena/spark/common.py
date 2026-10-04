# Copyright 2017 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Base classes shared by the Athena for Apache Spark cursors."""

from __future__ import annotations

import contextlib
import logging
import time
import uuid
from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Any, cast

import botocore

from pyathena import DatabaseError, NotSupportedError, OperationalError
from pyathena.common import BaseCursor
from pyathena.model import (
    AthenaCalculationExecution,
    AthenaCalculationExecutionStatus,
    AthenaQueryExecution,
    AthenaSessionStatus,
)
from pyathena.util import override, parse_output_location, retry_api_call

_logger = logging.getLogger(__name__)


class SparkBaseCursor(BaseCursor, metaclass=ABCMeta):
    """Abstract base class for Spark-enabled cursor implementations.

    This class provides the foundational functionality for executing PySpark code
    on Amazon Athena for Apache Spark. It manages Spark sessions, handles
    calculation execution lifecycle, and provides utilities for reading
    results from S3.

    Features:
        - Automatic Spark session management and lifecycle
        - Configurable engine resources (DPU allocation)
        - Session idle timeout and automatic cleanup
        - Standard output and error stream access via S3
        - Calculation execution status monitoring
        - Session validation and error handling

    Attributes:
        session_id: The Athena Spark session identifier.
        calculation_id: ID of the current calculation being executed.

    Note:
        This is an abstract base class used by concrete Spark cursor implementations
        like SparkCursor and AsyncSparkCursor. It should not be instantiated directly.
    """

    def __init__(
        self,
        session_id: str | None = None,
        description: str | None = None,
        engine_configuration: dict[str, Any] | None = None,
        notebook_version: str | None = None,
        session_idle_timeout_minutes: int | None = None,
        terminate_session_on_close: bool | None = None,
        **kwargs,
    ) -> None:
        """Initialize the cursor and start or attach to a Spark session.

        If waiting for a newly started session fails, that session is terminated
        regardless of ``terminate_session_on_close``; a supplied session is not.

        Args:
            session_id: ID of an existing session to use. If omitted, a new
                session is started.
            description: Description of a new session.
            engine_configuration: Engine configuration of a new session.
                Defaults to ``get_default_engine_configuration()``.
            notebook_version: Notebook version of a new session.
            session_idle_timeout_minutes: Idle timeout of a new session in minutes.
            terminate_session_on_close: Whether ``close()`` terminates the session.
                If None, only a session started by this cursor is terminated;
                a session supplied with ``session_id`` is left running.
            **kwargs: Arguments passed to ``BaseCursor``.

        Raises:
            OperationalError: If the supplied session does not exist, or the
                session cannot be started or does not become idle.
        """
        super().__init__(**kwargs)
        self._engine_configuration = (
            engine_configuration
            if engine_configuration
            else self.get_default_engine_configuration()
        )
        self._notebook_version = notebook_version
        self._session_description = description
        self._session_idle_timeout_minutes = session_idle_timeout_minutes
        if terminate_session_on_close is None:
            # Only a session started by this cursor.
            terminate_session_on_close = not session_id
        self._terminate_session_on_close = terminate_session_on_close
        self._calculation_id: str | None = None
        self._calculation_execution: AthenaCalculationExecution | None = None

        # Created before the session so that a local failure cannot leave
        # a newly started session behind.
        self._client = self.connection.s3_client

        if session_id:
            if self._exists_session(session_id):
                self._session_id = session_id
            else:
                raise OperationalError(f"Session: {session_id} not found.")
        else:
            self._session_id = self._start_session()

    @property
    def session_id(self) -> str:
        """The ID of the Spark session that this cursor runs calculations in."""
        return self._session_id

    @property
    def calculation_id(self) -> str | None:
        """The ID of the calculation tracked by this cursor, or None if there is none."""
        return self._calculation_id

    @staticmethod
    def get_default_engine_configuration() -> dict[str, Any]:
        """Return the engine configuration used when none is given.

        Returns:
            The ``EngineConfiguration`` of a new session: a coordinator DPU size of 1,
            at most 2 concurrent DPUs, and a default executor DPU size of 1.
        """
        return {
            "CoordinatorDpuSize": 1,
            "MaxConcurrentDpus": 2,
            "DefaultExecutorDpuSize": 1,
        }

    def _read_s3_file_as_text(self, uri) -> str:
        bucket, key = parse_output_location(uri)
        response = retry_api_call(
            self._client.get_object,
            config=self._retry_config,
            logger=_logger,
            Bucket=bucket,
            Key=key,
        )
        return cast(str, response["Body"].read().decode("utf-8").strip())

    def _get_session_status(self, session_id: str):
        request: dict[str, Any] = {"SessionId": session_id}
        try:
            response = retry_api_call(
                self._connection.client.get_session_status,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to get session status.")
            raise OperationalError(*e.args) from e
        else:
            return AthenaSessionStatus(response)

    def _wait_for_idle_session(self, session_id: str) -> None:
        """Poll a Spark session with ``GetSessionStatus`` until it is idle.

        Args:
            session_id: The session ID.

        Raises:
            OperationalError: If the session is terminated, degraded, or failed,
                or if the request fails.
        """
        while True:
            session_status = self._get_session_status(session_id)
            if session_status.state in [AthenaSessionStatus.STATE_IDLE]:
                break
            if session_status.state in [
                AthenaSessionStatus.STATE_TERMINATED,
                AthenaSessionStatus.STATE_DEGRADED,
                AthenaSessionStatus.STATE_FAILED,
            ]:
                message = f"Session: {session_id} is {session_status.state}."
                if session_status.state_change_reason:
                    message += f" {session_status.state_change_reason}"
                raise OperationalError(message)
            time.sleep(self._poll_interval)

    def _exists_session(self, session_id: str) -> bool:
        """Whether a Spark session exists, from ``GetSession``.

        Waits for an existing session to become idle before returning.

        Args:
            session_id: The session ID.

        Returns:
            True if the session exists; False if Athena rejects it with
            ``InvalidRequestException``.

        Raises:
            OperationalError: If the request fails for another reason, or if the
                session is terminated, degraded, or failed.
        """
        request: dict[str, Any] = {"SessionId": session_id}
        try:
            retry_api_call(
                self._connection.client.get_session,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            if (
                isinstance(e, botocore.exceptions.ClientError)
                and e.response["Error"]["Code"] == "InvalidRequestException"
            ):
                _logger.exception(f"Session: {session_id} not found.")
                return False
            raise OperationalError(*e.args) from e
        else:
            self._wait_for_idle_session(session_id)
            return True

    def _start_session(self) -> str:
        """Start a Spark session with ``StartSession`` and wait until it is idle.

        If waiting for the new session raises, including ``KeyboardInterrupt``,
        the session is terminated on a best-effort basis before the exception is
        re-raised.

        Returns:
            The ID of the new session.

        Raises:
            OperationalError: If the session cannot be started or does not
                become idle.
        """
        request: dict[str, Any] = {
            "WorkGroup": self._work_group,
            "EngineConfiguration": self._engine_configuration,
        }
        if self._session_description:
            request.update({"Description": self._session_description})
        if self._notebook_version:
            request.update({"NotebookVersion": self._notebook_version})
        if self._session_idle_timeout_minutes:
            request.update({"SessionIdleTimeoutInMinutes": self._session_idle_timeout_minutes})
        try:
            session_id: str = retry_api_call(
                self._connection.client.start_session,
                config=self._retry_config,
                logger=_logger,
                **request,
            )["SessionId"]
        except Exception as e:
            _logger.exception("Failed to start session.")
            raise OperationalError(*e.args) from e

        try:
            self._wait_for_idle_session(session_id)
        except BaseException:
            # The caller receives no cursor to close, so the session is released here.
            with contextlib.suppress(OperationalError):
                # Already logged with the session ID; the original error takes precedence.
                self._terminate_session_by_id(session_id)
            raise
        return session_id

    def _terminate_session(self) -> None:
        """Terminate the cursor's Spark session with ``TerminateSession``.

        Raises:
            OperationalError: If the request fails.
        """
        self._terminate_session_by_id(self._session_id)

    def _terminate_session_by_id(self, session_id: str) -> None:
        """Terminate a Spark session with ``TerminateSession``.

        Session startup calls this synchronously in every cursor variant,
        including those that override ``_terminate_session`` with a coroutine,
        so subclasses must not override this method with a coroutine.

        Args:
            session_id: The session ID.

        Raises:
            OperationalError: If the request fails.
        """
        request: dict[str, Any] = {"SessionId": session_id}
        try:
            retry_api_call(
                self._connection.client.terminate_session,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception(f"Failed to terminate session: {session_id}.")
            raise OperationalError(*e.args) from e

    @override
    def _poll_until_terminal(
        self, query_id: str
    ) -> AthenaQueryExecution | AthenaCalculationExecution:
        """Poll a calculation execution until it reaches a terminal state.

        Calls ``on_poll`` with every status and sleeps ``poll_interval`` seconds
        between requests.

        Args:
            query_id: The calculation execution ID.

        Returns:
            The calculation execution in a terminal state.

        Raises:
            OperationalError: If a status request fails.
        """
        while True:
            calculation_status = self._get_calculation_execution_status(query_id)
            if self._on_poll:
                self._on_poll(calculation_status)
            if calculation_status.state in AthenaCalculationExecutionStatus.TERMINAL_STATES:
                return self._get_calculation_execution(query_id)
            time.sleep(self._poll_interval)

    @override
    def _cancel_and_wait(self, calculation_id: str) -> None:
        """Request cancellation and store the calculation's terminal state.

        Args:
            calculation_id: The calculation execution ID.

        Raises:
            OperationalError: If the cancellation or a status request fails.
        """
        self._cancel(calculation_id)
        self._calculation_execution = cast(
            AthenaCalculationExecution, self._poll_until_terminal(calculation_id)
        )

    def _calculate(
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

        With ``kill_on_interrupt`` enabled, the request runs on a helper thread.
        On ``KeyboardInterrupt``, the cursor first tries to abandon the request.
        This succeeds only if the helper has not begun the request by then; the
        helper then never sends it, and the interrupt propagates. Otherwise the
        cursor waits for the request to finish, requests cancellation of the
        calculation it started, waits for a terminal state, stores the calculation
        ID and execution on the cursor, and re-raises the interrupt. Another
        ``KeyboardInterrupt`` during that wait propagates at once.

        Args:
            session_id: The session ID.
            code_block: The code to run.
            description: The calculation description.
            client_request_token: The idempotency token of the request.

        Returns:
            The calculation execution ID.

        Raises:
            KeyboardInterrupt: If interrupted while starting the calculation. A
                failure to start, cancel, or wait for the calculation becomes its
                ``__cause__``.
            DatabaseError: If the request fails.
        """
        request = self._build_start_calculation_execution_request(
            session_id=session_id,
            code_block=code_block,
            description=description,
            client_request_token=client_request_token or str(uuid.uuid4()),
        )
        return self._start_execution(lambda: self._start_calculation_execution(request))

    @override
    def _set_interrupted_execution_id(self, execution_id: str) -> None:
        """Keep the ID of a calculation started by an interrupted start request.

        Args:
            execution_id: The calculation execution ID.
        """
        self._calculation_id = execution_id

    def _start_calculation_execution(self, request: dict[str, Any]) -> str:
        """Send a ``StartCalculationExecution`` request.

        Args:
            request: The request parameters.

        Returns:
            The calculation execution ID.

        Raises:
            DatabaseError: If the request fails.
        """
        try:
            response = retry_api_call(
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
    def _cancel(self, query_id: str) -> None:
        """Stop a calculation execution with ``StopCalculationExecution``.

        Args:
            query_id: The calculation execution ID.

        Raises:
            OperationalError: If the request fails.
        """
        request: dict[str, Any] = {"CalculationExecutionId": query_id}
        try:
            retry_api_call(
                self._connection.client.stop_calculation_execution,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to cancel calculation.")
            raise OperationalError(*e.args) from e

    @override
    def close(self) -> None:
        """Close the cursor, terminating its Spark session if configured to.

        See ``terminate_session_on_close``. After a successful termination,
        further calls do not terminate the session again; after a failed one,
        calling this method again retries it.

        Raises:
            OperationalError: If terminating the session fails.
        """
        if self._terminate_session_on_close:
            self._terminate_session()
            # Terminated; later calls do nothing.
            self._terminate_session_on_close = False

    @override
    def executemany(
        self,
        operation: str,
        seq_of_parameters: list[dict[str, Any] | list[str] | None],
        **kwargs,
    ) -> None:
        raise NotSupportedError


class WithCalculationExecution:
    """Mixin class providing access to Spark calculation execution properties.

    This mixin provides property accessors for calculation execution metadata
    and status information. It's designed to be mixed with cursor classes
    that execute Spark calculations on Athena.

    Properties:
        - description: Human-readable description of the calculation
        - working_directory: S3 path where calculation files are stored
        - state: Current execution state (COMPLETED, FAILED, etc.)
        - state_change_reason: Explanation for state changes
        - submission_date_time: When the calculation was submitted
        - completion_date_time: When the calculation completed
        - dpu_execution_in_millis: DPU execution time in milliseconds
        - progress: Current execution progress information
        - std_out_s3_uri: S3 URI for standard output
        - std_error_s3_uri: S3 URI for standard error
        - result_s3_uri: S3 URI for calculation results
        - result_type: Type of result produced by the calculation

    Note:
        This class requires that the implementing class provides
        calculation_execution, session_id, and calculation_id properties.
    """

    def __init__(self):
        """Initialize the mixin, which keeps no state of its own."""
        super().__init__()

    @property
    @abstractmethod
    def calculation_execution(self) -> AthenaCalculationExecution | None:
        """The calculation execution that the other properties read, or None."""
        raise NotImplementedError  # pragma: no cover

    @property
    @abstractmethod
    def session_id(self) -> str:
        """The ID of the Spark session that runs the calculations."""
        raise NotImplementedError  # pragma: no cover

    @property
    @abstractmethod
    def calculation_id(self) -> str | None:
        """The ID of the current calculation, or None."""
        raise NotImplementedError  # pragma: no cover

    @property
    def description(self) -> str | None:
        """The ``Description`` of the calculation, or None if there is none."""
        if not self.calculation_execution:
            return None
        return self.calculation_execution.description

    @property
    def working_directory(self) -> str | None:
        """The ``WorkingDirectory`` of the calculation, or None if there is none."""
        if not self.calculation_execution:
            return None
        return self.calculation_execution.working_directory

    @property
    def state(self) -> str | None:
        """The ``State`` of the calculation, or None if there is none."""
        if not self.calculation_execution:
            return None
        return self.calculation_execution.state

    @property
    def state_change_reason(self) -> str | None:
        """The ``StateChangeReason`` of the calculation, or None if there is none."""
        if not self.calculation_execution:
            return None
        return self.calculation_execution.state_change_reason

    @property
    def submission_date_time(self) -> datetime | None:
        """The ``SubmissionDateTime`` of the calculation, or None if there is none."""
        if not self.calculation_execution:
            return None
        return self.calculation_execution.submission_date_time

    @property
    def completion_date_time(self) -> datetime | None:
        """The ``CompletionDateTime`` of the calculation, or None if there is none."""
        if not self.calculation_execution:
            return None
        return self.calculation_execution.completion_date_time

    @property
    def dpu_execution_in_millis(self) -> int | None:
        """The ``DpuExecutionInMillis`` statistic of the calculation, or None if there is none."""
        if not self.calculation_execution:
            return None
        return self.calculation_execution.dpu_execution_in_millis

    @property
    def progress(self) -> str | None:
        """The ``Progress`` statistic of the calculation, or None if there is none."""
        if not self.calculation_execution:
            return None
        return self.calculation_execution.progress

    @property
    def std_out_s3_uri(self) -> str | None:
        """The ``StdOutS3Uri`` of the calculation, or None if there is none."""
        if not self.calculation_execution:
            return None
        return self.calculation_execution.std_out_s3_uri

    @property
    def std_error_s3_uri(self) -> str | None:
        """The ``StdErrorS3Uri`` of the calculation, or None if there is none."""
        if not self.calculation_execution:
            return None
        return self.calculation_execution.std_error_s3_uri

    @property
    def result_s3_uri(self) -> str | None:
        """The ``ResultS3Uri`` of the calculation, or None if there is none."""
        if not self.calculation_execution:
            return None
        return self.calculation_execution.result_s3_uri

    @property
    def result_type(self) -> str | None:
        """The ``ResultType`` of the calculation, or None if there is none."""
        if not self.calculation_execution:
            return None
        return self.calculation_execution.result_type
