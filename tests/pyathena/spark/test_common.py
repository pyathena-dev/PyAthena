# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import asyncio
import logging
import threading
import uuid
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
from botocore.exceptions import ClientError

from pyathena import DatabaseError, NotSupportedError, OperationalError
from pyathena.aio.spark.cursor import AioSparkCursor
from pyathena.model import AthenaCalculationExecutionStatus, AthenaSessionStatus
from pyathena.spark.async_cursor import AsyncSparkCursor
from pyathena.spark.common import SparkBaseCursor
from pyathena.spark.cursor import SparkCursor
from pyathena.util import RetryConfig
from tests.pyathena.util import interrupt_start_waits

SPARK_CURSOR_CLASSES = [SparkCursor, AsyncSparkCursor, AioSparkCursor]
SYNC_SPARK_CURSOR_CLASSES = [SparkCursor, AsyncSparkCursor]
_TIMEOUT = 10


def _session_status(state: str, reason: str | None = None) -> AthenaSessionStatus:
    return AthenaSessionStatus(
        {"SessionId": "session_id", "Status": {"State": state, "StateChangeReason": reason}}
    )


def _cursor() -> SparkCursor:
    cursor = SparkCursor.__new__(SparkCursor)  # bypass __init__ to avoid AWS calls
    cursor._connection = MagicMock()
    cursor._retry_config = RetryConfig()
    cursor._poll_interval = 0
    return cursor


def _connection():
    connection = MagicMock()
    connection.client.start_session.return_value = {"SessionId": "new-session"}
    connection.client.get_session.return_value = {"SessionId": "supplied-session"}
    connection.client.get_session_status.return_value = {
        "Status": {"State": AthenaSessionStatus.STATE_IDLE}
    }
    return connection


def _calculation_cursor(cursor_class, kill_on_interrupt=True):
    """A cursor whose calculation requests go to a mocked Athena client.

    Args:
        cursor_class: The synchronous Spark cursor class.
        kill_on_interrupt: Whether the cursor cancels the calculation on interrupt.

    Returns:
        The cursor. Its ``_cancel`` is a mock, and status requests report
        ``CANCELED``.
    """
    cursor = cursor_class.__new__(cursor_class)  # bypass __init__ to avoid AWS calls
    cursor._session_id = "session_id"
    cursor._connection = MagicMock()
    cursor._connection.client.start_calculation_execution.return_value = {
        "CalculationExecutionId": "calculation_id"
    }
    cursor._retry_config = RetryConfig(attempt=2, multiplier=0)
    cursor._poll_interval = 0
    cursor._kill_on_interrupt = kill_on_interrupt
    cursor._on_poll = None
    cursor._calculation_id = None
    cursor._calculation_execution = None
    cursor._cancel = MagicMock()
    cursor._get_calculation_execution_status = MagicMock(
        return_value=MagicMock(state=AthenaCalculationExecutionStatus.STATE_CANCELED)
    )
    cursor._get_calculation_execution = MagicMock(
        return_value=MagicMock(state=AthenaCalculationExecutionStatus.STATE_CANCELED)
    )
    return cursor


def _block_start(cursor, response=None):
    """Make the cursor's start request block until released.

    Args:
        cursor: The cursor from ``_calculation_cursor``.
        response: The exception to raise once released; the default returns
            ``calculation_id``.

    Returns:
        An event set when the request starts, and an event that releases it.
    """
    started = threading.Event()
    release = threading.Event()

    def start_calculation_execution(**kwargs):
        started.set()
        assert release.wait(_TIMEOUT)
        if response:
            raise response
        return {"CalculationExecutionId": "calculation_id"}

    cursor._connection.client.start_calculation_execution.side_effect = start_calculation_execution
    return started, release


def _init_cursor(cursor_class, connection, **kwargs):
    return cursor_class(
        connection=connection,
        converter=MagicMock(),
        formatter=MagicMock(),
        retry_config=RetryConfig(),
        s3_staging_dir=None,
        schema_name=None,
        catalog_name=None,
        work_group="spark",
        poll_interval=0,
        encryption_option=None,
        kms_key=None,
        kill_on_interrupt=False,
        result_reuse_enable=False,
        result_reuse_minutes=60,
        **kwargs,
    )


def _close(cursor) -> None:
    """Close a Spark cursor, running ``AioSparkCursor.close()`` to completion.

    Args:
        cursor: The cursor to close.
    """
    if isinstance(cursor, AioSparkCursor):
        asyncio.run(cursor.close())
    else:
        cursor.close()


@pytest.fixture(params=SPARK_CURSOR_CLASSES, ids=lambda cls: cls.__name__)
def execute_cursor(request):
    """A Spark cursor whose calculation requests are mocked.

    Args:
        request: The pytest fixture request, selecting the cursor class.

    Returns:
        The cursor, with ``previous`` as its last calculation ID.
    """
    cursor = request.param.__new__(request.param)  # bypass __init__ to avoid AWS calls
    cursor._session_id = "session_id"
    cursor._calculation_id = "previous"
    cursor._calculation_execution = None
    mock = AsyncMock if request.param is AioSparkCursor else MagicMock
    cursor._calculate = mock(return_value="calculation_id")
    cursor._poll = mock(
        return_value=MagicMock(state=AthenaCalculationExecutionStatus.STATE_COMPLETED)
    )
    cursor._executor = MagicMock()
    return cursor


async def _execute(cursor, *args, **kwargs):
    """Call ``execute()``, awaiting it on ``AioSparkCursor``.

    Args:
        cursor: The Spark cursor.
        *args: Positional arguments for ``execute()``.
        **kwargs: Keyword arguments for ``execute()``.

    Returns:
        The result of ``execute()``.
    """
    if isinstance(cursor, AioSparkCursor):
        return await cursor.execute(*args, **kwargs)
    return cursor.execute(*args, **kwargs)


class TestSparkExecute:
    @pytest.mark.parametrize("parameters", [{}, [], {"value": 1}])
    async def test_parameters_rejected(self, execute_cursor, parameters):
        with pytest.raises(NotSupportedError, match="do not support parameters"):
            await _execute(execute_cursor, "code", parameters)

        execute_cursor._calculate.assert_not_called()
        assert execute_cursor.calculation_id == "previous"

    async def test_work_group_rejected(self, execute_cursor):
        with pytest.raises(TypeError, match="unexpected keyword argument 'work_group'"):
            await _execute(execute_cursor, "code", work_group=None)

        execute_cursor._calculate.assert_not_called()

    @pytest.mark.parametrize(
        ("args", "kwargs"), [((), {}), ((None,), {}), ((), {"parameters": None})]
    )
    async def test_none_parameters_accepted(self, execute_cursor, args, kwargs):
        await _execute(execute_cursor, "code", *args, **kwargs)

        execute_cursor._calculate.assert_called_once_with(
            session_id="session_id",
            code_block="code",
            description=None,
            client_request_token=None,
        )


class TestSparkBaseCursor:
    @pytest.mark.parametrize(
        "state",
        [
            AthenaSessionStatus.STATE_TERMINATED,
            AthenaSessionStatus.STATE_DEGRADED,
            AthenaSessionStatus.STATE_FAILED,
        ],
    )
    def test_wait_for_idle_session_raises_on_failure_state(self, state):
        cursor = _cursor()
        with (
            patch.object(
                SparkCursor,
                "_get_session_status",
                return_value=_session_status(state, "session failure reason"),
            ),
            patch("pyathena.spark.common.time.sleep", side_effect=AssertionError("slept")),
            pytest.raises(
                OperationalError,
                match=rf"^Session: session_id is {state}\. session failure reason$",
            ),
        ):
            cursor._wait_for_idle_session("session_id")

    def test_wait_for_idle_session_raises_without_reason(self):
        cursor = _cursor()
        with (
            patch.object(
                SparkCursor,
                "_get_session_status",
                return_value=_session_status(AthenaSessionStatus.STATE_TERMINATED),
            ),
            patch("pyathena.spark.common.time.sleep", side_effect=AssertionError("slept")),
            pytest.raises(OperationalError, match=r"^Session: session_id is TERMINATED\.$"),
        ):
            cursor._wait_for_idle_session("session_id")

    def test_wait_for_idle_session_waits_until_idle(self):
        cursor = _cursor()
        statuses = [
            _session_status(AthenaSessionStatus.STATE_CREATING),
            _session_status(AthenaSessionStatus.STATE_BUSY),
            _session_status(AthenaSessionStatus.STATE_IDLE),
        ]
        with (
            patch.object(SparkCursor, "_get_session_status", side_effect=statuses) as get_status,
            patch("pyathena.spark.common.time.sleep") as sleep,
        ):
            cursor._wait_for_idle_session("session_id")
        assert get_status.call_count == 3
        assert sleep.call_count == 2

    def test_exists_session_raises_on_failure_state(self):
        cursor = _cursor()
        with (
            patch.object(
                SparkCursor,
                "_get_session_status",
                return_value=_session_status(
                    AthenaSessionStatus.STATE_TERMINATED, "session failure reason"
                ),
            ),
            patch("pyathena.spark.common.time.sleep", side_effect=AssertionError("slept")),
            pytest.raises(OperationalError, match="session failure reason"),
        ):
            cursor._exists_session("session_id")
        cursor._connection.client.get_session.assert_called_once_with(SessionId="session_id")

    @pytest.mark.parametrize("cursor_class", SYNC_SPARK_CURSOR_CLASSES)
    @pytest.mark.parametrize("kill_on_interrupt", [True, False])
    def test_calculate_reuses_generated_token_on_retry(self, cursor_class, kill_on_interrupt):
        cursor = _calculation_cursor(cursor_class, kill_on_interrupt=kill_on_interrupt)
        client = cursor._connection.client
        client.start_calculation_execution.side_effect = [
            ClientError(
                {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}},
                "StartCalculationExecution",
            ),
            {"CalculationExecutionId": "calculation_id"},
        ]

        assert cursor._calculate(session_id="session_id", code_block="code") == "calculation_id"

        tokens = [
            c.kwargs["ClientRequestToken"]
            for c in client.start_calculation_execution.call_args_list
        ]
        assert len(tokens) == 2
        assert tokens[0] == tokens[1]
        uuid.UUID(tokens[0])
        for thread in threading.enumerate():
            if thread.name == "pyathena-start":
                thread.join(_TIMEOUT)
                assert not thread.is_alive()

    @pytest.mark.parametrize("cursor_class", SYNC_SPARK_CURSOR_CLASSES)
    def test_calculate_generates_token_per_call(self, cursor_class):
        cursor = _calculation_cursor(cursor_class)
        client = cursor._connection.client

        cursor._calculate(session_id="session_id", code_block="code")
        cursor._calculate(session_id="session_id", code_block="code")

        tokens = [
            c.kwargs["ClientRequestToken"]
            for c in client.start_calculation_execution.call_args_list
        ]
        assert tokens[0] != tokens[1]

    @pytest.mark.parametrize("cursor_class", SYNC_SPARK_CURSOR_CLASSES)
    def test_calculate_keeps_caller_token(self, cursor_class):
        cursor = _calculation_cursor(cursor_class)

        cursor._calculate(session_id="session_id", code_block="code", client_request_token="token")

        cursor._connection.client.start_calculation_execution.assert_called_once_with(
            SessionId="session_id", CodeBlock="code", ClientRequestToken="token"
        )

    @pytest.mark.parametrize("cursor_class", SYNC_SPARK_CURSOR_CLASSES)
    @pytest.mark.parametrize(
        "final_state",
        [
            AthenaCalculationExecutionStatus.STATE_CANCELED,
            AthenaCalculationExecutionStatus.STATE_COMPLETED,
        ],
    )
    def test_calculate_interrupted_while_starting(self, cursor_class, final_state):
        cursor = _calculation_cursor(cursor_class)
        final_execution = MagicMock(state=final_state)
        cursor._get_calculation_execution.return_value = final_execution
        started, release = _block_start(cursor)
        waits, raised = interrupt_start_waits(started, release)

        with waits, pytest.raises(KeyboardInterrupt) as exc_info:
            cursor._calculate(session_id="session_id", code_block="code")

        assert exc_info.value is raised[0]
        assert exc_info.value.__cause__ is None
        cursor._connection.client.start_calculation_execution.assert_called_once()
        cursor._cancel.assert_called_once_with("calculation_id")
        assert cursor.calculation_id == "calculation_id"
        assert cursor._calculation_execution is final_execution

    @pytest.mark.parametrize("cursor_class", SYNC_SPARK_CURSOR_CLASSES)
    @pytest.mark.parametrize("failing", ["start", "cancel", "wait"])
    def test_calculate_interrupted_while_starting_failure(self, cursor_class, failing):
        cursor = _calculation_cursor(cursor_class)
        error = OperationalError("failed")
        started, release = _block_start(
            cursor,
            response=ClientError(
                {"Error": {"Code": "InvalidRequestException", "Message": "failed"}},
                "StartCalculationExecution",
            )
            if failing == "start"
            else None,
        )
        if failing == "cancel":
            cursor._cancel.side_effect = error
        if failing == "wait":
            cursor._get_calculation_execution_status.side_effect = error
        waits, raised = interrupt_start_waits(started, release)

        with waits, pytest.raises(KeyboardInterrupt) as exc_info:
            cursor._calculate(session_id="session_id", code_block="code")

        assert exc_info.value is raised[0]
        assert cursor._calculation_execution is None
        if failing == "start":
            assert isinstance(exc_info.value.__cause__, DatabaseError)
            cursor._cancel.assert_not_called()
            assert cursor.calculation_id is None
        else:
            assert exc_info.value.__cause__ is error
            cursor._cancel.assert_called_once_with("calculation_id")
            assert cursor.calculation_id == "calculation_id"

    @pytest.mark.parametrize("cursor_class", SYNC_SPARK_CURSOR_CLASSES)
    def test_calculate_second_interrupt_while_starting(self, cursor_class):
        cursor = _calculation_cursor(cursor_class)
        started, release = _block_start(cursor)
        waits, raised = interrupt_start_waits(started, release, interrupts=2)

        try:
            with waits, pytest.raises(KeyboardInterrupt) as exc_info:
                cursor._calculate(session_id="session_id", code_block="code")
        finally:
            release.set()

        assert exc_info.value is raised[1]
        assert exc_info.value.__context__ is raised[0]
        cursor._cancel.assert_not_called()

    @pytest.mark.parametrize("cursor_class", SYNC_SPARK_CURSOR_CLASSES)
    @pytest.mark.parametrize("helper_starts", [False, True])
    def test_calculate_interrupted_before_request_is_sent(self, cursor_class, helper_starts):
        cursor = _calculation_cursor(cursor_class)
        targets = []

        class InterruptedThread:
            """A thread whose start() is interrupted; the test runs its target later."""

            def __init__(self, target, name, daemon):
                targets.append(target)

            def start(self):
                raise KeyboardInterrupt

        with (
            patch("pyathena.common.threading.Thread", InterruptedThread),
            pytest.raises(KeyboardInterrupt) as exc_info,
        ):
            cursor._calculate(session_id="session_id", code_block="code")
        if helper_starts:
            # The helper thread starts running after the interrupt was handled.
            targets[0]()

        assert exc_info.value.__cause__ is None
        cursor._connection.client.start_calculation_execution.assert_not_called()
        cursor._cancel.assert_not_called()
        assert cursor.calculation_id is None

    @pytest.mark.parametrize("cursor_class", SYNC_SPARK_CURSOR_CLASSES)
    def test_calculate_interrupt_without_kill_on_interrupt(self, cursor_class):
        cursor = _calculation_cursor(cursor_class, kill_on_interrupt=False)
        cursor._connection.client.start_calculation_execution.side_effect = KeyboardInterrupt()

        with (
            patch("pyathena.common.threading.Thread") as thread,
            pytest.raises(KeyboardInterrupt),
        ):
            cursor._calculate(session_id="session_id", code_block="code")

        thread.assert_not_called()
        cursor._connection.client.start_calculation_execution.assert_called_once()
        cursor._cancel.assert_not_called()

    def test_execute_interrupted_while_starting(self):
        cursor = _calculation_cursor(SparkCursor)
        started, release = _block_start(cursor)
        waits, _ = interrupt_start_waits(started, release)

        with waits, pytest.raises(KeyboardInterrupt):
            cursor.execute("code")

        assert cursor.calculation_id == "calculation_id"
        assert cursor.state == AthenaCalculationExecutionStatus.STATE_CANCELED

    @pytest.mark.parametrize("cursor_class", SPARK_CURSOR_CLASSES)
    def test_init_starts_session(self, cursor_class):
        connection = _connection()
        with patch.object(SparkBaseCursor, "_wait_for_idle_session"):
            cursor = _init_cursor(cursor_class, connection)

        assert cursor.session_id == "new-session"
        connection.client.terminate_session.assert_not_called()

    @pytest.mark.parametrize("cursor_class", SPARK_CURSOR_CLASSES)
    @pytest.mark.parametrize(
        "error",
        [OperationalError("Session did not become idle."), KeyboardInterrupt()],
    )
    def test_init_terminates_new_session_that_does_not_become_idle(self, cursor_class, error):
        connection = _connection()
        with (
            patch.object(SparkBaseCursor, "_wait_for_idle_session", side_effect=error),
            pytest.raises(type(error)) as exc_info,
        ):
            _init_cursor(cursor_class, connection)

        assert exc_info.value is error
        connection.client.terminate_session.assert_called_once_with(SessionId="new-session")

    @pytest.mark.parametrize("cursor_class", SPARK_CURSOR_CLASSES)
    @pytest.mark.parametrize(
        "state",
        [
            AthenaSessionStatus.STATE_TERMINATED,
            AthenaSessionStatus.STATE_DEGRADED,
            AthenaSessionStatus.STATE_FAILED,
        ],
    )
    def test_init_terminates_new_session_in_failure_state(self, cursor_class, state):
        connection = _connection()
        connection.client.get_session_status.return_value = {
            "SessionId": "new-session",
            "Status": {"State": state, "StateChangeReason": "session failure reason"},
        }
        with (
            patch("pyathena.spark.common.time.sleep", side_effect=AssertionError("slept")),
            pytest.raises(
                OperationalError,
                match=rf"^Session: new-session is {state}\. session failure reason$",
            ),
        ):
            _init_cursor(cursor_class, connection)

        connection.client.get_session_status.assert_called_once_with(SessionId="new-session")
        connection.client.terminate_session.assert_called_once_with(SessionId="new-session")

    @pytest.mark.parametrize("cursor_class", SPARK_CURSOR_CLASSES)
    def test_init_keeps_original_error_when_cleanup_fails(self, cursor_class, caplog):
        connection = _connection()
        connection.client.terminate_session.side_effect = ClientError(
            {"Error": {"Code": "InternalServerException", "Message": "Cleanup failed."}},
            "TerminateSession",
        )
        error = OperationalError("Session did not become idle.")
        with (
            caplog.at_level(logging.ERROR, logger="pyathena.spark.common"),
            patch.object(SparkBaseCursor, "_wait_for_idle_session", side_effect=error),
            pytest.raises(OperationalError) as exc_info,
        ):
            _init_cursor(cursor_class, connection)

        assert exc_info.value is error
        connection.client.terminate_session.assert_called_once_with(SessionId="new-session")
        assert "Failed to terminate session: new-session." in caplog.text

    @pytest.mark.parametrize("cursor_class", SPARK_CURSOR_CLASSES)
    def test_init_does_not_terminate_supplied_session(self, cursor_class):
        connection = _connection()
        error = OperationalError("Session did not become idle.")
        with (
            patch.object(SparkBaseCursor, "_wait_for_idle_session", side_effect=error),
            pytest.raises(OperationalError) as exc_info,
        ):
            _init_cursor(cursor_class, connection, session_id="supplied-session")

        assert exc_info.value is error
        connection.client.start_session.assert_not_called()
        connection.client.terminate_session.assert_not_called()

    @pytest.mark.parametrize("cursor_class", SPARK_CURSOR_CLASSES)
    def test_init_does_not_start_session_when_s3_client_fails(self, cursor_class):
        connection = _connection()
        type(connection).s3_client = PropertyMock(
            side_effect=ValueError("Invalid S3 client configuration.")
        )
        with pytest.raises(ValueError, match=r"^Invalid S3 client configuration\.$"):
            _init_cursor(cursor_class, connection)

        connection.client.start_session.assert_not_called()
        connection.client.terminate_session.assert_not_called()

    def test_async_init_does_not_start_session_when_executor_fails(self):
        connection = _connection()
        with pytest.raises(ValueError, match="max_workers must be greater than 0"):
            _init_cursor(AsyncSparkCursor, connection, max_workers=0)

        connection.client.start_session.assert_not_called()

    @pytest.mark.parametrize("cursor_class", SPARK_CURSOR_CLASSES)
    @pytest.mark.parametrize("session_id", [None, "supplied-session"])
    @pytest.mark.parametrize("terminate_session_on_close", [None, True, False])
    def test_close_terminates_session_by_ownership(
        self, cursor_class, session_id, terminate_session_on_close
    ):
        connection = _connection()
        cursor = _init_cursor(
            cursor_class,
            connection,
            session_id=session_id,
            terminate_session_on_close=terminate_session_on_close,
        )
        _close(cursor)

        if terminate_session_on_close is None:
            terminates = session_id is None
        else:
            terminates = terminate_session_on_close
        if terminates:
            connection.client.terminate_session.assert_called_once_with(SessionId=cursor.session_id)
        else:
            connection.client.terminate_session.assert_not_called()

    @pytest.mark.parametrize("cursor_class", SPARK_CURSOR_CLASSES)
    def test_close_does_not_terminate_session_twice(self, cursor_class):
        connection = _connection()
        cursor = _init_cursor(cursor_class, connection)
        _close(cursor)
        _close(cursor)

        connection.client.terminate_session.assert_called_once_with(SessionId="new-session")

    @pytest.mark.parametrize("cursor_class", SPARK_CURSOR_CLASSES)
    def test_close_retries_failed_termination(self, cursor_class):
        connection = _connection()
        connection.client.terminate_session.side_effect = [
            ClientError(
                {"Error": {"Code": "InternalServerException", "Message": "Termination failed."}},
                "TerminateSession",
            ),
            {},
        ]
        cursor = _init_cursor(cursor_class, connection)
        with pytest.raises(OperationalError):
            _close(cursor)
        _close(cursor)
        _close(cursor)

        assert connection.client.terminate_session.call_count == 2

    def test_async_close_shuts_down_executor_without_terminating_session(self):
        connection = _connection()
        cursor = _init_cursor(AsyncSparkCursor, connection, session_id="supplied-session")
        cursor.close()

        connection.client.terminate_session.assert_not_called()
        with pytest.raises(RuntimeError):
            cursor.calculation_execution("calculation_id")
