# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import asyncio
import logging
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from pyathena import OperationalError
from pyathena.aio.spark.cursor import AioSparkCursor
from pyathena.model import AthenaSessionStatus
from pyathena.spark.async_cursor import AsyncSparkCursor
from pyathena.spark.common import SparkBaseCursor
from pyathena.spark.cursor import SparkCursor
from pyathena.util import RetryConfig

SPARK_CURSOR_CLASSES = [SparkCursor, AsyncSparkCursor, AioSparkCursor]


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
        connection.session.client.side_effect = ValueError("Invalid S3 client configuration.")
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
