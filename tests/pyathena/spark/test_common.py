# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from unittest.mock import MagicMock, patch

import pytest

from pyathena import OperationalError
from pyathena.model import AthenaSessionStatus
from pyathena.spark.cursor import SparkCursor
from pyathena.util import RetryConfig


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
            pytest.raises(OperationalError, match="session failure reason"),
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
