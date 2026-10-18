# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import asyncio
import threading
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from pyathena.aio.arrow.cursor import AioArrowCursor
from pyathena.aio.connection import AioConnection
from pyathena.aio.cursor import AioCursor, AioDictCursor
from pyathena.aio.pandas.cursor import AioPandasCursor
from pyathena.aio.polars.cursor import AioPolarsCursor
from pyathena.aio.s3fs.cursor import AioS3FSCursor
from pyathena.aio.spark.cursor import AioSparkCursor
from pyathena.spark.common import SparkBaseCursor
from tests.pyathena.util import EVENT_TIMEOUT, cancel_when_started

CURSOR_CLASSES = [
    AioCursor,
    AioDictCursor,
    AioArrowCursor,
    AioPandasCursor,
    AioPolarsCursor,
    AioS3FSCursor,
    AioSparkCursor,
]
# Cursors that pass execute() keyword arguments to a DataFrame reader.
READER_CURSOR_CLASSES = [AioPandasCursor, AioPolarsCursor]


async def _connection(**kwargs: Any) -> AioConnection:
    return await AioConnection.create(
        region_name="us-east-1",
        s3_staging_dir="s3://bucket/path/",
        aws_access_key_id="access_key",
        aws_secret_access_key="secret_key",
        **kwargs,
    )


@pytest.fixture
async def offline_connection():
    """Yield a connection whose cursors fail the test if they call AWS.

    A Spark cursor reports a started session without calling AWS.
    """
    conn = await _connection()
    with (
        patch.object(
            conn.client, "_make_api_call", side_effect=AssertionError("unexpected AWS request")
        ) as request,
        patch.object(SparkBaseCursor, "_start_session", return_value="session"),
    ):
        yield conn
    request.assert_not_called()
    conn.close()


def _open_cursor(conn: AioConnection, cursor_class: type[Any], **kwargs: Any) -> Any:
    if issubclass(cursor_class, SparkBaseCursor):
        # The session was not started, so closing the cursor must not terminate it.
        kwargs["terminate_session_on_close"] = False
    return conn.cursor(cursor_class, **kwargs)


@pytest.mark.parametrize(
    "cursor_class", [c for c in CURSOR_CLASSES if c not in READER_CURSOR_CLASSES]
)
async def test_cursor_execute_unknown_keyword(offline_connection, cursor_class):
    async with _open_cursor(offline_connection, cursor_class) as cursor:
        previous = MagicMock(is_closed=False)
        if isinstance(cursor, SparkBaseCursor):
            cursor._calculation_id = "previous"
        else:
            cursor._query_id = "previous"
            cursor._result_set = previous
        state = vars(cursor).copy()

        with pytest.raises(
            TypeError,
            match=rf"{cursor_class.__name__}\.execute\(\) got an unexpected keyword "
            "argument 'work_gruop'",
        ):
            await cursor.execute("SELECT 1", work_gruop="typo")

        assert vars(cursor) == state
        previous.close.assert_not_called()


@pytest.mark.parametrize(
    ("cursor_class", "kwargs"),
    [
        (AioArrowCursor, {"block_size": 64, "connect_timeout": 1, "request_timeout": 2}),
        (AioS3FSCursor, {"block_size": 64, "csv_reader": None}),
        (AioPandasCursor, {"chunksize": 10, "parse_dates": []}),
        (AioPolarsCursor, {"chunksize": 10, "separator": "|"}),
    ],
)
async def test_cursor_execute_supported_keywords(offline_connection, cursor_class, kwargs):
    async with _open_cursor(offline_connection, cursor_class) as cursor:
        with (
            patch.object(cursor, "_execute", side_effect=RuntimeError("query started")),
            pytest.raises(RuntimeError, match="query started"),
        ):
            await cursor.execute("SELECT 1", **kwargs)


async def test_create_cancelled_closes_connection():
    """A connection built after create() is cancelled is closed."""
    started = threading.Event()
    release = threading.Event()
    closed = threading.Event()
    init = AioConnection.__init__

    def blocking_init(self, **kwargs):
        init(self, **kwargs)
        started.set()
        assert release.wait(EVENT_TIMEOUT)

    with (
        patch.object(AioConnection, "__init__", blocking_init),
        patch.object(AioConnection, "close", lambda self: closed.set()),
    ):
        task = asyncio.create_task(_connection())
        await cancel_when_started(task, started, release)
        assert await asyncio.to_thread(closed.wait, EVENT_TIMEOUT)
