# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from typing import Any

import pytest

from pyathena.aio.arrow.cursor import AioArrowCursor
from pyathena.aio.connection import AioConnection
from pyathena.aio.cursor import AioCursor, AioDictCursor
from pyathena.aio.pandas.cursor import AioPandasCursor
from pyathena.aio.polars.cursor import AioPolarsCursor
from pyathena.aio.s3fs.cursor import AioS3FSCursor
from pyathena.error import ProgrammingError

# Cursors whose arraysize is the GetQueryResults page size, capped at 1000.
PAGED_CURSORS = [AioCursor, AioDictCursor]
# Cursors whose arraysize only sets the fetchmany() batch, with no upper limit.
UNCAPPED_CURSORS = [AioArrowCursor, AioPandasCursor, AioPolarsCursor, AioS3FSCursor]


class RecordingAioCursor(AioCursor):
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        super().__init__(**kwargs)


async def _connection(**kwargs: Any) -> AioConnection:
    return await AioConnection.create(
        region_name="us-east-1",
        s3_staging_dir="s3://bucket/path/",
        aws_access_key_id="access_key",
        aws_secret_access_key="secret_key",
        **kwargs,
    )


class TestAioConnection:
    async def test_cursor_arguments_override_cursor_kwargs(self):
        conn = await _connection(
            cursor_class=RecordingAioCursor,
            cursor_kwargs={"kill_on_interrupt": False, "schema_name": "configured"},
        )

        cursor = conn.cursor(kill_on_interrupt=True)

        assert cursor.kwargs["kill_on_interrupt"] is True
        assert cursor.kwargs["schema_name"] == "configured"
        assert conn.cursor_kwargs == {"kill_on_interrupt": False, "schema_name": "configured"}

    @pytest.mark.parametrize("cursor_class", PAGED_CURSORS + UNCAPPED_CURSORS)
    async def test_cursor_arraysize(self, cursor_class):
        conn = await _connection(cursor_kwargs={"arraysize": 25})

        assert conn.cursor(cursor_class).arraysize == 25
        assert conn.cursor(cursor_class, arraysize=50).arraysize == 50

    @pytest.mark.parametrize("cursor_class", PAGED_CURSORS)
    async def test_cursor_arraysize_over_page_size(self, cursor_class):
        conn = await _connection()

        with pytest.raises(ProgrammingError):
            conn.cursor(cursor_class, arraysize=1001)

    @pytest.mark.parametrize("cursor_class", UNCAPPED_CURSORS)
    async def test_cursor_arraysize_uncapped(self, cursor_class):
        conn = await _connection()

        assert conn.cursor(cursor_class, arraysize=1001).arraysize == 1001

    @pytest.mark.parametrize("cursor_class", PAGED_CURSORS + UNCAPPED_CURSORS)
    async def test_cursor_arraysize_not_positive(self, cursor_class):
        conn = await _connection()

        with pytest.raises(ProgrammingError):
            conn.cursor(cursor_class, arraysize=0)
