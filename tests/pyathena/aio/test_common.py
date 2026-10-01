# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
from unittest.mock import AsyncMock, MagicMock

import pytest

from pyathena.aio.arrow.cursor import AioArrowCursor
from pyathena.aio.cursor import AioCursor, AioDictCursor
from pyathena.aio.pandas.cursor import AioPandasCursor
from pyathena.aio.polars.cursor import AioPolarsCursor
from pyathena.aio.s3fs.cursor import AioS3FSCursor
from pyathena.error import ProgrammingError


class TestWithAsyncFetch:
    @pytest.mark.parametrize(
        "cursor_class",
        [
            AioCursor,
            AioDictCursor,
            AioArrowCursor,
            AioPandasCursor,
            AioPolarsCursor,
            AioS3FSCursor,
        ],
    )
    def test_sync_iteration_raises(self, cursor_class):
        cursor = cursor_class(
            connection=MagicMock(), converter=None, formatter=None, retry_config=None
        )
        with pytest.raises(
            TypeError, match=rf"'{cursor_class.__name__}' object is not iterable; use 'async for'"
        ):
            iter(cursor)

    @pytest.mark.parametrize(
        "cursor_class",
        [AioArrowCursor, AioPandasCursor, AioPolarsCursor, AioS3FSCursor],
    )
    async def test_fetch_runs_result_set_fetch(self, cursor_class):
        cursor = cursor_class(
            connection=MagicMock(), converter=None, formatter=None, retry_config=None
        )
        cursor.result_set = MagicMock()
        cursor.result_set.fetchone.return_value = (1,)
        cursor.result_set.fetchmany.return_value = [(2,), (3,)]
        cursor.result_set.fetchall.return_value = [(4,)]

        assert await cursor.fetchone() == (1,)
        assert await cursor.fetchmany(2) == [(2,), (3,)]
        assert await cursor.fetchall() == [(4,)]
        cursor.result_set.fetchmany.assert_called_once_with(2)

    @pytest.mark.parametrize(
        "cursor_class",
        [AioArrowCursor, AioPandasCursor, AioPolarsCursor, AioS3FSCursor],
    )
    async def test_fetch_without_result_set(self, cursor_class):
        cursor = cursor_class(
            connection=MagicMock(), converter=None, formatter=None, retry_config=None
        )
        for fetch in (cursor.fetchone, cursor.fetchmany, cursor.fetchall):
            with pytest.raises(ProgrammingError, match=r"No result set\."):
                await fetch()

    @pytest.mark.parametrize(
        "cursor_class",
        [
            AioCursor,
            AioDictCursor,
            AioArrowCursor,
            AioPandasCursor,
            AioPolarsCursor,
            AioS3FSCursor,
        ],
    )
    async def test_async_iteration(self, cursor_class):
        cursor = cursor_class(
            connection=MagicMock(), converter=None, formatter=None, retry_config=None
        )
        cursor.result_set = MagicMock()
        # AioCursor's result set fetches asynchronously; the others run it in a thread.
        fetchone = AsyncMock if cursor_class in (AioCursor, AioDictCursor) else MagicMock
        cursor.result_set.fetchone = fetchone(side_effect=[(1,), (2,), None])

        rows = []
        async for row in cursor:
            rows.append(row)
            # Stop rather than loop forever if iteration never ends.
            if len(rows) > 2:
                break
        assert rows == [(1,), (2,)]
