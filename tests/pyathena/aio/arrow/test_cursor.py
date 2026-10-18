# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from unittest.mock import MagicMock, patch

import pytest

from pyathena.aio.arrow.cursor import AioArrowCursor
from pyathena.error import ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.util import RetryConfig


class TestAioArrowCursor:
    async def test_fetchone(self, aio_arrow_cursor):
        assert await aio_arrow_cursor.execute("SELECT * FROM one_row") is aio_arrow_cursor
        assert aio_arrow_cursor.rownumber == 0
        assert await aio_arrow_cursor.fetchone() == (1,)
        assert aio_arrow_cursor.rownumber == 1
        assert await aio_arrow_cursor.fetchone() is None

    async def test_as_arrow(self, aio_arrow_cursor):
        await aio_arrow_cursor.execute("SELECT * FROM one_row")
        table = aio_arrow_cursor.as_arrow()
        assert table.num_rows == 1
        assert table.num_columns == 1
        assert table.column_names == ["number_of_rows"]
        df = aio_arrow_cursor.as_polars()
        assert df.height == 1
        assert df.width == 1

    @pytest.mark.parametrize(
        "aio_arrow_cursor",
        [{"cursor_kwargs": {"unload": True}}],
        indirect=["aio_arrow_cursor"],
    )
    async def test_fetchone_unload(self, aio_arrow_cursor):
        await aio_arrow_cursor.execute("SELECT * FROM one_row")
        assert aio_arrow_cursor.as_arrow().num_rows == 1
        assert await aio_arrow_cursor.fetchone() == (1,)
        assert await aio_arrow_cursor.fetchone() is None

    @pytest.mark.parametrize(
        "execute_kwargs", [{}, {"connect_timeout": 3.0, "request_timeout": 4.0}]
    )
    async def test_read_options(self, execute_kwargs):
        """The cursor's read options reach the result set, and execute() overrides them.

        No AWS calls; the query and its result set are mocked.
        """
        cursor_kwargs = {"connect_timeout": 1.0, "request_timeout": 2.0}
        query_execution = MagicMock(state=AthenaQueryExecution.STATE_SUCCEEDED)
        cursor = AioArrowCursor(
            connection=MagicMock(),
            converter=MagicMock(),
            formatter=MagicMock(),
            retry_config=RetryConfig(),
            **cursor_kwargs,
        )
        with (
            patch.object(AioArrowCursor, "_execute", return_value="query_id"),
            patch.object(AioArrowCursor, "_poll", return_value=query_execution),
            patch("pyathena.aio.arrow.cursor.AthenaArrowResultSet") as result_set_class,
        ):
            await cursor.execute("SELECT 1", **execute_kwargs)
        kwargs = result_set_class.call_args.kwargs
        expected = {**cursor_kwargs, **execute_kwargs}
        assert {key: kwargs[key] for key in expected} == expected

    async def test_closed(self, aio_arrow_cursor):
        """GH-1207: a closed cursor rejects execute()."""
        aio_arrow_cursor.close()

        with pytest.raises(ProgrammingError):
            await aio_arrow_cursor.execute("SELECT * FROM one_row")
