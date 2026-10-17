# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from unittest.mock import MagicMock, patch

import pytest

from pyathena.aio.s3fs.cursor import AioS3FSCursor
from pyathena.model import AthenaQueryExecution
from pyathena.s3fs.reader import AthenaCSVReader, EmptyStringAsNullCSVReader
from pyathena.util import RetryConfig


class TestAioS3FSCursor:
    async def test_fetchone(self, aio_s3fs_cursor):
        assert await aio_s3fs_cursor.execute("SELECT * FROM one_row") is aio_s3fs_cursor
        assert aio_s3fs_cursor.rownumber == 0
        assert await aio_s3fs_cursor.fetchone() == (1,)
        assert aio_s3fs_cursor.rownumber == 1
        assert await aio_s3fs_cursor.fetchone() is None

    @pytest.mark.parametrize("execute_kwargs", [{}, {"csv_reader": AthenaCSVReader}])
    async def test_read_options(self, execute_kwargs):
        """The cursor's read options reach the result set, and execute() overrides them.

        No AWS calls; the query and its result set are mocked.
        """
        cursor_kwargs = {"csv_reader": EmptyStringAsNullCSVReader}
        query_execution = MagicMock(state=AthenaQueryExecution.STATE_SUCCEEDED)
        cursor = AioS3FSCursor(
            connection=MagicMock(),
            converter=MagicMock(),
            formatter=MagicMock(),
            retry_config=RetryConfig(),
            **cursor_kwargs,
        )
        with (
            patch.object(AioS3FSCursor, "_execute", return_value="query_id"),
            patch.object(AioS3FSCursor, "_poll", return_value=query_execution),
            patch("pyathena.aio.s3fs.cursor.AthenaS3FSResultSet") as result_set_class,
        ):
            await cursor.execute("SELECT 1", **execute_kwargs)
        kwargs = result_set_class.call_args.kwargs
        expected = {**cursor_kwargs, **execute_kwargs}
        assert {key: kwargs[key] for key in expected} == expected
