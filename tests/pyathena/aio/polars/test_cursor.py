# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from pyathena.aio.polars.cursor import AioPolarsCursor
from pyathena.error import ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.util import RetryConfig

_FIVE_ROWS = "SELECT number FROM (VALUES (1), (2), (3), (4), (5)) AS t(number)"


class TestAioPolarsCursor:
    async def test_fetchone(self, aio_polars_cursor):
        assert await aio_polars_cursor.execute("SELECT * FROM one_row") is aio_polars_cursor
        assert aio_polars_cursor.rownumber == 0
        assert await aio_polars_cursor.fetchone() == (1,)
        assert aio_polars_cursor.rownumber == 1
        assert await aio_polars_cursor.fetchone() is None

    async def test_as_polars(self, aio_polars_cursor):
        await aio_polars_cursor.execute("SELECT * FROM one_row")
        df = await aio_polars_cursor.as_polars()
        assert df.height == 1
        assert df.width == 1
        table = await aio_polars_cursor.as_arrow()
        assert table.num_rows == 1
        assert table.num_columns == 1

    @pytest.mark.parametrize(
        "aio_polars_cursor",
        [{"cursor_kwargs": {"unload": True}}],
        indirect=["aio_polars_cursor"],
    )
    async def test_fetchone_unload(self, aio_polars_cursor):
        await aio_polars_cursor.execute("SELECT * FROM one_row")
        assert (await aio_polars_cursor.as_polars()).height == 1
        assert await aio_polars_cursor.fetchone() == (1,)
        assert await aio_polars_cursor.fetchone() is None

    @pytest.mark.parametrize(
        "execute_kwargs",
        [{}, {"block_size": 2048, "cache_type": "none", "s3_max_workers": 3, "chunksize": 20}],
    )
    async def test_read_options(self, execute_kwargs):
        """The cursor's read options reach the result set, and execute() overrides them.

        No AWS calls; the query and its result set are mocked.
        """
        cursor_kwargs = {
            "block_size": 1024,
            "cache_type": "bytes",
            "s3_max_workers": 2,
            "chunksize": 10,
        }
        cursor = AioPolarsCursor(
            connection=MagicMock(),
            converter=MagicMock(),
            formatter=MagicMock(),
            retry_config=RetryConfig(),
            **cursor_kwargs,
        )
        query_execution = MagicMock(state=AthenaQueryExecution.STATE_SUCCEEDED)
        with (
            patch.object(AioPolarsCursor, "_execute", return_value="query_id"),
            patch.object(AioPolarsCursor, "_poll", return_value=query_execution),
            patch("pyathena.aio.polars.cursor.AthenaPolarsResultSet") as result_set_class,
        ):
            await cursor.execute("SELECT 1", **execute_kwargs)
        kwargs = result_set_class.call_args.kwargs
        expected = {**cursor_kwargs, **execute_kwargs}
        expected["max_workers"] = expected.pop("s3_max_workers")
        assert {key: kwargs[key] for key in expected} == expected

    async def test_closed(self, aio_polars_cursor):
        """GH-1207: a closed cursor rejects execute()."""
        aio_polars_cursor.close()

        with pytest.raises(ProgrammingError):
            await aio_polars_cursor.execute("SELECT * FROM one_row")

    @pytest.mark.parametrize(("chunksize", "sizes"), [(None, [5]), (2, [2, 2, 1])])
    async def test_iter_chunks(self, aio_polars_cursor, chunksize, sizes):
        """GH-1209: iter_chunks() yields the chunks, and as_polars() the whole result."""
        await aio_polars_cursor.execute(_FIVE_ROWS, chunksize=chunksize)
        chunks = [chunk async for chunk in aio_polars_cursor.iter_chunks()]
        assert [chunk.height for chunk in chunks] == sizes
        assert pl.concat(chunks)["number"].to_list() == [1, 2, 3, 4, 5]
        await aio_polars_cursor.execute(_FIVE_ROWS, chunksize=chunksize)
        assert (await aio_polars_cursor.as_polars())["number"].to_list() == [1, 2, 3, 4, 5]
        await aio_polars_cursor.execute(_FIVE_ROWS, chunksize=chunksize)
        assert (await aio_polars_cursor.as_arrow())["number"].to_pylist() == [1, 2, 3, 4, 5]

    async def test_iter_chunks_after_execute(self, aio_polars_cursor):
        """GH-1209: iter_chunks() reads the result set of its call, and raises once replaced."""
        await aio_polars_cursor.execute(_FIVE_ROWS, chunksize=2)
        chunks = aio_polars_cursor.iter_chunks()
        await aio_polars_cursor.execute("SELECT * FROM one_row")
        with pytest.raises(ProgrammingError):
            await anext(chunks)
