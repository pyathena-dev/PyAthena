# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from unittest.mock import MagicMock, patch

import pytest

from pyathena.aio.polars.cursor import AioPolarsCursor
from pyathena.error import ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.polars.result_set import AthenaPolarsResultSet
from pyathena.util import RetryConfig
from tests import ENV
from tests.pyathena.aio.conftest import _aio_connect


class TestAioPolarsCursor:
    async def test_fetchone(self, aio_polars_cursor):
        await aio_polars_cursor.execute("SELECT * FROM one_row")
        assert aio_polars_cursor.rownumber == 0
        assert await aio_polars_cursor.fetchone() == (1,)
        assert aio_polars_cursor.rownumber == 1
        assert await aio_polars_cursor.fetchone() is None

    async def test_fetchmany(self, aio_polars_cursor):
        await aio_polars_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        assert len(await aio_polars_cursor.fetchmany(10)) == 10
        assert len(await aio_polars_cursor.fetchmany(10)) == 5

    async def test_fetchall(self, aio_polars_cursor):
        await aio_polars_cursor.execute("SELECT * FROM one_row")
        assert await aio_polars_cursor.fetchall() == [(1,)]
        await aio_polars_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        assert await aio_polars_cursor.fetchall() == [(i,) for i in range(10000)]

    async def test_as_polars(self, aio_polars_cursor):
        await aio_polars_cursor.execute("SELECT * FROM one_row")
        df = aio_polars_cursor.as_polars()
        assert df.height == 1
        assert df.width == 1

    async def test_as_arrow(self, aio_polars_cursor):
        await aio_polars_cursor.execute("SELECT * FROM one_row")
        table = aio_polars_cursor.as_arrow()
        assert table.num_rows == 1
        assert table.num_columns == 1

    async def test_execute_returns_self(self, aio_polars_cursor):
        result = await aio_polars_cursor.execute("SELECT * FROM one_row")
        assert result is aio_polars_cursor

    async def test_no_result_set_raises(self, aio_polars_cursor):
        with pytest.raises(ProgrammingError):
            await aio_polars_cursor.fetchone()
        with pytest.raises(ProgrammingError):
            await aio_polars_cursor.fetchmany()
        with pytest.raises(ProgrammingError):
            await aio_polars_cursor.fetchall()
        with pytest.raises(ProgrammingError):
            aio_polars_cursor.as_polars()
        with pytest.raises(ProgrammingError):
            aio_polars_cursor.as_arrow()

    async def test_context_manager(self):
        from pyathena.aio.polars.cursor import AioPolarsCursor

        conn = await _aio_connect(schema_name=ENV.schema, cursor_class=AioPolarsCursor)
        try:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT * FROM one_row")
                assert await cursor.fetchone() == (1,)
        finally:
            conn.close()

    async def test_arraysize_default(self, aio_polars_cursor):
        assert aio_polars_cursor.arraysize == AthenaPolarsResultSet.DEFAULT_FETCH_SIZE

    async def test_invalid_arraysize(self, aio_polars_cursor):
        aio_polars_cursor.arraysize = 10000
        assert aio_polars_cursor.arraysize == 10000
        with pytest.raises(ProgrammingError):
            aio_polars_cursor.arraysize = -1

    async def test_description(self, aio_polars_cursor):
        await aio_polars_cursor.execute("SELECT CAST(1 AS INT) AS foobar FROM one_row")
        assert await aio_polars_cursor.fetchall() == [(1,)]
        assert aio_polars_cursor.description == [
            ("foobar", "integer", None, None, 10, 0, "UNKNOWN")
        ]

    async def test_description_initial(self, aio_polars_cursor):
        assert aio_polars_cursor.description is None

    async def test_cancel_initial(self, aio_polars_cursor):
        with pytest.raises(ProgrammingError):
            await aio_polars_cursor.cancel()

    async def test_executemany_fetch(self, aio_polars_cursor):
        await aio_polars_cursor.executemany(
            "SELECT %(x)d FROM one_row", [{"x": i} for i in range(1, 2)]
        )
        with pytest.raises(ProgrammingError):
            await aio_polars_cursor.fetchall()
        with pytest.raises(ProgrammingError):
            await aio_polars_cursor.fetchmany()
        with pytest.raises(ProgrammingError):
            await aio_polars_cursor.fetchone()
        with pytest.raises(ProgrammingError):
            aio_polars_cursor.as_polars()
        with pytest.raises(ProgrammingError):
            aio_polars_cursor.as_arrow()

    @pytest.mark.parametrize(
        "aio_polars_cursor",
        [{"cursor_kwargs": {"unload": True}}],
        indirect=["aio_polars_cursor"],
    )
    async def test_fetchone_unload(self, aio_polars_cursor):
        await aio_polars_cursor.execute("SELECT * FROM one_row")
        assert await aio_polars_cursor.fetchone() == (1,)
        assert await aio_polars_cursor.fetchone() is None

    @pytest.mark.parametrize(
        "aio_polars_cursor",
        [{"cursor_kwargs": {"unload": True}}],
        indirect=["aio_polars_cursor"],
    )
    async def test_as_polars_unload(self, aio_polars_cursor):
        await aio_polars_cursor.execute("SELECT * FROM one_row")
        df = aio_polars_cursor.as_polars()
        assert df.height == 1

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
