# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from unittest.mock import MagicMock, patch

import pytest

from pyathena.aio.pandas.cursor import AioPandasCursor
from pyathena.error import ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.pandas.result_set import AthenaPandasResultSet
from pyathena.util import RetryConfig
from tests import ENV
from tests.pyathena.aio.conftest import _aio_connect


class TestAioPandasCursor:
    async def test_binary_null_vs_empty(self, aio_pandas_cursor):
        query = """SELECT * FROM (VALUES
                    (1, CAST(NULL AS VARBINARY), 'null', CAST(NULL AS VARCHAR)),
                    (2, X'', 'empty', ''),
                    (3, X'00ff275c25', 'comma, quote" and' || chr(10) || 'newline', 'NULL')
                ) AS t(id, value, label, text_value) ORDER BY id"""
        await aio_pandas_cursor.execute(query, chunksize=2)
        result = aio_pandas_cursor
        rows = await result.fetchall()
        assert [row[:3] for row in rows] == [
            (1, None, "null"),
            (2, b"", "empty"),
            (3, b"\x00\xff'\\%", 'comma, quote" and\nnewline'),
        ]

    async def test_fetchone(self, aio_pandas_cursor):
        await aio_pandas_cursor.execute("SELECT * FROM one_row")
        assert aio_pandas_cursor.rownumber == 0
        assert await aio_pandas_cursor.fetchone() == (1,)
        assert aio_pandas_cursor.rownumber == 1
        assert await aio_pandas_cursor.fetchone() is None

    async def test_fetchmany(self, aio_pandas_cursor):
        await aio_pandas_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        assert len(await aio_pandas_cursor.fetchmany(10)) == 10
        assert len(await aio_pandas_cursor.fetchmany(10)) == 5

    async def test_fetchall(self, aio_pandas_cursor):
        await aio_pandas_cursor.execute("SELECT * FROM one_row")
        assert await aio_pandas_cursor.fetchall() == [(1,)]
        await aio_pandas_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        assert await aio_pandas_cursor.fetchall() == [(i,) for i in range(10000)]

    async def test_as_pandas(self, aio_pandas_cursor):
        await aio_pandas_cursor.execute("SELECT * FROM one_row")
        df = aio_pandas_cursor.as_pandas()
        assert len(df) == 1
        assert df.columns.tolist() == ["number_of_rows"]
        assert df["number_of_rows"].iloc[0] == 1

    async def test_execute_returns_self(self, aio_pandas_cursor):
        result = await aio_pandas_cursor.execute("SELECT * FROM one_row")
        assert result is aio_pandas_cursor

    async def test_no_result_set_raises(self, aio_pandas_cursor):
        with pytest.raises(ProgrammingError):
            await aio_pandas_cursor.fetchone()
        with pytest.raises(ProgrammingError):
            await aio_pandas_cursor.fetchmany()
        with pytest.raises(ProgrammingError):
            await aio_pandas_cursor.fetchall()
        with pytest.raises(ProgrammingError):
            aio_pandas_cursor.as_pandas()

    async def test_context_manager(self):
        from pyathena.aio.pandas.cursor import AioPandasCursor

        conn = await _aio_connect(schema_name=ENV.schema, cursor_class=AioPandasCursor)
        try:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT * FROM one_row")
                assert await cursor.fetchone() == (1,)
        finally:
            conn.close()

    async def test_arraysize_default(self, aio_pandas_cursor):
        assert aio_pandas_cursor.arraysize == AthenaPandasResultSet.DEFAULT_FETCH_SIZE

    async def test_invalid_arraysize(self, aio_pandas_cursor):
        aio_pandas_cursor.arraysize = 10000
        assert aio_pandas_cursor.arraysize == 10000
        with pytest.raises(ProgrammingError):
            aio_pandas_cursor.arraysize = -1

    async def test_description(self, aio_pandas_cursor):
        await aio_pandas_cursor.execute("SELECT CAST(1 AS INT) AS foobar FROM one_row")
        assert await aio_pandas_cursor.fetchall() == [(1,)]
        assert aio_pandas_cursor.description == [
            ("foobar", "integer", None, None, 10, 0, "UNKNOWN")
        ]

    async def test_description_initial(self, aio_pandas_cursor):
        assert aio_pandas_cursor.description is None

    async def test_cancel_initial(self, aio_pandas_cursor):
        with pytest.raises(ProgrammingError):
            await aio_pandas_cursor.cancel()

    async def test_executemany_fetch(self, aio_pandas_cursor):
        await aio_pandas_cursor.executemany(
            "SELECT %(x)d FROM one_row", [{"x": i} for i in range(1, 2)]
        )
        with pytest.raises(ProgrammingError):
            await aio_pandas_cursor.fetchall()
        with pytest.raises(ProgrammingError):
            await aio_pandas_cursor.fetchmany()
        with pytest.raises(ProgrammingError):
            await aio_pandas_cursor.fetchone()
        with pytest.raises(ProgrammingError):
            aio_pandas_cursor.as_pandas()

    @pytest.mark.parametrize(
        "aio_pandas_cursor",
        [{"cursor_kwargs": {"unload": True}}],
        indirect=["aio_pandas_cursor"],
    )
    async def test_fetchone_unload(self, aio_pandas_cursor):
        await aio_pandas_cursor.execute("SELECT * FROM one_row")
        assert await aio_pandas_cursor.fetchone() == (1,)
        assert await aio_pandas_cursor.fetchone() is None

    @pytest.mark.parametrize(
        "aio_pandas_cursor",
        [{"cursor_kwargs": {"unload": True}}],
        indirect=["aio_pandas_cursor"],
    )
    async def test_as_pandas_unload(self, aio_pandas_cursor):
        await aio_pandas_cursor.execute("SELECT * FROM one_row")
        df = aio_pandas_cursor.as_pandas()
        assert len(df) == 1

    @pytest.mark.parametrize(
        "execute_kwargs",
        [
            {},
            {
                "block_size": 2048,
                "cache_type": "none",
                "s3_max_workers": 3,
                "auto_optimize_chunksize": False,
            },
        ],
    )
    async def test_read_options(self, execute_kwargs):
        """The cursor's read options reach the result set, and execute() overrides them.

        No AWS calls; the query and its result set are mocked.
        """
        cursor_kwargs = {
            "block_size": 1024,
            "cache_type": "bytes",
            "s3_max_workers": 2,
            "auto_optimize_chunksize": True,
        }
        cursor = AioPandasCursor(
            connection=MagicMock(),
            converter=MagicMock(),
            formatter=MagicMock(),
            retry_config=RetryConfig(),
            **cursor_kwargs,
        )
        query_execution = MagicMock(state=AthenaQueryExecution.STATE_SUCCEEDED)
        with (
            patch.object(AioPandasCursor, "_execute", return_value="query_id"),
            patch.object(AioPandasCursor, "_poll", return_value=query_execution),
            patch("pyathena.aio.pandas.cursor.AthenaPandasResultSet") as result_set_class,
        ):
            await cursor.execute("SELECT 1", **execute_kwargs)
        kwargs = result_set_class.call_args.kwargs
        expected = {**cursor_kwargs, **execute_kwargs}
        expected["max_workers"] = expected.pop("s3_max_workers")
        assert {key: kwargs[key] for key in expected} == expected
