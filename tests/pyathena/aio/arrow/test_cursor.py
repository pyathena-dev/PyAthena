# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from unittest.mock import MagicMock, patch

import pytest

from pyathena.aio.arrow.cursor import AioArrowCursor
from pyathena.arrow.result_set import AthenaArrowResultSet
from pyathena.error import ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.util import RetryConfig
from tests import ENV
from tests.pyathena.aio.conftest import _aio_connect


class TestAioArrowCursor:
    @pytest.mark.parametrize(
        "aio_arrow_cursor",
        [
            {"cursor_kwargs": {"s3_max_workers": 2, "unload": False}},
            {"cursor_kwargs": {"s3_max_workers": 2, "unload": True}},
        ],
        indirect=True,
    )
    async def test_s3_workers_read_results(self, aio_arrow_cursor):
        """CSV and Parquet results use the configured PyAthena S3 reader."""
        await aio_arrow_cursor.execute("SELECT 1 AS value", s3_max_workers=1)
        assert await aio_arrow_cursor.fetchall() == [(1,)]
        assert aio_arrow_cursor.result_set._fs.handler.fs.max_workers == 1

    async def test_binary_null_vs_empty(self, aio_arrow_cursor):
        query = """SELECT * FROM (VALUES
                    (1, CAST(NULL AS VARBINARY), 'null', CAST(NULL AS VARCHAR)),
                    (2, X'', 'empty', ''),
                    (3, X'00ff275c25', 'comma, quote" and' || chr(10) || 'newline', 'NULL')
                ) AS t(id, value, label, text_value) ORDER BY id"""
        await aio_arrow_cursor.execute(query)
        result = aio_arrow_cursor
        rows = await result.fetchall()
        assert [row[:3] for row in rows] == [
            (1, None, "null"),
            (2, b"", "empty"),
            (3, b"\x00\xff'\\%", 'comma, quote" and\nnewline'),
        ]
        assert [row[3] for row in rows] == ["", "", "NULL"]

    async def test_fetchone(self, aio_arrow_cursor):
        await aio_arrow_cursor.execute("SELECT * FROM one_row")
        assert aio_arrow_cursor.rownumber == 0
        assert await aio_arrow_cursor.fetchone() == (1,)
        assert aio_arrow_cursor.rownumber == 1
        assert await aio_arrow_cursor.fetchone() is None

    async def test_fetchmany(self, aio_arrow_cursor):
        await aio_arrow_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        assert len(await aio_arrow_cursor.fetchmany(10)) == 10
        assert len(await aio_arrow_cursor.fetchmany(10)) == 5

    async def test_fetchall(self, aio_arrow_cursor):
        await aio_arrow_cursor.execute("SELECT * FROM one_row")
        assert await aio_arrow_cursor.fetchall() == [(1,)]
        await aio_arrow_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        assert await aio_arrow_cursor.fetchall() == [(i,) for i in range(10000)]

    async def test_as_arrow(self, aio_arrow_cursor):
        await aio_arrow_cursor.execute("SELECT * FROM one_row")
        table = aio_arrow_cursor.as_arrow()
        assert table.num_rows == 1
        assert table.num_columns == 1
        assert table.column_names == ["number_of_rows"]

    async def test_as_polars(self, aio_arrow_cursor):
        await aio_arrow_cursor.execute("SELECT * FROM one_row")
        df = aio_arrow_cursor.as_polars()
        assert df.height == 1
        assert df.width == 1

    async def test_execute_returns_self(self, aio_arrow_cursor):
        result = await aio_arrow_cursor.execute("SELECT * FROM one_row")
        assert result is aio_arrow_cursor

    async def test_no_result_set_raises(self, aio_arrow_cursor):
        with pytest.raises(ProgrammingError):
            await aio_arrow_cursor.fetchone()
        with pytest.raises(ProgrammingError):
            await aio_arrow_cursor.fetchmany()
        with pytest.raises(ProgrammingError):
            await aio_arrow_cursor.fetchall()
        with pytest.raises(ProgrammingError):
            aio_arrow_cursor.as_arrow()
        with pytest.raises(ProgrammingError):
            aio_arrow_cursor.as_polars()

    async def test_context_manager(self):
        from pyathena.aio.arrow.cursor import AioArrowCursor

        conn = await _aio_connect(schema_name=ENV.schema, cursor_class=AioArrowCursor)
        try:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT * FROM one_row")
                assert await cursor.fetchone() == (1,)
        finally:
            conn.close()

    async def test_arraysize_default(self, aio_arrow_cursor):
        assert aio_arrow_cursor.arraysize == AthenaArrowResultSet.DEFAULT_FETCH_SIZE

    async def test_invalid_arraysize(self, aio_arrow_cursor):
        aio_arrow_cursor.arraysize = 10000
        assert aio_arrow_cursor.arraysize == 10000
        with pytest.raises(ProgrammingError):
            aio_arrow_cursor.arraysize = -1

    async def test_description(self, aio_arrow_cursor):
        await aio_arrow_cursor.execute("SELECT CAST(1 AS INT) AS foobar FROM one_row")
        assert await aio_arrow_cursor.fetchall() == [(1,)]
        assert aio_arrow_cursor.description == [("foobar", "integer", None, None, 10, 0, "UNKNOWN")]

    async def test_description_initial(self, aio_arrow_cursor):
        assert aio_arrow_cursor.description is None

    async def test_cancel_initial(self, aio_arrow_cursor):
        with pytest.raises(ProgrammingError):
            await aio_arrow_cursor.cancel()

    async def test_executemany_fetch(self, aio_arrow_cursor):
        await aio_arrow_cursor.executemany(
            "SELECT %(x)d FROM one_row", [{"x": i} for i in range(1, 2)]
        )
        with pytest.raises(ProgrammingError):
            await aio_arrow_cursor.fetchall()
        with pytest.raises(ProgrammingError):
            await aio_arrow_cursor.fetchmany()
        with pytest.raises(ProgrammingError):
            await aio_arrow_cursor.fetchone()
        with pytest.raises(ProgrammingError):
            aio_arrow_cursor.as_arrow()
        with pytest.raises(ProgrammingError):
            aio_arrow_cursor.as_polars()

    @pytest.mark.parametrize(
        "aio_arrow_cursor",
        [{"cursor_kwargs": {"unload": True}}],
        indirect=["aio_arrow_cursor"],
    )
    async def test_fetchone_unload(self, aio_arrow_cursor):
        await aio_arrow_cursor.execute("SELECT * FROM one_row")
        assert await aio_arrow_cursor.fetchone() == (1,)
        assert await aio_arrow_cursor.fetchone() is None

    @pytest.mark.parametrize(
        "aio_arrow_cursor",
        [{"cursor_kwargs": {"unload": True}}],
        indirect=["aio_arrow_cursor"],
    )
    async def test_as_arrow_unload(self, aio_arrow_cursor):
        await aio_arrow_cursor.execute("SELECT * FROM one_row")
        table = aio_arrow_cursor.as_arrow()
        assert table.num_rows == 1

    @pytest.mark.parametrize(
        "execute_kwargs",
        [
            {},
            {"connect_timeout": 3.0, "request_timeout": 4.0, "s3_max_workers": 3},
            {"s3_max_workers": None},
        ],
    )
    async def test_read_options(self, execute_kwargs):
        """The cursor's read options reach the result set, and execute() overrides them.

        No AWS calls; the query and its result set are mocked.
        """
        cursor_kwargs = {"connect_timeout": 1.0, "request_timeout": 2.0, "s3_max_workers": 2}
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
            first_kwargs = result_set_class.call_args.kwargs.copy()
            await cursor.execute("SELECT 1")
        kwargs = first_kwargs
        expected = {**cursor_kwargs, **execute_kwargs}
        assert {key: kwargs[key] for key in expected} == expected

        defaults = cursor_kwargs.copy()
        second_kwargs = result_set_class.call_args.kwargs
        assert {key: second_kwargs[key] for key in defaults} == defaults
