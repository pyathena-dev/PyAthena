# -*- coding: utf-8 -*-
import pytest

from pyathena.error import ProgrammingError
from pyathena.polars.result_set import AthenaPolarsResultSet
from tests import ENV
from tests.pyathena.aio.conftest import _aio_connect


class TestAioPolarsCursor:
    async def test_fetchone(self, aio_polars_cursor):
        await aio_polars_cursor.execute("SELECT * FROM one_row")
        assert aio_polars_cursor.rownumber == 0
        assert aio_polars_cursor.fetchone() == (1,)
        assert aio_polars_cursor.rownumber == 1
        assert aio_polars_cursor.fetchone() is None

    async def test_fetchmany(self, aio_polars_cursor):
        await aio_polars_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        assert len(aio_polars_cursor.fetchmany(10)) == 10
        assert len(aio_polars_cursor.fetchmany(10)) == 5

    async def test_fetchall(self, aio_polars_cursor):
        await aio_polars_cursor.execute("SELECT * FROM one_row")
        assert aio_polars_cursor.fetchall() == [(1,)]
        await aio_polars_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        assert aio_polars_cursor.fetchall() == [(i,) for i in range(10000)]

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
            aio_polars_cursor.fetchone()
        with pytest.raises(ProgrammingError):
            aio_polars_cursor.fetchmany()
        with pytest.raises(ProgrammingError):
            aio_polars_cursor.fetchall()
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
                assert cursor.fetchone() == (1,)
        finally:
            conn.close()

    async def test_arraysize_default(self, aio_polars_cursor):
        assert aio_polars_cursor.arraysize == AthenaPolarsResultSet.DEFAULT_FETCH_SIZE

    async def test_invalid_arraysize(self, aio_polars_cursor):
        aio_polars_cursor.arraysize = 10000
        assert aio_polars_cursor.arraysize == 10000
        with pytest.raises(ProgrammingError):
            aio_polars_cursor.arraysize = -1

    @pytest.mark.parametrize(
        "aio_polars_cursor",
        [{"cursor_kwargs": {"unload": True}}],
        indirect=["aio_polars_cursor"],
    )
    async def test_fetchone_unload(self, aio_polars_cursor):
        await aio_polars_cursor.execute("SELECT * FROM one_row")
        assert aio_polars_cursor.fetchone() == (1,)
        assert aio_polars_cursor.fetchone() is None

    @pytest.mark.parametrize(
        "aio_polars_cursor",
        [{"cursor_kwargs": {"unload": True}}],
        indirect=["aio_polars_cursor"],
    )
    async def test_as_polars_unload(self, aio_polars_cursor):
        await aio_polars_cursor.execute("SELECT * FROM one_row")
        df = aio_polars_cursor.as_polars()
        assert df.height == 1
