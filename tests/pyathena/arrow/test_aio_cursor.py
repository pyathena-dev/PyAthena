# -*- coding: utf-8 -*-
import pytest

from pyathena.arrow.result_set import AthenaArrowResultSet
from pyathena.error import ProgrammingError
from tests import ENV
from tests.pyathena.aio.conftest import _aio_connect


@pytest.fixture
async def aio_arrow_cursor(request):
    from pyathena.arrow.aio_cursor import AioArrowCursor

    if not hasattr(request, "param"):
        setattr(request, "param", {})  # noqa: B010
    conn = await _aio_connect(schema_name=ENV.schema, cursor_class=AioArrowCursor, **request.param)
    try:
        async with conn.cursor() as cursor:
            yield cursor
    finally:
        conn.close()


class TestAioArrowCursor:
    async def test_fetchone(self, aio_arrow_cursor):
        await aio_arrow_cursor.execute("SELECT * FROM one_row")
        assert aio_arrow_cursor.rownumber == 0
        assert aio_arrow_cursor.fetchone() == (1,)
        assert aio_arrow_cursor.rownumber == 1
        assert aio_arrow_cursor.fetchone() is None

    async def test_fetchmany(self, aio_arrow_cursor):
        await aio_arrow_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        assert len(aio_arrow_cursor.fetchmany(10)) == 10
        assert len(aio_arrow_cursor.fetchmany(10)) == 5

    async def test_fetchall(self, aio_arrow_cursor):
        await aio_arrow_cursor.execute("SELECT * FROM one_row")
        assert aio_arrow_cursor.fetchall() == [(1,)]
        await aio_arrow_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        assert aio_arrow_cursor.fetchall() == [(i,) for i in range(10000)]

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
            aio_arrow_cursor.fetchone()
        with pytest.raises(ProgrammingError):
            aio_arrow_cursor.fetchmany()
        with pytest.raises(ProgrammingError):
            aio_arrow_cursor.fetchall()
        with pytest.raises(ProgrammingError):
            aio_arrow_cursor.as_arrow()
        with pytest.raises(ProgrammingError):
            aio_arrow_cursor.as_polars()

    async def test_context_manager(self):
        from pyathena.arrow.aio_cursor import AioArrowCursor

        conn = await _aio_connect(schema_name=ENV.schema, cursor_class=AioArrowCursor)
        try:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT * FROM one_row")
                assert cursor.fetchone() == (1,)
        finally:
            conn.close()

    async def test_arraysize_default(self, aio_arrow_cursor):
        assert aio_arrow_cursor.arraysize == AthenaArrowResultSet.DEFAULT_FETCH_SIZE

    async def test_invalid_arraysize(self, aio_arrow_cursor):
        aio_arrow_cursor.arraysize = 10000
        assert aio_arrow_cursor.arraysize == 10000
        with pytest.raises(ProgrammingError):
            aio_arrow_cursor.arraysize = -1

    @pytest.mark.parametrize(
        "aio_arrow_cursor",
        [{"cursor_kwargs": {"unload": True}}],
        indirect=["aio_arrow_cursor"],
    )
    async def test_fetchone_unload(self, aio_arrow_cursor):
        await aio_arrow_cursor.execute("SELECT * FROM one_row")
        assert aio_arrow_cursor.fetchone() == (1,)
        assert aio_arrow_cursor.fetchone() is None

    @pytest.mark.parametrize(
        "aio_arrow_cursor",
        [{"cursor_kwargs": {"unload": True}}],
        indirect=["aio_arrow_cursor"],
    )
    async def test_as_arrow_unload(self, aio_arrow_cursor):
        await aio_arrow_cursor.execute("SELECT * FROM one_row")
        table = aio_arrow_cursor.as_arrow()
        assert table.num_rows == 1
