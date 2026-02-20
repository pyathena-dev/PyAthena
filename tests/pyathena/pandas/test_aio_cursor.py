# -*- coding: utf-8 -*-
import pytest

from pyathena.error import ProgrammingError
from pyathena.pandas.result_set import AthenaPandasResultSet
from tests import ENV
from tests.pyathena.aio.conftest import _aio_connect


class TestAioPandasCursor:
    async def test_fetchone(self, aio_pandas_cursor):
        await aio_pandas_cursor.execute("SELECT * FROM one_row")
        assert aio_pandas_cursor.rownumber == 0
        assert aio_pandas_cursor.fetchone() == (1,)
        assert aio_pandas_cursor.rownumber == 1
        assert aio_pandas_cursor.fetchone() is None

    async def test_fetchmany(self, aio_pandas_cursor):
        await aio_pandas_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        assert len(aio_pandas_cursor.fetchmany(10)) == 10
        assert len(aio_pandas_cursor.fetchmany(10)) == 5

    async def test_fetchall(self, aio_pandas_cursor):
        await aio_pandas_cursor.execute("SELECT * FROM one_row")
        assert aio_pandas_cursor.fetchall() == [(1,)]
        await aio_pandas_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        assert aio_pandas_cursor.fetchall() == [(i,) for i in range(10000)]

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
            aio_pandas_cursor.fetchone()
        with pytest.raises(ProgrammingError):
            aio_pandas_cursor.fetchmany()
        with pytest.raises(ProgrammingError):
            aio_pandas_cursor.fetchall()
        with pytest.raises(ProgrammingError):
            aio_pandas_cursor.as_pandas()

    async def test_context_manager(self):
        from pyathena.pandas.aio_cursor import AioPandasCursor

        conn = await _aio_connect(schema_name=ENV.schema, cursor_class=AioPandasCursor)
        try:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT * FROM one_row")
                assert cursor.fetchone() == (1,)
        finally:
            conn.close()

    async def test_arraysize_default(self, aio_pandas_cursor):
        assert aio_pandas_cursor.arraysize == AthenaPandasResultSet.DEFAULT_FETCH_SIZE

    async def test_invalid_arraysize(self, aio_pandas_cursor):
        aio_pandas_cursor.arraysize = 10000
        assert aio_pandas_cursor.arraysize == 10000
        with pytest.raises(ProgrammingError):
            aio_pandas_cursor.arraysize = -1

    @pytest.mark.parametrize(
        "aio_pandas_cursor",
        [{"cursor_kwargs": {"unload": True}}],
        indirect=["aio_pandas_cursor"],
    )
    async def test_fetchone_unload(self, aio_pandas_cursor):
        await aio_pandas_cursor.execute("SELECT * FROM one_row")
        assert aio_pandas_cursor.fetchone() == (1,)
        assert aio_pandas_cursor.fetchone() is None

    @pytest.mark.parametrize(
        "aio_pandas_cursor",
        [{"cursor_kwargs": {"unload": True}}],
        indirect=["aio_pandas_cursor"],
    )
    async def test_as_pandas_unload(self, aio_pandas_cursor):
        await aio_pandas_cursor.execute("SELECT * FROM one_row")
        df = aio_pandas_cursor.as_pandas()
        assert len(df) == 1
