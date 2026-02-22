import pytest

from pyathena.aio.s3fs.cursor import AioS3FSCursor
from pyathena.error import ProgrammingError
from pyathena.s3fs.result_set import AthenaS3FSResultSet
from tests import ENV
from tests.pyathena.aio.conftest import _aio_connect


class TestAioS3FSCursor:
    async def test_fetchone(self, aio_s3fs_cursor):
        await aio_s3fs_cursor.execute("SELECT * FROM one_row")
        assert aio_s3fs_cursor.rownumber == 0
        assert await aio_s3fs_cursor.fetchone() == (1,)
        assert aio_s3fs_cursor.rownumber == 1
        assert await aio_s3fs_cursor.fetchone() is None

    async def test_fetchmany(self, aio_s3fs_cursor):
        await aio_s3fs_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        assert len(await aio_s3fs_cursor.fetchmany(10)) == 10
        assert len(await aio_s3fs_cursor.fetchmany(10)) == 5

    async def test_fetchall(self, aio_s3fs_cursor):
        await aio_s3fs_cursor.execute("SELECT * FROM one_row")
        assert await aio_s3fs_cursor.fetchall() == [(1,)]
        await aio_s3fs_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        assert await aio_s3fs_cursor.fetchall() == [(i,) for i in range(10000)]

    async def test_description(self, aio_s3fs_cursor):
        await aio_s3fs_cursor.execute("SELECT 1 AS foobar FROM one_row")
        assert await aio_s3fs_cursor.fetchall() == [(1,)]
        assert aio_s3fs_cursor.description == [("foobar", "integer", None, None, 10, 0, "UNKNOWN")]

    async def test_description_initial(self, aio_s3fs_cursor):
        assert aio_s3fs_cursor.description is None

    async def test_cancel_initial(self, aio_s3fs_cursor):
        with pytest.raises(ProgrammingError):
            await aio_s3fs_cursor.cancel()

    async def test_executemany_fetch(self, aio_s3fs_cursor):
        await aio_s3fs_cursor.executemany(
            "SELECT %(x)d FROM one_row", [{"x": i} for i in range(1, 2)]
        )
        with pytest.raises(ProgrammingError):
            await aio_s3fs_cursor.fetchall()
        with pytest.raises(ProgrammingError):
            await aio_s3fs_cursor.fetchmany()
        with pytest.raises(ProgrammingError):
            await aio_s3fs_cursor.fetchone()

    async def test_arraysize_default(self, aio_s3fs_cursor):
        assert aio_s3fs_cursor.arraysize == AthenaS3FSResultSet.DEFAULT_FETCH_SIZE

    async def test_invalid_arraysize(self, aio_s3fs_cursor):
        aio_s3fs_cursor.arraysize = 10000
        assert aio_s3fs_cursor.arraysize == 10000
        with pytest.raises(ProgrammingError):
            aio_s3fs_cursor.arraysize = -1

    async def test_async_iterator(self, aio_s3fs_cursor):
        await aio_s3fs_cursor.execute("SELECT * FROM one_row")
        rows = [row async for row in aio_s3fs_cursor]
        assert rows == [(1,)]

    async def test_context_manager(self):
        conn = await _aio_connect(schema_name=ENV.schema)
        try:
            async with conn.cursor(AioS3FSCursor) as cursor:
                await cursor.execute("SELECT * FROM one_row")
                assert await cursor.fetchone() == (1,)
        finally:
            conn.close()

    async def test_execute_returns_self(self, aio_s3fs_cursor):
        result = await aio_s3fs_cursor.execute("SELECT * FROM one_row")
        assert result is aio_s3fs_cursor
