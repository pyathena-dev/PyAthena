# -*- coding: utf-8 -*-
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql.schema import MetaData, Table

from tests import ASYNC_SQLALCHEMY_CONNECTION_STRING, ENV


def _async_conn_str(**kwargs):
    return ASYNC_SQLALCHEMY_CONNECTION_STRING.format(
        region_name=ENV.region_name,
        schema_name=ENV.schema,
        s3_staging_dir=ENV.s3_staging_dir,
        location=ENV.s3_staging_dir,
        **kwargs,
    )


class TestAsyncSQLAlchemyAthena:
    async def test_basic_query(self, async_engine):
        engine, conn = async_engine
        rows = (await conn.execute(text("SELECT * FROM one_row"))).fetchall()
        assert len(rows) == 1
        assert rows[0].number_of_rows == 1
        assert len(rows[0]) == 1

    async def test_unicode(self, async_engine):
        engine, conn = async_engine
        unicode_str = "密林"
        returned_str = (
            await conn.execute(
                sqlalchemy.select(
                    sqlalchemy.sql.expression.bindparam(
                        "あまぞん", unicode_str, type_=sqlalchemy.types.String()
                    )
                )
            )
        ).scalar()
        assert returned_str == unicode_str

    async def test_reflect_table(self, async_engine):
        engine, conn = async_engine
        one_row = await conn.run_sync(
            lambda sync_conn: Table("one_row", MetaData(schema=ENV.schema), autoload_with=sync_conn)
        )
        assert len(one_row.c) == 1
        assert one_row.c.number_of_rows is not None
        assert one_row.comment == "table comment"

    async def test_reflect_schemas(self, async_engine):
        engine, conn = async_engine

        def _inspect(sync_conn):
            insp = sqlalchemy.inspect(sync_conn)
            return insp.get_schema_names()

        schemas = await conn.run_sync(_inspect)
        assert ENV.schema in schemas
        assert "default" in schemas

    async def test_get_table_names(self, async_engine):
        engine, conn = async_engine

        def _inspect(sync_conn):
            insp = sqlalchemy.inspect(sync_conn)
            return insp.get_table_names(schema=ENV.schema)

        table_names = await conn.run_sync(_inspect)
        assert "many_rows" in table_names

    async def test_has_table(self, async_engine):
        engine, conn = async_engine

        def _inspect(sync_conn):
            insp = sqlalchemy.inspect(sync_conn)
            return (
                insp.has_table("one_row", schema=ENV.schema),
                insp.has_table("this_table_does_not_exist", schema=ENV.schema),
            )

        exists, not_exists = await conn.run_sync(_inspect)
        assert exists
        assert not not_exists

    async def test_get_columns(self, async_engine):
        engine, conn = async_engine

        def _inspect(sync_conn):
            insp = sqlalchemy.inspect(sync_conn)
            return insp.get_columns(table_name="one_row", schema=ENV.schema)

        columns = await conn.run_sync(_inspect)
        actual = columns[0]
        assert actual["name"] == "number_of_rows"
        assert isinstance(actual["type"], sqlalchemy.types.INTEGER)
        assert actual["nullable"]
        assert actual["default"] is None
        assert not actual["autoincrement"]
        assert actual["comment"] == "some comment"


class TestAsyncDialectProperties:
    async def test_aiorest_dialect(self):
        engine = create_async_engine(_async_conn_str())
        try:
            assert engine.dialect.is_async is True
            assert engine.dialect.driver == "aiorest"
            assert engine.dialect.supports_statement_cache is True
        finally:
            await engine.dispose()

    async def test_aiopandas_dialect(self):
        engine = create_async_engine(_async_conn_str().replace("+aiorest", "+aiopandas"))
        try:
            assert engine.dialect.is_async is True
            assert engine.dialect.driver == "aiopandas"
        finally:
            await engine.dispose()

    async def test_aioarrow_dialect(self):
        engine = create_async_engine(_async_conn_str().replace("+aiorest", "+aioarrow"))
        try:
            assert engine.dialect.is_async is True
            assert engine.dialect.driver == "aioarrow"
        finally:
            await engine.dispose()

    async def test_aiopolars_dialect(self):
        engine = create_async_engine(_async_conn_str().replace("+aiorest", "+aiopolars"))
        try:
            assert engine.dialect.is_async is True
            assert engine.dialect.driver == "aiopolars"
        finally:
            await engine.dispose()

    async def test_aios3fs_dialect(self):
        engine = create_async_engine(_async_conn_str().replace("+aiorest", "+aios3fs"))
        try:
            assert engine.dialect.is_async is True
            assert engine.dialect.driver == "aios3fs"
        finally:
            await engine.dispose()
