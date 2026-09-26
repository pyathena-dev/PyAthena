from datetime import datetime
from decimal import Decimal

import pytest
import sqlalchemy
from sqlalchemy import cast, literal, select, text, types
from sqlalchemy.sql.schema import Column, MetaData, Table

from pyathena.sqlalchemy.types import AthenaArray
from tests import ENV
from tests.pyathena.util import throttle_metadata_api


class TestAsyncSQLAlchemyAthena:
    @pytest.mark.parametrize(
        "async_engine",
        [
            {"driver": driver}
            for driver in ("aiorest", "aiopandas", "aioarrow", "aiopolars", "aios3fs")
        ],
        indirect=True,
    )
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (b"", b""),
            (b"\x00\xff'\\%", b"\x00\xff'\\%"),
            (bytes(range(256)), bytes(range(256))),
            (bytearray(b"\x00\xff"), b"\x00\xff"),
            (memoryview(b"\x00\xff"), b"\x00\xff"),
        ],
        ids=["empty", "special", "all_bytes", "bytearray", "memoryview"],
    )
    async def test_binary_parameters_and_literals(self, async_engine, value, expected):
        _, conn = async_engine
        columns = [
            cast(literal(value, type_=type_, literal_execute=literal_execute), type_)
            for type_ in (types.LargeBinary, types.BINARY, types.VARBINARY)
            for literal_execute in (False, True)
        ]
        statement = select(*columns)
        assert (await conn.execute(statement)).one() == (expected,) * len(columns)
        compiled = statement.compile(dialect=conn.dialect, compile_kwargs={"literal_binds": True})
        assert (await conn.exec_driver_sql(str(compiled))).one() == (expected,) * len(columns)

    @pytest.mark.parametrize(
        "async_engine",
        [
            {"driver": "aiorest"},
            {"driver": "aiopandas"},
            {"driver": "aioarrow"},
            {"driver": "aiopolars"},
            {"driver": "aios3fs"},
            {"driver": "aiopandas", "unload": True},
            {"driver": "aioarrow", "unload": True},
        ],
        indirect=["async_engine"],
        ids=["rest", "pandas_csv", "arrow_csv", "polars", "s3fs", "pandas_unload", "arrow_unload"],
    )
    async def test_binary_null_vs_empty(self, async_engine):
        _, conn = async_engine
        columns = [
            cast(literal(value, type_=type_, literal_execute=literal_execute), type_)
            for type_ in (types.LargeBinary, types.BINARY, types.VARBINARY)
            for value in (None, b"")
            for literal_execute in (False, True)
        ]
        statement = select(*columns)
        assert (await conn.execute(statement)).one() == (None, None, b"", b"") * 3
        compiled = statement.compile(dialect=conn.dialect, compile_kwargs={"literal_binds": True})
        assert (await conn.exec_driver_sql(str(compiled))).one() == (None, None, b"", b"") * 3

    @pytest.mark.parametrize(
        "async_engine",
        [
            {"driver": "aiorest"},
            {"driver": "aiopandas"},
            {"driver": "aioarrow"},
            {"driver": "aiopolars"},
            {"driver": "aios3fs"},
        ],
        indirect=True,
    )
    async def test_basic_query(self, async_engine):
        engine, conn = async_engine
        rows = (await conn.execute(text("SELECT * FROM one_row"))).fetchall()
        assert len(rows) == 1
        assert rows[0].number_of_rows == 1
        assert len(rows[0]) == 1

    async def test_unicode(self, async_engine):
        _, conn = async_engine
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
        _, conn = async_engine
        one_row = await conn.run_sync(
            lambda sync_conn: Table("one_row", MetaData(schema=ENV.schema), autoload_with=sync_conn)
        )
        assert len(one_row.c) == 1
        assert one_row.c.number_of_rows is not None
        assert one_row.comment == "table comment"

    async def test_reflect_schemas(self, async_engine):
        _, conn = async_engine

        def _inspect(sync_conn):
            insp = sqlalchemy.inspect(sync_conn)
            return insp.get_schema_names()

        schemas = await conn.run_sync(_inspect)
        assert ENV.schema in schemas
        assert "default" in schemas

    async def test_get_table_names(self, async_engine):
        _, conn = async_engine

        def _inspect(sync_conn):
            insp = sqlalchemy.inspect(sync_conn)
            return insp.get_table_names(schema=ENV.schema)

        table_names = await conn.run_sync(_inspect)
        assert "many_rows" in table_names

    async def test_throttled_reflection_reads_glue(self, async_engine, monkeypatch):
        _, conn = async_engine

        def reflect(sync_conn):
            insp = sqlalchemy.inspect(sync_conn)
            return (
                insp.get_table_comment("one_row", schema=ENV.schema),
                insp.get_table_options("one_row", schema=ENV.schema),
                insp.get_table_names(schema=ENV.schema),
            )

        expected = await conn.run_sync(reflect)
        # The adapter wraps an AioConnection, whose client serves the metadata API.
        client = (await conn.get_raw_connection()).driver_connection.driver_connection.client
        calls = throttle_metadata_api(
            client, monkeypatch, operations=("get_table_metadata", "list_table_metadata")
        )

        # Glue runs in a worker thread here, as the adapted cursor's calls do.
        assert await conn.run_sync(reflect) == expected
        assert sorted(calls) == ["get_table_metadata", "list_table_metadata"]

    async def test_has_table(self, async_engine):
        _, conn = async_engine

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
        _, conn = async_engine

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

    @pytest.mark.parametrize(
        ("operation", "expected"),
        [
            pytest.param(
                "INSERT INTO {table} SELECT id+10, group_id+10, value "
                "FROM {table} WHERE group_id=:group_id",
                [(1, 10), (2, 20), (3, 30), (11, 10), (12, 20), (13, 30)],
                id="insert",
            ),
            pytest.param(
                "UPDATE {table} SET value=value+1 WHERE group_id=:group_id",
                [(1, 11), (2, 21), (3, 31)],
                id="update",
            ),
            pytest.param("DELETE FROM {table} WHERE group_id=:group_id", [], id="delete"),
        ],
    )
    async def test_executemany_rowcount(self, async_engine, executemany_table, operation, expected):
        _, conn = async_engine
        statement = text(operation.format(table=executemany_table))
        result = await conn.execute(statement, [{"group_id": 1}, {"group_id": 2}, {"group_id": 99}])
        assert result.rowcount == 3
        assert not result.returns_rows
        with pytest.raises(sqlalchemy.exc.ResourceClosedError):
            result.fetchall()

        result = await conn.execute(statement, [{"group_id": 99}, {"group_id": 100}])
        assert result.rowcount == 0
        rows = (
            await conn.execute(text(f"SELECT id, value FROM {executemany_table} ORDER BY id"))
        ).fetchall()
        assert rows == expected

    async def test_executemany_failure(self, async_engine, executemany_table):
        _, conn = async_engine
        statement = text(
            f"UPDATE {executemany_table} SET value=value+1 "
            "WHERE group_id=CAST(:group_id AS INTEGER)"
        )
        with pytest.raises(sqlalchemy.exc.OperationalError):
            await conn.execute(
                statement, [{"group_id": "1"}, {"group_id": "invalid"}, {"group_id": "2"}]
            )
        rows = (
            await conn.execute(text(f"SELECT id, value FROM {executemany_table} ORDER BY id"))
        ).fetchall()
        assert rows == [(1, 11), (2, 21), (3, 30)]
        result = await conn.execute(statement, {"group_id": "2"})
        assert result.rowcount == 1

    async def test_insertmanyvalues(self, async_engine):
        _, conn = async_engine
        table_name = "insertmanyvalues_async"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column("id", types.Integer),
            Column("name", types.String),
            Column("data", types.LargeBinary),
            Column("ts", types.DateTime),
            Column("amount", types.Numeric(10, 3)),
            Column("tags", AthenaArray(types.Integer)),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/",
            awsathena_tblproperties={"table_type": "ICEBERG"},
        )
        rows = [
            {
                "id": 1,
                "name": "it's",
                "data": b"\x00\x01",
                "ts": datetime(2026, 1, 2, 3, 4, 5, 123000),
                "amount": Decimal("1.5"),
                "tags": [1, None],
            },
            {
                "id": 2,
                "name": None,
                "data": None,
                "ts": datetime(2026, 1, 2, 3, 4, 5, 123456),
                "amount": Decimal("12.345"),
                "tags": None,
            },
            {"id": 3, "name": "c", "data": b"", "ts": None, "amount": None, "tags": []},
            {"id": 4, "name": "d", "data": b"d", "ts": None, "amount": None, "tags": [4]},
            {"id": 5, "name": "e", "data": b"e", "ts": None, "amount": None, "tags": [5, 5]},
        ]

        statements = []

        def record(conn, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        await conn.run_sync(table.create)
        sqlalchemy.event.listen(conn.sync_connection, "before_cursor_execute", record)
        try:
            result = await conn.execute(
                table.insert(), rows, execution_options={"insertmanyvalues_page_size": 2}
            )
        finally:
            sqlalchemy.event.remove(conn.sync_connection, "before_cursor_execute", record)

        # One event per page; a DB API executemany would fire a single event.
        assert len(statements) == 3
        assert result.rowcount == 5
        actual = (await conn.execute(select(table).order_by(table.c.id))).mappings().all()
        assert [dict(row) for row in actual] == rows
