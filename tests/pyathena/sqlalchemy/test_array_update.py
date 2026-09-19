import pytest
from sqlalchemy import Column, Integer, MetaData, Table, bindparam, func, types, update
from sqlalchemy import exc as sa_exc
from sqlalchemy.orm import declarative_base

from pyathena.formatter import DefaultParameterFormatter
from pyathena.sqlalchemy.base import AthenaDialect
from pyathena.sqlalchemy.types import AthenaArray


def array_table(type_=None):
    return Table(
        "arrays", MetaData(), Column("id", Integer), Column("items", type_ or AthenaArray(Integer))
    )


@pytest.mark.parametrize(
    ("target", "value"),
    [(1, 2), (4, None), (slice(2, 3), [4]), (slice(2, 2), []), (slice(None), [])],
)
def test_partial_update_compiles_to_one_whole_column_assignment(target, value):
    table = array_table()
    statement = table.update().values({table.c["items"][target]: value}).where(table.c.id == 1)
    original_key = statement._generate_cache_key().key
    compiled = statement.compile(dialect=AthenaDialect())
    sql = str(compiled)
    assert sql.startswith("UPDATE arrays SET items=")
    assert "SET element_at" not in sql
    assert "SELECT" not in sql
    assert "WHERE arrays.id =" in sql
    assert statement._generate_cache_key().key == original_key
    parameters = {
        name: compiled._bind_processors.get(name, lambda v: v)(value)
        for name, value in compiled.params.items()
    }
    formatted = DefaultParameterFormatter().format(sql, parameters)
    assert "ARRAY[" in formatted


@pytest.mark.parametrize("index", [0, -1, None, 1.5, True])
def test_invalid_partial_update_index(index):
    table = array_table()
    with pytest.raises(sa_exc.CompileError, match="indices"):
        table.update().values({table.c["items"][index]: 1}).compile(dialect=AthenaDialect())


def test_multiple_updates_to_one_array_are_rejected():
    table = array_table()
    values = table.c["items"]
    for assignments in (
        {values[1]: 2, values[2]: 3},
        {values: [], values[1]: 2},
        {values[1]: 2, "items": []},
    ):
        with pytest.raises(sa_exc.CompileError, match="one assignment"):
            table.update().values(assignments).compile(dialect=AthenaDialect())


def test_slice_null_and_nested_slice_rejected():
    table = array_table(AthenaArray(Integer, dimensions=2))
    with pytest.raises(sa_exc.CompileError, match="non-NULL array"):
        table.update().values({table.c["items"][1:2]: None}).compile(dialect=AthenaDialect())
    with pytest.raises(sa_exc.CompileError, match="final"):
        table.update().values({table.c["items"][1:2][1]: [2]}).compile(dialect=AthenaDialect())


def test_bound_indices_and_values_are_reused_without_mutation():
    table = array_table()
    expression = table.c["items"][bindparam("index")]
    statement = table.update().values({expression: bindparam("value"), table.c.id: 2})
    compiled = statement.compile(dialect=AthenaDialect())
    assert set(compiled.params) == {"index", "value", "id"}
    assert compiled._bind_processors["index"](2) == 2
    with pytest.raises(ValueError, match="integers"):
        compiled._bind_processors["index"](1.5)
    assert str(statement.compile(dialect=AthenaDialect())) == str(compiled)


def test_nested_and_zero_indexed_update():
    table = array_table(AthenaArray(Integer, dimensions=2, zero_indexes=True))
    statement = table.update().values({table.c["items"][0][2]: 7})
    sql = str(statement.compile(dialect=AthenaDialect(), compile_kwargs={"literal_binds": True}))
    assert "ARRAY[concat(" in sql
    assert "sequence(" not in sql
    assert "IF(1 > 0, 1," in sql
    assert "IF(3 > 0, 3," in sql


def test_generic_array_partial_update():
    table = array_table(types.ARRAY(Integer))
    sql = str(table.update().values({table.c["items"][1]: 2}).compile(dialect=AthenaDialect()))
    assert "SET items=concat(" in sql


def test_write_index_expression_keeps_its_argument_types():
    table = array_table()
    index = func.length("abc")
    statement = table.update().values({table.c["items"][index]: 9})
    compiled = statement.compile(dialect=AthenaDialect())
    params = {
        name: compiled._bind_processors.get(name, lambda value: value)(value)
        for name, value in compiled.params.items()
    }
    assert "length('abc')" in DefaultParameterFormatter().format(str(compiled), params)


def test_ordered_partial_update_with_sql_expression():
    table = array_table()
    items = table.c["items"]
    statement = table.update().ordered_values((items[2], items[1] + 1), (table.c.id, 2))
    compiled = str(statement.compile(dialect=AthenaDialect()))
    assert compiled.index("SET items=") < compiled.index(", id=")
    assert "element_at(arrays.items" in compiled


def test_binary_element_assignment_uses_native_hex_parameter():
    table = array_table(AthenaArray(types.BINARY))
    compiled = (
        table.update().values({table.c["items"][1]: b"\x00\xff"}).compile(dialect=AthenaDialect())
    )
    params = {
        name: compiled._bind_processors.get(name, lambda value: value)(value)
        for name, value in compiled.params.items()
    }
    assert "FROM_HEX('00ff')" in DefaultParameterFormatter().format(str(compiled), params)


class PrefixString(types.TypeDecorator):
    impl = types.String
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return f"prefix:{value}"

    def bind_expression(self, bindvalue):
        return func.upper(bindvalue)


def test_explicit_assignment_type_and_callable_bindings():
    table = array_table(AthenaArray(types.String))
    stmt = table.update().values(
        {
            table.c["items"][bindparam("index", callable_=lambda: 1)]: bindparam(
                "value", type_=PrefixString(), callable_=lambda: "a"
            )
        }
    )
    compiled = stmt.compile(dialect=AthenaDialect())
    assert compiled._bind_processors["value"]("a") == "prefix:a"
    assert "upper(%(value)s)" in str(compiled)
    assert compiled.params["index"] == 1
    assert compiled.params["value"] == "a"
    table.update().values(
        {table.c["items"][1:2]: bindparam("values", callable_=lambda: ["a"])}
    ).compile(dialect=AthenaDialect())


def test_orm_partial_update_and_renamed_attribute_conflicts():
    base = declarative_base()

    class Model(base):
        __tablename__ = "arrays"
        id = Column(Integer, primary_key=True)
        values = Column("stored", AthenaArray(Integer), key="db_key")

    sql = str(update(Model).values({Model.values[1]: 2}).compile(dialect=AthenaDialect()))
    assert "UPDATE arrays SET stored=concat(" in sql
    for whole in (Model.values, "values"):
        with pytest.raises(sa_exc.CompileError, match="one assignment"):
            update(Model).values({Model.values[1]: 2, whole: []}).compile(dialect=AthenaDialect())
