import json
import pickle
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from types import SimpleNamespace

import pytest
from sqlalchemy import (
    LABEL_STYLE_TABLENAME_PLUS_COL,
    Column,
    Integer,
    MetaData,
    String,
    Table,
    all_,
    any_,
    bindparam,
    cast,
    column,
    func,
    literal,
    literal_column,
    select,
    text,
    types,
    update,
)
from sqlalchemy import exc as sa_exc
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import sqltypes

import pyathena
from pyathena.formatter import DefaultParameterFormatter
from pyathena.sqlalchemy.array import _ArrayWriteIndexType
from pyathena.sqlalchemy.base import AthenaDialect
from pyathena.sqlalchemy.types import (
    ARRAY,
    AthenaArray,
    AthenaDate,
    AthenaMap,
    AthenaStruct,
    AthenaTimestamp,
)


class Color(Enum):
    RED = "red"


class OffsetInteger(types.TypeDecorator):
    impl = Integer
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return value - 1 if value is not None else None

    def process_result_value(self, value, dialect):
        return value + 1 if value is not None else None


class DecoratedTimestamp(types.TypeDecorator):
    impl = types.TIMESTAMP
    cache_ok = True

    def process_result_value(self, value, dialect):
        assert value is None or isinstance(value, datetime)
        return value


class JSONEncodedDict(types.TypeDecorator):
    impl = String
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return json.dumps(value)

    def process_result_value(self, value, dialect):
        return json.loads(value)


class TupleArray(types.TypeDecorator):
    impl = AthenaArray(Integer)
    cache_ok = True

    def process_result_value(self, value, dialect):
        return tuple(value) if value is not None else None


class PrefixString(types.TypeDecorator):
    impl = types.String
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return f"prefix:{value}"

    def bind_expression(self, bindvalue):
        return func.upper(bindvalue)


def _array_update_table(type_=None):
    return Table(
        "arrays",
        MetaData(),
        Column("id", Integer),
        Column("items", type_ or AthenaArray(Integer)),
    )


class TestAthenaArray:
    def test_creation_with_default(self):
        array_type = AthenaArray()
        assert isinstance(array_type.item_type, sqltypes.String)

    def test_creation_with_type_class(self):
        array_type = AthenaArray(Integer)
        assert isinstance(array_type.item_type, sqltypes.Integer)

    def test_creation_with_type_instance(self):
        array_type = AthenaArray(Integer())
        assert isinstance(array_type.item_type, sqltypes.Integer)

    def test_creation_with_string_type(self):
        array_type = AthenaArray(String)
        assert isinstance(array_type.item_type, sqltypes.String)

    def test_python_type(self):
        array_type = AthenaArray()
        assert array_type.python_type is list

    def test_visit_name(self):
        array_type = AthenaArray()
        assert array_type.__visit_name__ == "array"

    def test_array_uppercase_visit_name(self):
        array_type = ARRAY()
        assert array_type.__visit_name__ == "ARRAY"

    def test_array_with_complex_type(self):
        array_type = AthenaArray(AthenaStruct(("name", String), ("age", Integer)))
        assert isinstance(array_type.item_type, AthenaStruct)
        assert "name" in array_type.item_type.fields
        assert "age" in array_type.item_type.fields

    def test_array_with_nested_array(self):
        array_type = AthenaArray(AthenaArray(Integer))
        assert isinstance(array_type.item_type, AthenaArray)
        assert isinstance(array_type.item_type.item_type, sqltypes.Integer)

    def test_array_with_map_type(self):
        array_type = AthenaArray(AthenaMap(String, Integer))
        assert isinstance(array_type.item_type, AthenaMap)
        assert isinstance(array_type.item_type.key_type, sqltypes.String)
        assert isinstance(array_type.item_type.value_type, sqltypes.Integer)

    @pytest.mark.parametrize(
        ("signature", "expected"),
        [
            ("array<integer>", AthenaArray(types.INTEGER)),
            ("ARRAY(ARRAY(VARCHAR(32)))", AthenaArray(AthenaArray(types.VARCHAR(32)))),
            ("array<decimal(18,7)>", AthenaArray(types.DECIMAL(18, 7))),
            (
                "array<map<string,array<int>>>",
                AthenaArray(AthenaMap(String, AthenaArray(types.INTEGER))),
            ),
            (
                'array<struct<"a,b":decimal(10,2),`c:d`:varchar(17)>>',
                AthenaArray(
                    AthenaStruct(("a,b", types.DECIMAL(10, 2)), ("c:d", types.VARCHAR(17)))
                ),
            ),
        ],
    )
    def test_array_reflection_preserves_element_types(self, signature, expected):
        actual = AthenaDialect()._get_column_type(signature)
        assert isinstance(actual, types.ARRAY)
        assert actual._static_cache_key == expected._static_cache_key

    @pytest.mark.parametrize("dimensions", [0, -1, True, 1.5])
    def test_array_rejects_invalid_dimensions(self, dimensions):
        with pytest.raises(ValueError, match="positive integer"):
            AthenaArray(Integer, dimensions=dimensions)

    def test_array_rejects_ambiguous_dimensions(self):
        with pytest.raises(ValueError, match="either nested ARRAY types or dimensions"):
            AthenaArray(AthenaArray(Integer), dimensions=2)

    def test_array_insert_uses_typed_parameter(self):
        formatter = DefaultParameterFormatter()
        table = Table("array_values", MetaData(), Column("items", types.ARRAY(Integer)))
        compiled = table.insert().values(items=[1, 2]).compile(dialect=AthenaDialect())
        params = {
            name: compiled._bind_processors[name](value) for name, value in compiled.params.items()
        }
        assert (
            formatter.format(str(compiled), params)
            == "INSERT INTO array_values (items) VALUES (CAST(ARRAY[1, 2] AS ARRAY(INTEGER)))"
        )

    def test_array_cache_key_includes_nested_fields(self):
        first = AthenaArray(AthenaStruct(("x", Integer)))
        second = AthenaArray(AthenaStruct(("x", String)))
        assert first._static_cache_key != second._static_cache_key
        assert hash(first._static_cache_key)

    @pytest.mark.parametrize(
        ("item_type", "value"),
        [
            (types.Numeric(), Decimal("1.50")),
            (types.Numeric(scale=2), Decimal("1.50")),
            (AthenaStruct(("amount", types.Numeric())), {"amount": Decimal("1.50")}),
        ],
    )
    def test_array_decimal_bind_requires_precision(self, item_type, value):
        statement = select(literal([value], AthenaArray(item_type)))
        with pytest.raises(sa_exc.CompileError, match="explicit Numeric precision"):
            statement.compile(dialect=AthenaDialect())

    def test_explicit_array_decimal_cast_keeps_default_precision(self):
        value = literal([Decimal("1.50")], AthenaArray(types.Numeric(10, 2)))
        sql = str(cast(value, AthenaArray(types.Numeric())).compile(dialect=AthenaDialect()))
        assert sql == "CAST(CAST(%(param_1)s AS ARRAY(DECIMAL(10, 2))) AS ARRAY(DECIMAL))"

    @pytest.mark.parametrize(
        "signature",
        [
            "array<map<int>>",
            "array<struct<x>>",
            "array<decimal(nope,2)>",
            "array(row(integer, varchar))",
        ],
    )
    def test_array_reflection_warns_for_unrecognized_nested_type(self, signature):
        with pytest.warns(sa_exc.SAWarning, match="Did not recognize type"):
            type_ = AthenaDialect()._get_column_type(signature)
        assert isinstance(type_, types.NullType)


class TestAthenaArrayComparator:
    @staticmethod
    def _compile_sql(expression):
        return str(
            expression.compile(dialect=AthenaDialect(), compile_kwargs={"literal_binds": True})
        )

    @pytest.mark.parametrize("array_type", [AthenaArray(Integer), types.ARRAY(Integer)])
    @pytest.mark.parametrize("index", [-2, 0, 1, 100])
    def test_array_index(self, array_type, index):
        value = column("items", array_type)
        assert (
            self._compile_sql(value[index]) == f"element_at(items, NULLIF(greatest({index}, 0), 0))"
        )
        assert isinstance(value[index].type, Integer)

    def test_array_dimensions_and_zero_indexes(self):
        value = column(
            "items", AthenaArray(Integer, dimensions=3, zero_indexes=True, as_tuple=True)
        )
        assert value[0].type.dimensions == 2
        assert value[0][0].type.dimensions == 1
        assert isinstance(value[0][0][0].type, Integer)
        assert value[0].type.as_tuple
        assert value[0].type.zero_indexes
        assert "NULLIF(greatest(1, 0), 0)" in self._compile_sql(value[0])
        assert "greatest(1, 1)" in self._compile_sql(value[:0])
        assert "least(1, cardinality(items))" in self._compile_sql(value[:0])
        assert "cardinality(items)" in self._compile_sql(value[0:])
        nested = column("nested", AthenaArray(AthenaArray(String)))
        assert isinstance(nested[1].type, AthenaArray)
        assert isinstance(nested[1][1].type, String)

    @pytest.mark.parametrize(
        "bounds", [slice(None), slice(1, 2), slice(-2, 100), slice(3, 1), slice(1, 3, 1)]
    )
    def test_array_slice(self, bounds):
        value = column("items", AthenaArray(Integer))
        result = value[bounds]
        assert result.type is value.type
        sql = self._compile_sql(result)
        assert sql.startswith("slice(items, greatest(")
        assert "greatest(least(" in sql

    @pytest.mark.parametrize("step", [0, 2, -1, True, 1.0, bindparam("step", 1)])
    def test_array_slice_rejects_steps(self, step):
        with pytest.raises(sa_exc.CompileError, match="step"):
            self._compile_sql(column("items", AthenaArray(Integer))[1:3:step])

    def test_decorated_array_index_and_slice(self):
        items = column("items", TupleArray())
        assert self._compile_sql(items[1]) == "element_at(items, NULLIF(greatest(1, 0), 0))"
        assert isinstance(items[1].type, Integer)
        compiled = select(items[1:2]).compile(dialect=AthenaDialect())
        assert "transform(slice(items," in str(compiled)
        processor = (
            compiled._result_columns[0]
            .type.dialect_impl(AthenaDialect())
            .result_processor(AthenaDialect(), None)
        )
        assert processor('{"_pyathena_array":["1","2"]}') == (1, 2)


class TestArrayTypeInspector:
    @pytest.mark.parametrize(
        ("type_", "value", "expected"),
        [
            (TupleArray(), 2, "2"),
            (String().with_variant(TupleArray(), "awsathena"), 2, "2"),
            (Integer().with_variant(TupleArray(), "awsathena"), 2, "2"),
            (String().with_variant(AthenaArray(String), "awsathena"), "a", "'a'"),
        ],
    )
    def test_decorated_and_variant_array_quantifiers(self, type_, value, expected):
        items = column("items", type_)
        statement = select(any_(items) == value, all_(items) > value)
        sql = str(
            statement.compile(dialect=AthenaDialect(), compile_kwargs={"literal_binds": True})
        )
        assert f"any_match((items), _pyathena_element_0 -> {expected} = _pyathena_element_0)" in sql
        assert f"all_match((items), _pyathena_element_1 -> {expected} < _pyathena_element_1)" in sql

    @pytest.mark.parametrize(
        ("type_", "ddl", "dml"),
        [
            (types.ARRAY(Integer), "ARRAY<INT>", "ARRAY(INTEGER)"),
            (AthenaArray(), "ARRAY<STRING>", "ARRAY(VARCHAR)"),
            (ARRAY(String(12)), "ARRAY<STRING>", "ARRAY(VARCHAR)"),
            (types.ARRAY(String, dimensions=2), "ARRAY<ARRAY<STRING>>", "ARRAY(ARRAY(VARCHAR))"),
            (AthenaArray(AthenaArray(Integer)), "ARRAY<ARRAY<INT>>", "ARRAY(ARRAY(INTEGER))"),
            (AthenaArray(types.Numeric(12, 3)), "ARRAY<DECIMAL(12, 3)>", "ARRAY(DECIMAL(12, 3))"),
            (AthenaArray(types.Float), "ARRAY<FLOAT>", "ARRAY(REAL)"),
            (AthenaArray(types.BINARY), "ARRAY<BINARY>", "ARRAY(VARBINARY)"),
        ],
    )
    def test_array_type_rendering(self, type_, ddl, dml):
        dialect = AthenaDialect()
        assert type_.compile(dialect=dialect) == ddl
        assert str(cast(literal(None), type_).compile(dialect=dialect)).endswith(f"AS {dml})")
        assert isinstance(type_.dialect_impl(dialect), types.ARRAY)

    @pytest.mark.parametrize("item_type", [types.Double, types.DOUBLE, types.DOUBLE_PRECISION])
    def test_array_double_precision_cast(self, item_type):
        sql = str(cast(literal(None), AthenaArray(item_type)).compile(dialect=AthenaDialect()))
        assert "AS ARRAY(DOUBLE)" in sql

    def test_untyped_array_preserves_json_scalar_types(self):
        dialect = AthenaDialect()
        untyped = AthenaArray(types.NullType())
        sql = str(select(Column("items", untyped)).compile(dialect=dialect))
        assert "json_format" not in sql
        assert untyped.result_processor(dialect, None)('[1,{"x":2},[3]]') == [1, {"x": 2}, [3]]
        with pytest.raises(sa_exc.CompileError, match="explicit element type"):
            select(literal([1], untyped)).compile(dialect=dialect)

    def test_unknown_array_does_not_rewrite_ordering(self):
        table = Table("arrays", MetaData(), Column("items", AthenaArray(types.NullType())))
        sql = str(select(table).order_by(text("lower(name)")).compile(dialect=AthenaDialect()))
        assert "ORDER BY lower(name)" in sql
        assert "anon_1" not in sql

    @pytest.mark.parametrize(
        ("item_type", "ddl", "dml"),
        [
            (AthenaDate(), "DATE", "DATE"),
            (AthenaTimestamp(), "TIMESTAMP", "TIMESTAMP(6)"),
            (AthenaTimestamp(precision=3), "TIMESTAMP", "TIMESTAMP(3)"),
        ],
    )
    def test_array_athena_temporal_element_type_compilation(self, item_type, ddl, dml):
        dialect = AthenaDialect()
        array = AthenaArray(item_type)
        assert dialect.type_compiler_instance.process(array) == f"ARRAY<{ddl}>"
        assert f"AS ARRAY({dml})" in str(select(literal([], array)).compile(dialect=dialect))


class TestArrayValueProcessor:
    @pytest.mark.parametrize(
        ("type_", "value", "expected"),
        [
            (AthenaArray(Integer), [1, None, 3], "ARRAY[1, NULL, 3]"),
            (
                AthenaArray(String),
                ["thr'ee", "réve🐍 illé", "a,b", "null"],
                "ARRAY['thr''ee', 'réve🐍 illé', 'a,b', 'null']",
            ),
            (
                AthenaArray(String, dimensions=2),
                [["one"], [], None],
                "ARRAY[ARRAY['one'], ARRAY[], NULL]",
            ),
            (AthenaArray(types.Date), [date(2025, 1, 2)], "ARRAY[DATE '2025-01-02']"),
            (AthenaArray(AthenaDate), [date(2025, 1, 2)], "ARRAY[DATE '2025-01-02']"),
            (
                AthenaArray(AthenaTimestamp),
                [datetime(2025, 1, 2, 3, 4, 5)],
                "ARRAY[TIMESTAMP '2025-01-02 03:04:05.000']",
            ),
            (
                AthenaArray(types.DateTime),
                [datetime(2025, 1, 2, 3, 4, 5, 123456)],
                "ARRAY[TIMESTAMP '2025-01-02 03:04:05.123456']",
            ),
            (AthenaArray(types.BINARY), [b"\x00\xff"], "ARRAY[X'00ff']"),
            (AthenaArray(Integer), [], "ARRAY[]"),
            (AthenaArray(Integer), None, "NULL"),
        ],
    )
    def test_array_bound_and_literal_values(self, type_, value, expected):
        dialect = AthenaDialect()
        assert type_.literal_processor(dialect)(value) == expected
        bound = type_.bind_processor(dialect)(value)
        actual = DefaultParameterFormatter().format("SELECT %(value)s", {"value": bound})
        assert actual == "SELECT " + expected.replace("NULL", "null")

    @pytest.mark.parametrize(
        ("type_", "expected"),
        [
            (AthenaArray(types.Date), "ARRAY[DATE '2025-01-02'' --']"),
            (AthenaArray(types.DateTime), "ARRAY[TIMESTAMP '2025-01-02'' --']"),
        ],
    )
    def test_array_literal_renders_temporal_string_elements(self, type_, expected):
        assert type_.literal_processor(AthenaDialect())(["2025-01-02' --"]) == expected

    def test_array_binding_preserves_in_parameters(self):
        formatter = DefaultParameterFormatter()
        assert (
            formatter.format("SELECT 1 WHERE 1 IN %(items)s", {"items": [1, 2]})
            == "SELECT 1 WHERE 1 IN (1, 2)"
        )

    def test_array_binary_binding_uses_native_literals(self):
        dialect = AthenaDialect(dbapi=pyathena)
        processor = AthenaArray(types.BINARY).bind_processor(dialect)
        assert (
            DefaultParameterFormatter().format("SELECT %(value)s", {"value": processor([b"\xff"])})
            == "SELECT ARRAY[X'ff']"
        )

    @pytest.mark.parametrize("value", [[[1]], "[1]", {"x": 1}])
    def test_array_binding_rejects_incorrect_shape(self, value):
        with pytest.raises(TypeError, match="ARRAY"):
            AthenaArray(Integer).bind_processor(AthenaDialect())(value)

    def test_array_textual_sql_preserves_native_fallback(self):
        value = "[[one, two], [a,b]]"
        assert (
            AthenaArray(String, dimensions=2).result_processor(AthenaDialect(), None)(value)
            == value
        )

    @pytest.mark.parametrize(
        ("type_", "encoded", "expected"),
        [
            (AthenaArray(Integer), '["1",null,"3"]', [1, None, 3]),
            (AthenaArray(String), '["001","null","a,b",""]', ["001", "null", "a,b", ""]),
            (
                AthenaArray(Integer, dimensions=2, as_tuple=True),
                '[["1"],[],null]',
                ((1,), (), None),
            ),
            (
                AthenaArray(types.Numeric(30, 20)),
                '["0.12345678901234567890"]',
                [Decimal("0.12345678901234567890")],
            ),
            (AthenaArray(types.Date), '["2025-01-02"]', [date(2025, 1, 2)]),
            (AthenaArray(AthenaDate), '["2025-01-02"]', [date(2025, 1, 2)]),
            (
                AthenaArray(AthenaTimestamp),
                '["2025-01-02 03:04:05"]',
                [datetime(2025, 1, 2, 3, 4, 5)],
            ),
            (
                AthenaArray(types.DateTime),
                '["2025-01-02 03:04:05.1","2025-01-02 03:04:05.123456789","2025-01-02T03:04:05"]',
                [
                    datetime(2025, 1, 2, 3, 4, 5, 100000),
                    datetime(2025, 1, 2, 3, 4, 5, 123456),
                    datetime(2025, 1, 2, 3, 4, 5),
                ],
            ),
            (AthenaArray(types.BINARY), '["00FF",""]', [b"\x00\xff", b""]),
            (AthenaArray(types.JSON), '[{"fraction":0.1}]', [{"fraction": 0.1}]),
            (
                AthenaArray(AthenaMap(Integer, String)),
                '[[["1","001"],["2",null]]]',
                [{1: "001", 2: None}],
            ),
            (
                AthenaArray(AthenaStruct(("name", String), ("n", Integer))),
                '[{"name":"001","n":"2"},null]',
                [{"name": "001", "n": 2}, None],
            ),
        ],
    )
    def test_array_result_conversion(self, type_, encoded, expected):
        processor = type_.result_processor(AthenaDialect(), None)
        assert processor(encoded) == expected
        assert processor(json.loads(encoded)) == expected
        assert processor(json.dumps({"_pyathena_array": json.loads(encoded)})) == expected
        assert processor('{"_pyathena_array":null}') is None
        assert processor(None) is None

    def test_array_custom_element_processors(self):
        dialect = AthenaDialect()
        enum = AthenaArray(types.Enum(Color))
        assert enum.result_processor(dialect, None)('["RED"]') == [Color.RED]
        decorated = AthenaArray(OffsetInteger())
        assert decorated.result_processor(dialect, None)('["4"]') == [5]
        sql = str(select(literal([5], decorated)).compile(dialect=dialect))
        assert "ARRAY(INTEGER)" in sql

    @pytest.mark.parametrize(
        ("item_type", "value"),
        [
            (DecoratedTimestamp(), datetime(2025, 1, 2, 3, 4, 5)),
            (JSONEncodedDict(), {"x": 1}),
            (OffsetInteger().with_variant(String(), "awsathena"), "unchanged"),
        ],
    )
    def test_decorator_bind_literal_and_result_paths(self, item_type, value):
        dialect = AthenaDialect()
        array = AthenaArray(item_type)
        bind = array.bind_processor(dialect)([value])
        rendered = DefaultParameterFormatter().format("SELECT %(value)s", {"value": bind})
        assert "ARRAY[" in rendered
        literal_sql = array.literal_processor(dialect)([value])
        assert "ARRAY[" in literal_sql
        select(literal([value], array)).compile(dialect=dialect)
        encoded = (
            value.isoformat(" ")
            if isinstance(value, datetime)
            else (json.dumps(value) if isinstance(value, dict) else value)
        )
        assert array.result_processor(dialect, None)(json.dumps([encoded])) == [value]

    @pytest.mark.parametrize(
        ("item_type", "value", "encoded", "sql", "projection"),
        [
            (
                String().with_variant(Integer(), "awsathena"),
                1,
                "1",
                "ARRAY[1]",
                "CAST(CAST(_pyathena_array_0 AS VARCHAR) AS JSON)",
            ),
            (
                String().with_variant(AthenaMap(String, Integer), "awsathena"),
                {"a": 1},
                {"a": "1"},
                "ARRAY[MAP(ARRAY['a'], ARRAY[1])]",
                "map_entries(_pyathena_array_0)",
            ),
        ],
    )
    def test_element_variant_bind_literal_and_result_paths(
        self, item_type, value, encoded, sql, projection
    ):
        dialect = AthenaDialect()
        array = AthenaArray(item_type)
        bind = array.bind_processor(dialect)([value])
        rendered = DefaultParameterFormatter().format("SELECT %(value)s", {"value": bind})
        assert rendered == f"SELECT {sql}"
        assert array.literal_processor(dialect)([value]) == sql
        assert array.result_processor(dialect, None)(json.dumps([encoded])) == [value]
        assert projection in str(select(column("items", array)).compile(dialect=dialect))

    def test_array_pickle_type_uses_overridden_processors(self):
        dialect = AthenaDialect(dbapi=SimpleNamespace(Binary=bytes, paramstyle="pyformat"))
        array = AthenaArray(types.PickleType())
        bound = array.bind_processor(dialect)([5])
        assert pickle.loads(bound.values[0]) == 5
        assert array.result_processor(dialect, None)(json.dumps([bound.values[0].hex()])) == [5]


class TestArrayJSONProjection:
    def test_array_result_projection_does_not_change_subquery_type(self):
        table = Table("array_values", MetaData(), Column("items", AthenaArray(Integer)))
        subquery = select(table.c["items"]).subquery()
        compiled = str(select(subquery.c["items"]).compile(dialect=AthenaDialect()))
        assert compiled.count("json_format(") == 1
        assert "SELECT array_values.items AS items" in compiled
        assert isinstance(subquery.c["items"].type, types.ARRAY)

    def test_array_distinct_and_union_keep_native_ordering(self):
        table = Table("arrays", MetaData(), Column("items", AthenaArray(Integer)))
        for statement in (
            select(table.c["items"]).distinct().order_by("items"),
            select(table.c["items"]).union_all(select(table.c["items"])).order_by("items"),
        ):
            sql = str(statement.compile(dialect=AthenaDialect()))
            assert sql.count("json_format(") == 1
            assert "ORDER BY anon_1.items" in sql

    def test_array_textual_ordering_and_hive_field_spaces(self):
        table = Table(
            "arrays", MetaData(), Column("id", Integer), Column("items", AthenaArray(Integer))
        )
        sql = str(select(table).order_by(text("id DESC")).compile(dialect=AthenaDialect()))
        assert "ORDER BY anon_1.id DESC" in sql
        reflected = AthenaDialect()._get_column_type("array<struct<first name:string>>")
        assert list(reflected.item_type.fields) == ["first name"]
        assert isinstance(reflected.item_type.fields["first name"], String)

    def test_textual_ordering_list_and_unresolved_expression(self):
        table = Table(
            "arrays", MetaData(), Column("id", Integer), Column("items", AthenaArray(Integer))
        )
        sql = str(select(table).order_by(text("items DESC, id")).compile(dialect=AthenaDialect()))
        assert "ORDER BY anon_1.items DESC, anon_1.id" in sql
        with pytest.raises(sa_exc.CompileError, match="column expressions"):
            select(table).order_by(text("cardinality(items)")).compile(dialect=AthenaDialect())

    def test_array_ordering_ordinals_use_selected_positions(self):
        first = Table(
            "first_table", MetaData(), Column("id", Integer), Column("items", AthenaArray(Integer))
        )
        second = Table("second_table", MetaData(), Column("id", Integer))
        sql = str(
            select(first.c.id, second.c.id, first.c["items"])
            .order_by(text("2"))
            .compile(dialect=AthenaDialect())
        )
        assert "ORDER BY anon_1.id_1" in sql
        sql = str(
            select(first)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .order_by(text("1"))
            .compile(dialect=AthenaDialect())
        )
        assert "ORDER BY anon_1.first_table_id" in sql
        numeric = Table(
            "numeric", MetaData(), Column("id", Integer), Column("1", AthenaArray(Integer))
        )
        sql = str(select(numeric).order_by(text('"1"')).compile(dialect=AthenaDialect()))
        assert 'ORDER BY anon_1."1"' in sql

    @pytest.mark.parametrize(
        "ordering", ["id > 5", "coalesce(name, ')')", "CASE WHEN id > 5 THEN 0 ELSE 1 END"]
    )
    def test_unsupported_array_text_ordering_raises_compile_error(self, ordering):
        table = Table("arrays", MetaData(), Column("items", AthenaArray(Integer)))
        with pytest.raises(sa_exc.CompileError, match="column expressions"):
            select(table).order_by(text(ordering)).compile(dialect=AthenaDialect())

    def test_array_ordering_resolves_unselected_from_columns(self):
        table = Table(
            "arrays", MetaData(), Column("id", Integer), Column("items", AthenaArray(Integer))
        )
        statement = select(table.c["items"]).order_by("id")
        sql = str(statement.compile(dialect=AthenaDialect()))
        assert "arrays.id AS _pyathena_order_0" in sql
        assert "ORDER BY anon_1._pyathena_order_0" in sql

    def test_array_ordering_resolves_qualified_selected_label(self):
        table = Table("arrays", MetaData(), Column("items", AthenaArray(Integer)))
        sql = str(
            select(table.c["items"]).order_by("arrays_items").compile(dialect=AthenaDialect())
        )
        assert "ORDER BY anon_1.items" in sql

    def test_array_ordering_unknown_label_raises_compile_error(self):
        table = Table("arrays", MetaData(), Column("items", AthenaArray(Integer)))
        with pytest.raises(sa_exc.CompileError, match="resolve ARRAY ORDER BY label"):
            select(table).order_by("missing").compile(dialect=AthenaDialect())

    @pytest.mark.parametrize(
        "projection", [text("id"), literal_column("*"), literal_column("arrays.*")]
    )
    @pytest.mark.parametrize("operation", ["order_by", "distinct", "union_all"])
    def test_array_rewrite_rejects_untracked_projection(self, projection, operation):
        table = Table(
            "arrays", MetaData(), Column("id", Integer), Column("items", AthenaArray(Integer))
        )
        statement = select(projection, table.c["items"])
        if operation == "order_by":
            statement = statement.order_by(table.c.id)
        elif operation == "distinct":
            statement = statement.distinct()
        else:
            statement = statement.union_all(statement)
        with pytest.raises(sa_exc.CompileError, match="explicit SELECT columns"):
            statement.compile(dialect=AthenaDialect())

    def test_array_rewrite_keeps_explicit_literal_columns(self):
        table = Table(
            "arrays", MetaData(), Column("id", Integer), Column("items", AthenaArray(Integer))
        )
        sql = str(
            select(literal_column("id"), table.c["items"])
            .order_by(table.c["items"])
            .compile(dialect=AthenaDialect())
        )
        assert sql.startswith("SELECT anon_1.id, json_format(")

    @pytest.mark.parametrize("compound", [False, True])
    def test_decorated_array_keeps_native_ordering_and_result_processor(self, compound):
        dialect = AthenaDialect()
        statement = select(literal([10], TupleArray()).label("items"))
        if compound:
            statement = statement.union_all(select(literal([2], TupleArray()).label("items")))
        compiled = statement.order_by("items").compile(dialect=dialect)
        assert "ORDER BY anon_1.items" in str(compiled)
        result_type = compiled._result_columns[0].type
        assert isinstance(result_type, TupleArray)
        processor = result_type.dialect_impl(dialect).result_processor(dialect, None)
        assert processor('{"_pyathena_array":["2",null]}') == (2, None)

    def test_array_variant_keeps_transport_and_result_types(self):
        dialect = AthenaDialect()
        type_ = String().with_variant(AthenaArray(Integer), "awsathena")
        compiled = (
            select(literal([2], type_).label("items")).order_by("items").compile(dialect=dialect)
        )
        assert "transform(anon_1.items" in str(compiled)
        processor = (
            compiled._result_columns[0].type.dialect_impl(dialect).result_processor(dialect, None)
        )
        assert processor('{"_pyathena_array":["2"]}') == [2]

    @pytest.mark.parametrize("compound", [False, True])
    def test_array_ordering_rejects_columns_outside_distinct_or_union(self, compound):
        table = Table(
            "arrays", MetaData(), Column("id", Integer), Column("items", AthenaArray(Integer))
        )
        statement = select(table.c["items"])
        statement = statement.union_all(statement) if compound else statement.distinct()
        with pytest.raises(sa_exc.CompileError, match="must refer to selected columns"):
            statement.order_by(table.c.id).compile(dialect=AthenaDialect())
        sql = str(statement.order_by(table.c["items"]).compile(dialect=AthenaDialect()))
        assert sql.count("FROM (") == 1
        assert "ORDER BY anon_1.items" in sql

    @pytest.mark.parametrize("expression", ["cardinality(items)", "arrays.id", "1"])
    def test_array_rewrite_requires_labels_for_literal_expressions(self, expression):
        table = Table("arrays", MetaData(), Column("items", AthenaArray(Integer)))
        value = literal_column(expression)
        with pytest.raises(sa_exc.CompileError, match="require an explicit label"):
            select(value, table.c["items"]).distinct().compile(dialect=AthenaDialect())
        sql = str(
            select(value.label("value"), table.c["items"])
            .distinct()
            .compile(dialect=AthenaDialect())
        )
        assert sql.startswith("SELECT anon_1.value, json_format(")


class TestArrayAssignmentType:
    def test_binary_element_assignment_uses_native_hex_parameter(self):
        table = _array_update_table(AthenaArray(types.BINARY))
        compiled = (
            table.update()
            .values({table.c["items"][1]: b"\x00\xff"})
            .compile(dialect=AthenaDialect())
        )
        params = {
            name: compiled._bind_processors.get(name, lambda value: value)(value)
            for name, value in compiled.params.items()
        }
        assert "FROM_HEX('00ff')" in DefaultParameterFormatter().format(str(compiled), params)

    def test_explicit_assignment_type_and_callable_value(self):
        table = _array_update_table(AthenaArray(types.String))
        stmt = table.update().values(
            {table.c["items"][1]: bindparam("value", type_=PrefixString(), callable_=lambda: "a")}
        )
        compiled = stmt.compile(dialect=AthenaDialect())
        assert compiled._bind_processors["value"]("a") == "prefix:a"
        assert "upper(%(value)s)" in str(compiled)
        assert compiled.params["value"] == "a"


class TestArrayWriteIndexType:
    @pytest.mark.parametrize("processor_name", ["bind_processor", "literal_processor"])
    @pytest.mark.parametrize("value", [None, True, 1.5, "1"])
    def test_rejects_non_integer_values(self, processor_name, value):
        processor = getattr(_ArrayWriteIndexType(), processor_name)(AthenaDialect())
        with pytest.raises(ValueError, match="non-NULL integers"):
            processor(value)

    def test_bind_and_literal_processors(self):
        type_ = _ArrayWriteIndexType()
        assert type_.bind_processor(AthenaDialect())(2) == 2
        assert type_.literal_processor(AthenaDialect())(2) == "2"


class TestArrayUpdate:
    def test_multiple_updates_to_one_array_are_rejected(self):
        table = _array_update_table()
        values = table.c["items"]
        for assignments in (
            {values[1]: 2, values[2]: 3},
            {values: [], values[1]: 2},
            {values[1]: 2, "items": []},
        ):
            with pytest.raises(sa_exc.CompileError, match="one assignment"):
                table.update().values(assignments).compile(dialect=AthenaDialect())

    def test_only_final_index_may_be_a_slice(self):
        table = _array_update_table(AthenaArray(Integer, dimensions=2))
        with pytest.raises(sa_exc.CompileError, match="final"):
            table.update().values({table.c["items"][1:2][1]: [2]}).compile(dialect=AthenaDialect())

    def test_bound_indices_and_values_are_reused_without_mutation(self):
        table = _array_update_table()
        expression = table.c["items"][bindparam("index")]
        statement = table.update().values({expression: bindparam("value"), table.c.id: 2})
        compiled = statement.compile(dialect=AthenaDialect())
        assert set(compiled.params) == {"index", "value", "id"}
        assert str(statement.compile(dialect=AthenaDialect())) == str(compiled)

    def test_partial_update_requires_target_table_column(self):
        table = _array_update_table()
        for column_ in (Column("items", AthenaArray(Integer)), _array_update_table().c["items"]):
            with pytest.raises(sa_exc.CompileError, match="target table"):
                table.update().values({column_[1]: 2}).compile(dialect=AthenaDialect())

    def test_ordered_partial_update_with_sql_expression(self):
        table = _array_update_table()
        items = table.c["items"]
        statement = table.update().ordered_values((items[2], items[1] + 1), (table.c.id, 2))
        compiled = str(statement.compile(dialect=AthenaDialect()))
        assert compiled.index("SET items=") < compiled.index(", id=")
        assert "element_at(arrays.items" in compiled

    def test_orm_partial_update_and_renamed_attribute_conflicts(self):
        base = declarative_base()

        class Model(base):
            __tablename__ = "arrays"
            id = Column(Integer, primary_key=True)
            values = Column("stored", AthenaArray(Integer), key="db_key")

        sql = str(update(Model).values({Model.values[1]: 2}).compile(dialect=AthenaDialect()))
        assert "UPDATE arrays SET stored=concat(" in sql
        for whole in (Model.values, "values"):
            with pytest.raises(sa_exc.CompileError, match="one assignment"):
                update(Model).values({Model.values[1]: 2, whole: []}).compile(
                    dialect=AthenaDialect()
                )


class TestArrayUpdateCompiler:
    def test_bound_index_uses_write_index_processor(self):
        table = _array_update_table()
        compiled = (
            table.update()
            .values({table.c["items"][bindparam("index")]: 9})
            .compile(dialect=AthenaDialect())
        )
        assert compiled._bind_processors["index"](2) == 2
        for value in (1.5, True, None):
            with pytest.raises(ValueError, match="non-NULL integers"):
                compiled._bind_processors["index"](value)

    def test_callable_index_and_slice_value(self):
        table = _array_update_table(AthenaArray(types.String))
        compiled = (
            table.update()
            .values({table.c["items"][bindparam("index", callable_=lambda: 1)]: "a"})
            .compile(dialect=AthenaDialect())
        )
        assert compiled.params["index"] == 1
        table.update().values(
            {table.c["items"][1:2]: bindparam("values", callable_=lambda: ["a"])}
        ).compile(dialect=AthenaDialect())

    @pytest.mark.parametrize(
        ("target", "value"),
        [(1, 2), (4, None), (slice(2, 3), [4]), (slice(2, 2), []), (slice(None), [])],
    )
    def test_partial_update_compiles_to_one_whole_column_assignment(self, target, value):
        table = _array_update_table()
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
    def test_invalid_partial_update_index(self, index):
        table = _array_update_table()
        with pytest.raises(sa_exc.CompileError, match="indices"):
            table.update().values({table.c["items"][index]: 1}).compile(dialect=AthenaDialect())

    def test_nested_and_zero_indexed_update(self):
        table = _array_update_table(AthenaArray(Integer, dimensions=2, zero_indexes=True))
        statement = table.update().values({table.c["items"][0][2]: 7})
        sql = str(
            statement.compile(dialect=AthenaDialect(), compile_kwargs={"literal_binds": True})
        )
        assert "ARRAY[concat(" in sql
        assert "sequence(" not in sql
        assert "IF(1 > 0, 1," in sql
        assert "IF(3 > 0, 3," in sql

    @pytest.mark.parametrize(
        "array_type",
        [
            types.ARRAY(Integer),
            TupleArray(),
            AthenaArray(Integer).with_variant(AthenaArray(Integer), "awsathena"),
        ],
    )
    @pytest.mark.parametrize("index", [1, bindparam("index")])
    def test_array_implementations_partial_update(self, array_type, index):
        table = _array_update_table(array_type)
        sql = str(
            table.update().values({table.c["items"][index]: 2}).compile(dialect=AthenaDialect())
        )
        assert "SET items=concat(" in sql

    @pytest.mark.parametrize("target", [1, slice(1, 2)])
    @pytest.mark.parametrize("expression", [False, True])
    def test_decimal_assignment_requires_precision(self, target, expression):
        value = [Decimal("1.23")] if isinstance(target, slice) else Decimal("1.23")
        table = _array_update_table(AthenaArray(types.Numeric()))
        if expression:
            value = table.c["items"][target]
        with pytest.raises(sa_exc.CompileError, match="precision"):
            table.update().values({table.c["items"][target]: value}).compile(
                dialect=AthenaDialect()
            )
        table = _array_update_table(AthenaArray(types.Numeric(8, 2)))
        if expression:
            value = table.c["items"][target]
        sql = str(
            table.update()
            .values({table.c["items"][target]: value})
            .compile(dialect=AthenaDialect())
        )
        assert "DECIMAL(8, 2)" in sql

    def test_write_index_expression_keeps_its_argument_types(self):
        table = _array_update_table()
        index = func.length("abc")
        statement = table.update().values({table.c["items"][index]: 9})
        compiled = statement.compile(dialect=AthenaDialect())
        params = {
            name: compiled._bind_processors.get(name, lambda value: value)(value)
            for name, value in compiled.params.items()
        }
        assert "length('abc')" in DefaultParameterFormatter().format(str(compiled), params)

    def test_null_slice_assignment_rejected(self):
        table = _array_update_table()
        with pytest.raises(sa_exc.CompileError, match="non-NULL array"):
            table.update().values({table.c["items"][1:2]: None}).compile(dialect=AthenaDialect())
