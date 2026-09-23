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
    literal,
    literal_column,
    select,
    text,
    types,
)
from sqlalchemy import exc as sa_exc
from sqlalchemy.sql import sqltypes

import pyathena
from pyathena.formatter import DefaultParameterFormatter
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
        ("item_type", "expected"), [(AthenaDate(), "DATE"), (AthenaTimestamp(), "TIMESTAMP")]
    )
    def test_array_athena_temporal_element_type_compilation(self, item_type, expected):
        dialect = AthenaDialect()
        array = AthenaArray(item_type)
        assert dialect.type_compiler_instance.process(array) == f"ARRAY<{expected}>"
        assert f"AS ARRAY({expected})" in str(select(literal([], array)).compile(dialect=dialect))


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
