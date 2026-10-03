# Copyright 2025 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import warnings
from datetime import date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import (
    Column,
    Date,
    Float,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    all_,
    any_,
    bindparam,
    cast,
    column,
    exc,
    func,
    select,
    table,
    text,
    types,
    union,
)
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import literal, literal_column, operators
from sqlalchemy.sql.compiler import FROM_LINTING
from sqlalchemy.sql.ddl import CreateTable

from pyathena.formatter import DefaultParameterFormatter
from pyathena.sqlalchemy.base import AthenaDialect
from pyathena.sqlalchemy.compiler import AthenaTypeCompiler
from pyathena.sqlalchemy.pandas import AthenaPandasDialect
from pyathena.sqlalchemy.types import (
    ARRAY,
    MAP,
    STRUCT,
    AthenaArray,
    AthenaMap,
    AthenaStruct,
    AthenaTimestamp,
)
from tests import ENV
from tests.pyathena.util import decorated

# Bind parameter names from SQLAlchemy's DifficultParametersTest.
DIFFICULT_PARAMETER_NAMES = [
    "boring",
    "per cent",
    "per % cent",
    "%percent",
    "par(ens)",
    "percent%(ens)yah",
    "col:ons",
    "_starts_with_underscore",
    "dot.s",
    "more :: %colons%",
    "_name",
    "___name",
    "[BracketsAndCase]",
    "42numbers",
    "percent%signs",
    "has spaces",
    "/slashes/",
    "more/slashes",
    "q?marks",
    "1param",
    "1col:on",
]


class TestAthenaTypeCompiler:
    @pytest.mark.parametrize("precision", [None, 3])
    def test_timestamp_ddl_ignores_precision(self, precision):
        compiler = AthenaTypeCompiler(AthenaDialect())
        assert compiler.process(AthenaTimestamp(precision)) == "TIMESTAMP"

    def test_visit_struct_empty(self):
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = AthenaStruct()
        with pytest.raises(exc.CompileError, match="STRUCT requires at least one field"):
            compiler.visit_struct(struct_type)

    def test_visit_struct_with_fields(self):
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = AthenaStruct(("name", String), ("age", Integer))
        result = compiler.visit_struct(struct_type)
        assert result == "STRUCT<name:STRING, age:INT>"

    def test_visit_struct_uppercase(self):
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = STRUCT(("id", Integer), ("title", String))
        result = compiler.visit_STRUCT(struct_type)
        assert result == "STRUCT<id:INT, title:STRING>"

    def test_visit_struct_no_fields_attribute(self):
        # Test struct type without fields attribute
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = type("MockStruct", (), {})()
        with pytest.raises(exc.CompileError, match="as STRUCT"):
            compiler.visit_struct(struct_type)

    def test_visit_struct_single_field(self):
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = AthenaStruct(("name", String))
        result = compiler.visit_struct(struct_type)
        assert result == "STRUCT<name:STRING>"

    def test_complex_types_render_hive_syntax_without_column(self):
        compiler = AthenaDialect().type_compiler_instance
        struct_type = AthenaStruct(
            ("profile", AthenaStruct(("name", String), ("age", Integer))),
            ("metrics", AthenaMap(String, Integer)),
        )
        map_type = AthenaMap(Integer, AthenaStruct(("n", Integer)))
        expected_struct = "STRUCT<profile:STRUCT<name:STRING, age:INT>, metrics:MAP<STRING, INT>>"
        assert compiler.process(struct_type) == expected_struct
        assert struct_type.compile(dialect=AthenaDialect()) == expected_struct
        assert compiler.process(map_type) == "MAP<INT, STRUCT<n:INT>>"
        assert compiler.process(struct_type, type_expression=Column("c", struct_type)) == (
            expected_struct
        )

    def test_visit_map_default(self):
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        map_type = AthenaMap()
        result = compiler.visit_map(map_type)
        assert result == "MAP<STRING, STRING>"

    def test_visit_map_with_types(self):
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        map_type = AthenaMap(String, Integer)
        result = compiler.visit_map(map_type)
        assert result == "MAP<STRING, INT>"

    def test_visit_map_uppercase(self):
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        map_type = MAP(Integer, String)
        result = compiler.visit_MAP(map_type)
        assert result == "MAP<INT, STRING>"

    def test_visit_map_no_attributes(self):
        # Test map type without key_type/value_type attributes
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        map_type = type("MockMap", (), {})()
        with pytest.raises(exc.CompileError, match="as MAP"):
            compiler.visit_map(map_type)

    def test_visit_array_default(self):
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        array_type = AthenaArray()
        result = compiler.visit_array(array_type)
        assert result == "ARRAY<STRING>"

    def test_visit_array_with_type(self):
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        array_type = AthenaArray(Integer)
        result = compiler.visit_array(array_type)
        assert result == "ARRAY<INT>"

    def test_visit_array_uppercase(self):
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        array_type = ARRAY(String)
        result = compiler.visit_ARRAY(array_type)
        assert result == "ARRAY<STRING>" or result == "ARRAY<VARCHAR>"

    def test_visit_array_no_attributes(self):
        # Test array type without item_type attribute
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        array_type = type("MockArray", (), {})()
        with pytest.raises(exc.CompileError, match="as ARRAY"):
            compiler.visit_array(array_type)

    def test_visit_json(self):
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        json_type = types.JSON()
        with pytest.raises(exc.CompileError, match="not supported in Athena DDL"):
            compiler.visit_JSON(json_type)

    @pytest.mark.parametrize(
        ("type_", "ddl", "cast_type"),
        [
            (Integer(), "INT", "INTEGER"),
            (types.INTEGER(), "INT", "INTEGER"),
            (types.CLOB(), "STRING", "VARCHAR"),
            (types.Text(), "STRING", "VARCHAR"),
            (types.VARCHAR(10), "VARCHAR(10)", "VARCHAR"),
            (types.BINARY(), "BINARY", "VARBINARY"),
        ],
    )
    def test_ddl_and_cast_types(self, type_, ddl, cast_type):
        dialect = AthenaDialect()
        assert dialect.type_compiler_instance.process(type_) == ddl
        assert str(cast(column("x"), type_).compile(dialect=dialect)) == f"CAST(x AS {cast_type})"

    @pytest.mark.parametrize(
        "type_", [types.JSON(), AthenaArray(types.JSON), AthenaMap(String, types.JSON)]
    )
    def test_json_is_rejected_in_ddl_and_kept_in_cast(self, type_):
        dialect = AthenaDialect()
        with pytest.raises(exc.CompileError, match="not supported in Athena DDL"):
            dialect.type_compiler_instance.process(type_)
        assert "JSON" in str(cast(column("x"), type_).compile(dialect=dialect))

    @pytest.mark.parametrize(
        ("type_", "ddl", "cast_type"),
        [
            (types.Float(), "FLOAT", "REAL"),
            (types.FLOAT(), "FLOAT", "REAL"),
            (types.REAL(), "FLOAT", "REAL"),
            (types.Double(), "DOUBLE", "DOUBLE"),
            (types.DOUBLE(), "DOUBLE", "DOUBLE"),
            (types.DOUBLE_PRECISION(), "DOUBLE", "DOUBLE"),
        ],
    )
    def test_floating_point_types(self, type_, ddl, cast_type):
        dialect = AthenaDialect()
        assert dialect.type_compiler_instance.process(type_) == ddl
        assert str(cast(column("x"), type_).compile(dialect=dialect)) == f"CAST(x AS {cast_type})"


class _DecoratedDateTime(types.TypeDecorator):
    impl = types.DateTime
    cache_ok = True


class _DecoratedMillisecondTimestamp(types.TypeDecorator):
    impl = AthenaTimestamp(precision=3)
    cache_ok = True


class _NestedDecoratedDateTime(types.TypeDecorator):
    impl = _DecoratedDateTime
    cache_ok = True


class TestAthenaStatementCompiler:
    """Test cases for Athena statement compiler functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = AthenaDialect()
        self.metadata = MetaData(schema=ENV.schema)
        self.test_table = Table(
            "test_athena_statement_compiler",
            self.metadata,
            Column("id", Integer),
            Column("data", ARRAY(String)),
            Column("numbers", ARRAY(Integer)),
        )

    def test_visit_filter_func_basic(self):
        """Test basic filter() function compilation."""
        # Test basic filter with string lambda expression
        stmt = select(func.filter(self.test_table.c.numbers, literal("x -> x > 0")))
        compiled = stmt.compile(dialect=self.dialect)

        sql_str = str(compiled)
        assert "filter(" in sql_str
        assert "x -> x > 0" in sql_str

    def test_visit_filter_func_array_literal(self):
        """Test filter() function with array literal."""
        # Test filter with array literal - using ARRAY constructor
        stmt = select(
            func.filter(
                func.array(literal(1), literal(2), literal(3), literal(-1)), literal("x -> x > 0")
            )
        )
        compiled = stmt.compile(dialect=self.dialect)

        sql_str = str(compiled)
        assert "filter(" in sql_str
        assert "x -> x > 0" in sql_str

    def test_visit_filter_func_complex_lambda(self):
        """Test filter() function with complex lambda expression."""
        # Test complex lambda expression
        complex_lambda = literal("x -> x IS NOT NULL AND x > 5")
        stmt = select(func.filter(self.test_table.c.numbers, complex_lambda))
        compiled = stmt.compile(dialect=self.dialect)

        sql_str = str(compiled)
        assert "filter(" in sql_str
        assert "x -> x IS NOT NULL AND x > 5" in sql_str

    def test_visit_filter_func_nested_access(self):
        """Test filter() function with nested field access."""
        # Test lambda with nested field access (for complex types)
        nested_lambda = literal("x -> x['timestamp'] > '2023-01-01'")
        stmt = select(func.filter(self.test_table.c.data, nested_lambda))
        compiled = stmt.compile(dialect=self.dialect)

        sql_str = str(compiled)
        assert "filter(" in sql_str
        assert "x -> x['timestamp'] > '2023-01-01'" in sql_str

    def test_visit_filter_func_wrong_argument_count(self):
        """Test filter() function with wrong number of arguments."""
        # Test error when wrong number of arguments provided
        stmt = select(func.filter(self.test_table.c.numbers))
        with pytest.raises(
            exc.CompileError, match="filter\\(\\) function expects exactly 2 arguments"
        ):
            stmt.compile(dialect=self.dialect)

        stmt = select(
            func.filter(self.test_table.c.numbers, literal("x -> x > 0"), literal("extra_arg"))
        )
        with pytest.raises(
            exc.CompileError, match="filter\\(\\) function expects exactly 2 arguments"
        ):
            stmt.compile(dialect=self.dialect)

    def test_visit_filter_func_integration_example(self):
        """Test filter() function with the original issue example."""
        # Test the example from the GitHub issue
        lambda_expr = literal(
            "x -> x['timestamp'] <= '2023-10-10' AND x['timestamp'] >= '2023-10-01' "
            "AND x['action_count'] >= 2"
        )
        stmt = select(func.count(func.filter(self.test_table.c.data, lambda_expr)))
        compiled = stmt.compile(dialect=self.dialect)

        sql_str = str(compiled)
        assert "count(" in sql_str
        assert "filter(" in sql_str
        assert "x -> x['timestamp'] <= '2023-10-10'" in sql_str
        assert "x['action_count'] >= 2" in sql_str

    def test_visit_char_length_func_existing(self):
        """Test existing char_length function still works."""
        # Ensure existing functionality isn't broken
        stmt = select(func.char_length(self.test_table.c.data))
        compiled = stmt.compile(dialect=self.dialect)

        sql_str = str(compiled)
        assert "length(" in sql_str

    @pytest.mark.parametrize(
        ("expression", "expected"),
        [
            (
                literal_column("15", type_=Integer()) / literal_column("10", type_=Integer()),
                "SELECT 15 / CAST(10 AS DOUBLE) AS anon_1",
            ),
            (
                literal(15) / literal(10),
                "SELECT 15 / CAST(10 AS DOUBLE) AS anon_1",
            ),
            (
                literal_column("5.52", type_=Numeric(10, 2))
                / literal_column("2.4", type_=Numeric(10, 2)),
                "SELECT CAST(5.52 AS DECIMAL(10, 2)) / CAST(2.4 AS DECIMAL(10, 2)) AS anon_1",
            ),
            (
                literal_column("5.52", type_=Numeric(10, 2)) / literal_column("2", type_=Integer()),
                "SELECT CAST(5.52 AS DECIMAL(10, 2)) / CAST(2 AS DECIMAL(10, 2)) AS anon_1",
            ),
            (
                literal_column("5.52", type_=Float()) / literal_column("2.4", type_=Float()),
                "SELECT CAST(5.52 AS DOUBLE) / CAST(2.4 AS DOUBLE) AS anon_1",
            ),
        ],
    )
    def test_visit_truediv_binary(self, expression, expected):
        compiled = select(expression).compile(
            dialect=self.dialect, compile_kwargs={"literal_binds": True}
        )

        assert str(compiled) == expected

    def _compile_sql(self, expression):
        return str(expression.compile(dialect=self.dialect, compile_kwargs={"literal_binds": True}))

    @pytest.mark.parametrize(("aggregate", "function"), [(any_, "any_match"), (all_, "all_match")])
    @pytest.mark.parametrize(
        ("op", "sql_operator"),
        [
            (operators.eq, "="),
            (operators.ne, "!="),
            (operators.lt, "<"),
            (operators.le, "<="),
            (operators.gt, ">"),
            (operators.ge, ">="),
        ],
    )
    def test_array_quantified_comparison(self, aggregate, function, op, sql_operator):
        items = column("items", AthenaArray(Integer))
        sql = self._compile_sql(op(2, aggregate(items)))
        assert sql == (
            f"{function}((items), _pyathena_element_0 -> 2 {sql_operator} _pyathena_element_0)"
        )

    def test_array_quantifier_null_negation_and_legacy_methods(self):
        items = column("items", AthenaArray(Integer))
        assert "NULL = _pyathena_element_0" in self._compile_sql(any_(items) == None)  # noqa: E711
        assert self._compile_sql(items.any(2)) == self._compile_sql(any_(items) == 2)
        assert self._compile_sql(items.all(2, operator=operators.lt)) == self._compile_sql(
            all_(items) > 2
        )
        assert self._compile_sql(~items.any(2)).startswith("NOT (any_match(")
        assert "2 > _pyathena_element_0" in self._compile_sql(any_(items) < 2)

    @pytest.mark.parametrize(
        ("scalar", "expected"),
        [
            (column("_pyathena_element_0", Integer), "_pyathena_element_0"),
            (literal_column("_pyathena_element_0 + 1"), "_pyathena_element_0 + 1"),
            (literal_column('"_pyathena_element_0" + 1'), '"_pyathena_element_0" + 1'),
            (text("_pyathena_element_0 + 1"), "_pyathena_element_0 + 1"),
        ],
    )
    def test_array_lambda_does_not_capture_column_names(self, scalar, expected):
        items = column("items", AthenaArray(Integer))
        sql = self._compile_sql(scalar == any_(items))
        assert f"_pyathena_element_1 -> {expected} = _pyathena_element_1" in sql

    def test_array_index_does_not_repeat_expression(self):
        items = column("items", AthenaArray(Integer))
        index = cast(func.floor(func.random() * 3), Integer) - 1
        sql = self._compile_sql(items[index])
        assert sql.count("random()") == 1
        assert "NULLIF(greatest(" in sql

    def test_multidimensional_array_quantifier_bind_type(self):
        items = column("items", AthenaArray(Integer, dimensions=2))
        assert "CAST(ARRAY[1, 2] AS ARRAY(INTEGER)) =" in self._compile_sql(items.any([1, 2]))

    def test_quantifier_preserves_explicit_array_bind(self):
        items = column("items", AthenaArray(Integer))
        needle = literal([1, 2], items.type)
        sql = self._compile_sql(needle == any_(func.array_agg(items)))
        assert "CAST(ARRAY[1, 2] AS ARRAY(INTEGER)) = _pyathena_element_0" in sql
        unknown = column("unknown", AthenaArray(types.NullType()))
        sql = self._compile_sql(needle == any_(unknown))
        assert "CAST(ARRAY[1, 2] AS ARRAY(INTEGER)) = _pyathena_element_0" in sql

    def test_subquery_any_remains_unchanged(self):
        sql = self._compile_sql(any_(select(column("item", Integer)).scalar_subquery()) == 2)
        assert "ANY (SELECT item)" in sql
        assert "any_match" not in sql
        items = column("items", AthenaArray(Integer))
        array_subquery = self._compile_sql(select(items == any_(select(items).scalar_subquery())))
        assert "ANY (SELECT items" in array_subquery
        assert "any_match" not in array_subquery

    def test_array_concat_and_cache_bind_values(self):
        items = column("items", AthenaArray(Integer))
        assert self._compile_sql(items.concat([2])) == "items || CAST(ARRAY[2] AS ARRAY(INTEGER))"
        first = select(items[bindparam("index")]).where(any_(items) == 2)
        second = select(items[bindparam("index")]).where(any_(items) == 3)
        assert first._generate_cache_key().key == second._generate_cache_key().key
        assert self._compile_sql(first.params(index=1)) != self._compile_sql(first.params(index=2))

    def test_quantifier_boolean_left_operand(self):
        flags = column("flags", AthenaArray(types.Boolean))
        assert "any_match" in self._compile_sql(any_(flags) == True)  # noqa: E712
        assert "all_match" in self._compile_sql(all_(flags) != False)  # noqa: E712
        assert "IS DISTINCT FROM" in self._compile_sql(any_(flags).is_distinct_from(True))
        comparison = any_(flags) == True  # noqa: E712
        assert self._compile_sql(~comparison) == (
            "any_match((flags), _pyathena_element_0 -> _pyathena_element_0 != true)"
        )
        assert self._compile_sql(~comparison.self_group()) == (
            "NOT (any_match((flags), _pyathena_element_0 -> _pyathena_element_0 = true))"
        )

    def test_quantifier_join_linter_tracks_original_tables(self):
        left = table("left_table", column("value", Integer))
        right = table("right_table", column("items", AthenaArray(Integer)))
        query = select(left, right).where(left.c.value == any_(right.c["items"]))
        with warnings.catch_warnings():
            warnings.simplefilter("error", exc.SAWarning)
            query.compile(dialect=AthenaDialect(), linting=FROM_LINTING)

    def test_generic_array_slice_step_is_rendered_for_cache_validation(self):
        items = column("items", types.ARRAY(Integer))
        query = select(items[2:3:1])
        compiled = query.compile(dialect=AthenaDialect())
        assert "Unsupported ARRAY slice step" in str(compiled)
        assert list(compiled.params.values()).count(1) == 1
        assert query._generate_cache_key().key == select(items[2:3:2])._generate_cache_key().key
        step_name = next(name for name, value in compiled.params.items() if value == 1)
        assert f"IF(%({step_name})s = 1" in str(compiled)
        for invalid in (2, 1.0, None):
            with pytest.raises(ValueError, match="step"):
                compiled._bind_processors[step_name](invalid)
        native = column("items", AthenaArray(Integer))
        assert (
            select(native[1:3:1])._generate_cache_key().key
            == select(native[1:3])._generate_cache_key().key
        )
        with pytest.raises(exc.CompileError, match="step"):
            native[1:3:2]

    def test_stepped_array_aggregate_with_inferred_element_type(self):
        expression = func.array_agg(func.length("abc"))[1:2:1]
        sql = self._compile_sql(select(expression))
        assert "array_agg(length('abc'))" in sql
        assert "Unsupported ARRAY slice step" in sql
        assert "ARRAY(NULL)" not in sql

    @pytest.mark.parametrize(
        ("type_", "value", "expected"),
        [
            (Date, date(2012, 10, 15), "DATE '2012-10-15'"),
            (types.DATE, date(1727, 4, 1), "DATE '1727-04-01'"),
            (
                types.DateTime,
                datetime(2012, 10, 15, 12, 57, 18, 39642),
                "TIMESTAMP '2012-10-15 12:57:18.039642'",
            ),
            (
                types.DATETIME,
                datetime(2012, 10, 15, 12, 57, 18),
                "TIMESTAMP '2012-10-15 12:57:18.000'",
            ),
            (
                types.TIMESTAMP,
                datetime(2012, 10, 15, 12, 57, 18, 396),
                "TIMESTAMP '2012-10-15 12:57:18.000396'",
            ),
        ],
    )
    @pytest.mark.parametrize(
        ("literal_execute", "compile_kwargs"),
        [(False, {"literal_binds": True}), (True, {"render_postcompile": True})],
    )
    def test_datetime_literal(self, type_, value, expected, literal_execute, compile_kwargs):
        stmt = select(literal(value, type_, literal_execute=literal_execute))
        sql = str(stmt.compile(dialect=self.dialect, compile_kwargs=compile_kwargs))
        assert sql == f"SELECT {expected} AS anon_1"

    @pytest.mark.parametrize(
        ("expression", "expected"),
        [
            (
                cast(column("col", String), types.DateTime),
                "CAST(col AS TIMESTAMP(6))",
            ),
            (
                cast(column("col", String), _DecoratedDateTime()),
                "CAST(col AS TIMESTAMP(6))",
            ),
            (
                cast(column("col", String), _NestedDecoratedDateTime()),
                "CAST(col AS TIMESTAMP(6))",
            ),
            (
                cast(column("col", String), AthenaTimestamp(precision=3)),
                "CAST(col AS TIMESTAMP(3))",
            ),
            (
                cast(column("col", String), _DecoratedMillisecondTimestamp()),
                "CAST(col AS TIMESTAMP(3))",
            ),
            (
                literal(
                    [datetime(2012, 10, 15, 12, 57, 18, 789999)],
                    AthenaArray(AthenaTimestamp(precision=3)),
                ),
                "CAST(ARRAY[TIMESTAMP '2012-10-15 12:57:18.789'] AS ARRAY(TIMESTAMP(3)))",
            ),
            (
                column("items", AthenaArray(types.DateTime)).concat(
                    [datetime(2012, 10, 15, 12, 57, 18, 396)]
                ),
                "items || CAST(ARRAY[TIMESTAMP '2012-10-15 12:57:18.000396'] "
                "AS ARRAY(TIMESTAMP(6)))",
            ),
        ],
    )
    def test_timestamp_cast_keeps_microseconds(self, expression, expected):
        assert expected in self._compile_sql(select(expression))

    @pytest.mark.parametrize(
        ("type_", "expected"),
        [
            (types.String(50), "VARCHAR"),
            (types.Text(), "VARCHAR"),
            (types.CHAR(3), "VARCHAR"),
            (types.LargeBinary(), "VARBINARY"),
            (types.VARBINARY(), "VARBINARY"),
            (types.Float(), "REAL"),
            (types.REAL(), "REAL"),
            (types.Double(), "DOUBLE"),
            (types.Numeric(10, 2), "DECIMAL(10, 2)"),
            (types.ARRAY(String), "ARRAY(VARCHAR)"),
            (AthenaMap(String, Integer), "MAP(VARCHAR, INTEGER)"),
            (AthenaStruct(("name", String)), "ROW(name VARCHAR)"),
            (Integer().with_variant(String(50), "awsathena"), "VARCHAR"),
            (Integer().with_variant(Float(), "awsathena"), "REAL"),
            (String().with_variant(Integer(), "awsathena"), "INTEGER"),
            (String().with_variant(Integer(), "postgresql"), "VARCHAR"),
            (decorated(String()).with_variant(Float(), "awsathena"), "REAL"),
            (
                types.DateTime().with_variant(AthenaTimestamp(precision=3), "awsathena"),
                "TIMESTAMP(3)",
            ),
            (types.ARRAY(String().with_variant(Integer(), "awsathena")), "ARRAY(INTEGER)"),
            (
                types.ARRAY(types.DateTime().with_variant(AthenaTimestamp(3), "awsathena")),
                "ARRAY(TIMESTAMP(3))",
            ),
            (
                AthenaMap(String, decorated(String().with_variant(Integer(), "awsathena"))),
                "MAP(VARCHAR, INTEGER)",
            ),
        ],
    )
    def test_cast_resolves_variants_and_decorators(self, type_, expected):
        for target in (type_, decorated(type_), decorated(decorated(type_))):
            assert self._compile_sql(cast(column("col"), target)) == f"CAST(col AS {expected})"

    @pytest.mark.parametrize(
        ("type_", "expected"),
        [
            (
                AthenaStruct(("name", String), ("age", Integer)),
                "ROW(name VARCHAR, age INTEGER)",
            ),
            (
                AthenaStruct(
                    ("personal", AthenaStruct(("name", String), ("age", Integer))),
                    ("scores", types.ARRAY(Integer)),
                    ("attrs", AthenaMap(String, String)),
                ),
                "ROW(personal ROW(name VARCHAR, age INTEGER), scores ARRAY(INTEGER), "
                "attrs MAP(VARCHAR, VARCHAR))",
            ),
            (
                AthenaMap(String, AthenaStruct(("value", String), ("count", Integer))),
                "MAP(VARCHAR, ROW(value VARCHAR, count INTEGER))",
            ),
            (
                AthenaMap(String, AthenaMap(Integer, AthenaStruct(("n", Integer)))),
                "MAP(VARCHAR, MAP(INTEGER, ROW(n INTEGER)))",
            ),
            (
                types.ARRAY(AthenaMap(String, AthenaStruct(("n", Integer)))),
                "ARRAY(MAP(VARCHAR, ROW(n INTEGER)))",
            ),
            (
                AthenaStruct(("tags", AthenaMap(String, types.ARRAY(Integer)))),
                "ROW(tags MAP(VARCHAR, ARRAY(INTEGER)))",
            ),
        ],
    )
    def test_complex_cast_keeps_dml_syntax(self, type_, expected):
        assert self._compile_sql(cast(column("col"), type_)) == f"CAST(col AS {expected})"

    @pytest.mark.parametrize(
        ("type_", "expected"),
        [
            (types.JSON(), "JSON"),
            (types.ARRAY(types.JSON), "ARRAY(JSON)"),
            (AthenaMap(String, types.JSON), "MAP(VARCHAR, JSON)"),
            (types.CLOB(), "VARCHAR"),
        ],
    )
    def test_cast_renders_types_rejected_or_respelled_in_ddl(self, type_, expected):
        assert self._compile_sql(cast(column("col"), type_)) == f"CAST(col AS {expected})"

    @pytest.mark.parametrize(
        ("base", "expected"),
        [
            (types.String, "VARCHAR"),
            (types.LargeBinary, "VARBINARY"),
            (types.Float, "REAL"),
            (types.Double, "DOUBLE"),
            (types.DateTime, "TIMESTAMP(6)"),
        ],
    )
    @pytest.mark.parametrize("visit_name", ["pyathena_custom", "DATE", "INTEGER", "JSON"])
    def test_cast_renders_subclass_by_base_class(self, base, expected, visit_name):
        # oracle.DATE, for example, subclasses DateTime with the visit name DATE.
        type_ = type("Custom", (base,), {"__visit_name__": visit_name})()
        assert self._compile_sql(cast(column("col"), type_)) == f"CAST(col AS {expected})"
        assert self._compile_sql(cast(column("col"), types.ARRAY(type_))) == (
            f"CAST(col AS ARRAY({expected}))"
        )

    def test_cast_applies_compilation_rule_of_decorator(self):
        class _Wide(types.TypeDecorator):
            impl = types.Integer
            cache_ok = True

        @compiles(_Wide, "awsathena")
        def _compile_wide(type_, compiler, **kw):
            return "BIGINT"

        assert self._compile_sql(cast(column("col"), _Wide())) == "CAST(col AS BIGINT)"
        # Element types render their resolved implementation.
        assert self._compile_sql(cast(column("col"), types.ARRAY(_Wide()))) == (
            "CAST(col AS ARRAY(INTEGER))"
        )

    def test_array_bind_rejects_decorated_numeric_without_precision(self):
        class _Amount(types.TypeDecorator):
            impl = types.Numeric
            cache_ok = True

        @compiles(_Amount, "awsathena")
        def _compile_amount(type_, compiler, **kw):
            return "DECIMAL"

        with pytest.raises(exc.CompileError, match="explicit Numeric precision"):
            self._compile_sql(select(literal([Decimal("1.23")], AthenaArray(_Amount()))))

    def test_array_bind_rejects_numeric_subclass_without_precision(self):
        class _Money(types.Numeric):
            __visit_name__ = "pyathena_money"
            cache_ok = True

        @compiles(_Money, "awsathena")
        def _compile_money(type_, compiler, **kw):
            return "DECIMAL"

        with pytest.raises(exc.CompileError, match="explicit Numeric precision"):
            self._compile_sql(select(literal([Decimal("1.23")], AthenaArray(_Money()))))

    def test_array_assignment_rejects_unknown_value_type(self):
        items = Table(
            "items",
            MetaData(),
            Column(
                "a",
                AthenaArray(types.NullType()).with_variant(AthenaArray(Integer), "awsathena"),
            ),
        )
        with pytest.raises(exc.CompileError, match="explicit element type"):
            items.update().values({items.c.a[1]: 1}).compile(dialect=self.dialect)

    @pytest.mark.parametrize(
        "type_", [AthenaStruct(), types.ARRAY(AthenaStruct()), AthenaMap(String, AthenaStruct())]
    )
    def test_cast_to_empty_struct_raises(self, type_):
        with pytest.raises(exc.CompileError, match="ROW requires at least one field"):
            self._compile_sql(cast(column("col"), type_))

    def test_timestamp_precision_applies_to_compared_values(self):
        col = column("col", AthenaTimestamp(precision=3))
        value = datetime(2012, 10, 15, 12, 57, 18, 789999)
        assert self._compile_sql(select(col).where(col == value)) == (
            "SELECT col \nWHERE col = TIMESTAMP '2012-10-15 12:57:18.789'"
        )
        bound = select(col).where(col == value).compile(dialect=self.dialect).binds["col_1"]
        processor = bound.type.dialect_impl(self.dialect).bind_processor(self.dialect)
        assert processor(bound.value) == datetime(2012, 10, 15, 12, 57, 18, 789000)

    def test_array_slice_fallback_keeps_bare_timestamp(self):
        items = column("items", types.ARRAY(types.DateTime))
        sql = str(select(items[2:3:1]).compile(dialect=self.dialect))
        assert "CAST(ARRAY[] AS ARRAY(TIMESTAMP))" in sql

    def test_timestamp_precision_in_cache_key(self):
        value = datetime(2012, 10, 15, 12, 57, 18, 789999)
        millis = select(literal(value, AthenaTimestamp(precision=3), literal_execute=True))
        micros = select(literal(value, AthenaTimestamp(precision=6), literal_execute=True))
        assert millis._generate_cache_key() != micros._generate_cache_key()

    @pytest.mark.parametrize(
        ("type_", "expected"),
        [(types.Date, "DATE '2012-10-15 10%%'"), (types.DateTime, "TIMESTAMP '2012-10-15 10%%'")],
    )
    def test_temporal_string_literal_doubles_percent(self, type_, expected):
        assert self._compile_sql(select(literal("2012-10-15 10%", type_))) == (
            f"SELECT {expected} AS anon_1"
        )

    def _format_sql(self, statement, parameters=None):
        """Format a statement with the parameter names SQLAlchemy sends to the cursor.

        Bind processors are not applied, so use this only for types without one.

        Args:
            statement: SQLAlchemy statement to compile.
            parameters: Values for the statement's bind parameters.

        Returns:
            The SQL string produced by ``DefaultParameterFormatter``.
        """
        compiled = statement.compile(dialect=self.dialect)
        # Expand before escaping names, as SQLAlchemy's execution context does.
        expanded = compiled.construct_expanded_state(parameters, escape_names=False)
        escaped_names = compiled.escaped_bind_names
        formatted_params = {escaped_names.get(k, k): v for k, v in expanded.parameters.items()}
        return DefaultParameterFormatter().format(expanded.statement, formatted_params)

    @pytest.mark.parametrize("name", DIFFICULT_PARAMETER_NAMES)
    def test_difficult_bind_parameter_name(self, name):
        id_ = column("id", Integer)
        stmt = select(id_).where(id_ == bindparam(name, type_=Integer))
        assert self._format_sql(stmt, {name: 3}).endswith("WHERE id = 3")

    @pytest.mark.parametrize("name", DIFFICULT_PARAMETER_NAMES)
    def test_difficult_expanding_bind_parameter_name(self, name):
        id_ = column("id", Integer)
        stmt = select(id_).where(id_.in_(bindparam(name, value=[1, 2])))
        assert self._format_sql(stmt, {name: [4, 1]}).endswith("WHERE id IN (4, 1)")

    def test_limit_rendered_multiple_times(self):
        limited = (
            select(self.test_table.c.id).order_by(self.test_table.c.id).limit(1).scalar_subquery()
        )
        sql = self._format_sql(union(select(limited), select(limited)).subquery().select())
        assert sql.count("LIMIT 1") == 2
        assert "%(" not in sql

    @pytest.mark.parametrize("pattern", ["%B%", "A%C", "A%C%Z", "%(x)s"])
    def test_like_pattern_is_not_truncated(self, pattern):
        stmt = select(column("x", String)).where(column("x", String).like(pattern))
        assert self._format_sql(stmt).endswith(f"WHERE x LIKE '{pattern}'")


class TestAthenaDDLCompiler:
    """Compile-only (no AWS) tests for the DDL compiler.

    Covers column type rendering in CREATE TABLE, where STRUCT and nested MAP
    types use Hive syntax, and S3 Tables support.

    S3 Tables are queried by setting the connection ``catalog_name`` to
    ``s3tablescatalog/<table-bucket>`` and using the namespace as the table
    schema (a two-part ``namespace.table`` identifier). They use managed
    storage, so CREATE TABLE emits only TBLPROPERTIES (no LOCATION, ROW FORMAT,
    or STORED AS clauses).
    """

    def _s3tables_dialect(self, **connect_opts):
        dialect = AthenaDialect()
        dialect._connect_options = {
            "catalog_name": "s3tablescatalog/bucket",
            "schema_name": "pyathena",
            **connect_opts,
        }
        return dialect

    def _ddl(self, *columns):
        table = Table(
            "events",
            MetaData(schema="analytics"),
            *columns,
            awsathena_location="s3://bucket/events/",
            awsathena_file_format="PARQUET",
        )
        return str(CreateTable(table).compile(dialect=AthenaDialect()))

    def test_create_table_s3tables_catalog_omits_location(self):
        table = Table(
            "tbl",
            MetaData(schema="pyathena"),
            Column("id", Integer),
            Column("name", String),
            awsathena_tblproperties={"table_type": "ICEBERG"},
        )
        ddl = str(CreateTable(table).compile(dialect=self._s3tables_dialect()))
        assert "CREATE TABLE pyathena.tbl (" in ddl
        assert "CREATE EXTERNAL TABLE" not in ddl
        assert "LOCATION" not in ddl
        assert "'table_type' = 'ICEBERG'" in ddl

    def test_create_table_s3tables_partition_transform(self):
        table = Table(
            "tbl",
            MetaData(schema="pyathena"),
            Column("id", Integer),
            Column("dt", Date, awsathena_partition=True, awsathena_partition_transform="day"),
            awsathena_tblproperties={"table_type": "ICEBERG"},
        )
        ddl = str(CreateTable(table).compile(dialect=self._s3tables_dialect()))
        assert "PARTITIONED BY (" in ddl
        assert "day(dt)" in ddl
        assert "LOCATION" not in ddl

    def test_create_iceberg_table_without_s3tables_catalog_requires_location(self):
        # Regression: an Iceberg table on a non-S3-Tables catalog still needs a location.
        table = Table(
            "tbl",
            MetaData(schema="some_db"),
            Column("id", Integer),
            awsathena_tblproperties={"table_type": "ICEBERG"},
        )
        with pytest.raises(exc.CompileError, match="location of the table should be specified"):
            CreateTable(table).compile(dialect=AthenaDialect())

    def test_create_table_s3tables_catalog_case_insensitive(self):
        # Athena resolves catalog names case-insensitively, so detection must too.
        table = Table(
            "tbl",
            MetaData(schema="pyathena"),
            Column("id", Integer),
            awsathena_tblproperties={"table_type": "ICEBERG"},
        )
        dialect = self._s3tables_dialect(catalog_name="S3TablesCatalog/bucket")
        ddl = str(CreateTable(table).compile(dialect=dialect))
        assert "LOCATION" not in ddl

    def test_create_table_s3tables_non_iceberg_raises(self):
        # S3 Tables support only Iceberg tables; a missing table_type must fail at
        # compile time instead of emitting CREATE EXTERNAL TABLE without LOCATION.
        table = Table(
            "tbl",
            MetaData(schema="pyathena"),
            Column("id", Integer),
        )
        with pytest.raises(exc.CompileError, match="S3 Tables support only Iceberg tables"):
            CreateTable(table).compile(dialect=self._s3tables_dialect())

    def test_create_table_s3tables_explicit_location_raises(self):
        # An explicit awsathena_location conflicts with managed storage and must not
        # be silently discarded.
        table = Table(
            "tbl",
            MetaData(schema="pyathena"),
            Column("id", Integer),
            awsathena_location="s3://bucket/path/to/",
            awsathena_tblproperties={"table_type": "ICEBERG"},
        )
        with pytest.raises(exc.CompileError, match="managed storage"):
            CreateTable(table).compile(dialect=self._s3tables_dialect())

    def test_create_table_s3tables_omits_connection_level_formats(self):
        # Connection-level file_format/row_format must not leak STORED AS or
        # ROW FORMAT clauses into managed Iceberg DDL.
        table = Table(
            "tbl",
            MetaData(schema="pyathena"),
            Column("id", Integer),
            awsathena_tblproperties={"table_type": "ICEBERG"},
        )
        dialect = self._s3tables_dialect(file_format="PARQUET", row_format="SERDE 'serde.Class'")
        ddl = str(CreateTable(table).compile(dialect=dialect))
        assert "STORED AS" not in ddl
        assert "ROW FORMAT" not in ddl
        assert "LOCATION" not in ddl
        assert "'table_type' = 'ICEBERG'" in ddl

    def test_create_connect_args_stores_connect_options_for_subclass_dialects(self):
        # Subclass dialects (pandas, arrow, etc.) call _create_connect_args directly
        # from their own create_connect_args; the connection options (including
        # catalog_name for S3 Tables detection) must still land on the dialect.
        dialect = AthenaPandasDialect()
        dialect.create_connect_args(
            make_url(
                "awsathena+pandas://athena.us-west-2.amazonaws.com:443/pyathena"
                "?s3_staging_dir=s3://bucket/path/to/"
                "&catalog_name=s3tablescatalog/bucket"
            )
        )
        assert dialect._connect_options["catalog_name"] == "s3tablescatalog/bucket"
        table = Table(
            "tbl",
            MetaData(schema="pyathena"),
            Column("id", Integer),
            awsathena_tblproperties={"table_type": "ICEBERG"},
        )
        ddl = str(CreateTable(table).compile(dialect=dialect))
        assert "LOCATION" not in ddl

    def test_create_table_renders_hive_struct_syntax(self):
        ddl = self._ddl(
            Column("id", Integer),
            Column("scores", AthenaArray(Integer)),
            Column(
                "profile",
                AthenaStruct(
                    ("name", String),
                    ("age", Integer),
                    ("ratio", Float(24)),
                    ("address", AthenaStruct(("city", String), ("zip", Integer))),
                ),
            ),
            Column(
                "labels",
                AthenaMap(String, AthenaStruct(("value", String), ("count", Integer))),
            ),
            Column(
                "nested_maps",
                AthenaMap(String, AthenaMap(Integer, AthenaStruct(("n", Integer)))),
            ),
            Column(
                "mixed",
                AthenaStruct(
                    ("tags", AthenaArray(String)),
                    ("attrs", AthenaMap(String, Integer)),
                    ("scores", AthenaArray(Integer)),
                ),
            ),
            Column(
                "deep",
                AthenaArray(
                    AthenaMap(String, AthenaStruct(("value", String), ("flag", types.Boolean)))
                ),
            ),
        )
        assert "id INT" in ddl
        assert "scores ARRAY<INT>" in ddl
        assert (
            "profile STRUCT<name:STRING, age:INT, ratio:FLOAT, "
            "address:STRUCT<city:STRING, zip:INT>>"
        ) in ddl
        assert "labels MAP<STRING, STRUCT<value:STRING, count:INT>>" in ddl
        assert "nested_maps MAP<STRING, MAP<INT, STRUCT<n:INT>>>" in ddl
        assert (
            "mixed STRUCT<tags:ARRAY<STRING>, attrs:MAP<STRING, INT>, scores:ARRAY<INT>>"
        ) in ddl
        assert "deep ARRAY<MAP<STRING, STRUCT<value:STRING, flag:BOOLEAN>>>" in ddl
        assert "ROW(" not in ddl
        assert "INTEGER" not in ddl
        assert "REAL" not in ddl
        assert "FLOAT(" not in ddl

    def test_struct_field_quoting_follows_ddl_preparer(self):
        struct_type = AthenaStruct(
            ("date", String),
            ("select", Integer),
            ('na"me', String),
            ("a`b", String),
            ("first name", String),
            ("_hidden", Integer),
        )
        ddl = self._ddl(Column("payload", struct_type))
        assert (
            'payload STRUCT<`date`:STRING, `select`:INT, `na"me`:STRING, '
            "`a``b`:STRING, `first name`:STRING, `_hidden`:INT>"
        ) in ddl
        cast_sql = str(cast(column("payload"), struct_type).compile(dialect=AthenaDialect()))
        assert cast_sql == (
            'CAST(payload AS ROW(date VARCHAR, "select" INTEGER, "na""me" VARCHAR, '
            '"a`b" VARCHAR, "first name" VARCHAR, _hidden INTEGER))'
        )

    @pytest.mark.parametrize(
        "type_", [AthenaStruct(), AthenaArray(AthenaStruct()), AthenaMap(String, AthenaStruct())]
    )
    def test_empty_struct_column_raises(self, type_):
        with pytest.raises(
            exc.CompileError,
            match=r"column 'empty'.*STRUCT requires at least one field",
        ):
            self._ddl(Column("empty", type_))

    def test_json_column_raises(self):
        with pytest.raises(
            exc.CompileError, match=r"column 'payload'.*not supported in Athena DDL"
        ):
            self._ddl(Column("payload", types.JSON))

    def test_integer_subclass_and_decorator_columns_use_int(self):
        ddl = self._ddl(
            Column("decorated", decorated(Integer())),
            Column("subclassed", type("MyInteger", (Integer,), {})()),
            Column("variant", Integer().with_variant(types.BigInteger(), "awsathena")),
            Column("text_value", types.CLOB),
        )
        assert "decorated INT,\n" in ddl
        assert "subclassed INT,\n" in ddl
        assert "variant BIGINT,\n" in ddl
        assert "text_value STRING\n" in ddl
        assert "INTEGER" not in ddl

    def test_unsupported_type_inside_struct_column_still_raises(self):
        with pytest.raises(exc.CompileError, match="not supported"):
            self._ddl(Column("payload", AthenaStruct(("when", types.Time))))

    def test_scalar_and_float_column_ddl_is_unchanged(self):
        ddl = self._ddl(
            Column("id", Integer),
            Column("label", String),
            Column("flag", types.Boolean),
            Column("ratio", Float),
            Column("ratio_prec", Float(24)),
            Column("real_value", types.REAL),
            Column("float_value", types.FLOAT),
            Column("wide", types.Double),
            Column("amount", Numeric(10, 2)),
        )
        assert "id INT" in ddl
        assert "label STRING" in ddl
        assert "flag BOOLEAN" in ddl
        assert "ratio FLOAT" in ddl
        assert "ratio_prec FLOAT" in ddl
        assert "real_value FLOAT" in ddl
        assert "float_value FLOAT" in ddl
        assert "wide DOUBLE" in ddl
        assert "amount DECIMAL(10, 2)" in ddl
        assert "INTEGER" not in ddl
        assert "REAL" not in ddl
        assert "FLOAT(" not in ddl
