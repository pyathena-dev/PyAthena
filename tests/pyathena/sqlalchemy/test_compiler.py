# Copyright 2025 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import warnings
from datetime import date, datetime

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
)
from sqlalchemy.engine.url import make_url
from sqlalchemy.sql import literal, literal_column, operators
from sqlalchemy.sql.compiler import FROM_LINTING
from sqlalchemy.sql.ddl import CreateTable

from pyathena.sqlalchemy.base import AthenaDialect
from pyathena.sqlalchemy.compiler import AthenaTypeCompiler
from pyathena.sqlalchemy.pandas import AthenaPandasDialect
from pyathena.sqlalchemy.types import ARRAY, MAP, STRUCT, AthenaArray, AthenaMap, AthenaStruct
from tests import ENV


class TestAthenaTypeCompiler:
    def test_visit_struct_empty(self):
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = AthenaStruct()
        result = compiler.visit_struct(struct_type)
        assert result == "ROW()"

    def test_visit_struct_with_fields(self):
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = AthenaStruct(("name", String), ("age", Integer))
        result = compiler.visit_struct(struct_type)
        # The exact order might vary, so we check that both fields are present
        assert "ROW(" in result
        assert "name STRING" in result or "name VARCHAR" in result
        assert "age INTEGER" in result
        assert result.endswith(")")

    def test_visit_struct_uppercase(self):
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = STRUCT(("id", Integer), ("title", String))
        result = compiler.visit_STRUCT(struct_type)
        assert "ROW(" in result
        assert "id INTEGER" in result
        assert "title STRING" in result or "title VARCHAR" in result
        assert result.endswith(")")

    def test_visit_struct_no_fields_attribute(self):
        # Test struct type without fields attribute
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = type("MockStruct", (), {})()
        result = compiler.visit_struct(struct_type)
        assert result == "ROW()"

    def test_visit_struct_single_field(self):
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = AthenaStruct(("name", String))
        result = compiler.visit_struct(struct_type)
        assert result == "ROW(name STRING)" or result == "ROW(name VARCHAR)"

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
        assert result == "MAP<STRING, INTEGER>" or result == "MAP<VARCHAR, INTEGER>"

    def test_visit_map_uppercase(self):
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        map_type = MAP(Integer, String)
        result = compiler.visit_MAP(map_type)
        assert result == "MAP<INTEGER, STRING>" or result == "MAP<INTEGER, VARCHAR>"

    def test_visit_map_no_attributes(self):
        # Test map type without key_type/value_type attributes
        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        map_type = type("MockMap", (), {})()
        result = compiler.visit_map(map_type)
        assert result == "MAP<STRING, STRING>"

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
        result = compiler.visit_array(array_type)
        assert result == "ARRAY<STRING>"

    def test_visit_json(self):
        """Test JSON type compilation."""
        from sqlalchemy import types

        dialect = AthenaDialect()
        compiler = AthenaTypeCompiler(dialect)
        json_type = types.JSON()
        result = compiler.visit_JSON(json_type)
        assert result == "JSON"


class _DecoratedDateTime(types.TypeDecorator):
    impl = types.DateTime
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
        [(types.Date, "DATE '2012-10-15 10%%'"), (types.DateTime, "TIMESTAMP '2012-10-15 10%%'")],
    )
    def test_temporal_string_literal_doubles_percent(self, type_, expected):
        assert self._compile_sql(select(literal("2012-10-15 10%", type_))) == (
            f"SELECT {expected} AS anon_1"
        )


class TestAthenaDDLCompiler:
    """Compile-only (no AWS) tests for the DDL compiler's S3 Tables support.

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
