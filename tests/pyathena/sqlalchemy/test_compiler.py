from unittest.mock import Mock

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
    cast,
    column,
    exc,
    func,
    select,
    types,
)
from sqlalchemy.engine.url import make_url
from sqlalchemy.sql import literal, literal_column
from sqlalchemy.sql.ddl import CreateTable

from pyathena.sqlalchemy.base import AthenaDialect
from pyathena.sqlalchemy.compiler import AthenaTypeCompiler
from pyathena.sqlalchemy.pandas import AthenaPandasDialect
from pyathena.sqlalchemy.types import ARRAY, MAP, STRUCT, AthenaArray, AthenaMap, AthenaStruct
from tests import ENV


class TestAthenaTypeCompiler:
    def test_visit_struct_empty(self):
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = AthenaStruct()
        result = compiler.visit_struct(struct_type)
        assert result == "ROW()"

    def test_visit_struct_with_fields(self):
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = AthenaStruct(("name", String), ("age", Integer))
        result = compiler.visit_struct(struct_type)
        # The exact order might vary, so we check that both fields are present
        assert "ROW(" in result
        assert "name STRING" in result or "name VARCHAR" in result
        assert "age INTEGER" in result
        assert result.endswith(")")

    def test_visit_struct_uppercase(self):
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = STRUCT(("id", Integer), ("title", String))
        result = compiler.visit_STRUCT(struct_type)
        assert "ROW(" in result
        assert "id INTEGER" in result
        assert "title STRING" in result or "title VARCHAR" in result
        assert result.endswith(")")

    def test_visit_struct_no_fields_attribute(self):
        # Test struct type without fields attribute
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = type("MockStruct", (), {})()
        result = compiler.visit_struct(struct_type)
        assert result == "ROW()"

    def test_visit_struct_single_field(self):
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = AthenaStruct(("name", String))
        result = compiler.visit_struct(struct_type)
        assert result == "ROW(name STRING)" or result == "ROW(name VARCHAR)"

    def test_visit_map_default(self):
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        map_type = AthenaMap()
        result = compiler.visit_map(map_type)
        assert result == "MAP<STRING, STRING>"

    def test_visit_map_with_types(self):
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        map_type = AthenaMap(String, Integer)
        result = compiler.visit_map(map_type)
        assert result == "MAP<STRING, INTEGER>" or result == "MAP<VARCHAR, INTEGER>"

    def test_visit_map_uppercase(self):
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        map_type = MAP(Integer, String)
        result = compiler.visit_MAP(map_type)
        assert result == "MAP<INTEGER, STRING>" or result == "MAP<INTEGER, VARCHAR>"

    def test_visit_map_no_attributes(self):
        # Test map type without key_type/value_type attributes
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        map_type = type("MockMap", (), {})()
        result = compiler.visit_map(map_type)
        assert result == "MAP<STRING, STRING>"

    def test_visit_array_default(self):
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        array_type = AthenaArray()
        result = compiler.visit_array(array_type)
        assert result == "ARRAY<STRING>"

    def test_visit_array_with_type(self):
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        array_type = AthenaArray(Integer)
        result = compiler.visit_array(array_type)
        assert result == "ARRAY<INTEGER>"

    def test_visit_array_uppercase(self):
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        array_type = ARRAY(String)
        result = compiler.visit_ARRAY(array_type)
        assert result == "ARRAY<STRING>" or result == "ARRAY<VARCHAR>"

    def test_visit_array_no_attributes(self):
        # Test array type without item_type attribute
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        array_type = type("MockArray", (), {})()
        result = compiler.visit_array(array_type)
        assert result == "ARRAY<STRING>"

    def test_visit_json(self):
        """Test JSON type compilation."""
        from sqlalchemy import types

        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        json_type = types.JSON()
        result = compiler.visit_JSON(json_type)
        assert result == "JSON"

    @pytest.mark.skipif(not hasattr(types, "Double"), reason="Requires SQLAlchemy 2.0")
    @pytest.mark.parametrize(
        ("type_name", "ddl", "cast_type"),
        [
            ("Float", "FLOAT", "REAL"),
            ("FLOAT", "FLOAT", "REAL"),
            ("REAL", "FLOAT", "REAL"),
            ("Double", "DOUBLE", "DOUBLE"),
            ("DOUBLE", "DOUBLE", "DOUBLE"),
            ("DOUBLE_PRECISION", "DOUBLE", "DOUBLE"),
        ],
    )
    def test_floating_point_types(self, type_name, ddl, cast_type):
        dialect = AthenaDialect()
        type_ = getattr(types, type_name)()
        assert dialect.type_compiler_instance.process(type_) == ddl
        assert str(cast(column("x"), type_).compile(dialect=dialect)) == f"CAST(x AS {cast_type})"


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


class TestAthenaDDLCompiler:
    """Compile-only (no AWS) tests for the DDL compiler.

    Covers column type rendering in CREATE TABLE, where STRUCT at any depth
    uses Hive syntax, and S3 Tables support.

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

    def test_create_table_renders_hive_struct_syntax(self):
        ddl = self._ddl(
            Column("id", Integer),
            Column(
                "profile",
                AthenaStruct(
                    ("name", String),
                    ("age", Integer),
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
                ),
            ),
            Column("items", AthenaArray(AthenaStruct(("name", String), ("qty", Integer)))),
            Column(
                "deep",
                AthenaArray(AthenaMap(String, AthenaStruct(("flag", types.Boolean)))),
            ),
        )
        assert "id INT" in ddl
        assert (
            "profile STRUCT<name:STRING, age:INTEGER, address:STRUCT<city:STRING, zip:INTEGER>>"
        ) in ddl
        assert "labels MAP<STRING, STRUCT<value:STRING, count:INTEGER>>" in ddl
        assert "nested_maps MAP<STRING, MAP<INTEGER, STRUCT<n:INTEGER>>>" in ddl
        assert "mixed STRUCT<tags:ARRAY<STRING>, attrs:MAP<STRING, INTEGER>>" in ddl
        assert "items ARRAY<STRUCT<name:STRING, qty:INTEGER>>" in ddl
        assert "deep ARRAY<MAP<STRING, STRUCT<flag:BOOLEAN>>>" in ddl
        assert "ROW(" not in ddl

    def test_struct_field_quoting_follows_ddl_preparer(self):
        ddl = self._ddl(
            Column(
                "payload",
                AthenaStruct(("date", String), ("a`b", String), ("first name", String)),
            )
        )
        assert "payload STRUCT<`date`:STRING, `a``b`:STRING, `first name`:STRING>" in ddl

    def test_struct_type_without_column_context_stays_row(self):
        struct_type = AthenaStruct(("name", String), ("tags", AthenaArray(String)))
        map_type = AthenaMap(String, AthenaStruct(("n", Integer)))
        array_type = AthenaArray(AthenaStruct(("n", Integer)))
        compiler = AthenaDialect().type_compiler_instance
        assert compiler.process(struct_type) == "ROW(name STRING, tags ARRAY<STRING>)"
        assert compiler.process(map_type) == "MAP<STRING, ROW(n INTEGER)>"
        assert compiler.process(array_type) == "ARRAY<ROW(n INTEGER)>"

    def test_empty_struct_column_stays_row(self):
        ddl = self._ddl(Column("empty", AthenaStruct()))
        assert "empty ROW()" in ddl

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
