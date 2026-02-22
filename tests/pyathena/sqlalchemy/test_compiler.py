from unittest.mock import Mock

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, exc, func, select
from sqlalchemy.sql import literal

from pyathena.sqlalchemy.base import AthenaDialect
from pyathena.sqlalchemy.compiler import AthenaTypeCompiler
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
