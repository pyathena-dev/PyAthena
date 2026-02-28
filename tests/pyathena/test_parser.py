import pytest

from pyathena.converter import _DEFAULT_CONVERTERS, _to_default, _to_struct
from pyathena.parser import TypedValueConverter, TypeNode, TypeSignatureParser


class TestTypeSignatureParser:
    def test_simple_type(self):
        parser = TypeSignatureParser()
        node = parser.parse("varchar")
        assert node.type_name == "varchar"
        assert node.children == []
        assert node.field_names is None

    def test_simple_type_case_insensitive(self):
        parser = TypeSignatureParser()
        node = parser.parse("VARCHAR")
        assert node.type_name == "varchar"

    def test_array_type(self):
        parser = TypeSignatureParser()
        node = parser.parse("array(varchar)")
        assert node.type_name == "array"
        assert len(node.children) == 1
        assert node.children[0].type_name == "varchar"

    def test_array_of_integer(self):
        parser = TypeSignatureParser()
        node = parser.parse("array(integer)")
        assert node.type_name == "array"
        assert node.children[0].type_name == "integer"

    def test_map_type(self):
        parser = TypeSignatureParser()
        node = parser.parse("map(varchar, integer)")
        assert node.type_name == "map"
        assert len(node.children) == 2
        assert node.children[0].type_name == "varchar"
        assert node.children[1].type_name == "integer"

    def test_row_type(self):
        parser = TypeSignatureParser()
        node = parser.parse("row(name varchar, age integer)")
        assert node.type_name == "row"
        assert len(node.children) == 2
        assert node.field_names == ["name", "age"]
        assert node.children[0].type_name == "varchar"
        assert node.children[1].type_name == "integer"

    def test_struct_type(self):
        parser = TypeSignatureParser()
        node = parser.parse("struct(name varchar, age integer)")
        assert node.type_name == "struct"
        assert node.field_names == ["name", "age"]

    def test_nested_array_of_row(self):
        parser = TypeSignatureParser()
        node = parser.parse("array(row(name varchar, age integer))")
        assert node.type_name == "array"
        assert len(node.children) == 1
        row_node = node.children[0]
        assert row_node.type_name == "row"
        assert row_node.field_names == ["name", "age"]
        assert row_node.children[0].type_name == "varchar"
        assert row_node.children[1].type_name == "integer"

    def test_map_with_complex_value(self):
        parser = TypeSignatureParser()
        node = parser.parse("map(varchar, row(x integer, y double))")
        assert node.type_name == "map"
        assert node.children[0].type_name == "varchar"
        assert node.children[1].type_name == "row"
        assert node.children[1].field_names == ["x", "y"]

    def test_deeply_nested(self):
        parser = TypeSignatureParser()
        node = parser.parse("array(row(data row(x integer, y integer), name varchar))")
        assert node.type_name == "array"
        row_node = node.children[0]
        assert row_node.type_name == "row"
        assert row_node.field_names == ["data", "name"]
        assert row_node.children[0].type_name == "row"
        assert row_node.children[0].field_names == ["x", "y"]
        assert row_node.children[1].type_name == "varchar"

    def test_parameterized_type(self):
        parser = TypeSignatureParser()
        node = parser.parse("decimal(10, 2)")
        assert node.type_name == "decimal"

    def test_varchar_with_length(self):
        parser = TypeSignatureParser()
        node = parser.parse("varchar(255)")
        assert node.type_name == "varchar"


class TestTypedValueConverter:
    @pytest.fixture
    def converter(self):
        return TypedValueConverter(
            converters=_DEFAULT_CONVERTERS,
            default_converter=_to_default,
            struct_parser=_to_struct,
        )

    def test_simple_varchar(self, converter):
        node = TypeNode("varchar")
        assert converter.convert("hello", node) == "hello"

    def test_simple_integer(self, converter):
        node = TypeNode("integer")
        assert converter.convert("42", node) == 42

    def test_array_of_varchar(self, converter):
        parser = TypeSignatureParser()
        node = parser.parse("array(varchar)")
        assert converter.convert("[1234, 5678]", node) == ["1234", "5678"]

    def test_array_of_integer(self, converter):
        parser = TypeSignatureParser()
        node = parser.parse("array(integer)")
        assert converter.convert("[1, 2, 3]", node) == [1, 2, 3]

    def test_map_varchar_integer(self, converter):
        parser = TypeSignatureParser()
        node = parser.parse("map(varchar, integer)")
        assert converter.convert('{"a": 1, "b": 2}', node) == {"a": 1, "b": 2}

    def test_row_named_fields(self, converter):
        parser = TypeSignatureParser()
        node = parser.parse("row(name varchar, age integer)")
        assert converter.convert("{name=Alice, age=25}", node) == {"name": "Alice", "age": 25}

    def test_nested_row(self, converter):
        parser = TypeSignatureParser()
        node = parser.parse("row(header row(seq integer, stamp varchar), x double)")
        result = converter.convert("{header={seq=123, stamp=2024}, x=4.5}", node)
        assert result == {"header": {"seq": 123, "stamp": "2024"}, "x": 4.5}

    def test_array_of_row_json(self, converter):
        """JSON path: array(row(...)) with nested dict elements."""
        parser = TypeSignatureParser()
        node = parser.parse("array(row(x integer, y double))")
        result = converter.convert('[{"x": 1, "y": 2.5}, {"x": 3, "y": 4.0}]', node)
        assert result == [{"x": 1, "y": 2.5}, {"x": 3, "y": 4.0}]
        assert isinstance(result[0]["x"], int)
        assert isinstance(result[0]["y"], float)

    def test_null_string_preserved_in_json(self, converter):
        """JSON path: string "null" in array(varchar) must not become None."""
        parser = TypeSignatureParser()
        node = parser.parse("array(varchar)")
        result = converter.convert('["null", "x"]', node)
        assert result == ["null", "x"]

    def test_map_with_row_value_native(self, converter):
        """Native path: map(varchar, row(...)) with nested struct values."""
        parser = TypeSignatureParser()
        node = parser.parse("map(varchar, row(x integer, y integer))")
        result = converter.convert("{a={x=1, y=2}, b={x=3, y=4}}", node)
        assert result == {"a": {"x": 1, "y": 2}, "b": {"x": 3, "y": 4}}
        assert isinstance(result["a"]["x"], int)

    def test_nested_row_json(self, converter):
        """JSON path: row containing row with nested dict values."""
        parser = TypeSignatureParser()
        node = parser.parse("row(inner row(a integer, b varchar), val double)")
        result = converter.convert('{"inner": {"a": 10, "b": "hello"}, "val": 3.14}', node)
        assert result == {"inner": {"a": 10, "b": "hello"}, "val": 3.14}
        assert isinstance(result["inner"]["a"], int)
        assert isinstance(result["val"], float)

    def test_struct_json_name_based_type_matching(self, converter):
        """JSON path: field types are matched by name, not position order."""
        parser = TypeSignatureParser()
        node = parser.parse("row(name varchar, age integer)")
        # JSON keys in reverse order compared to type definition
        result = converter.convert('{"age": 25, "name": "Alice"}', node)
        assert result == {"age": 25, "name": "Alice"}
        assert isinstance(result["age"], int)
        assert isinstance(result["name"], str)

    def test_map_json_null_value_preserved(self, converter):
        """JSON path: map with null values vs "null" string values."""
        parser = TypeSignatureParser()
        node = parser.parse("map(varchar, varchar)")
        result = converter.convert('{"a": null, "b": "null"}', node)
        assert result["a"] is None
        assert result["b"] == "null"
