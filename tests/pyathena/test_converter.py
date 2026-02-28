import pytest

from pyathena.converter import (
    DefaultTypeConverter,
    _to_array,
    _to_struct,
    parse_type_signature,
)

# ============================================================================
# Tests for _to_struct (JSON format - unchanged by breaking change)
# ============================================================================


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        (None, None),
        (
            '{"name": "John", "age": 30, "active": true}',
            {"name": "John", "age": 30, "active": True},
        ),
        (
            '{"user": {"name": "John", "age": 30}, "settings": {"theme": "dark"}}',
            {"user": {"name": "John", "age": 30}, "settings": {"theme": "dark"}},
        ),
        ("not valid json", None),
        ("", None),
    ],
)
def test_to_struct_json_formats(input_value, expected):
    """Test STRUCT conversion for various JSON formats and edge cases."""
    result = _to_struct(input_value)
    assert result == expected


# ============================================================================
# Tests for _to_struct (Athena native format - affected by _convert_value change)
# ============================================================================


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        # Values that were previously inferred as int/bool now stay as strings
        ("{a=1, b=2}", {"a": "1", "b": "2"}),
        ("{}", {}),
        ("{name=John, city=Tokyo}", {"name": "John", "city": "Tokyo"}),
        ("{Alice, 25}", {"0": "Alice", "1": "25"}),
        ("{John, 30, true}", {"0": "John", "1": "30", "2": "true"}),
        ("{name=John, age=30}", {"name": "John", "age": "30"}),
        ("{x=1, y=2, z=3}", {"x": "1", "y": "2", "z": "3"}),
        ("{active=true, count=42}", {"active": "true", "count": "42"}),
    ],
)
def test_to_struct_athena_native_formats(input_value, expected):
    """Test STRUCT conversion for Athena native formats.

    After the breaking change, _convert_value returns strings by default
    (no heuristic type inference for numbers/bools).
    """
    result = _to_struct(input_value)
    assert result == expected


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        # Single level nesting (Issue #627) - leaf values are strings
        (
            "{header={stamp=2024-01-01, seq=123}, x=4.736, y=0.583}",
            {"header": {"stamp": "2024-01-01", "seq": "123"}, "x": "4.736", "y": "0.583"},
        ),
        # Double nesting
        (
            "{outer={middle={inner=value}}, field=123}",
            {"outer": {"middle": {"inner": "value"}}, "field": "123"},
        ),
        # Multiple nested fields
        (
            "{pos={x=1, y=2}, vel={x=0.5, y=0.3}, timestamp=12345}",
            {
                "pos": {"x": "1", "y": "2"},
                "vel": {"x": "0.5", "y": "0.3"},
                "timestamp": "12345",
            },
        ),
        # Triple nesting
        (
            "{level1={level2={level3={value=deep}}}}",
            {"level1": {"level2": {"level3": {"value": "deep"}}}},
        ),
        # Mixed types in nested struct - values are now strings
        (
            "{metadata={id=123, active=true, name=test}, count=5}",
            {"metadata": {"id": "123", "active": "true", "name": "test"}, "count": "5"},
        ),
        # Nested struct with null value - null still becomes None
        (
            "{data={value=null, status=ok}, flag=true}",
            {"data": {"value": None, "status": "ok"}, "flag": "true"},
        ),
        # Complex nesting with multiple levels and fields
        (
            "{a={b={c=1, d=2}, e=3}, f=4, g={h=5}}",
            {"a": {"b": {"c": "1", "d": "2"}, "e": "3"}, "f": "4", "g": {"h": "5"}},
        ),
    ],
)
def test_to_struct_athena_nested_formats(input_value, expected):
    """Test STRUCT conversion for nested struct formats (Issue #627)."""
    result = _to_struct(input_value)
    assert result == expected


@pytest.mark.parametrize(
    "input_value",
    [
        "{formula=x=y+1, status=active}",  # Equals in value
        '{json={"key": "value"}, name=test}',  # Braces in value
        '{message=He said "hello", name=John}',  # Quotes in value
    ],
)
def test_to_struct_athena_complex_cases(input_value):
    """Test complex cases with special characters return None or partial dict (safe fallback)."""
    result = _to_struct(input_value)
    # With the new continue logic, these may return partial results instead of None
    # Check if they return None (strict safety) or partial results (lenient approach)
    assert result is None or isinstance(result, dict), (
        f"Complex case should return None or dict: {input_value} -> {result}"
    )


# ============================================================================
# Tests for _to_map (affected by _convert_value change)
# ============================================================================


def test_to_map_athena_numeric_keys():
    """Test Athena map with numeric keys - values are now strings."""
    from pyathena.converter import _to_map

    map_value = "{1=2, 3=4}"
    result = _to_map(map_value)
    expected = {"1": "2", "3": "4"}
    assert result == expected


# ============================================================================
# Tests for _to_array (JSON format - unchanged)
# ============================================================================


def test_to_array_athena_numeric_elements():
    """Test Athena array with numeric elements (JSON-parseable, unchanged)."""
    array_value = "[1, 2, 3, 4]"
    result = _to_array(array_value)
    expected = [1, 2, 3, 4]
    assert result == expected


def test_to_array_athena_mixed_elements():
    """Test Athena array with mixed type elements (native format, affected by change)."""
    array_value = "[1, hello, true, null]"
    result = _to_array(array_value)
    # JSON parsing fails (unquoted hello), so native format is used
    # _convert_value now returns strings
    expected = ["1", "hello", "true", None]
    assert result == expected


def test_to_array_athena_struct_elements():
    """Test Athena array with struct elements (native format, affected by change)."""
    array_value = "[{name=John, age=30}, {name=Jane, age=25}]"
    result = _to_array(array_value)
    expected = [{"name": "John", "age": "30"}, {"name": "Jane", "age": "25"}]
    assert result == expected


def test_to_array_athena_unnamed_struct_elements():
    """Test Athena array with unnamed struct elements (native format, affected by change)."""
    array_value = "[{Alice, 25}, {Bob, 30}]"
    result = _to_array(array_value)
    expected = [{"0": "Alice", "1": "25"}, {"0": "Bob", "1": "30"}]
    assert result == expected


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        # Array with nested structs (Issue #627) - leaf values are strings
        (
            "[{header={stamp=2024-01-01, seq=123}, x=4.736}]",
            [{"header": {"stamp": "2024-01-01", "seq": "123"}, "x": "4.736"}],
        ),
        # Multiple elements with nested structs
        (
            "[{pos={x=1, y=2}, vel={x=0.5}}, {pos={x=3, y=4}, vel={x=1.5}}]",
            [
                {"pos": {"x": "1", "y": "2"}, "vel": {"x": "0.5"}},
                {"pos": {"x": "3", "y": "4"}, "vel": {"x": "1.5"}},
            ],
        ),
        # Array with deeply nested structs
        (
            "[{data={meta={id=1, active=true}}}]",
            [{"data": {"meta": {"id": "1", "active": "true"}}}],
        ),
    ],
)
def test_to_array_athena_nested_struct_elements(input_value, expected):
    """Test Athena array with nested struct elements (Issue #627)."""
    result = _to_array(input_value)
    assert result == expected


@pytest.mark.parametrize(
    "input_value",
    [
        "[1, 2, 3]",  # Array JSON
        '"just a string"',  # String JSON
        "42",  # Number JSON
    ],
)
def test_to_struct_non_dict_json(input_value):
    """Test that non-dict JSON formats return None."""
    result = _to_struct(input_value)
    assert result is None


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        (None, None),
        # JSON-parseable arrays keep JSON types
        ("[1, 2, 3, 4, 5]", [1, 2, 3, 4, 5]),
        ('["apple", "banana", "cherry"]', ["apple", "banana", "cherry"]),
        ("[true, false, null]", [True, False, None]),
        (
            '[{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]',
            [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}],
        ),
        ("not valid json", None),
        ("", None),
        ("[]", []),
    ],
)
def test_to_array_json_formats(input_value, expected):
    """Test ARRAY conversion for various JSON formats and edge cases."""
    result = _to_array(input_value)
    assert result == expected


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        # JSON-parseable: keep JSON types
        ("[1, 2, 3]", [1, 2, 3]),
        ("[]", []),
        ("[true, false, null]", [True, False, None]),
        # Native format: strings (no heuristic inference)
        ("[apple, banana, cherry]", ["apple", "banana", "cherry"]),
        (
            "[{Alice, 25}, {Bob, 30}]",
            [{"0": "Alice", "1": "25"}, {"0": "Bob", "1": "30"}],
        ),
        (
            "[{name=John, age=30}, {name=Jane, age=25}]",
            [{"name": "John", "age": "30"}, {"name": "Jane", "age": "25"}],
        ),
        # Mixed native: numbers and strings stay as strings
        ("[1, 2.5, hello]", ["1", "2.5", "hello"]),
    ],
)
def test_to_array_athena_native_formats(input_value, expected):
    """Test ARRAY conversion for Athena native formats."""
    result = _to_array(input_value)
    assert result == expected


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        ("[ARRAY[1, 2], ARRAY[3, 4]]", None),  # Nested arrays (native format)
        ("[[1, 2], [3, 4]]", [[1, 2], [3, 4]]),  # Nested arrays (JSON format - parseable)
        ("[MAP(ARRAY['key'], ARRAY['value'])]", None),  # Complex nested structures
    ],
)
def test_to_array_complex_nested_cases(input_value, expected):
    """Test complex nested array cases behavior."""
    result = _to_array(input_value)
    assert result == expected


@pytest.mark.parametrize(
    "input_value",
    [
        '"just a string"',  # String JSON
        "42",  # Number JSON
        '{"key": "value"}',  # Object JSON
    ],
)
def test_to_array_non_array_json(input_value):
    """Test that non-array JSON formats return None."""
    result = _to_array(input_value)
    assert result is None


@pytest.mark.parametrize(
    "input_value",
    [
        "not an array",  # Not bracketed
        "[unclosed array",  # Malformed
        "closed array]",  # Malformed
        "[{malformed struct}",  # Malformed struct
    ],
)
def test_to_array_invalid_formats(input_value):
    """Test that invalid array formats return None."""
    result = _to_array(input_value)
    assert result is None


# ============================================================================
# Tests for DefaultTypeConverter (without type_hint)
# ============================================================================


class TestDefaultTypeConverter:
    @pytest.mark.parametrize(
        ("input_value", "expected"),
        [
            # JSON format keeps JSON types
            ('{"name": "Alice", "age": 25}', {"name": "Alice", "age": 25}),
            (None, None),
            ("", None),
            ("invalid json", None),
            # Native format: values are strings (breaking change)
            ("{a=1, b=2}", {"a": "1", "b": "2"}),
        ],
    )
    def test_struct_conversion(self, input_value, expected):
        """Test DefaultTypeConverter STRUCT conversion for various input formats."""
        converter = DefaultTypeConverter()
        result = converter.convert("row", input_value)
        assert result == expected

    @pytest.mark.parametrize(
        ("input_value", "expected"),
        [
            # JSON format keeps JSON types
            ("[1, 2, 3]", [1, 2, 3]),
            ('["a", "b", "c"]', ["a", "b", "c"]),
            (None, None),
            ("", None),
            ("invalid json", None),
            # Native format: values are strings
            ("[apple, banana]", ["apple", "banana"]),
            ("[]", []),
        ],
    )
    def test_array_conversion(self, input_value, expected):
        """Test DefaultTypeConverter ARRAY conversion for various input formats."""
        converter = DefaultTypeConverter()
        result = converter.convert("array", input_value)
        assert result == expected


# ============================================================================
# Tests for parse_type_signature
# ============================================================================


class TestParseTypeSignature:
    def test_simple_type(self):
        node = parse_type_signature("varchar")
        assert node.type_name == "varchar"
        assert node.children == []
        assert node.field_names is None

    def test_simple_type_case_insensitive(self):
        node = parse_type_signature("VARCHAR")
        assert node.type_name == "varchar"

    def test_array_type(self):
        node = parse_type_signature("array(varchar)")
        assert node.type_name == "array"
        assert len(node.children) == 1
        assert node.children[0].type_name == "varchar"

    def test_array_of_integer(self):
        node = parse_type_signature("array(integer)")
        assert node.type_name == "array"
        assert node.children[0].type_name == "integer"

    def test_map_type(self):
        node = parse_type_signature("map(varchar, integer)")
        assert node.type_name == "map"
        assert len(node.children) == 2
        assert node.children[0].type_name == "varchar"
        assert node.children[1].type_name == "integer"

    def test_row_type(self):
        node = parse_type_signature("row(name varchar, age integer)")
        assert node.type_name == "row"
        assert len(node.children) == 2
        assert node.field_names == ["name", "age"]
        assert node.children[0].type_name == "varchar"
        assert node.children[1].type_name == "integer"

    def test_struct_type(self):
        node = parse_type_signature("struct(name varchar, age integer)")
        assert node.type_name == "struct"
        assert node.field_names == ["name", "age"]

    def test_nested_array_of_row(self):
        node = parse_type_signature("array(row(name varchar, age integer))")
        assert node.type_name == "array"
        assert len(node.children) == 1
        row_node = node.children[0]
        assert row_node.type_name == "row"
        assert row_node.field_names == ["name", "age"]
        assert row_node.children[0].type_name == "varchar"
        assert row_node.children[1].type_name == "integer"

    def test_map_with_complex_value(self):
        node = parse_type_signature("map(varchar, row(x integer, y double))")
        assert node.type_name == "map"
        assert node.children[0].type_name == "varchar"
        assert node.children[1].type_name == "row"
        assert node.children[1].field_names == ["x", "y"]

    def test_deeply_nested(self):
        node = parse_type_signature("array(row(data row(x integer, y integer), name varchar))")
        assert node.type_name == "array"
        row_node = node.children[0]
        assert row_node.type_name == "row"
        assert row_node.field_names == ["data", "name"]
        assert row_node.children[0].type_name == "row"
        assert row_node.children[0].field_names == ["x", "y"]
        assert row_node.children[1].type_name == "varchar"

    def test_parameterized_type(self):
        node = parse_type_signature("decimal(10, 2)")
        assert node.type_name == "decimal"

    def test_varchar_with_length(self):
        node = parse_type_signature("varchar(255)")
        assert node.type_name == "varchar"


# ============================================================================
# Tests for typed conversion with type_hint
# ============================================================================


class TestTypedConversion:
    def test_array_varchar_keeps_strings(self):
        """Test that array(varchar) type hint keeps elements as strings."""
        converter = DefaultTypeConverter()
        # JSON format: [1234, 5678] would be parsed as ints by JSON,
        # but type_hint says varchar, so they should be strings
        result = converter.convert("array", "[1234, 5678]", type_hint="array(varchar)")
        assert result == ["1234", "5678"]

    def test_array_integer_converts_to_int(self):
        """Test that array(integer) type hint converts elements to ints."""
        converter = DefaultTypeConverter()
        result = converter.convert("array", "[1, 2, 3]", type_hint="array(integer)")
        assert result == [1, 2, 3]

    def test_array_boolean_converts(self):
        """Test that array(boolean) type hint converts elements to bools."""
        converter = DefaultTypeConverter()
        result = converter.convert("array", "[true, false]", type_hint="array(boolean)")
        assert result == [True, False]

    def test_array_with_null(self):
        """Test that nulls in arrays are preserved regardless of type hint."""
        converter = DefaultTypeConverter()
        result = converter.convert("array", "[1, null, 3]", type_hint="array(integer)")
        assert result == [1, None, 3]

    def test_map_varchar_integer(self):
        """Test map(varchar, integer) type hint."""
        converter = DefaultTypeConverter()
        result = converter.convert(
            "map", '{"key1": 1, "key2": 2}', type_hint="map(varchar, integer)"
        )
        assert result == {"key1": 1, "key2": 2}

    def test_map_native_format_with_hints(self):
        """Test map type hint with native format."""
        converter = DefaultTypeConverter()
        result = converter.convert("map", "{a=1, b=2}", type_hint="map(varchar, integer)")
        assert result == {"a": 1, "b": 2}

    def test_row_type_hint(self):
        """Test row type hint with named fields."""
        converter = DefaultTypeConverter()
        result = converter.convert(
            "row",
            '{"name": "Alice", "age": 25}',
            type_hint="row(name varchar, age integer)",
        )
        assert result == {"name": "Alice", "age": 25}

    def test_row_native_format_with_hints(self):
        """Test row type hint with native format."""
        converter = DefaultTypeConverter()
        result = converter.convert(
            "row",
            "{name=Alice, age=25}",
            type_hint="row(name varchar, age integer)",
        )
        assert result == {"name": "Alice", "age": 25}

    def test_nested_array_of_row(self):
        """Test array(row(...)) type hint preserves correct types."""
        converter = DefaultTypeConverter()
        result = converter.convert(
            "array",
            "[{name=Alice, age=25}, {name=Bob, age=30}]",
            type_hint="array(row(name varchar, age integer))",
        )
        assert result == [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
        ]

    def test_array_varchar_prevents_number_inference(self):
        """Test the core use case: array(varchar) prevents "1234" -> 1234."""
        converter = DefaultTypeConverter()
        # This is the key issue from #689: varchar values like "1234" should
        # not be converted to numbers when type_hint specifies varchar
        result = converter.convert(
            "array",
            "[1234, 5678, hello]",
            type_hint="array(varchar)",
        )
        assert result == ["1234", "5678", "hello"]

    def test_none_value_with_type_hint(self):
        """Test that None value returns None even with type hint."""
        converter = DefaultTypeConverter()
        result = converter.convert("array", None, type_hint="array(varchar)")
        assert result is None

    def test_simple_type_hint(self):
        """Test type hint for simple types."""
        converter = DefaultTypeConverter()
        result = converter.convert("varchar", "hello", type_hint="varchar")
        assert result == "hello"

    def test_type_hint_caching(self):
        """Test that parsed type hints are cached."""
        converter = DefaultTypeConverter()
        converter.convert("array", "[1, 2]", type_hint="array(integer)")
        assert "array(integer)" in converter._parsed_hints
        # Second call should use cache
        converter.convert("array", "[3, 4]", type_hint="array(integer)")
        assert len(converter._parsed_hints) == 1

    def test_empty_array_with_type_hint(self):
        """Test empty array with type hint."""
        converter = DefaultTypeConverter()
        result = converter.convert("array", "[]", type_hint="array(varchar)")
        assert result == []

    def test_map_varchar_varchar(self):
        """Test map(varchar, varchar) keeps all values as strings."""
        converter = DefaultTypeConverter()
        result = converter.convert("map", "{key1=123, key2=456}", type_hint="map(varchar, varchar)")
        assert result == {"key1": "123", "key2": "456"}

    def test_row_with_nested_struct(self):
        """Test row with nested struct field."""
        converter = DefaultTypeConverter()
        result = converter.convert(
            "row",
            "{header={seq=123, stamp=2024}, x=4.5}",
            type_hint="row(header row(seq integer, stamp varchar), x double)",
        )
        assert result == {"header": {"seq": 123, "stamp": "2024"}, "x": 4.5}
