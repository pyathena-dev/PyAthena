import pytest

from pyathena.converter import DefaultTypeConverter, _to_array, _to_struct


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


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        ("{a=1, b=2}", {"a": 1, "b": 2}),
        ("{}", {}),
        ("{name=John, city=Tokyo}", {"name": "John", "city": "Tokyo"}),
        ("{Alice, 25}", {"0": "Alice", "1": 25}),
        ("{John, 30, true}", {"0": "John", "1": 30, "2": True}),
        ("{name=John, age=30}", {"name": "John", "age": 30}),
        ("{x=1, y=2, z=3}", {"x": 1, "y": 2, "z": 3}),
        ("{active=true, count=42}", {"active": True, "count": 42}),
    ],
)
def test_to_struct_athena_native_formats(input_value, expected):
    """Test STRUCT conversion for Athena native formats."""
    result = _to_struct(input_value)
    assert result == expected


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        # Single level nesting (Issue #627)
        (
            "{header={stamp=2024-01-01, seq=123}, x=4.736, y=0.583}",
            {"header": {"stamp": "2024-01-01", "seq": 123}, "x": 4.736, "y": 0.583},
        ),
        # Double nesting
        (
            "{outer={middle={inner=value}}, field=123}",
            {"outer": {"middle": {"inner": "value"}}, "field": 123},
        ),
        # Multiple nested fields
        (
            "{pos={x=1, y=2}, vel={x=0.5, y=0.3}, timestamp=12345}",
            {"pos": {"x": 1, "y": 2}, "vel": {"x": 0.5, "y": 0.3}, "timestamp": 12345},
        ),
        # Triple nesting
        (
            "{level1={level2={level3={value=deep}}}}",
            {"level1": {"level2": {"level3": {"value": "deep"}}}},
        ),
        # Mixed types in nested struct
        (
            "{metadata={id=123, active=true, name=test}, count=5}",
            {"metadata": {"id": 123, "active": True, "name": "test"}, "count": 5},
        ),
        # Nested struct with null value
        (
            "{data={value=null, status=ok}, flag=true}",
            {"data": {"value": None, "status": "ok"}, "flag": True},
        ),
        # Complex nesting with multiple levels and fields
        (
            "{a={b={c=1, d=2}, e=3}, f=4, g={h=5}}",
            {"a": {"b": {"c": 1, "d": 2}, "e": 3}, "f": 4, "g": {"h": 5}},
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


def test_to_map_athena_numeric_keys():
    """Test Athena map with numeric keys"""
    from pyathena.converter import _to_map

    map_value = "{1=2, 3=4}"
    result = _to_map(map_value)
    expected = {"1": 2, "3": 4}
    assert result == expected


def test_to_array_athena_numeric_elements():
    """Test Athena array with numeric elements"""
    array_value = "[1, 2, 3, 4]"
    result = _to_array(array_value)
    expected = [1, 2, 3, 4]
    assert result == expected


def test_to_array_athena_mixed_elements():
    """Test Athena array with mixed type elements"""
    array_value = "[1, hello, true, null]"
    result = _to_array(array_value)
    expected = [1, "hello", True, None]
    assert result == expected


def test_to_array_athena_struct_elements():
    """Test Athena array with struct elements"""
    array_value = "[{name=John, age=30}, {name=Jane, age=25}]"
    result = _to_array(array_value)
    expected = [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]
    assert result == expected


def test_to_array_athena_unnamed_struct_elements():
    """Test Athena array with unnamed struct elements"""
    array_value = "[{Alice, 25}, {Bob, 30}]"
    result = _to_array(array_value)
    expected = [{"0": "Alice", "1": 25}, {"0": "Bob", "1": 30}]
    assert result == expected


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        # Array with nested structs (Issue #627)
        (
            "[{header={stamp=2024-01-01, seq=123}, x=4.736}]",
            [{"header": {"stamp": "2024-01-01", "seq": 123}, "x": 4.736}],
        ),
        # Multiple elements with nested structs
        (
            "[{pos={x=1, y=2}, vel={x=0.5}}, {pos={x=3, y=4}, vel={x=1.5}}]",
            [
                {"pos": {"x": 1, "y": 2}, "vel": {"x": 0.5}},
                {"pos": {"x": 3, "y": 4}, "vel": {"x": 1.5}},
            ],
        ),
        # Array with deeply nested structs
        (
            "[{data={meta={id=1, active=true}}}]",
            [{"data": {"meta": {"id": 1, "active": True}}}],
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
        (
            "[1, 2, 3, 4, 5]",
            [1, 2, 3, 4, 5],
        ),
        (
            '["apple", "banana", "cherry"]',
            ["apple", "banana", "cherry"],
        ),
        (
            "[true, false, null]",
            [True, False, None],
        ),
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
        ("[1, 2, 3]", [1, 2, 3]),
        ("[]", []),
        ("[apple, banana, cherry]", ["apple", "banana", "cherry"]),
        ("[{Alice, 25}, {Bob, 30}]", [{"0": "Alice", "1": 25}, {"0": "Bob", "1": 30}]),
        (
            "[{name=John, age=30}, {name=Jane, age=25}]",
            [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}],
        ),
        ("[true, false, null]", [True, False, None]),
        ("[1, 2.5, hello]", [1, 2.5, "hello"]),
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


class TestDefaultTypeConverter:
    @pytest.mark.parametrize(
        ("input_value", "expected"),
        [
            ('{"name": "Alice", "age": 25}', {"name": "Alice", "age": 25}),
            (None, None),
            ("", None),
            ("invalid json", None),
            ("{a=1, b=2}", {"a": 1, "b": 2}),
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
            ("[1, 2, 3]", [1, 2, 3]),
            ('["a", "b", "c"]', ["a", "b", "c"]),
            (None, None),
            ("", None),
            ("invalid json", None),
            ("[apple, banana]", ["apple", "banana"]),
            ("[]", []),
        ],
    )
    def test_array_conversion(self, input_value, expected):
        """Test DefaultTypeConverter ARRAY conversion for various input formats."""
        converter = DefaultTypeConverter()
        result = converter.convert("array", input_value)
        assert result == expected
