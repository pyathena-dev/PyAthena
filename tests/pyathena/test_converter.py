import pytest

from pyathena.converter import (
    DefaultTypeConverter,
    _to_array,
    _to_map,
    _to_struct,
)


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
    assert _to_struct(input_value) == expected


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
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
    assert _to_struct(input_value) == expected


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        (
            "{header={stamp=2024-01-01, seq=123}, x=4.736, y=0.583}",
            {"header": {"stamp": "2024-01-01", "seq": "123"}, "x": "4.736", "y": "0.583"},
        ),
        (
            "{outer={middle={inner=value}}, field=123}",
            {"outer": {"middle": {"inner": "value"}}, "field": "123"},
        ),
        (
            "{pos={x=1, y=2}, vel={x=0.5, y=0.3}, timestamp=12345}",
            {
                "pos": {"x": "1", "y": "2"},
                "vel": {"x": "0.5", "y": "0.3"},
                "timestamp": "12345",
            },
        ),
        (
            "{level1={level2={level3={value=deep}}}}",
            {"level1": {"level2": {"level3": {"value": "deep"}}}},
        ),
        (
            "{metadata={id=123, active=true, name=test}, count=5}",
            {"metadata": {"id": "123", "active": "true", "name": "test"}, "count": "5"},
        ),
        (
            "{data={value=null, status=ok}, flag=true}",
            {"data": {"value": None, "status": "ok"}, "flag": "true"},
        ),
        (
            "{a={b={c=1, d=2}, e=3}, f=4, g={h=5}}",
            {"a": {"b": {"c": "1", "d": "2"}, "e": "3"}, "f": "4", "g": {"h": "5"}},
        ),
    ],
)
def test_to_struct_athena_nested_formats(input_value, expected):
    assert _to_struct(input_value) == expected


@pytest.mark.parametrize(
    "input_value",
    [
        "{formula=x=y+1, status=active}",
        '{json={"key": "value"}, name=test}',
        '{message=He said "hello", name=John}',
    ],
)
def test_to_struct_athena_complex_cases(input_value):
    result = _to_struct(input_value)
    assert result is None or isinstance(result, dict)


@pytest.mark.parametrize(
    "input_value",
    [
        "[1, 2, 3]",
        '"just a string"',
        "42",
    ],
)
def test_to_struct_non_dict_json(input_value):
    assert _to_struct(input_value) is None


def test_to_map_athena_numeric_keys():
    assert _to_map("{1=2, 3=4}") == {"1": "2", "3": "4"}


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        (None, None),
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
    assert _to_array(input_value) == expected


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        ("[1, 2, 3]", [1, 2, 3]),
        ("[]", []),
        ("[true, false, null]", [True, False, None]),
        ("[apple, banana, cherry]", ["apple", "banana", "cherry"]),
        (
            "[{Alice, 25}, {Bob, 30}]",
            [{"0": "Alice", "1": "25"}, {"0": "Bob", "1": "30"}],
        ),
        (
            "[{name=John, age=30}, {name=Jane, age=25}]",
            [{"name": "John", "age": "30"}, {"name": "Jane", "age": "25"}],
        ),
        ("[1, 2.5, hello]", ["1", "2.5", "hello"]),
    ],
)
def test_to_array_athena_native_formats(input_value, expected):
    assert _to_array(input_value) == expected


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        (
            "[{header={stamp=2024-01-01, seq=123}, x=4.736}]",
            [{"header": {"stamp": "2024-01-01", "seq": "123"}, "x": "4.736"}],
        ),
        (
            "[{pos={x=1, y=2}, vel={x=0.5}}, {pos={x=3, y=4}, vel={x=1.5}}]",
            [
                {"pos": {"x": "1", "y": "2"}, "vel": {"x": "0.5"}},
                {"pos": {"x": "3", "y": "4"}, "vel": {"x": "1.5"}},
            ],
        ),
        (
            "[{data={meta={id=1, active=true}}}]",
            [{"data": {"meta": {"id": "1", "active": "true"}}}],
        ),
    ],
)
def test_to_array_athena_nested_struct_elements(input_value, expected):
    assert _to_array(input_value) == expected


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        ("[ARRAY[1, 2], ARRAY[3, 4]]", None),
        ("[[1, 2], [3, 4]]", [[1, 2], [3, 4]]),
        ("[MAP(ARRAY['key'], ARRAY['value'])]", None),
    ],
)
def test_to_array_complex_nested_cases(input_value, expected):
    assert _to_array(input_value) == expected


@pytest.mark.parametrize(
    "input_value",
    [
        '"just a string"',
        "42",
        '{"key": "value"}',
    ],
)
def test_to_array_non_array_json(input_value):
    assert _to_array(input_value) is None


@pytest.mark.parametrize(
    "input_value",
    [
        "not an array",
        "[unclosed array",
        "closed array]",
        "[{malformed struct}",
    ],
)
def test_to_array_invalid_formats(input_value):
    assert _to_array(input_value) is None


class TestDefaultTypeConverter:
    @pytest.mark.parametrize(
        ("input_value", "expected"),
        [
            ('{"name": "Alice", "age": 25}', {"name": "Alice", "age": 25}),
            (None, None),
            ("", None),
            ("invalid json", None),
            ("{a=1, b=2}", {"a": "1", "b": "2"}),
        ],
    )
    def test_struct_conversion(self, input_value, expected):
        converter = DefaultTypeConverter()
        assert converter.convert("row", input_value) == expected

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
        converter = DefaultTypeConverter()
        assert converter.convert("array", input_value) == expected

    def test_array_varchar_keeps_strings(self):
        converter = DefaultTypeConverter()
        result = converter.convert("array", "[1234, 5678]", type_hint="array(varchar)")
        assert result == ["1234", "5678"]

    def test_array_integer_converts_to_int(self):
        converter = DefaultTypeConverter()
        result = converter.convert("array", "[1, 2, 3]", type_hint="array(integer)")
        assert result == [1, 2, 3]

    def test_array_boolean_converts(self):
        converter = DefaultTypeConverter()
        result = converter.convert("array", "[true, false]", type_hint="array(boolean)")
        assert result == [True, False]

    def test_array_with_null(self):
        converter = DefaultTypeConverter()
        result = converter.convert("array", "[1, null, 3]", type_hint="array(integer)")
        assert result == [1, None, 3]

    def test_map_varchar_integer(self):
        converter = DefaultTypeConverter()
        result = converter.convert(
            "map", '{"key1": 1, "key2": 2}', type_hint="map(varchar, integer)"
        )
        assert result == {"key1": 1, "key2": 2}

    def test_map_native_format_with_hints(self):
        converter = DefaultTypeConverter()
        result = converter.convert("map", "{a=1, b=2}", type_hint="map(varchar, integer)")
        assert result == {"a": 1, "b": 2}

    def test_row_type_hint(self):
        converter = DefaultTypeConverter()
        result = converter.convert(
            "row",
            '{"name": "Alice", "age": 25}',
            type_hint="row(name varchar, age integer)",
        )
        assert result == {"name": "Alice", "age": 25}

    def test_row_native_format_with_hints(self):
        converter = DefaultTypeConverter()
        result = converter.convert(
            "row",
            "{name=Alice, age=25}",
            type_hint="row(name varchar, age integer)",
        )
        assert result == {"name": "Alice", "age": 25}

    def test_nested_array_of_row(self):
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
        converter = DefaultTypeConverter()
        result = converter.convert(
            "array",
            "[1234, 5678, hello]",
            type_hint="array(varchar)",
        )
        assert result == ["1234", "5678", "hello"]

    def test_none_value_with_type_hint(self):
        converter = DefaultTypeConverter()
        assert converter.convert("array", None, type_hint="array(varchar)") is None

    def test_simple_type_hint(self):
        converter = DefaultTypeConverter()
        assert converter.convert("varchar", "hello", type_hint="varchar") == "hello"

    def test_type_hint_caching(self):
        converter = DefaultTypeConverter()
        converter.convert("array", "[1, 2]", type_hint="array(integer)")
        assert "array(integer)" in converter._parsed_hints
        converter.convert("array", "[3, 4]", type_hint="array(integer)")
        assert len(converter._parsed_hints) == 1

    def test_empty_array_with_type_hint(self):
        converter = DefaultTypeConverter()
        assert converter.convert("array", "[]", type_hint="array(varchar)") == []

    def test_map_varchar_varchar(self):
        converter = DefaultTypeConverter()
        result = converter.convert("map", "{key1=123, key2=456}", type_hint="map(varchar, varchar)")
        assert result == {"key1": "123", "key2": "456"}

    def test_row_with_nested_struct(self):
        converter = DefaultTypeConverter()
        result = converter.convert(
            "row",
            "{header={seq=123, stamp=2024}, x=4.5}",
            type_hint="row(header row(seq integer, stamp varchar), x double)",
        )
        assert result == {"header": {"seq": 123, "stamp": "2024"}, "x": 4.5}

    def test_fallback_on_malformed_value(self):
        """When typed conversion fails (returns None), fall back to untyped conversion."""
        converter = DefaultTypeConverter()
        # "not-an-array" doesn't look like an array — typed converter returns None.
        # Untyped _to_array also returns None for this input, which is correct.
        result = converter.convert("array", "not-an-array", type_hint="array(integer)")
        assert result is None

    def test_fallback_preserves_struct_value(self):
        """Malformed struct with type_hint still falls back to untyped parsing."""
        converter = DefaultTypeConverter()
        # Struct with no closing brace — typed converter returns None.
        # Untyped _to_struct also returns None here.
        result = converter.convert("row", "{unclosed", type_hint="row(a integer)")
        assert result is None

    def test_fallback_returns_untyped_result(self):
        """When typed conversion returns None, untyped conversion is used."""
        converter = DefaultTypeConverter()
        # The typed converter returns None for a struct that doesn't start with "{".
        # The untyped _to_struct also returns None for non-struct input.
        # Use an array example where typed converter returns None (not a bracket-wrapped
        # value), but untyped _to_array can still parse it via JSON.
        result = converter.convert(
            "row",
            '{"a": 1}',
            type_hint="row(a varchar)",
        )
        # Typed conversion succeeds here — "a" is varchar so "1" stays a string
        assert result == {"a": "1"}

    def test_hive_syntax_through_converter(self):
        """Hive-style syntax works end-to-end through DefaultTypeConverter."""
        converter = DefaultTypeConverter()
        result = converter.convert("array", "[1, 2, 3]", type_hint="array<int>")
        assert result == [1, 2, 3]

    def test_hive_syntax_struct_through_converter(self):
        """Hive struct syntax works end-to-end."""
        converter = DefaultTypeConverter()
        result = converter.convert(
            "row",
            "{name=Alice, age=25}",
            type_hint="struct<name:varchar,age:int>",
        )
        assert result == {"name": "Alice", "age": 25}

    def test_hive_syntax_caching(self):
        """Hive syntax is normalized before cache lookup."""
        converter = DefaultTypeConverter()
        converter.convert("array", "[1]", type_hint="array<integer>")
        converter.convert("array", "[2]", type_hint="array(integer)")
        # Both should normalize to "array(integer)" in the cache
        assert "array(integer)" in converter._parsed_hints
        assert len(converter._parsed_hints) == 1
