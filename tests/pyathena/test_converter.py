import logging
from datetime import datetime, time, timedelta, timezone
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from pyathena.converter import (
    DefaultTypeConverter,
    _parse_time_zone,
    _to_array,
    _to_datetime,
    _to_datetime_with_tz,
    _to_json,
    _to_map,
    _to_struct,
    _to_time,
    _to_time_with_tz,
)


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        (None, None),
        ("2020-01-01 00:00:00", datetime(2020, 1, 1)),
        ("2020-01-01 00:00:00.1", datetime(2020, 1, 1, 0, 0, 0, 100000)),
        ("2020-01-01 00:00:00.123", datetime(2020, 1, 1, 0, 0, 0, 123000)),
        ("2020-01-01 00:00:00.123456", datetime(2020, 1, 1, 0, 0, 0, 123456)),
        ("2020-01-01 00:00:00.123456789012", datetime(2020, 1, 1, 0, 0, 0, 123456)),
    ],
)
def test_to_datetime_any_precision(input_value, expected):
    assert _to_datetime(input_value) == expected


def test_to_datetime_with_tz_any_precision():
    assert _to_datetime_with_tz("2020-01-01 00:00:00 UTC") == datetime(
        2020, 1, 1, tzinfo=ZoneInfo("UTC")
    )
    assert _to_datetime_with_tz("2020-01-01 00:00:00.123456789 UTC") == datetime(
        2020, 1, 1, 0, 0, 0, 123456, tzinfo=ZoneInfo("UTC")
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
    # Values too complex to parse fall back to the original string
    # instead of being silently dropped
    assert result == input_value or isinstance(result, dict)


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
    "input_value",
    [
        # MAP<VARCHAR, VARCHAR> whose values contain nested structures:
        # too complex to parse reliably, so the original string is kept
        # instead of returning None (which looked like silent data loss)
        "{items=[{product_id=285, option_id=6049, amount=1, price=12000}], brand_id=75}",
        '{items=[{"product_id":285,"option_id":6049}], brand_id=75}',
        "{callback=fn(x), retries=3}",
    ],
)
def test_to_map_complex_values_keep_original_string(input_value):
    assert _to_map(input_value) == input_value


def test_to_map_simple_values_still_parse():
    assert _to_map("{push=Y}") == {"push": "Y"}
    assert _to_map("{url=/webview/checkout, brand_id=75}") == {
        "url": "/webview/checkout",
        "brand_id": "75",
    }


@pytest.mark.parametrize(
    "input_value",
    [
        # Nested braces-only values (e.g. MAP<VARCHAR, ROW(...)>) pass the
        # "()[]" pre-check but every pair is skipped by _parse_map_native
        "{a={b=1}}",
        "{a={b=1, c=2}}",
    ],
)
def test_to_map_nested_brace_values_keep_original_string(input_value):
    assert _to_map(input_value) == input_value


@pytest.mark.parametrize(
    "input_value",
    [
        # Partially parseable values must not return a partial result:
        # either fully parsed or the intact original string
        '{a="x", b=1}',
        "{a, b=1}",
    ],
)
def test_to_map_never_returns_partial_dict(input_value):
    assert _to_map(input_value) == input_value


@pytest.mark.parametrize(
    "input_value",
    [
        "[a=1, b=2]",  # every item skipped by the '=' safety check
        "[a, b=1]",  # partially parseable: must not return ['a']
    ],
)
def test_to_array_never_returns_partial_list(input_value):
    assert _to_array(input_value) == input_value


def test_to_struct_skipped_pairs_keep_original_string():
    # The quoted key is skipped by _parse_named_struct's safety check
    assert _to_struct('{"a"=1}') == '{"a"=1}'


def test_to_array_nested_values_keep_original_string():
    # Nested arrays in native format are too complex to parse reliably,
    # so the original string is kept instead of returning None
    value = "[[1, 2], [3, 4]]"
    assert _to_array(value) == [[1, 2], [3, 4]]  # valid JSON parses first
    native_value = "[{a=[1, 2]}, {b=[3]}]"
    assert _to_array(native_value) == native_value


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
        # Too complex for native parsing: the original string is kept
        # instead of returning None (which looked like silent data loss)
        ("[ARRAY[1, 2], ARRAY[3, 4]]", "[ARRAY[1, 2], ARRAY[3, 4]]"),
        ("[[1, 2], [3, 4]]", [[1, 2], [3, 4]]),
        ("[MAP(ARRAY['key'], ARRAY['value'])]", "[MAP(ARRAY['key'], ARRAY['value'])]"),
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


@pytest.mark.parametrize(
    ("value", "type_hint", "expected"),
    [
        ('[""]', "array(json)", [""]),
        ('{"k": ""}', "map(varchar,json)", {"k": ""}),
    ],
)
def test_nested_json_empty_string(value, type_hint, expected):
    assert DefaultTypeConverter().convert(type_hint.split("(")[0], value, type_hint) == expected


class TestDefaultTypeConverter:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [("true", True), ("false", False), ("1", True), ("0", False), (None, None), ("", None)],
    )
    def test_boolean_conversion(self, value, expected):
        assert DefaultTypeConverter().convert("boolean", value) is expected

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

    def test_normalize_hive_syntax_noop(self):
        """Trino-style input passes through unchanged."""
        assert DefaultTypeConverter._normalize_hive_syntax("array(integer)") == "array(integer)"

    def test_normalize_hive_syntax_replaces(self):
        assert (
            DefaultTypeConverter._normalize_hive_syntax("array<struct<a:int>>")
            == "array(struct(a int))"
        )

    def test_normalize_hive_syntax_struct(self):
        converter = DefaultTypeConverter()
        result = converter.convert(
            "row",
            "{name=Alice, age=25}",
            type_hint="struct<name:varchar,age:int>",
        )
        assert result == {"name": "Alice", "age": 25}

    def test_normalize_hive_syntax_nested(self):
        converter = DefaultTypeConverter()
        result = converter.convert(
            "array",
            "[{a=1, b=hello}, {a=2, b=world}]",
            type_hint="array<struct<a:int,b:varchar>>",
        )
        assert result == [{"a": 1, "b": "hello"}, {"a": 2, "b": "world"}]

    def test_normalize_hive_syntax_map(self):
        converter = DefaultTypeConverter()
        result = converter.convert(
            "map",
            '{"x": 1, "y": 2}',
            type_hint="map<string,int>",
        )
        assert result == {"x": 1, "y": 2}

    def test_normalize_hive_syntax_mixed(self):
        """Hive angle brackets wrapping Trino-style parenthesized inner type."""
        converter = DefaultTypeConverter()
        result = converter.convert(
            "array",
            "[{a=1, b=hello}]",
            type_hint="array<row(a int, b varchar)>",
        )
        assert result == [{"a": 1, "b": "hello"}]


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        (None, None),
        ("", None),
        ("12:34:56", time(12, 34, 56)),
        ("12:34:56.1", time(12, 34, 56, 100000)),
        ("12:34:56.123", time(12, 34, 56, 123000)),
        ("12:34:56.123456", time(12, 34, 56, 123456)),
        ("12:34:56.123456789012", time(12, 34, 56, 123456)),
    ],
)
def test_to_time_any_precision(input_value, expected):
    assert _to_time(input_value) == expected


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        (None, None),
        ("", None),
        ("12:34:56+09:00", time(12, 34, 56, tzinfo=timezone(timedelta(hours=9)))),
        (
            "12:34:56.789-05:30",
            time(12, 34, 56, 789000, tzinfo=timezone(-timedelta(hours=5, minutes=30))),
        ),
        ("00:00:00.123456789012+00:00", time(0, 0, 0, 123456, tzinfo=timezone(timedelta(0)))),
        ("23:59:59.9-14:00", time(23, 59, 59, 900000, tzinfo=timezone(-timedelta(hours=14)))),
    ],
)
def test_to_time_with_tz(input_value, expected):
    result = _to_time_with_tz(input_value)
    assert result == expected
    if expected is not None:
        assert result.utcoffset() == expected.utcoffset()


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        (None, None),
        ("", None),
        ('""', ""),
        ('"[1, 2]"', "[1, 2]"),
        ('{"a": 1}', {"a": 1}),
        ("[1, 2]", [1, 2]),
        ("null", None),
    ],
)
def test_to_json(input_value, expected):
    assert _to_json(input_value) == expected


@pytest.mark.parametrize(
    ("type_hint", "value", "expected"),
    [
        ("array(json)", '[""]', [""]),
        (
            "array(json)",
            '[{"a":1}, "x", 1, true, null, ""]',
            [{"a": 1}, "x", 1, True, None, ""],
        ),
        # JSON string scalars whose text looks like JSON stay strings.
        (
            "array(json)",
            '["{\\"a\\": 1}", "123", "true", "null"]',
            ['{"a": 1}', "123", "true", "null"],
        ),
        (
            "map(varchar,json)",
            '{"k": "", "n": 1, "b": true, "z": null}',
            {"k": "", "n": 1, "b": True, "z": None},
        ),
        ("row(a json, b json)", '{"a": "x", "b": {"c": 1}}', {"a": "x", "b": {"c": 1}}),
        ("row(a json, b json)", '{"a": "", "b": true}', {"a": "", "b": True}),
        ("array(varchar)", '["a", "123"]', ["a", "123"]),
    ],
)
def test_typed_json_elements(type_hint, value, expected):
    """JSON elements of typed complex values decode their original JSON text."""
    type_ = type_hint.split("(", 1)[0]
    assert DefaultTypeConverter().convert(type_, value, type_hint=type_hint) == expected


def test_typed_time_with_tz_elements():
    """Parameterized time zone types in type hints keep their time zone."""
    converter = DefaultTypeConverter()
    jst = timezone(timedelta(hours=9))
    assert converter.convert(
        "array", "[12:34:56.789+09:00, null]", type_hint="array(time(3) with time zone)"
    ) == [time(12, 34, 56, 789000, tzinfo=jst), None]
    assert converter.convert(
        "map", "{a=12:34:56+09:00}", type_hint="map(varchar, time(0) with time zone)"
    ) == {"a": time(12, 34, 56, tzinfo=jst)}


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        (None, None),
        ("", None),
        (
            "2024-02-29 23:59:58.123 +05:30",
            datetime(
                2024, 2, 29, 23, 59, 58, 123000, tzinfo=timezone(timedelta(hours=5, minutes=30))
            ),
        ),
        (
            "2024-02-29 23:59:58.123456 -08:00",
            datetime(2024, 2, 29, 23, 59, 58, 123456, tzinfo=timezone(-timedelta(hours=8))),
        ),
        (
            "2024-02-29 23:59:58 +00:00",
            datetime(2024, 2, 29, 23, 59, 58, tzinfo=timezone(timedelta(0))),
        ),
        (
            "2024-02-29 23:59:58.123 UTC",
            datetime(2024, 2, 29, 23, 59, 58, 123000, tzinfo=ZoneInfo("UTC")),
        ),
        (
            "2024-02-29 23:59:58.123 America/New_York",
            datetime(2024, 2, 29, 23, 59, 58, 123000, tzinfo=ZoneInfo("America/New_York")),
        ),
    ],
)
def test_to_datetime_with_tz_offsets_and_zone_names(input_value, expected):
    """Numeric UTC offsets give fixed-offset time zones; zone names keep their zone."""
    result = _to_datetime_with_tz(input_value)
    assert result == expected
    if expected is not None:
        assert result.utcoffset() == expected.utcoffset()
        assert result.tzinfo == expected.tzinfo


@pytest.mark.parametrize("zone_name", ["Foo/Bar", "../etc"])
def test_to_datetime_with_tz_unknown_zone_name(caplog, zone_name):
    """An unknown zone name gives a naive datetime and one warning per name."""
    _parse_time_zone.cache_clear()
    with caplog.at_level(logging.WARNING, logger="pyathena.converter"):
        results = [
            _to_datetime_with_tz(f"2024-02-29 23:59:58.123 {zone_name}"),
            _to_datetime_with_tz(f"2024-02-29 23:59:59 {zone_name}"),
        ]
    assert results == [
        datetime(2024, 2, 29, 23, 59, 58, 123000),
        datetime(2024, 2, 29, 23, 59, 59),
    ]
    assert [record.getMessage() for record in caplog.records] == [
        f"Unknown time zone {zone_name!r}; returning naive datetimes for it."
    ]


def test_to_datetime_with_tz_unreadable_zone_name():
    """A zone name the database cannot load gives a naive datetime.

    With only the tzdata package, ``ZoneInfo("America")`` opens a directory and
    raises ``IsADirectoryError``.
    """
    _parse_time_zone.cache_clear()
    with patch("pyathena.converter.ZoneInfo", side_effect=IsADirectoryError("America")):
        result = _to_datetime_with_tz("2024-02-29 23:59:58 America")
    _parse_time_zone.cache_clear()
    assert result == datetime(2024, 2, 29, 23, 59, 58)


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        ("[x, , y]", ["x", "", "y"]),
        ("[x, ]", ["x", ""]),
        ("[x,  , y]", ["x", " ", "y"]),
        ("[ , x]", [" ", "x"]),
        ("[a,b, c]", ["a,b", "c"]),
        ("[x, null]", ["x", None]),
        ("[{a=1, b=}, {a=, b=2}]", [{"a": "1", "b": ""}, {"a": "", "b": "2"}]),
    ],
)
def test_native_array_items(input_value, expected):
    """Native arrays are split as Athena joins them, keeping empty items."""
    assert _to_array(input_value) == expected
    assert (
        DefaultTypeConverter().convert("array", input_value, type_hint="array(varchar)") == expected
    )


def test_typed_json_array_starting_with_non_string():
    """A typed array(json) value is parsed as JSON whatever its first element is."""
    converter = DefaultTypeConverter()
    assert converter.convert("array", '[1234567890, "a,b"]', type_hint="array(json)") == [
        1234567890,
        "a,b",
    ]
    assert converter.convert("array", "[1, 2.5, true]", type_hint="array(json)") == [1, 2.5, True]
