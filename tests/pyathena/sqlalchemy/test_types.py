import json

import pytest
from sqlalchemy import types

from pyathena.sqlalchemy.base import ischema_names
from pyathena.sqlalchemy.rest import AthenaRestDialect


def test_double_column_type():
    assert ischema_names["double"] is types.DOUBLE


class _JSONText(types.TypeDecorator):
    impl = types.JSON
    cache_ok = True


class TestAthenaJSON:
    @staticmethod
    def _process(type_, value, coltype, dialect=None):
        dialect = dialect or AthenaRestDialect()
        processor = type_.dialect_impl(dialect).result_processor(dialect, coltype)
        return processor(value) if processor else value

    @pytest.mark.parametrize("type_", [types.JSON(), _JSONText()])
    @pytest.mark.parametrize(
        "value",
        [{"a": 1}, [1, 2, 3], '{"a": 1}', "text", 1, True, None],
    )
    def test_keeps_converted_json_results(self, type_, value):
        assert self._process(type_, value, "json") == value

    @pytest.mark.parametrize("type_", [types.JSON(), _JSONText()])
    @pytest.mark.parametrize(
        ("value", "expected"),
        [('{"a": 1}', {"a": 1}), ("[1, 2, 3]", [1, 2, 3]), ('"text"', "text"), (None, None)],
    )
    def test_decodes_json_text_of_other_types(self, type_, value, expected):
        assert self._process(type_, value, "varchar") == expected

    def test_decodes_json_text_with_dialect_deserializer(self):
        dialect = AthenaRestDialect(json_deserializer=lambda v: ("custom", json.loads(v)))
        assert self._process(types.JSON(), '{"a": 1}', "varchar", dialect) == (
            "custom",
            {"a": 1},
        )
        assert self._process(types.JSON(), {"a": 1}, "json", dialect) == {"a": 1}
