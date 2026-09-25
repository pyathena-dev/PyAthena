from datetime import date, datetime

import pytest
from sqlalchemy import types

from pyathena.sqlalchemy.base import AthenaDialect
from pyathena.sqlalchemy.types import (
    AthenaDate,
    AthenaTimestamp,
)


class TestAthenaDate:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (date(2017, 1, 1), "DATE '2017-01-01'"),
            (datetime(2017, 1, 1, 12, 34, 56), "DATE '2017-01-01'"),
        ],
    )
    def test_process_renders_date_only_literal(self, value, expected):
        assert AthenaDate.process(value) == expected

    def test_process_falls_back_to_str(self):
        assert AthenaDate.process("2017-01-01") == "DATE '2017-01-01'"

    def test_process_escapes_str(self):
        assert AthenaDate.process("2017-01-01' OR 1=1 --") == "DATE '2017-01-01'' OR 1=1 --'"

    @pytest.mark.parametrize("type_", [types.Date, types.DATE, AthenaDate])
    def test_python_type(self, type_):
        assert type_().dialect_impl(AthenaDialect()).python_type is date


class TestAthenaTimestamp:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            # A sub-millisecond part keeps all six digits (timestamp(6));
            # other values keep three digits (timestamp(3)).
            (
                datetime(2017, 1, 1, 12, 34, 56, 789012),
                "TIMESTAMP '2017-01-01 12:34:56.789012'",
            ),
            (
                datetime(2017, 1, 1, 12, 34, 56, 789000),
                "TIMESTAMP '2017-01-01 12:34:56.789'",
            ),
            (
                datetime(2017, 1, 1, 12, 34, 56),
                "TIMESTAMP '2017-01-01 12:34:56.000'",
            ),
        ],
    )
    def test_process_renders_literal_precision(self, value, expected):
        assert AthenaTimestamp.process(value) == expected

    def test_process_falls_back_to_str(self):
        assert (
            AthenaTimestamp.process("2017-01-01 12:34:56.789")
            == "TIMESTAMP '2017-01-01 12:34:56.789'"
        )

    def test_process_escapes_str(self):
        assert (
            AthenaTimestamp.process("2017-01-01' OR 1=1 --") == "TIMESTAMP '2017-01-01'' OR 1=1 --'"
        )

    @pytest.mark.parametrize(
        ("precision", "expected"),
        [
            (0, "TIMESTAMP '2017-01-01 12:34:56'"),
            (3, "TIMESTAMP '2017-01-01 12:34:56.789'"),
            (6, "TIMESTAMP '2017-01-01 12:34:56.789999'"),
        ],
    )
    def test_literal_processor_precision(self, precision, expected):
        processor = AthenaTimestamp(precision=precision).literal_processor(AthenaDialect())
        assert processor(datetime(2017, 1, 1, 12, 34, 56, 789999)) == expected

    @pytest.mark.parametrize(
        ("precision", "expected"),
        [
            (0, datetime(2017, 1, 1, 12, 34, 56)),
            (3, datetime(2017, 1, 1, 12, 34, 56, 789000)),
            (5, datetime(2017, 1, 1, 12, 34, 56, 789990)),
        ],
    )
    def test_bind_processor_truncates(self, precision, expected):
        processor = AthenaTimestamp(precision=precision).bind_processor(AthenaDialect())
        assert processor(datetime(2017, 1, 1, 12, 34, 56, 789999)) == expected
        assert processor(None) is None

    @pytest.mark.parametrize("precision", [None, 6])
    def test_bind_processor_without_truncation(self, precision):
        assert AthenaTimestamp(precision=precision).bind_processor(AthenaDialect()) is None

    @pytest.mark.parametrize("precision", [-1, 7])
    def test_invalid_precision(self, precision):
        with pytest.raises(ValueError, match="precision"):
            AthenaTimestamp(precision=precision)

    @pytest.mark.parametrize(
        "type_", [types.DateTime, types.DATETIME, types.TIMESTAMP, AthenaTimestamp]
    )
    def test_python_type(self, type_):
        assert type_().dialect_impl(AthenaDialect()).python_type is datetime
