from datetime import date, datetime

import pytest

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


class TestAthenaTimestamp:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            # Athena TIMESTAMP has millisecond precision, so the six digits
            # strftime("%f") emits are truncated to three.
            (
                datetime(2017, 1, 1, 12, 34, 56, 789012),
                "TIMESTAMP '2017-01-01 12:34:56.789'",
            ),
            (
                datetime(2017, 1, 1, 12, 34, 56),
                "TIMESTAMP '2017-01-01 12:34:56.000'",
            ),
        ],
    )
    def test_process_renders_millisecond_precision_literal(self, value, expected):
        assert AthenaTimestamp.process(value) == expected

    def test_process_falls_back_to_str(self):
        assert (
            AthenaTimestamp.process("2017-01-01 12:34:56.789")
            == "TIMESTAMP '2017-01-01 12:34:56.789'"
        )
