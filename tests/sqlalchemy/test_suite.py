# -*- coding: utf-8 -*-
import pytest
from sqlalchemy.testing.suite import *  # noqa
from sqlalchemy.testing.suite import FetchLimitOffsetTest as _FetchLimitOffsetTest
from sqlalchemy.testing.suite import HasTableTest as _HasTableTest
from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
from sqlalchemy.testing.suite import IntegerTest as _IntegerTest
from sqlalchemy.testing.suite import StringTest as _StringTest
from sqlalchemy.testing.suite import TrueDivTest as _TrueDivTest

del BinaryTest  # noqa
del ComponentReflectionTest  # noqa
del ComponentReflectionTestExtra  # noqa
del CompositeKeyReflectionTest  # noqa
del CTETest  # noqa
del DateTimeMicrosecondsTest  # noqa
del DifficultParametersTest  # noqa
del DistinctOnTest  # noqa
del HasIndexTest  # noqa
del IdentityAutoincrementTest  # noqa
del JoinTest  # noqa
del LongNameBlowoutTest  # noqa
del QuotedNameArgumentTest  # noqa
del RowCountTest  # noqa
del SimpleUpdateDeleteTest  # noqa
del TimeMicrosecondsTest  # noqa
del TimeTest  # noqa
del TimestampMicrosecondsTest  # noqa
del UuidTest  # noqa


class HasTableTest(_HasTableTest):
    @pytest.mark.skip("No cache is used when creating tables.")
    def test_has_table_cache(self, metadata):
        pass


class InsertBehaviorTest(_InsertBehaviorTest):
    @pytest.mark.skip("Athena does not support auto-incrementing.")
    def test_insert_from_select_autoinc(self, connection):
        pass

    @pytest.mark.skip("Athena does not support auto-incrementing.")
    def test_insert_from_select_autoinc_no_rows(self, connection):
        pass

    @pytest.mark.skip("TODO")
    def test_no_results_for_non_returning_insert(self, connection, style, executemany):
        # TODO
        pass


class TrueDivTest(_TrueDivTest):
    @pytest.mark.skip("Athena returns an integer for operations between integers.")
    def test_truediv_integer(self, connection, left, right, expected):
        pass

    @pytest.mark.skip("Athena returns an integer for operations between integers.")
    def test_truediv_integer_bound(self, connection):
        pass

    @pytest.mark.skip("TODO")
    def test_truediv_numeric(self, connection, left, right, expected):
        # TODO
        pass

    @pytest.mark.skip("TODO")
    def test_truediv_float(self, connection, left, right, expected):
        # TODO: AssertionError: 2.299999908606215 != 2.3
        pass


class FetchLimitOffsetTest(_FetchLimitOffsetTest):
    @pytest.mark.skip("Athena does not support expressions in the offset clause.")
    def test_simple_limit_expr_offset(self, connection):
        pass

    @pytest.mark.skip("Athena does not support expressions in the limit clause.")
    def test_expr_limit(self, connection):
        pass

    @pytest.mark.skip("Athena does not support expressions in the limit clause.")
    def test_expr_limit_offset(self, connection):
        pass

    @pytest.mark.skip("Athena does not support expressions in the limit clause.")
    def test_expr_limit_simple_offset(self, connection):
        pass

    @pytest.mark.skip("Athena does not support expressions in the offset clause.")
    def test_expr_offset(self, connection):
        pass

    @pytest.mark.skip("TODO")
    def test_limit_render_multiple_times(self, connection):
        # TODO
        pass


class IntegerTest(_IntegerTest):
    @pytest.mark.skip("TODO")
    def test_huge_int(self, integer_round_trip, intvalue):
        # TODO
        pass


class StringTest(_StringTest):
    @pytest.mark.skip("TODO")
    def test_dont_truncate_rightside(self, metadata, connection, expr, expected):
        # TODO
        pass
