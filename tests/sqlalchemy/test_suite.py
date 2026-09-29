import pytest
from sqlalchemy import func, select, testing
from sqlalchemy.testing import eq_
from sqlalchemy.testing.suite import *  # noqa: F403
from sqlalchemy.testing.suite import FetchLimitOffsetTest as _FetchLimitOffsetTest
from sqlalchemy.testing.suite import HasTableTest as _HasTableTest
from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
from sqlalchemy.testing.suite import IntegerTest as _IntegerTest
from sqlalchemy.testing.suite import SimpleUpdateDeleteTest as _SimpleUpdateDeleteTest
from sqlalchemy.testing.suite import StringTest as _StringTest

del BinaryTest  # noqa: F821
del ComponentReflectionTest  # noqa: F821
del ComponentReflectionTestExtra  # noqa: F821
del CompositeKeyReflectionTest  # noqa: F821
del CTETest  # noqa: F821
del DateTimeMicrosecondsTest  # noqa: F821
del DifficultParametersTest  # noqa: F821
del DistinctOnTest  # noqa: F821
del HasIndexTest  # noqa: F821
del IdentityAutoincrementTest  # noqa: F821
del JoinTest  # noqa: F821
del LongNameBlowoutTest  # noqa: F821
del QuotedNameArgumentTest  # noqa: F821
del RowCountTest  # noqa: F821
del TimeMicrosecondsTest  # noqa: F821
del TimeTest  # noqa: F821
del TimestampMicrosecondsTest  # noqa: F821
del UuidTest  # noqa: F821


class SimpleUpdateDeleteTest(_SimpleUpdateDeleteTest):
    @testing.variation("criteria", ["rows", "norows", "aggregate"])
    @testing.requires.update_where_target_in_subquery
    def test_update_where_target_in_subquery(self, connection, criteria):
        t = self.tables.plain_pk
        if criteria.rows:
            subquery = select(t.c.id).where(t.c.id < 3)
            expected = [(1, "updated"), (2, "updated"), (3, "d3")]
            rowcount = 2
        elif criteria.norows:
            subquery = select(t.c.id).where(t.c.id < 0)
            expected = [(1, "d1"), (2, "d2"), (3, "d3")]
            rowcount = 0
        elif criteria.aggregate:
            subquery = select(func.max(t.c.id))
            expected = [(1, "d1"), (2, "d2"), (3, "updated")]
            rowcount = 1
        else:
            criteria.fail()

        r = connection.execute(t.update().where(t.c.id.in_(subquery)), {"data": "updated"})
        assert not r.is_insert
        assert not r.returns_rows
        assert r.rowcount == rowcount
        eq_(connection.execute(t.select().order_by(t.c.id)).fetchall(), expected)


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
