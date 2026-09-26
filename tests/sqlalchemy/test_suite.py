import json as _json
import logging
from datetime import date
from datetime import datetime as _datetime
from decimal import Decimal

import pytest
from botocore.exceptions import ClientError
from sqlalchemy import (
    CHAR,
    VARCHAR,
    Integer,
    MetaData,
    String,
    all_,
    any_,
    bindparam,
    cast,
    func,
    inspect,
    literal,
    literal_column,
    select,
    text,
    types,
    update,
)
from sqlalchemy import Table as SATable
from sqlalchemy import exc as sa_exc
from sqlalchemy import testing as sa_testing
from sqlalchemy.orm import Session, registry
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.testing import eq_, fixtures
from sqlalchemy.testing.schema import Column, Table
from sqlalchemy.testing.suite import *  # noqa: F403
from sqlalchemy.testing.suite import BinaryTest as _BinaryTest
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy.testing.suite import ComponentReflectionTestExtra as _ComponentReflectionTestExtra
from sqlalchemy.testing.suite import CTETest as _CTETest
from sqlalchemy.testing.suite import FetchLimitOffsetTest as _FetchLimitOffsetTest
from sqlalchemy.testing.suite import HasTableTest as _HasTableTest
from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
from sqlalchemy.testing.suite import LongNameBlowoutTest as _LongNameBlowoutTest
from sqlalchemy.testing.suite import QuotedNameArgumentTest as _QuotedNameArgumentTest
from sqlalchemy.testing.suite import SimpleUpdateDeleteTest as _SimpleUpdateDeleteTest
from sqlalchemy.testing.suite import StringTest as _StringTest

from pyathena.error import OperationalError
from pyathena.sqlalchemy.types import (
    AthenaArray,
    AthenaDate,
    AthenaMap,
    AthenaStruct,
    AthenaTimestamp,
)


def _raw_connection(connection):
    """Return the PyAthena connection behind a SQLAlchemy connection for either dialect."""
    raw_connection = connection.connection.driver_connection
    if connection.dialect.is_async:
        raw_connection = raw_connection.driver_connection
    return raw_connection


def _metadata_error(code, message):
    return ClientError({"Error": {"Code": code, "Message": message}}, "GetTableMetadata")


def _fail_get_table_metadata(monkeypatch, raw_connection, error, attempt=1):
    """Make every GetTableMetadata call raise ``error``; return the call list.

    ``attempt`` shortens the connection's retry policy; ``None`` leaves it as is.
    """
    calls = []

    def fail_metadata(**kwargs):
        calls.append(kwargs)
        raise error

    monkeypatch.setattr(raw_connection.client, "get_table_metadata", fail_metadata)
    if attempt is not None:
        monkeypatch.setattr(raw_connection.retry_config, "attempt", attempt)
    return calls


del CompositeKeyReflectionTest  # noqa: F821
del DateTimeMicrosecondsTest  # noqa: F821
del DifficultParametersTest  # noqa: F821
del DistinctOnTest  # noqa: F821
del HasIndexTest  # noqa: F821
del IdentityAutoincrementTest  # noqa: F821
del JoinTest  # noqa: F821
del TimeMicrosecondsTest  # noqa: F821
del TimeTest  # noqa: F821
del TimestampMicrosecondsTest  # noqa: F821
del UuidTest  # noqa: F821


class BinaryTest(_BinaryTest):
    @sa_testing.combinations(types.LargeBinary, types.BINARY, types.VARBINARY, argnames="datatype")
    @sa_testing.combinations(
        ("empty", b""),
        ("special", b"\x00\xff'\\%"),
        ("all_bytes", bytes(range(256))),
        argnames="data",
        id_="ia",
    )
    def test_literal(self, literal_round_trip, datatype, data):
        literal_round_trip(datatype, [data], [data])

    def test_reflected_binary_roundtrip(self, connection):
        binary_table = self.tables.binary_table
        data = b"\x00\xff'\\%"
        connection.execute(binary_table.insert(), {"id": 1, "binary_data": data})
        reflected = SATable(
            binary_table.name,
            MetaData(),
            schema=binary_table.schema,
            autoload_with=connection,
        )
        assert isinstance(reflected.c.binary_data.type, types.BINARY)
        row = connection.execute(
            select(reflected.c.binary_data).where(reflected.c.binary_data == data)
        ).one()
        assert row == (data,)


class CTETest(_CTETest):
    @classmethod
    def define_tables(cls, metadata):
        super().define_tables(metadata)
        # The suite removes unsupported foreign keys, so parent_id cannot infer its type.
        metadata.tables["some_table"].c.parent_id.type = Integer()


class _ArrayTimestamp(types.TypeDecorator):
    impl = types.TIMESTAMP
    cache_ok = True

    def process_result_value(self, value, dialect):
        assert value is None or isinstance(value, _datetime)
        return value


class _ArrayJSONText(types.TypeDecorator):
    impl = String
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return _json.dumps(value)

    def process_result_value(self, value, dialect):
        return _json.loads(value)


class _ArrayTuple(types.TypeDecorator):
    impl = AthenaArray(Integer)
    cache_ok = True

    def process_result_value(self, value, dialect):
        return tuple(value) if value is not None else None


class ArrayUpdateTest(fixtures.TestBase):
    __backend__ = True
    __requires__ = ("array_type",)

    def test_element_resize_and_null(self, connection, metadata):
        table = Table(
            "array_element_updates",
            metadata,
            Column("id", Integer),
            Column("items", AthenaArray(Integer)),
            Column("marker", Integer),
        )
        table.create(connection)
        connection.execute(
            table.insert(),
            [
                {"id": 1, "items": [1, 2, 3]},
                {"id": 2, "items": []},
                {"id": 3, "items": None},
            ],
        )
        items = table.c["items"]
        connection.execute(
            table.update().where(table.c.id == 1).values({items[5]: 9, table.c.marker: 42})
        )
        connection.execute(table.update().where(table.c.id == 1).values({items[2]: None}))
        connection.execute(table.update().where(table.c.id > 1).values({items[2]: 7}))
        eq_(
            connection.execute(select(items, table.c.marker).order_by(table.c.id)).all(),
            [([1, None, 3, None, 9], 42), ([None, 7], None), ([None, 7], None)],
        )

    def test_slice_resize(self, connection, metadata):
        cases = [
            ([1, 2, 3], slice(2, 2), [8, 9], [1, 8, 9, 3]),
            ([1, 2, 3], slice(2, 3), [8], [1, 8]),
            ([1, 2, 3], slice(2, 3), [], [1]),
            ([1, 2, 3], slice(3, 1), [8], [1, 2, 8, 3]),
            ([1, 2, 3], slice(5, 9), [8], [1, 2, 3, None, 8]),
            ([1, 2, 3], slice(2, 9), [8], [1, 8]),
            ([1, 2, 3], slice(None), [8, 9], [8, 9]),
            ([], slice(1, 2), [], []),
            (None, slice(2, 2), [8], [None, 8]),
        ]
        table = Table(
            "array_slice_updates",
            metadata,
            Column("id", Integer),
            Column("items", AthenaArray(Integer)),
        )
        table.create(connection)
        connection.execute(
            table.insert(),
            [{"id": i, "items": before} for i, (before, _, _, _) in enumerate(cases)],
        )
        for i, (_, bounds, replacement, _) in enumerate(cases):
            connection.execute(
                table.update()
                .where(table.c.id == i)
                .values({table.c["items"][bounds]: replacement})
            )
        eq_(
            connection.execute(select(table.c["items"]).order_by(table.c.id)).scalars().all(),
            [expected for _, _, _, expected in cases],
        )

    def test_nested_zero_indexed_and_cached_bindings(self, connection, metadata):
        table = Table(
            "array_nested_updates",
            metadata,
            Column("id", Integer),
            Column("items", AthenaArray(Integer, dimensions=2, zero_indexes=True)),
        )
        table.create(connection)
        connection.execute(
            table.insert(), [{"id": 1, "items": [[1], None]}, {"id": 2, "items": None}]
        )
        items = table.c["items"]
        statement = (
            table.update()
            .where(table.c.id == bindparam("row_id"))
            .values({items[bindparam("outer")][bindparam("inner")]: bindparam("value")})
        )
        connection.execute(statement, {"row_id": 1, "outer": 1, "inner": 1, "value": 7})
        connection.execute(statement, {"row_id": 2, "outer": 0, "inner": 0, "value": 8})
        connection.execute(table.update().where(table.c.id == 1).values({items[0][:0]: [4, 5]}))
        eq_(
            connection.execute(select(items).order_by(table.c.id)).scalars().all(),
            [[[4, 5], [None, 7]], [[8]]],
        )
        with pytest.raises(sa_exc.DBAPIError):
            connection.execute(statement, {"row_id": 1, "outer": -1, "inner": 0, "value": 9})

    def test_expression_values_and_indices(self, connection, metadata):
        table = Table(
            "array_expression_updates",
            metadata,
            Column("id", Integer),
            Column("items", AthenaArray(Integer)),
            Column("binary_items", AthenaArray(types.BINARY)),
            Column("tuple_items", _ArrayTuple()),
            Column("decimal_items", AthenaArray(types.Numeric(8, 2))),
            Column("timestamp_items", AthenaArray(types.TIMESTAMP)),
        )
        table.create(connection)
        connection.execute(
            table.insert().values(
                id=1,
                items=[1, 2, 3],
                binary_items=[b"abc"],
                tuple_items=[1, 2],
                decimal_items=[Decimal("0.00")],
                timestamp_items=[
                    _datetime(2024, 1, 1, microsecond=123000),
                    _datetime(2024, 1, 2, microsecond=456000),
                ],
            )
        )
        items = table.c["items"]
        connection.execute(table.update().values({table.c.decimal_items[1]: Decimal("1.23")}))
        eq_(connection.execute(select(table.c.decimal_items)).scalar_one(), [Decimal("1.23")])
        connection.execute(
            table.update().ordered_values(
                (items[func.length("abc")], items[1] + 8),
                (table.c.binary_items[1], b"\x00\xff"),
                (table.c.tuple_items[bindparam("tuple_index")], bindparam("tuple_value")),
                (table.c.decimal_items[1], table.c.decimal_items[1] + Decimal("3.33")),
                (table.c.timestamp_items[1], table.c.timestamp_items[2]),
                (table.c.id, 2),
            ),
            {"tuple_index": 1, "tuple_value": 5},
        )
        eq_(connection.execute(select(table.c.tuple_items)).scalar_one(), (5, 2))
        connection.execute(
            table.update().values(
                {items[1:2]: items[2:3].concat([4]), table.c.tuple_items[1:1]: [6, 7]}
            )
        )
        eq_(
            connection.execute(select(table)).one(),
            (
                2,
                [2, 9, 4, 9],
                [b"\x00\xff"],
                (6, 7, 2),
                [Decimal("4.56")],
                [_datetime(2024, 1, 2, microsecond=456000)] * 2,
            ),
        )

    def test_orm_and_long_array_update(self, connection, metadata):
        table = Table(
            "array_orm_updates",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("items", AthenaArray(Integer)),
        )
        table.create(connection)
        connection.execute(
            table.insert().values(
                id=1,
                items=func.concat(func.sequence(1, 10000), literal([10001], AthenaArray(Integer))),
            )
        )
        mapping = registry()

        class Record:
            pass

        mapping.map_imperatively(Record, table)
        try:
            with Session(bind=connection) as session:
                session.execute(update(Record).where(Record.id == 1).values({Record.items[1]: 99}))
                session.flush()
            row = connection.execute(select(table.c["items"])).scalar_one()
            eq_((len(row), row[0], row[-1]), (10001, 99, 10001))
            connection.execute(
                table.update().values({table.c["items"][2]: select(literal(77)).scalar_subquery()})
            )
            eq_(connection.execute(select(table.c["items"])).scalar_one()[:3], [99, 77, 3])
        finally:
            mapping.dispose()

    def test_cached_literal_assignment_failures(self, connection, metadata):
        table = Table(
            "array_cached_invalid_updates", metadata, Column("items", AthenaArray(Integer))
        )
        table.create(connection)
        connection.execute(table.insert().values(items=[1, 2]))
        connection = connection.execution_options(compiled_cache={})
        items = table.c["items"]
        connection.execute(table.update().values({items[1]: 3}))
        with pytest.raises(sa_exc.DBAPIError, match="Invalid ARRAY index"):
            connection.execute(table.update().values({items[0]: 4}))
        eq_(connection.execute(select(items)).scalar_one(), [3, 2])
        connection.execute(table.update().values({items[1:2]: [7]}))
        with pytest.raises(sa_exc.DBAPIError, match="NULL ARRAY slice assignment"):
            connection.execute(table.update().values({items[1:2]: None}))
        eq_(connection.execute(select(items)).scalar_one(), [7])

    def test_null_slice_binding_rejected(self, connection, metadata):
        table = Table("array_null_slice", metadata, Column("items", types.ARRAY(Integer)))
        table.create(connection)
        connection.execute(table.insert().values(items=[1, 2]))
        statement = table.update().values({table.c["items"][1:2]: bindparam("replacement")})
        connection.execute(statement, {"replacement": [3]})
        with pytest.raises(sa_exc.DBAPIError):
            connection.execute(statement, {"replacement": None})
        eq_(connection.execute(select(table.c["items"])).scalar_one(), [3])


class ArrayExpressionTest(fixtures.TestBase):
    __backend__ = True
    __requires__ = ("array_type",)

    def test_decorated_and_variant_arrays(self, connection):
        array = literal([1, 2, 3], _ArrayTuple())
        variant = literal([1, 2], String().with_variant(_ArrayTuple(), "awsathena"))
        statement = select(
            array[1],
            array[1:2],
            any_(array) == 2,
            all_(array) > 0,
            array.concat([4]),
            any_(variant) == 2,
            variant,
        )
        eq_(
            tuple(connection.execute(statement).one()),
            (1, (1, 2), True, True, (1, 2, 3, 4), True, (1, 2)),
        )

    @sa_testing.combinations(
        (String(), String, ["a", "b"], "a"),
        (Integer(), Integer, [1, 2], 1),
        argnames="base_type,item_type,values,needle",
    )
    def test_variant_array_quantifier_scalar_bind(
        self, connection, base_type, item_type, values, needle
    ):
        array = literal(values, base_type.with_variant(AthenaArray(item_type), "awsathena"))
        statement = select(any_(array) == needle, all_(array) == needle)
        eq_(tuple(connection.execute(statement).one()), (True, False))
        eq_(tuple(connection.execute(statement).one()), (True, False))

    def test_cached_steps_and_boolean_quantifiers(self, connection):
        inferred = func.array_agg(func.length(literal("abc")))
        eq_(connection.execute(select(inferred[1:2:1])).scalar_one(), [3])
        array = literal([1, 2, 3], types.ARRAY(Integer))
        eq_(connection.execute(select(array[1:2:1])).scalar_one(), [1, 2])
        with pytest.raises(sa_exc.StatementError, match="step") as error:
            connection.execute(select(array[1:2:2])).all()
        assert isinstance(error.value.orig, ValueError)
        flags = literal([True, False], AthenaArray(types.Boolean))
        comparison = any_(flags) == True  # noqa: E712
        eq_(
            tuple(
                connection.execute(
                    select(comparison, all_(flags) == True, ~comparison, ~comparison.self_group())  # noqa: E712
                ).one()
            ),
            (True, False, True, False),
        )

    def test_index_slice_and_concat(self, connection):
        array = literal([1, None, 3], AthenaArray(Integer))
        empty = literal([], AthenaArray(Integer))
        missing = literal(None, AthenaArray(Integer))
        expressions = [
            array[1],
            array[2],
            array[0],
            array[-1],
            array[10],
            array[literal(None, Integer)],
            array[:],
            array[:2],
            array[2:],
            array[-2:100],
            array[3:1],
            empty[1:3],
            missing[1:3],
            array.concat([4]),
            array[1:2].concat(array[3:]),
        ]
        eq_(
            tuple(connection.execute(select(*expressions)).one()),
            (
                1,
                None,
                None,
                None,
                None,
                None,
                [1, None, 3],
                [1, None],
                [None, 3],
                [1, None, 3],
                [],
                [],
                None,
                [1, None, 3, 4],
                [1, None, 3],
            ),
        )

    def test_dimensions_zero_indexes_and_bound_index(self, connection):
        array = literal([[1, 2], [3]], AthenaArray(Integer, dimensions=2, zero_indexes=True))
        statement = select(array[bindparam("index")], array[:0], array[0][1], array[1:])
        eq_(tuple(connection.execute(statement, {"index": 0}).one()), ([1, 2], [[1, 2]], 2, [[3]]))
        eq_(tuple(connection.execute(statement, {"index": 1}).one()), ([3], [[1, 2]], 2, [[3]]))
        eq_(connection.execute(select(array.any([1, 2]))).scalar_one(), True)
        plain = literal([1, 2], types.ARRAY(Integer))
        eq_(connection.execute(select(plain[bindparam("index")]), {"index": 2}).scalar_one(), 2)
        eq_(
            connection.execute(
                select(literal([1, 2], AthenaArray(Integer)) == any_(func.array_agg(plain)))
            ).scalar_one(),
            True,
        )

    def test_quantified_comparisons(self, connection):
        cases = [
            ([1, 2, None], True, False, False, False),
            ([1, None], None, False, False, None),
            ([2, None], True, None, False, False),
            ([3, None], None, False, None, None),
            ([1, 3], False, False, False, True),
            ([], False, True, True, True),
            (None, None, None, None, None),
        ]
        for values, eq_any, eq_all, lt_all, neg_any in cases:
            array = literal(values, AthenaArray(Integer))
            row = connection.execute(
                select(
                    any_(array) == 2,
                    all_(array) == 2,
                    all_(array) > 2,
                    ~array.any(2),
                    any_(array) == None,  # noqa: E711
                )
            ).one()
            eq_(
                tuple(row),
                (
                    eq_any,
                    eq_all,
                    lt_all,
                    neg_any if eq_any is not None else None,
                    False if values == [] else None,
                ),
            )

    def test_where_and_lambda_names(self, connection, metadata):
        table = Table(
            "array_expressions",
            metadata,
            Column("id", Integer),
            Column("items", AthenaArray(Integer)),
            Column("_pyathena_element_0", Integer),
        )
        table.create(connection)
        connection.execute(
            table.insert(),
            [
                {"id": 1, "items": [1, 3], "_pyathena_element_0": 3},
                {"id": 2, "items": [2, 4], "_pyathena_element_0": 1},
            ],
        )
        predicate = (table.c._pyathena_element_0 == any_(table.c["items"])) & (
            table.c["items"][1] == 1
        )
        eq_(connection.execute(select(table.c.id).where(predicate)).scalars().all(), [1])
        textual_predicate = literal_column("_pyathena_element_0 + 1") == any_(table.c["items"])
        eq_(connection.execute(select(table.c.id).where(textual_predicate)).scalars().all(), [2])


class NativeArrayTest(fixtures.TestBase):
    __backend__ = True
    __requires__ = ("array_type",)

    def test_native_ordering(self, connection, metadata):
        table = Table(
            "native_array_order",
            metadata,
            Column("id", Integer),
            Column("value", AthenaArray(Integer)),
        )
        table.create(connection)
        connection.execute(
            table.insert(),
            [{"id": 1, "value": [10]}, {"id": 2, "value": [2]}, {"id": 3, "value": [2]}],
        )
        value = table.c.value.label("items")
        for ordering in (value, "items", text("items")):
            stmt = select(value).distinct().order_by(ordering)
            eq_(connection.execute(stmt).scalars().all(), [[2], [10]])
        eq_(
            connection.execute(select(value).order_by(table.c.id.desc()).limit(2)).scalars().all(),
            [[2], [2]],
        )
        union = (
            select(table.c.value)
            .where(table.c.id == 1)
            .union_all(select(table.c.value).where(table.c.id == 2))
            .order_by("value")
        )
        eq_(connection.execute(union).scalars().all(), [[2], [10]])
        eq_(
            connection.execute(select(value, table.c.id).order_by(text("items DESC, id"))).all(),
            [([10], 1), ([2], 2), ([2], 3)],
        )
        eq_(
            connection.execute(select(value).order_by("id")).scalars().all(),
            [[10], [2], [2]],
        )
        eq_(
            connection.execute(select(table.c.value).order_by("native_array_order_value"))
            .scalars()
            .all(),
            [[2], [2], [10]],
        )

        eq_(
            connection.execute(
                select(literal_column("cardinality(value)").label("size"), value).order_by(
                    table.c.id
                )
            ).all(),
            [(1, [10]), (1, [2]), (1, [2])],
        )

    def test_decorated_array_ordering(self, connection):
        values = select(literal([10], _ArrayTuple()).label("items")).union_all(
            select(literal([2], _ArrayTuple()).label("items"))
        )
        eq_(connection.execute(values.order_by("items")).scalars().all(), [(2,), (10,)])
        source = values.subquery()
        statement = select(source.c["items"]).distinct().order_by("items")
        eq_(connection.execute(statement).scalars().all(), [(2,), (10,)])

    def test_athena_temporal_elements(self, connection):
        for item_type, value in (
            (AthenaDate(), date(2025, 1, 2)),
            (AthenaTimestamp(), _datetime(2025, 1, 2, 3, 4, 5)),
        ):
            for literal_execute in (False, True):
                statement = select(
                    literal([value], AthenaArray(item_type), literal_execute=literal_execute)
                )
                eq_(connection.execute(statement).scalar_one(), [value])

    def test_review_regressions(self, connection):
        decimal_value = literal([Decimal("1.50")], AthenaArray(types.Numeric(10, 2)))
        eq_(
            connection.execute(
                select(cast(decimal_value, AthenaArray(types.Numeric())))
            ).scalar_one(),
            [Decimal("2")],
        )
        expressions = [
            literal(["a-very-long-string"], AthenaArray(String(3))),
            literal([0.1], AthenaArray(types.Double)),
            literal([0.1], AthenaArray(types.DOUBLE_PRECISION)),
            func.array_agg(func.length(literal("abc"))),
        ]
        # Aggregate and scalar expressions are checked separately for Athena grouping rules.
        eq_(
            tuple(connection.execute(select(*expressions[:3])).one()),
            (["a-very-long-string"], [0.1], [0.1]),
        )
        eq_(connection.execute(select(expressions[3])).scalar_one(), [3])
        custom_values = [
            (AthenaArray(_ArrayTimestamp()), [_datetime(2025, 1, 2, 3, 4, 5)]),
            (AthenaArray(_ArrayJSONText()), [{"nested": [1, 2], "fraction": 0.1}]),
        ]
        for type_, value in custom_values:
            for literal_execute in (False, True):
                eq_(
                    connection.execute(
                        select(literal(value, type_, literal_execute=literal_execute))
                    ).scalar_one(),
                    value,
                )

    def test_reflection_and_executemany(self, connection, metadata):
        table = Table(
            "native_array_values",
            metadata,
            Column("id", Integer),
            Column("numbers", types.ARRAY(Integer)),
            Column("labels", types.ARRAY(String, dimensions=2)),
            Column("amounts", AthenaArray(types.Numeric(30, 20))),
        )
        table.create(connection)
        values = [
            {
                "id": 1,
                "numbers": [1, None, 3],
                "labels": [["001", "null", "a,b", ""], ["thr'ee", "réve🐍 illé"]],
                "amounts": [Decimal("0.12345678901234567890")],
            },
            {"id": 2, "numbers": [], "labels": [[], None], "amounts": []},
            {"id": 3, "numbers": None, "labels": None, "amounts": None},
        ]
        connection.execute(table.insert(), values)
        reflected = Table(table.name, MetaData(), autoload_with=connection)
        assert isinstance(reflected.c.numbers.type, types.ARRAY)
        assert isinstance(reflected.c.numbers.type.item_type, types.Integer)
        assert isinstance(reflected.c.labels.type.item_type, types.ARRAY)
        assert reflected.c.amounts.type.item_type.precision == 30
        assert reflected.c.amounts.type.item_type.scale == 20
        for source in (table, reflected):
            rows = connection.execute(select(source).order_by(source.c.id)).mappings().all()
            eq_([dict(row) for row in rows], values)
        result = connection.execute(select(table.c.labels).where(table.c.id == 1))
        cursor = getattr(result.context.cursor, "_cursor", result.context.cursor)
        assert cursor.effective_engine_version == "Athena engine version 3"

    @sa_testing.combinations(False, True, argnames="literal_execute")
    def test_typed_scalar_and_complex_elements(self, connection, literal_execute):
        cases = [
            (AthenaArray(String), ["100%", "%(param_1)s", "back\\slash", "line\nbreak", "null"]),
            (AthenaArray(types.Boolean), [True, False, None]),
            (AthenaArray(types.Date), [date(2025, 1, 2), None]),
            (AthenaArray(types.DateTime), [_datetime(2025, 1, 2, 3, 4, 5, 123000)]),
            (AthenaArray(types.BINARY), [b"\x00\xff", b"", None]),
            (AthenaArray(AthenaMap(Integer, String)), [{1: "001", 2: "a,b"}, {}, None]),
            (
                AthenaArray(AthenaStruct(("name", String), ("n", Integer))),
                [{"name": "a,b", "n": 2}, None],
            ),
            (AthenaArray(Integer, as_tuple=True), (1, None, 3)),
            (AthenaArray(types.JSON), [{"n": 1, "s": "a,b", "fraction": 0.1}, [1, None], None]),
        ]
        expressions = [
            literal(value, type_=type_, literal_execute=literal_execute).label(f"v{index}")
            for index, (type_, value) in enumerate(cases)
        ]
        row = connection.execute(select(*expressions)).one()
        eq_(tuple(row), tuple(value for _, value in cases))

    def test_nested_complex_reflection(self, connection, metadata):
        table = Table(
            "native_array_complex",
            metadata,
            Column(
                "value",
                AthenaArray(
                    AthenaStruct(
                        ("label", String),
                        ("numbers", AthenaArray(Integer)),
                        ("amount", types.Numeric(12, 3)),
                    )
                ),
            ),
        )
        table.create(connection)
        value = [{"label": "001,a", "numbers": [1, None], "amount": Decimal("12.340")}]
        connection.execute(table.insert().values(value=value))
        reflected = Table(table.name, MetaData(), autoload_with=connection)
        item = reflected.c.value.type.item_type
        assert isinstance(item, AthenaStruct)
        assert isinstance(item.fields["numbers"], types.ARRAY)
        assert item.fields["amount"].scale == 3
        eq_(connection.execute(select(reflected.c.value)).scalar_one(), value)


class SimpleUpdateDeleteTest(_SimpleUpdateDeleteTest):
    @sa_testing.variation("criteria", ["rows", "norows", "aggregate"])
    @sa_testing.requires.update_where_target_in_subquery
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


class ComponentReflectionTest(_ComponentReflectionTest):
    @classmethod
    def define_reflected_tables(cls, metadata, schema):
        super().define_reflected_tables(metadata, schema)
        # Iceberg has STRING but does not support CHAR.
        key = f"{schema}.users" if schema else "users"
        metadata.tables[key].c.test1.type = String()
        for table in metadata.tables.values():
            # The upstream fixture unconditionally indexes email_address.
            table.indexes.clear()
        for name in ("comment_test", "no_constraints"):
            key = f"{schema}.{name}" if schema else name
            # Athena does not persist Iceberg table comments. Keep the other
            # reflection fixtures on the suite's default Iceberg table type.
            options = metadata.tables[key].dialect_options["awsathena"]
            options["tblproperties"] = {"classification": "parquet"}
            options["file_format"] = "PARQUET"
        key = f"{schema}.comment_test" if schema else "comment_test"
        # Glue rejects newline and carriage return characters in Hive column comments.
        metadata.tables[key].c.d3.comment = "Comment with escapes"

    def test_get_comments(self, connection):
        self._test_get_comments(connection)

    @sa_testing.requires.schemas
    def test_get_comments_with_schema(self, connection):
        self._test_get_comments(connection, sa_testing.config.test_schema)

    @sa_testing.combinations((True, sa_testing.requires.schemas), False, argnames="use_schema")
    def test_get_hive_multi_table_comment(self, connection, use_schema):
        schema = sa_testing.config.test_schema if use_schema else None
        names = ["comment_test", "no_constraints", "users"]
        expected = self.exp_comments(schema=schema)
        assert inspect(connection).get_multi_table_comment(schema=schema, filter_names=names) == {
            (schema, name): expected[(schema, name)] for name in names
        }

    def exp_columns(self, *args, **kwargs):
        columns = super().exp_columns(*args, **kwargs)
        for table_columns in columns.values():
            for column in table_columns:
                # Athena DDL does not create NOT NULL or auto-increment constraints.
                column["nullable"] = True
                column["autoincrement"] = False
                if column["name"] == "d3":
                    column["comment"] = "Comment with escapes"
        return columns

    @sa_testing.requires.autoincrement_insert
    def test_autoincrement_col(self, connection):
        super().test_autoincrement_col(connection)


class ComponentReflectionTestExtra(_ComponentReflectionTestExtra):
    @sa_testing.combinations(True, False, argnames="list_first")
    @sa_testing.combinations(True, False, argnames="cursor_catalog")
    def test_reuses_table_metadata(
        self, connection, metadata, monkeypatch, caplog, list_first, cursor_catalog
    ):
        table = Table("listed_metadata", metadata, Column("id", Integer, comment="identifier"))
        table.create(connection)
        inspector = inspect(connection)
        raw_connection = _raw_connection(connection)
        caplog.set_level(logging.WARNING, logger="pyathena.sqlalchemy.base")
        if cursor_catalog:
            monkeypatch.setitem(
                raw_connection.cursor_kwargs, "catalog_name", raw_connection.catalog_name
            )
            monkeypatch.setattr(raw_connection, "catalog_name", None)
        client = raw_connection.client
        calls = []

        def record_call(model, **kwargs):
            calls.append(model.name)

        client.meta.events.register("before-call.athena", record_call)
        try:
            schema = raw_connection.schema_name
            if list_first:
                assert table.name in inspector.get_table_names()
                listed_calls = list(calls)
                assert table.name in inspector.get_table_names(schema=schema)
                assert table.name not in inspector.get_view_names(schema=schema)
                assert calls == listed_calls
            assert inspector.get_columns(table.name)[0]["comment"] == "identifier"
            if any("information_schema" in record.getMessage() for record in caplog.records):
                # The request counts below assume metadata served by the API.
                pytest.skip("table metadata was throttled; columns came from information_schema")
            initial_calls = list(calls)
            assert inspector.get_columns(table.name, schema=schema)[0]["name"] == "id"
            assert inspector.get_table_options(table.name, schema=schema)["awsathena_location"]
            assert inspector.get_table_comment(table.name, schema=schema) == {"text": None}
            assert inspector.has_table(table.name.upper(), schema=schema)
            assert calls == initial_calls
            if list_first:
                assert "ListTableMetadata" in calls
                assert "GetTableMetadata" not in calls
                assert (
                    inspector.get_multi_columns(schema=schema, filter_names=[table.name])[
                        (schema, table.name)
                    ][0]["name"]
                    == "id"
                )
                assert calls == initial_calls
            calls.clear()
            inspector.clear_cache()
            assert inspector.get_columns(table.name)[0]["name"] == "id"
            # A fresh lookup may retry when Athena throttles metadata requests.
            metadata_calls = calls.count("GetTableMetadata")
            assert metadata_calls > 0
            assert inspector.get_columns(table.name)[0]["name"] == "id"
            assert calls.count("GetTableMetadata") == metadata_calls
            if list_first:
                assert table.name in inspector.get_table_names(schema=schema)
                assert "ListTableMetadata" in calls
        finally:
            client.meta.events.unregister("before-call.athena", record_call)

    def test_preserves_table_metadata_until_clear_cache(self, connection, metadata):
        table = Table("cached_metadata", metadata, Column("id", Integer))
        table.create(connection)
        inspector = inspect(connection)
        assert [column["name"] for column in inspector.get_columns(table.name)] == ["id"]
        table_name = connection.dialect.identifier_preparer.format_table(table)
        connection.exec_driver_sql(f"ALTER TABLE {table_name} ADD COLUMNS (added string)")
        assert [column["name"] for column in inspect(connection).get_columns(table.name)] == [
            "id",
            "added",
        ]
        assert table.name in inspector.get_table_names()
        schema = connection.connection.schema_name
        assert [column["name"] for column in inspector.get_columns(table.name, schema=schema)] == [
            "id"
        ]
        inspector.clear_cache()
        assert [column["name"] for column in inspector.get_columns(table.name)] == ["id", "added"]

    @sa_testing.combinations((String, None), (VARCHAR, 52), (CHAR, 52), argnames="type_,length")
    @sa_testing.combinations(False, True, argnames="information_schema")
    def test_hive_string_length_reflection(
        self, connection, metadata, type_, length, information_schema
    ):
        table = Table(
            "string_length",
            metadata,
            Column("data", type_(52)),
            awsathena_tblproperties={"classification": "parquet"},
            awsathena_file_format="PARQUET",
        )
        table.create(connection)
        if information_schema:
            # Exercise the fallback with a real query without forcing an API failure.
            dialect = connection.dialect
            raw_connection = dialect._raw_connection(connection)
            columns = dialect._columns_from_information_schema(
                raw_connection, raw_connection.schema_name, table.name
            )
        else:
            columns = inspect(connection).get_columns(table.name)
        reflected_type = columns[0]["type"]
        assert type(reflected_type) is type_
        # Generic String compiles to Hive STRING without a length constraint.
        assert reflected_type.length == length

    def test_hive_comments_unicode(self, connection, metadata):
        table = Table(
            "unicode_comments",
            metadata,
            Column("data", Integer, comment="é試蛇ẟΩ✨"),
            comment="試蛇ẟΩ✨",
            awsathena_tblproperties={"classification": "parquet"},
            awsathena_file_format="PARQUET",
        )
        table.create(connection)
        inspector = inspect(connection)
        assert inspector.get_table_comment(table.name) == {"text": table.comment}
        assert inspector.get_columns(table.name)[0]["comment"] == table.c.data.comment

    @pytest.mark.skip("Iceberg strings do not preserve CHAR or VARCHAR length constraints.")
    def test_string_length_reflection(self, connection, metadata, type_):
        pass

    @pytest.mark.skip("Athena CREATE TABLE does not support NOT NULL constraints.")
    def test_nullable_reflection(self, connection, metadata):
        pass


@pytest.mark.skip("Athena table names do not support spaces or embedded quote characters.")
class QuotedNameArgumentTest(_QuotedNameArgumentTest):
    pass


class LongNameBlowoutTest(_LongNameBlowoutTest):
    @sa_testing.combinations(
        ("fk", sa_testing.requires.foreign_key_ddl),
        ("pk", sa_testing.requires.primary_key_constraint_reflection),
        ("ix", sa_testing.requires.index_reflection),
        ("ck", sa_testing.requires.check_constraint_reflection),
        ("uq", sa_testing.requires.unique_constraint_reflection),
        argnames="type_",
    )
    def test_long_convention_name(self, type_, metadata, connection):
        # Reuse the upstream fixtures without calling its parametrized wrapper.
        actual_name, reflected_name = getattr(self, type_)(metadata, connection)
        assert len(actual_name) > 255
        if reflected_name is not None:
            overlap = actual_name[: len(reflected_name)]
            if len(overlap) < len(actual_name):
                assert overlap[:-5] == reflected_name[:-5]
            else:
                assert overlap == reflected_name


class HasTableTest(_HasTableTest):
    @sa_testing.combinations(True, False, argnames="exists")
    def test_throttled_metadata_requests_use_information_schema(
        self, connection, metadata, monkeypatch, exists
    ):
        name = "throttled_columns" if exists else "throttled_missing"
        if exists:
            Table(
                name,
                metadata,
                Column("id", Integer, comment="identifier"),
                Column("label", String),
            ).create(connection)
        raw_connection = _raw_connection(connection)
        # Glue would answer a throttled lookup in this catalog first; this case
        # covers the information_schema path it falls back to without Glue.
        monkeypatch.setattr(raw_connection, "glue_metadata_fallback", False)
        # Retries are not shortened: the fallback must not wait for them.
        error = _metadata_error("ThrottlingException", "Rate exceeded")
        calls = _fail_get_table_metadata(monkeypatch, raw_connection, error, attempt=None)
        # The fallback must answer from the catalog even when result reuse is on.
        monkeypatch.setitem(raw_connection.cursor_kwargs, "result_reuse_enable", True)
        queries = []

        def record_query(params, **kwargs):
            queries.append(params)

        event = "provide-client-params.athena.StartQueryExecution"
        raw_connection.client.meta.events.register(event, record_query)
        try:
            inspector = inspect(connection)
            assert inspector.has_table(name) is exists
            assert inspector.has_table(name.upper(), schema=raw_connection.schema_name) is exists
            if exists:
                columns = inspector.get_columns(name)
                assert [column["name"] for column in columns] == ["id", "label"]
                assert columns[0]["comment"] == "identifier"
                assert isinstance(columns[0]["type"], Integer)
                assert isinstance(columns[1]["type"], String)
                assert all(
                    column["dialect_options"]["awsathena_partition"] is None for column in columns
                )
                # A later listing seeds full metadata but does not replace the
                # fallback columns; clear_cache() does. A new argument
                # combination bypasses reflection.cache and reaches the dialect.
                assert name in inspector.get_table_names()
                assert inspector.get_columns(name, schema=raw_connection.schema_name) is columns
                inspector.clear_cache()
                assert inspector.get_columns(name) is not columns
            else:
                with pytest.raises(sa_exc.NoSuchTableError):
                    inspector.get_columns(name)
        finally:
            raw_connection.client.meta.events.unregister(event, record_query)
        # One metadata attempt, then one information_schema query per lookup.
        # Reflected columns are reused by case and schema variants; absence is not.
        assert len(calls) == (2 if exists else 3)
        assert len(queries) == len(calls)
        for query in queries:
            assert "FROM information_schema.columns" in query["QueryString"]
            assert "WHERE table_schema = " in query["QueryString"]
            assert "lower(" not in query["QueryString"]
            reuse = query["ResultReuseConfiguration"]["ResultReuseByAgeConfiguration"]
            assert reuse["Enabled"] is False
        # Table options need the metadata API and still report the throttled request.
        monkeypatch.setattr(raw_connection.retry_config, "attempt", 1)
        with pytest.raises(OperationalError) as caught:
            inspector.get_table_options(name)
        assert caught.value.__cause__ is error

    @sa_testing.combinations(
        "AccessDeniedException", "InternalServerException", None, argnames="code"
    )
    def test_metadata_errors_do_not_establish_absence(self, connection, monkeypatch, code):
        # This suite runs against the Glue Data Catalog, which states missing
        # tables and permission failures in an envelope this client recognizes.
        # An unrecognized message there (code None) has an unknown cause, so it
        # propagates rather than being re-asked of information_schema, which
        # filters by Lake Formation instead of erroring and would report a table
        # the caller cannot see as absent. Outside Glue the fallback does answer
        # it; that case has no catalog here and is covered by
        # TestAthenaDialect::test_unrecognized_metadata_error_asks_information_schema.
        raw_connection = _raw_connection(connection)
        retried = code == "InternalServerException"
        if retried:
            # A code the retry policy covers is retried with that policy, then
            # propagates; it does not reach the information_schema fallback.
            monkeypatch.setattr(
                raw_connection.retry_config, "exceptions", ("ThrottlingException", code)
            )
            monkeypatch.setattr(raw_connection.retry_config, "multiplier", 0)
        message = (
            "Catalog error (Service: AmazonDataCatalog; Status Code: 400; "
            f"Error Code: {code}; Request ID: example; Proxy: null)"
            if code
            else "is not authorized to perform: glue:GetTable"
        )
        error = _metadata_error("MetadataException", message)
        calls = _fail_get_table_metadata(
            monkeypatch, raw_connection, error, attempt=2 if retried else 1
        )
        inspector = inspect(connection)
        for _ in range(2):
            with pytest.raises(OperationalError) as caught:
                inspector.has_table("unavailable_metadata")
            assert caught.value.__cause__ is error
        assert len(calls) == (4 if retried else 2)

    @sa_testing.combinations((True, sa_testing.requires.schemas), False, argnames="use_schema")
    def test_has_table_cache_drop(self, connection, metadata, use_schema):
        schema = sa_testing.config.test_schema if use_schema else None
        table = Table("cache_drop", metadata, Column("id", Integer), schema=schema)
        table.create(connection)
        inspector = inspect(connection)
        assert inspector.has_table(table.name, schema=schema)

        table.drop(connection)
        assert inspector.has_table(table.name, schema=schema)
        assert not connection.dialect.has_table(connection, table.name, schema=schema)
        assert not inspect(connection).has_table(table.name, schema=schema)
        inspector.clear_cache()
        assert not inspector.has_table(table.name, schema=schema)

        table.create(connection)
        assert not inspector.has_table(table.name, schema=schema)
        assert connection.dialect.has_table(connection, table.name, schema=schema)
        assert inspect(connection).has_table(table.name, schema=schema)
        inspector.clear_cache()
        assert inspector.has_table(table.name, schema=schema)

    @sa_testing.requires.schemas
    @sa_testing.combinations(12, 129, argnames="length")
    def test_has_table_cache_schema(self, connection, metadata, length):
        table = Table("cache_schema".ljust(length, "t"), metadata, Column("id", Integer))
        other = Table(
            table.name,
            metadata,
            Column("id", Integer),
            schema=sa_testing.config.test_schema,
        )
        table.create(connection)
        inspector = inspect(connection)
        assert inspector.has_table(table.name)
        assert not inspector.has_table(other.name, schema=other.schema)
        other.create(connection)
        assert not inspector.has_table(other.name, schema=other.schema)
        inspector.clear_cache()
        assert inspector.has_table(other.name, schema=other.schema)


class IdentifierReflectionTest(fixtures.TestBase):
    @sa_testing.combinations("select", "_reflection", "quoted_lowercase", argnames="name")
    def test_quoted_identifier(self, connection, metadata, name):
        table = Table(
            name,
            metadata,
            Column("from", Integer, quote=True, comment="quoted column"),
            quote=True,
            comment="quoted table",
        )
        table.create(connection)
        connection.execute(table.insert().values({"from": 1}))
        assert connection.execute(select(table.c["from"])).scalar_one() == 1
        inspector = inspect(connection)
        assert inspector.has_table(table.name)
        columns = inspector.get_columns(table.name)
        assert [column["name"] for column in columns] == ["from"]
        assert columns[0]["comment"] == "quoted column"
        # Athena persists Iceberg column comments, but not table comments.
        assert inspector.get_table_comment(table.name) == {"text": None}
        assert inspector.get_table_options(table.name)["awsathena_location"]

    @sa_testing.combinations(128, 129, 255, argnames="length")
    def test_identifier_length(self, connection, metadata, caplog, length):
        table = Table("t" * length, metadata, Column("c" * 255, Integer))
        inspector = inspect(connection)
        assert not inspector.has_table(table.name)
        missing_schema = f"{sa_testing.config.test_schema}_missing"
        caplog.clear()
        assert not inspector.has_table(table.name, schema=missing_schema)
        with pytest.raises(sa_exc.NoSuchTableError):
            inspector.get_columns(table.name, schema=missing_schema)
        assert not [record for record in caplog.records if record.levelno >= logging.ERROR]
        table.create(connection)
        connection.execute(table.insert().values({"c" * 255: 1}))
        assert connection.execute(select(table)).scalar_one() == 1
        assert not inspector.has_table(table.name)
        inspector.clear_cache()
        assert inspector.has_table(table.name)
        assert inspector.get_columns(table.name)[0]["name"] == "c" * 255
        uppercase_name = quoted_name(str(table.name).upper(), quote=True)
        assert inspector.has_table(uppercase_name)
        assert inspector.get_columns(uppercase_name)[0]["name"] == "c" * 255
        table.drop(connection)
        assert inspector.has_table(table.name)
        inspector.clear_cache()
        assert not inspector.has_table(table.name)
        with pytest.raises(sa_exc.NoSuchTableError):
            inspector.get_columns(table.name)

        too_long = Table("t" * 256, metadata, Column("id", Integer))
        try:
            with pytest.raises(sa_exc.IdentifierError):
                too_long.create(connection)
        finally:
            # The rejected table must not be visited by teardown.
            metadata.remove(too_long)


class InsertBehaviorTest(_InsertBehaviorTest):
    @pytest.mark.skip("Athena does not support auto-incrementing.")
    def test_insert_from_select_autoinc(self, connection):
        pass

    @pytest.mark.skip("Athena does not support auto-incrementing.")
    def test_insert_from_select_autoinc_no_rows(self, connection):
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


class StringTest(_StringTest):
    @pytest.mark.skip("TODO")
    def test_dont_truncate_rightside(self, metadata, connection, expr, expected):
        # TODO
        pass
