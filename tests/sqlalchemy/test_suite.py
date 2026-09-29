import json as _json
from datetime import date
from datetime import datetime as _datetime
from decimal import Decimal

import pytest
from sqlalchemy import (
    Integer,
    MetaData,
    String,
    all_,
    any_,
    bindparam,
    cast,
    func,
    literal,
    literal_column,
    select,
    testing,
    text,
    types,
    update,
)
from sqlalchemy import Table as SATable
from sqlalchemy import exc as sa_exc
from sqlalchemy import testing as sa_testing
from sqlalchemy.orm import Session, registry
from sqlalchemy.testing import eq_, fixtures
from sqlalchemy.testing.schema import Column, Table
from sqlalchemy.testing.suite import *  # noqa: F403
from sqlalchemy.testing.suite import BinaryTest as _BinaryTest
from sqlalchemy.testing.suite import CTETest as _CTETest
from sqlalchemy.testing.suite import FetchLimitOffsetTest as _FetchLimitOffsetTest
from sqlalchemy.testing.suite import HasTableTest as _HasTableTest
from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
from sqlalchemy.testing.suite import IntegerTest as _IntegerTest
from sqlalchemy.testing.suite import SimpleUpdateDeleteTest as _SimpleUpdateDeleteTest
from sqlalchemy.testing.suite import StringTest as _StringTest

from pyathena.sqlalchemy.types import (
    AthenaArray,
    AthenaDate,
    AthenaMap,
    AthenaStruct,
    AthenaTimestamp,
)

del ComponentReflectionTest  # noqa: F821
del ComponentReflectionTestExtra  # noqa: F821
del CompositeKeyReflectionTest  # noqa: F821
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
