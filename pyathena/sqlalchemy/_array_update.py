from __future__ import annotations

from typing import Any

from sqlalchemy import exc, types, util
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import BinaryExpression, BindParameter, ColumnElement, Null, Slice
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.visitors import InternalTraversal

from pyathena.formatter import _ComplexParameter
from pyathena.sqlalchemy.types import _array_item_type, _bind_complex, _literal_complex


class _AssignmentType(types.TypeDecorator[Any]):
    impl = types.NullType
    cache_ok = True

    def __init__(self, item_type):
        super().__init__()
        self.item_type = item_type

    def bind_processor(self, dialect):
        def process(value):
            value = _bind_complex(value, self.item_type, dialect)
            if isinstance(value, (bytes, bytearray)):
                return _ComplexParameter("FROM_HEX", (value.hex(),))
            return value

        return process

    def literal_processor(self, dialect):
        return lambda value: _literal_complex(value, self.item_type, dialect)


class _IndexType(types.TypeDecorator[int]):
    impl = types.Integer
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if type(value) is not int:
            raise ValueError("ARRAY write indices must be non-NULL integers")
        return value

    def process_literal_param(self, value, dialect):
        return self.process_bind_param(value, dialect)


class _ArrayUpdate(ColumnElement[Any]):
    __visit_name__ = "athena_array_update"
    inherit_cache = True
    _traverse_internals = [  # noqa: RUF012
        ("column", InternalTraversal.dp_clauseelement),
        ("path", InternalTraversal.dp_clauseelement_list),
        ("value", InternalTraversal.dp_clauseelement),
        ("type", InternalTraversal.dp_type),
    ]

    def __init__(self, column, path, value, value_type):
        self.column = column
        self.path = path
        self.type = column.type
        self.value_type = value_type
        self.value = (
            value._with_binary_element_type(_AssignmentType(value_type))
            if isinstance(value, BindParameter)
            else value
        )

    @property
    def _from_objects(self):
        return self.column._from_objects + self.value._from_objects


def rewrite_array_update(statement):
    values = statement._ordered_values
    if values is None:
        values = list((statement._values or {}).items())
    rewritten = []
    seen = set()
    partial = set()
    for key, value in values:
        base = key
        path: list[Any] = []
        while isinstance(base, BinaryExpression) and base.operator is operators.getitem:
            if not isinstance(base.left.type, types.ARRAY):
                break
            path.insert(0, base.right)
            base = base.left
        name = base if isinstance(base, str) else getattr(base, "key", None)
        if path:
            if not isinstance(base, Column) or base.table is not statement.table:
                raise exc.CompileError("ARRAY updates require a column of the target table")
            if name in seen:
                raise exc.CompileError("Only one assignment per ARRAY column is supported")
            if any(isinstance(index, Slice) for index in path[:-1]):
                raise exc.CompileError("Only the final ARRAY update index can be a slice")
            partial.add(name)
            value_type = key.type
            value = _ArrayUpdate(base, path, value, value_type)
            key = base
        elif name in partial:
            raise exc.CompileError("Only one assignment per ARRAY column is supported")
        seen.add(name)
        rewritten.append((key, value))
    if not partial:
        return statement
    result = statement._clone()
    if statement._ordered_values is not None:
        result._ordered_values = rewritten
    else:
        result._values = util.immutabledict(rewritten)
    return result


def _index_sql(compiler, index: ColumnElement[Any], **kw):
    if isinstance(index, Null):
        raise exc.CompileError("ARRAY write indices must be non-NULL positive integers")
    if (
        isinstance(index, BindParameter)
        and not index.required
        and (type(index.value) is not int or index.value <= 0)
    ):
        raise exc.CompileError("ARRAY write indices must be positive integers after normalization")
    if not isinstance(index.type, (types.Integer, types.NullType, types.ARRAY)):
        raise exc.CompileError("ARRAY write indices must be integers")

    if isinstance(index, BindParameter):
        index = index._with_binary_element_type(_IndexType())
    sql = compiler.process(index, **kw)
    failure = (
        f"CAST(concat('Invalid ARRAY index: ', coalesce(CAST({sql} AS VARCHAR), 'NULL')) AS BIGINT)"
    )
    return f"IF({sql} > 0, {sql}, {failure})"


def compile_array_update(compiler, expression, **kw):
    value = expression.value
    final_slice = isinstance(expression.path[-1], Slice)
    if final_slice and (
        isinstance(value, Null)
        or (isinstance(value, BindParameter) and not value.required and value.value is None)
    ):
        raise exc.CompileError("An ARRAY slice assignment requires a non-NULL array")
    rhs = compiler.process(value, **kw)
    rhs_type = compiler._complex_dml_type(expression.value_type)
    rhs = f"CAST({rhs} AS {rhs_type})"
    if final_slice:
        # Reject SQL expressions that evaluate to NULL without issuing a second statement.
        failure = (
            f"slice(CAST(ARRAY[] AS {rhs_type}), "
            "CAST(concat('NULL ARRAY slice assignment', coalesce(CAST(cardinality("
            f"{rhs}) AS VARCHAR), '')) AS BIGINT), 0)"
        )
        rhs = f"IF({rhs} IS NULL, {failure}, {rhs})"

    def rebuild(array, array_type, path):
        array_sql_type = compiler._complex_dml_type(array_type)
        array = f"coalesce({array}, CAST(ARRAY[] AS {array_sql_type}))"
        bound = path[0]
        if isinstance(bound, Slice):
            if not isinstance(bound.step, Null) and not (
                isinstance(bound.step, BindParameter)
                and bound.step.unique
                and type(bound.step.value) is int
                and bound.step.value == 1
            ):
                raise exc.CompileError("Athena ARRAY slices support only step=None or step=1")
            start = (
                "1" if isinstance(bound.start, Null) else _index_sql(compiler, bound.start, **kw)
            )
            stop = (
                f"cardinality({array})"
                if isinstance(bound.stop, Null)
                else _index_sql(compiler, bound.stop, **kw)
            )
            prefix = f"slice({array}, 1, least({start} - 1, cardinality({array})))"
            element_type = compiler._complex_dml_type(_array_item_type(array_type))
            padding = (
                f"repeat(CAST(NULL AS {element_type}), "
                f"CAST(greatest({start} - 1 - cardinality({array}), 0) AS INTEGER))"
            )
            tail_start = f"greatest({start}, {stop} + 1)"
            suffix = (
                f"slice({array}, {tail_start}, "
                f"greatest(cardinality({array}) - {tail_start} + 1, 0))"
            )
            return compiler._array_slice_step(
                f"concat({prefix}, {padding}, {rhs}, {suffix})", bound.step, **kw
            )
        index = _index_sql(compiler, bound, **kw)
        variable = compiler._array_lambda_name()
        previous = f"element_at({array}, {index})"
        replacement = (
            rebuild(previous, _array_item_type(array_type), path[1:]) if len(path) > 1 else rhs
        )
        return (
            f"transform(sequence(1, greatest(cardinality({array}), {index})), "
            f"{variable} -> IF({variable} = {index}, {replacement}, "
            f"element_at({array}, {variable})))"
        )

    return rebuild(compiler.process(expression.column, **kw), expression.type, expression.path)
