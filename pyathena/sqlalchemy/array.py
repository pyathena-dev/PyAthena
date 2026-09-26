"""Athena ARRAY types, expressions, JSON projection, and nested value processing."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import cast, exc, types, util
from sqlalchemy.sql import operators, sqltypes
from sqlalchemy.sql.elements import BinaryExpression, BindParameter, ColumnElement, Null, Slice
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.sql.visitors import InternalTraversal

from pyathena.converter import _parse_datetime
from pyathena.formatter import _ComplexParameter
from pyathena.sqlalchemy.map import AthenaMap
from pyathena.sqlalchemy.struct import AthenaStruct
from pyathena.sqlalchemy.temporal import AthenaDate, AthenaTimestamp

# SQLAlchemy 2.0.0's ARRAY comparator is not generic at runtime.
if TYPE_CHECKING:
    _ArrayComparatorBase = sqltypes.ARRAY.Comparator[Any]
else:
    _ArrayComparatorBase = sqltypes.ARRAY.Comparator


class AthenaArray(sqltypes.ARRAY[Any]):
    """SQLAlchemy type for Athena ARRAY complex type.

    ARRAY represents an ordered collection of elements of the same type.

    Args:
        item_type: SQLAlchemy type for array elements. Defaults to String.
        as_tuple: Return tuples instead of lists. Defaults to False.
        dimensions: Fixed number of array dimensions. Defaults to one dimension.
        zero_indexes: Translate zero-based SQLAlchemy indexes to one-based SQL indexes.

    Example:
        >>> from sqlalchemy import Column, Table, MetaData, types
        >>> from pyathena.sqlalchemy.types import AthenaArray
        >>> metadata = MetaData()
        >>> posts = Table('posts', metadata,
        ...     Column('tags', AthenaArray(types.String))
        ... )

    See Also:
        AWS Athena ARRAY Type:
        https://docs.aws.amazon.com/athena/latest/ug/arrays.html
    """

    __visit_name__ = "array"

    class Comparator(_ArrayComparatorBase):
        """Build array indexing expressions with inclusive SQL slice bounds."""

        def _setup_getitem(self, index):
            if isinstance(index, slice):
                if index.step is not None and (type(index.step) is not int or index.step != 1):
                    raise exc.CompileError("Athena ARRAY slices support only step=None or step=1")
                start, stop = index.start, index.stop
                if self.type.zero_indexes:
                    start = start + 1 if start is not None else None
                    stop = stop + 1 if stop is not None else None
                return operators.getitem, Slice(start, stop, None), self.type
            if self.type.zero_indexes:
                index = index + 1
            return operators.getitem, index, _ArrayTypeInspector.item_type(self.type)

    comparator_factory = Comparator

    def __init__(
        self,
        item_type: Any = None,
        as_tuple: bool = False,
        dimensions: int | None = None,
        zero_indexes: bool = False,
    ) -> None:
        if dimensions is not None and (
            isinstance(dimensions, bool) or not isinstance(dimensions, int) or dimensions < 1
        ):
            raise ValueError("ARRAY dimensions must be a positive integer.")
        item_type = item_type() if isinstance(item_type, type) else item_type
        if isinstance(item_type, sqltypes.ARRAY):
            if dimensions is not None:
                raise ValueError("Use either nested ARRAY types or dimensions, not both.")
            # Preserve the public nested AthenaArray constructor and item_type.
            super().__init__(sqltypes.String(), as_tuple, dimensions, zero_indexes)
            self.item_type = item_type
        else:
            super().__init__(item_type or sqltypes.String(), as_tuple, dimensions, zero_indexes)

    def bind_expression(self, bindvalue):
        """Cast a bound ARRAY value to its declared Athena element type."""
        # The cast also gives empty arrays and NULL-only arrays their element type.
        return cast(bindvalue, self)._annotate({"_pyathena_array_bind": True})

    def bind_processor(self, dialect):
        """Return a processor that marks native ARRAY, MAP, and ROW parameters."""
        return _ArrayValueProcessor(self, dialect).bind

    def literal_processor(self, dialect):
        """Return a processor that renders typed Athena array literals."""
        return _ArrayValueProcessor(self, dialect).literal

    def column_expression(self, colexpr):
        """Project the outer ARRAY result as JSON while retaining its Python type."""
        return (
            colexpr
            if _ArrayTypeInspector.has_unknown_element(self)
            else _ArrayJSONProjection(colexpr, self)
        )

    def result_processor(self, dialect, coltype):
        """Return a processor that restores the declared Python element types."""
        return _ArrayValueProcessor(self, dialect).result


class _ArraySliceStepType(types.TypeDecorator[int]):
    """Validate step values when SQLAlchemy reuses a generic ARRAY slice statement."""

    impl = types.Integer
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if type(value) is not int or value != 1:
            raise ValueError("Athena ARRAY slices support only step=None or step=1")
        return value

    def process_literal_param(self, value, dialect):
        return self.process_bind_param(value, dialect)


class ARRAY(AthenaArray):
    """Uppercase alias for AthenaArray type."""

    __visit_name__ = "ARRAY"


class _ArrayJSONProjection(ColumnElement[Any]):
    """SQL expression that serializes an outer SELECT's ARRAY column as JSON.

    SQLAlchemy calls ``AthenaArray.column_expression`` for result columns, so
    predicates and intermediate SELECTs keep using native ARRAY values. The
    Athena statement compiler renders this wrapper as a JSON envelope; the
    ARRAY result processor then restores its declared Python element types.

    ``type`` keeps the original column type, including an outer TypeDecorator's
    result processor. ``array_type`` describes the native ARRAY value that the
    compiler must serialize. This object represents SQL, not fetched row data.
    """

    __visit_name__ = "athena_array_json_projection"
    inherit_cache = True
    _traverse_internals = [  # noqa: RUF012
        ("element", InternalTraversal.dp_clauseelement),
        ("type", InternalTraversal.dp_type),
        ("array_type", InternalTraversal.dp_type),
    ]

    def __init__(self, element, type_):
        self.element = element
        self.type = element.type
        self.array_type = type_


def _decode_datetime(value: str) -> datetime:
    """Decode an ARRAY element as a datetime.

    Args:
        value: The element as ISO 8601 text, or as Athena TIMESTAMP text of any
            precision.

    Returns:
        The datetime. Fractional digits beyond microseconds are truncated.
    """
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        # Python 3.10 accepts only 3 or 6 fractional digits.
        return _parse_datetime(value)


class _ArrayTypeInspector:
    """Interpret nested ARRAY element types for SQL compilation and value conversion.

    Type inspection is shared by the compiler and value processors. Resolving
    a TypeDecorator uses the current dialect; dimensions and unknown elements
    can be inspected without one.
    """

    def __init__(self, dialect: Any) -> None:
        self.dialect = dialect

    def array_type(self, type_: TypeEngine[Any]) -> sqltypes.ARRAY[Any] | None:
        """Resolve the dialect's ARRAY implementation through variants and decorators."""
        implementation = type_.dialect_impl(self.dialect)
        while isinstance(implementation, types.TypeDecorator):
            implementation = self.decorator_impl(implementation)
        return implementation if isinstance(implementation, sqltypes.ARRAY) else None

    @staticmethod
    def item_type(type_: sqltypes.ARRAY[Any]) -> TypeEngine[Any]:
        if type_.dimensions is not None and type_.dimensions > 1:
            return AthenaArray(
                type_.item_type,
                as_tuple=type_.as_tuple,
                dimensions=type_.dimensions - 1,
                zero_indexes=type_.zero_indexes,
            )
        return type_.item_type

    def variant(self, type_: TypeEngine[Any]) -> TypeEngine[Any] | None:
        """Return the type's ``with_variant()`` type for this dialect.

        Args:
            type_: The declared type.

        Returns:
            The variant type, or None when the type has no variant for this dialect.
        """
        return type_._variant_mapping.get(self.dialect.name)

    def decorator_impl(self, type_: types.TypeDecorator[Any]) -> TypeEngine[Any]:
        variant = self.variant(type_)
        if variant is not None:
            return variant
        return type_.load_dialect_impl(self.dialect)

    @staticmethod
    def has_unknown_element(type_: TypeEngine[Any]) -> bool:
        if isinstance(type_, sqltypes.ARRAY):
            return _ArrayTypeInspector.has_unknown_element(type_.item_type)
        if isinstance(type_, AthenaMap):
            return _ArrayTypeInspector.has_unknown_element(
                type_.key_type
            ) or _ArrayTypeInspector.has_unknown_element(type_.value_type)
        if isinstance(type_, AthenaStruct):
            return any(
                _ArrayTypeInspector.has_unknown_element(field) for field in type_.fields.values()
            )
        return isinstance(type_, types.NullType)


class _ArrayValueProcessor:
    """Convert ARRAY values and their typed elements to and from Athena transport.

    SQLAlchemy constructs processors per type and dialect. Keep that context
    here and share the recursive ARRAY/MAP/ROW traversal across bind parameters,
    SQL literals, and fetched JSON results.
    """

    def __init__(self, type_: TypeEngine[Any], dialect: Any) -> None:
        self.type_ = type_
        self.dialect = dialect
        self._type_inspector = _ArrayTypeInspector(dialect)

    def bind(self, value: Any) -> Any:
        return self._bind(value, self.type_)

    def literal(self, value: Any) -> str:
        return self._literal(value, self.type_)

    def result(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                # Textual SQL does not receive column_expression. Preserve the
                # DBAPI's raw fallback when native nested data is ambiguous.
                return value
        if isinstance(value, dict) and "_pyathena_array" in value:
            value = value["_pyathena_array"]
        return self._decode(
            value,
            self.type_,
            self.type_.as_tuple if isinstance(self.type_, sqltypes.ARRAY) else False,
        )

    @staticmethod
    def _complex_values(value: Any, type_: TypeEngine[Any]):
        if isinstance(type_, sqltypes.ARRAY):
            if not isinstance(value, (list, tuple)):
                raise TypeError("ARRAY values must be lists or tuples.")
            item_type = _ArrayTypeInspector.item_type(type_)
            return "ARRAY", [(item, item_type) for item in value]
        if isinstance(type_, AthenaMap):
            if not isinstance(value, Mapping):
                raise TypeError("MAP values must be mappings.")
            return "MAP", [
                (list(value), AthenaArray(type_.key_type)),
                (list(value.values()), AthenaArray(type_.value_type)),
            ]
        if isinstance(type_, AthenaStruct):
            if isinstance(value, Mapping):
                if set(value) != set(type_.fields):
                    raise ValueError("ROW value fields must match the declared fields.")
                values = [value[name] for name in type_.fields]
            elif isinstance(value, (list, tuple)) and len(value) == len(type_.fields):
                values = list(value)
            else:
                raise TypeError("ROW values must match the declared fields.")
            return "ROW", list(zip(values, type_.fields.values(), strict=True))
        return None

    def _bind(self, value: Any, type_: TypeEngine[Any]) -> Any:
        variant = self._type_inspector.variant(type_)
        if variant is not None:
            return self._bind(value, variant)
        if isinstance(type_, types.TypeDecorator):
            if type(type_).bind_processor is not types.TypeDecorator.bind_processor:
                processor = type_.bind_processor(self.dialect)
                return processor(value) if processor else value
            if type_._has_bind_processor:
                value = type_.process_bind_param(value, self.dialect)
            return self._bind(value, self._type_inspector.decorator_impl(type_))
        if value is None:
            return None
        complex_values = self._complex_values(value, type_)
        if complex_values is not None:
            constructor, items = complex_values
            return _ComplexParameter(
                constructor, tuple(self._bind(item, item_type) for item, item_type in items)
            )
        if isinstance(type_, types.JSON):
            serializer = self.dialect._json_serializer or json.dumps
            return _ComplexParameter("JSON_PARSE", (serializer(value),))
        if isinstance(value, (list, tuple, Mapping)):
            raise TypeError("ARRAY element shape does not match its declared type.")
        if isinstance(type_, (types.LargeBinary, types.BINARY, types.VARBINARY)):
            return bytes(value)
        if isinstance(type_, (types.Date, types.DateTime)):
            return value
        processor = type_.dialect_impl(self.dialect).bind_processor(self.dialect)
        return processor(value) if processor else value

    def _literal(self, value: Any, type_: TypeEngine[Any]) -> str:
        variant = self._type_inspector.variant(type_)
        if variant is not None:
            return self._literal(value, variant)
        if isinstance(type_, types.TypeDecorator):
            if type(type_).literal_processor is not types.TypeDecorator.literal_processor:
                literal_override = type_.literal_processor(self.dialect)
                if literal_override is not None:
                    return literal_override(value)
            if type_._has_literal_processor:
                value = type_.process_literal_param(value, self.dialect)
            elif type_._has_bind_processor:
                value = type_.process_bind_param(value, self.dialect)
            return self._literal(value, self._type_inspector.decorator_impl(type_))
        if value is None:
            return "NULL"
        complex_values = self._complex_values(value, type_)
        if complex_values is not None:
            constructor, items = complex_values
            opening, closing = ("[", "]") if constructor == "ARRAY" else ("(", ")")
            values = ", ".join(self._literal(item, item_type) for item, item_type in items)
            return f"{constructor}{opening}{values}{closing}"
        if isinstance(type_, types.JSON):
            serializer = self.dialect._json_serializer or json.dumps
            processor = types.String().literal_processor(self.dialect)
            return f"JSON_PARSE({processor(serializer(value))})"
        if isinstance(value, (list, tuple, Mapping)):
            raise TypeError("ARRAY element shape does not match its declared type.")
        if isinstance(type_, (types.LargeBinary, types.BINARY, types.VARBINARY)):
            return f"X'{bytes(value).hex()}'"
        processor = type_.dialect_impl(self.dialect).literal_processor(self.dialect)
        if processor is None:
            raise exc.CompileError(f"No ARRAY element literal processor for {type_!r}.")
        return str(processor(value))

    def _decode(self, value: Any, type_: TypeEngine[Any], as_tuple: bool = False) -> Any:
        variant = self._type_inspector.variant(type_)
        if variant is not None:
            return self._decode(value, variant, as_tuple)
        if isinstance(type_, types.TypeDecorator):
            value = self._decode(value, self._type_inspector.decorator_impl(type_), as_tuple)
            if type(type_).result_processor is not types.TypeDecorator.result_processor:
                processor = type_.result_processor(self.dialect, None)
                return processor(value) if processor else value
            if type_._has_result_processor:
                return type_.process_result_value(value, self.dialect)
            return value
        if value is None:
            return None
        if isinstance(type_, sqltypes.ARRAY):
            item_type = _ArrayTypeInspector.item_type(type_)
            items = [self._decode(item, item_type, as_tuple) for item in value]
            return tuple(items) if as_tuple else items
        if isinstance(type_, AthenaMap):
            map_items = value.items() if isinstance(value, dict) else value
            return {
                self._decode(key, type_.key_type): self._decode(item, type_.value_type, as_tuple)
                for key, item in map_items
            }
        if isinstance(type_, AthenaStruct):
            if not type_.fields:
                return value
            return {
                name: self._decode(value[name], field_type, as_tuple)
                for name, field_type in type_.fields.items()
            }
        if isinstance(type_, types.JSON):
            return value
        if isinstance(type_, types.Boolean):
            return value if isinstance(value, bool) else value.lower() == "true"
        if isinstance(type_, types.Integer):
            return int(value)
        if isinstance(type_, types.Numeric):
            return Decimal(value) if type_.asdecimal else float(value)
        if isinstance(type_, (types.DateTime, AthenaTimestamp)):
            return value if isinstance(value, datetime) else _decode_datetime(value)
        if isinstance(type_, (types.Date, AthenaDate)):
            return value if isinstance(value, date) else date.fromisoformat(value)
        if isinstance(type_, (types.LargeBinary, types.BINARY, types.VARBINARY)):
            return value if isinstance(value, bytes) else bytes.fromhex(value)
        if isinstance(type_, types.String):
            value = str(value)
            processor = type_.dialect_impl(self.dialect).result_processor(self.dialect, None)
            return processor(value) if processor else value
        return value


class _ArrayAssignmentType(types.TypeDecorator[Any]):
    """Preserve declared element processors for ARRAY assignment values."""

    impl = types.NullType
    cache_ok = True

    def __init__(self, item_type):
        super().__init__()
        self.item_type = item_type

    def bind_processor(self, dialect):
        processor = _ArrayValueProcessor(self.item_type, dialect)

        def process(value):
            value = processor.bind(value)
            if isinstance(value, (bytes, bytearray)):
                return _ComplexParameter("FROM_HEX", (value.hex(),))
            return value

        return process

    def literal_processor(self, dialect):
        return _ArrayValueProcessor(self.item_type, dialect).literal

    def bind_expression(self, bindvalue):
        expression = self.item_type.bind_expression(bindvalue)
        return bindvalue if expression is None else expression


class _ArrayWriteIndexType(types.TypeDecorator[int]):
    """Reject non-integer and NULL bound ARRAY write indices."""

    impl = types.Integer
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if type(value) is not int:
            raise ValueError("ARRAY write indices must be non-NULL integers")
        return value

    def process_literal_param(self, value, dialect):
        return self.process_bind_param(value, dialect)


class _ArrayUpdate(ColumnElement[Any]):
    """Whole-column expression generated from one partial ARRAY assignment."""

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
            value._with_binary_element_type(
                _ArrayAssignmentType(value_type if value.type._isnull else value.type)
            )
            if isinstance(value, BindParameter)
            else value
        )

    @property
    def _from_objects(self):
        return self.column._from_objects + self.value._from_objects

    @classmethod
    def rewrite(cls, statement, dialect):
        inspector = _ArrayTypeInspector(dialect)
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
                if inspector.array_type(base.left.type) is None:
                    break
                path.insert(0, base.right)
                base = base.left
            name = base if isinstance(base, str) else getattr(base, "key", None)
            if path:
                if (
                    not isinstance(base, Column)
                    or base.table is None
                    or base.table._deannotate() is not statement.table._deannotate()
                ):
                    raise exc.CompileError("ARRAY updates require a column of the target table")
                if name in seen:
                    raise exc.CompileError("Only one assignment per ARRAY column is supported")
                if any(isinstance(index, Slice) for index in path[:-1]):
                    raise exc.CompileError("Only the final ARRAY update index can be a slice")
                partial.add(name)
                value_type = key.type
                value = cls(base, path, value, value_type)
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


class _ArrayUpdateCompiler:
    """Render an ARRAY assignment by rebuilding its affected nested arrays."""

    def __init__(self, compiler):
        self.compiler = compiler
        self._type_inspector = _ArrayTypeInspector(compiler.dialect)

    def process(self, expression, **kw):
        compiler = self.compiler
        value = expression.value
        final_slice = isinstance(expression.path[-1], Slice)
        if final_slice and (
            isinstance(value, Null)
            or (
                isinstance(value, BindParameter)
                and not value.required
                and value.callable is None
                and value.value is None
            )
        ):
            raise exc.CompileError("An ARRAY slice assignment requires a non-NULL array")
        rhs = compiler.process(value, **kw)
        rhs_type = compiler._complex_dml_type(expression.value_type, require_precision=True)
        rhs = f"CAST({rhs} AS {rhs_type})"
        if final_slice:
            # Reject SQL expressions that evaluate to NULL without issuing a second statement.
            failure = (
                f"slice(CAST(ARRAY[] AS {rhs_type}), "
                "CAST(concat('NULL ARRAY slice assignment', coalesce(CAST(cardinality("
                f"{rhs}) AS VARCHAR), '')) AS BIGINT), 0)"
            )
            rhs = f"IF({rhs} IS NULL, {failure}, {rhs})"
        return self._rebuild(
            compiler.process(expression.column, **kw), expression.type, expression.path, rhs, **kw
        )

    def _index_sql(self, index: ColumnElement[Any], **kw):
        compiler = self.compiler
        if isinstance(index, Null):
            raise exc.CompileError("ARRAY write indices must be non-NULL positive integers")
        if (
            isinstance(index, BindParameter)
            and not index.required
            and index.callable is None
            and (type(index.value) is not int or index.value <= 0)
        ):
            raise exc.CompileError(
                "ARRAY write indices must be positive integers after normalization"
            )
        if not isinstance(index.type, (types.Integer, types.NullType)) and not (
            isinstance(index, BindParameter)
            and self._type_inspector.array_type(index.type) is not None
        ):
            raise exc.CompileError("ARRAY write indices must be integers")

        if isinstance(index, BindParameter):
            index = index._with_binary_element_type(_ArrayWriteIndexType())
        sql = compiler.process(index, **kw)
        failure = (
            "CAST(concat('Invalid ARRAY index: ', "
            f"coalesce(CAST({sql} AS VARCHAR), 'NULL')) AS BIGINT)"
        )
        return f"IF({sql} > 0, {sql}, {failure})"

    def _rebuild(self, array, array_type, path, rhs, **kw):
        compiler = self.compiler
        array_type = self._type_inspector.array_type(array_type)
        if array_type is None:
            raise exc.CompileError("Partial ARRAY updates require an ARRAY column type")
        array_sql_type = compiler._complex_dml_type(array_type)
        array = f"coalesce({array}, CAST(ARRAY[] AS {array_sql_type}))"
        bound = path[0]
        if isinstance(bound, Slice):
            return self._rebuild_slice(array, array_type, bound, rhs, **kw)
        return self._rebuild_element(array, array_type, bound, path[1:], rhs, **kw)

    def _prefix_and_padding(self, array, start, array_type):
        prefix = f"slice({array}, 1, least({start} - 1, cardinality({array})))"
        element_type = self.compiler._complex_dml_type(_ArrayTypeInspector.item_type(array_type))
        padding = (
            f"repeat(CAST(NULL AS {element_type}), "
            f"CAST(greatest({start} - 1 - cardinality({array}), 0) AS INTEGER))"
        )
        return prefix, padding

    def _rebuild_slice(self, array, array_type, bound, rhs, **kw):
        if not isinstance(bound.step, Null) and not (
            isinstance(bound.step, BindParameter)
            and bound.step.unique
            and type(bound.step.value) is int
            and bound.step.value == 1
        ):
            raise exc.CompileError("Athena ARRAY slices support only step=None or step=1")
        start = "1" if isinstance(bound.start, Null) else self._index_sql(bound.start, **kw)
        stop = (
            f"cardinality({array})"
            if isinstance(bound.stop, Null)
            else self._index_sql(bound.stop, **kw)
        )
        prefix, padding = self._prefix_and_padding(array, start, array_type)
        tail_start = f"greatest({start}, {stop} + 1)"
        suffix = (
            f"slice({array}, {tail_start}, greatest(cardinality({array}) - {tail_start} + 1, 0))"
        )
        return self.compiler._array_slice_step(
            f"concat({prefix}, {padding}, {rhs}, {suffix})", bound.step, array_type, **kw
        )

    def _rebuild_element(self, array, array_type, bound, remaining_path, rhs, **kw):
        index = self._index_sql(bound, **kw)
        previous = f"element_at({array}, {index})"
        replacement = (
            self._rebuild(
                previous, _ArrayTypeInspector.item_type(array_type), remaining_path, rhs, **kw
            )
            if remaining_path
            else rhs
        )
        prefix, padding = self._prefix_and_padding(array, index, array_type)
        suffix = f"slice({array}, {index} + 1, greatest(cardinality({array}) - {index}, 0))"
        return f"concat({prefix}, {padding}, ARRAY[{replacement}], {suffix})"
