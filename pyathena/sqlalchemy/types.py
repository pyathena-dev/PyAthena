from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import cast, exc, types
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.sql.visitors import InternalTraversal

from pyathena.formatter import _ComplexParameter

if TYPE_CHECKING:
    from sqlalchemy import Dialect
    from sqlalchemy.sql.type_api import _LiteralProcessorType


def get_double_type() -> type[Any]:
    """Get the appropriate type for DOUBLE based on SQLAlchemy version.

    SQLAlchemy 2.0+ provides a native DOUBLE type, while earlier versions
    only have FLOAT. This function returns the appropriate type based on
    what's available.

    Returns:
        types.DOUBLE for SQLAlchemy 2.0+, types.FLOAT for earlier versions.
    """
    if hasattr(types, "DOUBLE"):
        return types.DOUBLE
    return types.FLOAT


class AthenaBinary(types.LargeBinary):
    """SQLAlchemy binary type with Athena hexadecimal literals."""

    def literal_processor(self, dialect: Dialect) -> _LiteralProcessorType[bytes]:
        def process(value: bytes) -> str:
            return f"X'{value.hex()}'"

        return process


class AthenaTimestamp(TypeEngine[datetime]):
    """SQLAlchemy type for Athena TIMESTAMP values.

    This type handles the conversion of Python datetime objects to Athena's
    TIMESTAMP literal syntax. When used in queries, datetime values are
    rendered as ``TIMESTAMP 'YYYY-MM-DD HH:MM:SS.mmm'``.

    The type supports millisecond precision (3 decimal places) which matches
    Athena's TIMESTAMP type precision.

    Example:
        >>> from sqlalchemy import Column, Table, MetaData
        >>> from pyathena.sqlalchemy.types import AthenaTimestamp
        >>> metadata = MetaData()
        >>> events = Table('events', metadata,
        ...     Column('event_time', AthenaTimestamp)
        ... )
    """

    __visit_name__ = "TIMESTAMP"

    render_literal_cast = True
    render_bind_cast = True

    @staticmethod
    def process(value: datetime | Any | None) -> str:
        if isinstance(value, datetime):
            return f"""TIMESTAMP '{value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}'"""
        return f"TIMESTAMP '{value!s}'"

    def literal_processor(self, dialect: Dialect) -> _LiteralProcessorType[datetime] | None:
        return self.process


class AthenaDate(TypeEngine[date]):
    """SQLAlchemy type for Athena DATE values.

    This type handles the conversion of Python date objects to Athena's
    DATE literal syntax. When used in queries, date values are rendered
    as ``DATE 'YYYY-MM-DD'``.

    Example:
        >>> from sqlalchemy import Column, Table, MetaData
        >>> from pyathena.sqlalchemy.types import AthenaDate
        >>> metadata = MetaData()
        >>> orders = Table('orders', metadata,
        ...     Column('order_date', AthenaDate)
        ... )
    """

    __visit_name__ = "DATE"

    render_literal_cast = True
    render_bind_cast = True

    @staticmethod
    def process(value: date | Any) -> str:
        # datetime is a subclass of date, so this branch also covers datetime,
        # which is truncated to its date part.
        if isinstance(value, date):
            return f"DATE '{value:%Y-%m-%d}'"
        return f"DATE '{value!s}'"

    def literal_processor(self, dialect: Dialect) -> _LiteralProcessorType[date] | None:
        return self.process


class Tinyint(sqltypes.Integer):
    """SQLAlchemy type for Athena TINYINT (8-bit signed integer).

    TINYINT stores values from -128 to 127. This type is useful for
    columns that contain small integer values to optimize storage.
    """

    __visit_name__ = "tinyint"


class TINYINT(Tinyint):
    """Uppercase alias for Tinyint type.

    This provides SQLAlchemy-style uppercase naming convention.
    """

    __visit_name__ = "TINYINT"


class AthenaStruct(TypeEngine[dict[str, Any]]):
    """SQLAlchemy type for Athena STRUCT/ROW complex type.

    STRUCT represents a record with named fields, similar to a database row
    or a Python dictionary with typed values. Each field has a name and a
    data type.

    Args:
        *fields: Field specifications. Each can be either:
            - A string (field name, defaults to STRING type)
            - A tuple of (field_name, field_type)

    Example:
        >>> from sqlalchemy import Column, Table, MetaData, types
        >>> from pyathena.sqlalchemy.types import AthenaStruct
        >>> metadata = MetaData()
        >>> users = Table('users', metadata,
        ...     Column('address', AthenaStruct(
        ...         ('street', types.String),
        ...         ('city', types.String),
        ...         ('zip_code', types.Integer)
        ...     ))
        ... )

    See Also:
        AWS Athena STRUCT Type:
        https://docs.aws.amazon.com/athena/latest/ug/rows-and-structs.html
    """

    __visit_name__ = "struct"

    def __init__(self, *fields: str | tuple[str, Any]) -> None:
        self.fields: dict[str, TypeEngine[Any]] = {}

        for field in fields:
            if isinstance(field, str):
                self.fields[field] = sqltypes.String()
            elif isinstance(field, tuple) and len(field) == 2:
                field_name, field_type = field
                if isinstance(field_type, TypeEngine):
                    self.fields[field_name] = field_type
                else:
                    # Assume it's a SQLAlchemy type class and instantiate it
                    self.fields[field_name] = field_type()
            else:
                raise ValueError(f"Invalid field specification: {field}")

    def __getitem__(self, key: str) -> TypeEngine[Any]:
        return self.fields[key]

    @property
    def _static_cache_key(self):
        return (
            type(self),
            tuple((name, type_._static_cache_key) for name, type_ in self.fields.items()),
        )

    @property
    def python_type(self) -> type:
        return dict


class STRUCT(AthenaStruct):
    """Uppercase alias for AthenaStruct type."""

    __visit_name__ = "STRUCT"


class AthenaMap(TypeEngine[dict[str, Any]]):
    """SQLAlchemy type for Athena MAP complex type.

    MAP represents a collection of key-value pairs where all keys have the
    same type and all values have the same type.

    Args:
        key_type: SQLAlchemy type for map keys. Defaults to String.
        value_type: SQLAlchemy type for map values. Defaults to String.

    Example:
        >>> from sqlalchemy import Column, Table, MetaData, types
        >>> from pyathena.sqlalchemy.types import AthenaMap
        >>> metadata = MetaData()
        >>> settings = Table('settings', metadata,
        ...     Column('config', AthenaMap(types.String, types.Integer))
        ... )

    See Also:
        AWS Athena MAP Type:
        https://docs.aws.amazon.com/athena/latest/ug/maps.html
    """

    __visit_name__ = "map"

    def __init__(self, key_type: Any = None, value_type: Any = None) -> None:
        if key_type is None:
            self.key_type: TypeEngine[Any] = sqltypes.String()
        elif isinstance(key_type, TypeEngine):
            self.key_type = key_type
        else:
            # Assume it's a SQLAlchemy type class and instantiate it
            self.key_type = key_type()

        if value_type is None:
            self.value_type: TypeEngine[Any] = sqltypes.String()
        elif isinstance(value_type, TypeEngine):
            self.value_type = value_type
        else:
            # Assume it's a SQLAlchemy type class and instantiate it
            self.value_type = value_type()

    @property
    def python_type(self) -> type:
        return dict


class MAP(AthenaMap):
    """Uppercase alias for AthenaMap type."""

    __visit_name__ = "MAP"


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


class _ArrayTypeInspector:
    """Interpret nested ARRAY element types for SQL compilation and value conversion.

    Type inspection is shared by the compiler and value processors. Resolving
    a TypeDecorator uses the current dialect; dimensions and unknown elements
    can be inspected without one.
    """

    def __init__(self, dialect: Any) -> None:
        self.dialect = dialect

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

    def decorator_impl(self, type_: types.TypeDecorator[Any]) -> TypeEngine[Any]:
        if self.dialect.name in type_._variant_mapping:
            return type_._variant_mapping[self.dialect.name]
        implementation = type_.load_dialect_impl(self.dialect)
        if isinstance(implementation, AthenaTimestamp):
            return types.TIMESTAMP()
        if isinstance(implementation, AthenaDate):
            return types.DATE()
        return implementation

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
    """Convert one declared ARRAY type between Python values and Athena transport.

    SQLAlchemy constructs processors per type and dialect. Keep that context
    here and share the recursive ARRAY/MAP/ROW traversal across bind parameters,
    SQL literals, and fetched JSON results.
    """

    def __init__(self, array_type: AthenaArray, dialect: Any) -> None:
        self.array_type = array_type
        self.dialect = dialect
        self._type_inspector = _ArrayTypeInspector(dialect)

    def bind(self, value: Any) -> Any:
        return self._bind(value, self.array_type)

    def literal(self, value: Any) -> str:
        return self._literal(value, self.array_type)

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
        return self._decode(value, self.array_type, self.array_type.as_tuple)

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
        if isinstance(type_, types.TypeDecorator):
            if (
                self.dialect.name not in type_._variant_mapping
                and type(type_).bind_processor is not types.TypeDecorator.bind_processor
            ):
                processor = type_.bind_processor(self.dialect)
                return processor(value) if processor else value
            if self.dialect.name not in type_._variant_mapping and type_._has_bind_processor:
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
        if isinstance(type_, types.TypeDecorator):
            if (
                self.dialect.name not in type_._variant_mapping
                and type(type_).literal_processor is not types.TypeDecorator.literal_processor
            ):
                literal_override = type_.literal_processor(self.dialect)
                if literal_override is not None:
                    return literal_override(value)
            if self.dialect.name not in type_._variant_mapping:
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
        if isinstance(type_, types.DateTime) and isinstance(value, datetime):
            return AthenaTimestamp.process(value)
        if isinstance(type_, types.Date) and isinstance(value, date):
            return AthenaDate.process(value)
        processor = type_.dialect_impl(self.dialect).literal_processor(self.dialect)
        if processor is None:
            raise exc.CompileError(f"No ARRAY element literal processor for {type_!r}.")
        return str(processor(value))

    def _decode(self, value: Any, type_: TypeEngine[Any], as_tuple: bool = False) -> Any:
        if isinstance(type_, types.TypeDecorator):
            value = self._decode(value, self._type_inspector.decorator_impl(type_), as_tuple)
            if (
                self.dialect.name not in type_._variant_mapping
                and type(type_).result_processor is not types.TypeDecorator.result_processor
            ):
                processor = type_.result_processor(self.dialect, None)
                return processor(value) if processor else value
            if self.dialect.name not in type_._variant_mapping and type_._has_result_processor:
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
            return value if isinstance(value, datetime) else datetime.fromisoformat(value)
        if isinstance(type_, (types.Date, AthenaDate)):
            return value if isinstance(value, date) else date.fromisoformat(value)
        if isinstance(type_, (types.LargeBinary, types.BINARY, types.VARBINARY)):
            return value if isinstance(value, bytes) else bytes.fromhex(value)
        if isinstance(type_, types.String):
            value = str(value)
            processor = type_.dialect_impl(self.dialect).result_processor(self.dialect, None)
            return processor(value) if processor else value
        return value
