from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import types
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.type_api import TypeEngine

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

    render_literal_cast = True
    render_bind_cast = True

    @staticmethod
    def process(value: date | datetime | Any) -> str:
        if isinstance(value, (date, datetime)):
            f"DATE '{value:%Y-%m-%d}'"
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


class AthenaArray(TypeEngine[list[Any]]):
    """SQLAlchemy type for Athena ARRAY complex type.

    ARRAY represents an ordered collection of elements of the same type.

    Args:
        item_type: SQLAlchemy type for array elements. Defaults to String.

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

    def __init__(self, item_type: Any = None) -> None:
        if item_type is None:
            self.item_type: TypeEngine[Any] = sqltypes.String()
        elif isinstance(item_type, TypeEngine):
            self.item_type = item_type
        else:
            # Assume it's a SQLAlchemy type class and instantiate it
            self.item_type = item_type()

    @property
    def python_type(self) -> type:
        return list


class ARRAY(AthenaArray):
    """Uppercase alias for AthenaArray type."""

    __visit_name__ = "ARRAY"
