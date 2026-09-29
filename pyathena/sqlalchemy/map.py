"""Athena MAP types."""

from __future__ import annotations

from typing import Any

from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.type_api import TypeEngine


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
