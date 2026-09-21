"""Athena STRUCT/ROW types."""

from __future__ import annotations

from typing import Any

from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.type_api import TypeEngine


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
