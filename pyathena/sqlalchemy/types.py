"""Public SQLAlchemy type imports for PyAthena.

Type-specific implementations live in their own modules. Re-export them here
so existing ``pyathena.sqlalchemy.types`` imports remain supported.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import types
from sqlalchemy.sql import sqltypes

from pyathena.sqlalchemy.array import ARRAY, AthenaArray
from pyathena.sqlalchemy.map import MAP, AthenaMap
from pyathena.sqlalchemy.struct import STRUCT, AthenaStruct
from pyathena.sqlalchemy.temporal import AthenaDate, AthenaTimestamp

if TYPE_CHECKING:
    from sqlalchemy import Dialect
    from sqlalchemy.sql.type_api import _LiteralProcessorType

__all__ = [
    "ARRAY",
    "MAP",
    "STRUCT",
    "TINYINT",
    "AthenaArray",
    "AthenaBinary",
    "AthenaDate",
    "AthenaMap",
    "AthenaStruct",
    "AthenaTimestamp",
    "Tinyint",
    "get_double_type",
]


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
