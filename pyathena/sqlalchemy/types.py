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
from pyathena.util import override

if TYPE_CHECKING:
    from sqlalchemy import Dialect
    from sqlalchemy.sql.type_api import _LiteralProcessorType, _ResultProcessorType

__all__ = [
    "ARRAY",
    "MAP",
    "STRUCT",
    "TINYINT",
    "AthenaArray",
    "AthenaBinary",
    "AthenaDate",
    "AthenaJSON",
    "AthenaMap",
    "AthenaStruct",
    "AthenaTimestamp",
    "Tinyint",
]


class AthenaBinary(types.LargeBinary):
    """SQLAlchemy binary type with Athena hexadecimal literals."""

    @override
    def literal_processor(self, dialect: Dialect) -> _LiteralProcessorType[bytes]:
        def process(value: bytes) -> str:
            return f"X'{value.hex()}'"

        return process


class AthenaJSON(types.JSON):
    """SQLAlchemy JSON type that keeps the values PyAthena has decoded.

    PyAthena's default converters decode results of the Athena ``json`` type,
    so this type returns them unchanged, and a JSON string scalar stays a
    ``str``. With a custom converter that does not decode ``json`` results,
    this type returns their text. Results of other Athena types, such as JSON
    text in a ``varchar`` column, are decoded with the dialect's JSON
    deserializer.
    """

    @override
    def result_processor(
        self, dialect: Dialect, coltype: object
    ) -> _ResultProcessorType[Any] | None:
        """Return a processor decoding JSON text of Athena types other than ``json``.

        Args:
            dialect: The dialect fetching the value.
            coltype: The Athena type name from the cursor description.

        Returns:
            The processor, or None for the Athena ``json`` type.
        """
        if coltype == "json":
            return None
        processor: _ResultProcessorType[Any] | None = super().result_processor(dialect, coltype)
        return processor


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
