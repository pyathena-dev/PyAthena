"""Type converters for Apache Arrow cursor results."""

from __future__ import annotations

import logging
from collections.abc import Callable
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from pyathena.converter import (
    _TIMESTAMP_TEXT_LENGTHS,
    Converter,
    _to_binary,
    _to_date,
    _to_datetime_with_tz,
    _to_decimal,
    _to_default,
    _to_json,
    _to_time,
    _to_time_with_tz,
)
from pyathena.util import override

if TYPE_CHECKING:
    from pyarrow import ChunkedArray, TimestampType

_logger = logging.getLogger(__name__)


_DEFAULT_ARROW_CONVERTERS: dict[str, Callable[[str | None], Any | None]] = {
    "date": _to_date,
    "time": _to_time,
    "time with time zone": _to_time_with_tz,
    "timestamp with time zone": _to_datetime_with_tz,
    "decimal": _to_decimal,
    "varbinary": _to_binary,
    "json": _to_json,
}


def _to_timestamp(column: ChunkedArray, type_: TimestampType) -> ChunkedArray:
    """Convert timestamp text to a timestamp type, truncating finer fractions.

    Athena writes up to 12 fractional digits, which pyarrow does not parse into a
    timestamp type whose unit holds fewer.

    Args:
        column: The timestamp text, with NULL as null or as an empty string.
        type_: The timestamp type.

    Returns:
        The timestamps.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    length = _TIMESTAMP_TEXT_LENGTHS[type_.unit]
    if (pc.max(pc.utf8_length(column)).as_py() or 0) > length:
        column = pc.utf8_slice_codeunits(column, 0, length)
    return pc.if_else(pc.equal(column, ""), pa.scalar(None, pa.string()), column).cast(type_)


class DefaultArrowTypeConverter(Converter):
    """Optimized type converter for Apache Arrow Table results.

    This converter is specifically designed for the ArrowCursor and provides
    optimized type conversion for Apache Arrow's columnar data format.
    It converts Athena data types to Python types that are efficiently
    handled by Apache Arrow.

    The converter focuses on:
        - Converting date/time types to appropriate Python objects
        - Handling decimal and binary types for Arrow compatibility
        - Preserving JSON and complex types
        - Maintaining high performance for columnar operations

    Example:
        >>> from pyathena.arrow.converter import DefaultArrowTypeConverter
        >>> converter = DefaultArrowTypeConverter()
        >>>
        >>> # Used automatically by ArrowCursor
        >>> cursor = connection.cursor(ArrowCursor)
        >>> # converter is applied automatically to results

    Note:
        This converter is used by default in ArrowCursor.
        Most users don't need to instantiate it directly.
    """

    def __init__(self) -> None:
        """Initialize the converter with the default Arrow conversion functions and types."""
        super().__init__(
            mappings=deepcopy(_DEFAULT_ARROW_CONVERTERS),
            default=_to_default,
            types=self._dtypes,
        )

    @property
    def _dtypes(self) -> dict[str, type[Any]]:
        if not hasattr(self, "__dtypes"):
            import pyarrow as pa

            self.__dtypes = {
                "boolean": pa.bool_(),
                "tinyint": pa.int8(),
                "smallint": pa.int16(),
                "integer": pa.int32(),
                "bigint": pa.int64(),
                "float": pa.float32(),
                "real": pa.float64(),
                "double": pa.float64(),
                "char": pa.string(),
                "varchar": pa.string(),
                "string": pa.string(),
                "timestamp": pa.timestamp("us"),
                "date": pa.timestamp("ms"),
                "time": pa.string(),
                "time with time zone": pa.string(),
                "timestamp with time zone": pa.string(),
                "varbinary": pa.string(),
                "array": pa.string(),
                "map": pa.string(),
                "row": pa.string(),
                "decimal": pa.string(),
                "json": pa.string(),
            }
        return self.__dtypes

    @override
    def convert(self, type_: str, value: str | None, type_hint: str | None = None) -> Any | None:
        converter = self.get(type_)
        return converter(value)


class DefaultArrowUnloadTypeConverter(Converter):
    """Type converter for Arrow UNLOAD operations.

    This converter is designed for use with UNLOAD queries that write
    results directly to Parquet files in S3. Since UNLOAD operations
    bypass the normal conversion process and write data in native
    Parquet format, this converter has minimal functionality.

    Note:
        Used automatically when ArrowCursor is configured with unload=True.
        UNLOAD results are read directly as Arrow tables from Parquet files.
    """

    def __init__(self) -> None:
        """Initialize the converter with no type mappings."""
        super().__init__(
            mappings={},
            default=_to_default,
        )

    @override
    def convert(self, type_: str, value: str | None, type_hint: str | None = None) -> Any | None:
        converter = self.get(type_)
        return converter(value)
