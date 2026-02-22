from __future__ import annotations

import logging
from collections.abc import Callable
from copy import deepcopy
from typing import Any

from pyathena.converter import (
    Converter,
    _to_binary,
    _to_date,
    _to_decimal,
    _to_default,
    _to_json,
    _to_time,
)

_logger = logging.getLogger(__name__)


_DEFAULT_ARROW_CONVERTERS: dict[str, Callable[[str | None], Any | None]] = {
    "date": _to_date,
    "time": _to_time,
    "decimal": _to_decimal,
    "varbinary": _to_binary,
    "json": _to_json,
}


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
                "timestamp": pa.timestamp("ms"),
                "date": pa.timestamp("ms"),
                "time": pa.string(),
                "varbinary": pa.string(),
                "array": pa.string(),
                "map": pa.string(),
                "row": pa.string(),
                "decimal": pa.string(),
                "json": pa.string(),
            }
        return self.__dtypes

    def convert(self, type_: str, value: str | None) -> Any | None:
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
        super().__init__(
            mappings={},
            default=_to_default,
        )

    def convert(self, type_: str, value: str | None) -> Any | None:
        pass
