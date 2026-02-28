from __future__ import annotations

import logging
from collections.abc import Callable
from copy import deepcopy
from typing import Any

from pyathena.converter import (
    Converter,
    _to_binary,
    _to_date,
    _to_default,
    _to_json,
    _to_time,
)

_logger = logging.getLogger(__name__)


_DEFAULT_POLARS_CONVERTERS: dict[str, Callable[[str | None], Any | None]] = {
    "date": _to_date,
    "time": _to_time,
    "varbinary": _to_binary,
    "json": _to_json,
}


class DefaultPolarsTypeConverter(Converter):
    """Optimized type converter for Polars DataFrame results.

    This converter is specifically designed for the PolarsCursor and provides
    optimized type conversion for Polars DataFrames.

    The converter focuses on:
        - Converting date/time types to appropriate Python objects
        - Handling decimal and binary types
        - Preserving JSON and complex types
        - Maintaining high performance for columnar operations

    Example:
        >>> from pyathena.polars.converter import DefaultPolarsTypeConverter
        >>> converter = DefaultPolarsTypeConverter()
        >>>
        >>> # Used automatically by PolarsCursor
        >>> cursor = connection.cursor(PolarsCursor)
        >>> # converter is applied automatically to results

    Note:
        This converter is used by default in PolarsCursor.
        Most users don't need to instantiate it directly.
    """

    def __init__(self) -> None:
        super().__init__(
            mappings=deepcopy(_DEFAULT_POLARS_CONVERTERS),
            default=_to_default,
            types=self._dtypes,
        )

    @property
    def _dtypes(self) -> dict[str, Any]:
        import polars as pl

        if not hasattr(self, "__dtypes"):
            self.__dtypes = {
                "boolean": pl.Boolean,
                "tinyint": pl.Int8,
                "smallint": pl.Int16,
                "integer": pl.Int32,
                "bigint": pl.Int64,
                "float": pl.Float32,
                "real": pl.Float64,
                "double": pl.Float64,
                "char": pl.String,
                "varchar": pl.String,
                "string": pl.String,
                "timestamp": pl.Datetime,
                "date": pl.Date,
                "time": pl.String,
                "varbinary": pl.String,
                "array": pl.String,
                "map": pl.String,
                "row": pl.String,
                "decimal": pl.Decimal,
                "json": pl.String,
            }
        return self.__dtypes

    def get_dtype(self, type_: str, precision: int = 0, scale: int = 0) -> Any:
        """Get the Polars data type for a given Athena type.

        Args:
            type_: The Athena data type name.
            precision: The precision for decimal types.
            scale: The scale for decimal types.

        Returns:
            The Polars data type.
        """
        import polars as pl

        if type_ == "decimal":
            return pl.Decimal(precision=precision, scale=scale)
        return self._types.get(type_)

    def convert(self, type_: str, value: str | None, type_hint: str | None = None) -> Any | None:
        converter = self.get(type_)
        return converter(value)


class DefaultPolarsUnloadTypeConverter(Converter):
    """Type converter for Polars UNLOAD operations.

    This converter is designed for use with UNLOAD queries that write
    results directly to Parquet files in S3. Since UNLOAD operations
    bypass the normal conversion process and write data in native
    Parquet format, this converter has minimal functionality.

    Note:
        Used automatically when PolarsCursor is configured with unload=True.
        UNLOAD results are read directly as Polars DataFrames from Parquet files.
    """

    def __init__(self) -> None:
        super().__init__(
            mappings={},
            default=_to_default,
        )

    def convert(self, type_: str, value: str | None, type_hint: str | None = None) -> Any | None:
        converter = self.get(type_)
        return converter(value)
