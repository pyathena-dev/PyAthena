# Copyright 2019 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Type converters for Polars cursor results."""

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
    _to_time_with_tz,
)
from pyathena.util import override

_logger = logging.getLogger(__name__)


_DEFAULT_POLARS_CONVERTERS: dict[str, Callable[[str | None], Any | None]] = {
    "date": _to_date,
    "time": _to_time,
    "time with time zone": _to_time_with_tz,
    "varbinary": _to_binary,
    "json": _to_json,
}


class DefaultPolarsTypeConverter(Converter):
    """Optimized type converter for Polars DataFrame results.

    This converter is specifically designed for the PolarsCursor and provides
    optimized type conversion for Polars DataFrames.

    The converter focuses on:
        - Mapping Athena types to Polars dtypes when reading CSV results,
          including ``decimal`` as ``pl.Decimal(precision, scale)``
        - Converting date, time, and varbinary values, and parsing JSON values,
          in rows returned by fetchone(), fetchmany(), and fetchall()
        - Keeping array, map, and row values as strings

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
        """Initialize the converter with the default Polars conversion functions and dtypes."""
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
                "time with time zone": pl.String,
                "varbinary": pl.String,
                "array": pl.String,
                "map": pl.String,
                "row": pl.String,
                "decimal": pl.Decimal,
                "json": pl.String,
            }
        return self.__dtypes

    @override
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

    @override
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
        """Initialize the converter with no type mappings."""
        super().__init__(
            mappings={},
            default=_to_default,
        )

    @override
    def convert(self, type_: str, value: str | None, type_hint: str | None = None) -> Any | None:
        converter = self.get(type_)
        return converter(value)
