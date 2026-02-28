from __future__ import annotations

import logging
from collections.abc import Callable
from copy import deepcopy
from typing import Any

from pyathena.converter import (
    Converter,
    _to_binary,
    _to_boolean,
    _to_decimal,
    _to_default,
    _to_json,
)

_logger = logging.getLogger(__name__)


_DEFAULT_PANDAS_CONVERTERS: dict[str, Callable[[str | None], Any | None]] = {
    "boolean": _to_boolean,
    "decimal": _to_decimal,
    "varbinary": _to_binary,
    "json": _to_json,
}


class DefaultPandasTypeConverter(Converter):
    """Optimized type converter for pandas DataFrame results.

    This converter is specifically designed for the PandasCursor and provides
    optimized type conversion that works well with pandas data types.
    It minimizes conversions for types that pandas handles efficiently
    and only converts complex types that need special handling.

    The converter focuses on:
        - Preserving numeric types for pandas optimization
        - Converting only complex types (json, binary, etc.)
        - Maintaining compatibility with pandas data type inference

    Example:
        >>> from pyathena.pandas.converter import DefaultPandasTypeConverter
        >>> converter = DefaultPandasTypeConverter()
        >>>
        >>> # Used automatically by PandasCursor
        >>> cursor = connection.cursor(PandasCursor)
        >>> # converter is applied automatically to results

    Note:
        This converter is used by default in PandasCursor.
        Most users don't need to instantiate it directly.
    """

    def __init__(self) -> None:
        super().__init__(
            mappings=deepcopy(_DEFAULT_PANDAS_CONVERTERS),
            default=_to_default,
            types=self._dtypes,
        )

    @property
    def _dtypes(self) -> dict[str, type[Any]]:
        if not hasattr(self, "__dtypes"):
            import pandas as pd

            self.__dtypes = {
                "tinyint": pd.Int64Dtype(),
                "smallint": pd.Int64Dtype(),
                "integer": pd.Int64Dtype(),
                "bigint": pd.Int64Dtype(),
                "float": float,
                "real": float,
                "double": float,
                "char": str,
                "varchar": str,
                "string": str,
                "array": str,
                "map": str,
                "row": str,
            }
        return self.__dtypes

    def convert(self, type_: str, value: str | None, type_hint: str | None = None) -> Any | None:
        pass


class DefaultPandasUnloadTypeConverter(Converter):
    """Type converter for pandas UNLOAD operations.

    This converter is designed for use with UNLOAD queries that write
    results directly to Parquet files in S3. Since UNLOAD operations
    bypass the normal conversion process and write data in native
    Parquet format, this converter has minimal functionality.

    Note:
        Used automatically when PandasCursor is configured with unload=True.
        UNLOAD results are read directly as DataFrames from Parquet files.
    """

    def __init__(self) -> None:
        super().__init__(
            mappings={},
            default=_to_default,
        )

    def convert(self, type_: str, value: str | None, type_hint: str | None = None) -> Any | None:
        pass
