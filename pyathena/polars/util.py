"""Utilities for converting Polars types to Athena metadata.

This module provides functions to convert Polars schema and type information
to Athena-compatible column metadata, enabling proper type mapping when
reading query results in Polars format.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import polars as pl


def to_column_info(schema: pl.Schema) -> tuple[dict[str, Any], ...]:
    """Convert a Polars schema to Athena column information.

    Iterates through all fields in the schema and converts each field's
    type information to an Athena-compatible column metadata dictionary.

    Args:
        schema: A Polars Schema object containing field definitions.

    Returns:
        A tuple of dictionaries, each containing column metadata with keys:
        - Name: The column name
        - Type: The Athena SQL type name
        - Precision: Numeric precision (0 for non-numeric types)
        - Scale: Numeric scale (0 for non-numeric types)
        - Nullable: Always "NULLABLE" for Polars types
    """
    columns = []
    for name, dtype in schema.items():
        type_, precision, scale = get_athena_type(dtype)
        columns.append(
            {
                "Name": name,
                "Type": type_,
                "Precision": precision,
                "Scale": scale,
                "Nullable": "NULLABLE",
            }
        )
    return tuple(columns)


def get_athena_type(dtype: Any) -> tuple[str, int, int]:
    """Map a Polars data type to an Athena SQL type.

    Converts Polars type identifiers to corresponding Athena SQL type names
    with appropriate precision and scale values. Handles all common Polars
    types including numeric, string, binary, temporal, and complex types.

    Args:
        dtype: A Polars DataType object to convert.

    Returns:
        A tuple of (type_name, precision, scale) where:
        - type_name: The Athena SQL type (e.g., "varchar", "bigint", "timestamp")
        - precision: The numeric precision or max length
        - scale: The numeric scale (decimal places)

    Note:
        Unknown types default to "string" with maximum varchar length.
        Decimal types preserve their original precision and scale.
    """
    import polars as pl

    # Use base_type() to handle parameterized types correctly
    # (e.g., Datetime(time_unit="us") -> Datetime)
    base_dtype = dtype.base_type() if hasattr(dtype, "base_type") else dtype

    # Type mapping: Polars type -> (Athena type, precision, scale)
    type_mapping: dict[Any, tuple[str, int, int]] = {
        pl.Boolean: ("boolean", 0, 0),
        pl.Int8: ("tinyint", 3, 0),
        pl.Int16: ("smallint", 5, 0),
        pl.Int32: ("integer", 10, 0),
        pl.Int64: ("bigint", 19, 0),
        pl.UInt8: ("tinyint", 3, 0),
        pl.UInt16: ("smallint", 5, 0),
        pl.UInt32: ("integer", 10, 0),
        pl.UInt64: ("bigint", 19, 0),
        pl.Float32: ("float", 17, 0),
        pl.Float64: ("double", 17, 0),
        pl.String: ("varchar", 2147483647, 0),
        pl.Utf8: ("varchar", 2147483647, 0),
        pl.Date: ("date", 0, 0),
        pl.Datetime: ("timestamp", 3, 0),
        pl.Time: ("time", 0, 0),
        pl.Binary: ("varbinary", 1073741824, 0),
    }

    # Check base type using both base_dtype and original dtype
    for polars_type, athena_info in type_mapping.items():
        if base_dtype == polars_type or dtype == polars_type:
            return athena_info

    # Handle parameterized types that didn't match above
    dtype_str = str(dtype).lower()
    if "list" in dtype_str:
        return ("array", 0, 0)
    if "struct" in dtype_str:
        return ("row", 0, 0)
    if "decimal" in dtype_str:
        # Extract precision and scale from Decimal type if available
        if hasattr(dtype, "precision") and hasattr(dtype, "scale"):
            return ("decimal", dtype.precision, dtype.scale)
        return ("decimal", 38, 9)  # Default precision and scale

    return ("string", 2147483647, 0)
