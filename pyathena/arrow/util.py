"""Utilities for converting PyArrow types to Athena metadata.

This module provides functions to convert PyArrow schema and type information
to Athena-compatible column metadata, enabling proper type mapping when
reading query results in Apache Arrow format.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from pyarrow import Schema
    from pyarrow.lib import DataType


def to_column_info(schema: Schema) -> tuple[dict[str, Any], ...]:
    """Convert a PyArrow schema to Athena column information.

    Iterates through all fields in the schema and converts each field's
    type information to an Athena-compatible column metadata dictionary.

    Args:
        schema: A PyArrow Schema object containing field definitions.

    Returns:
        A tuple of dictionaries, each containing column metadata with keys:
        - Name: The column name
        - Type: The Athena SQL type name
        - Precision: Numeric precision (0 for non-numeric types)
        - Scale: Numeric scale (0 for non-numeric types)
        - Nullable: Either "NULLABLE" or "NOT_NULL"
    """
    columns = []
    for field in schema:
        type_, precision, scale = get_athena_type(field.type)
        columns.append(
            {
                "Name": field.name,
                "Type": type_,
                "Precision": precision,
                "Scale": scale,
                "Nullable": "NULLABLE" if field.nullable else "NOT_NULL",
            }
        )
    return tuple(columns)


def get_athena_type(type_: DataType) -> tuple[str, int, int]:
    """Map a PyArrow data type to an Athena SQL type.

    Converts PyArrow type identifiers to corresponding Athena SQL type names
    with appropriate precision and scale values. Handles all common Arrow
    types including numeric, string, binary, temporal, and complex types.

    Args:
        type_: A PyArrow DataType object to convert.

    Returns:
        A tuple of (type_name, precision, scale) where:
        - type_name: The Athena SQL type (e.g., "varchar", "bigint", "timestamp")
        - precision: The numeric precision or max length
        - scale: The numeric scale (decimal places)

    Note:
        Unknown types default to "string" with maximum varchar length.
        Decimal types preserve their original precision and scale.
    """
    import pyarrow.lib as types

    if type_.id in [types.Type_BOOL]:  # 1
        return "boolean", 0, 0
    if type_.id in [types.Type_UINT8, types.Type_INT8]:  # 2, 3
        return "tinyint", 3, 0
    if type_.id in [types.Type_UINT16, types.Type_INT16]:  # 4, 5
        return "smallint", 5, 0
    if type_.id in [types.Type_UINT32, types.Type_INT32]:  # 6, 7
        return "integer", 10, 0
    if type_.id in [types.Type_UINT64, types.Type_INT64]:  # 8, 9
        return "bigint", 19, 0
    if type_.id in [types.Type_HALF_FLOAT, types.Type_FLOAT]:  # 10, 11
        return "float", 17, 0
    if type_.id in [types.Type_DOUBLE]:  # 12
        return "double", 17, 0
    if type_.id in [types.Type_STRING, types.Type_LARGE_STRING]:  # 13, 34
        return "varchar", 2147483647, 0
    if type_.id in [
        types.Type_BINARY,
        types.Type_FIXED_SIZE_BINARY,
        types.Type_LARGE_BINARY,
    ]:  # 14, 15, 35
        return "varbinary", 1073741824, 0
    if type_.id in [types.Type_DATE32, types.Type_DATE64]:  # 16, 17
        return "date", 0, 0
    if type_.id == types.Type_TIMESTAMP:  # 18
        return "timestamp", 3, 0
    if type_.id in [types.Type_DECIMAL128, types.Decimal256Type]:  # 23, 24
        type_ = cast(types.Decimal128Type, type_)
        return "decimal", type_.precision, type_.scale
    if type_.id in [
        types.Type_LIST,
        types.Type_FIXED_SIZE_LIST,
        types.Type_LARGE_LIST,
    ]:  # 25, 32, 36
        return "array", 0, 0
    if type_.id in [types.Type_STRUCT]:  # 26
        return "row", 0, 0
    if type_.id in [types.Type_MAP]:  # 30
        return "map", 0, 0
    return "string", 2147483647, 0
