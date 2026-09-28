# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
"""Expected query results for the shared tables in ``tests.pyathena.tables``.

How to read this module:

- ``ExpectedResult`` is the entry point. It builds a query of a table's columns,
  optionally followed by ``CastColumn`` items, and returns what a cursor should
  give for it: ``rows()``, ``description()``, ``dbapi_types()``, and the column
  types of pandas, Arrow, and Polars results.
- The ``*_VALUES`` dicts state how a cursor returns a value of each type. They
  are keyed by base type, the Athena type without its parameters, such as
  ``decimal`` for ``DECIMAL(10,1)``. Each rule takes the value from the table
  definition and the column's Athena type.
- The ``_*_TYPES`` dicts, ``arrow_type()``, and ``polars_type()`` give the
  column types of the pandas, Arrow, and Polars results.

The rules state what the cursors return; they never call PyAthena's converters.
They cover the column types of the shared tables. A column of another type
raises ``KeyError`` or ``NotImplementedError`` until a rule is added here.
"""

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, time, timezone
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa

from pyathena import BINARY, BOOLEAN, DATE, DATETIME, JSON, NUMBER, STRING, TIME
from tests.pyathena.tables import Column, Table

# Athena types


def base_type(athena_type: str) -> str:
    """Return an Athena type without its parameters.

    Args:
        athena_type: An Athena type, such as ``DECIMAL(10,1)`` or ``ARRAY<int>``.

    Returns:
        The lowercase base type, such as ``decimal`` or ``array``.
    """
    match = re.match(r"[a-z]+(?: [a-z]+)*", athena_type.strip().lower())
    assert match, athena_type
    return match.group()


def type_parameters(athena_type: str) -> tuple[int, ...]:
    """Return the numeric parameters of an Athena type.

    Args:
        athena_type: An Athena type, such as ``DECIMAL(10,1)``.

    Returns:
        The parameters of the outer type, such as ``(10, 1)``; empty for a type
        without them, such as ``ARRAY<DECIMAL(10,1)>``.
    """
    match = re.match(r"\s*[a-zA-Z ]+\(([\d,\s]+)\)", athena_type)
    return tuple(int(p) for p in match.group(1).split(",")) if match else ()


def type_arguments(athena_type: str) -> list[str]:
    """Return the types nested in an array, map, or struct type.

    Args:
        athena_type: An Athena type, such as ``MAP<int, int>``.

    Returns:
        The element type of an array, the key and value types of a map, or the
        field types of a struct; empty for other types.
    """
    if base_type(athena_type) == "struct":
        return [field_type for _, field_type in _struct_fields(athena_type)]
    return _split_arguments(athena_type)


def _split_arguments(athena_type: str) -> list[str]:
    """Split the text between a type's outer angle brackets at top-level commas.

    Args:
        athena_type: An Athena type, such as ``STRUCT<a: int, b: int>``.

    Returns:
        The arguments, such as ``["a: int", "b: int"]``; empty for a type
        without angle brackets.
    """
    if "<" not in athena_type:
        return []
    inner = athena_type[athena_type.index("<") + 1 : athena_type.rindex(">")]
    arguments, depth, start = [], 0, 0
    for i, char in enumerate(inner):
        if char in "<(":
            depth += 1
        elif char in ">)":
            depth -= 1
        elif char == "," and depth == 0:
            arguments.append(inner[start:i].strip())
            start = i + 1
    arguments.append(inner[start:].strip())
    return arguments


def _struct_fields(athena_type: str) -> list[tuple[str, str]]:
    """Return the fields of a struct type.

    Args:
        athena_type: A struct type, such as ``STRUCT<a: int, b: int>``.

    Returns:
        ``(name, type)`` pairs.
    """
    return [
        (name.strip(), field_type.strip())
        for name, field_type in (a.split(":", 1) for a in _split_arguments(athena_type))
    ]


# Cast columns


@dataclass(frozen=True)
class CastColumn:
    """A result column that the query casts from a table column.

    Attributes:
        name: The result column name.
        source: The table column to cast.
        athena_type: The type to cast to.
    """

    name: str
    source: str
    athena_type: str

    def sql(self) -> str:
        """Return the select-list item.

        Returns:
            The ``CAST`` expression with its alias.
        """
        return f"CAST({self.source} AS {self.athena_type}) AS {self.name}"


TIMESTAMP_TZ = CastColumn("col_timestamp_tz", "col_timestamp", "timestamp with time zone")
TIME_OF_TIMESTAMP = CastColumn("col_time", "col_timestamp", "time")
ARRAY_JSON = CastColumn("col_array_json", "col_array", "json")
MAP_JSON = CastColumn("col_map_json", "col_map", "json")

# Cursor descriptions

# (type code, precision, scale) in the cursor description, by base type. The
# precision of varchar(n) and the precision and scale of decimal(p,s) come from
# the type parameters.
_DESCRIPTION: Mapping[str, tuple[str, int, int]] = {
    "boolean": ("boolean", 0, 0),
    "tinyint": ("tinyint", 3, 0),
    "smallint": ("smallint", 5, 0),
    "int": ("integer", 10, 0),
    "bigint": ("bigint", 19, 0),
    "float": ("float", 17, 0),
    "double": ("double", 17, 0),
    "string": ("varchar", 2147483647, 0),
    "varchar": ("varchar", 0, 0),
    "timestamp": ("timestamp", 3, 0),
    "timestamp with time zone": ("timestamp with time zone", 3, 0),
    "time": ("time", 3, 0),
    "date": ("date", 0, 0),
    "binary": ("varbinary", 1073741824, 0),
    "array": ("array", 0, 0),
    "map": ("map", 0, 0),
    "struct": ("row", 0, 0),
    "decimal": ("decimal", 0, 0),
    "json": ("json", 0, 0),
}

_DBAPI_TYPES = {
    "boolean": BOOLEAN,
    "tinyint": NUMBER,
    "smallint": NUMBER,
    "int": NUMBER,
    "bigint": NUMBER,
    "float": NUMBER,
    "double": NUMBER,
    "decimal": NUMBER,
    "string": STRING,
    "varchar": STRING,
    "array": STRING,
    "map": STRING,
    "struct": STRING,
    "timestamp": DATETIME,
    "timestamp with time zone": DATETIME,
    "time": TIME,
    "date": DATE,
    "binary": BINARY,
    "json": JSON,
}

# Value rules

# A rule(value, athena_type) returns what a cursor gives for a value from the
# table definition. A cast column uses the rule of the type it casts to, with
# the value and type of its source column.
ValueRules = Mapping[str, Callable[[Any, str], Any]]


def _same(value: Any, athena_type: str) -> Any:
    """Return the value unchanged.

    Args:
        value: The value from the table definition.
        athena_type: The value's Athena type.

    Returns:
        The value.
    """
    return value


# Scalar types that the cursors return as the Python value itself.
_SCALAR_VALUES: ValueRules = dict.fromkeys(
    (
        "boolean",
        "tinyint",
        "smallint",
        "int",
        "bigint",
        "float",
        "double",
        "string",
        "varchar",
        "timestamp",
        "date",
        "binary",
        "decimal",
    ),
    _same,
)


def _athena_text(value: Any, athena_type: str) -> str:
    """Return a value as Athena renders it in a CSV result.

    Args:
        value: The value from the table definition.
        athena_type: The value's Athena type.

    Returns:
        The text, such as ``[1, 2]`` for an array, ``{1=2, 3=4}`` for a map,
        and ``{a=1, b=2}`` for a struct.

    Raises:
        NotImplementedError: For a nested scalar type other than integers and
            strings, whose rendering has no rule here.
    """
    name = base_type(athena_type)
    if value is None:
        return "null"
    if name == "array":
        (element_type,) = type_arguments(athena_type)
        return f"[{', '.join(_athena_text(e, element_type) for e in value)}]"
    if name == "map":
        key_type, value_type = type_arguments(athena_type)
        entries = (f"{_athena_text(k, key_type)}={_athena_text(x, value_type)}" for k, x in value)
        return f"{{{', '.join(entries)}}}"
    if name == "struct":
        entries = (f"{k}={_athena_text(value[k], t)}" for k, t in _struct_fields(athena_type))
        return f"{{{', '.join(entries)}}}"
    if name in ("tinyint", "smallint", "int", "bigint", "string", "varchar"):
        return str(value)
    raise NotImplementedError(f"No text rendering rule for {athena_type}.")


def _member_value(value: Any, athena_type: str) -> str | None:
    """Return a value nested in an array, map, or struct as the cursors parse it.

    Args:
        value: The nested value.
        athena_type: The value's Athena type.

    Returns:
        Its text rendering, or None for a null.
    """
    return None if value is None else _athena_text(value, athena_type)


def _check_scalar_members(athena_type: str) -> None:
    """Check that an array, map, or struct type nests only scalar types.

    Args:
        athena_type: The array, map, or struct type.

    Raises:
        NotImplementedError: For a nested array, map, or struct, which the
            cursors parse differently.
    """
    if any(base_type(t) in ("array", "map", "struct") for t in type_arguments(athena_type)):
        raise NotImplementedError(f"No rule for the nesting in {athena_type}.")


def _python_array(value: Any, athena_type: str) -> list[Any]:
    """Return an array as a cursor without type hints returns it.

    Args:
        value: The value from the table definition.
        athena_type: The array type.

    Returns:
        The parsed JSON if Athena's rendering is valid JSON, such as ``[1, 2]``;
        otherwise the elements' text, such as ``["a", "b"]`` for ``[a, b]``,
        with None for a null element.
    """
    _check_scalar_members(athena_type)
    (element_type,) = type_arguments(athena_type)
    try:
        return json.loads(_athena_text(value, athena_type))
    except ValueError:
        return [_member_value(e, element_type) for e in value]


def _python_map(value: Any, athena_type: str) -> dict[str, str | None]:
    """Return a map as a cursor without type hints returns it.

    Args:
        value: The value from the table definition.
        athena_type: The map type.

    Returns:
        The keys and values as their text renderings; a null value is None.
    """
    _check_scalar_members(athena_type)
    key_type, value_type = type_arguments(athena_type)
    return {_athena_text(k, key_type): _member_value(x, value_type) for k, x in value}


def _python_struct(value: Any, athena_type: str) -> dict[str, str | None]:
    """Return a struct as a cursor without type hints returns it.

    Args:
        value: The value from the table definition.
        athena_type: The struct type.

    Returns:
        The field values as their text renderings, in declared order; a null
        value is None.
    """
    _check_scalar_members(athena_type)
    return {k: _member_value(value[k], t) for k, t in _struct_fields(athena_type)}


def _json_text(value: Any, athena_type: str) -> str:
    """Return a value cast to JSON, as the compact JSON text in a CSV result.

    Args:
        value: The value from the table definition.
        athena_type: The value's Athena type.

    Returns:
        The JSON text; a map becomes an object with string keys.
    """
    return json.dumps(
        dict(value) if base_type(athena_type) == "map" else value, separators=(",", ":")
    )


def _parsed_json(value: Any, athena_type: str) -> Any:
    """Return a value cast to JSON, after JSON parsing.

    Args:
        value: The value from the table definition.
        athena_type: The value's Athena type.

    Returns:
        The parsed JSON value; a map becomes a dict with string keys.
    """
    return json.loads(_json_text(value, athena_type))


def _native(value: Any, athena_type: str) -> Any:
    """Return an array, map, or struct as read from Parquet.

    Args:
        value: The value from the table definition.
        athena_type: The array, map, or struct type.

    Returns:
        An array as a list, a map as a list of key-value tuples, and a struct as
        a dict.
    """
    _check_scalar_members(athena_type)
    return dict(value) if base_type(athena_type) == "struct" else list(value)


# Rows of Cursor, S3FSCursor, PolarsCursor, pyathena.pandas.util.as_pandas, and
# SQLAlchemy.
# Without type hints, the cursors parse Athena's text rendering of arrays, maps,
# and structs, so nested values that are not JSON are strings.
PYTHON_VALUES: ValueRules = {
    **_SCALAR_VALUES,
    "timestamp with time zone": lambda v, t: v.replace(tzinfo=timezone.utc),
    "time": lambda v, t: v.time(),
    "array": _python_array,
    "map": _python_map,
    "struct": _python_struct,
    "json": _parsed_json,
}

_TEXT_VALUES: ValueRules = dict.fromkeys(("array", "map", "struct"), _athena_text)
_NATIVE_VALUES: ValueRules = dict.fromkeys(("array", "map", "struct"), _native)

# Rows and DataFrames of PandasCursor.
PANDAS_VALUES: ValueRules = {
    **_SCALAR_VALUES,
    **_TEXT_VALUES,
    "timestamp": lambda v, t: pd.Timestamp(v),
    "date": lambda v, t: pd.Timestamp(v),
    "time": lambda v, t: v.time(),
    "json": _parsed_json,
}

# Rows of ArrowCursor.
ARROW_VALUES: ValueRules = {
    **_SCALAR_VALUES,
    **_TEXT_VALUES,
    "time": lambda v, t: v.time(),
    "json": _parsed_json,
}

# Arrow tables and Polars DataFrames of ArrowCursor. Types without an Arrow
# equivalent in the CSV reader are strings, and dates are timestamps.
ARROW_TABLE_VALUES: ValueRules = {
    **_SCALAR_VALUES,
    **_TEXT_VALUES,
    "time": lambda v, t: v.strftime("%H:%M:%S.%f")[:-3],
    "date": lambda v, t: datetime.combine(v, time()),
    "binary": lambda v, t: " ".join(f"{b:02x}" for b in v),
    "json": _json_text,
    "decimal": lambda v, t: str(v),
}

# Rows, Arrow tables, and DataFrames of the pandas and Arrow cursors with
# unload=True, which read Parquet. Compare pandas rows through
# ExpectedResult.with_array_lists().
UNLOAD_VALUES: ValueRules = {
    **_SCALAR_VALUES,
    **_NATIVE_VALUES,
    "timestamp": lambda v, t: pd.Timestamp(v),
}

# Polars DataFrames of ArrowCursor with unload=True, which hold a map as a list
# of key-value dicts.
UNLOAD_POLARS_VALUES: ValueRules = {
    **_SCALAR_VALUES,
    **_NATIVE_VALUES,
    "map": lambda v, t: [{"key": k, "value": x} for k, x in _native(v, t)],
}

# Column types that the PolarsCursor tests leave out.
POLARS_EXCLUDED_TYPES = frozenset(("binary", "array", "map", "struct"))

# pandas, Arrow, and Polars column types

# pandas 3 infers its "str" dtype for strings; pandas 2 uses object columns.
STRING_TYPE = pd.Series(["a"]).dtype.type

# The dtype.type of a PandasCursor column, by base type.
_PANDAS_TYPES = {
    "boolean": np.bool_,
    "tinyint": np.int64,
    "smallint": np.int64,
    "int": np.int64,
    "bigint": np.int64,
    "float": np.float64,
    "double": np.float64,
    "string": STRING_TYPE,
    "varchar": STRING_TYPE,
    "timestamp": np.datetime64,
    "date": np.datetime64,
    "time": np.object_,
    "binary": np.object_,
    "array": STRING_TYPE,
    "map": STRING_TYPE,
    "struct": STRING_TYPE,
    "json": np.object_,
    "decimal": np.object_,
}
_PANDAS_UNLOAD_TYPES = {
    **_PANDAS_TYPES,
    "tinyint": np.int8,
    "smallint": np.int16,
    "int": np.int32,
    "float": np.float32,
    **dict.fromkeys(("date", "binary", "array", "map", "struct", "decimal"), np.object_),
}

# The Arrow type of an ArrowCursor column, by base type.
_ARROW_TYPES = {
    "boolean": pa.bool_(),
    "tinyint": pa.int8(),
    "smallint": pa.int16(),
    "int": pa.int32(),
    "bigint": pa.int64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "string": pa.string(),
    "varchar": pa.string(),
    "timestamp": pa.timestamp("ms"),
    "date": pa.timestamp("ms"),
    **dict.fromkeys(("time", "binary", "array", "map", "struct", "json", "decimal"), pa.string()),
}
_ARROW_UNLOAD_TYPES = {
    **_ARROW_TYPES,
    "timestamp": pa.timestamp("ns"),
    "date": pa.date32(),
    "binary": pa.binary(),
}

# The Polars type of a PolarsCursor column, by base type.
_POLARS_TYPES = {
    "boolean": pl.Boolean,
    "tinyint": pl.Int8,
    "smallint": pl.Int16,
    "int": pl.Int32,
    "bigint": pl.Int64,
    "float": pl.Float32,
    "double": pl.Float64,
    "string": pl.String,
    "varchar": pl.String,
    "timestamp": pl.Datetime("us"),
    "date": pl.Date,
}


def arrow_type(athena_type: str, unload: bool = False) -> pa.DataType:
    """Return the Arrow type of an ArrowCursor column.

    Args:
        athena_type: The column's Athena type.
        unload: Whether the cursor reads Parquet written by UNLOAD.

    Returns:
        The Arrow type.
    """
    name = base_type(athena_type)
    if not unload:
        return _ARROW_TYPES[name]
    if name == "decimal":
        return pa.decimal128(*type_parameters(athena_type))
    if name == "array":
        (element_type,) = type_arguments(athena_type)
        return pa.list_(pa.field("array_element", arrow_type(element_type, unload)))
    if name == "map":
        key_type, value_type = (arrow_type(t, unload) for t in type_arguments(athena_type))
        return pa.map_(key_type, pa.field("entries", value_type))
    if name == "struct":
        return pa.struct(
            [pa.field(n, arrow_type(t, unload)) for n, t in _struct_fields(athena_type)]
        )
    return _ARROW_UNLOAD_TYPES[name]


def polars_type(arrow: pa.DataType) -> pl.DataType:
    """Return the Polars type of a DataFrame column converted from an Arrow type.

    Args:
        arrow: The Arrow type.

    Returns:
        The Polars type.
    """
    if pa.types.is_timestamp(arrow):
        return pl.Datetime(arrow.unit)
    if pa.types.is_decimal(arrow):
        return pl.Decimal(arrow.precision, arrow.scale)
    if pa.types.is_map(arrow):
        entry = [
            pl.Field("key", polars_type(arrow.key_type)),
            pl.Field("value", polars_type(arrow.item_type)),
        ]
        return pl.List(pl.Struct(entry))
    if pa.types.is_list(arrow):
        return pl.List(polars_type(arrow.value_type))
    if pa.types.is_struct(arrow):
        return pl.Struct([pl.Field(f.name, polars_type(f.type)) for f in arrow])
    return {
        pa.bool_(): pl.Boolean,
        pa.int8(): pl.Int8,
        pa.int16(): pl.Int16,
        pa.int32(): pl.Int32,
        pa.int64(): pl.Int64,
        pa.float32(): pl.Float32,
        pa.float64(): pl.Float64,
        pa.string(): pl.String,
        pa.date32(): pl.Date,
        pa.binary(): pl.Binary,
    }[arrow]


# The expected result of a query


@dataclass(frozen=True)
class ExpectedResult:
    """A query of a table's columns, followed by cast columns, and its expected result.

    Attributes:
        table: The table.
        casts: The cast columns, after the table columns.
        exclude_types: Base types of the table columns to leave out.
    """

    table: Table
    casts: tuple[CastColumn, ...] = ()
    exclude_types: frozenset[str] = frozenset()

    @property
    def columns(self) -> list[Column]:
        """The selected table columns."""
        return [c for c in self.table.columns if base_type(c.athena_type) not in self.exclude_types]

    @property
    def names(self) -> list[str]:
        """The result column names."""
        return [c.name for c in self.columns] + [c.name for c in self.casts]

    @property
    def sql(self) -> str:
        """The query."""
        items = [c.name for c in self.columns] + [c.sql() for c in self.casts]
        return f"SELECT {', '.join(items)} FROM {self.table.name}"

    def _sources(self) -> list[tuple[str, int, str]]:
        """Return where each result column's value comes from.

        Returns:
            ``(athena_type, source_index, source_type)`` per result column: the
            column's type, and the index and type of the table column whose value
            it holds. A table column is its own source.
        """
        index = {c.name: i for i, c in enumerate(self.table.columns)}
        by_name = {c.name: c for c in self.table.columns}
        return [(c.athena_type, index[c.name], c.athena_type) for c in self.columns] + [
            (c.athena_type, index[c.source], by_name[c.source].athena_type) for c in self.casts
        ]

    def _types(self) -> list[str]:
        """Return the Athena type of each result column.

        Returns:
            The types in result order.
        """
        return [athena_type for athena_type, _, _ in self._sources()]

    def description(self, unload: bool = False) -> list[tuple[Any, ...]]:
        """Return the expected cursor description.

        Args:
            unload: Whether the cursor reads Parquet written by UNLOAD, whose
                columns are nullable and whose varchar columns lose their length.

        Returns:
            One DB API description tuple per result column.
        """
        result = []
        for name, athena_type in zip(self.names, self._types(), strict=True):
            name_type = base_type(athena_type)
            if unload and name_type == "varchar":
                name_type = "string"
            code, precision, scale = _DESCRIPTION[name_type]
            if name_type in ("varchar", "decimal"):
                precision, scale = (*type_parameters(athena_type), 0)[:2]
            null_ok = "NULLABLE" if unload else "UNKNOWN"
            result.append((name, code, None, None, precision, scale, null_ok))
        return result

    def dbapi_types(self) -> list[Any]:
        """Return the expected DB API type object of each result column.

        Returns:
            The type objects in result order.
        """
        return [_DBAPI_TYPES[base_type(t)] for t in self._types()]

    def rows(self, values: ValueRules) -> list[tuple[Any, ...]]:
        """Return the expected rows.

        Args:
            values: The value rules of the cursor under test, such as
                ``PYTHON_VALUES``.

        Returns:
            One tuple per row of the table.
        """
        sources = self._sources()
        return [
            tuple(
                values[base_type(athena_type)](row[index], source_type)
                for athena_type, index, source_type in sources
            )
            for row in self.table.rows
        ]

    def with_array_lists(self, row: Any) -> tuple[Any, ...]:
        """Return a result row with the NumPy arrays of array columns as lists.

        pandas returns array values read from Parquet as NumPy arrays, which do
        not compare with ``==``. Other columns keep their values.

        Args:
            row: A result row, or a pandas row from ``DataFrame.iterrows``.

        Returns:
            The row as a tuple.
        """
        return tuple(
            list(v) if base_type(t) == "array" and isinstance(v, np.ndarray) else v
            for t, v in zip(self._types(), row, strict=True)
        )

    def dicts(self, values: ValueRules) -> list[dict[str, Any]]:
        """Return the expected rows as dicts, as ``polars.DataFrame.to_dicts`` returns them.

        Args:
            values: The value rules of the cursor under test.

        Returns:
            One dict per row of the table.
        """
        return [dict(zip(self.names, row, strict=True)) for row in self.rows(values)]

    def pandas_types(self, unload: bool = False) -> list[type]:
        """Return the expected ``dtype.type`` of each PandasCursor column.

        Args:
            unload: Whether the cursor reads Parquet written by UNLOAD.

        Returns:
            The types in result order.
        """
        types_ = _PANDAS_UNLOAD_TYPES if unload else _PANDAS_TYPES
        return [types_[base_type(t)] for t in self._types()]

    def arrow_schema(self, unload: bool = False) -> pa.Schema:
        """Return the expected schema of an ArrowCursor table.

        Args:
            unload: Whether the cursor reads Parquet written by UNLOAD.

        Returns:
            The schema.
        """
        return pa.schema(
            [
                pa.field(n, arrow_type(t, unload))
                for n, t in zip(self.names, self._types(), strict=True)
            ]
        )

    def polars_types(self) -> list[pl.DataType]:
        """Return the expected type of each PolarsCursor column.

        Returns:
            The types in result order.
        """
        return [
            pl.Decimal(*type_parameters(t))
            if base_type(t) == "decimal"
            else _POLARS_TYPES[base_type(t)]
            for t in self._types()
        ]
