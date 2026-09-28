# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
"""Expected query results for the shared tables in ``tests.pyathena.tables``.

The expectations are derived from a table's column types and row values with
explicit rules per Athena type family, such as ``array`` or ``decimal``. The
rules state what a cursor returns; they never call PyAthena's converters.
"""

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import timezone
from typing import Any

from sqlalchemy import types

from pyathena import BINARY, BOOLEAN, DATE, DATETIME, JSON, NUMBER, STRING, TIME
from pyathena.sqlalchemy.types import TINYINT, AthenaArray, AthenaStruct
from tests.pyathena.tables import Column, Table


def family(athena_type: str) -> str:
    """Return the type family, the type name without its parameters.

    Args:
        athena_type: An Athena type, such as ``DECIMAL(10,1)`` or ``ARRAY<int>``.

    Returns:
        The lowercase family, such as ``decimal`` or ``array``.
    """
    match = re.match(r"[a-z]+(?: [a-z]+)*", athena_type.strip().lower())
    assert match, athena_type
    return match.group()


def _parameters(athena_type: str) -> tuple[int, ...]:
    """Return the numeric parameters of a type.

    Args:
        athena_type: An Athena type, such as ``DECIMAL(10,1)``.

    Returns:
        The parameters of the outer type, such as ``(10, 1)``; empty for a type
        without them, such as ``ARRAY<DECIMAL(10,1)>``.
    """
    match = re.match(r"\s*[a-zA-Z ]+\(([\d,\s]+)\)", athena_type)
    return tuple(int(p) for p in match.group(1).split(",")) if match else ()


def _type_arguments(athena_type: str) -> list[str]:
    """Return the type arguments of a complex type.

    Args:
        athena_type: An Athena type, such as ``MAP<int, int>``.

    Returns:
        The element type of an array, the key and value types of a map, or the
        ``name: type`` fields of a struct; empty for other types.
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
        for name, field_type in (a.split(":", 1) for a in _type_arguments(athena_type))
    ]


@dataclass(frozen=True)
class Cast:
    """A column the query casts from a table column.

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


TIMESTAMP_TZ = Cast("col_timestamp_tz", "col_timestamp", "timestamp with time zone")
TIME_OF_TIMESTAMP = Cast("col_time", "col_timestamp", "time")
ARRAY_JSON = Cast("col_array_json", "col_array", "json")
MAP_JSON = Cast("col_map_json", "col_map", "json")

# (type code, precision, scale) in the cursor description, by type family.
# The precision of varchar(n) and the precision and scale of decimal(p,s) come
# from the type parameters.
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

# The SQLAlchemy type class a reflected column has, by type family.
_SQLALCHEMY_TYPES = {
    "boolean": types.BOOLEAN,
    "tinyint": TINYINT,
    "smallint": types.SMALLINT,
    "int": types.INTEGER,
    "bigint": types.BIGINT,
    "float": types.FLOAT,
    "double": types.DOUBLE,
    "string": types.String,
    "varchar": types.VARCHAR,
    "timestamp": types.TIMESTAMP,
    "date": types.DATE,
    "binary": types.BINARY,
    "array": AthenaArray,
    "map": types.String,
    "struct": AthenaStruct,
    "decimal": types.DECIMAL,
}


def _parsed_json(value: Any, athena_type: str) -> Any:
    """Return a value as Athena renders it with ``CAST(... AS json)``, after JSON parsing.

    Args:
        value: The value from the table definition.
        athena_type: The value's Athena type.

    Returns:
        The parsed JSON value; a map becomes a dict with string keys.
    """
    if family(athena_type) == "map":
        value = dict(value)
    return json.loads(json.dumps(value))


def _athena_text(value: Any, athena_type: str) -> str:
    """Return a value as Athena renders it in a CSV result.

    Args:
        value: The value from the table definition.
        athena_type: The value's Athena type.

    Returns:
        The text, such as ``[1, 2]`` for an array, ``{1=2, 3=4}`` for a map,
        and ``{a=1, b=2}`` for a struct.

    Raises:
        NotImplementedError: For a type without a rendering rule here, such as
            float and double, whose rendering follows Java's formatting.
    """
    name = family(athena_type)
    arguments = _type_arguments(athena_type)
    if value is None:
        return "null"
    if name == "array":
        return f"[{', '.join(_athena_text(e, arguments[0]) for e in value)}]"
    if name == "map":
        key_type, value_type = arguments
        entries = (f"{_athena_text(k, key_type)}={_athena_text(x, value_type)}" for k, x in value)
        return f"{{{', '.join(entries)}}}"
    if name == "struct":
        items = _struct_items(value, athena_type)
        return f"{{{', '.join(f'{k}={_athena_text(x, t)}' for k, x, t in items)}}}"
    if name == "boolean":
        return str(value).lower()
    if name in ("tinyint", "smallint", "int", "bigint", "string", "varchar", "date"):
        return str(value)
    if name == "timestamp":
        return value.isoformat(sep=" ", timespec="milliseconds")
    if name == "binary":
        return " ".join(f"{b:02x}" for b in value)
    if name == "decimal":
        return f"{value:.{_parameters(athena_type)[1]}f}"
    raise NotImplementedError(f"No text rendering rule for {athena_type}.")


_COMPLEX_FAMILIES = ("array", "map", "struct")

# The scalar types the rules render when nested in an array, map, or struct.
_NESTED_SCALAR_FAMILIES = (
    "boolean",
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "string",
    "varchar",
    "date",
    "timestamp",
    "binary",
    "decimal",
)


def _struct_items(value: Mapping[str, Any], athena_type: str) -> list[tuple[str, Any, str]]:
    """Return a struct value's fields in declared order.

    Args:
        value: The struct value from the table definition; a missing field is
            null, as in the generated Parquet data.
        athena_type: The struct type.

    Returns:
        ``(name, value, type)`` per declared field.

    Raises:
        ValueError: If the value has a field that the type does not declare.
    """
    fields = _struct_fields(athena_type)
    if unknown := set(value) - {name for name, _ in fields}:
        raise ValueError(f"Undeclared struct fields {sorted(unknown)} for {athena_type}.")
    return [(name, value.get(name), field_type) for name, field_type in fields]


def _member_types(athena_type: str) -> list[str]:
    """Return the types nested directly in a complex type.

    Args:
        athena_type: An array, map, or struct type.

    Returns:
        The element type, the key and value types, or the field types.
    """
    if family(athena_type) == "struct":
        return [field_type for _, field_type in _struct_fields(athena_type)]
    return _type_arguments(athena_type)


def _check_nesting(athena_type: str) -> None:
    """Check that the rules model the nesting of a complex type.

    They model scalars of ``_NESTED_SCALAR_FAMILIES`` nested in an array, map,
    or struct, and arrays of maps or structs of such scalars.

    Args:
        athena_type: An array, map, or struct type.

    Raises:
        NotImplementedError: For deeper nesting or another nested scalar type;
            add a rule for it here together with the column that needs it.
    """
    members = _member_types(athena_type)
    if family(athena_type) == "array" and family(members[0]) in ("map", "struct"):
        members = _member_types(members[0])
    if any(family(m) not in _NESTED_SCALAR_FAMILIES for m in members):
        raise NotImplementedError(f"No expectation rule for the nesting in {athena_type}.")


def _element_text(value: Any, athena_type: str) -> str | None:
    """Return a nested scalar value as a cursor without type hints returns it.

    Args:
        value: The nested value.
        athena_type: The value's Athena type, a scalar type.

    Returns:
        Athena's text rendering of the value, or None for a null.

    Raises:
        ValueError: For a value that the cursors might not parse back as
            itself: a string other than words of letters, digits, and
            underscores separated by single spaces, the word null, or a value
            rendered as empty text, such as empty binary.
    """
    if value is None:
        return None
    if family(athena_type) in ("string", "varchar") and (
        not re.fullmatch(r"\w+(?: \w+)*", value) or value.lower() == "null"
    ):
        raise ValueError(f"Unsupported nested string value: {value!r}")
    text = _athena_text(value, athena_type)
    if not text:
        raise ValueError(f"Unsupported nested value rendered as empty text: {value!r}")
    return text


def _python_array(value: Any, athena_type: str) -> Any:
    """Return an array as a cursor without type hints returns it.

    Args:
        value: The value from the table definition.
        athena_type: The array type.

    Returns:
        None for a null. Otherwise the parsed JSON if Athena's rendering is
        valid JSON, such as ``[1, 2]``, or else a list of the elements, where a
        map or struct element is a dict and any other element is its text
        rendering.
    """
    _check_nesting(athena_type)
    if value is None:
        return None
    (element_type,) = _type_arguments(athena_type)
    element_family = family(element_type)
    if element_family == "map":
        return [_python_map(e, element_type) for e in value]
    if element_family == "struct":
        return [_python_struct(e, element_type) for e in value]
    elements = [_element_text(e, element_type) for e in value]
    try:
        return json.loads(_athena_text(value, athena_type))
    except ValueError:
        return elements


def _python_map(value: Any, athena_type: str) -> dict[str, Any] | None:
    """Return a map as a cursor without type hints returns it.

    Args:
        value: The value from the table definition.
        athena_type: The map type.

    Returns:
        None for a null; otherwise the keys and values as their text renderings.
    """
    _check_nesting(athena_type)
    if value is None:
        return None
    key_type, value_type = _type_arguments(athena_type)
    return {_element_text(k, key_type): _element_text(x, value_type) for k, x in value}


def _python_struct(value: Any, athena_type: str) -> dict[str, Any] | None:
    """Return a struct as a cursor without type hints returns it.

    Args:
        value: The value from the table definition.
        athena_type: The struct type.

    Returns:
        None for a null; otherwise the field values as their text renderings.
    """
    _check_nesting(athena_type)
    if value is None:
        return None
    return {k: _element_text(x, t) for k, x, t in _struct_items(value, athena_type)}


def _same(value: Any, athena_type: str) -> Any:
    """Return the value unchanged.

    Args:
        value: The value from the table definition.
        athena_type: The value's Athena type.

    Returns:
        The value.
    """
    return value


# A representation maps a type family to a rule(value, athena_type) that
# returns what the cursor gives for that value. Cast columns use the rule of
# the target family with the source column's value and type.
Representation = Mapping[str, Callable[[Any, str], Any]]

_SCALARS: Representation = dict.fromkeys(
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

# Rows of Cursor, S3FSCursor, pyathena.pandas.util.as_pandas, and SQLAlchemy.
# Without type hints, the cursors parse Athena's text rendering of arrays, maps,
# and structs, so nested values that are not JSON are strings.
PYTHON: Representation = {
    **_SCALARS,
    "timestamp with time zone": lambda v, t: v.replace(tzinfo=timezone.utc),
    "time": lambda v, t: v.time(),
    "array": _python_array,
    "map": _python_map,
    "struct": _python_struct,
    "json": _parsed_json,
}


@dataclass(frozen=True)
class Selection:
    """A query of a table's columns, followed by cast columns.

    Attributes:
        table: The table.
        casts: The cast columns, after the table columns.
    """

    table: Table
    casts: tuple[Cast, ...] = ()

    @property
    def names(self) -> list[str]:
        """The result column names."""
        return [c.name for c in self.table.columns] + [c.name for c in self.casts]

    @property
    def sql(self) -> str:
        """The query."""
        items = [c.name for c in self.table.columns] + [c.sql() for c in self.casts]
        return f"SELECT {', '.join(items)} FROM {self.table.name}"

    def _items(self) -> list[tuple[str, str, int, str]]:
        """Return how each result column is derived from the table.

        Returns:
            ``(name, athena_type, source_index, source_type)`` per result column,
            where the source is the table column the value comes from. A table
            column is its own source.
        """
        columns = self.table.columns
        index = {c.name: i for i, c in enumerate(columns)}
        return [(c.name, c.athena_type, i, c.athena_type) for i, c in enumerate(columns)] + [
            (c.name, c.athena_type, index[c.source], columns[index[c.source]].athena_type)
            for c in self.casts
        ]

    def description(self) -> list[tuple[Any, ...]]:
        """Return the expected cursor description.

        Returns:
            One DB API description tuple per result column.
        """
        result = []
        for name, athena_type, _, _ in self._items():
            code, precision, scale = _DESCRIPTION[family(athena_type)]
            if parameters := _parameters(athena_type):
                precision, scale = (*parameters, 0)[:2]
            result.append((name, code, None, None, precision, scale, "UNKNOWN"))
        return result

    def dbapi_types(self) -> list[Any]:
        """Return the expected DB API type object of each result column.

        Returns:
            The type objects in result order.
        """
        return [_DBAPI_TYPES[family(athena_type)] for _, athena_type, _, _ in self._items()]

    def rows(self, representation: Representation) -> list[tuple[Any, ...]]:
        """Return the expected rows.

        Args:
            representation: The rules of the cursor under test, such as ``PYTHON``.

        Returns:
            One tuple per row of the table.
        """
        items = self._items()
        return [
            tuple(
                representation[family(athena_type)](row[index], source_type)
                for _, athena_type, index, source_type in items
            )
            for row in self.table.rows
        ]


def assert_sqlalchemy_type(sqlalchemy_type: Any, column: Column) -> None:
    """Assert that a reflected SQLAlchemy type matches a table column.

    Args:
        sqlalchemy_type: The reflected column type.
        column: The column in the table definition.

    Raises:
        AssertionError: If the type class or its parameters differ.
    """
    name = family(column.athena_type)
    assert isinstance(sqlalchemy_type, _SQLALCHEMY_TYPES[name]), (column.name, sqlalchemy_type)
    if name == "varchar":
        assert sqlalchemy_type.length == _parameters(column.athena_type)[0], column.name
    elif name == "decimal":
        precision, scale = _parameters(column.athena_type)
        assert (sqlalchemy_type.precision, sqlalchemy_type.scale) == (precision, scale)
    elif name == "array":
        (element,) = _type_arguments(column.athena_type)
        assert isinstance(sqlalchemy_type.item_type, _SQLALCHEMY_TYPES[family(element)])
