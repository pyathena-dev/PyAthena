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
        The parameters, such as ``(10, 1)``; empty for a type without them.
    """
    match = re.search(r"\(([\d,\s]+)\)", athena_type)
    return tuple(int(p) for p in match.group(1).split(",")) if match else ()


def _element_type(athena_type: str) -> str:
    """Return the element type of an array type.

    Args:
        athena_type: An array type, such as ``ARRAY<int>``.

    Returns:
        The element type, such as ``int``.
    """
    return athena_type[athena_type.index("<") + 1 : athena_type.rindex(">")]


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


def _same(value: Any, athena_type: str) -> Any:
    """Return the value unchanged.

    Args:
        value: The value from the table definition.
        athena_type: The value's Athena type.

    Returns:
        The value.
    """
    return value


def _as_list(value: Any, athena_type: str) -> Any:
    """Return an array value as a list.

    Args:
        value: The value from the table definition.
        athena_type: The value's Athena type.

    Returns:
        The elements as a list.
    """
    return list(value)


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
# Without type hints, map and struct values are strings.
PYTHON: Representation = {
    **_SCALARS,
    "timestamp with time zone": lambda v, t: v.replace(tzinfo=timezone.utc),
    "time": lambda v, t: v.time(),
    "array": _as_list,
    "map": lambda v, t: {str(k): str(x) for k, x in v},
    "struct": lambda v, t: {k: str(x) for k, x in v.items()},
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
        element = _element_type(column.athena_type)
        assert isinstance(sqlalchemy_type.item_type, _SQLALCHEMY_TYPES[family(element)])
