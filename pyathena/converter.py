from __future__ import annotations

import binascii
import json
import logging
from abc import ABCMeta, abstractmethod
from collections.abc import Callable
from copy import deepcopy
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from dateutil.tz import gettz

from pyathena.parser import TypedValueConverter, TypeNode, TypeSignatureParser, _split_array_items
from pyathena.util import strtobool

_logger = logging.getLogger(__name__)


def _to_date(value: str | datetime | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _to_datetime(varchar_value: str | None) -> datetime | None:
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, "%Y-%m-%d %H:%M:%S.%f")


def _to_datetime_with_tz(varchar_value: str | None) -> datetime | None:
    if varchar_value is None:
        return None
    datetime_, _, tz = varchar_value.rpartition(" ")
    return datetime.strptime(datetime_, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=gettz(tz))


def _to_time(varchar_value: str | None) -> time | None:
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, "%H:%M:%S.%f").time()


def _to_float(varchar_value: str | None) -> float | None:
    if varchar_value is None:
        return None
    return float(varchar_value)


def _to_int(varchar_value: str | None) -> int | None:
    if varchar_value is None:
        return None
    return int(varchar_value)


def _to_decimal(varchar_value: str | None) -> Decimal | None:
    if not varchar_value:
        return None
    return Decimal(varchar_value)


def _to_boolean(varchar_value: str | None) -> bool | None:
    if not varchar_value:
        return None
    return bool(strtobool(varchar_value))


def _to_binary(varchar_value: str | None) -> bytes | None:
    if varchar_value is None:
        return None
    return binascii.a2b_hex("".join(varchar_value.split(" ")))


def _to_json(varchar_value: str | None) -> Any | None:
    if varchar_value is None:
        return None
    return json.loads(varchar_value)


def _to_array(varchar_value: str | None) -> list[Any] | None:
    """Convert array data to Python list.

    Supports two formats:
    1. JSON format: '[1, 2, 3]' or '["a", "b", "c"]' (recommended)
    2. Athena native format: '[1, 2, 3]' (basic cases only)

    For complex arrays, use CAST(array_column AS JSON) in your SQL query.

    Args:
        varchar_value: String representation of array data

    Returns:
        List representation of array, or None if parsing fails
    """
    if varchar_value is None:
        return None

    # Quick check: if it doesn't look like an array, return None
    if not (varchar_value.startswith("[") and varchar_value.endswith("]")):
        return None

    # Optimize: Try JSON parsing first (most reliable)
    try:
        result = json.loads(varchar_value)
        if isinstance(result, list):
            return result
    except json.JSONDecodeError:
        # If JSON parsing fails, fall back to basic parsing for simple cases
        pass

    inner = varchar_value[1:-1].strip()
    if not inner:
        return []

    try:
        # For nested arrays, too complex for basic parsing
        if "[" in inner:
            # Contains nested arrays - too complex for basic parsing
            return None
        # Try native parsing (including struct arrays)
        return _parse_array_native(inner)
    except Exception:
        return None


def _to_map(varchar_value: str | None) -> dict[str, Any] | None:
    """Convert map data to Python dictionary.

    Supports two formats:
    1. JSON format: '{"key1": "value1", "key2": "value2"}' (recommended)
    2. Athena native format: '{key1=value1, key2=value2}' (basic cases only)

    For complex maps, use CAST(map_column AS JSON) in your SQL query.

    Args:
        varchar_value: String representation of map data

    Returns:
        Dictionary representation of map, or None if parsing fails
    """
    if varchar_value is None:
        return None

    # Quick check: if it doesn't look like a map, return None
    if not (varchar_value.startswith("{") and varchar_value.endswith("}")):
        return None

    # Optimize: Check if it looks like JSON vs Athena native format
    # JSON objects typically have quoted keys: {"key": value}
    # Athena native format has unquoted keys: {key=value}
    inner_preview = varchar_value[1:10] if len(varchar_value) > 10 else varchar_value[1:-1]

    if '"' in inner_preview or varchar_value.startswith('{"'):
        # Likely JSON format - try JSON parsing
        try:
            result = json.loads(varchar_value)
            return result if isinstance(result, dict) else None
        except json.JSONDecodeError:
            # If JSON parsing fails, fall back to native format parsing
            pass

    inner = varchar_value[1:-1].strip()
    if not inner:
        return {}

    try:
        # MAP format is always key=value pairs
        # But for complex structures, return None to keep as string
        if any(char in inner for char in "()[]"):
            # Contains complex structures (arrays, structs), skip parsing
            return None
        return _parse_map_native(inner)
    except Exception:
        return None


def _to_struct(varchar_value: str | None) -> dict[str, Any] | None:
    """Convert struct data to Python dictionary.

    Supports two formats:
    1. JSON format: '{"key": "value", "num": 123}' (recommended)
    2. Athena native format: '{key=value, num=123}' (basic cases only)

    For complex structs, use CAST(struct_column AS JSON) in your SQL query.

    Args:
        varchar_value: String representation of struct data

    Returns:
        Dictionary representation of struct, or None if parsing fails
    """
    if varchar_value is None:
        return None

    # Quick check: if it doesn't look like a struct, return None
    if not (varchar_value.startswith("{") and varchar_value.endswith("}")):
        return None

    # Optimize: Check if it looks like JSON vs Athena native format
    # JSON objects typically have quoted keys: {"key": value}
    # Athena native format has unquoted keys: {key=value}
    inner_preview = varchar_value[1:10] if len(varchar_value) > 10 else varchar_value[1:-1]

    if '"' in inner_preview or varchar_value.startswith('{"'):
        # Likely JSON format - try JSON parsing
        try:
            result = json.loads(varchar_value)
            return result if isinstance(result, dict) else None
        except json.JSONDecodeError:
            # If JSON parsing fails, fall back to native format parsing
            pass

    inner = varchar_value[1:-1].strip()
    if not inner:
        return {}

    try:
        if "=" in inner:
            # Named struct: {a=1, b=2}
            return _parse_named_struct(inner)
        # Unnamed struct: {Alice, 25}
        return _parse_unnamed_struct(inner)
    except Exception:
        return None


def _parse_array_native(inner: str) -> list[Any] | None:
    """Parse array native format: 1, 2, 3 or {a, b}, {c, d}.

    Args:
        inner: Interior content of array without brackets.

    Returns:
        List with parsed values, or None if no valid values found.
    """
    result = []

    # Smart split by comma - respect brace groupings
    items = _split_array_items(inner)

    for item in items:
        if not item:
            continue

        # Handle struct (ROW) values in format {a, b, c} or {key=value, ...}
        if item.strip().startswith("{") and item.strip().endswith("}"):
            # This is a struct value - parse it as a struct
            struct_value = _to_struct(item.strip())
            if struct_value is not None:
                result.append(struct_value)
            continue

        # Skip items with nested arrays or complex quoting (safety check)
        if any(char in item for char in '[]="'):
            continue

        # Convert item to appropriate type
        converted_item = _convert_value(item)
        result.append(converted_item)

    return result if result else None


def _parse_map_native(inner: str) -> dict[str, Any] | None:
    """Parse map native format: key1=value1, key2=value2.

    Args:
        inner: Interior content of map without braces.

    Returns:
        Dictionary with parsed key-value pairs, or None if no valid pairs found.
    """
    result = {}

    # Simple split by comma for basic cases
    pairs = [pair.strip() for pair in inner.split(",")]

    for pair in pairs:
        if "=" not in pair:
            continue

        key, value = pair.split("=", 1)
        key = key.strip()
        value = value.strip()

        # Skip pairs with special characters (safety check)
        if any(char in key for char in '{}="') or any(char in value for char in '{}="'):
            continue

        # Convert both key and value to appropriate types
        converted_key = _convert_value(key)
        converted_value = _convert_value(value)
        # Always use string keys for consistency with expected test behavior
        result[str(converted_key)] = converted_value

    return result if result else None


def _parse_named_struct(inner: str) -> dict[str, Any] | None:
    """Parse named struct format: key1=value1, key2=value2.

    Supports nested structs: outer={inner_key=inner_value}, field=value.

    Args:
        inner: Interior content of struct without braces.

    Returns:
        Dictionary with parsed key-value pairs, or None if no valid pairs found.
    """
    result = {}

    # Use smart split to handle nested structures
    pairs = _split_array_items(inner)

    for pair in pairs:
        if "=" not in pair:
            continue

        key, value = pair.split("=", 1)
        key = key.strip()
        value = value.strip()

        # Skip if key contains special characters (safety check)
        if any(char in key for char in '{}="'):
            continue

        # Handle nested struct values
        if value.startswith("{") and value.endswith("}"):
            # Try to parse as nested struct
            nested_struct = _to_struct(value)
            if nested_struct is not None:
                result[key] = nested_struct
                continue

        # Convert value to appropriate type
        result[key] = _convert_value(value)

    return result if result else None


def _parse_unnamed_struct(inner: str) -> dict[str, Any]:
    """Parse unnamed struct format: Alice, 25.

    Args:
        inner: Interior content of struct without braces.

    Returns:
        Dictionary with indexed keys mapping to parsed values.
    """
    values = [v.strip() for v in inner.split(",")]
    return {str(i): _convert_value(value) for i, value in enumerate(values)}


def _convert_value(value: str) -> Any:
    """Convert string value without type inference.

    Returns the string as-is, except for null which becomes None.
    This is a safe default that avoids incorrect type conversions
    (e.g., converting varchar "1234" to int 1234 inside complex types).

    Use :class:`~pyathena.parser.TypedValueConverter` for type-aware conversion.

    Args:
        value: String value to convert.

    Returns:
        None for "null" values, otherwise the original string.
    """
    if value.lower() == "null":
        return None
    return value


def _to_default(varchar_value: str | None) -> str | None:
    return varchar_value


_DEFAULT_CONVERTERS: dict[str, Callable[[str | None], Any | None]] = {
    "boolean": _to_boolean,
    "tinyint": _to_int,
    "smallint": _to_int,
    "integer": _to_int,
    "bigint": _to_int,
    "float": _to_float,
    "real": _to_float,
    "double": _to_float,
    "char": _to_default,
    "varchar": _to_default,
    "string": _to_default,
    "timestamp": _to_datetime,
    "timestamp with time zone": _to_datetime_with_tz,
    "date": _to_date,
    "time": _to_time,
    "varbinary": _to_binary,
    "array": _to_array,
    "map": _to_map,
    "row": _to_struct,
    "decimal": _to_decimal,
    "json": _to_json,
}


class Converter(metaclass=ABCMeta):
    """Abstract base class for converting Athena data types to Python objects.

    Converters handle the transformation of string values returned by Athena
    into appropriate Python data types. Different cursor implementations may
    use different converters to optimize for their specific use cases.

    This class provides a framework for mapping Athena data type names to
    conversion functions and handles the conversion process during result
    set processing.

    Attributes:
        mappings: Dictionary mapping Athena type names to conversion functions.
        default: Default conversion function for unmapped types.
        types: Optional dictionary mapping type names to Python type objects.
    """

    def __init__(
        self,
        mappings: dict[str, Callable[[str | None], Any | None]],
        default: Callable[[str | None], Any | None] = _to_default,
        types: dict[str, type[Any]] | None = None,
    ) -> None:
        if mappings:
            self._mappings = mappings
        else:
            self._mappings = {}
        self._default = default
        if types:
            self._types = types
        else:
            self._types = {}

    @property
    def mappings(self) -> dict[str, Callable[[str | None], Any | None]]:
        """Get the current type conversion mappings.

        Returns:
            Dictionary mapping Athena data types to conversion functions.
        """
        return self._mappings

    @property
    def types(self) -> dict[str, type[Any]]:
        """Get the current type mappings for result set descriptions.

        Returns:
            Dictionary mapping Athena data types to Python types.
        """
        return self._types

    def get(self, type_: str) -> Callable[[str | None], Any | None]:
        """Get the conversion function for a specific Athena data type.

        Args:
            type_: The Athena data type name.

        Returns:
            The conversion function for the type, or the default converter if not found.
        """
        return self.mappings.get(type_, self._default)

    def set(self, type_: str, converter: Callable[[str | None], Any | None]) -> None:
        """Set a custom conversion function for an Athena data type.

        Args:
            type_: The Athena data type name.
            converter: The conversion function to use for this type.
        """
        self.mappings[type_] = converter

    def remove(self, type_: str) -> None:
        """Remove a custom conversion function for an Athena data type.

        Args:
            type_: The Athena data type name to remove.
        """
        self.mappings.pop(type_, None)

    def get_dtype(self, type_: str, precision: int = 0, scale: int = 0) -> type[Any] | None:
        """Get the data type for a given Athena type.

        Subclasses may override this to provide custom type handling
        (e.g., for decimal types with precision and scale).

        Args:
            type_: The Athena data type name.
            precision: The precision for decimal types.
            scale: The scale for decimal types.

        Returns:
            The corresponding Python type, or None if not found.
        """
        return self._types.get(type_)

    def update(self, mappings: dict[str, Callable[[str | None], Any | None]]) -> None:
        """Update multiple conversion functions at once.

        Args:
            mappings: Dictionary of type names to conversion functions.
        """
        self.mappings.update(mappings)

    @abstractmethod
    def convert(self, type_: str, value: str | None, type_hint: str | None = None) -> Any | None:
        raise NotImplementedError  # pragma: no cover


class DefaultTypeConverter(Converter):
    """Default implementation of the Converter for standard Python types.

    This converter provides mappings for all standard Athena data types to
    their corresponding Python types using built-in conversion functions.
    It's used by the standard Cursor class by default.

    Supported conversions:
        - Numeric types: integer, bigint, real, double, decimal
        - String types: varchar, char
        - Date/time types: date, timestamp, time (with timezone support)
        - Boolean: boolean
        - Binary: varbinary
        - Complex types: array, map, row/struct
        - JSON: json

    When ``type_hint`` is provided (an Athena DDL type signature string like
    ``"array(row(name varchar, age integer))"``), nested values within complex
    types are converted according to the specified types instead of using
    heuristic inference.

    Example:
        >>> converter = DefaultTypeConverter()
        >>> converter.convert('integer', '42')
        42
        >>> converter.convert('date', '2023-01-15')
        datetime.date(2023, 1, 15)
        >>> converter.convert('array', '[1, 2, 3]', type_hint='array(varchar)')
        ['1', '2', '3']
    """

    def __init__(self) -> None:
        super().__init__(mappings=deepcopy(_DEFAULT_CONVERTERS), default=_to_default)
        self._parser = TypeSignatureParser()
        self._typed_converter = TypedValueConverter(
            converters=_DEFAULT_CONVERTERS,
            default_converter=_to_default,
            struct_parser=_to_struct,
        )
        self._parsed_hints: dict[str, TypeNode] = {}

    def convert(self, type_: str, value: str | None, type_hint: str | None = None) -> Any | None:
        if value is None:
            return None
        if type_hint:
            type_node = self._get_or_parse_hint(type_hint)
            return self._typed_converter.convert(value, type_node)
        converter = self.get(type_)
        return converter(value)

    def _get_or_parse_hint(self, type_hint: str) -> TypeNode:
        """Get or parse a type hint string into a TypeNode, with caching.

        Args:
            type_hint: Athena DDL type signature string.

        Returns:
            Parsed TypeNode.
        """
        if type_hint not in self._parsed_hints:
            self._parsed_hints[type_hint] = self._parser.parse(type_hint)
        return self._parsed_hints[type_hint]
