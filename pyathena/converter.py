from __future__ import annotations

import binascii
import json
import logging
from abc import ABCMeta, abstractmethod
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from dateutil.tz import gettz

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


def _split_array_items(inner: str) -> list[str]:
    """Split array items by comma, respecting brace and bracket groupings.

    Args:
        inner: Interior content of array without brackets.

    Returns:
        List of item strings.
    """
    items = []
    current_item = ""
    brace_depth = 0
    bracket_depth = 0

    for char in inner:
        if char == "{":
            brace_depth += 1
        elif char == "}":
            brace_depth -= 1
        elif char == "[":
            bracket_depth += 1
        elif char == "]":
            bracket_depth -= 1
        elif char == "," and brace_depth == 0 and bracket_depth == 0:
            # Top-level comma - end current item
            items.append(current_item.strip())
            current_item = ""
            continue

        current_item += char

    # Add the last item
    if current_item.strip():
        items.append(current_item.strip())

    return items


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

    Use :func:`_convert_value_with_type` for type-aware conversion.

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


@dataclass
class TypeNode:
    """Parsed representation of an Athena DDL type signature.

    Represents a node in a type tree, where complex types (array, map, row)
    have children representing their element/field types.

    Attributes:
        type_name: The base type name (e.g., "array", "map", "row", "varchar").
        children: Child type nodes for complex types.
        field_names: Field names for row/struct types (parallel to children).
    """

    type_name: str
    children: list[TypeNode] = field(default_factory=list)
    field_names: list[str] | None = None


def _split_top_level(s: str, delimiter: str = ",") -> list[str]:
    """Split a string by delimiter, respecting nested parentheses.

    Args:
        s: String to split.
        delimiter: Character to split on.

    Returns:
        List of parts split at top-level delimiters.
    """
    parts: list[str] = []
    current: list[str] = []
    depth = 0

    for char in s:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == delimiter and depth == 0:
            parts.append("".join(current).strip())
            current = []
            continue
        current.append(char)

    if current:
        parts.append("".join(current).strip())
    return parts


def parse_type_signature(type_str: str) -> TypeNode:
    """Parse an Athena DDL type signature string into a TypeNode tree.

    Handles simple types (varchar, integer), parameterized types (decimal(10,2)),
    and complex types (array, map, row/struct) with arbitrary nesting.

    Args:
        type_str: Athena DDL type string (e.g., "array(row(name varchar, age integer))").

    Returns:
        TypeNode representing the parsed type tree.
    """
    type_str = type_str.strip()

    paren_idx = type_str.find("(")
    if paren_idx == -1:
        return TypeNode(type_name=type_str.lower())

    type_name = type_str[:paren_idx].strip().lower()

    # Find matching closing paren
    inner = type_str[paren_idx + 1 : -1].strip()

    if type_name in ("row", "struct"):
        parts = _split_top_level(inner)
        field_names: list[str] = []
        children: list[TypeNode] = []
        for part in parts:
            part = part.strip()
            # Split into field_name and type at first space
            space_idx = _find_field_name_boundary(part)
            if space_idx == -1:
                children.append(parse_type_signature(part))
                field_names.append(part)
            else:
                field_name = part[:space_idx].strip()
                type_part = part[space_idx + 1 :].strip()
                field_names.append(field_name)
                children.append(parse_type_signature(type_part))
        return TypeNode(type_name=type_name, children=children, field_names=field_names)

    if type_name == "array":
        child = parse_type_signature(inner)
        return TypeNode(type_name=type_name, children=[child])

    if type_name == "map":
        parts = _split_top_level(inner)
        if len(parts) == 2:
            key_type = parse_type_signature(parts[0])
            value_type = parse_type_signature(parts[1])
            return TypeNode(type_name=type_name, children=[key_type, value_type])
        return TypeNode(type_name=type_name)

    # Types with parameters like decimal(10, 2), varchar(255)
    return TypeNode(type_name=type_name)


def _find_field_name_boundary(part: str) -> int:
    """Find the boundary between field name and type in a row field definition.

    Handles cases like "name varchar" and "data row(x integer, y integer)".

    Args:
        part: A single field definition string.

    Returns:
        Index of the space separating field name from type, or -1 if not found.
    """
    depth = 0
    for i, char in enumerate(part):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == " " and depth == 0:
            return i
    return -1


def _convert_value_with_type(value: str, type_node: TypeNode) -> Any:
    """Convert a value using type information from a TypeNode.

    For complex types (array, map, row), parses the structure and
    recursively converts elements using child type information.
    For simple types, uses the standard converter function.

    Args:
        value: String value to convert.
        type_node: Parsed type information.

    Returns:
        Converted value.
    """
    if type_node.type_name == "array":
        return _convert_typed_array(value, type_node)
    if type_node.type_name == "map":
        return _convert_typed_map(value, type_node)
    if type_node.type_name in ("row", "struct"):
        return _convert_typed_struct(value, type_node)
    # Simple type: use the standard converter
    converter_fn = _DEFAULT_CONVERTERS.get(type_node.type_name, _to_default)
    return converter_fn(value)


def _convert_element(value: str, type_node: TypeNode) -> Any:
    """Convert a single element within a complex type using type information.

    Handles null values before delegating to type-specific conversion.

    Args:
        value: String value to convert.
        type_node: Type information for this element.

    Returns:
        Converted value, or None for null.
    """
    if value.lower() == "null":
        return None
    return _convert_value_with_type(value, type_node)


def _convert_typed_array(value: str, type_node: TypeNode) -> list[Any] | None:
    """Convert an array value using type information.

    Args:
        value: String representation of the array.
        type_node: Type node with array element type as first child.

    Returns:
        List of converted elements, or None if parsing fails.
    """
    if not (value.startswith("[") and value.endswith("]")):
        return None

    element_type = type_node.children[0] if type_node.children else TypeNode("varchar")

    # Try JSON first
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [
                None if elem is None else _convert_element(str(elem), element_type)
                for elem in parsed
            ]
    except json.JSONDecodeError:
        pass

    # Native format
    inner = value[1:-1].strip()
    if not inner:
        return []

    if "[" in inner:
        return None  # Nested arrays not supported in native format

    items = _split_array_items(inner)
    result: list[Any] = []
    for item in items:
        item = item.strip()
        if not item:
            continue
        if item.startswith("{") and item.endswith("}"):
            if element_type.type_name in ("row", "struct"):
                result.append(_convert_typed_struct(item, element_type))
            elif element_type.type_name == "map":
                result.append(_convert_typed_map(item, element_type))
            else:
                result.append(_to_struct(item))
        else:
            result.append(_convert_element(item, element_type))

    return result if result else None


def _convert_typed_map(value: str, type_node: TypeNode) -> dict[str, Any] | None:
    """Convert a map value using type information.

    Args:
        value: String representation of the map.
        type_node: Type node with key type and value type as children.

    Returns:
        Dictionary of converted key-value pairs, or None if parsing fails.
    """
    if not (value.startswith("{") and value.endswith("}")):
        return None

    key_type = type_node.children[0] if len(type_node.children) > 0 else TypeNode("varchar")
    value_type = type_node.children[1] if len(type_node.children) > 1 else TypeNode("varchar")

    # Try JSON first
    inner_preview = value[1:10] if len(value) > 10 else value[1:-1]
    if '"' in inner_preview or value.startswith('{"'):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return {
                    str(
                        _convert_element(str(k), key_type) if k is not None else k
                    ): _convert_element(str(v), value_type) if v is not None else None
                    for k, v in parsed.items()
                }
        except json.JSONDecodeError:
            pass

    # Native format
    inner = value[1:-1].strip()
    if not inner:
        return {}

    if any(char in inner for char in "()[]"):
        return None

    pairs = [pair.strip() for pair in inner.split(",")]
    result: dict[str, Any] = {}
    for pair in pairs:
        if "=" not in pair:
            continue
        k, v = pair.split("=", 1)
        k = k.strip()
        v = v.strip()
        if any(char in k for char in '{}="') or any(char in v for char in '{}="'):
            continue
        converted_key = _convert_element(k, key_type)
        converted_value = _convert_element(v, value_type)
        result[str(converted_key)] = converted_value

    return result if result else None


def _convert_typed_struct(value: str, type_node: TypeNode) -> dict[str, Any] | None:
    """Convert a struct/row value using type information.

    Args:
        value: String representation of the struct.
        type_node: Type node with field types and names.

    Returns:
        Dictionary of converted field values, or None if parsing fails.
    """
    if not (value.startswith("{") and value.endswith("}")):
        return None

    field_names = type_node.field_names or []
    field_types = type_node.children or []

    # Try JSON first
    inner_preview = value[1:10] if len(value) > 10 else value[1:-1]
    if '"' in inner_preview or value.startswith('{"'):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                result: dict[str, Any] = {}
                for i, (k, v) in enumerate(parsed.items()):
                    ft = field_types[i] if i < len(field_types) else TypeNode("varchar")
                    result[k] = _convert_element(str(v), ft) if v is not None else None
                return result
        except json.JSONDecodeError:
            pass

    inner = value[1:-1].strip()
    if not inner:
        return {}

    if "=" in inner:
        # Named struct
        pairs = _split_array_items(inner)
        result = {}
        field_index = 0
        for pair in pairs:
            if "=" not in pair:
                continue
            k, v = pair.split("=", 1)
            k = k.strip()
            v = v.strip()
            if any(char in k for char in '{}="'):
                continue

            ft = _get_field_type(k, field_names, field_types, field_index)
            field_index += 1

            if v.startswith("{") and v.endswith("}"):
                if ft.type_name in ("row", "struct"):
                    result[k] = _convert_typed_struct(v, ft)
                elif ft.type_name == "map":
                    result[k] = _convert_typed_map(v, ft)
                else:
                    result[k] = _to_struct(v)
            else:
                result[k] = _convert_element(v, ft)
        return result if result else None

    # Unnamed struct
    values = [v.strip() for v in inner.split(",")]
    result = {}
    for i, v in enumerate(values):
        ft = field_types[i] if i < len(field_types) else TypeNode("varchar")
        name = field_names[i] if i < len(field_names) else str(i)
        result[name] = _convert_element(v, ft)
    return result


def _get_field_type(
    field_name: str,
    field_names: list[str],
    field_types: list[TypeNode],
    field_index: int,
) -> TypeNode:
    """Look up the type for a struct field by name or index.

    Tries name-based lookup first, then falls back to positional index.

    Args:
        field_name: Name of the field to look up.
        field_names: List of known field names from the type hint.
        field_types: List of corresponding field types.
        field_index: Current positional index as fallback.

    Returns:
        TypeNode for the field, defaulting to varchar if not found.
    """
    if field_name in field_names:
        idx = field_names.index(field_name)
        if idx < len(field_types):
            return field_types[idx]
    if field_index < len(field_types):
        return field_types[field_index]
    return TypeNode("varchar")


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
        self._parsed_hints: dict[str, TypeNode] = {}

    def convert(self, type_: str, value: str | None, type_hint: str | None = None) -> Any | None:
        if value is None:
            return None
        if type_hint:
            type_node = self._get_or_parse_hint(type_hint)
            return _convert_value_with_type(value, type_node)
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
            self._parsed_hints[type_hint] = parse_type_signature(type_hint)
        return self._parsed_hints[type_hint]
