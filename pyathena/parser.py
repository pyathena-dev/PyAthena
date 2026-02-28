from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

# Aliases for Athena type names that differ between Hive DDL and Trino DDL.
_TYPE_ALIASES: dict[str, str] = {
    "int": "integer",
}

# Pattern for normalizing Hive-style type signatures to Trino-style.
# Matches angle brackets and colons used in Hive DDL (e.g., array<struct<a:int>>).
_HIVE_SYNTAX_RE: re.Pattern[str] = re.compile(r"[<>:]")
_HIVE_REPLACEMENTS: dict[str, str] = {"<": "(", ">": ")", ":": " "}


def _normalize_hive_syntax(type_str: str) -> str:
    """Normalize Hive-style DDL syntax to Trino-style.

    Converts angle-bracket notation (``array<struct<a:int>>``) to
    parenthesized notation (``array(struct(a int))``).

    Args:
        type_str: Type signature string, possibly using Hive syntax.

    Returns:
        Normalized type signature using Trino-style parenthesized notation.
    """
    if "<" not in type_str:
        return type_str
    return _HIVE_SYNTAX_RE.sub(lambda m: _HIVE_REPLACEMENTS[m.group()], type_str)


def _split_array_items(inner: str) -> list[str]:
    """Split array items by comma, respecting brace and bracket groupings.

    Args:
        inner: Interior content of array without brackets.

    Returns:
        List of item strings.
    """
    items: list[str] = []
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
            items.append(current_item.strip())
            current_item = ""
            continue

        current_item += char

    if current_item.strip():
        items.append(current_item.strip())

    return items


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
    _field_type_map: dict[str, TypeNode] | None = field(default=None, repr=False)

    def get_field_type(self, name: str) -> TypeNode | None:
        """Look up a child type node by field name using a cached dict.

        Returns:
            The TypeNode for the named field, or None if not found.
        """
        if self._field_type_map is None and self.field_names:
            self._field_type_map = {
                fn: self.children[i]
                for i, fn in enumerate(self.field_names)
                if i < len(self.children)
            }
        if self._field_type_map:
            return self._field_type_map.get(name)
        return None


class TypeSignatureParser:
    """Parse Athena DDL type signature strings into a type tree."""

    def parse(self, type_str: str) -> TypeNode:
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
            name = type_str.lower()
            return TypeNode(type_name=_TYPE_ALIASES.get(name, name))

        type_name = type_str[:paren_idx].strip().lower()
        type_name = _TYPE_ALIASES.get(type_name, type_name)

        close_idx = self._find_matching_paren(type_str, paren_idx)
        inner = type_str[paren_idx + 1 : close_idx].strip()

        if type_name in ("row", "struct"):
            parts = self._split_type_args(inner)
            field_names: list[str] = []
            children: list[TypeNode] = []
            for part in parts:
                part = part.strip()
                space_idx = self._find_field_name_boundary(part)
                if space_idx == -1:
                    children.append(self.parse(part))
                    field_names.append(part)
                else:
                    field_name = part[:space_idx].strip()
                    type_part = part[space_idx + 1 :].strip()
                    field_names.append(field_name)
                    children.append(self.parse(type_part))
            return TypeNode(type_name=type_name, children=children, field_names=field_names)

        if type_name == "array":
            child = self.parse(inner)
            return TypeNode(type_name=type_name, children=[child])

        if type_name == "map":
            parts = self._split_type_args(inner)
            if len(parts) == 2:
                key_type = self.parse(parts[0])
                value_type = self.parse(parts[1])
                return TypeNode(type_name=type_name, children=[key_type, value_type])
            return TypeNode(type_name=type_name)

        # Types with parameters like decimal(10, 2), varchar(255)
        return TypeNode(type_name=type_name)

    def _split_type_args(self, s: str) -> list[str]:
        """Split a type signature argument string by comma, respecting nested parentheses.

        Args:
            s: Type signature argument string to split.

        Returns:
            List of type argument strings.
        """
        parts: list[str] = []
        current: list[str] = []
        depth = 0

        for char in s:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif char == "," and depth == 0:
                parts.append("".join(current).strip())
                current = []
                continue
            current.append(char)

        if current:
            parts.append("".join(current).strip())
        return parts

    @staticmethod
    def _find_matching_paren(s: str, open_idx: int) -> int:
        """Find the index of the closing parenthesis matching the one at *open_idx*.

        Args:
            s: The full string.
            open_idx: Index of the opening ``(``.

        Returns:
            Index of the matching ``)``.
        """
        depth = 0
        for i in range(open_idx, len(s)):
            if s[i] == "(":
                depth += 1
            elif s[i] == ")":
                depth -= 1
                if depth == 0:
                    return i
        return len(s) - 1

    def _find_field_name_boundary(self, part: str) -> int:
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


class TypedValueConverter:
    """Convert values using TypeNode type information.

    Dependencies are injected via the constructor to avoid circular imports
    between parser.py and converter.py.

    Args:
        converters: Mapping of type names to conversion functions.
        default_converter: Fallback conversion function for unknown types.
        struct_parser: Function to parse untyped struct values.
    """

    def __init__(
        self,
        converters: dict[str, Callable[[str | None], Any | None]],
        default_converter: Callable[[str | None], Any | None],
        struct_parser: Callable[[str | None], dict[str, Any] | None],
    ) -> None:
        self._converters = converters
        self._default_converter = default_converter
        self._struct_parser = struct_parser

    def convert(self, value: str, type_node: TypeNode) -> Any:
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
            return self._convert_typed_array(value, type_node)
        if type_node.type_name == "map":
            return self._convert_typed_map(value, type_node)
        if type_node.type_name in ("row", "struct"):
            return self._convert_typed_struct(value, type_node)
        converter_fn = self._converters.get(type_node.type_name, self._default_converter)
        return converter_fn(value)

    @staticmethod
    def _to_json_str(value: Any) -> str:
        """Convert a JSON-parsed value back to a string for further conversion.

        Uses json.dumps for dict/list to produce valid JSON, and str() for
        scalar types to produce converter-compatible strings.

        Args:
            value: A value from json.loads output.

        Returns:
            String representation suitable for type conversion.
        """
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return str(value)

    def _convert_element(self, value: str, type_node: TypeNode) -> Any:
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
        return self.convert(value, type_node)

    def _convert_typed_array(self, value: str, type_node: TypeNode) -> list[Any] | None:
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

        # Try JSON first (only if content looks like JSON)
        inner_preview = value[1:10] if len(value) > 10 else value[1:-1]
        if '"' in inner_preview or value.startswith(("[{", "[null", "[[")):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return [
                        None
                        if elem is None
                        else self.convert(self._to_json_str(elem), element_type)
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
                    result.append(self._convert_typed_struct(item, element_type))
                elif element_type.type_name == "map":
                    result.append(self._convert_typed_map(item, element_type))
                else:
                    result.append(self._struct_parser(item))
            else:
                result.append(self._convert_element(item, element_type))

        return result if result else None

    def _convert_typed_map(self, value: str, type_node: TypeNode) -> dict[str, Any] | None:
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
                        str(self.convert(self._to_json_str(k), key_type) if k is not None else k): (
                            self.convert(self._to_json_str(v), value_type)
                            if v is not None
                            else None
                        )
                        for k, v in parsed.items()
                    }
            except json.JSONDecodeError:
                pass

        # Native format
        inner = value[1:-1].strip()
        if not inner:
            return {}

        pairs = _split_array_items(inner)
        result: dict[str, Any] = {}
        for pair in pairs:
            if "=" not in pair:
                continue
            k, v = pair.split("=", 1)
            k = k.strip()
            v = v.strip()
            if any(char in k for char in '{}="'):
                continue
            if v.startswith("{") and v.endswith("}"):
                if value_type.type_name in ("row", "struct"):
                    result[str(self._convert_element(k, key_type))] = self._convert_typed_struct(
                        v, value_type
                    )
                elif value_type.type_name == "map":
                    result[str(self._convert_element(k, key_type))] = self._convert_typed_map(
                        v, value_type
                    )
                else:
                    result[str(self._convert_element(k, key_type))] = self._struct_parser(v)
            else:
                converted_key = self._convert_element(k, key_type)
                converted_value = self._convert_element(v, value_type)
                result[str(converted_key)] = converted_value

        return result if result else None

    def _convert_typed_struct(self, value: str, type_node: TypeNode) -> dict[str, Any] | None:
        """Convert a struct/row value using type information.

        Args:
            value: String representation of the struct.
            type_node: Type node with field types and names.

        Returns:
            Dictionary of converted field values, or None if parsing fails.
        """
        if not (value.startswith("{") and value.endswith("}")):
            return None

        field_types = type_node.children or []

        # Try JSON first
        inner_preview = value[1:10] if len(value) > 10 else value[1:-1]
        if '"' in inner_preview or value.startswith('{"'):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, dict):
                    result: dict[str, Any] = {}
                    for i, (k, v) in enumerate(parsed.items()):
                        ft = self._get_field_type(k, type_node, i)
                        result[k] = (
                            self.convert(self._to_json_str(v), ft) if v is not None else None
                        )
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

                ft = self._get_field_type(k, type_node, field_index)
                field_index += 1

                if v.startswith("{") and v.endswith("}"):
                    if ft.type_name in ("row", "struct"):
                        result[k] = self._convert_typed_struct(v, ft)
                    elif ft.type_name == "map":
                        result[k] = self._convert_typed_map(v, ft)
                    else:
                        result[k] = self._struct_parser(v)
                else:
                    result[k] = self._convert_element(v, ft)
            return result if result else None

        # Unnamed struct
        field_names = type_node.field_names or []
        values = _split_array_items(inner)
        result = {}
        for i, v in enumerate(values):
            ft = field_types[i] if i < len(field_types) else TypeNode("varchar")
            name = field_names[i] if i < len(field_names) else str(i)
            result[name] = self._convert_element(v, ft)
        return result

    @staticmethod
    def _get_field_type(
        field_name: str,
        type_node: TypeNode,
        field_index: int,
    ) -> TypeNode:
        """Look up the type for a struct field by name or index.

        Uses the TypeNode's cached dict for O(1) name lookup, then falls
        back to positional index.

        Args:
            field_name: Name of the field to look up.
            type_node: The parent row/struct TypeNode.
            field_index: Current positional index as fallback.

        Returns:
            TypeNode for the field, defaulting to varchar if not found.
        """
        ft = type_node.get_field_type(field_name)
        if ft is not None:
            return ft
        field_types = type_node.children or []
        if field_index < len(field_types):
            return field_types[field_index]
        return TypeNode("varchar")
