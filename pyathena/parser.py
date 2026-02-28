from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


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
            return TypeNode(type_name=type_str.lower())

        type_name = type_str[:paren_idx].strip().lower()

        inner = type_str[paren_idx + 1 : -1].strip()

        if type_name in ("row", "struct"):
            parts = self._split_top_level(inner)
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
            parts = self._split_top_level(inner)
            if len(parts) == 2:
                key_type = self.parse(parts[0])
                value_type = self.parse(parts[1])
                return TypeNode(type_name=type_name, children=[key_type, value_type])
            return TypeNode(type_name=type_name)

        # Types with parameters like decimal(10, 2), varchar(255)
        return TypeNode(type_name=type_name)

    def _split_top_level(self, s: str, delimiter: str = ",") -> list[str]:
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

        # Try JSON first
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [
                    None if elem is None else self._convert_element(str(elem), element_type)
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
                        str(
                            self._convert_element(str(k), key_type) if k is not None else k
                        ): self._convert_element(str(v), value_type) if v is not None else None
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
                        result[k] = self._convert_element(str(v), ft) if v is not None else None
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

                ft = self._get_field_type(k, field_names, field_types, field_index)
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
        values = [v.strip() for v in inner.split(",")]
        result = {}
        for i, v in enumerate(values):
            ft = field_types[i] if i < len(field_types) else TypeNode("varchar")
            name = field_names[i] if i < len(field_names) else str(i)
            result[name] = self._convert_element(v, ft)
        return result

    def _get_field_type(
        self,
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
