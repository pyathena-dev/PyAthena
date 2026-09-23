# Copyright 2023 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Utility classes for PyAthena SQLAlchemy dialect."""


def _split_type_arguments(value: str) -> list[str]:
    """Split type arguments without splitting nested types or quoted field names."""
    parts = []
    start = 0
    brackets: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(value):
        char = value[index]
        if quote:
            if char == quote:
                if index + 1 < len(value) and value[index + 1] == quote:
                    index += 1
                else:
                    quote = None
        elif char in ('"', "`"):
            quote = char
        elif char in "(<":
            brackets.append(")" if char == "(" else ">")
        elif char in ")>":
            if not brackets or brackets.pop() != char:
                raise ValueError(f"Unbalanced type arguments: {value!r}")
        elif char == "," and not brackets:
            parts.append(value[start:index].strip())
            start = index + 1
        index += 1
    if brackets or quote:
        raise ValueError(f"Unbalanced type arguments: {value!r}")
    parts.append(value[start:].strip())
    return parts


class _HashableDict(dict):  # type: ignore[type-arg]
    """A dictionary subclass that can be used as a dictionary key.

    SQLAlchemy's reflection caching requires hashable objects. This class
    enables dictionary values (like table properties) to be cached by
    making them hashable through tuple conversion.
    """

    def __hash__(self):  # type: ignore[override]
        return hash(tuple(sorted(self.items())))
