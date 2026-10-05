# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Validation of keyword names forwarded by cursors."""

from collections.abc import Callable, Collection
from inspect import Parameter, signature
from typing import Any


def keyword_parameters(func: Callable[..., Any]) -> set[str]:
    """Return the explicitly named parameters that accept keyword arguments.

    Args:
        func: The callable whose signature to inspect.

    Returns:
        Its positional-or-keyword and keyword-only parameter names.
    """
    return {
        name
        for name, parameter in signature(func).parameters.items()
        if parameter.kind in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.KEYWORD_ONLY)
    }


def constructor_keyword_parameters(cls: type[Any]) -> set[str]:
    """Return explicit constructor keyword names across a class's MRO.

    Args:
        cls: The class whose forwarding constructors to inspect.

    Returns:
        The named constructor parameters, excluding ``self``.
    """
    names: set[str] = set()
    for base in cls.__mro__:
        if "__init__" in vars(base):
            names.update(keyword_parameters(vars(base)["__init__"]))
    return names - {"self"}


def validate_kwargs(method: str, kwargs: dict[str, Any], allowed: Collection[str] = ()) -> None:
    """Reject the first keyword name that a cursor does not support.

    Args:
        method: The method name included in the error.
        kwargs: Extra keyword arguments given to the method.
        allowed: The extra keyword names the method supports.

    Raises:
        TypeError: If a keyword name is not allowed.
    """
    for name in kwargs:
        if name not in allowed:
            raise TypeError(f"{method}() got an unexpected keyword argument '{name}'")
