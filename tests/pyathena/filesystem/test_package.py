# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import ast
from pathlib import Path

import pytest

import pyathena.filesystem

_PACKAGE = "pyathena.filesystem"
_ROOT = Path(pyathena.filesystem.__file__).parent


def _module_name(path):
    return ".".join((_PACKAGE, *path.relative_to(_ROOT).with_suffix("").parts)).removesuffix(
        ".__init__"
    )


def _outside_package(name):
    return (name == "pyathena" or name.startswith("pyathena.")) and not (
        name == _PACKAGE or name.startswith(f"{_PACKAGE}.")
    )


def _imports_outside_package(path):
    """List the imports of PyAthena modules outside the package, with their lines.

    All imports count, including those under ``TYPE_CHECKING`` and the calls
    of ``importlib.import_module()`` or ``__import__()`` with a literal name;
    relative imports are resolved against the module.
    """
    module = _module_name(path)
    # The package that a relative import of level 1 starts from.
    package = module if path.stem == "__init__" else module.rpartition(".")[0]

    def resolve(name, level):
        """Resolve a module name relative to the package by ``level`` dots."""
        if not level:
            return name
        parent = package.rsplit(".", level - 1)[0] if level > 1 else package
        return f"{parent}.{name}" if name else parent

    found = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            base = resolve(node.module or "", node.level)
            # ``from pyathena import filesystem`` names a submodule of the package.
            names = [base] if base != "pyathena" else [f"pyathena.{a.name}" for a in node.names]
        elif (
            isinstance(node, ast.Call)
            and (
                (isinstance(node.func, ast.Attribute) and node.func.attr == "import_module")
                or (
                    isinstance(node.func, ast.Name)
                    and node.func.id in ("import_module", "__import__")
                )
            )
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            # A relative name, such as ``import_module("..util", __package__)``.
            name = node.args[0].value
            level = len(name) - len(name.lstrip("."))
            names = [resolve(name[level:], level)]
        else:
            continue
        found.extend((node.lineno, name) for name in names if _outside_package(name))
    return found


@pytest.mark.parametrize(
    "path",
    sorted(p for pattern in ("*.py", "*.pyi") for p in _ROOT.rglob(pattern)),
    ids=lambda p: str(p.relative_to(_ROOT)),
)
def test_imports_nothing_outside_package(path):
    # GH-1388: the filesystem depends on nothing in PyAthena outside its package.
    assert _imports_outside_package(path) == []
