# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Check that repository files carry the PyAthena license header."""

# Usage (from the repository root):
#   uv run python scripts/check_license_headers.py
#
# Checks tracked and untracked, non-ignored files in the working tree against
# the header described in docs/contributing.md. Reports missing headers and
# stale UNHEADED_FILES entries; never modifies files.

import re
import subprocess
import sys
from pathlib import Path

HEADER_LINES = (
    r"Copyright \d{4} The PyAthena authors",
    "",
    re.escape("Licensed under the MIT License."),
    re.escape("See LICENSE or https://opensource.org/licenses/MIT."),
    "",
    re.escape("SPDX-License-Identifier: MIT"),
)


def _block(opening: str | None, prefix: str, closing: str | None) -> re.Pattern[str]:
    blank = re.escape(prefix.rstrip())
    lines = [f"{re.escape(prefix)}{line}" if line else blank for line in HEADER_LINES]
    if opening is not None:
        lines.insert(0, re.escape(opening))
    if closing is not None:
        lines.append(re.escape(closing))
    return re.compile("\n".join(lines) + "(\n|$)")


# Comment syntaxes used in the repository; add one for a new file format.
HEADER_BLOCKS = (
    _block(None, "# ", None),
    _block(None, "// ", None),
    _block("<!--", "", "-->"),
    _block("..", "   ", None),
    _block("{#", "", "-#}"),
    _block("/*", " * ", " */"),
)

SHEBANG = re.compile(r"#!.*\n")
ENCODING = re.compile(r"#.*coding[:=][ \t]*[-\w.]+.*\n")
FRONT_MATTER = "---\n"

# Formats without comment syntax or with generated content.
EXEMPT_SUFFIXES = frozenset({".csv", ".gz", ".json", ".lock", ".png", ".tsv"})

# Existing files without the header, classified in
# https://github.com/pyathena-dev/PyAthena/issues/790 and described in NOTICE.
# New files carry the header; remove entries whose files gain the header or
# are deleted.
UNHEADED_FILES = frozenset(
    {
        ".github/PULL_REQUEST_TEMPLATE.md",
        "LICENSE",
        "NOTICE",
        "cloudformation/github_actions_oidc.yaml",
        "docs/aio.md",
        "docs/arrow.md",
        "docs/conf.py",
        "docs/cursor.md",
        "docs/pandas.md",
        "docs/polars.md",
        "docs/s3fs.md",
        "docs/sqlalchemy.md",
        "docs/usage.md",
        "pyathena/__init__.py",
        "pyathena/aio/arrow/cursor.py",
        "pyathena/aio/common.py",
        "pyathena/aio/pandas/cursor.py",
        "pyathena/aio/polars/cursor.py",
        "pyathena/aio/result_set.py",
        "pyathena/arrow/async_cursor.py",
        "pyathena/arrow/converter.py",
        "pyathena/arrow/cursor.py",
        "pyathena/arrow/result_set.py",
        "pyathena/arrow/util.py",
        "pyathena/async_cursor.py",
        "pyathena/common.py",
        "pyathena/connection.py",
        "pyathena/converter.py",
        "pyathena/cursor.py",
        "pyathena/filesystem/s3.py",
        "pyathena/filesystem/s3_object.py",
        "pyathena/formatter.py",
        "pyathena/model.py",
        "pyathena/pandas/__init__.py",
        "pyathena/pandas/async_cursor.py",
        "pyathena/pandas/converter.py",
        "pyathena/pandas/cursor.py",
        "pyathena/pandas/result_set.py",
        "pyathena/pandas/util.py",
        "pyathena/parser.py",
        "pyathena/polars/__init__.py",
        "pyathena/polars/async_cursor.py",
        "pyathena/polars/cursor.py",
        "pyathena/result_set.py",
        "pyathena/s3fs/async_cursor.py",
        "pyathena/s3fs/cursor.py",
        "pyathena/sqlalchemy/array.py",
        "pyathena/sqlalchemy/base.py",
        "pyathena/sqlalchemy/compiler.py",
        "pyathena/sqlalchemy/constants.py",
        "pyathena/sqlalchemy/temporal.py",
        "pyathena/sqlalchemy/types.py",
        "pyathena/util.py",
        "pyproject.toml",
        "tests/__init__.py",
        "tests/pyathena/aio/sqlalchemy/test_base.py",
        "tests/pyathena/aio/test_cursor.py",
        "tests/pyathena/arrow/test_async_cursor.py",
        "tests/pyathena/conftest.py",
        "tests/pyathena/filesystem/test_s3.py",
        "tests/pyathena/filesystem/test_s3_async.py",
        "tests/pyathena/pandas/test_async_cursor.py",
        "tests/pyathena/pandas/test_cursor.py",
        "tests/pyathena/pandas/test_util.py",
        "tests/pyathena/polars/test_async_cursor.py",
        "tests/pyathena/s3fs/test_cursor.py",
        "tests/pyathena/sqlalchemy/test_array.py",
        "tests/pyathena/sqlalchemy/test_base.py",
        "tests/pyathena/sqlalchemy/test_temporal.py",
        "tests/pyathena/sqlalchemy/test_types.py",
        "tests/pyathena/test_async_cursor.py",
        "tests/pyathena/test_converter.py",
        "tests/pyathena/test_cursor.py",
        "tests/pyathena/test_model.py",
        "tests/pyathena/test_util.py",
        "tests/resources/queries/create_table.sql.jinja2",
        "tests/sqlalchemy/test_suite.py",
    }
)


def has_license_header(text: str) -> bool:
    """Return whether the header starts the file.

    The header may follow a shebang and an encoding declaration, or open YAML
    front matter as comments.
    """
    pos = 0
    if match := SHEBANG.match(text, pos):
        pos = match.end()
    if match := ENCODING.match(text, pos):
        pos = match.end()
    if pos == 0 and text.startswith(FRONT_MATTER):
        pos = len(FRONT_MATTER)
    return any(block.match(text, pos) for block in HEADER_BLOCKS)


def exemption_reason(root: Path, path: str) -> str | None:
    """Return why a file needs no header, or None when it needs one."""
    file = root / path
    if file.is_symlink():
        return "symbolic link"
    if Path(path).suffix in EXEMPT_SUFFIXES:
        return "data or generated file"
    data = file.read_bytes()
    if b"\0" in data:
        return "binary file"
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return "binary file"
    if not text.strip():
        return "empty file"
    return None


def check(root: Path, paths: list[str]) -> list[str]:
    """Return problems for the given repository-relative paths."""
    problems = []
    existing = {path for path in paths if (root / path).is_symlink() or (root / path).is_file()}
    for path in sorted(existing):
        file = root / path
        reason = exemption_reason(root, path)
        headed = reason is None and has_license_header(file.read_text(encoding="utf-8"))
        if path in UNHEADED_FILES:
            if reason is not None:
                problems.append(f"{path}: listed in UNHEADED_FILES but exempt as {reason}")
            elif headed:
                problems.append(f"{path}: listed in UNHEADED_FILES but has the header")
        elif reason is None and not headed:
            problems.append(f"{path}: missing license header")
    problems.extend(
        f"{path}: listed in UNHEADED_FILES but not found"
        for path in sorted(UNHEADED_FILES - existing)
    )
    return problems


def repository_files(root: Path) -> list[str]:
    """Return tracked and untracked, non-ignored files."""
    output = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
    ).stdout
    return [path for path in output.decode("utf-8").split("\0") if path]


def main() -> int:
    root = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"], check=True, capture_output=True, text=True
        ).stdout.strip()
    )
    problems = check(root, repository_files(root))
    if not problems:
        return 0
    sys.stderr.write("".join(f"{problem}\n" for problem in problems))
    sys.stderr.write("See docs/contributing.md for the header of new original files.\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())
