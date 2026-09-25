# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Check that repository files carry the PyAthena license header."""

# Usage: just license-headers (also run by just lint)
#
# Checks tracked and untracked, non-ignored files in the working tree against
# the header described in docs/contributing.md, with the exemptions in
# scripts/config/license_headers.toml. Reports missing headers and stale
# unheaded-files entries; never modifies files.

import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

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


HASH = (_block(None, "# ", None),)
HTML = _block("<!--", "", "-->")
JINJA = _block("{#", "", "-#}")

# Comment syntaxes by file suffix; other files use HASH. Add an entry for a new
# file format with another comment syntax.
SUFFIX_BLOCKS = {
    ".css": (_block("/*", " * ", " */"),),
    ".html": (JINJA, HTML),
    ".jinja2": (JINJA,),
    ".jsonc": (_block(None, "// ", None),),
    ".md": (HTML,),
    ".rst": (_block("..", "   ", None),),
}

SHEBANG = re.compile(r"#!.*\n")
ENCODING = re.compile(r"#.*coding[:=][ \t]*[-\w.]+.*\n")
# YAML front matter in Markdown files; elsewhere, a leading YAML document marker.
FRONT_MATTER = "---\n"
FRONT_MATTER_END = re.compile(r"\n---[ \t]*(\n|$)")

CONFIG = "scripts/config/license_headers.toml"


@dataclass(frozen=True)
class Config:
    """Exemptions read from CONFIG."""

    exempt_suffixes: frozenset[str]
    unheaded_files: frozenset[str]


def load_config(file: Path) -> Config:
    """Read the exemptions, rejecting unknown keys and duplicate entries."""
    with file.open("rb") as f:
        data = tomllib.load(f)
    keys = {"exempt-suffixes", "unheaded-files"}
    if set(data) != keys:
        raise ValueError(f"{file}: expected keys {sorted(keys)}, found {sorted(data)}")
    values = {}
    for key in sorted(keys):
        items = data[key]
        if not isinstance(items, list) or not all(isinstance(item, str) for item in items):
            raise ValueError(f"{file}: {key} must be a list of strings")
        if len(set(items)) != len(items):
            raise ValueError(f"{file}: {key} has duplicate entries")
        values[key] = frozenset(items)
    return Config(values["exempt-suffixes"], values["unheaded-files"])


def _matches(blocks: tuple[re.Pattern[str], ...], text: str, pos: int, end: int) -> bool:
    return any((match := block.match(text, pos)) and match.end() <= end for block in blocks)


def _front_matter_header(text: str, suffix: str, blocks: tuple[re.Pattern[str], ...]) -> bool:
    if not text.startswith(FRONT_MATTER):
        return False
    pos = len(FRONT_MATTER)
    if suffix != ".md":
        return _matches(HASH, text, pos, len(text))
    if not (end := FRONT_MATTER_END.search(text, pos - 1)):
        return False
    while pos <= end.start():
        if _matches(HASH, text, pos, end.start() + 1):
            return True
        pos = text.index("\n", pos) + 1
    return _matches(blocks, text, end.end(), len(text))


def has_license_header(text: str, suffix: str) -> bool:
    """Return whether the header starts a file with the given suffix.

    The header may follow a shebang, an encoding declaration, or a leading YAML
    document marker. In a Markdown file with YAML front matter, it may be
    written as YAML comments inside the front matter or follow it.
    """
    pos = 0
    if match := SHEBANG.match(text, pos):
        pos = match.end()
    if match := ENCODING.match(text, pos):
        pos = match.end()
    blocks = SUFFIX_BLOCKS.get(suffix, HASH)
    return _matches(blocks, text, pos, len(text)) or _front_matter_header(text, suffix, blocks)


def exemption_reason(root: Path, path: str, exempt_suffixes: frozenset[str]) -> str | None:
    """Return why a file needs no header, or None when it needs one."""
    file = root / path
    if file.is_symlink():
        return "symbolic link"
    if Path(path).suffix in exempt_suffixes:
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


def check(root: Path, paths: list[str], config: Config) -> list[str]:
    """Return problems for the given repository-relative paths."""
    problems = []
    existing = {path for path in paths if (root / path).is_symlink() or (root / path).is_file()}
    for path in sorted(existing):
        file = root / path
        reason = exemption_reason(root, path, config.exempt_suffixes)
        headed = reason is None and has_license_header(
            file.read_text(encoding="utf-8"), Path(path).suffix
        )
        if path in config.unheaded_files:
            if reason is not None:
                problems.append(f"{path}: listed as unheaded in {CONFIG} but exempt as {reason}")
            elif headed:
                problems.append(f"{path}: listed as unheaded in {CONFIG} but has the header")
        elif reason is None and not headed:
            problems.append(f"{path}: missing license header")
    problems.extend(
        f"{path}: listed as unheaded in {CONFIG} but not found"
        for path in sorted(config.unheaded_files - existing)
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
    return [path for path in os.fsdecode(output).split("\0") if path]


def main() -> int:
    output = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], check=True, capture_output=True
    ).stdout
    root = Path(os.fsdecode(output.removesuffix(b"\n")))
    problems = check(root, repository_files(root), load_config(root / CONFIG))
    if not problems:
        return 0
    sys.stderr.write("".join(f"{problem}\n" for problem in problems))
    sys.stderr.write("See docs/contributing.md for the header of new original files.\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())
