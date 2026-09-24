# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import subprocess

import pytest

from scripts.check_license_headers import (
    UNHEADED_FILES,
    check,
    exemption_reason,
    has_license_header,
    repository_files,
)

LINES = [
    "Copyright 2026 The PyAthena authors",
    "",
    "Licensed under the MIT License.",
    "See LICENSE or https://opensource.org/licenses/MIT.",
    "",
    "SPDX-License-Identifier: MIT",
]


def prefixed(prefix: str) -> str:
    return "".join(f"{prefix}{line}".rstrip() + "\n" for line in LINES)


HASH = prefixed("# ")
BODY = "".join(f"{line}\n" for line in LINES)


class TestHasLicenseHeader:
    @pytest.mark.parametrize(
        "text",
        [
            HASH + "\nimport os\n",
            HASH,
            "#!/usr/bin/env bash\n" + HASH + "\nset -eu\n",
            "# -*- coding: utf-8 -*-\n" + HASH,
            "#!/usr/bin/env python\n# -*- coding: utf-8 -*-\n" + HASH,
            "---\n" + HASH + "name: Bug report\n---\n\nBody\n",
            "<!--\n" + BODY + "-->\n\n# Title\n",
            "..\n" + prefixed("   ") + "\n.. _label:\n",
            "{#\n" + BODY + "-#}\n\nSELECT 1\n",
            "/*\n" + prefixed(" * ") + " */\n",
            prefixed("// ") + "\n{}\n",
            HASH.replace("2026", "2017"),
        ],
    )
    def test_accepted(self, text):
        assert has_license_header(text)

    @pytest.mark.parametrize(
        "text",
        [
            "import os\n",
            "import os\n\n" + HASH,
            HASH.replace("The PyAthena authors", "laughingman7743"),
            HASH.replace("2026", "26"),
            HASH.replace("SPDX-License-Identifier: MIT", "SPDX-License-Identifier: Apache-2.0"),
            HASH.replace("#\n", "", 1),
            "<!--\n" + BODY,
            "{#\n" + BODY + "#}\n",
            "\n" + HASH,
            "---\ntitle: x\n" + HASH,
            "# Copyright 2026 The PyAthena authors\n",
        ],
    )
    def test_rejected(self, text):
        assert not has_license_header(text)


class TestExemptionReason:
    @pytest.mark.parametrize(
        ("name", "content", "expected"),
        [
            ("empty.py", b"", "empty file"),
            ("blank.py", b"\n  \n", "empty file"),
            ("image.bin", b"\x89PNG\x00", "binary file"),
            ("latin1.txt", b"caf\xe9\n", "binary file"),
            ("rows.tsv", b"a\tb\n", "data or generated file"),
            ("rows.csv", b"a,b\n", "data or generated file"),
            ("data.json", b"{}\n", "data or generated file"),
            ("uv.lock", b"version = 1\n", "data or generated file"),
            ("module.py", b"x = 1\n", None),
        ],
    )
    def test_file(self, tmp_path, name, content, expected):
        (tmp_path / name).write_bytes(content)
        assert exemption_reason(tmp_path, name) == expected

    def test_symlink(self, tmp_path):
        (tmp_path / "target.md").write_text("text\n")
        (tmp_path / "link.md").symlink_to("target.md")
        assert exemption_reason(tmp_path, "link.md") == "symbolic link"


class TestCheck:
    @pytest.fixture
    def root(self, tmp_path):
        for path in UNHEADED_FILES:
            file = tmp_path / path
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text("content\n")
        return tmp_path

    def test_clean(self, root):
        (root / "new.py").write_text(HASH)
        (root / "empty.py").write_text("")
        assert check(root, [*UNHEADED_FILES, "new.py", "empty.py"]) == []

    def test_missing_header(self, root):
        (root / "new.py").write_text("x = 1\n")
        assert check(root, [*UNHEADED_FILES, "new.py"]) == ["new.py: missing license header"]

    def test_listed_file_with_header(self, root):
        (root / "LICENSE").write_text(HASH)
        assert check(root, list(UNHEADED_FILES)) == [
            "LICENSE: listed in UNHEADED_FILES but has the header"
        ]

    def test_listed_file_exempt(self, root):
        (root / "LICENSE").write_text("")
        assert check(root, list(UNHEADED_FILES)) == [
            "LICENSE: listed in UNHEADED_FILES but exempt as empty file"
        ]

    def test_listed_file_not_found(self, root):
        (root / "LICENSE").unlink()
        assert check(root, list(UNHEADED_FILES)) == [
            "LICENSE: listed in UNHEADED_FILES but not found"
        ]


def test_repository_files(tmp_path):
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    (tmp_path / ".gitignore").write_text("ignored.py\n")
    (tmp_path / "tracked.py").write_text("")
    (tmp_path / "untracked.py").write_text("")
    (tmp_path / "ignored.py").write_text("")
    subprocess.run(["git", "add", "tracked.py"], cwd=tmp_path, check=True)
    assert sorted(repository_files(tmp_path)) == [".gitignore", "tracked.py", "untracked.py"]
