# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import os
import tarfile
import zipfile
from email.parser import Parser
from pathlib import Path

import pytest


@pytest.mark.skipif(
    not os.environ.get("BENCHMARK_DIST_DIR"), reason="Run after building the main distributions"
)
def test_main_distributions_exclude_benchmark_code_and_dependencies():
    directory = Path(os.environ["BENCHMARK_DIST_DIR"])
    wheels = list(directory.glob("*.whl"))
    sdists = list(directory.glob("*.tar.gz"))
    assert len(wheels) == len(sdists) == 1
    with zipfile.ZipFile(wheels[0]) as archive:
        names = archive.namelist()
        assert all(name.startswith("pyathena/") or ".dist-info/" in name for name in names)
        metadata = Parser().parsestr(
            archive.read(next(n for n in names if n.endswith("/METADATA"))).decode()
        )
        assert metadata["Name"].lower() == "pyathena"
        assert metadata["Requires-Python"] == ">=3.10"
        requirements = "\n".join(metadata.get_all("Requires-Dist", []))
        assert "awswrangler" not in requirements
        assert "psutil" not in requirements
        assert "file:" not in requirements
    with tarfile.open(sdists[0]) as archive:
        names = [name.split("/", 1)[1] for name in archive.getnames() if "/" in name]
        assert all(
            name.startswith("pyathena/")
            or name
            in {
                "pyathena",
                ".gitignore",
                "LICENSE",
                "NOTICE",
                "README.md",
                "pyproject.toml",
                "PKG-INFO",
            }
            for name in names
        )
