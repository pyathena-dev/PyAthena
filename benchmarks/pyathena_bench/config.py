# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Validated configuration and SQL shared by every adapter."""

from __future__ import annotations

import json
import math
import re
import tomllib
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

FLAT_COLUMNS = (
    '"timestamp", country_code, url, project, file.filename AS filename, '
    "file.version AS version, file.type AS file_type, details.python AS python, "
    "details.installer.name AS installer, details.system.name AS system, "
    "details.cpu AS cpu, http.status_code AS status_code, "
    "http.bytes_served AS bytes_served, tls_protocol, tls_cipher"
)
NESTED_COLUMNS = (
    '"timestamp", country_code, url, project, file, details, http, tls_protocol, tls_cipher'
)


def identifier(value: str) -> str:
    """Accept simple identifiers before interpolating SQL or resource names."""
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise ValueError(f"Invalid identifier: {value!r}")
    return value


@dataclass(frozen=True)
class Settings:
    region: str = "us-west-2"
    workgroup: str = "pyathena"
    profile: str | None = None
    database: str = "pyathena_benchmark"
    table: str = "pypi_file_downloads"
    download_date: str = "2026-09-17"
    scales: dict[str, int] = field(default_factory=lambda: {"small": 10000})
    warmups: int = 1
    repetitions: int = 5
    arraysizes: list[int] = field(default_factory=lambda: [100, 1000])
    chunksizes: list[int] = field(default_factory=lambda: [10000, 100000])
    concurrency: list[int] = field(default_factory=lambda: [1, 10, 50, 100])
    executor_workers: int = 32
    poll_interval: float = 1.0
    sample_interval: float = 0.05
    heartbeat_interval: float = 0.01
    timeout_seconds: float = 3600
    memory_fraction: float = 0.8

    def __post_init__(self) -> None:
        identifier(self.database)
        identifier(self.table)
        date.fromisoformat(self.download_date)
        if not self.scales:
            raise ValueError("At least one scale is required")
        for name, size in self.scales.items():
            identifier(name)
            if type(size) is not int or size < 1:
                raise ValueError("Scale sizes must be positive integers")
        for name in ("warmups", "repetitions", "executor_workers"):
            value = getattr(self, name)
            if type(value) is not int or value < (0 if name == "warmups" else 1):
                raise ValueError(f"Invalid {name}")
        for name in ("arraysizes", "chunksizes", "concurrency"):
            values = getattr(self, name)
            if not values or any(type(v) is not int or v < 1 for v in values):
                raise ValueError(f"Invalid {name}")
        if any(v > 1000 for v in self.arraysizes):
            raise ValueError("arraysizes must not exceed Athena's 1000-row API limit")
        for name in ("poll_interval", "sample_interval", "heartbeat_interval", "timeout_seconds"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"Invalid {name}")
        if not 0 < self.memory_fraction < 1:
            raise ValueError("memory_fraction must be between zero and one")

    @classmethod
    def load(cls, path: Path) -> Settings:
        with path.open("rb") as stream:
            data = tomllib.load(stream)
        if set(data) - {"aws", "source", "scales", "measurement"}:
            raise ValueError("Unknown configuration section")
        return cls(
            **data.get("aws", {}),
            **data.get("source", {}),
            scales=data.get("scales", {"small": 10000}),
            **data.get("measurement", {}),
        )


def select_sql(database: str, table: str, shape: str) -> str:
    if shape not in {"flat", "nested"}:
        raise ValueError(f"Unknown shape: {shape}")
    columns = FLAT_COLUMNS if shape == "flat" else NESTED_COLUMNS
    return f'SELECT {columns} FROM "{identifier(database)}"."{identifier(table)}"'


def read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def write_json(path: Path, value: Any) -> None:
    """Replace a manifest atomically so interrupted writes remain readable."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)
