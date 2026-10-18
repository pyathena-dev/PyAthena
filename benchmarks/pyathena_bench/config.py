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
# The flat projection with one column of Athena's json type, NULL where details is NULL.
JSON_COLUMNS = f"{FLAT_COLUMNS}, CAST(details AS JSON) AS details_json"
# The flat projection with temporal columns derived from the epoch microseconds of
# "timestamp"; from_unixtime() returns timestamp(3). UNLOAD and CTAS write Parquet
# through Hive, which has no time or with time zone types, so their reads project
# the timestamp and date columns only.
_SECONDS = '"timestamp" / 1e6'
PARQUET_TEMPORAL_COLUMNS = (
    f"{FLAT_COLUMNS}, from_unixtime({_SECONDS}) AS event_time, "
    f"CAST(from_unixtime({_SECONDS}) AS date) AS event_date"
)
TEMPORAL_COLUMNS = (
    f"{PARQUET_TEMPORAL_COLUMNS}, "
    f"from_unixtime({_SECONDS}, 'UTC') AS event_time_utc, "
    f"from_unixtime({_SECONDS}, 'America/New_York') AS event_time_zone, "
    f"from_unixtime({_SECONDS}, '+09:00') AS event_time_offset, "
    f"CAST(from_unixtime({_SECONDS}) AS time(3)) AS event_clock"
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
    # The filesystem suite: object sizes in bytes by scale name, the block size of
    # reads, writes, and copied parts, and the swept max_workers and S3 connection pools.
    filesystem_sizes: dict[str, int] = field(default_factory=lambda: {"small": 64 * 2**20})
    filesystem_block_size: int = 8 * 2**20
    filesystem_max_workers: list[int] = field(default_factory=lambda: [8, 40])
    filesystem_pool_connections: list[int] = field(default_factory=lambda: [10, 40])

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
        if not self.filesystem_sizes:
            raise ValueError("At least one filesystem size is required")
        for name, size in self.filesystem_sizes.items():
            identifier(name)
            if type(size) is not int or size < 1:
                raise ValueError("Filesystem sizes must be positive integers")
        # S3's minimum part size, below which a multipart upload cannot complete.
        if type(self.filesystem_block_size) is not int or self.filesystem_block_size < 5 * 2**20:
            raise ValueError("filesystem block_size must be at least 5 MiB")
        # A size up to block_size would be one request instead of parallel ranges or parts.
        if any(
            not self.filesystem_block_size < size <= 10_000 * self.filesystem_block_size
            for size in self.filesystem_sizes.values()
        ):
            raise ValueError("Filesystem sizes must take 2 to 10,000 parts of block_size")
        for name in ("filesystem_max_workers", "filesystem_pool_connections"):
            values = getattr(self, name)
            if not values or any(type(v) is not int or v < 1 for v in values):
                raise ValueError(f"Invalid {name}")

    def suite_scales(self, suite: str | None) -> dict[str, int]:
        """Return the scales of a suite by name.

        Args:
            suite: Suite name, or None for the snapshots that ``prepare`` creates.

        Returns:
            The object sizes in bytes of the ``filesystem`` suite, and the row
            counts of the snapshots of the other suites.
        """
        return self.filesystem_sizes if suite == "filesystem" else self.scales

    @classmethod
    def load(cls, path: Path) -> Settings:
        with path.open("rb") as stream:
            data = tomllib.load(stream)
        if set(data) - {"aws", "source", "scales", "measurement", "filesystem"}:
            raise ValueError("Unknown configuration section")
        return cls(
            **data.get("aws", {}),
            **data.get("source", {}),
            scales=data.get("scales", {"small": 10000}),
            **data.get("measurement", {}),
            **{f"filesystem_{k}": v for k, v in data.get("filesystem", {}).items()},
        )


def select_sql(database: str, table: str, shape: str, transport: str) -> str:
    """Return the measured query of a shape over a snapshot table.

    Args:
        database: Database of the snapshot.
        table: Snapshot table.
        shape: ``flat``, ``nested``, ``json``, or ``temporal``.
        transport: ``csv``, ``unload``, or ``ctas``. The ``temporal`` shape
            projects only the columns that Parquet can hold for ``unload`` and
            ``ctas``.

    Returns:
        The SELECT statement without a ``LIMIT``.

    Raises:
        ValueError: If the shape is unknown.
    """
    if shape == "temporal" and transport in {"unload", "ctas"}:
        columns: str | None = PARQUET_TEMPORAL_COLUMNS
    else:
        columns = {
            "flat": FLAT_COLUMNS,
            "nested": NESTED_COLUMNS,
            "json": JSON_COLUMNS,
            "temporal": TEMPORAL_COLUMNS,
        }.get(shape)
    if columns is None:
        raise ValueError(f"Unknown shape: {shape}")
    return f'SELECT {columns} FROM "{identifier(database)}"."{identifier(table)}"'


def read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def write_json(path: Path, value: Any) -> None:
    """Replace a manifest atomically so interrupted writes remain readable."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)
