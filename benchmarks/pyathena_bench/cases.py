# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Explicit capability matrix, including unsupported combinations."""

from __future__ import annotations

from dataclasses import dataclass, replace

from pyathena.filesystem.s3_core import S3Core
from pyathena_bench.config import Settings

FAMILIES = ("cursor", "dict", "s3fs", "pandas", "arrow", "polars")
# The cursor families that read result files as DataFrames or tables, also from UNLOAD.
RESULT_FILE_FAMILIES = ("pandas", "arrow", "polars")
# pandas.read_sql_query() through a SQLAlchemy engine, by the PyAthena driver it uses.
# A driver's cursor takes the options of the cursor family of the same name; the rest
# driver's Cursor takes those of no family in particular.
SQLALCHEMY_FAMILIES = {
    "sqla_rest": "rest",
    "sqla_pandas": "pandas",
    "sqla_arrow": "arrow",
    "sqla_polars": "polars",
    "sqla_s3fs": "s3fs",
}
# The filesystem suite transfers one S3 object with PyAthena's S3FileSystem (API "sync")
# or AioS3FileSystem (API "aio"), or with s3fs (API "s3fs"); its transport is the
# operation: whole reads and writes through an opened file, get_file() and put_file()
# through a local file, and a copy.
FILESYSTEM_FAMILY = "s3"
FILESYSTEM_APIS = ("sync", "aio", "s3fs")
FILESYSTEM_OPERATIONS = ("read", "get", "write", "put", "copy")
# s3fs copies objects above its ``s3fs.core.MANAGED_COPY_THRESHOLD`` as multipart
# copies, and PyAthena those above its largest part.
SINGLE_COPY_THRESHOLDS = {
    "sync": S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE,
    "aio": S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE,
    "s3fs": 150 * 2**20,
}
ALL_FAMILIES = (*FAMILIES, "wrangler", *SQLALCHEMY_FAMILIES, FILESYSTEM_FAMILY)
SUITES = ("single", "concurrent", "init", "filesystem")
SHAPES = ("flat", "nested", "json", "temporal")


@dataclass(frozen=True)
class Case:
    family: str
    api: str = "sync"
    transport: str = "csv"
    output: str = "rows"
    arraysize: int = 1000
    chunksize: int | None = None
    shape: str = "flat"
    concurrency: int = 1
    suite: str = "single"
    unsupported: str | None = None
    # The pandas.read_csv()/read_parquet() dtype_backend given to execute().
    dtype_backend: str | None = None
    # The filesystem's max_workers and the S3 client's max_pool_connections of the
    # filesystem suite.
    max_workers: int | None = None
    pool_connections: int | None = None

    @property
    def id(self) -> str:
        if self.suite == "filesystem":
            return (
                f"{self.suite}-{self.family}-{self.api}-{self.transport}-"
                f"w{self.max_workers}-p{self.pool_connections}"
            )
        backend = f"-{self.dtype_backend}" if self.dtype_backend else ""
        return (
            f"{self.suite}-{self.family}-{self.api}-{self.transport}-{self.output}-"
            f"a{self.arraysize}-c{self.chunksize or 0}{backend}-{self.shape}-n{self.concurrency}"
        )

    def unsupported_reason(self, settings: Settings, scale: str) -> str | None:
        """Return why the case does not run at a scale.

        A copy of an object up to the API's threshold in
        ``SINGLE_COPY_THRESHOLDS`` is a single CopyObject request, which
        neither ``max_workers`` nor the connection pool affects.

        Args:
            settings: Configuration with the filesystem suite's object sizes.
            scale: Scale of the suite.

        Returns:
            The reason, or None if the case runs.
        """
        if (
            self.suite == "filesystem"
            and self.transport == "copy"
            and settings.filesystem_sizes[scale] <= SINGLE_COPY_THRESHOLDS[self.api]
        ):
            threshold = SINGLE_COPY_THRESHOLDS[self.api] // 2**20
            unit = f"{threshold // 1024} GiB" if threshold % 1024 == 0 else f"{threshold} MiB"
            return f"Objects up to {unit} are copied with a single CopyObject request"
        return self.unsupported


def matrix(settings: Settings, suite: str, shape: str) -> list[Case]:
    """Return every case of a suite and result shape, including unsupported ones.

    The ``json`` shape adds a ``json`` column to the flat projection and covers
    the native pandas and Wrangler reads in the single-query suite. Their UNLOAD
    and CTAS reads are marked unsupported, because Parquet cannot hold the
    ``json`` type; the initialization suite, which prepares UNLOAD fixtures too,
    has no json cases.

    The ``temporal`` shape adds temporal columns to the flat projection and
    covers every family in the single-query suite, UNLOAD and CTAS reads
    included (see ``select_sql()``). Like json, it has no cases in the other
    suites.

    The ``filesystem`` suite does not query Athena and has no shape; its cases
    belong to the ``flat`` shape.

    Args:
        settings: Configuration with the arraysizes, chunk sizes, and concurrency,
            and the filesystem suite's max_workers and connection pools.
        suite: ``single``, ``concurrent``, ``init``, or ``filesystem``.
        shape: ``flat``, ``nested``, ``json``, or ``temporal``.

    Returns:
        The cases in a stable order.

    Raises:
        ValueError: If the suite or shape is unknown.
    """
    if suite not in SUITES or shape not in SHAPES:
        raise ValueError("Unknown suite or shape")
    if suite == "filesystem":
        if shape != "flat":
            return []
        return [
            Case(
                FILESYSTEM_FAMILY,
                api,
                operation,
                "bytes",
                suite=suite,
                max_workers=workers,
                pool_connections=pool,
            )
            for operation in FILESYSTEM_OPERATIONS
            for api in FILESYSTEM_APIS
            for workers in settings.filesystem_max_workers
            for pool in settings.filesystem_pool_connections
        ]
    if shape in {"json", "temporal"} and suite != "single":
        return []
    cases: list[Case] = []
    arraysizes = settings.arraysizes if suite == "single" else [1000]
    for family in FAMILIES:
        for api in ("sync", "thread", "aio"):
            cases.extend(Case(family, api, arraysize=a, shape=shape) for a in arraysizes)
            if family in RESULT_FILE_FAMILIES:
                for transport in ("csv", "unload"):
                    chunks: list[int | None] = (
                        [None] if family == "arrow" else [None, *settings.chunksizes]
                    )
                    # Single queries also measure pandas UNLOAD reads with pyarrow dtypes.
                    backends: tuple[str | None, ...] = (
                        (None, "pyarrow")
                        if suite == "single" and family == "pandas" and transport == "unload"
                        else (None,)
                    )
                    cases.extend(
                        Case(
                            family,
                            api,
                            transport,
                            "native",
                            chunksize=chunk,
                            shape=shape,
                            dtype_backend=backend,
                        )
                        for chunk in chunks
                        for backend in backends
                    )
                cases.extend(
                    Case(family, api, "unload", arraysize=a, shape=shape) for a in arraysizes
                )
    for transport in ("csv", "ctas", "unload"):
        for chunk in (None, *settings.chunksizes):
            reason = (
                "Wrangler CSV does not support nested types"
                if shape == "nested" and transport == "csv"
                else None
            )
            cases.append(
                Case(
                    "wrangler",
                    transport=transport,
                    output="native",
                    chunksize=chunk,
                    shape=shape,
                    unsupported=reason,
                )
            )
    if suite == "single":
        for family, driver in SQLALCHEMY_FAMILIES.items():
            transports = ["csv", "unload"] if driver in RESULT_FILE_FAMILIES else ["csv"]
            cases.extend(
                Case(family, transport=transport, output="native", chunksize=chunk, shape=shape)
                for transport in transports
                for chunk in (None, *settings.chunksizes)
            )
    if shape == "json":
        cases = [
            c
            if c.transport == "csv"
            else replace(c, unsupported="Athena cannot write the json type to Parquet")
            for c in cases
            if c.family in {"pandas", "wrangler"} and c.output == "native"
        ]
    if suite == "concurrent":
        # Keep the scaling suite bounded; chunk and arraysize sweeps belong to single.
        cases = [
            replace(c, suite=suite, concurrency=n)
            for c in cases
            if c.api in {"thread", "aio"}
            and c.arraysize == 1000
            and c.chunksize is None
            and c.transport == "csv"
            and (
                (c.family in {"cursor", "dict", "s3fs"} and c.output == "rows")
                or (c.family in RESULT_FILE_FAMILIES and c.output == "native")
            )
            for n in settings.concurrency
        ]
    elif suite == "init":
        cases = [
            replace(c, suite=suite, api=api)
            for c in cases
            if c.api == "sync"
            and c.arraysize == 1000
            and c.chunksize is None
            and c.family != "wrangler"
            and (
                (c.family in {"cursor", "dict", "s3fs"} and c.output == "rows")
                or (c.family in RESULT_FILE_FAMILIES and c.output == "native")
            )
            for api in ("direct", "to_thread")
        ]
    return cases
