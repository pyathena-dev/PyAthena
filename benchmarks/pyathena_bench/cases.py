# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Explicit capability matrix, including unsupported combinations."""

from __future__ import annotations

from dataclasses import dataclass, replace

from pyathena_bench.config import Settings

FAMILIES = ("cursor", "dict", "s3fs", "pandas", "arrow", "polars")


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

    @property
    def id(self) -> str:
        return (
            f"{self.suite}-{self.family}-{self.api}-{self.transport}-{self.output}-"
            f"a{self.arraysize}-c{self.chunksize or 0}-{self.shape}-n{self.concurrency}"
        )


def matrix(settings: Settings, suite: str, shape: str) -> list[Case]:
    if suite not in {"single", "concurrent", "init"} or shape not in {"flat", "nested"}:
        raise ValueError("Unknown suite or shape")
    cases: list[Case] = []
    arraysizes = settings.arraysizes if suite == "single" else [1000]
    for family in FAMILIES:
        for api in ("sync", "thread", "aio"):
            cases.extend(Case(family, api, arraysize=a, shape=shape) for a in arraysizes)
            if family in {"pandas", "arrow", "polars"}:
                for transport in ("csv", "unload"):
                    chunks: list[int | None] = (
                        [None] if family == "arrow" else [None, *settings.chunksizes]
                    )
                    for chunk in chunks:
                        reason = None
                        if family == "pandas" and transport == "unload" and chunk:
                            reason = (
                                "Pandas UNLOAD reads all Parquet; chunksize does not bound memory"
                            )
                        cases.append(
                            Case(
                                family,
                                api,
                                transport,
                                "native",
                                chunksize=chunk,
                                shape=shape,
                                unsupported=reason,
                            )
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
                or (c.family in {"pandas", "arrow", "polars"} and c.output == "native")
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
                or (c.family in {"pandas", "arrow", "polars"} and c.output == "native")
            )
            for api in ("direct", "to_thread")
        ]
    return cases
