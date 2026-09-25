# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Distribute benchmark runs over identical hosts through an S3 job queue."""

from __future__ import annotations

import json
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from pyathena_bench.cases import Case, matrix
from pyathena_bench.config import Settings

# Row cursors page through GetQueryResults, which has an account-wide request-rate quota.
API_FAMILIES = {"cursor", "dict"}
QUEUE_ROOT = "fleet"


@dataclass(frozen=True)
class Filters:
    """Case filters shared by ``plan``, ``run``, and ``jobs``."""

    family: list[str] | None = None
    api: list[str] | None = None
    transport: list[str] | None = None
    output_kind: str | None = None
    arraysize: list[int] | None = None

    def select(self, cases: Iterable[Case]) -> list[Case]:
        """Return the cases that match every configured filter.

        Args:
            cases: Candidate cases.

        Returns:
            Matching cases in their original order.
        """
        return [
            c
            for c in cases
            if (not self.family or c.family in self.family)
            and (not self.api or c.api in self.api)
            and (not self.transport or c.transport in self.transport)
            and (not self.output_kind or c.output == self.output_kind)
            and (not self.arraysize or c.arraysize in self.arraysize)
        ]


def api_pages(case: Case, rows: int) -> int:
    """Estimate GetQueryResults pages that one trial of a case requests.

    Args:
        case: Benchmark case.
        rows: Rows in the selected snapshot.

    Returns:
        Page count summed over the trial's concurrent queries; 0 for cases that
        do not page through the API.
    """
    if case.family not in API_FAMILIES or case.suite == "init":
        return 0
    return -(-rows // case.arraysize) * case.concurrency


def expand_jobs(
    settings: Settings,
    suites: list[str],
    scales: list[str],
    shapes: list[str],
    filters: Filters,
    split_pages: int | None = None,
) -> list[dict[str, Any]]:
    """Split a selection into independent ``run`` invocations.

    A job covers one suite, scale, shape, family, API, transport, and output
    kind, and additionally one arraysize for API row cursors. Jobs that page
    through GetQueryResults are marked so workers can bound their concurrency.
    When one trial of such a job needs at least ``split_pages`` pages, each
    measured repetition becomes a separate single-trial job without a warmup.

    Args:
        settings: Configuration that defines the matrix and repetitions.
        suites: Suites to expand.
        scales: Scale names defined in the configuration.
        shapes: Result shapes to expand.
        filters: Case filters applied before grouping.
        split_pages: Per-trial page threshold for splitting repetitions.

    Returns:
        Job dictionaries ordered with API-heavy jobs first and longer jobs earlier.

    Raises:
        ValueError: If no case matches the selection.
    """
    jobs: list[dict[str, Any]] = []
    for suite in suites:
        for scale in scales:
            rows = settings.scales[scale]
            for shape in shapes:
                groups: dict[tuple[Any, ...], list[Case]] = {}
                for case in filters.select(matrix(settings, suite, shape)):
                    arraysize = case.arraysize if case.family in API_FAMILIES else None
                    key = (case.family, case.api, case.transport, case.output, arraysize)
                    groups.setdefault(key, []).append(case)
                for (family, api, transport, output, arraysize), cases in groups.items():
                    base = f"{suite}-{scale}-{shape}-{family}-{api}-{transport}-{output}"
                    args = [
                        "--suite", suite, "--scale", scale, "--shape", shape,
                        "--family", family, "--api", api, "--transport", transport,
                        "--output-kind", output,
                    ]  # fmt: skip
                    if arraysize is not None:
                        base += f"-a{arraysize}"
                        args += ["--arraysize", str(arraysize)]
                    runnable = [c for c in cases if c.unsupported is None]
                    pages = max((api_pages(c, rows) for c in runnable), default=0)
                    weight = rows * max(len(runnable), 1)
                    if split_pages and pages >= split_pages:
                        jobs.extend(
                            {
                                "id": f"{base}-r{repetition}",
                                "args": [*args, "--warmups", "0", "--repetitions", "1"],
                                "api_heavy": True,
                                "pages": pages * len(runnable),
                                "weight": weight,
                            }
                            for repetition in range(1, settings.repetitions + 1)
                        )
                    else:
                        trials = settings.warmups + settings.repetitions
                        jobs.append(
                            {
                                "id": base,
                                "args": args,
                                "api_heavy": pages > 0,
                                "pages": pages * len(runnable) * trials,
                                "weight": weight * trials,
                            }
                        )
    if not jobs:
        raise ValueError("No cases match the selected filters")
    return sorted(jobs, key=lambda j: (not j["api_heavy"], -j["pages"], -j["weight"]))


def now() -> str:
    """Return the current UTC time in ISO 8601 format.

    Returns:
        Timestamp with second precision.
    """
    return datetime.now(UTC).isoformat(timespec="seconds")


class Queue:
    """S3 layout of one named fleet run under ``fleet/<name>/`` in the scratch bucket."""

    def __init__(self, s3: Any, bucket: str, name: str) -> None:
        if not name or "/" in name:
            raise ValueError("Queue name must be a non-empty path segment")
        self.s3 = s3
        self.bucket = bucket
        self.prefix = f"{QUEUE_ROOT}/{name}/"

    def key(self, *parts: str) -> str:
        """Return the object key for a queue path.

        Args:
            *parts: Path segments below the queue prefix.

        Returns:
            Object key.
        """
        return self.prefix + "/".join(parts)

    def put_if_absent(self, key: str, body: dict[str, Any]) -> bool:
        """Create an object only if no object exists at the key.

        Args:
            key: Object key.
            body: JSON-serializable content.

        Returns:
            True if this call created the object; False if it already existed.

        Raises:
            ClientError: For S3 errors other than a failed precondition.
        """
        try:
            self.s3.put_object(
                Bucket=self.bucket, Key=key, Body=json.dumps(body).encode(), IfNoneMatch="*"
            )
            return True
        except ClientError as error:
            if error.response["Error"]["Code"] in {
                "PreconditionFailed",
                "ConditionalRequestConflict",
            }:
                return False
            raise

    def names(self, folder: str) -> set[str]:
        """List the object names directly below a queue folder.

        Args:
            folder: Folder name, such as ``claims`` or ``done``.

        Returns:
            Object names without the folder prefix.
        """
        names: set[str] = set()
        prefix = self.key(folder) + "/"
        for page in self.s3.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=prefix
        ):
            names.update(item["Key"][len(prefix) :] for item in page.get("Contents", []))
        return names

    def read(self, *parts: str) -> bytes:
        """Read a queue object.

        Args:
            *parts: Path segments below the queue prefix.

        Returns:
            Object content.
        """
        return self.s3.get_object(Bucket=self.bucket, Key=self.key(*parts))["Body"].read()

    def acquire_slot(self, slots: int, owner: dict[str, Any]) -> str | None:
        """Take one of the API slots that bound concurrent API-heavy jobs.

        Args:
            slots: Number of API slots.
            owner: Description stored in the slot object.

        Returns:
            Slot object key, or None if every slot is taken.
        """
        for index in range(slots):
            key = self.key("slots", str(index))
            if self.put_if_absent(key, owner):
                return key
        return None

    def release(self, key: str) -> None:
        """Delete a queue object such as a slot.

        Args:
            key: Object key.
        """
        self.s3.delete_object(Bucket=self.bucket, Key=key)

    def upload_tree(self, directory: Path, *parts: str) -> None:
        """Upload every file below a directory to a queue path.

        Args:
            directory: Local directory; a missing directory uploads nothing.
            *parts: Destination path segments below the queue prefix.
        """
        if not directory.exists():
            return
        for path in sorted(p for p in directory.rglob("*") if p.is_file()):
            self.s3.upload_file(
                str(path), self.bucket, self.key(*parts, path.relative_to(directory).as_posix())
            )


def submit(queue: Queue, jobs: list[dict[str, Any]], config: Path, manifest: Path) -> None:
    """Publish jobs, configuration, and manifest for a fleet run.

    Args:
        queue: Destination queue.
        jobs: Jobs from :func:`expand_jobs`.
        config: Configuration file every worker uses for its runs.
        manifest: Prepared manifest shared by every worker.

    Raises:
        ValueError: If the queue already exists or job IDs are not unique.
    """
    if len({j["id"] for j in jobs}) != len(jobs):
        raise ValueError("Job IDs must be unique")
    if not queue.put_if_absent(queue.key("queue.json"), {"created": now(), "jobs": len(jobs)}):
        raise ValueError("Queue already exists; choose a new name")
    for name, path in (("config.toml", config), ("manifest.json", manifest)):
        queue.s3.upload_file(str(path), queue.bucket, queue.key(name))
    queue.s3.put_object(
        Bucket=queue.bucket, Key=queue.key("jobs.json"), Body=json.dumps(jobs, indent=1).encode()
    )


def run_job(job: dict[str, Any], workdir: Path) -> int:
    """Run one job as a separate orchestrator process.

    Args:
        job: Job dictionary.
        workdir: Directory with the queue's ``config.toml`` and ``manifest.json``.

    Returns:
        Process exit code.
    """
    (workdir / "logs").mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable, "-m", "pyathena_bench", "--config", str(workdir / "config.toml"),
        "run", "--manifest", str(workdir / "manifest.json"),
        "--out", str(workdir / "results" / job["id"]), *job["args"],
    ]  # fmt: skip
    with (workdir / "logs" / f"{job['id']}.log").open("w") as stream:
        return subprocess.call(command, stdout=stream, stderr=subprocess.STDOUT)


def work(
    queue: Queue,
    workdir: Path,
    api_slots: int,
    poll_seconds: float = 30.0,
    runner: Callable[[dict[str, Any], Path], int] = run_job,
) -> list[str]:
    """Claim and run queued jobs until every job has been claimed.

    Each host runs one worker, so jobs on a host never overlap. API-heavy jobs
    start only after the worker takes one of ``api_slots`` shared slots.

    Args:
        queue: Queue to consume.
        workdir: Local directory for queue files, results, and logs.
        api_slots: Maximum number of API-heavy jobs running across the fleet.
        poll_seconds: Wait before rescanning when only slot-limited jobs remain.
        runner: Function that runs one job and returns its exit code.

    Returns:
        IDs of the jobs this worker ran.
    """
    workdir.mkdir(parents=True, exist_ok=True)
    for name in ("config.toml", "manifest.json", "jobs.json"):
        (workdir / name).write_bytes(queue.read(name))
    jobs = json.loads((workdir / "jobs.json").read_text())
    host = socket.gethostname()
    completed: list[str] = []
    while True:
        claimed = queue.names("claims")
        pending = [j for j in jobs if j["id"] not in claimed]
        if not pending:
            return completed
        started = False
        for job in pending:
            owner = {"host": host, "job": job["id"], "at": now()}
            slot = queue.acquire_slot(api_slots, owner) if job["api_heavy"] else None
            if job["api_heavy"] and slot is None:
                continue
            try:
                if not queue.put_if_absent(queue.key("claims", job["id"]), owner):
                    continue
                start = now()
                code = runner(job, workdir)
                queue.upload_tree(workdir / "results" / job["id"], "results", job["id"])
                log = workdir / "logs" / f"{job['id']}.log"
                if log.exists():
                    queue.s3.upload_file(str(log), queue.bucket, queue.key("logs", log.name))
                queue.s3.put_object(
                    Bucket=queue.bucket,
                    Key=queue.key("done", job["id"]),
                    Body=json.dumps(
                        {"exit_code": code, "host": host, "start": start, "end": now()}
                    ).encode(),
                )
                completed.append(job["id"])
                started = True
                break
            finally:
                if slot is not None:
                    queue.release(slot)
        if not started:
            time.sleep(poll_seconds)


def status(queue: Queue) -> dict[str, Any]:
    """Summarize queue progress and failed jobs.

    Args:
        queue: Queue to inspect.

    Returns:
        Counts of jobs, claims, and completions, with failed job details.
    """
    jobs = json.loads(queue.read("jobs.json"))
    done = queue.names("done")
    failed = {}
    for name in sorted(done):
        record = json.loads(queue.read("done", name))
        if record["exit_code"] != 0:
            failed[name] = record
    return {
        "jobs": len(jobs),
        "claimed": len(queue.names("claims")),
        "done": len(done),
        "slots_in_use": sorted(queue.names("slots")),
        "failed": failed,
    }
