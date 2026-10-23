# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Distribute benchmark runs over identical hosts through an S3 job queue."""

from __future__ import annotations

import contextlib
import http.client
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Any

from botocore.exceptions import ClientError

from pyathena_bench.aws import active_queries, client, session
from pyathena_bench.cases import Case, matrix
from pyathena_bench.config import Settings
from pyathena_bench.report import read_records, read_trial_starts, read_trials, report

# Row cursors page through GetQueryResults, which has an account-wide request-rate quota;
# so does read_sql_query() through the REST driver.
API_FAMILIES = {"cursor", "dict", "sqla_rest"}
QUEUE_ROOT = "fleet"
# An instance tag with the value "true" keeps a host running after its worker exits.
KEEP_HOST_TAG = "KeepHost"
# A file in a worker's directory that it treats like a Spot interruption notice, to
# rehearse interruptions.
SIMULATED_NOTICE = "interruption-notice"
METADATA_URL = "http://169.254.169.254/latest"
_logger = logging.getLogger(__name__)
# Errors of an instance metadata request, including a missing path (HTTP 404).
METADATA_ERRORS = (OSError, http.client.HTTPException)
# S3 errors of a conditional write whose condition failed.
PRECONDITION_ERRORS = {"PreconditionFailed", "ConditionalRequestConflict"}
# Error codes of requests that Athena throttled.
THROTTLING_ERRORS = {"ThrottlingException", "TooManyRequestsException"}
# Retries per Athena request while quiescing; the default client's 5 retries did
# not outlast the throttling of concurrent history scans in a fleet.
QUIESCE_RETRIES = 10
# Times a worker quiesces its own failed job while Athena only throttles it.
QUIESCE_ROUNDS = 3
# Virtual environments that a job can name, each synced with the dependency group of
# the same name beside the default environment, which has the polars1 group.
ENVIRONMENTS = ("polars2", "s3fs")
# APIs whose cases run only in a named environment, which their jobs get without a
# suffix to their IDs, as the API already names it.
API_ENVIRONMENTS = {"s3fs": "s3fs"}


@dataclass(frozen=True)
class Filters:
    """Case filters shared by ``plan``, ``run``, and ``jobs``."""

    family: list[str] | None = None
    api: list[str] | None = None
    transport: list[str] | None = None
    output_kind: str | None = None
    arraysize: list[int] | None = None
    # Chunk sizes to keep, with 0 for reads without chunks.
    chunksize: list[int] | None = None

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
            and (not self.chunksize or (c.chunksize or 0) in self.chunksize)
        ]


def api_pages(case: Case, rows: int) -> int:
    """Estimate GetQueryResults pages that one trial of a case requests.

    Args:
        case: Benchmark case.
        rows: Rows in the selected snapshot.

    The initialization suite counts too: it validates the constructed result
    set by reading every row.

    Returns:
        Page count summed over the trial's concurrent queries; 0 for cases that
        do not page through the API.
    """
    if case.family not in API_FAMILIES:
        return 0
    # The first page of a SELECT result also carries the column labels.
    return -(-(rows + 1) // case.arraysize) * case.concurrency


def job_order(job: dict[str, Any]) -> tuple[bool, int, int]:
    """Return the sort key that puts API-heavy jobs first and longer jobs earlier.

    Args:
        job: Job dictionary.

    Returns:
        Key for :func:`sorted`.
    """
    return (not job["api_heavy"], -job["pages"], -job["weight"])


def merge_jobs(job_lists: Iterable[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Combine job lists from several ``jobs`` invocations into one queue order.

    Args:
        job_lists: Job lists from :func:`expand_jobs`.

    Returns:
        All jobs, ordered as :func:`expand_jobs` orders one list.

    Raises:
        ValueError: If job IDs are not unique, or a job names an unknown environment.
    """
    jobs = [job for jobs in job_lists for job in jobs]
    if len({j["id"] for j in jobs}) != len(jobs):
        raise ValueError("Job IDs must be unique")
    unknown = [j["id"] for j in jobs if j.get("environment") not in (None, *ENVIRONMENTS)]
    if unknown:
        raise ValueError(f"Jobs name an unknown environment: {', '.join(unknown)}")
    return sorted(jobs, key=job_order)


def expand_jobs(
    settings: Settings,
    suites: list[str],
    scales: list[str],
    shapes: list[str],
    filters: Filters,
    split_pages: int | None = None,
    explicit_trials: bool = False,
    environment: str | None = None,
) -> list[dict[str, Any]]:
    """Split a selection into independent ``run`` invocations.

    A job covers one suite, scale, shape, family, API, transport, and output
    kind, and additionally one arraysize for row output. With a chunk size
    filter, each job runs and names the chunk sizes of its group's cases. Jobs that page through
    GetQueryResults are marked with the number of simultaneous paging queries
    so workers can bound the fleet's request rate. When one trial of such a job
    needs at least ``split_pages`` pages, each measured repetition becomes a
    separate single-trial job without a warmup. Jobs of the filesystem and
    objects suites, which do not query Athena, are weighed by the size of
    their object and by their object count.

    Args:
        settings: Configuration that defines the matrix and repetitions.
        suites: Suites to expand.
        scales: Scale names defined in the configuration.
        shapes: Result shapes to expand.
        filters: Case filters applied before grouping.
        split_pages: Per-trial page threshold for splitting repetitions.
        explicit_trials: Whether jobs that are not split carry the warmups and
            repetitions of ``settings`` as arguments, so a queue can mix trial
            counts under one stored configuration.
        environment: One of ``ENVIRONMENTS`` that runs the jobs, which their IDs
            name, or None for the default environment. Jobs of an API in
            ``API_ENVIRONMENTS`` run in that API's environment instead.

    Returns:
        Job dictionaries ordered with API-heavy jobs first and longer jobs earlier.

    Raises:
        ValueError: If no case matches the selection, or ``environment`` differs
            from the environment of a selected API.
    """
    jobs: list[dict[str, Any]] = []
    for suite in suites:
        for scale in scales:
            rows = settings.suite_scales(suite)[scale]
            if suite == "filesystem":
                # Weighed as rows that take about as long as transferring the bytes:
                # 100,000 rows and 1 GB each take seconds.
                rows //= 10_000
            for shape in shapes:
                groups: dict[tuple[Any, ...], list[Case]] = {}
                for case in filters.select(matrix(settings, suite, shape)):
                    arraysize = case.arraysize if case.output == "rows" else None
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
                    if filters.chunksize:
                        # The job runs its group's chunk sizes, which its ID names in
                        # order, so equal selections share an ID.
                        chunks = [str(c) for c in sorted({c.chunksize or 0 for c in cases})]
                        base += "-c" + "_".join(chunks)
                        args += ["--chunksize", *chunks]
                    api_environment = API_ENVIRONMENTS.get(api)
                    if api_environment and environment not in (None, api_environment):
                        raise ValueError(
                            f"The {api} API runs in the {api_environment} environment, "
                            f"not {environment}"
                        )
                    runnable = [c for c in cases if c.unsupported_reason(settings, scale) is None]
                    pages = max((api_pages(c, rows) for c in runnable), default=0)
                    api_weight = max(
                        (c.concurrency for c in runnable if api_pages(c, rows)), default=0
                    )
                    weight = rows * max(len(runnable), 1)
                    if split_pages and pages >= split_pages:
                        jobs.extend(
                            {
                                "id": f"{base}-r{repetition}",
                                "args": [*args, "--warmups", "0", "--repetitions", "1"],
                                "api_heavy": True,
                                "api_weight": api_weight,
                                "pages": pages * len(runnable),
                                "weight": weight,
                                **({"environment": api_environment} if api_environment else {}),
                            }
                            for repetition in range(1, settings.repetitions + 1)
                        )
                    else:
                        trials = settings.warmups + settings.repetitions
                        if explicit_trials:
                            args += [
                                "--warmups", str(settings.warmups),
                                "--repetitions", str(settings.repetitions),
                            ]  # fmt: skip
                        jobs.append(
                            {
                                "id": base,
                                "args": args,
                                "api_heavy": pages > 0,
                                "api_weight": api_weight,
                                "pages": pages * len(runnable) * trials,
                                "weight": weight * trials,
                                **({"environment": api_environment} if api_environment else {}),
                            }
                        )
    if not jobs:
        raise ValueError("No cases match the selected filters")
    if environment:
        for job in jobs:
            if "environment" in job:
                continue
            job["id"] += f"-{environment}"
            job["environment"] = environment
    return sorted(jobs, key=job_order)


def interpreter(environment: str | None) -> Path:
    """Return the Python interpreter that runs a job.

    Args:
        environment: The job's environment, a virtual environment named
            ``.venv-<environment>`` beside the one of this process, or None for
            this process's interpreter.

    Returns:
        Path of the interpreter.
    """
    if environment is None:
        return Path(sys.executable)
    return Path(sys.prefix).with_name(f".venv-{environment}") / "bin" / "python"


def now() -> str:
    """Return the current UTC time in ISO 8601 format.

    Returns:
        Timestamp with second precision.
    """
    return datetime.now(UTC).isoformat(timespec="seconds")


@dataclass
class Lease:
    """A claim or API slot object held by one worker until it expires.

    The body records the owner, the attempt number, the state (``running``,
    ``interrupted``, or ``released``), and the times of the last write
    (``renewed``) and of the expiry, as Unix times. A worker renews its leases
    while it runs a job; another worker may take over a lease that has expired
    or was ended.
    """

    key: str
    body: dict[str, Any]
    etag: str

    def takeable(self, at: float) -> bool:
        """Return whether another worker may take over the lease.

        Args:
            at: Current Unix time.

        Returns:
            True if the lease was ended or expired before ``at``.
        """
        return self.body["state"] != "running" or self.body["expires"] < at


class Queue:
    """S3 layout of one named fleet run under ``<base>fleet/<name>/`` in a bucket."""

    def __init__(self, s3: Any, bucket: str, name: str, base: str = "") -> None:
        if not name or "/" in name:
            raise ValueError("Queue name must be a non-empty path segment")
        if base and not base.endswith("/"):
            raise ValueError("Queue base prefix must end with a slash")
        self.s3 = s3
        self.bucket = bucket
        self.prefix = f"{base}{QUEUE_ROOT}/{name}/"

    def key(self, *parts: str) -> str:
        """Return the object key for a queue path.

        Args:
            *parts: Path segments below the queue prefix.

        Returns:
            Object key.
        """
        return self.prefix + "/".join(parts)

    def put(self, key: str, body: Any, **condition: str) -> str | None:
        """Write a JSON object, optionally only if a condition holds.

        Args:
            key: Object key.
            body: JSON-serializable content.
            **condition: ``IfNoneMatch="*"`` or ``IfMatch=<etag>``.

        Returns:
            The new ETag, or None if the condition failed.

        Raises:
            ClientError: For S3 errors other than a failed precondition.
        """
        try:
            response = self.s3.put_object(
                Bucket=self.bucket, Key=key, Body=json.dumps(body).encode(), **condition
            )
        except ClientError as error:
            if error.response["Error"]["Code"] in PRECONDITION_ERRORS:
                return None
            raise
        return str(response["ETag"])

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
        return self.put(key, body, IfNoneMatch="*") is not None

    def read_lease(self, key: str) -> Lease | None:
        """Read a claim or slot object.

        Args:
            key: Object key.

        Returns:
            The lease, or None if the object does not exist.
        """
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            if error.response["Error"]["Code"] in {"NoSuchKey", "404"}:
                return None
            raise
        return Lease(key, json.loads(response["Body"].read()), str(response["ETag"]))

    def put_lease(self, key: str, body: dict[str, Any], **condition: str) -> Lease | None:
        """Write a lease conditionally, recognizing a write that already succeeded.

        When the response to a successful conditional write is lost, the SDK
        retries it, and the retry fails its condition. An object that holds
        exactly ``body`` is therefore this worker's own write.

        Args:
            key: Object key.
            body: Lease content, unique through its expiry time.
            **condition: ``IfNoneMatch="*"`` or ``IfMatch=<etag>``.

        Returns:
            The lease, or None if the condition failed for another writer's object.
        """
        etag = self.put(key, body, **condition)
        if etag is not None:
            return Lease(key, body, etag)
        current = self.read_lease(key)
        if current is not None and current.body == body:
            return current
        return None

    def acquire(self, key: str, owner: dict[str, Any], lease_seconds: float) -> Lease | None:
        """Take a lease on an object that is absent, expired, or ended.

        Creation and takeover are conditional writes, so exactly one worker
        obtains each lease. A takeover increments the attempt number.

        Args:
            key: Object key of the claim or slot.
            owner: Description of the worker and job.
            lease_seconds: Seconds until the lease expires unless renewed.

        Returns:
            The lease, or None if another worker holds it.
        """
        return self.take(key, self.read_lease(key), owner, lease_seconds)

    def take(
        self, key: str, current: Lease | None, owner: dict[str, Any], lease_seconds: float
    ) -> Lease | None:
        """Take a lease given the object that :meth:`read_lease` returned for it.

        Args:
            key: Object key of the claim or slot.
            current: The object as read, or None if it did not exist.
            owner: Description of the worker and job.
            lease_seconds: Seconds until the lease expires unless renewed.

        Returns:
            The lease, or None if another worker holds it or wrote it since.
        """
        at = time.time()
        body = {
            **owner,
            "attempt": 1,
            "state": "running",
            "renewed": at,
            "expires": at + lease_seconds,
        }
        if current is None:
            return self.put_lease(key, body, IfNoneMatch="*")
        if not current.takeable(at):
            return None
        body["attempt"] = current.body["attempt"] + 1
        return self.put_lease(key, body, IfMatch=current.etag)

    def renew(self, lease: Lease, lease_seconds: float, **changes: Any) -> bool:
        """Extend or update a lease if this worker still holds it.

        Args:
            lease: Lease obtained from :meth:`acquire`; updated in place.
            lease_seconds: Seconds from now until the lease expires.
            **changes: Other fields to change, such as ``state``.

        Returns:
            False if another worker took over the lease.
        """
        at = time.time()
        body = {**lease.body, "renewed": at, "expires": at + lease_seconds, **changes}
        written = self.put_lease(lease.key, body, IfMatch=lease.etag)
        if written is None:
            return False
        lease.body, lease.etag = written.body, written.etag
        return True

    def end_lease(self, lease: Lease, state: str) -> bool:
        """End a lease that this worker still holds, so another worker can take it.

        The object is overwritten with ``state`` rather than deleted, so that a
        lease another worker took over in the meantime stays intact. If a
        concurrent renewal by this worker gave the object a newer ETag, the
        current object is ended when it has the same owner and attempt.

        Args:
            lease: Lease to end.
            state: ``released`` or ``interrupted``.

        Returns:
            False if another worker took over the lease.
        """
        if self.renew(lease, 0, state=state, expires=0):
            return True
        current = self.read_lease(lease.key)
        if current is None or any(
            current.body.get(k) != lease.body.get(k) for k in ("host", "job", "attempt")
        ):
            return False
        return self.renew(current, 0, state=state, expires=0)

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

    def acquire_slots(
        self, slots: int, count: int, owner: dict[str, Any], lease_seconds: float
    ) -> list[Lease] | None:
        """Take API slots that bound simultaneous GetQueryResults paging.

        Every slot is read first, and slots are written only when enough are
        free; a slot whose lease expired, because its worker died, is free.

        Args:
            slots: Number of API slots in the fleet.
            count: Slots needed, one per simultaneous paging query.
            owner: Description stored in each slot object.
            lease_seconds: Seconds until each slot lease expires unless renewed.

        Returns:
            Slot leases, or None if not enough slots are free. Slots taken
            before a shortage or an error are released.

        Raises:
            ValueError: If the job needs more slots than the fleet has.
        """
        needed = max(count, 1)
        if needed > slots:
            raise ValueError(f"A job needs {needed} API slots, but only {slots} exist")
        at = time.time()
        free = []
        for index in range(slots):
            key = self.key("slots", str(index))
            current = self.read_lease(key)
            if current is None or current.takeable(at):
                free.append((key, current))
        if len(free) < needed:
            return None
        taken: list[Lease] = []
        try:
            for key, current in free:
                lease = self.take(key, current, owner, lease_seconds)
                if lease is not None:
                    taken.append(lease)
                    if len(taken) == needed:
                        return taken
        except BaseException:
            for lease in taken:
                self.end_lease(lease, "released")
            raise
        for lease in taken:
            self.end_lease(lease, "released")
        return None

    def exists(self, *parts: str) -> bool:
        """Return whether a queue object exists.

        Args:
            *parts: Path segments below the queue prefix.

        Returns:
            True if the object exists.
        """
        return bool(
            self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.key(*parts), MaxKeys=1).get(
                "KeyCount"
            )
        )

    def release(self, key: str) -> None:
        """Delete a queue object such as a slot.

        Args:
            key: Object key.
        """
        self.s3.delete_object(Bucket=self.bucket, Key=key)

    def upload_tree(self, directory: Path, *parts: str, pattern: str = "*") -> None:
        """Upload every file below a directory to a queue path.

        Args:
            directory: Local directory; a missing directory uploads nothing.
            *parts: Destination path segments below the queue prefix.
            pattern: Glob pattern of the files to upload, relative to ``directory``.
        """
        if not directory.exists():
            return
        for path in sorted(p for p in directory.rglob(pattern) if p.is_file()):
            self.s3.upload_file(
                str(path), self.bucket, self.key(*parts, path.relative_to(directory).as_posix())
            )

    def download_tree(self, directory: Path, *parts: str) -> list[str]:
        """Download every object below a queue path into a local directory.

        Args:
            directory: Local destination directory.
            *parts: Source path segments below the queue prefix.

        Returns:
            Relative paths of the downloaded files.

        Raises:
            ValueError: If an object name would be written outside ``directory``.
        """
        source = self.key(*parts) + "/"
        names = sorted(self.names("/".join(parts)))
        root = directory.resolve()
        for name in names:
            path = (directory / name).resolve()
            if not path.is_relative_to(root):
                raise ValueError(f"Object {source + name} is outside {directory}")
            path.parent.mkdir(parents=True, exist_ok=True)
            self.s3.download_file(self.bucket, source + name, str(path))
        return names


def submit(queue: Queue, jobs: list[dict[str, Any]], config: Path, manifest: Path) -> None:
    """Publish jobs, configuration, and manifest for a fleet run.

    Args:
        queue: Destination queue.
        jobs: Jobs from :func:`expand_jobs`.
        config: Configuration file every worker uses for its runs.
        manifest: Prepared manifest shared by every worker.

    Raises:
        ValueError: If the name is reserved or job IDs are not unique.
    """
    if len({j["id"] for j in jobs}) != len(jobs):
        raise ValueError("Job IDs must be unique")
    # The reservation keeps concurrent submissions from mixing files under one name.
    reservation = queue.key("reservation.json")
    if not queue.put_if_absent(reservation, {"host": socket.gethostname(), "at": now()}):
        raise ValueError("Queue name is already reserved; choose a new name")
    try:
        if queue.exists("queue.json"):
            # A published queue owns its name even without a reservation object.
            raise ValueError("Queue already exists; choose a new name")
        for name, path in (("config.toml", config), ("manifest.json", manifest)):
            queue.s3.upload_file(str(path), queue.bucket, queue.key(name))
        queue.put(queue.key("jobs.json"), jobs)
        # Workers start only after this marker.
        if not queue.put_if_absent(queue.key("queue.json"), {"created": now(), "jobs": len(jobs)}):
            raise ValueError("Queue already exists; choose a new name")
    except BaseException:
        # Allow the same name to be published again after an interrupted upload.
        queue.release(reservation)
        raise


def run_job(
    job: dict[str, Any],
    workdir: Path,
    output: Path,
    resume: list[Path],
    stop: threading.Event,
    checkpoint: str,
) -> int:
    """Run one job attempt as a separate orchestrator process.

    When ``stop`` is set, the orchestrator receives SIGINT, which stops the
    running trial, cancels its reported queries, and records the trial as
    interrupted; it is killed if it has not exited a minute later. The
    orchestrator runs in its own process group, which is killed when it
    ends, so no trial process outlives it.

    Args:
        job: Job dictionary, whose environment selects the interpreter (see
            :func:`interpreter`).
        workdir: Directory with the queue's ``config.toml`` and ``manifest.json``.
        output: New output directory for this attempt.
        resume: Output directories of earlier attempts whose completed trials
            are not repeated.
        stop: Event that interrupts the attempt.
        checkpoint: The attempt's ``s3://`` prefix for per-trial checkpoints.

    Returns:
        Process exit code.
    """
    (workdir / "logs").mkdir(parents=True, exist_ok=True)
    command = [
        str(interpreter(job.get("environment"))), "-m", "pyathena_bench",
        "--config", str(workdir / "config.toml"),
        "run", "--manifest", str(workdir / "manifest.json"), "--out", str(output),
        "--checkpoint", checkpoint, *job["args"],
    ]  # fmt: skip
    if resume:
        command += ["--resume", *map(str, resume)]
    with (workdir / "logs" / f"{job['id']}-{output.name}.log").open("w") as stream:
        process = subprocess.Popen(
            command, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True
        )
        try:
            while process.poll() is None:
                if stop.wait(1):
                    process.send_signal(signal.SIGINT)
                    try:
                        return process.wait(timeout=60)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        return process.wait()
            return process.returncode
        finally:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(process.pid, signal.SIGKILL)


def trial_evidence(output: Path) -> tuple[set[str], tuple[str, ...]]:
    """Collect the queries of a job's trials that may still be active.

    A trial recorded as successful ran its queries to completion. Any other
    started trial may also have started queries whose IDs it never reported,
    even if it reported others: a trial can run several queries, and its
    process can be stopped while a query starts.

    Args:
        output: Job output directory with ``events.jsonl`` or the checkpoint
            of a fleet attempt.

    Returns:
        The query IDs that trials not recorded as successful reported, in
        events or in their records, and the S3 output prefixes of those
        trials; both are empty if the job wrote no events or checkpoint.
    """
    records = read_trials(output)
    succeeded = {t.get("trial") for t in records if t.get("status") == "ok"}
    ids = {
        e["query_id"]
        for e in read_records(output / "events.jsonl")
        if e.get("event") == "query" and e.get("query_id") and e.get("trial") not in succeeded
    }
    for record in records:
        if record.get("trial") not in succeeded:
            ids.update(record.get("query_ids", []))
    prefixes = {
        e["output"]
        for e in read_trial_starts(output)
        if e.get("output") and e.get("trial") not in succeeded
    }
    return ids, tuple(sorted(prefixes))


def is_throttling(error: BaseException) -> bool:
    """Return whether an AWS request failed because it was throttled.

    Args:
        error: Exception raised by a client request.

    Returns:
        True for a :class:`ClientError` with a code in ``THROTTLING_ERRORS``.
    """
    return (
        isinstance(error, ClientError)
        and error.response.get("Error", {}).get("Code") in THROTTLING_ERRORS
    )


def quiesce(settings: Settings, output: Path) -> list[str]:
    """Stop a job's queries and wait until none is active.

    The reported queries of trials not recorded as successful are stopped
    first. Then the workgroup history is scanned for queries that write under
    those trials' output prefixes, which covers queries whose IDs were never
    reported before a trial process stopped; without such a trial, nothing is
    scanned. A failed scan does not skip the status checks of the queries
    known so far. Each Athena request is retried up to ``QUIESCE_RETRIES``
    times.

    Args:
        settings: Configuration with the AWS region, workgroup, and timeouts.
        output: Job output directory with ``events.jsonl`` or the checkpoint
            of a fleet attempt.

    Returns:
        Queries that could not be confirmed as stopped, with the reason, and
        a failed history scan as ``query history scan of <output>: <error>``.

    Raises:
        ClientError: If Athena still throttled a request after its retries
            (see :func:`is_throttling`) and nothing else kept the queries from
            being confirmed as stopped; every known query was asked to stop.
    """
    reported, prefixes = trial_evidence(output)
    athena = client(session(settings), "athena", max_retries=QUIESCE_RETRIES)
    stop_errors: dict[str, str] = {}
    # Queries left unconfirmed by throttled requests; they are not polled.
    throttled: dict[str, ClientError] = {}

    def stop(ids: Iterable[str]) -> None:
        for query_id in sorted(ids):
            try:
                athena.stop_query_execution(QueryExecutionId=query_id)
            except ClientError as exc:
                if is_throttling(exc):
                    throttled[query_id] = exc
                else:
                    stop_errors[query_id] = str(exc)

    stop(reported)
    found: set[str] = set()
    scan_error: Exception | None = None
    if prefixes:
        try:
            active_queries(athena, settings.workgroup, prefixes, found)
        except Exception as exc:
            # Queries found before a failed scan are still stopped and checked.
            scan_error = exc
    stop(found - reported)
    errors = []
    deadline = time.monotonic() + settings.timeout_seconds
    for query_id in sorted((reported | found) - throttled.keys()):
        while True:
            try:
                state = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"][
                    "Status"
                ]["State"]
            except ClientError as exc:
                if is_throttling(exc):
                    throttled[query_id] = exc
                else:
                    errors.append(f"{query_id}: {stop_errors.get(query_id, exc)}")
                break
            if state not in {"QUEUED", "RUNNING"}:
                break
            if time.monotonic() >= deadline:
                errors.append(f"{query_id}: still {state}; {stop_errors.get(query_id, '')}")
                break
            time.sleep(settings.poll_interval)
    throttling = [f"{query_id}: {exc}" for query_id, exc in sorted(throttled.items())]
    if scan_error is not None:
        failure = f"query history scan of {output.name}: {type(scan_error).__name__}: {scan_error}"
        (throttling if is_throttling(scan_error) else errors).append(failure)
    if throttling and not errors:
        # Only throttling kept the queries from being confirmed as stopped.
        raise scan_error or next(iter(throttled.values()))
    return errors + throttling


class SpotInterruptionError(Exception):
    """A Spot interruption notice stopped the worker before every job was claimed."""

    def __init__(self, ran: list[str]) -> None:
        super().__init__("Stopped by a Spot interruption notice")
        self.ran = ran


class JobWatch:
    """Renew a running job's leases and watch for an interruption notice.

    One thread checks every ``poll_seconds`` for an interruption notice and
    for the claim's expiry, without S3 requests; another renews the claim and
    the API slots ten times per lease period. ``interrupted`` is set on a
    notice, and ``claim_lost`` when another worker took over the claim or
    renewals failed or stalled until a third of the lease period was left;
    ``stop`` is set with either and stops the runner. A lost slot is listed in
    ``lost_slots`` and does not stop the job.
    """

    def __init__(
        self,
        queue: Queue,
        claim: Lease,
        slots: list[Lease],
        lease_seconds: float,
        notice: Callable[[], bool],
        poll_seconds: float = 5.0,
    ) -> None:
        self.queue = queue
        self.claim = claim
        self.slots = list(slots)
        self.lease_seconds = lease_seconds
        self.notice = notice
        self.renew_seconds = lease_seconds / 10
        self.poll_seconds = min(poll_seconds, self.renew_seconds)
        self.interrupted = threading.Event()
        self.claim_lost = threading.Event()
        self.stop_event = threading.Event()
        self.lost_slots: list[str] = []
        self._stopped = threading.Event()
        self._threads = [
            threading.Thread(target=self._watch, daemon=True),
            threading.Thread(target=self._renew, daemon=True),
        ]

    def __enter__(self) -> JobWatch:
        for thread in self._threads:
            thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        # After a stop, a renewal stuck in S3 must not delay the release of the
        # slots; a late renewal fails against the changed ETags.
        self.stop(10 if self.stop_event.is_set() else None)

    def stop(self, timeout: float | None = None) -> None:
        """Stop the threads and wait for them.

        Args:
            timeout: Seconds to wait for each thread, or None to wait until it
                ends, after which the caller may update the leases.
        """
        self._stopped.set()
        for thread in self._threads:
            if thread.is_alive():
                thread.join(timeout)

    def _watch(self) -> None:
        while not self._stopped.wait(self.poll_seconds):
            if check_notice(self.notice):
                self.interrupted.set()
                self.stop_event.set()
                return
            # A third of the lease, at least 100 seconds with the minimum
            # --lease-seconds, is left for run_job() to stop the runner, which
            # takes about a minute, before another worker can take the claim
            # over, so that unless this host stalls, the attempt starts no
            # query after that worker's scan.
            if time.time() >= self.claim.body["expires"] - self.lease_seconds / 3:
                self._lose_claim()
                return

    def _renew(self) -> None:
        while not self._stopped.wait(self.renew_seconds):
            if not self.claim_lost.is_set() and not self._renew_lease(self.claim):
                self._lose_claim()
            for slot in list(self.slots):
                if not self._renew_lease(slot):
                    self.slots.remove(slot)
                    self.lost_slots.append(slot.key)

    def _renew_lease(self, lease: Lease) -> bool:
        try:
            return self.queue.renew(lease, self.lease_seconds)
        except Exception:
            # The lease has time left for a retry until it expires; _watch gives
            # the claim up earlier.
            return time.time() < lease.body["expires"]

    def _lose_claim(self) -> None:
        self.claim_lost.set()
        self.stop_event.set()


def pending_jobs(queue: Queue, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return the jobs that are neither done nor held by a live claim.

    Args:
        queue: Queue to inspect.
        jobs: Jobs in queue order.

    Returns:
        Jobs without a ``done`` marker whose claim is absent, expired, or
        ended, in queue order.
    """
    done = queue.names("done")
    claimed = queue.names("claims")
    at = time.time()
    pending = []
    for job in jobs:
        if job["id"] in done:
            continue
        if job["id"] in claimed:
            lease = queue.read_lease(queue.key("claims", job["id"]))
            if lease is not None and not lease.takeable(at):
                continue
        pending.append(job)
    return pending


@dataclass(frozen=True)
class Outcome:
    """Result of one job attempt.

    ``state`` is ``done`` when the attempt recorded the job's outcome,
    ``interrupted`` after a Spot interruption notice, ``lost`` when another
    worker took over the job's claim or a slot, and ``deferred`` when Athena
    throttled the quiescing of an earlier attempt, so the job was released
    without running. ``errors`` lists queries that could not be confirmed as
    stopped. ``noticed`` tells whether the watch observed an interruption
    notice after the attempt checked for one, for example while queries were
    quiesced.
    """

    state: str
    errors: tuple[str, ...] = ()
    noticed: bool = False


def run_attempt(
    queue: Queue,
    job: dict[str, Any],
    claim: Lease,
    slots: list[Lease],
    workdir: Path,
    runner: Callable[..., int],
    settle: Callable[[Path], list[str]],
    lease_seconds: float,
    notice: Callable[[], bool],
) -> Outcome:
    """Run one attempt of a claimed job and record its outcome.

    The attempt writes to ``results/<job>/attempt-<n>/``, and its orchestrator
    writes per-trial checkpoints there between trials. A takeover (attempt 2
    or later) first downloads the earlier attempts, quiesces their trials,
    and passes them to the runner so that completed trials are not repeated.
    If quiescing failed only because Athena throttled it, the claim is
    released for a later attempt, which quiesces the earlier attempts again.
    An interruption notice or a lost claim stops the runner: after a notice,
    the attempt is uploaded and the claim released as interrupted; after a
    lost claim, the attempt's queries are quiesced and nothing is recorded.
    A failed run's queries are also quiesced; while the claim is held, this
    is repeated up to ``QUIESCE_ROUNDS`` times, a renewal interval apart, if
    Athena only throttled it.
    Otherwise, after a conditional renewal confirms that the claim is still
    held, the job's report merges every attempt into ``job-summary.csv`` and
    ``job-summary.md`` of this attempt, and the first ``done`` marker of the
    job records the exit code and names that attempt.

    Args:
        queue: Queue of the job.
        job: Job dictionary.
        claim: Claim lease of the job.
        slots: API slot leases held for the job.
        workdir: Local working directory.
        runner: Function that runs the attempt; see :func:`run_job`.
        settle: Function that quiesces an output directory and returns errors;
            it raises a throttling error as :func:`quiesce` does.
        lease_seconds: Seconds a lease lasts without renewal.
        notice: Function that returns whether an interruption notice exists.

    Returns:
        The outcome of the attempt.
    """
    job_dir = workdir / "results" / job["id"]
    attempt = claim.body["attempt"]
    output = job_dir / f"attempt-{attempt}"
    remote = ("results", job["id"], output.name)
    log = workdir / "logs" / f"{job['id']}-{output.name}.log"

    def upload_attempt() -> None:
        queue.upload_tree(output, *remote)
        if log.exists():
            queue.s3.upload_file(str(log), queue.bucket, queue.key("logs", log.name))

    def settle_error(path: Path, exc: Exception) -> str:
        return f"Could not quiesce {path.name}: {type(exc).__name__}: {exc}"

    with JobWatch(queue, claim, slots, lease_seconds, notice) as watch:
        start = now()
        resume = [
            job_dir / f"attempt-{earlier}"
            for earlier in range(1, attempt)
            if queue.download_tree(
                job_dir / f"attempt-{earlier}", "results", job["id"], f"attempt-{earlier}"
            )
        ]
        errors: list[str] = []
        deferred = False
        for earlier in resume:
            try:
                errors += settle(earlier)
            except Exception as exc:
                if not errors and is_throttling(exc):
                    deferred = True
                    break
                errors.append(settle_error(earlier, exc))
        code = 1
        if not errors and not deferred and not watch.stop_event.is_set():
            code = runner(
                job,
                workdir,
                output,
                resume,
                watch.stop_event,
                f"s3://{queue.bucket}/{queue.key(*remote)}/",
            )
        if watch.interrupted.is_set():
            watch.stop(timeout=10)
            # Upload before releasing the claim, so that the next attempt resumes
            # from every recorded trial; both are small within a notice's two minutes.
            upload_attempt()
            queue.end_lease(claim, "interrupted")
            return Outcome("interrupted")
        if code and not errors and not deferred:
            # A failed or stopped run can leave queries running. While the
            # claim is held, quiescing that Athena only throttled is repeated,
            # so that throttling alone does not stop the worker.
            for round_ in range(1, QUIESCE_ROUNDS + 1):
                try:
                    errors = settle(output)
                    break
                except Exception as exc:
                    errors = [settle_error(output, exc)]
                    if not is_throttling(exc) or round_ == QUIESCE_ROUNDS:
                        break
                _logger.warning(f"Athena throttled quiescing {job['id']}; retrying")
                # The pause lets concurrent history scans thin out; a notice or
                # a lost claim ends it and the rounds.
                if watch.stop_event.wait(lease_seconds / 10):
                    break
        upload_attempt()

    # Each outcome reads the watch's notice last: after a lost claim, the watch
    # is joined for only 10 seconds, and a notice check may still finish.
    if deferred:
        # Nothing ran, so the next attempt resumes from the same earlier attempts.
        queue.end_lease(claim, "released")
        return Outcome("deferred", noticed=watch.interrupted.is_set())

    def abandon() -> Outcome:
        # A claim lost only by its local expiry, for example while a renewal that
        # S3 accepted stalled, may still be this worker's; releasing it, which
        # works only then, returns the job to the fleet.
        queue.end_lease(claim, "released")
        return Outcome("lost", tuple(errors), noticed=watch.interrupted.is_set())

    # The conditional renewal proves that this worker still owns the job, so a
    # former owner never records it, and leaves at least --lease-seconds for
    # the few seconds of publication.
    if watch.claim_lost.is_set() or not queue.renew(claim, lease_seconds):
        return abandon()
    report_error = None
    try:
        report(job_dir)
    except Exception as exc:
        # A job without trials, or with unreadable records, gets a failed done marker.
        report_error = f"{type(exc).__name__}: {exc}"
    if report_error is None:
        try:
            # The job's summary goes below this attempt, which the done marker names,
            # so a former owner's late upload cannot replace the recorded summary.
            for name in ("summary.csv", "summary.md"):
                queue.s3.upload_file(
                    str(job_dir / name), queue.bucket, queue.key(*remote, f"job-{name}")
                )
        except Exception:
            # Another worker publishes the job again from the uploaded attempts.
            queue.end_lease(claim, "released")
            raise
    if time.time() >= claim.body["expires"]:
        # Publication outlasted the renewed claim, which another worker may own now.
        return abandon()
    recorded = queue.put_lease(
        queue.key("done", job["id"]),
        {
            "exit_code": code,
            "host": socket.gethostname(),
            "attempt": attempt,
            "start": start,
            "end": now(),
            "quiesce_errors": errors,
            "report_error": report_error,
            "summary": None if report_error else "/".join(remote[1:]),
            "lost_slots": watch.lost_slots,
        },
        IfNoneMatch="*",
    )
    if recorded is None:
        return abandon()
    return Outcome("done", tuple(errors), noticed=watch.interrupted.is_set())


def work(
    queue: Queue,
    workdir: Path,
    api_slots: int,
    poll_seconds: float = 30.0,
    runner: Callable[..., int] = run_job,
    settle: Callable[[Path], list[str]] | None = None,
    lease_seconds: float = 600.0,
    notice: Callable[[], bool] | None = None,
    wait: bool = False,
) -> list[str]:
    """Claim and run queued jobs until no job is left to claim.

    Each host runs one worker, so jobs on a host never overlap. A claim and
    each API slot are leases that the worker renews while the job runs; a job
    or slot whose lease expired, because its worker died, returns to the
    fleet: a running worker takes it over. An API-heavy job starts only after
    the worker takes one shared slot per simultaneous paging query; while it
    waits, no later API-heavy job takes slots. After a failed job, the worker
    cancels the job's observed queries and waits until they stop before
    claiming another job. A takeover whose quiescing Athena throttled releases
    its claim, and the worker continues with the next pending job.

    Args:
        queue: Queue to consume.
        workdir: Local directory for queue files, results, and logs.
        api_slots: Fleet-wide number of simultaneous GetQueryResults paging queries.
        poll_seconds: Wait before rescanning when only slot-limited jobs remain,
            and between checks for an unpublished queue.
        runner: Function that runs one attempt; see :func:`run_job`.
        settle: Function that quiesces a failed job, given its output
            directory, and returns errors, raising a throttling error as
            :func:`quiesce` does; defaults to :func:`quiesce` with the queued
            configuration.
        lease_seconds: Seconds a claim or slot lasts without renewal; the
            worker renews its leases ten times per period.
        notice: Function that returns whether a Spot interruption notice
            exists; defaults to :func:`interruption_notice` with the
            rehearsal file ``interruption-notice`` in ``workdir``, which the
            worker removes when it starts.
        wait: Whether to wait for the queue to be published instead of failing.

    Returns:
        IDs of the jobs this worker ran to completion.

    Raises:
        ValueError: If the queue has not been completely published and
            ``wait`` is false, a job needs more API slots than ``api_slots``,
            or a job's environment does not exist on this host.
        SpotInterruptionError: After an interruption notice, once the running attempt's
            records are uploaded and its claim is released as interrupted, or
            once the attempt of a failed job ends, if the notice arrived while
            its queries were quiesced or is found after they could not be
            confirmed as stopped, or once a deferred takeover released its
            claim.
        RuntimeError: If an attempt stopped by a lost claim could not be quiesced;
            the worker's host is then kept.
    """
    while not queue.exists("queue.json"):
        if not wait:
            raise ValueError("Queue is missing or incompletely published")
        time.sleep(poll_seconds)
    workdir.mkdir(parents=True, exist_ok=True)
    if notice is None:
        rehearsal = workdir / SIMULATED_NOTICE
        # A rehearsed notice stops one worker; a restarted worker starts clean.
        rehearsal.unlink(missing_ok=True)

        def notice() -> bool:
            return interruption_notice(simulated=rehearsal)

    for name in ("config.toml", "manifest.json", "jobs.json"):
        (workdir / name).write_bytes(queue.read(name))
    jobs = json.loads((workdir / "jobs.json").read_text())
    too_wide = [j["id"] for j in jobs if j["api_heavy"] and j.get("api_weight", 1) > api_slots]
    if too_wide:
        raise ValueError(f"Jobs need more than {api_slots} API slots: {', '.join(too_wide)}")
    missing = [j["id"] for j in jobs if not interpreter(j.get("environment")).exists()]
    if missing:
        raise ValueError(f"Jobs need an environment missing on this host: {', '.join(missing)}")
    if settle is None:
        settings = Settings.load(workdir / "config.toml")

        def settle(output: Path) -> list[str]:
            return quiesce(settings, output)

    host = socket.gethostname()
    completed: list[str] = []
    while True:
        if check_notice(notice):
            raise SpotInterruptionError(completed)
        pending = pending_jobs(queue, jobs)
        if not pending:
            return completed
        started = False
        reserved = False
        for job in pending:
            owner = {"host": host, "job": job["id"], "at": now()}
            slots: list[Lease] = []
            if job["api_heavy"]:
                if reserved:
                    continue
                acquired = queue.acquire_slots(
                    api_slots, job.get("api_weight", 1), owner, lease_seconds
                )
                if acquired is None:
                    # Let slots drain for this job instead of starving it with smaller ones.
                    reserved = True
                    continue
                slots = acquired
            try:
                claim = queue.acquire(queue.key("claims", job["id"]), owner, lease_seconds)
                if claim is None:
                    continue
                outcome = run_attempt(
                    queue,
                    job,
                    claim,
                    slots,
                    workdir,
                    runner,
                    settle,
                    lease_seconds,
                    notice,
                )
                if outcome.state == "interrupted":
                    raise SpotInterruptionError(completed)
                if outcome.state == "deferred":
                    _logger.warning(
                        f"Deferred {job['id']}: Athena throttled quiescing its earlier attempts"
                    )
                    if outcome.noticed or check_notice(notice):
                        raise SpotInterruptionError(completed)
                    # A later pass, or another worker, retries the job; try the next one.
                    continue
                if outcome.state == "lost" and outcome.errors:
                    # The job is unfinished, so the host is kept rather than retired.
                    raise RuntimeError(
                        f"Could not quiesce a stopped attempt of {job['id']}: "
                        + "; ".join(outcome.errors)
                    )
                if outcome.state == "done":
                    completed.append(job["id"])
                if outcome.noticed or (outcome.errors and check_notice(notice)):
                    # The host is being reclaimed, so it must not be retired.
                    raise SpotInterruptionError(completed)
                if outcome.errors:
                    # Unconfirmed queries could overlap later trials on this host.
                    return completed
                started = True
                break
            finally:
                for slot in slots:
                    queue.end_lease(slot, "released")
        if not started:
            time.sleep(poll_seconds)


def instance_metadata(path: str, timeout: float = 2.0) -> str:
    """Read a value from the EC2 instance metadata service (IMDSv2).

    Args:
        path: Path below ``meta-data/``, such as ``instance-id``.
        timeout: Seconds to wait for each metadata request.

    Returns:
        Metadata value.
    """
    token = urllib.request.Request(
        f"{METADATA_URL}/api/token",
        method="PUT",
        headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
    )
    with urllib.request.urlopen(token, timeout=timeout) as response:
        value = response.read().decode()
    request = urllib.request.Request(
        f"{METADATA_URL}/meta-data/{path}", headers={"X-aws-ec2-metadata-token": value}
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode()


def check_notice(notice: Callable[[], bool]) -> bool:
    """Call a notice check, treating an unexpected error as no notice.

    Args:
        notice: Function that returns whether an interruption notice exists.

    Returns:
        The result, or False if the check raised.
    """
    try:
        return notice()
    except Exception:
        _logger.warning("Could not check for an interruption notice", exc_info=True)
        return False


def interruption_notice(
    metadata: Callable[[str], str] | None = None, simulated: Path | None = None
) -> bool:
    """Return whether this host has a Spot interruption notice.

    EC2 publishes ``spot/instance-action`` in instance metadata about two
    minutes before it stops or terminates a Spot Instance; the path does not
    exist otherwise. A file at ``simulated`` counts as a notice, so that a
    pilot can rehearse an interruption.

    Args:
        metadata: Function that reads an instance metadata path; defaults to
            :func:`instance_metadata`.
        simulated: File that counts as a notice, or None.

    Returns:
        True if a notice exists; False also when instance metadata is unavailable.
    """
    if simulated is not None and simulated.exists():
        return True
    try:
        (metadata or instance_metadata)("spot/instance-action")
    except METADATA_ERRORS:
        return False
    return True


def keep_host(metadata: Callable[[str], str]) -> bool:
    """Return whether the host's ``KeepHost`` instance tag is ``true``.

    The launch template exposes instance tags in instance metadata.

    Args:
        metadata: Function that reads an instance metadata path.

    Returns:
        True if the tag exists with the value ``true``.
    """
    try:
        return metadata(f"tags/instance/{KEEP_HOST_TAG}").strip().lower() == "true"
    except METADATA_ERRORS:
        return False


def suspend_rebalance(autoscaling: Any, metadata: Callable[[str], str] = instance_metadata) -> str:
    """Suspend zone rebalancing in the host's Auto Scaling group.

    With ``AZRebalance`` active, the group may launch a host in one zone and
    terminate a busy host in another when Spot capacity or retired hosts leave
    the zones uneven. The process stays suspended until the stack is deleted.

    Args:
        autoscaling: Auto Scaling client.
        metadata: Function that reads an instance metadata path.

    Returns:
        Name of the Auto Scaling group.
    """
    host = metadata("instance-id")
    group = autoscaling.describe_auto_scaling_instances(InstanceIds=[host])["AutoScalingInstances"][
        0
    ]["AutoScalingGroupName"]
    autoscaling.suspend_processes(AutoScalingGroupName=group, ScalingProcesses=["AZRebalance"])
    return str(group)


def retire_host(
    queue: Queue,
    autoscaling: Any,
    ran: list[str],
    metadata: Callable[[str], str] = instance_metadata,
) -> bool:
    """Record a finished worker and terminate its host unless the host is kept.

    The record under ``hosts/`` in the queue names the host's instance type,
    Availability Zone, and purchase option. Terminating with a decremented
    desired capacity keeps the group from launching a replacement. A host
    whose ``KeepHost`` tag is ``true`` keeps running.

    Args:
        queue: Queue the worker consumed.
        autoscaling: Auto Scaling client.
        ran: IDs of the jobs the worker ran.
        metadata: Function that reads an instance metadata path.

    Returns:
        True if termination was requested.
    """
    host = metadata("instance-id")
    kept = keep_host(metadata)
    queue.put(
        queue.key("hosts", f"{host}.json"),
        {
            "host": socket.gethostname(),
            "instance_type": metadata("instance-type"),
            "availability_zone": metadata("placement/availability-zone"),
            "lifecycle": metadata("instance-life-cycle"),
            "ran": ran,
            "end": now(),
            "kept": kept,
        },
    )
    if kept:
        return False
    autoscaling.terminate_instance_in_auto_scaling_group(
        InstanceId=host, ShouldDecrementDesiredCapacity=True
    )
    return True


def status(queue: Queue) -> dict[str, Any]:
    """Summarize queue progress, failed jobs, and leases.

    Args:
        queue: Queue to inspect.

    Returns:
        Counts of jobs, claims, and completions, with the details of failed
        jobs (a non-zero exit code or a report error), the
        claims of unfinished jobs, and the API slots in use, each lease with
        its host, attempt, state, and whether it has expired.
    """
    if not queue.exists("queue.json"):
        raise ValueError("Queue is missing or incompletely published")
    jobs = json.loads(queue.read("jobs.json"))
    done = queue.names("done")
    failed = {}
    for name in sorted(done):
        record = json.loads(queue.read("done", name))
        if record["exit_code"] != 0 or record.get("report_error"):
            failed[name] = record
    at = time.time()

    def describe(folder: str, names: Iterable[str]) -> dict[str, Any]:
        leases = {}
        for name in sorted(names):
            lease = queue.read_lease(queue.key(folder, name))
            if lease is not None and lease.body["state"] != "released":
                leases[name] = {
                    **{k: lease.body.get(k) for k in ("host", "job", "attempt", "state")},
                    "expired": lease.body["state"] == "running" and lease.takeable(at),
                }
        return leases

    claims = queue.names("claims")
    return {
        "jobs": len(jobs),
        "claimed": len(claims),
        "done": len(done),
        "running": describe("claims", claims - done),
        "slots_in_use": describe("slots", queue.names("slots")),
        "failed": failed,
    }
