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

from pyathena_bench.aws import active_queries, client, session
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
    kind, and additionally one arraysize for row output. Jobs that page through
    GetQueryResults are marked with the number of simultaneous paging queries
    so workers can bound the fleet's request rate. When one trial of such a job
    needs at least ``split_pages`` pages, each measured repetition becomes a
    separate single-trial job without a warmup.

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
                    runnable = [c for c in cases if c.unsupported is None]
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
                                "api_weight": api_weight,
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

    def acquire_slots(self, slots: int, count: int, owner: dict[str, Any]) -> list[str] | None:
        """Take API slots that bound simultaneous GetQueryResults paging.

        Args:
            slots: Number of API slots in the fleet.
            count: Slots needed, one per simultaneous paging query.
            owner: Description stored in each slot object.

        Returns:
            Slot object keys, or None if not enough slots are free. Slots taken
            before a shortage or an error are released.

        Raises:
            ValueError: If the job needs more slots than the fleet has.
        """
        needed = max(count, 1)
        if needed > slots:
            raise ValueError(f"A job needs {needed} API slots, but only {slots} exist")
        taken: list[str] = []
        try:
            for index in range(slots):
                key = self.key("slots", str(index))
                if self.put_if_absent(key, owner):
                    taken.append(key)
                    if len(taken) == needed:
                        return taken
        except BaseException:
            for key in taken:
                self.release(key)
            raise
        for key in taken:
            self.release(key)
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
        queue.s3.put_object(
            Bucket=queue.bucket,
            Key=queue.key("jobs.json"),
            Body=json.dumps(jobs, indent=1).encode(),
        )
        # Workers start only after this marker.
        if not queue.put_if_absent(queue.key("queue.json"), {"created": now(), "jobs": len(jobs)}):
            raise ValueError("Queue already exists; choose a new name")
    except BaseException:
        # Allow the same name to be published again after an interrupted upload.
        queue.release(reservation)
        raise


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


def trial_evidence(output: Path) -> tuple[set[str], tuple[str, ...]]:
    """Collect the query IDs and output prefixes that a job's trials used.

    Args:
        output: Job output directory with ``events.jsonl``.

    Returns:
        Reported query IDs and the S3 output prefixes of started trials; both
        are empty if the job wrote no events.
    """
    events = output / "events.jsonl"
    ids: set[str] = set()
    prefixes: set[str] = set()
    if not events.exists():
        return ids, ()
    for line in events.read_text().splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("event") == "query" and event.get("query_id"):
            ids.add(event["query_id"])
        elif event.get("event") == "trial_start" and event.get("output"):
            prefixes.add(event["output"])
    return ids, tuple(sorted(prefixes))


def quiesce(settings: Settings, output: Path) -> list[str]:
    """Stop a failed job's queries and wait until none is active.

    Besides the reported query IDs, the workgroup history is scanned for queries
    that write under the job's trial prefixes, which covers queries whose IDs
    were never reported before a trial process died.

    Args:
        settings: Configuration with the AWS region, workgroup, and timeouts.
        output: Job output directory with ``events.jsonl``.

    Returns:
        Queries that could not be confirmed as stopped, with the reason.
    """
    reported, prefixes = trial_evidence(output)
    athena = client(session(settings), "athena")
    ids = set(reported)
    if prefixes:
        ids |= active_queries(athena, settings.workgroup, prefixes)
    stop_errors: dict[str, str] = {}
    for query_id in sorted(ids):
        try:
            athena.stop_query_execution(QueryExecutionId=query_id)
        except ClientError as exc:
            stop_errors[query_id] = str(exc)
    errors = []
    deadline = time.monotonic() + settings.timeout_seconds
    for query_id in sorted(ids):
        while True:
            try:
                state = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"][
                    "Status"
                ]["State"]
            except ClientError as exc:
                errors.append(f"{query_id}: {stop_errors.get(query_id, exc)}")
                break
            if state not in {"QUEUED", "RUNNING"}:
                break
            if time.monotonic() >= deadline:
                errors.append(f"{query_id}: still {state}; {stop_errors.get(query_id, '')}")
                break
            time.sleep(settings.poll_interval)
    return errors


def work(
    queue: Queue,
    workdir: Path,
    api_slots: int,
    poll_seconds: float = 30.0,
    runner: Callable[[dict[str, Any], Path], int] = run_job,
    settle: Callable[[Path], list[str]] | None = None,
) -> list[str]:
    """Claim and run queued jobs until every job has been claimed.

    Each host runs one worker, so jobs on a host never overlap. An API-heavy
    job starts only after the worker takes one shared slot per simultaneous
    paging query; while it waits, no later API-heavy job takes slots. After a
    failed job, the worker cancels the job's observed queries and waits until
    they stop before claiming another job.

    Args:
        queue: Queue to consume.
        workdir: Local directory for queue files, results, and logs.
        api_slots: Fleet-wide number of simultaneous GetQueryResults paging queries.
        poll_seconds: Wait before rescanning when only slot-limited jobs remain.
        runner: Function that runs one job and returns its exit code.
        settle: Function that quiesces a failed job, given its output
            directory, and returns errors; defaults to :func:`quiesce` with the
            queued configuration.

    Returns:
        IDs of the jobs this worker ran.

    Raises:
        ValueError: If the queue has not been completely published, or a job
            needs more API slots than ``api_slots``.
    """
    if not queue.exists("queue.json"):
        raise ValueError("Queue is missing or incompletely published")
    workdir.mkdir(parents=True, exist_ok=True)
    for name in ("config.toml", "manifest.json", "jobs.json"):
        (workdir / name).write_bytes(queue.read(name))
    jobs = json.loads((workdir / "jobs.json").read_text())
    too_wide = [j["id"] for j in jobs if j["api_heavy"] and j.get("api_weight", 1) > api_slots]
    if too_wide:
        raise ValueError(f"Jobs need more than {api_slots} API slots: {', '.join(too_wide)}")
    if settle is None:
        settings = Settings.load(workdir / "config.toml")

        def settle(output: Path) -> list[str]:
            return quiesce(settings, output)

    host = socket.gethostname()
    completed: list[str] = []
    while True:
        claimed = queue.names("claims")
        pending = [j for j in jobs if j["id"] not in claimed]
        if not pending:
            return completed
        started = False
        reserved = False
        for job in pending:
            owner = {"host": host, "job": job["id"], "at": now()}
            slots: list[str] = []
            if job["api_heavy"]:
                if reserved:
                    continue
                acquired = queue.acquire_slots(api_slots, job.get("api_weight", 1), owner)
                if acquired is None:
                    # Let slots drain for this job instead of starving it with smaller ones.
                    reserved = True
                    continue
                slots = acquired
            try:
                if not queue.put_if_absent(queue.key("claims", job["id"]), owner):
                    continue
                start = now()
                code = runner(job, workdir)
                try:
                    settle_errors = settle(workdir / "results" / job["id"]) if code else []
                except Exception as exc:
                    settle_errors = [f"Could not quiesce the job: {type(exc).__name__}: {exc}"]
                queue.upload_tree(workdir / "results" / job["id"], "results", job["id"])
                log = workdir / "logs" / f"{job['id']}.log"
                if log.exists():
                    queue.s3.upload_file(str(log), queue.bucket, queue.key("logs", log.name))
                queue.s3.put_object(
                    Bucket=queue.bucket,
                    Key=queue.key("done", job["id"]),
                    Body=json.dumps(
                        {
                            "exit_code": code,
                            "host": host,
                            "start": start,
                            "end": now(),
                            "quiesce_errors": settle_errors,
                        }
                    ).encode(),
                )
                completed.append(job["id"])
                if settle_errors:
                    # Unconfirmed queries could overlap later trials on this host.
                    return completed
                started = True
                break
            finally:
                for slot in slots:
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
    if not queue.exists("queue.json"):
        raise ValueError("Queue is missing or incompletely published")
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
