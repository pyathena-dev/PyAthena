# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Fresh-process trials, external resource sampling, and incremental evidence."""

from __future__ import annotations

import asyncio
import hashlib
import importlib.metadata
import json
import logging
import multiprocessing
import os
import platform
import resource
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

import psutil

from pyathena.util import parse_output_location
from pyathena_bench.adapters import BACKEND_VERSIONS, Adapter, run_adapter
from pyathena_bench.aws import (
    cancel_queries,
    client,
    run_locations,
    session,
    split_location,
    validate_inputs,
    validate_manifest,
    validate_objects,
)
from pyathena_bench.cases import Case
from pyathena_bench.config import Settings, select_sql, write_json
from pyathena_bench.filesystem import missing_api_reason, run_filesystem
from pyathena_bench.report import CHECKPOINT, read_trials, trial_key

_logger = logging.getLogger(__name__)


class Observer:
    """Observe SDK events for all libraries, including failed and chunked reads."""

    def __init__(self, pipe: Connection) -> None:
        self.pipe = pipe
        self.lock = threading.Lock()
        self.completed: dict[str, dict[str, Any]] = {}

    def send(self, event: dict[str, Any]) -> None:
        with self.lock:
            self.pipe.send(event)

    def before_start(self, params: dict[str, Any], context: dict[str, Any], **kwargs: Any) -> None:
        context["benchmark_request"] = {
            "sql": params["QueryString"],
            "output": params.get("ResultConfiguration", {}).get("OutputLocation"),
        }

    def started(self, parsed: dict[str, Any], context: dict[str, Any], **kwargs: Any) -> None:
        if "QueryExecutionId" in parsed:
            self.send(
                {
                    "event": "query",
                    "query_id": parsed["QueryExecutionId"],
                    **context.get("benchmark_request", {}),
                }
            )

    def polled(self, parsed: dict[str, Any], **kwargs: Any) -> None:
        query = parsed.get("QueryExecution", {})
        query_id = query.get("QueryExecutionId")
        if query_id and query.get("Status", {}).get("State") in {
            "SUCCEEDED",
            "FAILED",
            "CANCELLED",
        }:
            with self.lock:
                if query_id in self.completed:
                    return
                item = {
                    "event": "athena",
                    "query_id": query_id,
                    "observed_complete_at": time.perf_counter(),
                    "state": query["Status"]["State"],
                    "statistics": query.get("Statistics", {}),
                    "engine": query.get("EngineVersion", {}),
                }
                self.completed[query_id] = item
                self.pipe.send(item)

    def register(self, session_: Any) -> None:
        session_.events.register(
            "before-parameter-build.athena.StartQueryExecution", self.before_start
        )
        session_.events.register("after-call.athena.StartQueryExecution", self.started)
        session_.events.register("after-call.athena.GetQueryExecution", self.polled)


def worker(pipe: Connection, payload: dict[str, Any]) -> None:
    observer = Observer(pipe)
    try:
        settings = Settings(**payload["settings"])
        case = Case(**payload["case"])
        session_ = session(settings)
        observer.register(session_)
        if case.suite == "filesystem":
            measurement = run_filesystem(
                settings,
                case,
                session_,
                payload["source"],
                payload["destination"],
                payload["size"],
                Path(payload["temp_dir"]),
            )
        else:
            adapter = Adapter(
                settings,
                case,
                session_,
                payload["database"],
                payload["output"],
                payload["temp_table"],
            )
            measurement = run_adapter(
                adapter, payload["sql"], payload["expected_rows"], payload.get("fixture")
            )
        observer.send({"event": "ready"})
        pipe.recv()
        result = asyncio.run(measurement)
        result["athena"] = list(observer.completed.values())
        for query in result["queries"]:
            if query.get("query_id") is None and len(observer.completed) == 1:
                query["query_id"] = next(iter(observer.completed))
            metadata = observer.completed.get(query.get("query_id"))
            if metadata and "ready_at" in query:
                query["post_completion_setup_seconds"] = (
                    query.pop("ready_at") - metadata["observed_complete_at"]
                )
            else:
                query.pop("ready_at", None)
        # Linux reports KiB; macOS reports bytes. This includes process startup.
        result["process_high_water_rss_bytes"] = int(
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            * (1 if sys.platform == "darwin" else 1024)
        )
        observer.send({"event": "result", "result": result})
    except BaseException as exc:
        observer.send(
            {
                "event": "result",
                "result": {"status": "error", "error": f"{type(exc).__name__}: {exc}"},
            }
        )
    finally:
        pipe.close()


def append_json(path: Path, value: Any) -> None:
    with path.open("a") as stream:
        stream.write(json.dumps(value, sort_keys=True) + "\n")
        stream.flush()


def thread_snapshot(process: psutil.Process) -> tuple[int, dict[int, float] | None]:
    count = process.num_threads()
    try:
        return count, {t.id: t.user_time + t.system_time for t in process.threads()}
    except psutil.AccessDenied:
        # macOS can deny task_for_pid even for our own child; RSS remains available.
        return count, None


def directory_bytes(path: Path) -> int:
    """Return the total size of the regular files below a directory.

    Args:
        path: Directory to measure; files removed during the walk are skipped.

    Returns:
        Total size in bytes.
    """
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += (Path(root) / name).stat().st_size
            except FileNotFoundError:
                continue
    return total


def start_with_temp_dir(process: Any, temp_dir: Path) -> None:
    """Start a spawned trial process with its own temporary directory.

    Polars before 1.39.0 downloads cloud objects for lazy CSV scans into a
    file cache under ``POLARS_TEMP_DIR`` and keeps the files after the process
    exits. A per-trial directory lets the parent measure and remove any
    storage Polars writes there, and the local file of the filesystem suite,
    which the child receives in its payload as ``temp_dir``.

    Args:
        process: Unstarted multiprocessing process; it inherits the environment.
        temp_dir: Directory to expose as ``POLARS_TEMP_DIR`` to the child.
    """
    previous = os.environ.get("POLARS_TEMP_DIR")
    os.environ["POLARS_TEMP_DIR"] = str(temp_dir)
    try:
        process.start()
    finally:
        if previous is None:
            os.environ.pop("POLARS_TEMP_DIR", None)
        else:
            os.environ["POLARS_TEMP_DIR"] = previous


def supervise(
    payload: dict[str, Any], events: Path, settings: Settings, target: Any = worker
) -> dict[str, Any]:
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    temp_dir = Path(tempfile.mkdtemp(prefix="pyathena-bench-trial-"))
    process = context.Process(target=target, args=(child, {**payload, "temp_dir": str(temp_dir)}))
    temp_peak = 0
    start_with_temp_dir(process, temp_dir)
    child.close()
    watched = psutil.Process(process.pid)
    deadline = time.monotonic() + settings.timeout_seconds
    query_ids: set[str] = set()
    result: dict[str, Any] | None = None
    baseline = None
    peak = 0
    max_threads = 0
    thread_cpu: dict[int, float] = {}
    thread_baseline: dict[int, float] = {}
    thread_cpu_available = True
    samples: list[dict[str, Any]] = []
    interrupted = False
    eof = False
    try:
        while process.is_alive() or (not eof and parent.poll()):
            while not eof and parent.poll():
                try:
                    event = parent.recv()
                except EOFError:
                    eof = True
                    break
                append_json(events, {"trial": payload["trial"], **event})
                if event["event"] == "ready":
                    baseline = watched.memory_info().rss
                    _, initial_threads = thread_snapshot(watched)
                    thread_baseline = initial_threads or {}
                    thread_cpu_available = initial_threads is not None
                    peak = baseline
                    parent.send("go")
                elif event["event"] == "query":
                    query_ids.add(event["query_id"])
                elif event["event"] == "result":
                    result = event["result"]
            if baseline is not None and process.is_alive():
                try:
                    rss = watched.memory_info().rss
                    thread_count, threads = thread_snapshot(watched)
                    thread_cpu_available = thread_cpu_available and threads is not None
                    peak = max(peak, rss)
                    max_threads = max(max_threads, thread_count)
                    for thread_id, cpu in (threads or {}).items():
                        thread_cpu[thread_id] = max(
                            thread_cpu.get(thread_id, 0), cpu - thread_baseline.get(thread_id, 0)
                        )
                    temp_peak = max(temp_peak, directory_bytes(temp_dir))
                    samples.append({"at": time.time(), "rss_bytes": rss, "threads": thread_count})
                    if rss > psutil.virtual_memory().total * settings.memory_fraction:
                        result = {
                            "status": "memory_limit",
                            "error": "Configured RSS limit exceeded",
                        }
                        break
                except psutil.NoSuchProcess:
                    pass
            if time.monotonic() >= deadline:
                result = {"status": "timeout"}
                break
            if result is not None:
                break
            time.sleep(settings.sample_interval)
    except KeyboardInterrupt:
        interrupted = True
        result = {"status": "interrupted"}
    except Exception as exc:
        result = {"status": "monitor_error", "error": f"{type(exc).__name__}: {exc}"}
    finally:
        if process.is_alive():
            if result is not None and result.get("status") == "ok":
                process.join(timeout=5)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            if process.is_alive():
                process.kill()
        process.join(timeout=5)
        parent.close()
        temp_peak = max(temp_peak, directory_bytes(temp_dir))
        shutil.rmtree(temp_dir, ignore_errors=True)
    if result is None:
        result = {"status": "worker_exit", "exit_code": process.exitcode}
    if result["status"] != "ok" and query_ids:
        result["cancellation_errors"] = cancel_queries(settings, query_ids)
    result.update(
        rss_baseline_bytes=baseline,
        rss_peak_bytes=peak,
        rss_increase_bytes=peak - baseline if baseline is not None else None,
        max_threads=max_threads,
        temp_dir_peak_bytes=temp_peak,
        thread_cpu_seconds=thread_cpu,
        thread_cpu_available=thread_cpu_available,
        resource_samples=samples,
        query_ids=sorted(query_ids),
        interrupted=interrupted,
    )
    return result


def output_files(s3: Any, location: str) -> dict[str, int]:
    """Summarize the data files that a trial's UNLOAD or CTAS queries wrote.

    Athena writes a ``<query_id>-manifest.csv`` that lists the data files of
    each UNLOAD or CTAS query below the query's output location, for PyAthena
    and Wrangler alike, so the manifests below the trial's location name the
    files. Sizes come from the listing of that location, or from
    ``HeadObject`` for a file outside it.

    Args:
        s3: S3 client.
        location: The trial's ``s3://`` output prefix.

    Returns:
        The number of files and their total, largest, and smallest sizes in bytes.

    Raises:
        ValueError: If no manifest is below the location.
    """
    bucket, prefix = parse_output_location(location)
    listed: dict[str, int] = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        listed.update((item["Key"], item["Size"]) for item in page.get("Contents", []))
    manifests = [key for key in listed if key.endswith("-manifest.csv")]
    if not manifests:
        raise ValueError(f"No query manifest below {location}")
    sizes = []
    for manifest in manifests:
        body: str = s3.get_object(Bucket=bucket, Key=manifest)["Body"].read().decode()
        for line in filter(None, body.splitlines()):
            file_bucket, key = parse_output_location(line)
            size = listed.get(key) if file_bucket == bucket else None
            if size is None:
                size = s3.head_object(Bucket=file_bucket, Key=key)["ContentLength"]
            sizes.append(size)
    return {
        "count": len(sizes),
        "bytes": sum(sizes),
        "max_bytes": max(sizes, default=0),
        "min_bytes": min(sizes, default=0),
    }


def environment(settings: Settings) -> dict[str, Any]:
    root = Path(__file__).resolve().parents[2]
    git = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, check=True
    )
    status = subprocess.run(
        ["git", "status", "--porcelain"], cwd=root, capture_output=True, text=True, check=True
    )
    return {
        "git_sha": git.stdout.strip(),
        "dirty": bool(status.stdout),
        "python": sys.version,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "backends_imported_before_baseline": BACKEND_VERSIONS,
        "cpu_count": psutil.cpu_count(),
        "memory_bytes": psutil.virtual_memory().total,
        "dependencies": {d.metadata["Name"]: d.version for d in importlib.metadata.distributions()},
        "lock_sha256": hashlib.sha256((root / "uv.lock").read_bytes()).hexdigest(),
        "settings": asdict(settings),
    }


def completed_trials(directories: list[Path]) -> set[tuple[str, str, int | None]]:
    """Collect the trials that earlier attempts of a job recorded as finished.

    Args:
        directories: Output directories of earlier attempts.

    Returns:
        ``(scale, case_id, repetition)`` for successful measured trials, and
        ``(scale, case_id, None)`` for cases recorded as unsupported. Warmups
        are not included: each attempt runs its own before its measured trials.
    """
    return {
        trial_key(trial)
        for directory in directories
        for trial in read_trials(directory)
        if (trial["status"] == "ok" and not trial.get("warmup")) or trial["status"] == "unsupported"
    }


def s3_checkpoint(s3: Any, location: str) -> Callable[[str, dict[str, Any]], None]:
    """Return a function that writes a fleet attempt's per-trial records to S3.

    Args:
        s3: S3 client.
        location: The attempt's ``s3://`` prefix; records go below ``checkpoint/``.

    Returns:
        A function that writes a record under a name and raises the error of a
        failed write.
    """
    bucket, prefix = split_location(location)

    def write(name: str, record: dict[str, Any]) -> None:
        s3.put_object(
            Bucket=bucket,
            Key=f"{prefix}{CHECKPOINT}/{name}",
            Body=json.dumps(record, sort_keys=True).encode(),
        )

    return write


def run(
    settings: Settings,
    manifest: dict[str, Any],
    cases: list[Case],
    scales: list[str],
    output: Path,
    completed: set[tuple[str, str, int | None]] | None = None,
    checkpoint: Callable[[str, dict[str, Any]], None] | None = None,
) -> bool:
    """Run every trial of the selected cases into a new output directory.

    Args:
        settings: Configuration.
        manifest: Prepared manifest.
        cases: Cases to run.
        scales: Scales to run.
        output: New output directory.
        completed: Trials from :func:`completed_trials` to skip, so that a
            resumed job runs only the missing measured repetitions, after its
            own warmups; a case without missing repetitions is skipped.
        checkpoint: Function from :func:`s3_checkpoint` that receives each
            trial's ``trial_start`` event before the trial and its record
            after it, between trials, so that a fleet worker can resume the
            job if the host is lost. A trial whose start could not be written
            does not run and is recorded as failed, because a later attempt
            could not stop its queries; a record that could not be written is
            only logged, and a resumed job repeats that trial.

    Returns:
        True if every trial succeeded.
    """
    completed = completed or set()
    resources = validate_manifest(settings, manifest)
    if any(case.suite == "filesystem" for case in cases):
        validate_objects(settings, manifest, scales)
    if any(case.suite != "filesystem" for case in cases):
        validate_inputs(settings, manifest, scales)
    output.mkdir(parents=True, exist_ok=False)
    write_json(output / "environment.json", environment(settings))
    write_json(output / "manifest.json", manifest)
    success = True
    s3 = client(session(settings), "s3")
    locations = run_locations(resources, manifest["run_id"])
    for scale in scales:
        for case in cases:
            unsupported = case.unsupported_reason(settings, scale) or missing_api_reason(case)
            if unsupported:
                if (scale, case.id, None) in completed:
                    continue
                append_json(
                    output / "trials.jsonl",
                    {
                        "case": asdict(case),
                        "case_id": case.id,
                        "scale": scale,
                        "status": "unsupported",
                        "reason": unsupported,
                    },
                )
                continue
            if all(
                (scale, case.id, repetition) in completed
                for repetition in range(settings.repetitions)
            ):
                continue
            for repetition in range(-settings.warmups, settings.repetitions):
                # Warmups run again on this host; measured trials are not repeated.
                if repetition >= 0 and (scale, case.id, repetition) in completed:
                    continue
                trial = uuid.uuid4().hex
                payload: dict[str, Any] = {
                    "settings": asdict(settings),
                    "case": asdict(case),
                    "trial": trial,
                    "case_id": case.id,
                    "scale": scale,
                    "repetition": repetition,
                    "warmup": repetition < 0,
                }
                if case.suite == "filesystem":
                    # Written to the scratch bucket, from which the source is read or copied.
                    payload.update(
                        source=manifest["objects"][scale]["location"],
                        destination=f"{locations['scratch']}trials/{trial}/object",
                        size=manifest["objects"][scale]["size"],
                    )
                else:
                    payload.update(
                        database=resources["ScratchDatabase"],
                        output=f"{locations['results']}trials/{trial}/",
                        temp_table=f"b_{manifest['run_id']}_{trial}",
                        sql=select_sql(
                            resources["ScratchDatabase"],
                            manifest["scales"][scale]["table"],
                            case.shape,
                            case.transport,
                        ),
                        expected_rows=manifest["scales"][scale]["rows"],
                        fixture=manifest["fixtures"].get(f"{scale}-{case.shape}-{case.transport}"),
                    )
                start = {"event": "trial_start", **payload}
                result: dict[str, Any]
                try:
                    if checkpoint:
                        checkpoint(f"{trial}-start.json", start)
                except Exception as exc:
                    result = {
                        "status": "error",
                        "error": f"Could not write the start checkpoint: "
                        f"{type(exc).__name__}: {exc}",
                    }
                else:
                    append_json(output / "events.jsonl", start)
                    result = supervise(payload, output / "events.jsonl", settings)
                if (
                    result["status"] == "ok"
                    and repetition >= 0
                    and case.transport in {"unload", "ctas"}
                    and case.suite != "init"
                ):
                    try:
                        result["output_files"] = output_files(s3, payload["output"])
                    except Exception as exc:
                        # The measurement stands; only the file summary is missing.
                        result["output_files_error"] = f"{type(exc).__name__}: {exc}"
                record = {
                    "trial": trial,
                    "case_id": case.id,
                    "case": asdict(case),
                    "scale": scale,
                    "repetition": repetition,
                    "warmup": repetition < 0,
                    **result,
                }
                append_json(output / "trials.jsonl", record)
                if checkpoint:
                    try:
                        checkpoint(f"{trial}.json", record)
                    except Exception:
                        _logger.warning(
                            f"Could not write the checkpoint {trial}.json", exc_info=True
                        )
                success = success and result["status"] == "ok"
                # A failed worker can leave an unobserved query running in Athena.
                # Do not let it contaminate subsequent trials before cleanup.
                if result["status"] != "ok":
                    return False
    return success
