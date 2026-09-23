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
import multiprocessing
import platform
import resource
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import asdict
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

import psutil

from pyathena_bench.adapters import BACKEND_VERSIONS, Adapter, run_adapter
from pyathena_bench.aws import cancel_queries, session, validate_inputs, validate_manifest
from pyathena_bench.cases import Case
from pyathena_bench.config import Settings, select_sql, write_json


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
        adapter = Adapter(
            settings, case, session_, payload["database"], payload["output"], payload["temp_table"]
        )
        observer.send({"event": "ready"})
        pipe.recv()
        result = asyncio.run(
            run_adapter(adapter, payload["sql"], payload["expected_rows"], payload.get("fixture"))
        )
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


def supervise(
    payload: dict[str, Any], events: Path, settings: Settings, target: Any = worker
) -> dict[str, Any]:
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(target=target, args=(child, payload))
    process.start()
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
    if result is None:
        result = {"status": "worker_exit", "exit_code": process.exitcode}
    if result["status"] != "ok" and query_ids:
        result["cancellation_errors"] = cancel_queries(settings, query_ids)
    result.update(
        rss_baseline_bytes=baseline,
        rss_peak_bytes=peak,
        rss_increase_bytes=peak - baseline if baseline is not None else None,
        max_threads=max_threads,
        thread_cpu_seconds=thread_cpu,
        thread_cpu_available=thread_cpu_available,
        resource_samples=samples,
        query_ids=sorted(query_ids),
        interrupted=interrupted,
    )
    return result


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
        "lock_sha256": hashlib.sha256((root / "benchmarks/uv.lock").read_bytes()).hexdigest(),
        "settings": asdict(settings),
    }


def run(
    settings: Settings, manifest: dict[str, Any], cases: list[Case], scales: list[str], output: Path
) -> bool:
    resources = validate_manifest(settings, manifest)
    validate_inputs(settings, manifest, scales)
    output.mkdir(parents=True, exist_ok=False)
    write_json(output / "environment.json", environment(settings))
    write_json(output / "manifest.json", manifest)
    success = True
    for scale in scales:
        for case in cases:
            if case.unsupported:
                append_json(
                    output / "trials.jsonl",
                    {
                        "case": asdict(case),
                        "case_id": case.id,
                        "scale": scale,
                        "status": "unsupported",
                        "reason": case.unsupported,
                    },
                )
                continue
            for repetition in range(-settings.warmups, settings.repetitions):
                trial = uuid.uuid4().hex
                prefix = f"runs/{manifest['run_id']}/trials/{trial}/"
                payload = {
                    "settings": asdict(settings),
                    "case": asdict(case),
                    "trial": trial,
                    "case_id": case.id,
                    "scale": scale,
                    "repetition": repetition,
                    "warmup": repetition < 0,
                    "database": resources["ScratchDatabase"],
                    "output": f"s3://{resources['Bucket']}/{prefix}",
                    "temp_table": f"b_{manifest['run_id']}_{trial}",
                    "sql": select_sql(
                        resources["ScratchDatabase"], manifest["scales"][scale]["table"], case.shape
                    ),
                    "expected_rows": manifest["scales"][scale]["rows"],
                    "fixture": manifest["fixtures"].get(f"{scale}-{case.shape}-{case.transport}"),
                }
                append_json(output / "events.jsonl", {"event": "trial_start", **payload})
                result = supervise(payload, output / "events.jsonl", settings)
                append_json(
                    output / "trials.jsonl",
                    {
                        "trial": trial,
                        "case_id": case.id,
                        "case": asdict(case),
                        "scale": scale,
                        "repetition": repetition,
                        "warmup": repetition < 0,
                        **result,
                    },
                )
                success = success and result["status"] == "ok"
                # A failed worker can leave an unobserved query running in Athena.
                # Do not let it contaminate subsequent trials before cleanup.
                if result["status"] != "ok":
                    return False
    return success
