# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Reports keep unsupported cases and failures visible."""

from __future__ import annotations

import csv
import json
import statistics
import warnings
from collections import defaultdict
from pathlib import Path
from typing import Any


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def summarize(trials: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for trial in trials:
        groups[(trial["scale"], trial["case_id"])].append(trial)
    rows = []
    for (scale, case_id), group in sorted(groups.items()):
        measured = [t for t in group if not t.get("warmup")]
        good = [t for t in measured if t["status"] == "ok"]
        latency = [q["total_seconds"] for t in good for q in t.get("queries", [])]
        init = [
            q["init_seconds"] for t in good for q in t.get("queries", []) if "init_seconds" in q
        ]
        retrieval = [
            q["post_completion_setup_seconds"] + q["consume_seconds"]
            for t in good
            for q in t.get("queries", [])
            if "post_completion_setup_seconds" in q and "consume_seconds" in q
        ]
        throughput = [
            q["bytes"] / 2**20 / q["total_seconds"]
            for t in good
            for q in t.get("queries", [])
            if "bytes" in q
        ]
        lag = [v for t in good for v in (t.get("loop_lag_seconds") or [])]
        phases = {}
        for key in ("execute_seconds", "consume_seconds", "post_completion_setup_seconds"):
            phases[f"median_{key}"] = percentile(
                [q[key] for t in good for q in t.get("queries", []) if key in q], 0.5
            )
        for key in (
            "EngineExecutionTimeInMillis",
            "QueryQueueTimeInMillis",
            "TotalExecutionTimeInMillis",
            "DataScannedInBytes",
        ):
            phases[f"median_athena_{key}"] = percentile(
                [
                    q["statistics"][key]
                    for t in good
                    for q in t.get("athena", [])
                    if key in q.get("statistics", {})
                ],
                0.5,
            )
        reasons = set()
        for trial in group:
            if trial["status"] == "ok":
                if "output_files_error" in trial and not trial.get("warmup"):
                    # The timing stands, but the file columns miss this trial.
                    reasons.add(f"Output files not listed: {trial['output_files_error']}")
                continue
            prefix = "Warmup: " if trial.get("warmup") else ""
            if trial.get("superseded"):
                prefix = "Superseded: "
            failures = [q for q in trial.get("queries", []) if q["status"] != "ok"]
            if failures:
                for query in failures:
                    reason = query.get("error", query["status"])
                    if query["status"] == "row_count_mismatch":
                        reason += f": expected {query['expected_rows']}, got {query['rows']}"
                    elif query["status"] == "size_mismatch":
                        reason += f": expected {query['expected_bytes']} B, got {query['bytes']} B"
                    reasons.add(prefix + reason)
            else:
                reasons.add(prefix + trial.get("reason", trial.get("error", trial["status"])))
            reasons.update(prefix + error for error in trial.get("cancellation_errors", []))
        rows.append(
            {
                "scale": scale,
                "case": case_id,
                "successful_trials": len(good),
                "failed_trials": sum(
                    t["status"] not in {"ok", "unsupported"} and not t.get("superseded")
                    for t in measured
                ),
                "failed_warmups": sum(
                    t.get("warmup", False) and t["status"] != "ok" for t in group
                ),
                "unsupported": any(t["status"] == "unsupported" for t in measured),
                "median_seconds": percentile(latency, 0.5),
                "p95_seconds": percentile(latency, 0.95),
                "median_init_seconds": percentile(init, 0.5),
                "median_client_result_seconds": percentile(retrieval, 0.5),
                "median_mib_per_second": percentile(throughput, 0.5),
                **phases,
                "median_queries_per_second": statistics.median(
                    [t["successful_queries_per_second"] for t in good]
                )
                if good
                else None,
                "peak_rss_bytes": max((t["rss_peak_bytes"] for t in good), default=None),
                "median_rss_increase_bytes": percentile(
                    [
                        t["rss_increase_bytes"]
                        for t in good
                        if t.get("rss_increase_bytes") is not None
                    ],
                    0.5,
                ),
                "peak_temp_dir_bytes": max(
                    (
                        t["temp_dir_peak_bytes"]
                        for t in good
                        if t.get("temp_dir_peak_bytes") is not None
                    ),
                    default=None,
                ),
                "loop_lag_p95_seconds": percentile(lag, 0.95),
                "loop_lag_max_seconds": max(lag, default=None),
                "peak_threads": max((t["max_threads"] for t in good), default=None),
                "max_output_files": max(
                    (t["output_files"]["count"] for t in good if "output_files" in t),
                    default=None,
                ),
                "max_output_file_bytes": max(
                    (t["output_files"]["max_bytes"] for t in good if "output_files" in t),
                    default=None,
                ),
                "notes": "; ".join(sorted(reasons)),
            }
        )
    return rows


def read_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    text = path.read_text()
    lines = text.splitlines()
    records = []
    for index, line in enumerate(lines):
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            if index != len(lines) - 1 or text.endswith("\n"):
                raise
            warnings.warn(f"Ignoring an incomplete final record in {path}", stacklevel=2)
    return records


# A fleet attempt's per-trial records in S3, written between trials; see runner.run().
CHECKPOINT = "checkpoint"


def trial_key(trial: dict[str, Any]) -> tuple[str, str, int | None]:
    """Return the scale, case, and repetition that identify a trial across attempts.

    Args:
        trial: Trial record or ``trial_start`` event.

    Returns:
        The key; the repetition is None for a case recorded as unsupported.
    """
    return (trial["scale"], trial["case_id"], trial.get("repetition"))


def read_trials(directory: Path) -> list[dict[str, Any]]:
    """Read the trial records of one output directory, including its checkpoint.

    Args:
        directory: Output directory of a run or of a fleet attempt.

    Returns:
        Records from ``trials.jsonl`` and checkpoint records missing from it.
    """
    trials = read_records(directory / "trials.jsonl")
    recorded = {trial.get("trial") for trial in trials}
    for path in sorted((directory / CHECKPOINT).glob("*.json")):
        if not path.name.endswith("-start.json") and path.stem not in recorded:
            trials.append(json.loads(path.read_text()))
    return trials


def read_trial_starts(directory: Path) -> list[dict[str, Any]]:
    """Read the ``trial_start`` events of one output directory, including its checkpoint.

    Args:
        directory: Output directory of a run or of a fleet attempt.

    Returns:
        Events from ``events.jsonl`` and checkpoint events missing from it.
    """
    starts = [e for e in read_records(directory / "events.jsonl") if e["event"] == "trial_start"]
    started = {event.get("trial") for event in starts}
    starts.extend(
        json.loads(path.read_text())
        for path in sorted((directory / CHECKPOINT).glob("*-start.json"))
        if path.name.removesuffix("-start.json") not in started
    )
    return starts


def collect_trials(directory: Path) -> list[dict[str, Any]]:
    """Read the trials of an output directory and of its attempt subdirectories.

    A fleet job keeps each attempt in an ``attempt-<n>`` subdirectory; their
    trials are merged. A started trial without a final record is reported as
    incomplete. A failed or incomplete trial whose measured repetition another
    attempt recorded as successful is marked ``superseded``: it is noted but
    not counted as failed, so a resumed job counts the trials of an
    uninterrupted run. A repetition that two attempts recorded as successful
    counts once.

    Args:
        directory: Output directory of a run, or a job directory with attempts.

    Returns:
        Trial records.
    """
    sources = [directory, *sorted(p for p in directory.glob("attempt-*") if p.is_dir())]
    trials = [t for source in sources for t in read_trials(source)]
    recorded = {trial.get("trial") for trial in trials}
    trials.extend(
        {**event, "status": "incomplete", "error": "Trial started without a final record"}
        for source in sources
        for event in read_trial_starts(source)
        if event["trial"] not in recorded
    )
    succeeded = {trial_key(t) for t in trials if t["status"] == "ok" and not t.get("warmup")}
    merged = []
    counted: set[tuple[str, str, int | None]] = set()
    for trial in trials:
        if trial["status"] == "ok" and not trial.get("warmup"):
            if len(sources) > 1 and trial_key(trial) in counted:
                continue
            counted.add(trial_key(trial))
        elif trial["status"] != "unsupported" and not trial.get("warmup"):
            if trial_key(trial) in succeeded:
                trial = {**trial, "superseded": True}
        merged.append(trial)
    return merged


def report(directory: Path) -> None:
    trials = collect_trials(directory)
    rows = summarize(trials)
    if not rows:
        raise ValueError("No trials to report")
    with (directory / "summary.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    lines = [
        "# Cursor benchmark measurements",
        "",
        "Only successful measured trials contribute to timing summaries; warmups are excluded.",
        "Compare cases with the same scale, shape, output representation, and transport.",
        "Initialization timings include I/O; RSS covers construction and validation.",
        "Client result time starts at observed Athena completion and excludes polling latency.",
        "Total time includes Athena execution and polling; init time excludes validation.",
        "Thread counts include SDK and native library threads, not just executor workers.",
        "Peak temp dir is the per-trial temporary directory, outside the RSS: Polars' file cache,",
        "and the local file of the filesystem suite's get and put.",
        "Output files: the most Parquet files an UNLOAD or CTAS trial wrote, and the largest.",
        "Throughput: the filesystem suite's object size per total time, in MiB per second.",
        "",
        "| Scale | Case | OK | Fail | Warmup fail | Unsupported | Total (s) | "
        "Client result (s) | Init (s) | Throughput (MiB/s) | Peak RSS (B) | RSS increase (B) | "
        "Peak temp dir (B) | Output files | Largest file (B) | Peak threads | Notes |",
        "| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: "
        "| ---: | ---: | ---: | --- |",
    ]
    for row in rows:
        values = [
            row[k]
            for k in (
                "scale",
                "case",
                "successful_trials",
                "failed_trials",
                "failed_warmups",
                "unsupported",
                "median_seconds",
                "median_client_result_seconds",
                "median_init_seconds",
                "median_mib_per_second",
                "peak_rss_bytes",
                "median_rss_increase_bytes",
                "peak_temp_dir_bytes",
                "max_output_files",
                "max_output_file_bytes",
                "peak_threads",
                "notes",
            )
        ]
        lines.append(
            "| "
            + " | ".join(
                str(v).replace("|", "\\|").replace("\n", " ") if v is not None else "N/A"
                for v in values
            )
            + " |"
        )
    (directory / "summary.md").write_text("\n".join(lines) + "\n")
