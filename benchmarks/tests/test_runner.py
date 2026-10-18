# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import json
import os
import time
from dataclasses import replace
from io import BytesIO
from pathlib import Path

import pytest
from pyathena_bench.cases import Case
from pyathena_bench.config import Settings
from pyathena_bench.report import read_records
from pyathena_bench.runner import Observer, completed_trials, output_files, run, supervise


def successful_worker(pipe, payload):
    pipe.send({"event": "ready"})
    pipe.recv()
    time.sleep(0.05)
    pipe.send({"event": "result", "result": {"status": "ok"}})
    pipe.close()


def waiting_worker(pipe, payload):
    time.sleep(30)


def cache_writing_worker(pipe, payload):
    pipe.send({"event": "ready"})
    pipe.recv()
    cache = Path(os.environ["POLARS_TEMP_DIR"]) / "file-cache"
    cache.mkdir(parents=True)
    (cache / "object").write_bytes(b"x" * 4096)
    time.sleep(0.2)
    result = {"status": "ok", "temp_dir": os.environ["POLARS_TEMP_DIR"]}
    pipe.send({"event": "result", "result": result})
    pipe.close()


RUN_MANIFEST = {
    "run_id": "a" * 32,
    "scales": {"small": {"table": "input", "rows": 10}},
    "fixtures": {},
}


@pytest.fixture
def run_stubs(monkeypatch):
    """Replace the AWS calls of ``run()`` and return the S3 client it creates."""
    s3 = object()
    monkeypatch.setattr(
        "pyathena_bench.runner.validate_manifest",
        lambda *args: {"ScratchDatabase": "scratch", "Bucket": "out", "Results": "s3://out/"},
    )
    monkeypatch.setattr("pyathena_bench.runner.validate_inputs", lambda *args: None)
    monkeypatch.setattr("pyathena_bench.runner.environment", lambda *args: {})
    monkeypatch.setattr("pyathena_bench.runner.session", lambda settings: None)
    monkeypatch.setattr("pyathena_bench.runner.client", lambda session_, service: s3)
    return s3


def test_observer_keeps_first_completion_and_query_id():
    events = []

    class Pipe:
        def send(self, value):
            events.append(value)

    observer = Observer(Pipe())
    context = {}
    observer.before_start(
        {"QueryString": "SELECT 1", "ResultConfiguration": {"OutputLocation": "s3://out/"}}, context
    )
    observer.started({"QueryExecutionId": "q"}, context)
    response = {
        "QueryExecution": {
            "QueryExecutionId": "q",
            "Status": {"State": "SUCCEEDED"},
            "Statistics": {"DataScannedInBytes": 123},
        }
    }
    observer.polled(response)
    observer.polled(response)
    assert [e["event"] for e in events] == ["query", "athena"]
    assert events[0]["output"] == "s3://out/"
    assert events[1]["statistics"]["DataScannedInBytes"] == 123


class TestRunner:
    def test_trial_uses_child_process_and_external_memory_samples(self, tmp_path):
        result = supervise(
            {"trial": "test"},
            tmp_path / "events.jsonl",
            replace(Settings(), timeout_seconds=20),
            successful_worker,
        )
        assert result["status"] == "ok"
        assert result["rss_peak_bytes"] >= result["rss_baseline_bytes"] > 0
        assert result["resource_samples"]
        events = [json.loads(line) for line in (tmp_path / "events.jsonl").read_text().splitlines()]
        assert events[0]["event"] == "ready"

    def test_each_trial_gets_a_measured_and_removed_polars_temp_dir(self, tmp_path, monkeypatch):
        monkeypatch.setenv("POLARS_TEMP_DIR", "/parent/value")
        result = supervise(
            {"trial": "test"},
            tmp_path / "events.jsonl",
            replace(Settings(), timeout_seconds=20),
            cache_writing_worker,
        )
        assert result["status"] == "ok"
        assert result["temp_dir_peak_bytes"] >= 4096
        assert result["temp_dir"] != "/parent/value"
        assert not Path(result["temp_dir"]).exists()
        assert os.environ["POLARS_TEMP_DIR"] == "/parent/value"

    def test_timeout_does_not_become_a_successful_timing(self, tmp_path):
        result = supervise(
            {"trial": "test"},
            tmp_path / "events.jsonl",
            replace(Settings(), timeout_seconds=0.1),
            waiting_worker,
        )
        assert result["status"] == "timeout"

    @pytest.mark.parametrize("status", ["timeout", "memory_limit", "worker_exit", "error"])
    def test_failed_trial_stops_suite_before_any_more_queries(
        self, monkeypatch, tmp_path, run_stubs, status
    ):
        calls = []

        def fail(payload, events, settings):
            calls.append(payload)
            return {"status": status}

        monkeypatch.setattr("pyathena_bench.runner.supervise", fail)
        assert not run(
            Settings(), RUN_MANIFEST, [Case("cursor"), Case("dict")], ["small"], tmp_path / "run"
        )
        assert len(calls) == 1
        assert calls[0]["warmup"]
        result = json.loads((tmp_path / "run/trials.jsonl").read_text())
        assert result["status"] == status


class FakeS3:
    """An S3 client with a fixed paginated listing and object bodies."""

    def __init__(self, pages, bodies=None, heads=None):
        self.pages = pages
        self.bodies = bodies or {}
        self.heads = heads or {}
        self.requests = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, **kwargs):
        self.requests.append(("list", kwargs))
        return iter(self.pages)

    def get_object(self, **kwargs):
        self.requests.append(("get", kwargs["Key"]))
        return {"Body": BytesIO(self.bodies[kwargs["Key"]].encode())}

    def head_object(self, **kwargs):
        self.requests.append(("head", kwargs["Bucket"], kwargs["Key"]))
        return {"ContentLength": self.heads[(kwargs["Bucket"], kwargs["Key"])]}


def test_output_files_follow_the_query_manifests():
    root = "runs/r/trials/t/"
    s3 = FakeS3(
        [
            {
                "Contents": [
                    {"Key": f"{root}q.csv", "Size": 10},
                    {"Key": f"{root}u-manifest.csv", "Size": 2},
                    {"Key": f"{root}u.metadata", "Size": 1},
                    {"Key": f"{root}unload/20261012_0001", "Size": 300},
                    # Not in a manifest, so not a data file of the query.
                    {"Key": f"{root}unload/leftover", "Size": 999},
                ]
            },
            {
                "Contents": [
                    {"Key": f"{root}unload/20261012_0002", "Size": 100},
                    {"Key": f"{root}tables/c-manifest.csv", "Size": 2},
                ]
            },
            {},
        ],
        bodies={
            f"{root}u-manifest.csv": (
                f"s3://out/{root}unload/20261012_0001\ns3://out/{root}unload/20261012_0002\n"
            ),
            f"{root}tables/c-manifest.csv": "s3://elsewhere/ctas/f1\n",
        },
        heads={("elsewhere", "ctas/f1"): 50},
    )
    assert output_files(s3, f"s3://out/{root}") == {
        "count": 3,
        "bytes": 450,
        "max_bytes": 300,
        "min_bytes": 50,
    }
    assert s3.requests[0] == ("list", {"Bucket": "out", "Prefix": root})
    assert ("head", "elsewhere", "ctas/f1") in s3.requests


def test_output_files_without_a_manifest_is_an_error():
    s3 = FakeS3([{"Contents": [{"Key": "runs/r/trials/t/q.csv", "Size": 10}]}])
    with pytest.raises(ValueError, match="No query manifest"):
        output_files(s3, "s3://out/runs/r/trials/t/")


@pytest.mark.parametrize(
    ("case", "listed"),
    [(Case("pandas", transport="unload", output="native"), True), (Case("pandas"), False)],
)
def test_unload_trials_record_their_output_files(monkeypatch, tmp_path, run_stubs, case, listed):
    monkeypatch.setattr("pyathena_bench.runner.supervise", lambda *args: {"status": "ok"})
    calls = []

    def summarize(s3, location):
        calls.append((s3, location))
        return {"count": 1, "bytes": 5, "max_bytes": 5, "min_bytes": 5}

    monkeypatch.setattr("pyathena_bench.runner.output_files", summarize)
    settings = replace(Settings(), warmups=1, repetitions=2)
    assert run(settings, RUN_MANIFEST, [case], ["small"], tmp_path / "run")
    trials = [json.loads(line) for line in (tmp_path / "run/trials.jsonl").read_text().splitlines()]
    # Measured trials only, with the run's one client.
    assert [("output_files" in t) for t in trials] == [False, listed, listed]
    assert len(calls) == (2 if listed else 0)
    assert all(
        s3 is run_stubs and location.startswith(f"s3://out/runs/{'a' * 32}/trials/")
        for s3, location in calls
    )


def test_temporal_unload_trials_query_no_time_zone_columns(monkeypatch, tmp_path, run_stubs):
    payloads = []
    monkeypatch.setattr(
        "pyathena_bench.runner.supervise",
        lambda payload, events, settings: payloads.append(payload) or {"status": "ok"},
    )
    monkeypatch.setattr("pyathena_bench.runner.output_files", lambda *args: {})
    cases = [
        Case("arrow", transport=transport, output="native", shape="temporal")
        for transport in ("csv", "unload")
    ]
    settings = replace(Settings(), warmups=0, repetitions=1)
    assert run(settings, RUN_MANIFEST, cases, ["small"], tmp_path / "run")
    csv, unload = (p["sql"] for p in payloads)
    assert "AS event_time_utc" in csv
    assert "AS event_date" in unload
    assert "AS event_time_utc" not in unload


def test_filesystem_trials_transfer_the_prepared_object_within_the_scratch_bucket(
    monkeypatch, tmp_path, run_stubs
):
    monkeypatch.setattr(
        "pyathena_bench.runner.validate_manifest",
        lambda *args: {"ScratchDatabase": "s", "Bucket": "scratch", "Results": "s3://res/p/"},
    )
    monkeypatch.setattr(
        "pyathena_bench.runner.validate_inputs",
        lambda *args: pytest.fail("The filesystem suite reads no snapshot"),
    )
    validated = []
    monkeypatch.setattr(
        "pyathena_bench.runner.validate_objects", lambda *args: validated.append(args[2])
    )
    payloads = []
    monkeypatch.setattr(
        "pyathena_bench.runner.supervise",
        lambda payload, events, settings: payloads.append(payload) or {"status": "ok"},
    )
    source = "s3://scratch/runs/r/objects/small-x"
    manifest = {**RUN_MANIFEST, "objects": {"small": {"location": source, "size": 64}}}
    case = Case(
        "s3", "aio", "write", "bytes", suite="filesystem", max_workers=8, pool_connections=10
    )
    settings = replace(Settings(), warmups=0, repetitions=1)
    assert run(settings, manifest, [case], ["small"], tmp_path / "run")
    assert validated == [["small"]]
    [payload] = payloads
    assert payload["source"] == source
    assert payload["size"] == 64
    trials = f"s3://scratch/runs/{RUN_MANIFEST['run_id']}/trials/"
    assert payload["destination"] == f"{trials}{payload['trial']}/object"
    assert "sql" not in payload


def test_copies_without_a_multipart_copy_are_recorded_as_unsupported(
    monkeypatch, tmp_path, run_stubs
):
    monkeypatch.setattr("pyathena_bench.runner.validate_objects", lambda *args: None)
    monkeypatch.setattr(
        "pyathena_bench.runner.supervise",
        lambda payload, events, settings: pytest.fail("No trial runs for a single CopyObject"),
    )
    manifest = {**RUN_MANIFEST, "objects": {"small": {"location": "s3://out/o", "size": 2**30}}}
    case = Case(
        "s3", "sync", "copy", "bytes", suite="filesystem", max_workers=8, pool_connections=10
    )
    assert run(Settings(), manifest, [case], ["small"], tmp_path / "run")
    [record] = [json.loads(line) for line in (tmp_path / "run" / "trials.jsonl").open()]
    assert record["status"] == "unsupported"
    assert "single CopyObject" in record["reason"]


class TestResume:
    def test_finished_trials_are_skipped_and_outputs_go_to_the_results_location(
        self, monkeypatch, tmp_path, run_stubs
    ):
        monkeypatch.setattr(
            "pyathena_bench.runner.validate_manifest",
            lambda *args: {"ScratchDatabase": "s", "Bucket": "out", "Results": "s3://res/p/"},
        )
        payloads = []
        monkeypatch.setattr(
            "pyathena_bench.runner.supervise",
            lambda payload, events, settings: payloads.append(payload) or {"status": "ok"},
        )
        case = Case("cursor")
        earlier = tmp_path / "attempt-1"
        earlier.mkdir()
        records = [
            {"scale": "small", "case_id": case.id, "repetition": 0, "status": "ok"},
            {"scale": "small", "case_id": case.id, "repetition": 1, "status": "interrupted"},
        ]
        (earlier / "trials.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records))
        settings = replace(Settings(), warmups=0, repetitions=3)
        assert run(
            settings, RUN_MANIFEST, [case], ["small"], tmp_path / "a2", completed_trials([earlier])
        )
        assert [p["repetition"] for p in payloads] == [1, 2]
        prefix = f"s3://res/p/runs/{RUN_MANIFEST['run_id']}/trials/"
        assert all(p["output"].startswith(prefix) for p in payloads)

    @pytest.mark.parametrize(
        ("finished", "expected"), [([0], [-1, 1, 2]), ([0, 1, 2], [])], ids=["missing", "complete"]
    )
    def test_each_attempt_runs_its_own_warmups(
        self, monkeypatch, tmp_path, run_stubs, finished, expected
    ):
        payloads = []
        monkeypatch.setattr(
            "pyathena_bench.runner.supervise",
            lambda payload, events, settings: payloads.append(payload) or {"status": "ok"},
        )
        case = Case("cursor")
        checkpoint = tmp_path / "attempt-1" / "checkpoint"
        checkpoint.mkdir(parents=True)
        warmup = {"scale": "small", "case_id": case.id, "repetition": -1, "warmup": True}
        records = [{**warmup, "trial": "w", "status": "ok"}]
        records += [
            {"trial": f"t{r}", "scale": "small", "case_id": case.id, "repetition": r,
             "status": "ok"}
            for r in finished
        ]  # fmt: skip
        # Earlier records can come from the checkpoint of a lost host.
        for record in records:
            (checkpoint / f"{record['trial']}.json").write_text(json.dumps(record))
        settings = replace(Settings(), warmups=1, repetitions=3)
        assert run(
            settings,
            RUN_MANIFEST,
            [case],
            ["small"],
            tmp_path / "attempt-2",
            completed_trials([tmp_path / "attempt-1"]),
        )
        assert [p["repetition"] for p in payloads] == expected

    def test_checkpoints_are_written_between_trials(self, monkeypatch, tmp_path, run_stubs):
        order = []
        monkeypatch.setattr(
            "pyathena_bench.runner.supervise",
            lambda payload, events, settings: order.append(("trial", payload["trial"]))
            or {"status": "ok"},
        )
        settings = replace(Settings(), warmups=0, repetitions=2)
        assert run(
            settings,
            RUN_MANIFEST,
            [Case("cursor")],
            ["small"],
            tmp_path / "run",
            checkpoint=lambda name, record: order.append(("checkpoint", name)),
        )
        trials = [t for kind, t in order if kind == "trial"]
        assert order == [
            ("checkpoint", f"{trials[0]}-start.json"),
            ("trial", trials[0]),
            ("checkpoint", f"{trials[0]}.json"),
            ("checkpoint", f"{trials[1]}-start.json"),
            ("trial", trials[1]),
            ("checkpoint", f"{trials[1]}.json"),
        ]

    def test_a_trial_whose_start_checkpoint_fails_does_not_run(
        self, monkeypatch, tmp_path, run_stubs
    ):
        ran = []
        monkeypatch.setattr(
            "pyathena_bench.runner.supervise",
            lambda payload, events, settings: ran.append(payload["trial"]) or {"status": "ok"},
        )
        written = []

        def checkpoint(name, record):
            if name.endswith("-start.json"):
                raise OSError("S3 unavailable")
            written.append(name)

        output = tmp_path / "run"
        settings = replace(Settings(), warmups=0, repetitions=2)
        assert not run(
            settings, RUN_MANIFEST, [Case("cursor")], ["small"], output, checkpoint=checkpoint
        )
        # The next attempt could not quiesce the trial, so it fails without running.
        assert ran == []
        [record] = read_records(output / "trials.jsonl")
        assert record["status"] == "error"
        assert record["error"] == "Could not write the start checkpoint: OSError: S3 unavailable"
        assert written == [f"{record['trial']}.json"]
        assert read_records(output / "events.jsonl") == []

    def test_a_failed_record_checkpoint_does_not_stop_the_run(
        self, monkeypatch, tmp_path, run_stubs, caplog
    ):
        monkeypatch.setattr(
            "pyathena_bench.runner.supervise", lambda payload, events, settings: {"status": "ok"}
        )

        def checkpoint(name, record):
            if not name.endswith("-start.json"):
                raise OSError("S3 unavailable")

        settings = replace(Settings(), warmups=0, repetitions=2)
        assert run(
            settings,
            RUN_MANIFEST,
            [Case("cursor")],
            ["small"],
            tmp_path / "run",
            checkpoint=checkpoint,
        )
        assert len(read_records(tmp_path / "run" / "trials.jsonl")) == 2
        assert caplog.text.count("Could not write the checkpoint") == 2
