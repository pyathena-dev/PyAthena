# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError
from pyathena_bench.__main__ import main
from pyathena_bench.config import Settings
from pyathena_bench.fleet import (
    Filters,
    Queue,
    expand_jobs,
    quiesce,
    status,
    submit,
    trial_evidence,
    work,
)

SETTINGS = Settings(scales={"small": 10000, "xlarge": 10000000})


class FakeS3:
    """In-memory S3 with conditional creation, as used by the fleet queue."""

    def __init__(self):
        self.objects = {}

    def put_object(self, **kwargs):
        if kwargs.get("IfNoneMatch") == "*" and kwargs["Key"] in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[kwargs["Key"]] = kwargs["Body"]

    def get_object(self, **kwargs):
        return {"Body": io.BytesIO(self.objects[kwargs["Key"]])}

    def delete_object(self, **kwargs):
        self.objects.pop(kwargs["Key"], None)

    def upload_file(self, filename, bucket, key):
        self.objects[key] = Path(filename).read_bytes()

    def list_objects_v2(self, **kwargs):
        keys = [k for k in self.objects if k.startswith(kwargs["Prefix"])]
        return {"KeyCount": len(keys[: kwargs.get("MaxKeys", 1000)])}

    def get_paginator(self, name):
        def paginate(**kwargs):
            keys = [k for k in sorted(self.objects) if k.startswith(kwargs["Prefix"])]
            return [{"Contents": [{"Key": k} for k in keys]}]

        return SimpleNamespace(paginate=paginate)


def queue_with(jobs, tmp_path):
    s3 = FakeS3()
    queue = Queue(s3, "bucket", "run1")
    config = tmp_path / "config.toml"
    config.write_text("[scales]\nsmall = 10000\n")
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    submit(queue, jobs, config, manifest)
    return s3, queue


def job(job_id, heavy=False, api_weight=1):
    return {
        "id": job_id,
        "args": [],
        "api_heavy": heavy,
        "api_weight": api_weight if heavy else 0,
        "pages": 0,
        "weight": 0,
    }


def no_queries(ids):
    return []


class TestExpandJobs:
    def test_row_cursor_jobs_are_split_by_arraysize_and_marked_api_heavy(self):
        jobs = expand_jobs(SETTINGS, ["single"], ["small"], ["flat"], Filters(family=["cursor"]))
        assert len(jobs) == 6
        assert all(j["api_heavy"] for j in jobs)
        assert {j["id"] for j in jobs} >= {
            "single-small-flat-cursor-sync-csv-rows-a100",
            "single-small-flat-cursor-aio-csv-rows-a1000",
        }
        assert jobs[0]["pages"] >= jobs[-1]["pages"]
        assert jobs[0]["args"][-2:] == ["--arraysize", "100"]

    def test_dataframe_chunk_variants_share_one_job(self):
        jobs = expand_jobs(
            SETTINGS,
            ["single"],
            ["small"],
            ["flat"],
            Filters(family=["pandas"], api=["sync"], transport=["csv"], output_kind="native"),
        )
        assert [j["id"] for j in jobs] == ["single-small-flat-pandas-sync-csv-native"]
        assert not jobs[0]["api_heavy"]
        assert "--arraysize" not in jobs[0]["args"]

    def test_long_api_trials_become_single_repetition_jobs(self):
        jobs = expand_jobs(
            SETTINGS,
            ["single"],
            ["xlarge"],
            ["flat"],
            Filters(family=["cursor"], api=["sync"]),
            split_pages=50000,
        )
        split = [j for j in jobs if "-a100-" in j["id"]]
        assert [j["id"][-3:] for j in split] == ["-r1", "-r2", "-r3", "-r4", "-r5"]
        assert all(j["args"][-4:] == ["--warmups", "0", "--repetitions", "1"] for j in split)
        assert [j["id"] for j in jobs if "-a1000" in j["id"]] == [
            "single-xlarge-flat-cursor-sync-csv-rows-a1000"
        ]

    def test_api_heavy_jobs_come_first(self):
        jobs = expand_jobs(
            SETTINGS, ["single"], ["small"], ["flat"], Filters(family=["cursor", "arrow"])
        )
        heavy = [j["api_heavy"] for j in jobs]
        assert heavy == sorted(heavy, reverse=True)


class TestExpandJobsArraysize:
    def test_row_output_jobs_keep_one_arraysize_for_every_family(self):
        jobs = expand_jobs(
            SETTINGS,
            ["single"],
            ["small"],
            ["flat"],
            Filters(family=["s3fs"], api=["sync"], arraysize=[100]),
        )
        assert [j["id"] for j in jobs] == ["single-small-flat-s3fs-sync-csv-rows-a100"]
        assert jobs[0]["args"][-2:] == ["--arraysize", "100"]
        assert not jobs[0]["api_heavy"]

    def test_concurrent_row_jobs_weigh_simultaneous_paging_queries(self):
        settings = Settings(scales={"small": 10000}, concurrency=[1, 10])
        jobs = expand_jobs(
            settings, ["concurrent"], ["small"], ["flat"], Filters(family=["cursor"], api=["aio"])
        )
        assert [(j["api_heavy"], j["api_weight"]) for j in jobs] == [(True, 10)]


class TestQueue:
    def test_submit_refuses_an_existing_queue_and_duplicate_ids(self, tmp_path):
        s3, queue = queue_with([job("a")], tmp_path)
        with pytest.raises(ValueError, match="already reserved"):
            submit(queue, [job("b")], tmp_path / "config.toml", tmp_path / "manifest.json")
        with pytest.raises(ValueError, match="unique"):
            submit(
                Queue(s3, "bucket", "run2"),
                [job("a"), job("a")],
                tmp_path / "config.toml",
                tmp_path / "manifest.json",
            )

    def test_workers_run_each_job_once_and_record_results(self, tmp_path):
        s3, queue = queue_with([job("heavy", heavy=True), job("light")], tmp_path)
        runs = []
        settled = []

        def runner(item, workdir):
            runs.append(item["id"])
            out = workdir / "results" / item["id"]
            out.mkdir(parents=True)
            (out / "summary.csv").write_text("x")
            return 0 if item["id"] == "light" else 1

        assert work(
            queue,
            tmp_path / "w1",
            api_slots=1,
            poll_seconds=0,
            runner=runner,
            settle=settled.append,
        )
        assert (
            work(
                queue,
                tmp_path / "w2",
                api_slots=1,
                poll_seconds=0,
                runner=runner,
                settle=no_queries,
            )
            == []
        )
        assert sorted(runs) == ["heavy", "light"]
        assert [path.name for path in settled] == ["heavy"]
        assert queue.key("results", "light", "summary.csv") in s3.objects
        assert not queue.names("slots")
        summary = status(queue)
        assert summary["done"] == 2
        assert list(summary["failed"]) == ["heavy"]

    def test_api_heavy_jobs_wait_for_a_free_slot(self, tmp_path):
        s3, queue = queue_with([job("heavy", heavy=True), job("light")], tmp_path)
        other_host = queue.key("slots", "0")
        s3.put_object(Bucket="bucket", Key=other_host, Body=b"{}")
        order = []

        def runner(item, workdir):
            order.append(item["id"])
            s3.delete_object(Bucket="bucket", Key=other_host)
            return 0

        work(queue, tmp_path / "w", api_slots=1, poll_seconds=0, runner=runner, settle=no_queries)
        assert order == ["light", "heavy"]


class TestCommandLine:
    def test_jobs_command_prints_the_expanded_queue(self, tmp_path, capsys):
        config = tmp_path / "config.toml"
        config.write_text("[scales]\nsmall = 10000\n")
        argv = ["--config", str(config), "jobs", "--suite", "single", "--scale", "small"]
        assert main([*argv, "--family", "cursor", "--shape", "flat", "nested"]) == 0
        jobs = json.loads(capsys.readouterr().out)
        assert len(jobs) == 12
        assert {j["args"][5] for j in jobs} == {"flat", "nested"}

    def test_plan_honors_arraysize_filter_and_repetition_overrides(self, tmp_path, capsys):
        config = tmp_path / "config.toml"
        config.write_text("[scales]\nsmall = 10000\n")
        argv = ["--config", str(config), "plan", "--suite", "single", "--scale", "small"]
        filters = ["--family", "cursor", "--api", "sync", "--arraysize", "1000"]
        assert main([*argv, *filters, "--warmups", "0", "--repetitions", "1"]) == 0
        plan = json.loads(capsys.readouterr().out)
        assert [c["arraysize"] for c in plan["cases"]] == [1000]
        assert plan["trials"] == 1


class TestQueueSafety:
    def test_interrupted_publication_can_be_repeated_and_is_not_consumed(self, tmp_path):
        s3 = FakeS3()
        queue = Queue(s3, "bucket", "run1")
        config = tmp_path / "config.toml"
        config.write_text("[scales]\nsmall = 10000\n")
        with pytest.raises(FileNotFoundError):
            submit(queue, [job("a")], config, tmp_path / "missing.json")
        with pytest.raises(ValueError, match="incompletely published"):
            work(queue, tmp_path / "w", api_slots=1, runner=lambda *a: 0, settle=no_queries)
        manifest = tmp_path / "manifest.json"
        manifest.write_text("{}")
        submit(queue, [job("a")], config, manifest)
        assert status(queue)["jobs"] == 1

    def test_failed_jobs_are_quiesced_and_unconfirmed_queries_stop_the_worker(self, tmp_path):
        _, queue = queue_with([job("first"), job("second")], tmp_path)
        runs = []

        def runner(item, workdir):
            runs.append(item["id"])
            out = workdir / "results" / item["id"]
            out.mkdir(parents=True)
            (out / "events.jsonl").write_text(
                json.dumps({"event": "query", "query_id": "q1", "trial": "t"}) + "\n"
            )
            return 1

        def settle(output):
            assert trial_evidence(output)[0] == {"q1"}
            return ["q1: still RUNNING"]

        assert work(queue, tmp_path / "w", api_slots=1, runner=runner, settle=settle) == ["first"]
        assert runs == ["first"]
        assert status(queue)["failed"]["first"]["quiesce_errors"] == ["q1: still RUNNING"]

    def test_waiting_heavy_job_reserves_slots_from_later_heavy_jobs(self, tmp_path, monkeypatch):
        s3, queue = queue_with(
            [job("wide", heavy=True, api_weight=2), job("narrow", heavy=True)], tmp_path
        )
        other_host = queue.key("slots", "0")
        s3.put_object(Bucket="bucket", Key=other_host, Body=b"{}")
        attempts = []

        def sleep_and_release(seconds):
            attempts.append(sorted(queue.names("claims")))
            s3.delete_object(Bucket="bucket", Key=other_host)

        order = []

        def runner(item, workdir):
            order.append(item["id"])
            return 0

        monkeypatch.setattr("pyathena_bench.fleet.time.sleep", sleep_and_release)
        work(queue, tmp_path / "w", api_slots=2, runner=runner, settle=no_queries)
        assert attempts == [[]]
        assert order == ["wide", "narrow"]

    def test_trial_evidence_reads_reported_queries_and_trial_prefixes(self, tmp_path):
        (tmp_path / "events.jsonl").write_text(
            "\n".join(
                [
                    json.dumps({"event": "trial_start", "output": "s3://b/runs/r/trials/t1/"}),
                    json.dumps({"event": "query", "query_id": "a"}),
                    json.dumps({"event": "athena", "query_id": "b"}),
                    "{incomplete",
                ]
            )
        )
        assert trial_evidence(tmp_path) == ({"a"}, ("s3://b/runs/r/trials/t1/",))
        assert trial_evidence(tmp_path / "missing") == (set(), ())

    def test_jobs_wider_than_the_api_slots_stop_the_worker_before_claiming(self, tmp_path):
        _, queue = queue_with([job("wide", heavy=True, api_weight=11)], tmp_path)
        with pytest.raises(ValueError, match="more than 10 API slots"):
            work(queue, tmp_path / "w", api_slots=10, runner=lambda *a: 0, settle=no_queries)
        assert not queue.names("claims")

    def test_slot_acquisition_errors_release_taken_slots(self, tmp_path):
        s3, queue = queue_with([], tmp_path)
        original = s3.put_object

        def flaky(**kwargs):
            if kwargs["Key"].endswith("slots/1"):
                raise ClientError({"Error": {"Code": "InternalError"}}, "PutObject")
            original(**kwargs)

        s3.put_object = flaky
        with pytest.raises(ClientError):
            queue.acquire_slots(3, 2, {"host": "h"})
        assert not queue.names("slots")

    def test_overlapping_submissions_cannot_share_a_name(self, tmp_path):
        s3, queue = queue_with([job("a")], tmp_path)
        with pytest.raises(ValueError, match="reserved"):
            submit(queue, [job("b")], tmp_path / "config.toml", tmp_path / "manifest.json")
        assert json.loads(queue.read("jobs.json"))[0]["id"] == "a"


class FakeAthena:
    def __init__(self, states, stop_errors=()):
        self.states = states
        self.stop_errors = set(stop_errors)
        self.stopped = []

    def stop_query_execution(self, **kwargs):
        query_id = kwargs["QueryExecutionId"]
        self.stopped.append(query_id)
        if query_id in self.stop_errors:
            raise ClientError({"Error": {"Code": "InvalidRequestException"}}, "StopQueryExecution")
        self.states[query_id] = "CANCELLED"

    def get_query_execution(self, **kwargs):
        return {"QueryExecution": {"Status": {"State": self.states[kwargs["QueryExecutionId"]]}}}


class TestQuiesce:
    def test_unreported_queries_under_trial_prefixes_are_stopped(self, tmp_path, monkeypatch):
        (tmp_path / "events.jsonl").write_text(
            json.dumps({"event": "trial_start", "output": "s3://b/runs/r/trials/t1/"}) + "\n"
        )
        athena = FakeAthena({"unreported": "RUNNING"})
        found = {}

        def active(client_, workgroup, prefixes):
            found["prefixes"] = prefixes
            return {"unreported"}

        monkeypatch.setattr("pyathena_bench.fleet.session", lambda settings: None)
        monkeypatch.setattr("pyathena_bench.fleet.client", lambda session_, name: athena)
        monkeypatch.setattr("pyathena_bench.fleet.active_queries", active)
        assert quiesce(Settings(poll_interval=0.01), tmp_path) == []
        assert found["prefixes"] == ("s3://b/runs/r/trials/t1/",)
        assert athena.stopped == ["unreported"]

    def test_stop_errors_are_ignored_once_the_query_is_terminal(self, tmp_path, monkeypatch):
        (tmp_path / "events.jsonl").write_text(
            json.dumps({"event": "query", "query_id": "done"}) + "\n"
        )
        athena = FakeAthena({"done": "SUCCEEDED"}, stop_errors={"done"})
        monkeypatch.setattr("pyathena_bench.fleet.session", lambda settings: None)
        monkeypatch.setattr("pyathena_bench.fleet.client", lambda session_, name: athena)
        assert quiesce(Settings(poll_interval=0.01), tmp_path) == []


class TestFollowUpSafety:
    def test_a_queue_without_a_reservation_is_not_overwritten(self, tmp_path):
        s3, queue = queue_with([job("a")], tmp_path)
        s3.delete_object(Bucket="bucket", Key=queue.key("reservation.json"))
        with pytest.raises(ValueError, match="already exists"):
            submit(queue, [job("b")], tmp_path / "config.toml", tmp_path / "manifest.json")
        assert json.loads(queue.read("jobs.json"))[0]["id"] == "a"
        assert not queue.exists("reservation.json")

    def test_a_failed_existence_check_releases_the_reservation(self, tmp_path):
        s3 = FakeS3()
        queue = Queue(s3, "bucket", "run1")

        def failing(**kwargs):
            raise ClientError({"Error": {"Code": "InternalError"}}, "ListObjectsV2")

        s3.list_objects_v2 = failing
        with pytest.raises(ClientError):
            submit(queue, [job("a")], tmp_path / "config.toml", tmp_path / "manifest.json")
        assert queue.key("reservation.json") not in s3.objects

    def test_quiesce_errors_are_recorded_before_the_worker_stops(self, tmp_path):
        _, queue = queue_with([job("first"), job("second")], tmp_path)

        def settle(output):
            raise RuntimeError("Could not inspect query history")

        ran = work(queue, tmp_path / "w", api_slots=1, runner=lambda *a: 1, settle=settle)
        assert ran == ["first"]
        errors = status(queue)["failed"]["first"]["quiesce_errors"]
        assert errors == [
            "Could not quiesce the job: RuntimeError: Could not inspect query history"
        ]
