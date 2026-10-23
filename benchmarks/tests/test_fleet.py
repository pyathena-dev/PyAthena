# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import csv
import http.client
import io
import json
import signal
import threading
import time
import urllib.error
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError
from pyathena_bench.__main__ import main
from pyathena_bench.cases import Case
from pyathena_bench.config import Settings
from pyathena_bench.fleet import (
    QUIESCE_RETRIES,
    QUIESCE_ROUNDS,
    Filters,
    JobWatch,
    Lease,
    Queue,
    SpotInterruptionError,
    check_notice,
    expand_jobs,
    interruption_notice,
    is_throttling,
    merge_jobs,
    quiesce,
    retire_host,
    run_job,
    status,
    submit,
    suspend_rebalance,
    trial_evidence,
    work,
)

SETTINGS = Settings(scales={"small": 10000, "xlarge": 10000000})


class FakeS3:
    """In-memory S3 with conditional writes, as used by the fleet queue."""

    def __init__(self):
        self.objects = {}
        self.etags = {}
        self.writes = 0

    def put_object(self, **kwargs):
        key = kwargs["Key"]
        if kwargs.get("IfNoneMatch") == "*" and key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        if "IfMatch" in kwargs and self.etags.get(key) != kwargs["IfMatch"]:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.writes += 1
        self.objects[key] = kwargs["Body"]
        self.etags[key] = f'"{self.writes}"'
        return {"ETag": self.etags[key]}

    def get_object(self, **kwargs):
        if kwargs["Key"] not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {
            "Body": io.BytesIO(self.objects[kwargs["Key"]]),
            "ETag": self.etags.get(kwargs["Key"], '"0"'),
        }

    def delete_object(self, **kwargs):
        self.objects.pop(kwargs["Key"], None)
        self.etags.pop(kwargs["Key"], None)

    def upload_file(self, filename, bucket, key):
        self.put_object(Key=key, Body=Path(filename).read_bytes())

    def download_file(self, bucket, key, filename):
        Path(filename).write_bytes(self.objects[key])

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


def throttling(operation, code="ThrottlingException"):
    return ClientError({"Error": {"Code": code, "Message": "Rate exceeded"}}, operation)


def live_slots(queue):
    """Return the API slots that a worker holds; released slots stay as objects."""
    return [
        name
        for name in queue.names("slots")
        if not queue.read_lease(queue.key("slots", name)).takeable(time.time())
    ]


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

    def test_jobs_of_another_environment_are_named_and_marked_for_it(self):
        jobs = expand_jobs(
            SETTINGS,
            ["single"],
            ["small"],
            ["flat"],
            Filters(family=["polars"], api=["sync"], transport=["unload"], output_kind="native"),
            environment="polars2",
        )
        # The suffix keeps the claims, done markers, and results apart from the default's.
        assert [(j["id"], j["environment"]) for j in jobs] == [
            ("single-small-flat-polars-sync-unload-native-polars2", "polars2")
        ]
        default = expand_jobs(SETTINGS, ["single"], ["small"], ["flat"], Filters(family=["polars"]))
        assert not any("environment" in j for j in default)

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

    def test_trial_overrides_are_carried_by_unsplit_jobs(self):
        jobs = expand_jobs(
            replace(SETTINGS, warmups=0, repetitions=3),
            ["single"],
            ["xlarge"],
            ["flat"],
            Filters(family=["cursor"], api=["sync"]),
            split_pages=50000,
            explicit_trials=True,
        )
        whole = [j for j in jobs if "-a1000" in j["id"]]
        assert whole[0]["args"][-4:] == ["--warmups", "0", "--repetitions", "3"]
        assert whole[0]["pages"] == 10001 * 3
        split = [j for j in jobs if "-a100-" in j["id"]]
        assert [j["id"][-3:] for j in split] == ["-r1", "-r2", "-r3"]
        assert all(j["args"][-4:] == ["--warmups", "0", "--repetitions", "1"] for j in split)

    def test_api_heavy_jobs_come_first(self):
        jobs = expand_jobs(
            SETTINGS, ["single"], ["small"], ["flat"], Filters(family=["cursor", "arrow"])
        )
        heavy = [j["api_heavy"] for j in jobs]
        assert heavy == sorted(heavy, reverse=True)


class TestExpandJobsFilesystem:
    def test_one_job_per_api_and_operation_runs_the_worker_and_pool_sweep(self):
        settings = replace(SETTINGS, filesystem_sizes={"large": 2**30})
        jobs = expand_jobs(settings, ["filesystem"], ["large"], ["flat", "nested"], Filters())
        assert len(jobs) == 15
        assert not any(j["api_heavy"] for j in jobs)
        job = next(j for j in jobs if j["id"] == "filesystem-large-flat-s3-aio-read-bytes")
        assert job["args"][:4] == ["--suite", "filesystem", "--scale", "large"]
        assert job["args"][-4:] == ["--transport", "read", "--output-kind", "bytes"]
        cases = len(settings.filesystem_max_workers) * len(settings.filesystem_pool_connections)
        trials = settings.warmups + settings.repetitions
        assert job["weight"] == 2**30 // 10_000 * cases * trials
        # A 1 GiB copy is a single CopyObject, so the copy job only records unsupported cases.
        copy = next(j for j in jobs if j["id"] == "filesystem-large-flat-s3-aio-copy-bytes")
        assert copy["weight"] == 2**30 // 10_000 * trials

    def test_s3fs_jobs_run_in_the_s3fs_environment_without_a_suffix(self):
        settings = replace(SETTINGS, filesystem_sizes={"large": 2**30})
        jobs = expand_jobs(settings, ["filesystem"], ["large"], ["flat"], Filters())
        environments = {j["id"]: j.get("environment") for j in jobs}
        assert environments["filesystem-large-flat-s3-s3fs-read-bytes"] == "s3fs"
        assert environments["filesystem-large-flat-s3-sync-read-bytes"] is None
        assert not any(j["id"].endswith("-s3fs-s3fs") for j in jobs)
        # s3fs copies a 1 GiB object as a multipart copy, which the job runs.
        copy = next(j for j in jobs if j["id"] == "filesystem-large-flat-s3-s3fs-copy-bytes")
        cases = len(settings.filesystem_max_workers) * len(settings.filesystem_pool_connections)
        assert copy["weight"] == 2**30 // 10_000 * cases * (settings.warmups + settings.repetitions)

    def test_s3fs_jobs_reject_another_environment(self):
        settings = replace(SETTINGS, filesystem_sizes={"large": 2**30})
        with pytest.raises(ValueError, match="s3fs API runs in the s3fs environment, not polars2"):
            expand_jobs(
                settings, ["filesystem"], ["large"], ["flat"], Filters(), environment="polars2"
            )
        jobs = expand_jobs(
            settings, ["filesystem"], ["large"], ["flat"], Filters(), environment="s3fs"
        )
        assert {j.get("environment") for j in jobs} == {"s3fs"}
        assert any(j["id"] == "filesystem-large-flat-s3-sync-read-bytes-s3fs" for j in jobs)


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

    def test_initialization_validation_of_row_cursors_is_api_heavy(self):
        jobs = expand_jobs(
            SETTINGS, ["init"], ["small"], ["flat"], Filters(family=["cursor", "arrow"])
        )
        heavy = {j["id"]: (j["api_heavy"], j["api_weight"]) for j in jobs}
        assert heavy["init-small-flat-cursor-direct-csv-rows-a1000"] == (True, 1)
        assert heavy["init-small-flat-arrow-direct-csv-native"] == (False, 0)

    def test_page_estimate_includes_the_column_label_row(self):
        cursor = Filters(family=["cursor"], api=["direct"])
        jobs = expand_jobs(SETTINGS, ["init"], ["small"], ["flat"], cursor, split_pages=11)
        assert [j["id"] for j in jobs] == [
            f"init-small-flat-cursor-direct-csv-rows-a1000-r{repetition}"
            for repetition in range(1, 6)
        ]
        assert {j["pages"] for j in jobs} == {11}
        unsplit = expand_jobs(SETTINGS, ["init"], ["small"], ["flat"], cursor, split_pages=12)
        assert [j["id"] for j in unsplit] == ["init-small-flat-cursor-direct-csv-rows-a1000"]

    def test_concurrent_row_jobs_weigh_simultaneous_paging_queries(self):
        settings = Settings(scales={"small": 10000}, concurrency=[1, 10])
        jobs = expand_jobs(
            settings, ["concurrent"], ["small"], ["flat"], Filters(family=["cursor"], api=["aio"])
        )
        assert [(j["api_heavy"], j["api_weight"]) for j in jobs] == [(True, 10)]


class TestExpandJobsSelection:
    def test_chunksize_filter_is_passed_to_the_job_and_named_in_its_id(self):
        selection = Filters(
            family=["pandas"], api=["sync"], transport=["csv"], output_kind="native"
        )
        jobs = [
            job
            for chunks in ([0], [100000])
            for job in expand_jobs(
                SETTINGS,
                ["single"],
                ["small"],
                ["flat"],
                replace(selection, chunksize=chunks),
            )
        ]
        assert [j["id"] for j in jobs] == [
            "single-small-flat-pandas-sync-csv-native-c0",
            "single-small-flat-pandas-sync-csv-native-c100000",
        ]
        assert [j["args"][-2:] for j in jobs] == [["--chunksize", "0"], ["--chunksize", "100000"]]
        assert merge_jobs([jobs[:1], jobs[1:]]) == jobs
        # A group without chunked cases names only what it runs.
        rows = expand_jobs(
            SETTINGS,
            ["single"],
            ["small"],
            ["flat"],
            Filters(family=["s3fs"], api=["sync"], arraysize=[1000], chunksize=[0, 100000]),
        )
        assert [(j["id"], j["args"][-2:]) for j in rows] == [
            ("single-small-flat-s3fs-sync-csv-rows-a1000-c0", ["--chunksize", "0"])
        ]
        # Equal selections in another order or with repeats get the same job.
        again = expand_jobs(
            SETTINGS, ["single"], ["small"], ["flat"], replace(selection, chunksize=[100000, 0, 0])
        )
        assert [(j["id"], j["args"][-3:]) for j in again] == [
            ("single-small-flat-pandas-sync-csv-native-c0_100000", ["--chunksize", "0", "100000"])
        ]
        cases = [Case("pandas", output="native", chunksize=c) for c in (None, 10000, 100000)]
        assert [c.chunksize for c in replace(selection, chunksize=[0]).select(cases)] == [None]
        assert [c.chunksize for c in replace(selection, chunksize=[100000]).select(cases)] == [
            100000
        ]

    def test_read_sql_query_through_the_rest_driver_pages_through_the_api(self):
        jobs = expand_jobs(
            SETTINGS,
            ["single"],
            ["small"],
            ["flat"],
            Filters(family=["sqla_rest", "sqla_pandas"], chunksize=[0]),
        )
        heavy = {j["id"]: (j["api_heavy"], j["api_weight"]) for j in jobs}
        assert heavy == {
            "single-small-flat-sqla_rest-sync-csv-native-c0": (True, 1),
            "single-small-flat-sqla_pandas-sync-csv-native-c0": (False, 0),
            "single-small-flat-sqla_pandas-sync-unload-native-c0": (False, 0),
        }


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

        def runner(item, workdir, out, resume, stop, checkpoint):
            runs.append(item["id"])
            out.mkdir(parents=True)
            (out / "trials.jsonl").write_text(json.dumps(ok_trial(0)) + "\n")
            return 0 if item["id"] == "light" else 1

        assert work(
            queue,
            tmp_path / "w1",
            api_slots=1,
            poll_seconds=0,
            runner=runner,
            settle=lambda output: settled.append(output) or [],
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
        assert [path.parent.name for path in settled] == ["heavy"]
        assert queue.key("results", "light", "attempt-1", "trials.jsonl") in s3.objects
        assert queue.key("results", "light", "attempt-1", "job-summary.csv") in s3.objects
        assert not live_slots(queue)
        summary = status(queue)
        assert summary["done"] == 2
        assert list(summary["failed"]) == ["heavy"]

    def test_api_heavy_jobs_wait_for_a_free_slot(self, tmp_path):
        s3, queue = queue_with([job("heavy", heavy=True), job("light")], tmp_path)
        other_host = queue.key("slots", "0")
        assert queue.acquire(other_host, {"host": "other"}, 600)
        order = []

        def runner(item, workdir, *args):
            order.append(item["id"])
            s3.delete_object(Bucket="bucket", Key=other_host)
            return 0

        work(queue, tmp_path / "w", api_slots=1, poll_seconds=0, runner=runner, settle=no_queries)
        assert order == ["light", "heavy"]


class TestMergeJobs:
    def test_merged_lists_follow_the_queue_order(self):
        heavy = {**job("heavy", heavy=True), "pages": 10}
        longer = {**job("longer"), "weight": 2}
        merged = merge_jobs([[job("shorter"), longer], [heavy]])
        assert [j["id"] for j in merged] == ["heavy", "longer", "shorter"]

    def test_duplicate_ids_across_lists_are_rejected(self):
        with pytest.raises(ValueError, match="unique"):
            merge_jobs([[job("a")], [job("a")]])

    def test_unknown_environments_are_rejected_before_publishing(self):
        known = {**job("b"), "environment": "polars2"}
        with pytest.raises(ValueError, match=r"unknown environment: c$"):
            merge_jobs([[job("a"), known], [{**job("c"), "environment": "polars3"}]])


METADATA = {
    "instance-id": "i-123",
    "instance-type": "r7g.2xlarge",
    "placement/availability-zone": "us-west-2b",
    "instance-life-cycle": "spot",
}


def metadata_with(**tags):
    def read(path):
        if path.startswith("tags/instance/"):
            name = path.removeprefix("tags/instance/")
            if name not in tags:
                raise urllib.error.HTTPError(path, 404, "Not Found", None, None)
            return tags[name]
        return METADATA[path]

    return read


class FakeAutoScaling:
    """Auto Scaling client that records calls in order."""

    def __init__(self):
        self.calls = []

    def describe_auto_scaling_instances(self, **kwargs):
        self.calls.append(("describe", kwargs))
        return {"AutoScalingInstances": [{"AutoScalingGroupName": "fleet-group"}]}

    def suspend_processes(self, **kwargs):
        self.calls.append(("suspend", kwargs))

    def terminate_instance_in_auto_scaling_group(self, **kwargs):
        self.calls.append(("terminate", kwargs))


class TestRetireHost:
    def test_worker_start_suspends_zone_rebalancing(self):
        autoscaling = FakeAutoScaling()
        assert suspend_rebalance(autoscaling, metadata_with()) == "fleet-group"
        assert autoscaling.calls == [
            ("describe", {"InstanceIds": ["i-123"]}),
            (
                "suspend",
                {"AutoScalingGroupName": "fleet-group", "ScalingProcesses": ["AZRebalance"]},
            ),
        ]

    def test_finished_worker_records_its_host_and_terminates_it(self, tmp_path):
        s3, queue = queue_with([job("a")], tmp_path)
        autoscaling = FakeAutoScaling()
        assert retire_host(queue, autoscaling, ["a"], metadata=metadata_with(KeepHost="false"))
        assert autoscaling.calls == [
            ("terminate", {"InstanceId": "i-123", "ShouldDecrementDesiredCapacity": True}),
        ]
        record = json.loads(s3.objects[queue.key("hosts", "i-123.json")])
        assert record["ran"] == ["a"]
        assert record["kept"] is False
        assert (record["instance_type"], record["availability_zone"], record["lifecycle"]) == (
            "r7g.2xlarge",
            "us-west-2b",
            "spot",
        )

    def test_keep_tag_leaves_the_host_and_its_group_unchanged(self, tmp_path):
        s3, queue = queue_with([job("a")], tmp_path)
        autoscaling = FakeAutoScaling()
        assert not retire_host(queue, autoscaling, [], metadata=metadata_with(KeepHost="true"))
        assert autoscaling.calls == []
        assert json.loads(s3.objects[queue.key("hosts", "i-123.json")])["kept"] is True


class TestCommandLine:
    def test_jobs_command_prints_the_expanded_queue(self, tmp_path, capsys):
        config = tmp_path / "config.toml"
        config.write_text("[scales]\nsmall = 10000\n")
        argv = ["--config", str(config), "jobs", "--suite", "single", "--scale", "small"]
        assert main([*argv, "--family", "cursor", "--shape", "flat", "nested"]) == 0
        jobs = json.loads(capsys.readouterr().out)
        assert len(jobs) == 12
        assert {j["args"][5] for j in jobs} == {"flat", "nested"}

    def test_jobs_command_carries_trial_overrides(self, tmp_path, capsys):
        config = tmp_path / "config.toml"
        config.write_text("[scales]\nsmall = 10000\n")
        argv = ["--config", str(config), "jobs", "--suite", "single", "--scale", "small"]
        filters = ["--family", "pandas", "--api", "sync", "--transport", "csv"]
        assert main([*argv, *filters, "--output-kind", "native", "--repetitions", "3"]) == 0
        jobs = json.loads(capsys.readouterr().out)
        assert [j["args"][-4:] for j in jobs] == [["--warmups", "1", "--repetitions", "3"]]

    def test_jobs_command_selects_the_environment(self, tmp_path, capsys):
        config = tmp_path / "config.toml"
        config.write_text("[scales]\nsmall = 10000\n")
        argv = ["--config", str(config), "jobs", "--suite", "single", "--scale", "small"]
        assert main([*argv, "--family", "polars", "--environment", "polars2"]) == 0
        jobs = json.loads(capsys.readouterr().out)
        assert {j["environment"] for j in jobs} == {"polars2"}

    def test_plan_honors_arraysize_filter_and_repetition_overrides(self, tmp_path, capsys):
        config = tmp_path / "config.toml"
        config.write_text("[scales]\nsmall = 10000\n")
        argv = ["--config", str(config), "plan", "--suite", "single", "--scale", "small"]
        filters = ["--family", "cursor", "--api", "sync", "--arraysize", "1000"]
        assert main([*argv, *filters, "--warmups", "0", "--repetitions", "1"]) == 0
        plan = json.loads(capsys.readouterr().out)
        assert [c["arraysize"] for c in plan["cases"]] == [1000]
        assert plan["trials"] == 1

    def test_filesystem_scales_are_the_object_sizes(self, tmp_path, capsys):
        config = tmp_path / "config.toml"
        config.write_text("[scales]\nsmall = 10000\n[filesystem]\nsizes = { large = 1073741824 }\n")
        argv = ["--config", str(config), "plan", "--suite", "filesystem"]
        assert main([*argv, "--scale", "large", "--api", "aio", "--transport", "read"]) == 0
        plan = json.loads(capsys.readouterr().out)
        assert plan["scales"] == {"large": 2**30}
        assert {c["id"] for c in plan["cases"]} == {
            f"filesystem-s3-aio-read-w{w}-p{p}" for w in (8, 40) for p in (10, 40)
        }
        assert plan["warnings"] == []
        with pytest.raises(ValueError, match="Select distinct scales"):
            main([*argv, "--scale", "small"])

    def test_prepare_uploads_the_selected_filesystem_objects_with_the_manifest(
        self, tmp_path, monkeypatch
    ):
        config = tmp_path / "config.toml"
        config.write_text("[filesystem]\nsizes = { large = 1073741824 }\n")
        calls = []
        monkeypatch.setattr(
            "pyathena_bench.__main__.prepare",
            lambda settings, stack, path, scales: calls.append(("prepare", scales)) or {},
        )
        monkeypatch.setattr(
            "pyathena_bench.__main__.prepare_objects",
            lambda settings, manifest, path, scales: calls.append(("objects", scales)),
        )
        argv = ["--config", str(config), "prepare", "--stack", "s", "--manifest", "m.json"]
        assert main([*argv, "--filesystem-scale", "large"]) == 0
        assert calls == [("prepare", []), ("objects", ["large"])]
        with pytest.raises(ValueError, match="Select distinct scales"):
            main([*argv, "--filesystem-scale", "small"])


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

        def runner(item, workdir, out, resume, stop, checkpoint):
            runs.append(item["id"])
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
        assert queue.acquire(other_host, {"host": "other"}, 600)
        attempts = []

        def sleep_and_release(seconds):
            attempts.append(sorted(queue.names("claims")))
            s3.delete_object(Bucket="bucket", Key=other_host)

        order = []

        def runner(item, workdir, *args):
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
        # A trial process killed while writing leaves an incomplete final record.
        with pytest.warns(UserWarning, match="incomplete final record"):
            assert trial_evidence(tmp_path) == ({"a"}, ("s3://b/runs/r/trials/t1/",))
        assert trial_evidence(tmp_path / "missing") == (set(), ())

    def test_jobs_of_a_missing_environment_stop_the_worker_before_claiming(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr("pyathena_bench.fleet.sys.prefix", str(tmp_path / ".venv"))
        _, queue = queue_with(
            [job("default"), {**job("other"), "environment": "polars2"}], tmp_path
        )
        with pytest.raises(ValueError, match=r"environment missing on this host: other$"):
            work(queue, tmp_path / "w", api_slots=1, runner=lambda *a: 0, settle=no_queries)
        assert not queue.names("claims")

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
            return original(**kwargs)

        s3.put_object = flaky
        with pytest.raises(ClientError):
            queue.acquire_slots(3, 2, {"host": "h"}, 600)
        assert not live_slots(queue)

    def test_overlapping_submissions_cannot_share_a_name(self, tmp_path):
        s3, queue = queue_with([job("a")], tmp_path)
        with pytest.raises(ValueError, match="reserved"):
            submit(queue, [job("b")], tmp_path / "config.toml", tmp_path / "manifest.json")
        assert json.loads(queue.read("jobs.json"))[0]["id"] == "a"


class FakeAthena:
    def __init__(self, states, stop_errors=None):
        self.states = states
        # Error codes of failing StopQueryExecution requests, by query ID.
        self.stop_errors = stop_errors or {}
        self.stopped = []

    def stop_query_execution(self, **kwargs):
        query_id = kwargs["QueryExecutionId"]
        self.stopped.append(query_id)
        if query_id in self.stop_errors:
            raise ClientError({"Error": {"Code": self.stop_errors[query_id]}}, "StopQueryExecution")
        self.states[query_id] = "CANCELLED"

    def get_query_execution(self, **kwargs):
        return {"QueryExecution": {"Status": {"State": self.states[kwargs["QueryExecutionId"]]}}}


@pytest.fixture
def use_athena(monkeypatch):
    """Return a function that makes quiesce() use a fake client and history scan."""

    def install(athena, active=None):
        monkeypatch.setattr("pyathena_bench.fleet.session", lambda settings: None)
        monkeypatch.setattr("pyathena_bench.fleet.client", lambda session_, name, **_: athena)
        if active is not None:

            def scan(client_, workgroup, prefixes, ids):
                ids.update(active(client_, workgroup, prefixes))
                return ids

            monkeypatch.setattr("pyathena_bench.fleet.active_queries", scan)

    return install


def write_events(path, *events):
    (path / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))


class TestQuiesce:
    def test_unreported_queries_under_trial_prefixes_are_stopped(self, tmp_path, use_athena):
        (tmp_path / "events.jsonl").write_text(
            json.dumps({"event": "trial_start", "output": "s3://b/runs/r/trials/t1/"}) + "\n"
        )
        athena = FakeAthena({"unreported": "RUNNING"})
        found = {}

        def active(client_, workgroup, prefixes):
            found["prefixes"] = prefixes
            return {"unreported"}

        use_athena(athena, active)
        assert quiesce(Settings(poll_interval=0.01), tmp_path) == []
        assert found["prefixes"] == ("s3://b/runs/r/trials/t1/",)
        assert athena.stopped == ["unreported"]

    def test_stop_errors_are_ignored_once_the_query_is_terminal(self, tmp_path, use_athena):
        (tmp_path / "events.jsonl").write_text(
            json.dumps({"event": "query", "query_id": "done"}) + "\n"
        )
        athena = FakeAthena({"done": "SUCCEEDED"}, stop_errors={"done": "InvalidRequestException"})
        use_athena(athena)
        assert quiesce(Settings(poll_interval=0.01), tmp_path) == []

    def test_reported_queries_stop_before_unfinished_trials_are_scanned(
        self, tmp_path, monkeypatch, use_athena
    ):
        # The host was lost, so only the attempt's checkpoint exists.
        checkpoint = tmp_path / "checkpoint"
        checkpoint.mkdir()
        records = {
            "t0": {"status": "ok", "query_ids": ["finished"]},
            "t1": {"status": "interrupted", "query_ids": ["reported"]},
            "t2": None,
        }
        for trial, record in records.items():
            start = {"event": "trial_start", "trial": trial, "output": f"s3://b/trials/{trial}/"}
            (checkpoint / f"{trial}-start.json").write_text(json.dumps(start))
            if record is not None:
                (checkpoint / f"{trial}.json").write_text(json.dumps({"trial": trial, **record}))
        athena = FakeAthena({"reported": "RUNNING", "unreported": "RUNNING"})
        scans = []

        def active(client_, workgroup, prefixes):
            scans.append((prefixes, list(athena.stopped)))
            return {"reported", "unreported"}

        use_athena(athena, active)
        retries = []
        monkeypatch.setattr(
            "pyathena_bench.fleet.client",
            lambda session_, name, max_retries: retries.append(max_retries) or athena,
        )
        assert quiesce(Settings(poll_interval=0.01), tmp_path) == []
        # The successful trial is neither stopped nor scanned for.
        assert scans == [(("s3://b/trials/t1/", "s3://b/trials/t2/"), ["reported"])]
        assert athena.stopped == ["reported", "unreported"]
        assert retries == [QUIESCE_RETRIES]

    def test_a_job_whose_trials_succeeded_is_not_scanned(self, tmp_path, use_athena):
        write_events(
            tmp_path,
            {"event": "trial_start", "trial": "t0", "output": "s3://b/trials/t0/"},
            {"event": "query", "trial": "t0", "query_id": "finished"},
        )
        (tmp_path / "trials.jsonl").write_text(json.dumps(ok_trial(0)) + "\n")
        athena = FakeAthena({})

        def active(client_, workgroup, prefixes):
            pytest.fail("A job whose trials all succeeded needs no history scan")

        use_athena(athena, active)
        assert quiesce(Settings(poll_interval=0.01), tmp_path) == []
        assert athena.stopped == []

    def test_a_throttled_history_scan_raises_after_the_known_queries_stop(
        self, tmp_path, monkeypatch, use_athena
    ):
        write_events(
            tmp_path,
            {"event": "trial_start", "trial": "t1", "output": "s3://b/trials/t1/"},
            {"event": "query", "trial": "t1", "query_id": "reported"},
        )
        athena = FakeAthena({"reported": "RUNNING", "found": "RUNNING"})
        stopped_at_scan = []

        def active(client_, workgroup, prefixes, ids):
            stopped_at_scan.extend(athena.stopped)
            # A later page of the history is throttled after a match was found.
            ids.add("found")
            raise throttling("ListQueryExecutions")

        use_athena(athena)
        monkeypatch.setattr("pyathena_bench.fleet.active_queries", active)
        with pytest.raises(ClientError, match="ThrottlingException"):
            quiesce(Settings(poll_interval=0.01), tmp_path)
        assert stopped_at_scan == ["reported"]
        assert athena.stopped == ["reported", "found"]

    def test_a_throttled_stop_does_not_skip_the_other_queries(self, tmp_path, use_athena):
        write_events(
            tmp_path,
            {"event": "trial_start", "trial": "t1", "output": "s3://b/trials/t1/"},
            *({"event": "query", "trial": "t1", "query_id": q} for q in ("first", "second")),
        )
        athena = FakeAthena(
            {"first": "RUNNING", "second": "RUNNING", "unreported": "RUNNING"},
            stop_errors={"first": "ThrottlingException"},
        )
        use_athena(athena, lambda client_, workgroup, prefixes: {"unreported"})
        with pytest.raises(ClientError, match="ThrottlingException"):
            quiesce(Settings(poll_interval=0.01), tmp_path)
        # The history scan still found and stopped the unreported query.
        assert athena.stopped == ["first", "second", "unreported"]
        assert athena.states["second"] == athena.states["unreported"] == "CANCELLED"

    @pytest.mark.parametrize("operation", ["StopQueryExecution", "GetQueryExecution"])
    def test_throttling_does_not_hide_a_query_that_did_not_stop(
        self, tmp_path, use_athena, operation
    ):
        write_events(
            tmp_path,
            *({"event": "query", "trial": "t1", "query_id": q} for q in ("running", "throttled")),
        )
        athena = FakeAthena(
            {"running": "RUNNING", "throttled": "RUNNING"},
            stop_errors={"running": "InvalidRequestException"},
        )
        status = athena.get_query_execution

        def throttled_status(**kwargs):
            if kwargs["QueryExecutionId"] == "throttled":
                raise throttling(operation)
            return status(**kwargs)

        if operation == "StopQueryExecution":
            athena.stop_errors["throttled"] = "ThrottlingException"
        else:
            athena.get_query_execution = throttled_status
        use_athena(athena)
        running, throttled = quiesce(Settings(poll_interval=0.01, timeout_seconds=0.05), tmp_path)
        assert running.startswith("running: still RUNNING")
        assert throttled.startswith("throttled: An error occurred (ThrottlingException)")

    def test_a_throttled_scan_does_not_hide_a_query_that_did_not_stop(self, tmp_path, use_athena):
        write_events(
            tmp_path,
            {"event": "trial_start", "trial": "t1", "output": "s3://b/trials/t1/"},
            {"event": "query", "trial": "t1", "query_id": "q"},
        )
        athena = FakeAthena({"q": "RUNNING"}, stop_errors={"q": "AccessDeniedException"})

        def active(client_, workgroup, prefixes):
            raise throttling("ListQueryExecutions")

        use_athena(athena, active)
        running, scan = quiesce(Settings(poll_interval=0.01, timeout_seconds=0.05), tmp_path)
        assert running.startswith("q: still RUNNING; An error occurred (AccessDeniedException)")
        assert scan.startswith(
            f"query history scan of {tmp_path.name}: ClientError: An error occurred "
            "(ThrottlingException)"
        )

    def test_a_failed_scan_is_reported_after_the_known_queries_stop(
        self, tmp_path, monkeypatch, use_athena
    ):
        write_events(
            tmp_path,
            {"event": "trial_start", "trial": "t1", "output": "s3://b/trials/t1/"},
            {"event": "query", "trial": "t1", "query_id": "reported"},
        )
        athena = FakeAthena({"reported": "RUNNING", "found": "RUNNING"})

        def active(client_, workgroup, prefixes, ids):
            ids.add("found")
            raise RuntimeError("Could not inspect query history: [x]")

        use_athena(athena)
        monkeypatch.setattr("pyathena_bench.fleet.active_queries", active)
        assert quiesce(Settings(poll_interval=0.01), tmp_path) == [
            f"query history scan of {tmp_path.name}: RuntimeError: "
            "Could not inspect query history: [x]"
        ]
        assert athena.states == {"reported": "CANCELLED", "found": "CANCELLED"}

    def test_a_throttled_status_check_raises(self, tmp_path, use_athena):
        write_events(tmp_path, {"event": "query", "trial": "t1", "query_id": "reported"})
        athena = FakeAthena({"reported": "RUNNING"})

        def throttled(**kwargs):
            raise throttling("GetQueryExecution")

        athena.get_query_execution = throttled
        use_athena(athena)
        with pytest.raises(ClientError, match="ThrottlingException"):
            quiesce(Settings(poll_interval=0.01), tmp_path)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (throttling("ListQueryExecutions"), True),
        (throttling("StartQueryExecution", "TooManyRequestsException"), True),
        (throttling("StopQueryExecution", "InvalidRequestException"), False),
        (RuntimeError("ThrottlingException"), False),
    ],
)
def test_is_throttling(error, expected):
    assert is_throttling(error) is expected


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
            "Could not quiesce attempt-1: RuntimeError: Could not inspect query history"
        ]


def ok_trial(repetition, **changes):
    return {
        "trial": f"t{repetition}",
        "case_id": "case",
        "scale": "small",
        "repetition": repetition,
        "warmup": False,
        "status": "ok",
        "queries": [{"total_seconds": 1}],
        "successful_queries_per_second": 1,
        "rss_peak_bytes": 1,
        "max_threads": 1,
        **changes,
    }


def take_over(queue, job_id, host="other", attempt=2):
    """Write the claim that another worker holds after taking the job over."""
    at = time.time()
    queue.put(
        queue.key("claims", job_id),
        {"host": host, "job": job_id, "attempt": attempt, "state": "running",
         "renewed": at, "expires": at + 600},
    )  # fmt: skip


def recording_runner(calls, code=0, trials=None):
    """Return a runner that records its calls and writes the given trial records."""

    def runner(item, workdir, out, resume, stop, checkpoint):
        calls.append((item["id"], out.name, [p.name for p in resume]))
        out.mkdir(parents=True)
        records = (trials or {}).get(item["id"], [])
        (out / "trials.jsonl").write_text("".join(json.dumps(t) + "\n" for t in records))
        return code

    return runner


class TestLease:
    def test_downloads_stay_below_the_destination(self, tmp_path):
        _, queue = queue_with([], tmp_path)
        queue.put(queue.key("results", "a", "attempt-1", "..", "..", "escape.json"), {})
        with pytest.raises(ValueError, match="outside"):
            queue.download_tree(tmp_path / "w" / "attempt-1", "results", "a", "attempt-1")
        assert not (tmp_path / "w" / "escape.json").exists()

    def test_only_one_worker_takes_over_an_expired_lease(self, tmp_path):
        _, queue = queue_with([], tmp_path)
        key = queue.key("claims", "a")
        assert queue.acquire(key, {"host": "dead"}, -1).body["attempt"] == 1
        stale = queue.read_lease(key)
        first = queue.acquire(key, {"host": "one"}, 600)
        assert first.body["attempt"] == 2
        assert queue.acquire(key, {"host": "two"}, 600) is None
        # A worker that read the expired lease before the takeover cannot overwrite it.
        assert queue.put(key, {"host": "late"}, IfMatch=stale.etag) is None
        assert queue.renew(first, 600)
        assert not queue.renew(stale, 600)
        assert queue.read_lease(key).body["host"] == "one"

    @pytest.mark.parametrize("operation", ["create", "takeover", "renew"])
    def test_a_retried_write_that_already_succeeded_keeps_the_lease(self, tmp_path, operation):
        s3, queue = queue_with([], tmp_path)
        key = queue.key("claims", "a")
        if operation != "create":
            lease = queue.acquire(key, {"host": "h"}, -1 if operation == "takeover" else 600)
        original = s3.put_object

        def lost_response(**kwargs):
            # The first write succeeds, but its response is lost and the SDK retries.
            original(**kwargs)
            return original(**kwargs)

        s3.put_object = lost_response
        if operation == "renew":
            assert queue.renew(lease, 600)
            assert queue.read_lease(key).etag == lease.etag
        else:
            taken = queue.acquire(key, {"host": "me"}, 600)
            assert taken.body["host"] == "me"
            assert taken.body["attempt"] == (1 if operation == "create" else 2)
            assert queue.read_lease(key).etag == taken.etag

    def test_acquire_does_not_write_a_live_lease(self, tmp_path):
        s3, queue = queue_with([], tmp_path)
        key = queue.key("claims", "a")
        queue.acquire(key, {"host": "busy"}, 600)
        attempts = []
        original = s3.put_object
        s3.put_object = lambda **kwargs: attempts.append(kwargs["Key"]) or original(**kwargs)
        assert queue.acquire(key, {"host": "me"}, 600) is None
        assert attempts == []

    def test_ending_an_expired_lease_keeps_another_workers_takeover(self, tmp_path):
        _, queue = queue_with([], tmp_path)
        key = queue.key("slots", "0")
        mine = queue.acquire(key, {"host": "slow"}, -1)
        theirs = queue.acquire(key, {"host": "other"}, 600)
        assert not queue.end_lease(mine, "released")
        assert queue.read_lease(key).body == theirs.body
        assert live_slots(queue) == ["0"]

    @pytest.mark.parametrize("state", ["interrupted", "released"])
    def test_ending_a_lease_after_its_own_concurrent_renewal(self, tmp_path, state):
        _, queue = queue_with([], tmp_path)
        key = queue.key("claims", "a")
        lease = queue.acquire(key, {"host": "h", "job": "a"}, 600)
        copy = Lease(key, dict(lease.body), lease.etag)
        assert queue.renew(lease, 600)
        assert queue.end_lease(copy, state)
        assert queue.read_lease(key).body["state"] == state

    def test_slots_are_written_only_when_enough_are_free(self, tmp_path):
        s3, queue = queue_with([], tmp_path)
        queue.acquire(queue.key("slots", "0"), {"host": "busy"}, 600)
        attempts = []
        original = s3.put_object
        s3.put_object = lambda **kwargs: attempts.append(kwargs["Key"]) or original(**kwargs)
        assert queue.acquire_slots(2, 2, {"host": "me"}, 600) is None
        assert attempts == []


class TestJobWatch:
    def test_renews_the_claim_and_slots(self, tmp_path):
        _, queue = queue_with([], tmp_path)
        claim = queue.acquire(queue.key("claims", "a"), {"host": "h"}, 1)
        slot = queue.acquire(queue.key("slots", "0"), {"host": "h"}, 1)
        renewed = claim.body["renewed"], slot.body["renewed"]
        # The renew interval is a tenth of the lease period.
        with JobWatch(queue, claim, [slot], 1, lambda: False) as watch:
            time.sleep(0.5)
            assert not watch.stop_event.is_set()
        assert queue.read_lease(claim.key).body["renewed"] > renewed[0]
        assert queue.read_lease(slot.key).body["renewed"] > renewed[1]

    def test_a_claim_taken_over_by_another_worker_is_lost(self, tmp_path):
        _, queue = queue_with([], tmp_path)
        claim = queue.acquire(queue.key("claims", "a"), {"host": "h"}, 0.1)
        take_over(queue, "a")
        with JobWatch(queue, claim, [], 0.1, lambda: False) as watch:
            assert watch.stop_event.wait(5)
        assert watch.claim_lost.is_set()
        assert not watch.interrupted.is_set()
        assert queue.read_lease(claim.key).body["host"] == "other"

    def test_a_claim_is_lost_while_a_third_of_the_lease_is_left(self, tmp_path):
        _, queue = queue_with([], tmp_path)
        claim = queue.acquire(queue.key("claims", "a"), {"host": "h"}, 1.5)

        def fail(lease, seconds, **changes):
            raise OSError("S3 unavailable")

        queue.renew = fail
        with JobWatch(queue, claim, [], 1.5, lambda: False) as watch:
            assert watch.claim_lost.wait(5)
            # The runner has time to stop before another worker can take the claim over.
            assert time.time() < claim.body["expires"]
            assert not queue.read_lease(claim.key).takeable(time.time())

    def test_a_stalled_renewal_that_outlasts_the_claim_loses_it(self, tmp_path):
        _, queue = queue_with([], tmp_path)
        claim = queue.acquire(queue.key("claims", "a"), {"host": "h"}, 0.2)
        stalled = threading.Event()
        released = threading.Event()

        def stall(lease, seconds, **changes):
            stalled.set()
            released.wait()
            return False

        queue.renew = stall
        with JobWatch(queue, claim, [], 0.1, lambda: False) as watch:
            try:
                assert stalled.wait(5)
                # The claim expires while its renewal is still blocked in S3.
                assert watch.claim_lost.wait(5)
                assert not watch.interrupted.is_set()
            finally:
                # Unblock the renewal before leaving the block joins the thread.
                released.set()


class TestInterruptionNotice:
    def test_notice_comes_from_instance_metadata_or_a_rehearsal_file(self, tmp_path):
        def missing(path):
            raise urllib.error.HTTPError(path, 404, "Not Found", None, None)

        rehearsal = tmp_path / "notice"
        assert not interruption_notice(missing, rehearsal)
        assert interruption_notice(lambda path: '{"action": "terminate"}', rehearsal)
        rehearsal.touch()
        assert interruption_notice(missing, rehearsal)

    def test_notice_errors_count_as_no_notice(self, tmp_path):
        def incomplete(path):
            raise http.client.IncompleteRead(b"")

        assert not interruption_notice(incomplete)

        def broken():
            raise RuntimeError("unexpected")

        assert not check_notice(broken)
        _, queue = queue_with([job("a")], tmp_path)
        assert work(queue, tmp_path / "w", api_slots=1, runner=lambda *a: 0, settle=no_queries,
                    notice=broken) == ["a"]  # fmt: skip


class FakeProcess:
    """A child process that runs until it receives SIGINT."""

    pid = 4321

    def __init__(self, command, stdout, stderr, start_new_session):
        self.command = command
        self.start_new_session = start_new_session
        self.signals = []

    def poll(self):
        return 130 if self.signals else None

    def send_signal(self, signum):
        self.signals.append(signum)

    def wait(self, timeout=None):
        return 130


class TestRunJob:
    @pytest.fixture
    def processes(self, monkeypatch):
        """Return the fake orchestrator processes that ``run_job()`` starts."""
        started = []

        def popen(command, stdout, stderr, start_new_session):
            started.append(FakeProcess(command, stdout, stderr, start_new_session))
            return started[-1]

        monkeypatch.setattr("pyathena_bench.fleet.subprocess.Popen", popen)
        return started

    def test_resumes_writes_checkpoints_and_interrupts_with_sigint(
        self, tmp_path, monkeypatch, processes
    ):
        killed = []
        monkeypatch.setattr(
            "pyathena_bench.fleet.os.killpg", lambda pid, signum: killed.append((pid, signum))
        )
        stop = threading.Event()
        stop.set()
        earlier = tmp_path / "results" / "a" / "attempt-1"
        code = run_job(
            {"id": "a", "args": ["--suite", "single"]},
            tmp_path,
            tmp_path / "results" / "a" / "attempt-2",
            [earlier],
            stop,
            "s3://bucket/fleet/q/results/a/attempt-2/",
        )
        assert code == 130
        [process] = processes
        assert process.signals == [signal.SIGINT]
        assert process.command[-6:] == [
            "--checkpoint", "s3://bucket/fleet/q/results/a/attempt-2/",
            "--suite", "single", "--resume", str(earlier),
        ]  # fmt: skip
        # The orchestrator's own process group, including any trial process, is killed.
        assert process.start_new_session
        assert killed == [(4321, signal.SIGKILL)]
        assert (tmp_path / "logs" / "a-attempt-2.log").exists()

    def test_a_job_of_another_environment_runs_its_interpreter(
        self, tmp_path, monkeypatch, processes
    ):
        monkeypatch.setattr("pyathena_bench.fleet.os.killpg", lambda pid, signum: None)
        monkeypatch.setattr("pyathena_bench.fleet.sys.prefix", str(tmp_path / ".venv"))
        # A set event stops the fake process at once.
        stop = threading.Event()
        stop.set()
        run_job(
            {"id": "a", "args": [], "environment": "polars2"},
            tmp_path,
            tmp_path / "results" / "a" / "attempt-1",
            [],
            stop,
            "s3://bucket/fleet/q/results/a/attempt-1/",
        )
        [process] = processes
        assert process.command[0] == str(tmp_path / ".venv-polars2" / "bin" / "python")


class TestWork:
    def test_expired_claims_are_taken_over_and_live_ones_left(self, tmp_path):
        _, queue = queue_with([job("live"), job("expired")], tmp_path)
        queue.acquire(queue.key("claims", "live"), {"host": "busy", "job": "live"}, 600)
        queue.acquire(queue.key("claims", "expired"), {"host": "dead"}, -1)
        calls = []
        ran = work(
            queue, tmp_path / "w", api_slots=1, poll_seconds=0, runner=recording_runner(calls),
            settle=no_queries,
        )  # fmt: skip
        # Nothing else is pending, so the worker exits while the live claim runs.
        assert ran == ["expired"]
        assert calls == [("expired", "attempt-2", [])]
        assert status(queue)["running"]["live"] == {
            "host": "busy", "job": "live", "attempt": 1, "state": "running", "expired": False,
        }  # fmt: skip

    def test_an_expired_slot_returns_to_the_fleet(self, tmp_path):
        _, queue = queue_with([job("heavy", heavy=True)], tmp_path)
        queue.acquire(queue.key("slots", "0"), {"host": "dead"}, -1)
        assert status(queue)["slots_in_use"]["0"]["expired"]
        ran = work(
            queue, tmp_path / "w", api_slots=1, poll_seconds=0, runner=lambda *a: 0,
            settle=no_queries,
        )  # fmt: skip
        assert ran == ["heavy"]
        assert not live_slots(queue)

    def test_an_interrupted_job_is_resumed_by_another_worker_with_the_same_trials(self, tmp_path):
        s3, queue = queue_with([job("heavy", heavy=True), job("next")], tmp_path)
        notice = threading.Event()

        def interrupted_runner(item, workdir, out, resume, stop, checkpoint):
            out.mkdir(parents=True)
            (out / "trials.jsonl").write_text(json.dumps(ok_trial(0)) + "\n")
            notice.set()
            assert stop.wait(5)
            with (out / "trials.jsonl").open("a") as stream:
                stream.write(json.dumps(ok_trial(1, status="interrupted", queries=[])) + "\n")
            return 1

        with pytest.raises(SpotInterruptionError) as raised:
            work(
                queue, tmp_path / "w1", api_slots=1, poll_seconds=0, runner=interrupted_runner,
                settle=no_queries, lease_seconds=0.5, notice=notice.is_set,
            )  # fmt: skip
        assert raised.value.ran == []
        assert queue.read_lease(queue.key("claims", "heavy")).body["state"] == "interrupted"
        assert not live_slots(queue)
        assert queue.key("results", "heavy", "attempt-1", "trials.jsonl") in s3.objects
        assert not queue.names("done")

        settled = []
        calls = []
        writes = []
        original_put = s3.put_object
        s3.put_object = lambda **kwargs: writes.append(kwargs["Key"]) or original_put(**kwargs)
        runner = recording_runner(calls, trials={"heavy": [ok_trial(1), ok_trial(2)]})
        ran = work(
            queue, tmp_path / "w2", api_slots=1, poll_seconds=0, runner=runner,
            settle=lambda output: settled.append(output.name) or [],
        )  # fmt: skip
        assert ran == ["heavy", "next"]
        assert calls == [("heavy", "attempt-2", ["attempt-1"]), ("next", "attempt-1", [])]
        # The interrupted attempt's queries are quiesced before the job continues.
        assert settled == ["attempt-1"]
        summary_key = queue.key("results", "heavy", "attempt-2", "job-summary.csv")
        order = [key for key in writes if key in {summary_key, queue.key("done", "heavy")}]
        # A recorded job always has its summary, below the attempt that the marker names.
        assert order == [summary_key, queue.key("done", "heavy")]
        assert json.loads(queue.read("done", "heavy"))["summary"] == "heavy/attempt-2"
        row = next(csv.DictReader(io.StringIO(s3.objects[summary_key].decode())))
        assert (row["successful_trials"], row["failed_trials"]) == ("3", "0")
        # The interrupted trial stays visible.
        assert "Superseded: interrupted" in row["notes"]

    def test_a_job_resumes_from_the_checkpoint_of_a_lost_host(self, tmp_path):
        s3, queue = queue_with([job("a")], tmp_path)
        # The host died after one trial; only its checkpoint is in S3.
        queue.acquire(queue.key("claims", "a"), {"host": "dead", "job": "a"}, -1)
        checkpoint = ("results", "a", "attempt-1", "checkpoint")
        start = {"event": "trial_start", "trial": "t1", "output": "s3://r/runs/x/trials/t1/",
                 "case_id": "case", "scale": "small", "repetition": 1}  # fmt: skip
        queue.put(queue.key(*checkpoint, "t0.json"), ok_trial(0))
        queue.put(queue.key(*checkpoint, "t1-start.json"), start)
        prefixes = []
        calls = []

        def settle(output):
            prefixes.extend(trial_evidence(output)[1])
            return []

        runner = recording_runner(calls, trials={"a": [ok_trial(1)]})
        assert work(queue, tmp_path / "w", api_slots=1, runner=runner, settle=settle) == ["a"]
        assert calls == [("a", "attempt-2", ["attempt-1"])]
        # The started trial is quiesced, and the finished one is not repeated.
        assert prefixes == ["s3://r/runs/x/trials/t1/"]
        summary = queue.key("results", "a", "attempt-2", "job-summary.csv")
        row = next(csv.DictReader(io.StringIO(s3.objects[summary].decode())))
        assert row["successful_trials"] == "2"

    def test_a_notice_while_quiescing_an_earlier_attempt_skips_the_run(self, tmp_path):
        _, queue = queue_with([job("a")], tmp_path)
        queue.acquire(queue.key("claims", "a"), {"host": "dead"}, -1)
        queue.put(queue.key("results", "a", "attempt-1", "events.jsonl"), {})
        notice = threading.Event()

        def settle(output):
            notice.set()
            time.sleep(0.2)
            return []

        def runner(*args):
            pytest.fail("A job must not start after an interruption notice")

        with pytest.raises(SpotInterruptionError):
            work(queue, tmp_path / "w", api_slots=1, poll_seconds=0, runner=runner,
                 settle=settle, lease_seconds=0.5, notice=notice.is_set)  # fmt: skip
        lease = queue.read_lease(queue.key("claims", "a"))
        assert (lease.body["state"], lease.body["attempt"]) == ("interrupted", 2)

    def test_a_notice_between_jobs_stops_the_worker_without_claiming(self, tmp_path):
        _, queue = queue_with([job("a")], tmp_path)
        with pytest.raises(SpotInterruptionError):
            work(queue, tmp_path / "w", api_slots=1, runner=lambda *a: 0, notice=lambda: True)
        assert not queue.names("claims")

    def test_waits_for_publication_and_removes_a_rehearsal_notice(self, tmp_path, monkeypatch):
        s3 = FakeS3()
        queue = Queue(s3, "bucket", "run1", "results/stack/")
        config = tmp_path / "config.toml"
        config.write_text("[scales]\nsmall = 10000\n")
        manifest = tmp_path / "manifest.json"
        manifest.write_text("{}")
        rehearsal = tmp_path / "w" / "interruption-notice"
        rehearsal.parent.mkdir()
        rehearsal.touch()
        monkeypatch.setattr(
            "pyathena_bench.fleet.time.sleep",
            lambda seconds: submit(queue, [job("a")], config, manifest),
        )

        def no_instance_action(path):
            raise urllib.error.HTTPError(path, 404, "Not Found", None, None)

        monkeypatch.setattr("pyathena_bench.fleet.instance_metadata", no_instance_action)
        ran = work(queue, tmp_path / "w", api_slots=1, runner=lambda *a: 0, settle=no_queries,
                   wait=True)  # fmt: skip
        # The default notice reads the rehearsal file, which the worker removed when it started.
        assert not rehearsal.exists()
        assert ran == ["a"]

    def test_a_lost_claim_stops_the_attempt_and_quiesces_it(self, tmp_path):
        _, queue = queue_with([job("a")], tmp_path)
        stopped = []
        settled = []

        def runner(item, workdir, out, resume, stop, checkpoint):
            # Another worker took over the claim, for example after this host stalled.
            take_over(queue, "a")
            stopped.append(stop.wait(5))
            return 130

        ran = work(queue, tmp_path / "w", api_slots=1, poll_seconds=0, runner=runner,
                   settle=lambda output: settled.append(output.name) or [],
                   lease_seconds=0.5)  # fmt: skip
        assert stopped == [True]
        assert settled == ["attempt-1"]
        assert ran == []
        assert not queue.names("done")

    def test_a_former_owner_records_nothing_after_a_takeover(self, tmp_path):
        s3, queue = queue_with([job("a")], tmp_path)

        def runner(item, workdir, out, resume, stop, checkpoint):
            # This host stalled; another worker took the claim over before the
            # watch noticed.
            take_over(queue, "a")
            out.mkdir(parents=True)
            (out / "trials.jsonl").write_text(json.dumps(ok_trial(0)) + "\n")
            return 0

        assert work(queue, tmp_path / "w", api_slots=1, runner=runner, settle=no_queries) == []
        assert not queue.names("done")
        assert not [key for key in s3.objects if key.endswith("job-summary.csv")]

    def test_losing_a_slot_does_not_stop_the_job(self, tmp_path):
        _, queue = queue_with([job("heavy", heavy=True)], tmp_path)

        def runner_slot_only(item, workdir, out, resume, stop, checkpoint):
            # Another worker took over the slot, for example after this host stalled.
            queue.put(queue.key("slots", "0"), {"host": "other", "state": "running",
                                                  "expires": time.time() + 600})  # fmt: skip
            time.sleep(0.2)
            assert not stop.is_set()
            return 0

        assert work(queue, tmp_path / "w", api_slots=1, poll_seconds=0, runner=runner_slot_only,
                    settle=no_queries, lease_seconds=0.5) == ["heavy"]  # fmt: skip
        assert json.loads(queue.read("done", "heavy"))["lost_slots"] == [queue.key("slots", "0")]
        # The other worker's slot is kept.
        assert queue.read_lease(queue.key("slots", "0")).body["host"] == "other"

    def test_an_unconfirmed_stopped_attempt_keeps_the_host(self, tmp_path):
        _, queue = queue_with([job("a")], tmp_path)

        def runner(item, workdir, out, resume, stop, checkpoint):
            take_over(queue, "a")
            assert stop.wait(5)
            return 130

        with pytest.raises(RuntimeError, match="Could not quiesce a stopped attempt of a"):
            work(queue, tmp_path / "w", api_slots=1, runner=runner,
                 settle=lambda output: ["q1: still RUNNING"], lease_seconds=0.5)  # fmt: skip
        assert not queue.names("done")

    def test_cleanup_errors_survive_a_claim_lost_during_cleanup(self, tmp_path):
        _, queue = queue_with([job("a"), job("b")], tmp_path)
        calls = []

        def settle(output):
            # The failed job's queries do not stop, and the claim is lost meanwhile.
            take_over(queue, "a")
            return ["q1: still RUNNING"]

        with pytest.raises(RuntimeError, match="q1: still RUNNING"):
            work(queue, tmp_path / "w", api_slots=1, runner=recording_runner(calls, code=1),
                 settle=settle)  # fmt: skip
        # No further trials start while the query may still run.
        assert [call[0] for call in calls] == ["a"]
        assert not queue.names("done")

    def test_a_claim_lost_only_locally_returns_to_the_fleet(self, tmp_path):
        _, queue = queue_with([job("a")], tmp_path)
        renew = queue.renew
        failing = threading.Event()
        failing.set()

        def accepted_but_lost(lease, seconds, **changes):
            # During the first attempt, S3 applies each renewal, but its response
            # is lost, so the claim stays live in S3 while it expires locally.
            if failing.is_set() and changes.get("state") is None:
                at = time.time()
                queue.put(lease.key, {**lease.body, "renewed": at, "expires": at + 600})
                raise ClientError({"Error": {"Code": "RequestTimeout"}}, "PutObject")
            return renew(lease, seconds, **changes)

        queue.renew = accepted_but_lost
        calls = []

        def runner(item, workdir, out, resume, stop, checkpoint):
            calls.append(out.name)
            if len(calls) == 1:
                assert stop.wait(5)
                failing.clear()
                return 130
            return 0

        assert work(queue, tmp_path / "w", api_slots=1, poll_seconds=0, runner=runner,
                    settle=no_queries, lease_seconds=0.3) == ["a"]  # fmt: skip
        # The first attempt released its claim, so the job ran again instead of
        # staying claimed by a worker that had given it up.
        assert calls == ["attempt-1", "attempt-2"]
        assert json.loads(queue.read("done", "a"))["attempt"] == 2

    def test_a_failed_summary_upload_releases_the_claim(self, tmp_path):
        s3, queue = queue_with([job("a")], tmp_path)
        original = s3.upload_file

        def failing(filename, bucket, key):
            if key.endswith("job-summary.csv"):
                raise ClientError({"Error": {"Code": "InternalError"}}, "PutObject")
            return original(filename, bucket, key)

        s3.upload_file = failing
        runner = recording_runner([], trials={"a": [ok_trial(0)]})
        with pytest.raises(ClientError):
            work(queue, tmp_path / "w", api_slots=1, runner=runner, settle=no_queries)
        assert not queue.names("done")
        assert queue.read_lease(queue.key("claims", "a")).body["state"] == "released"

    def test_a_throttled_takeover_releases_the_job_for_a_later_attempt(self, tmp_path):
        _, queue = queue_with([job("a", heavy=True), job("b")], tmp_path)
        # The host of the first attempt was lost during a trial.
        queue.acquire(queue.key("claims", "a"), {"host": "dead", "job": "a"}, -1)
        start = {"event": "trial_start", "trial": "t1", "output": "s3://r/runs/x/trials/t1/",
                 "case_id": "case", "scale": "small", "repetition": 1}  # fmt: skip
        queue.put(queue.key("results", "a", "attempt-1", "checkpoint", "t1-start.json"), start)
        settled = []

        def settle(output):
            settled.append(output.name)
            if len(settled) == 1:
                raise throttling("ListQueryExecutions")
            return []

        calls = []
        runner = recording_runner(calls, trials={"a": [ok_trial(1)], "b": [ok_trial(0)]})
        ran = work(queue, tmp_path / "w", api_slots=1, poll_seconds=0, runner=runner,
                   settle=settle)  # fmt: skip
        # The worker ran the next job meanwhile and kept working until the job was done.
        assert ran == ["b", "a"]
        assert settled == ["attempt-1", "attempt-1"]
        # The job started only after its first attempt was quiesced, and the
        # released attempt left nothing to resume from.
        assert calls == [("b", "attempt-1", []), ("a", "attempt-3", ["attempt-1"])]
        assert json.loads(queue.read("done", "a"))["attempt"] == 3
        assert status(queue)["failed"] == {}
        assert not live_slots(queue)

    def test_throttling_after_an_unconfirmed_query_fails_the_takeover(self, tmp_path):
        _, queue = queue_with([job("a"), job("b")], tmp_path)
        for attempt in (1, 2):
            queue.acquire(queue.key("claims", "a"), {"host": "dead", "job": "a"}, -1)
            queue.put(queue.key("results", "a", f"attempt-{attempt}", "events.jsonl"), {})

        def settle(output):
            if output.name == "attempt-1":
                return ["q1: still RUNNING"]
            raise throttling("ListQueryExecutions")

        calls = []
        ran = work(queue, tmp_path / "w", api_slots=1, runner=recording_runner(calls),
                   settle=settle)  # fmt: skip
        # Throttling was not the only reason, so the job fails and the worker stops.
        assert (ran, calls) == (["a"], [])
        first, second = status(queue)["failed"]["a"]["quiesce_errors"]
        assert first == "q1: still RUNNING"
        assert second.startswith("Could not quiesce attempt-2: ClientError: ")

    def test_a_failed_job_whose_quiescing_is_throttled_stops_the_worker(self, tmp_path):
        _, queue = queue_with([job("first"), job("second")], tmp_path)
        settled = []

        def settle(output):
            settled.append(output.name)
            raise throttling("ListQueryExecutions")

        # The job ran, so it is recorded as failed instead of being released.
        ran = work(queue, tmp_path / "w", api_slots=1, runner=lambda *a: 1, settle=settle,
                   lease_seconds=0.5)  # fmt: skip
        assert ran == ["first"]
        assert settled == ["attempt-1"] * QUIESCE_ROUNDS
        [error] = status(queue)["failed"]["first"]["quiesce_errors"]
        assert error.startswith("Could not quiesce attempt-1: ClientError: ")

    def test_a_failed_job_quiesced_after_throttling_keeps_the_worker(self, tmp_path):
        _, queue = queue_with([job("first"), job("second")], tmp_path)
        settled = []

        def settle(output):
            settled.append(output.name)
            if len(settled) == 1:
                raise throttling("ListQueryExecutions")
            return []

        def runner(item, *args):
            return 1 if item["id"] == "first" else 0

        ran = work(queue, tmp_path / "w", api_slots=1, runner=runner, settle=settle,
                   lease_seconds=0.5)  # fmt: skip
        assert ran == ["first", "second"]
        assert settled == ["attempt-1", "attempt-1"]
        done = json.loads(queue.read("done", "first"))
        assert (done["exit_code"], done["quiesce_errors"]) == (1, [])

    def test_a_notice_while_quiescing_a_failed_job_keeps_the_host(self, tmp_path):
        _, queue = queue_with([job("first"), job("second")], tmp_path)
        notice = threading.Event()

        def settle(output):
            notice.set()
            raise throttling("ListQueryExecutions")

        # A normal return would retire the host, which is being reclaimed anyway.
        with pytest.raises(SpotInterruptionError) as raised:
            work(queue, tmp_path / "w", api_slots=1, runner=lambda *a: 1, settle=settle,
                 lease_seconds=0.5, notice=notice.is_set)  # fmt: skip
        assert raised.value.ran == ["first"]
        [error] = status(queue)["failed"]["first"]["quiesce_errors"]
        assert error.startswith("Could not quiesce attempt-1: ClientError: ")
        assert "second" not in queue.names("claims")

    @pytest.mark.parametrize(
        ("quiescing", "ran"), [("throttled", ["first"]), ("ok", ["first"]), ("lost", [])]
    )
    def test_a_notice_observed_while_quiescing_outlasts_a_failed_check(
        self, tmp_path, quiescing, ran
    ):
        _, queue = queue_with([job("first"), job("second")], tmp_path)
        armed = threading.Event()
        seen = threading.Event()

        def notice():
            # Only the watch thread sees the notice; the worker's own check fails.
            if armed.is_set() and threading.current_thread() is not threading.main_thread():
                seen.set()
                return True
            return False

        def settle(output):
            armed.set()
            assert seen.wait(5)
            if quiescing == "throttled":
                raise throttling("ListQueryExecutions")
            if quiescing == "lost":
                # Another worker took the claim over before it was renewed.
                take_over(queue, "first")
            return []

        with pytest.raises(SpotInterruptionError) as raised:
            work(queue, tmp_path / "w", api_slots=1, runner=lambda *a: 1, settle=settle,
                 lease_seconds=0.5, notice=notice)  # fmt: skip
        assert raised.value.ran == ran
        assert "second" not in queue.names("claims")

    def test_a_lost_claim_does_not_repeat_throttled_quiescing(self, tmp_path):
        _, queue = queue_with([job("a")], tmp_path)
        settled = []

        def runner(item, workdir, out, resume, stop, checkpoint):
            take_over(queue, "a")
            stop.wait(5)
            return 130

        def settle(output):
            settled.append(output.name)
            raise throttling("ListQueryExecutions")

        with pytest.raises(RuntimeError, match="Could not quiesce a stopped attempt of a"):
            work(queue, tmp_path / "w", api_slots=1, runner=runner, settle=settle,
                 lease_seconds=0.5)  # fmt: skip
        assert settled == ["attempt-1"]

    def test_a_notice_after_a_throttled_takeover_stops_before_the_next_claim(self, tmp_path):
        _, queue = queue_with([job("a"), job("b")], tmp_path)
        queue.acquire(queue.key("claims", "a"), {"host": "dead", "job": "a"}, -1)
        queue.put(queue.key("results", "a", "attempt-1", "events.jsonl"), {})
        notice = threading.Event()

        def settle(output):
            notice.set()
            raise throttling("ListQueryExecutions")

        with pytest.raises(SpotInterruptionError):
            work(queue, tmp_path / "w", api_slots=1, runner=recording_runner([]),
                 settle=settle, notice=notice.is_set)  # fmt: skip
        assert queue.read_lease(queue.key("claims", "a")).takeable(time.time())
        assert "b" not in queue.names("claims")

    def test_a_notice_observed_after_a_throttled_takeover_outlasts_a_failed_check(self, tmp_path):
        _, queue = queue_with([job("a"), job("b")], tmp_path)
        queue.acquire(queue.key("claims", "a"), {"host": "dead", "job": "a"}, -1)
        queue.put(queue.key("results", "a", "attempt-1", "events.jsonl"), {})
        armed = threading.Event()
        polling = threading.Event()
        uploading = threading.Event()
        upload_tree = queue.upload_tree

        def upload(*args, **kwargs):
            uploading.set()
            return upload_tree(*args, **kwargs)

        queue.upload_tree = upload

        def notice():
            # Only one request of the watch thread sees the notice; the worker's
            # own check fails.
            if (
                not armed.is_set()
                or polling.is_set()
                or threading.current_thread() is threading.main_thread()
            ):
                return False
            # The request is still in flight when the attempt checks for a notice.
            polling.set()
            return uploading.wait(5)

        def settle(output):
            if armed.is_set():
                return []
            armed.set()
            assert polling.wait(5)
            raise throttling("ListQueryExecutions")

        # Without the observed notice, the worker would run "b" and then "a".
        with pytest.raises(SpotInterruptionError) as raised:
            work(queue, tmp_path / "w", api_slots=1, poll_seconds=0,
                 runner=recording_runner([], trials={"a": [ok_trial(0)], "b": [ok_trial(0)]}),
                 settle=settle, lease_seconds=2, notice=notice)  # fmt: skip
        assert raised.value.ran == []
        assert queue.read_lease(queue.key("claims", "a")).takeable(time.time())
        assert "b" not in queue.names("claims")


class TestWorkerCommand:
    def test_worker_uses_the_results_location_without_the_stack(self, tmp_path, monkeypatch):
        config = tmp_path / "config.toml"
        config.write_text("[scales]\nsmall = 10000\n")
        queues = []

        def stack_resources(*args):
            pytest.fail("A worker with --results needs no stack outputs")

        monkeypatch.setattr("pyathena_bench.__main__.stack_resources", stack_resources)
        monkeypatch.setattr("pyathena_bench.__main__.session", lambda settings: None)
        monkeypatch.setattr("pyathena_bench.__main__.client", lambda *a: FakeS3())
        monkeypatch.setattr(
            "pyathena_bench.__main__.work", lambda queue, *a, **k: queues.append(queue) or []
        )
        argv = ["--config", str(config), "worker", "--results", "s3://bucket/p/stack/"]
        assert main([*argv, "--name", "q", "--wait"]) == 0
        [queue] = queues
        assert (queue.bucket, queue.prefix) == ("bucket", "p/stack/fleet/q/")
