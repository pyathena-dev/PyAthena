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
from pyathena_bench.fleet import Filters, Queue, expand_jobs, status, submit, work

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


def job(job_id, heavy=False):
    return {"id": job_id, "args": [], "api_heavy": heavy, "pages": 0, "weight": 0}


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


class TestQueue:
    def test_submit_refuses_an_existing_queue_and_duplicate_ids(self, tmp_path):
        s3, queue = queue_with([job("a")], tmp_path)
        with pytest.raises(ValueError, match="already exists"):
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

        def runner(item, workdir):
            runs.append(item["id"])
            out = workdir / "results" / item["id"]
            out.mkdir(parents=True)
            (out / "summary.csv").write_text("x")
            return 0 if item["id"] == "light" else 1

        assert work(queue, tmp_path / "w1", api_slots=1, poll_seconds=0, runner=runner)
        assert work(queue, tmp_path / "w2", api_slots=1, poll_seconds=0, runner=runner) == []
        assert sorted(runs) == ["heavy", "light"]
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

        work(queue, tmp_path / "w", api_slots=1, poll_seconds=0, runner=runner)
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
