# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import asyncio
import os
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import pytest
from fsspec.asyn import get_loop
from pyathena_bench.cases import Case
from pyathena_bench.config import Settings
from pyathena_bench.objects import (
    batch,
    check_result,
    listing_keys,
    object_paths,
    prepare_trial,
    run_objects,
)

PATHS = object_paths("s3://b/set/files/", 3)
TARGETS = object_paths("s3://b/trial/", 3)


def test_object_paths_and_listing_keys_are_in_key_order():
    assert PATHS == [
        "s3://b/set/files/000000",
        "s3://b/set/files/000001",
        "s3://b/set/files/000002",
    ]
    assert listing_keys("s3://b/set/listing/", 2, 2) == [
        "s3://b/set/listing/000000/000",
        "s3://b/set/listing/000000/001",
        "s3://b/set/listing/000001/000",
        "s3://b/set/listing/000001/001",
    ]


class RecordingFileSystem:
    """Records each batch call; the asyncio methods return coroutines."""

    loop = get_loop()

    def __init__(self, cat_bytes=b"xyzw"):
        self.calls = []
        self.threads = set()
        self.cat_bytes = cat_bytes

    def record(self, name, *args, **kwargs):
        self.calls.append((name, args, kwargs))
        if name in {"cat", "_cat"}:
            return dict.fromkeys(args[0], self.cat_bytes)
        # A find returns one entry for its prefix; the other operations echo their paths.
        return [args[0]] if name in {"find", "_find"} else list(args[0])

    def info(self, path):
        self.threads.add(threading.get_ident())
        return {"name": path}

    async def _info(self, path):
        return {"name": path}

    def __getattr__(self, name):
        if name.startswith("_"):

            async def coroutine(*args, **kwargs):
                return self.record(name, *args, **kwargs)

            return coroutine
        return lambda *args, **kwargs: self.record(name, *args, **kwargs)


def run_batch(fs, case, paths=PATHS, targets=TARGETS, local=Path("/local"), size=4):
    result = batch(fs, case, paths, targets, local, size)
    return asyncio.run(result) if case.api == "aio" else result


@pytest.mark.parametrize(
    ("api", "method", "options"),
    [("sync", "cat", {}), ("aio", "_cat", {"batch_size": 8}), ("s3fs", "cat", {"batch_size": 8})],
)
def test_cat_passes_the_batch_size_of_the_asyncio_apis(api, method, options):
    fs = RecordingFileSystem()
    case = Case("s3", api, "cat", "objects", suite="objects", max_workers=8)
    assert len(run_batch(fs, case)) == 3
    assert fs.calls == [(method, (PATHS,), options)]


@pytest.mark.parametrize("api", ["sync", "aio", "s3fs"])
def test_get_and_put_pair_the_objects_with_local_files_of_the_same_name(api, tmp_path):
    options = {} if api == "sync" else {"batch_size": 8}
    fs = RecordingFileSystem()
    locals_ = [str(tmp_path / f"{i:06d}") for i in range(3)]
    run_batch(fs, Case("s3", api, "get", "objects", suite="objects", max_workers=8), local=tmp_path)
    run_batch(fs, Case("s3", api, "put", "objects", suite="objects", max_workers=8), local=tmp_path)
    prefix = "_" if api == "aio" else ""
    assert fs.calls == [
        (f"{prefix}get", (PATHS, locals_), options),
        (f"{prefix}put", (locals_, TARGETS), options),
    ]


@pytest.mark.parametrize("api", ["sync", "aio", "s3fs"])
def test_pipe_writes_one_buffer_of_the_object_size_to_every_target(api):
    fs = RecordingFileSystem()
    run_batch(fs, Case("s3", api, "pipe", "objects", suite="objects", max_workers=8), size=4)
    [(method, (values,), options)] = fs.calls
    assert method == ("_pipe" if api == "aio" else "pipe")
    assert values == dict.fromkeys(TARGETS, bytes(4))
    assert options == ({} if api == "sync" else {"batch_size": 8})


@pytest.mark.parametrize("api", ["sync", "aio", "s3fs"])
def test_copy_excludes_tags_and_annotations(api):
    fs = RecordingFileSystem()
    run_batch(fs, Case("s3", api, "copy", "objects", suite="objects", max_workers=8))
    expected = {
        **({} if api == "sync" else {"batch_size": 8}),
        "TaggingDirective": "REPLACE",
        "AnnotationDirective": "EXCLUDE",
    }
    assert fs.calls == [("_copy" if api == "aio" else "copy", (PATHS, TARGETS), expected)]


@pytest.mark.parametrize("api", ["sync", "aio", "s3fs"])
def test_rm_and_find_take_no_batch_size(api):
    fs = RecordingFileSystem()
    run_batch(fs, Case("s3", api, "rm", "objects", suite="objects", max_workers=8), paths=TARGETS)
    run_batch(
        fs, Case("s3", api, "find", "objects", suite="objects", max_workers=8), paths=["s3://b/l/"]
    )
    prefix = "_" if api == "aio" else ""
    assert fs.calls == [(f"{prefix}rm", (TARGETS,), {}), (f"{prefix}find", ("s3://b/l/",), {})]


@pytest.mark.parametrize("api", ["sync", "aio", "s3fs"])
def test_info_fans_out_over_every_object(api):
    fs = RecordingFileSystem()
    paths = object_paths("s3://b/set/files/", 20)
    case = Case("s3", api, "info", "objects", suite="objects", max_workers=4)
    entries = run_batch(fs, case, paths=paths)
    assert [e["name"] for e in entries] == paths
    assert fs.calls == []
    if api == "sync":
        # A thread pool of the case's concurrency, not the calling thread.
        assert threading.get_ident() not in fs.threads
        assert 1 <= len(fs.threads) <= 4


class FakeS3:
    """Holds objects by key with their sizes; lists and deletes them by prefix."""

    def __init__(self, objects=None, failing_keys=(), delete_errors=None):
        self.objects = dict(objects or {})
        self.copies = []
        self.deleted = []
        self.failing_keys = set(failing_keys)
        self.delete_errors = delete_errors

    def copy_object(self, **params):
        if params["Key"] in self.failing_keys:
            raise RuntimeError("Throttled")
        self.copies.append((params["CopySource"]["Key"], params["Key"]))
        self.objects[params["Key"]] = self.objects[params["CopySource"]["Key"]]

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, **params):
        contents = [
            {"Key": k, "Size": v}
            for k, v in sorted(self.objects.items())
            if k.startswith(params["Prefix"])
        ]
        return [{"Contents": contents}] if contents else [{}]

    def delete_objects(self, **params):
        for item in params["Delete"]["Objects"]:
            self.deleted.append(item["Key"])
            self.objects.pop(item["Key"], None)
        return {"Errors": self.delete_errors} if self.delete_errors else {}


def test_prepare_trial_creates_sparse_files_for_put_and_copies_for_rm(tmp_path):
    s3 = FakeS3({"set/files/000000": 4, "set/files/000001": 4})
    paths = object_paths("s3://b/set/files/", 2)
    targets = object_paths("s3://b/trial/", 2)
    prepare_trial(
        s3, Case("s3", "sync", "put", "objects", suite="objects"), paths, targets, tmp_path / "p", 4
    )
    files = sorted(p.name for p in (tmp_path / "p").iterdir())
    assert files == ["000000", "000001"]
    assert all(os.stat(tmp_path / "p" / f).st_size == 4 for f in files)
    assert all(os.stat(tmp_path / "p" / f).st_blocks == 0 for f in files)
    assert s3.copies == []
    prepare_trial(
        s3, Case("s3", "sync", "rm", "objects", suite="objects"), paths, targets, tmp_path / "r", 4
    )
    # The copies run in threads, in any order.
    assert sorted(s3.copies) == [
        ("set/files/000000", "trial/000000"),
        ("set/files/000001", "trial/000001"),
    ]
    assert s3.objects["trial/000001"] == 4


@pytest.mark.parametrize(
    ("operation", "result", "objects", "expected"),
    [
        ("cat", {"a": b"xy", "b": b"xy"}, {}, {"objects": 2, "bytes": 4}),
        ("put", None, {"trial/0": 4, "trial/1": 4, "other/0": 4}, {"objects": 2, "bytes": 8}),
        ("pipe", None, {"trial/0": 4}, {"objects": 1, "bytes": 4}),
        ("copy", None, {}, {"objects": 0, "bytes": 0}),
        ("rm", None, {"trial/0": 4}, {"objects": 1}),
        ("find", ["k"] * 5, {}, {"objects": 5}),
        ("info", [{}] * 3, {}, {"objects": 3}),
    ],
)
def test_check_result_counts_what_the_operation_left(
    operation, result, objects, expected, tmp_path
):
    case = Case("s3", "sync", operation, "objects", suite="objects")
    assert check_result(FakeS3(objects), case, result, 2, 4, "s3://b/trial/", tmp_path) == expected


def test_check_result_of_a_get_counts_the_local_files(tmp_path):
    (tmp_path / "a").write_bytes(b"abcd")
    (tmp_path / "b").write_bytes(b"ab")
    case = Case("s3", "sync", "get", "objects", suite="objects")
    assert check_result(FakeS3(), case, None, 2, 4, "s3://b/trial/", tmp_path) == {
        "objects": 2,
        "bytes": 6,
    }


class TestRunObjects:
    SET: ClassVar = {
        "files": "s3://b/set/files/",
        "listing": "s3://b/set/listing/",
        "count": 2,
        "size": 4,
        "keys": 6,
    }

    @pytest.fixture
    def s3(self, monkeypatch):
        """Replace the connection and filesystem; the fake S3 holds the set."""
        s3 = FakeS3({"set/files/000000": 4, "set/files/000001": 4})
        monkeypatch.setattr(
            "pyathena_bench.objects.connect",
            lambda **kwargs: SimpleNamespace(s3_client=s3, close=lambda: None),
        )
        monkeypatch.setattr("pyathena_bench.objects.filesystem", lambda *args: self.fs)
        self.fs = RecordingFileSystem()
        return s3

    def run(self, operation, api="sync", tmp_path=None):
        case = Case(
            "s3", api, operation, "objects", suite="objects", max_workers=8, pool_connections=8
        )
        return asyncio.run(run_objects(Settings(), case, None, self.SET, "s3://b/trial/", tmp_path))

    def test_cat_checks_the_bytes_and_records_cpu_time(self, s3, tmp_path):
        result = self.run("cat", tmp_path=tmp_path)
        [query] = result["queries"]
        assert result["status"] == "ok"
        assert query["status"] == "ok"
        assert (query["objects"], query["bytes"], query["expected_bytes"]) == (2, 8, 8)
        assert query["cpu_seconds"] >= 0
        assert s3.deleted == []

    def test_short_bytes_are_a_size_mismatch(self, s3, tmp_path):
        # The filesystem returns 2 bytes per object; the set's objects have 4.
        self.fs = RecordingFileSystem(cat_bytes=b"xy")
        result = self.run("cat", tmp_path=tmp_path)
        assert result["status"] == "error"
        assert result["queries"][0]["status"] == "size_mismatch"
        assert (result["queries"][0]["bytes"], result["queries"][0]["expected_bytes"]) == (4, 8)

    def test_find_counts_the_listing_keys(self, s3, tmp_path):
        # The recording find returns one entry per path given: the prefix, not 6 keys.
        result = self.run("find", tmp_path=tmp_path)
        assert result["queries"][0]["status"] == "count_mismatch"
        assert result["queries"][0]["expected_objects"] == 6
        assert result["queries"][0]["objects"] == 1
        assert result["queries"][0]["expected_bytes"] is None

    def test_created_objects_are_counted_then_deleted_with_the_local_files(self, s3, tmp_path):
        s3.objects.update({"trial/000000": 4, "trial/000001": 4})
        result = self.run("put", tmp_path=tmp_path)
        assert result["status"] == "ok"
        assert result["queries"][0]["bytes"] == 8
        assert sorted(s3.deleted) == ["trial/000000", "trial/000001"]
        assert not (tmp_path / "objects").exists()
        assert "cpu_seconds" in result["queries"][0]

    def test_rm_copies_the_set_first_and_expects_nothing_left(self, s3, tmp_path):
        result = self.run("rm", tmp_path=tmp_path)
        # The recording rm deletes nothing, so the copies remain.
        assert sorted(s3.copies) == [
            ("set/files/000000", "trial/000000"),
            ("set/files/000001", "trial/000001"),
        ]
        assert result["queries"][0]["status"] == "count_mismatch"
        assert result["queries"][0]["objects"] == 0
        assert sorted(s3.deleted) == ["trial/000000", "trial/000001"]

    def test_a_failed_preparation_is_an_error_whose_copies_are_removed(self, s3, tmp_path):
        s3.failing_keys = {"trial/000001"}
        result = self.run("rm", tmp_path=tmp_path)
        [query] = result["queries"]
        assert (result["status"], query["status"]) == ("error", "error")
        assert query["error"] == "RuntimeError: Throttled"
        assert "cpu_seconds" not in query
        assert s3.copies == [("set/files/000000", "trial/000000")]
        assert s3.deleted == ["trial/000000"]
        assert result["successful_queries_per_second"] == 0.0
        assert not (tmp_path / "objects").exists()

    def test_a_failed_local_removal_makes_the_trial_an_error(self, s3, tmp_path, monkeypatch):
        def failing_rmtree(path):
            raise PermissionError(f"{path}: busy")

        monkeypatch.setattr("pyathena_bench.objects.shutil.rmtree", failing_rmtree)
        result = self.run("get", tmp_path=tmp_path)
        [query] = result["queries"]
        assert (result["status"], query["status"]) == ("error", "error")
        assert query["error"].startswith("Removal failed: PermissionError: ")
        assert result["successful_queries_per_second"] == 0.0

    def test_a_failed_removal_makes_the_trial_an_error(self, s3, tmp_path):
        s3.objects.update({"trial/000000": 4, "trial/000001": 4})
        s3.delete_errors = [{"Key": "trial/000000", "Code": "AccessDenied"}]
        result = self.run("put", tmp_path=tmp_path)
        [query] = result["queries"]
        assert (result["status"], query["status"]) == ("error", "error")
        assert query["error"].startswith("Removal failed: RuntimeError: [{'Key': 'trial/000000'")
        # The measurement of the operation is kept.
        assert (query["objects"], query["bytes"]) == (2, 8)
        assert query["cpu_seconds"] >= 0

    def test_loop_lag_is_sampled_for_the_aio_api_only(self, s3, tmp_path, monkeypatch):
        async def slow_cat(paths, **kwargs):
            await asyncio.sleep(0.05)
            return dict.fromkeys(paths, b"xyzw")

        monkeypatch.setattr(RecordingFileSystem, "_cat", staticmethod(slow_cat), raising=False)
        # The default heartbeat interval is 10 ms, so a 50 ms operation is sampled.
        result = self.run("cat", api="aio", tmp_path=tmp_path)
        assert result["status"] == "ok"
        assert len(result["loop_lag_seconds"]) >= 1
        assert self.run("cat", api="s3fs", tmp_path=tmp_path)["loop_lag_seconds"] is None

    def test_a_failed_operation_is_recorded_and_still_cleaned_up(self, s3, tmp_path, monkeypatch):
        def failing(*args, **kwargs):
            raise RuntimeError("Forbidden")

        monkeypatch.setattr(RecordingFileSystem, "copy", failing, raising=False)
        s3.objects["trial/000000"] = 4
        result = self.run("copy", tmp_path=tmp_path)
        [query] = result["queries"]
        assert query["status"] == "error"
        assert query["error"] == "RuntimeError: Forbidden"
        # The operation's wall and CPU time are kept.
        assert query["total_seconds"] >= 0
        assert query["cpu_seconds"] >= 0
        assert s3.deleted == ["trial/000000"]
