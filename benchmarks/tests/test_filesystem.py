# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import asyncio
import io
import os
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from fsspec.implementations.memory import MemoryFileSystem
from pyathena_bench import filesystem as filesystem_module
from pyathena_bench.cases import SINGLE_COPY_THRESHOLDS, Case
from pyathena_bench.config import Settings
from pyathena_bench.filesystem import (
    filesystem,
    get_object,
    missing_api_reason,
    put_object,
    read_object,
    run_filesystem,
    transfer,
    write_object,
)


@pytest.fixture
def memory_fs():
    fs = MemoryFileSystem(skip_instance_cache=True)
    fs.store = {}
    fs.pseudo_dirs = [""]
    return fs


def test_written_object_has_the_size_in_blocks_and_reads_back_whole(memory_fs):
    assert write_object(memory_fs, "/bucket/object", 10, 4) == 10
    assert memory_fs.cat_file("/bucket/object") == bytes(10)
    assert read_object(memory_fs, "/bucket/object", 4) == 10


def test_put_uploads_a_sparse_local_file_that_get_downloads_whole(memory_fs, tmp_path):
    assert put_object(memory_fs, "/bucket/object", tmp_path / "up", 10) == 10
    assert os.stat(tmp_path / "up").st_size == 10
    # Sparse: the file holds no block, so an upload reads no disk.
    assert os.stat(tmp_path / "up").st_blocks == 0
    assert memory_fs.cat_file("/bucket/object") == bytes(10)
    assert get_object(memory_fs, "/bucket/object", tmp_path / "down") == 10
    assert (tmp_path / "down").read_bytes() == bytes(10)


class RecordingFileSystem:
    """Records the thread and options of each transfer."""

    def __init__(self):
        self.threads = []
        self.copies = []
        self.transfers = []
        self.connected = False

    def connect(self):
        self.connected = True

    def open(self, path, mode, block_size):
        self.threads.append(threading.get_ident())
        return io.BytesIO()

    def get_file(self, rpath, lpath, **kwargs):
        self.threads.append(threading.get_ident())
        self.transfers.append(("get_file", rpath, lpath, kwargs))
        Path(lpath).write_bytes(b"")

    def put_file(self, lpath, rpath, **kwargs):
        self.threads.append(threading.get_ident())
        self.transfers.append(("put_file", lpath, rpath, kwargs))

    def cp_file(self, path1, path2, **kwargs):
        self.copies.append(("cp_file", path1, path2, kwargs))

    async def _cp_file(self, path1, path2, **kwargs):
        # Recorded only when the coroutine runs.
        self.copies.append(("_cp_file", path1, path2, kwargs))


@pytest.mark.parametrize(
    ("api", "operation", "on_loop_thread"),
    [
        ("sync", "read", True),
        ("sync", "write", True),
        ("sync", "get", True),
        ("sync", "put", True),
        ("s3fs", "read", True),
        ("s3fs", "put", True),
        ("aio", "read", False),
        ("aio", "write", False),
        ("aio", "get", False),
        ("aio", "put", False),
    ],
)
def test_aio_file_transfers_run_in_a_thread_as_the_aio_cursor_reads(
    api, operation, on_loop_thread, tmp_path
):
    fs = RecordingFileSystem()
    case = Case("s3", api, operation, "bytes", suite="filesystem", max_workers=8)

    async def run():
        await transfer(
            fs, case, "s3://b/source", "s3://b/destination", 0, 5 * 2**20, tmp_path / "object"
        )
        return threading.get_ident()

    loop_thread = asyncio.run(run())
    [thread] = fs.threads
    assert (thread == loop_thread) is on_loop_thread


@pytest.mark.parametrize(
    ("api", "size_option"), [("sync", "block_size"), ("aio", "block_size"), ("s3fs", "chunksize")]
)
@pytest.mark.parametrize("operation", ["get", "put"])
def test_local_file_transfers_pass_the_block_size_in_the_api_name(
    api, size_option, operation, tmp_path
):
    fs = RecordingFileSystem()
    case = Case("s3", api, operation, "bytes", suite="filesystem", max_workers=8)
    local = tmp_path / "object"
    transferred = asyncio.run(
        transfer(fs, case, "s3://b/source", "s3://b/destination", 20, 5, local)
    )
    assert transferred == (20 if operation == "put" else 0)
    expected = (
        ("get_file", "s3://b/source", str(local), {size_option: 5})
        if operation == "get"
        else ("put_file", str(local), "s3://b/destination", {size_option: 5})
    )
    assert fs.transfers == [expected]


@pytest.mark.parametrize(
    ("api", "method", "options"),
    [
        (
            "sync",
            "cp_file",
            {
                "max_workers": 8,
                "block_size": 5,
                "TaggingDirective": "REPLACE",
                "AnnotationDirective": "EXCLUDE",
            },
        ),
        (
            "aio",
            "_cp_file",
            {
                "max_workers": 8,
                "block_size": 5,
                "TaggingDirective": "REPLACE",
                "AnnotationDirective": "EXCLUDE",
            },
        ),
        ("s3fs", "cp_file", {"block": 5}),
    ],
)
def test_copy_passes_the_api_options_of_the_case(api, method, options, tmp_path):
    fs = RecordingFileSystem()
    case = Case("s3", api, "copy", "bytes", suite="filesystem", max_workers=8)
    copied = asyncio.run(
        transfer(fs, case, "s3://b/source", "s3://b/destination", 20, 5, tmp_path / "object")
    )
    assert copied == 20
    assert fs.copies == [(method, "s3://b/source", "s3://b/destination", options)]


@pytest.mark.parametrize(
    ("api", "operation", "size", "runs"),
    [
        ("sync", "copy", 5 * 2**30, False),
        ("sync", "copy", 5 * 2**30 + 1, True),
        ("aio", "copy", 5 * 2**30, False),
        ("s3fs", "copy", 150 * 2**20, False),
        ("s3fs", "copy", 150 * 2**20 + 1, True),
        ("sync", "read", 16 * 2**20, True),
        ("sync", "get", 16 * 2**20, True),
        ("sync", "write", 16 * 2**20, True),
        ("sync", "put", 16 * 2**20, True),
    ],
)
def test_only_multipart_copies_run(api, operation, size, runs):
    case = Case("s3", api, operation, "bytes", suite="filesystem", max_workers=8)
    settings = Settings(filesystem_sizes={"small": size})
    assert (case.unsupported_reason(settings, "small") is None) is runs
    assert Case("cursor").unsupported_reason(settings, "small") is None


def test_single_copy_reasons_name_the_threshold():
    settings = Settings(filesystem_sizes={"small": 16 * 2**20})
    reasons = {
        api: Case("s3", api, "copy", "bytes", suite="filesystem").unsupported_reason(
            settings, "small"
        )
        for api in ("sync", "s3fs")
    }
    assert reasons == {
        "sync": "Objects up to 5 GiB are copied with a single CopyObject request",
        "s3fs": "Objects up to 150 MiB are copied with a single CopyObject request",
    }


@pytest.mark.skipif(filesystem_module.s3fs is None, reason="Runs in the s3fs environment")
def test_s3fs_copies_above_its_managed_threshold():
    assert SINGLE_COPY_THRESHOLDS["s3fs"] == filesystem_module.s3fs.core.MANAGED_COPY_THRESHOLD


class RecordingS3FS:
    """Stands in for the s3fs module: records the constructor arguments."""

    def __init__(self):
        self.created = []

    def S3FileSystem(self, **kwargs):  # noqa: N802
        self.created.append(kwargs)
        return RecordingFileSystem()


class FakeCredentials:
    access_key = "AKIA"
    secret_key = "secret"
    token = "token"

    def get_frozen_credentials(self):
        return self


def test_s3fs_filesystem_takes_the_session_credentials_and_the_case_sizes(monkeypatch):
    s3fs = RecordingS3FS()
    monkeypatch.setattr("pyathena_bench.filesystem.s3fs", s3fs)
    session = SimpleNamespace(get_credentials=lambda: FakeCredentials())
    case = Case(
        "s3", "s3fs", "read", "bytes", suite="filesystem", max_workers=8, pool_connections=10
    )
    fs = filesystem(Settings(region="us-west-2"), case, session, connection=None)
    assert isinstance(fs, RecordingFileSystem)
    # The client is created before the transfer, as the PyAthena filesystems' is.
    assert fs.connected
    assert s3fs.created == [
        {
            "key": "AKIA",
            "secret": "secret",
            "token": "token",
            "client_kwargs": {"region_name": "us-west-2"},
            "config_kwargs": {
                "connect_timeout": 10,
                "read_timeout": 60,
                "max_pool_connections": 10,
            },
            "max_concurrency": 8,
            "skip_instance_cache": True,
        }
    ]


def test_s3fs_api_needs_the_s3fs_package(monkeypatch):
    monkeypatch.setattr("pyathena_bench.filesystem.s3fs", None)
    case = Case(
        "s3", "s3fs", "read", "bytes", suite="filesystem", max_workers=8, pool_connections=10
    )
    assert missing_api_reason(case) == "s3fs is not installed in this environment"
    assert missing_api_reason(Case("s3", "sync", "read", "bytes", suite="filesystem")) is None
    with pytest.raises(RuntimeError, match="s3fs environment"):
        filesystem(Settings(), case, SimpleNamespace(), connection=None)


class FakeS3Client:
    def __init__(self, size, head_error=None):
        self.size = size
        self.head_error = head_error
        self.deleted = []

    def head_object(self, **params):
        if self.head_error:
            raise self.head_error
        return {"ContentLength": self.size}

    def delete_object(self, **params):
        self.deleted.append((params["Bucket"], params["Key"]))


class TestRunFilesystem:
    @pytest.fixture
    def s3_client(self, monkeypatch):
        """Replace the connection and filesystems; the fake client answers HeadObject."""
        client = FakeS3Client(10)
        connection = SimpleNamespace(s3_client=client, close=lambda: None)
        monkeypatch.setattr("pyathena_bench.filesystem.connect", lambda **kwargs: connection)
        for name in ("S3FileSystem", "AioS3FileSystem"):
            monkeypatch.setattr(
                f"pyathena_bench.filesystem.{name}", lambda **kwargs: RecordingFileSystem()
            )
        return client

    @pytest.fixture(autouse=True)
    def temp_dir(self, tmp_path):
        """The trial's directory, as the supervisor creates it for the child."""
        self.temp_dir = tmp_path / "trial"
        self.temp_dir.mkdir()

    def run(self, operation, api="aio"):
        case = Case(
            "s3", api, operation, "bytes", suite="filesystem", max_workers=8, pool_connections=10
        )
        return asyncio.run(
            run_filesystem(
                Settings(), case, None, "s3://b/source", "s3://b/out/object", 10, self.temp_dir
            )
        )

    def test_written_object_is_checked_and_deleted(self, s3_client):
        result = self.run("write")
        assert result["status"] == "ok"
        assert result["queries"][0]["bytes"] == 10
        assert s3_client.deleted == [("b", "out/object")]

    def test_uploaded_object_is_checked_with_head_object_and_deleted(self, s3_client):
        # The upload reports 10 bytes; the object's size comes from S3.
        s3_client.size = 11
        result = self.run("put", api="sync")
        assert result["queries"][0]["status"] == "size_mismatch"
        assert result["queries"][0]["bytes"] == 11
        assert s3_client.deleted == [("b", "out/object")]
        assert not (self.temp_dir / "object").exists()

    def test_downloaded_size_is_the_local_file_and_the_file_is_removed(self, s3_client):
        local = []
        s3_client.head_object = lambda **params: pytest.fail("A get checks the local file")

        def get_file(rpath, lpath, **kwargs):
            local.append(lpath)
            Path(lpath).write_bytes(bytes(9))

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(RecordingFileSystem, "get_file", staticmethod(get_file))
            result = self.run("get", api="sync")
        assert result["queries"][0]["status"] == "size_mismatch"
        assert result["queries"][0]["bytes"] == 9
        assert local == [str(self.temp_dir / "object")]
        assert not (self.temp_dir / "object").exists()
        assert s3_client.deleted == []

    def test_short_object_is_a_size_mismatch(self, s3_client):
        s3_client.size = 9
        result = self.run("copy")
        assert result["status"] == "error"
        assert result["queries"][0]["status"] == "size_mismatch"
        assert result["queries"][0]["bytes"] == 9

    def test_loop_lag_covers_the_transfer_but_not_the_size_check(self, s3_client, monkeypatch):
        head_object = s3_client.head_object

        def slow_head_object(**params):
            # Blocks the event loop, as the synchronous client does.
            time.sleep(0.2)
            return head_object(**params)

        monkeypatch.setattr(s3_client, "head_object", slow_head_object)
        result = self.run("write")
        assert result["status"] == "ok"
        assert max(result["loop_lag_seconds"], default=0) < 0.1

    def test_failed_size_check_is_recorded_as_the_transfer_error(self, s3_client):
        s3_client.head_error = RuntimeError("Forbidden")
        result = self.run("write")
        assert result["queries"][0]["status"] == "error"
        assert result["queries"][0]["error"] == "RuntimeError: Forbidden"
        assert "total_seconds" in result["queries"][0]
