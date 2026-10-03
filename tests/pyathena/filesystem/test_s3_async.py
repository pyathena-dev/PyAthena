import asyncio
import os
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import uuid
from datetime import UTC, datetime
from itertools import chain
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import boto3
import fsspec
import pytest
from fsspec import Callback

from pyathena.filesystem.s3 import S3File, S3FileSystem
from pyathena.filesystem.s3_async import AioS3File, AioS3FileSystem
from pyathena.filesystem.s3_object import (
    S3MultipartUploadPart,
    S3Object,
    S3ObjectType,
    S3StorageClass,
)
from tests import ENV
from tests.pyathena.conftest import connect


@pytest.fixture(scope="class")
def register_async_filesystem():
    fsspec.register_implementation(
        "s3", "pyathena.filesystem.s3_async.AioS3FileSystem", clobber=True
    )
    fsspec.register_implementation(
        "s3a", "pyathena.filesystem.s3_async.AioS3FileSystem", clobber=True
    )


@pytest.mark.usefixtures("register_async_filesystem")
class TestAioS3FileSystem:
    def test_parse_path(self):
        actual = AioS3FileSystem.parse_path("s3://bucket")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = AioS3FileSystem.parse_path("s3://bucket/")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = AioS3FileSystem.parse_path("s3://bucket/path/to/obj")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] is None

        actual = AioS3FileSystem.parse_path("s3://bucket/path/to/obj?versionId=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = AioS3FileSystem.parse_path("s3a://bucket")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = AioS3FileSystem.parse_path("s3a://bucket/")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = AioS3FileSystem.parse_path("s3a://bucket/path/to/obj")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] is None

        actual = AioS3FileSystem.parse_path("s3a://bucket/path/to/obj?versionId=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = AioS3FileSystem.parse_path("bucket")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = AioS3FileSystem.parse_path("bucket/")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = AioS3FileSystem.parse_path("bucket/path/to/obj")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] is None

        actual = AioS3FileSystem.parse_path("bucket/path/to/obj?versionId=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = AioS3FileSystem.parse_path("bucket/path/to/obj?versionID=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = AioS3FileSystem.parse_path("bucket/path/to/obj?versionid=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = AioS3FileSystem.parse_path("bucket/path/to/obj?version_id=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

    def test_parse_path_invalid(self):
        with pytest.raises(ValueError, match="Invalid S3 path format"):
            AioS3FileSystem.parse_path("http://bucket")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            AioS3FileSystem.parse_path("s3://bucket?")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            AioS3FileSystem.parse_path("s3://bucket?foo=bar")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            AioS3FileSystem.parse_path("s3://bucket/path/to/obj?foo=bar")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            AioS3FileSystem.parse_path("s3a://bucket?")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            AioS3FileSystem.parse_path("s3a://bucket?foo=bar")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            AioS3FileSystem.parse_path("s3a://bucket/path/to/obj?foo=bar")

    @pytest.mark.parametrize("max_workers", [1, 4])
    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_part_sizes(self, max_workers):
        # GH-951: the parts are within the S3 part size limits whatever the
        # number of workers; a single worker used to copy the whole object
        # as one part larger than 5 GiB.
        fs = AioS3FileSystem(
            connection=mock.MagicMock(), max_workers=max_workers, skip_instance_cache=True
        )
        sync_fs = fs._sync_fs
        sync_fs._create_multipart_upload = mock.MagicMock(
            return_value=SimpleNamespace(upload_id="uploadid")
        )
        sync_fs._upload_part_copy = mock.MagicMock(
            side_effect=lambda **kw: SimpleNamespace(etag='"e"', part_number=kw["part_number"])
        )
        sync_fs._complete_multipart_upload = mock.MagicMock()

        await fs._copy_object_with_multipart_upload(
            bucket1="bucket",
            key1="src",
            size1=5 * 2**30 + 2**20,
            bucket2="bucket",
            key2="dst",
        )

        parts = sorted(
            (c.kwargs["part_number"], c.kwargs["copy_source_ranges"])
            for c in sync_fs._upload_part_copy.call_args_list
        )
        assert parts == [
            (1, (0, 5 * 2**29 + 2**19)),
            (2, (5 * 2**29 + 2**19, 5 * 2**30 + 2**20)),
        ]

    @pytest.mark.parametrize(
        "block_size",
        [
            S3FileSystem.MULTIPART_UPLOAD_MIN_PART_SIZE - 1,
            S3FileSystem.MULTIPART_UPLOAD_MAX_PART_SIZE + 1,
        ],
    )
    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_invalid_block_size(self, block_size):
        # GH-926: the message states the accepted range.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs._call = mock.MagicMock()

        with pytest.raises(
            ValueError,
            match=r"between 5 MiB \(5242880 bytes\) and 5 GiB \(5368709120 bytes\), inclusive",
        ):
            await fs._copy_object_with_multipart_upload(
                bucket1="bucket",
                key1="src",
                size1=5 * 2**30 + 2**20,
                bucket2="bucket",
                key2="dst",
                block_size=block_size,
            )
        fs._sync_fs._call.assert_not_called()

    @pytest.mark.parametrize("commit", [True, False])
    def test_transaction_pipe_put_file(self, tmp_path, commit):
        # GH-977: pipe_file() and put_file() join the transaction of this
        # filesystem; they used to write through the internal S3FileSystem,
        # which is not in the transaction, and were not rolled back.
        # A real client selects the request parameters of each operation.
        fs = AioS3FileSystem(
            key="dummy", secret="dummy", region_name="us-east-1", skip_instance_cache=True
        )
        put_object = fs._sync_fs._put_object = mock.MagicMock()
        local = tmp_path / "local.txt"
        local.write_bytes(b"local")

        def write():
            with fs.transaction:
                fs.pipe_file("s3://bucket/k1", b"data")
                fs.put_file(str(local), "s3://bucket/k2")
                put_object.assert_not_called()
                if not commit:
                    raise RuntimeError("rollback")

        if commit:
            write()
            assert [
                (c.kwargs["key"], c.kwargs["body"], c.kwargs.get("ContentType"))
                for c in put_object.call_args_list
            ] == [("k1", b"data", None), ("k2", b"local", "text/plain")]
        else:
            with pytest.raises(RuntimeError, match="rollback"):
                write()
            put_object.assert_not_called()

    def test_transaction_pipe_put_file_create_existing(self, tmp_path):
        # GH-972: in a transaction, put_file(mode="create") also raises, when
        # the file is opened, for an existing object.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs.exists = mock.MagicMock(return_value=True)
        fs._sync_fs._call = mock.MagicMock()
        local = tmp_path / "local"
        local.write_bytes(b"a")
        with fs.transaction:
            with pytest.raises(FileExistsError):
                fs.pipe_file("s3://bucket/k1", b"data", mode="create")
            with pytest.raises(FileExistsError):
                fs.put_file(str(local), "s3://bucket/k2", mode="create")
        assert fs._sync_fs.exists.call_count == 2
        fs._sync_fs._call.assert_not_called()

    @pytest.mark.parametrize("mode", ["overwrite", "create"])
    def test_put_file_mode(self, tmp_path, mode):
        # GH-972: fsspec's mode argument used to be sent to PutObject.
        # A real client selects the request parameters of each operation.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs._client = boto3.client(
            "s3", region_name="us-east-1", aws_access_key_id="dummy", aws_secret_access_key="dummy"
        )
        fs._sync_fs.exists = mock.MagicMock(return_value=False)
        fs._sync_fs._call = mock.MagicMock(return_value={"ETag": '"e"'})
        local = tmp_path / "local"
        local.write_bytes(b"a")

        fs.put_file(str(local), "s3://bucket/key", mode=mode)

        (call,) = fs._sync_fs._call.call_args_list
        assert "mode" not in call.kwargs
        assert call.kwargs.get("IfNoneMatch") == ("*" if mode == "create" else None)

    def test_transaction_pipe_file_write(self):
        # GH-997: in a transaction, a non-contiguous memoryview is written,
        # and a failed write does not replace the object with an empty one
        # when the transaction commits.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        put_object = fs._sync_fs._put_object = mock.MagicMock()

        with fs.transaction:
            with (
                mock.patch.object(AioS3File, "write", side_effect=RuntimeError("write failed")),
                pytest.raises(RuntimeError, match="write failed"),
            ):
                fs.pipe_file("s3://bucket/k1", b"data")
            fs.pipe_file("s3://bucket/k2", memoryview(b"ab" * 4)[::2])

        assert [(c.kwargs["key"], c.kwargs["body"]) for c in put_object.call_args_list] == [
            ("k2", b"aaaa")
        ]

    @pytest.mark.parametrize("kwargs", [{"block_size": 4}, {}])
    def test_transaction_pipe_put_file_exceeding_max_parts(self, tmp_path, kwargs):
        # GH-953: in a transaction, as outside one, pipe_file() and put_file()
        # reject data that does not fit in the maximum number of parts before
        # opening the file.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs.MULTIPART_UPLOAD_MAX_PARTS = 3
        fs._sync_fs.default_block_size = 4
        fs._sync_fs._call = mock.MagicMock()
        fs.open = mock.MagicMock()
        local = tmp_path / "local"
        local.write_bytes(b"a" * 13)

        with fs.transaction:
            with pytest.raises(ValueError, match="block_size"):
                fs.pipe_file("s3://bucket/k1", b"a" * 13, **kwargs)
            with pytest.raises(ValueError, match="block_size"):
                fs.put_file(str(local), "s3://bucket/k2", **kwargs)
        fs.open.assert_not_called()
        fs._sync_fs._call.assert_not_called()

    def test_transaction_put_file_block_size(self, tmp_path):
        # In a transaction, put_file() passes block_size and max_workers to
        # open() instead of the S3 API, as outside one.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs.open = mock.MagicMock()
        fs.open.return_value.__enter__.return_value.blocksize = 8
        local = tmp_path / "local"
        local.write_bytes(b"a" * 13)

        with fs.transaction:
            fs.put_file(str(local), "s3://bucket/key", block_size=8)

        fs.open.assert_called_once_with(
            "s3://bucket/key",
            "wb",
            block_size=8,
            max_workers=fs._sync_fs.max_workers,
            s3_additional_kwargs={},
        )

    def test_touch_sync_wrapper(self):
        # GH-977: touch() used to be fsspec's open()-based default, which
        # dropped the PutObject parameters and returned None.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs._call = mock.MagicMock(return_value={"ETag": '"e"'})

        actual = fs.touch("s3://bucket/key", ContentType="text/plain")
        assert isinstance(actual, dict)
        assert fs._sync_fs._call.call_args.kwargs == {
            "Bucket": "bucket",
            "Key": "key",
            "ContentType": "text/plain",
        }

        fs._sync_fs.exists = mock.MagicMock(return_value=True)
        with pytest.raises(ValueError, match="Cannot touch the existing file"):
            fs.touch("s3://bucket/key", truncate=False)

    @pytest.mark.asyncio
    async def test_rm_requests(self):
        # GH-971: _rm() sent the keys of every bucket to the bucket of the
        # first path, dropped the DeleteObjects parameters, ignored per-key
        # errors and emptied a bucket path.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        sync_fs = fs._sync_fs
        sync_fs._call = mock.MagicMock(return_value={})

        await fs._rm(["s3://b1/a", "s3://b2/b"], ExpectedBucketOwner="111122223333")
        assert sorted(
            (c.kwargs["Bucket"], c.kwargs["Delete"]["Objects"], c.kwargs["ExpectedBucketOwner"])
            for c in sync_fs._call.call_args_list
        ) == [
            ("b1", [{"Key": "a"}], "111122223333"),
            ("b2", [{"Key": "b"}], "111122223333"),
        ]

        sync_fs._call.reset_mock()
        with pytest.raises(ValueError, match="Cannot delete the bucket"):
            await fs._rm("s3://bucket", recursive=True)
        sync_fs._call.assert_not_called()

        sync_fs._call.return_value = {
            "Errors": [{"Key": "locked", "Code": "AccessDenied", "Message": "Access Denied"}]
        }
        with pytest.raises(OSError, match=r"bucket/locked \(AccessDenied: Access Denied\)"):
            await fs._rm("s3://bucket/locked")

        sync_fs._call.side_effect = PermissionError("Access Denied")
        sync_fs.dircache["bucket/a"] = []
        with pytest.raises(PermissionError, match="Access Denied"):
            await fs._rm("s3://bucket/a")
        assert "bucket/a" not in sync_fs.dircache

    @pytest.mark.asyncio
    async def test_rm_request_error_keeps_errors(self):
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)

        def call(method, **request):
            if request["Bucket"] == "b1":
                raise PermissionError("Access Denied")
            return {"Errors": [{"Key": "b", "Code": "AccessDenied", "Message": "Access Denied"}]}

        fs._sync_fs._call = call
        with pytest.raises(PermissionError, match="Access Denied") as exc_info:
            await fs._rm(["s3://b1/a", "s3://b2/b"])
        assert exc_info.value.__notes__ == [
            "Failed to delete objects: b2/b (AccessDenied: Access Denied)"
        ]

    @pytest.mark.asyncio
    async def test_rm_cancel_invalidates_cache_after_each_request(self):
        # The request threads keep running after _rm() is cancelled, so each
        # request invalidates the cache of its objects when it finishes.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        started = {"b1": threading.Event(), "b2": threading.Event()}
        release = {"b1": threading.Event(), "b2": threading.Event()}

        def call(method, **request):
            started[request["Bucket"]].set()
            release[request["Bucket"]].wait(10)
            return {}

        async def wait_invalidated(path):
            for _ in range(100):
                if path not in fs.dircache:
                    return
                await asyncio.sleep(0.01)

        fs._sync_fs._call = call
        task = asyncio.create_task(fs._rm(["s3://b1/a", "s3://b2/b"]))
        for event in started.values():
            await asyncio.to_thread(event.wait, 10)
        # Cancel every other task, as asyncio.run() does at shutdown, so the
        # request tasks are cancelled directly, not only through _rm().
        for other in asyncio.all_tasks():
            if other is not asyncio.current_task():
                other.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # Cached while the requests still run, e.g. by a concurrent info().
        fs.dircache["b1/a"] = []
        fs.dircache["b2/b"] = []

        release["b1"].set()
        await wait_invalidated("b1/a")
        assert "b1/a" not in fs.dircache
        assert "b2/b" in fs.dircache

        release["b2"].set()
        await wait_invalidated("b2/b")
        assert "b2/b" not in fs.dircache

    @pytest.mark.asyncio
    async def test_rm_maxdepth(self):
        # GH-962: _rm() did not pass maxdepth when expanding the path.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        sync_fs = fs._sync_fs
        sync_fs._call = mock.MagicMock(return_value={})
        sync_fs.find = mock.MagicMock(return_value=["bucket/dir/a"])
        sync_fs.exists = mock.MagicMock(return_value=True)

        # batch_size is part of fsspec's async _rm() signature.
        await fs._rm("s3://bucket/dir", recursive=True, maxdepth=1, batch_size=10)
        sync_fs.find.assert_called_once_with("bucket/dir", maxdepth=1, withdirs=True, detail=False)
        (call,) = sync_fs._call.call_args_list
        assert call.kwargs["Delete"]["Objects"] == [{"Key": "dir"}, {"Key": "dir/a"}]

    @pytest.mark.parametrize(("mode", "open_mode"), [("overwrite", "wb"), ("create", "xb")])
    def test_put_file_in_transaction_open_parameters(self, tmp_path, mode, open_mode):
        # GH-969: the open() parameters of put_file() go to open(), and the
        # other parameters, also in s3_additional_kwargs, to S3.
        # GH-972: fsspec's mode argument selects the mode of the file.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs.open = mock.MagicMock()
        fs.open.return_value.__enter__.return_value.blocksize = 4
        lpath = tmp_path / "data.csv"
        lpath.write_bytes(b"a")

        fs._put_file_in_transaction(
            str(lpath),
            "s3://bucket/key",
            Callback(),
            mode,
            block_size=S3FileSystem.MULTIPART_UPLOAD_MIN_PART_SIZE,
            max_workers=2,
            StorageClass="STANDARD_IA",
        )

        fs.open.assert_called_once_with(
            "s3://bucket/key",
            open_mode,
            block_size=S3FileSystem.MULTIPART_UPLOAD_MIN_PART_SIZE,
            max_workers=2,
            s3_additional_kwargs={"StorageClass": "STANDARD_IA", "ContentType": "text/csv"},
        )

    @pytest.mark.asyncio
    async def test_cp_file_directory(self):
        # GH-1008: recursive copy() passes the directories, which used to be
        # sent to CopyObject and fail with NoSuchKey.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._info = mock.AsyncMock(return_value=S3FileSystem._directory_object("bucket", "src"))
        fs._sync_fs._call = mock.MagicMock()

        await fs._cp_file("s3://bucket/src", "s3://bucket/dst")
        fs._sync_fs._call.assert_not_called()

    @pytest.mark.asyncio
    async def test_mv(self):
        # GH-1008: the files are copied in parallel, the directories are
        # skipped, and only the copied sources are deleted.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        started = asyncio.Event()
        copies = []

        async def copy_file(path1, path2, **kwargs):
            if path1 == "s3://bucket/d":
                return False
            copies.append((path1, path2, kwargs))
            if len(copies) == 2:
                started.set()
            # Both copies start before either finishes.
            await asyncio.wait_for(started.wait(), 1)
            return True

        fs._copy_file = copy_file
        fs._sync_fs._call = mock.MagicMock(return_value={})

        await fs._mv(
            ["s3://bucket/a", "s3://bucket/d", "s3://bucket/c"],
            ["s3://bucket/x/a", "s3://bucket/x/d", "s3://bucket/x/c"],
            RequestPayer="requester",
        )
        assert sorted(copies) == [
            ("s3://bucket/a", "s3://bucket/x/a", {"RequestPayer": "requester"}),
            ("s3://bucket/c", "s3://bucket/x/c", {"RequestPayer": "requester"}),
        ]
        fs._sync_fs._call.assert_called_once_with(
            fs._sync_fs._client.delete_objects,
            Bucket="bucket",
            Delete={"Objects": [{"Key": "a"}, {"Key": "c"}], "Quiet": True},
        )

    @pytest.mark.parametrize("size", [10, 5 * 2**30 + 1])
    @pytest.mark.asyncio
    async def test_cp_file_multipart_parameters(self, size):
        # GH-967: block_size and max_workers control a multipart copy and are
        # not sent to S3, whatever the size of the object.
        fs = AioS3FileSystem(
            key="dummy", secret="dummy", region_name="us-east-1", skip_instance_cache=True
        )
        fs._info = mock.AsyncMock(
            return_value=S3Object(
                init={"ContentLength": size},
                type=S3ObjectType.S3_OBJECT_TYPE_FILE,
                bucket="bucket",
                key="src",
            )
        )
        sync_fs = fs._sync_fs
        sync_fs._copy_object = mock.MagicMock()
        sync_fs._create_multipart_upload = mock.MagicMock(
            return_value=SimpleNamespace(upload_id="uploadid")
        )
        running = []
        concurrency = []

        def upload_part_copy(**kw):
            running.append(kw["part_number"])
            concurrency.append(len(running))
            time.sleep(0.01)
            running.remove(kw["part_number"])
            return SimpleNamespace(etag='"e"', part_number=kw["part_number"])

        sync_fs._upload_part_copy = mock.MagicMock(side_effect=upload_part_copy)
        sync_fs._complete_multipart_upload = mock.MagicMock()

        await fs._cp_file(
            "s3://bucket/src",
            "s3://bucket/dst",
            block_size=S3FileSystem.MULTIPART_UPLOAD_MAX_PART_SIZE // 2,
            max_workers=1,
            RequestPayer="requester",
            ContentType="text/csv",
        )

        if size <= S3FileSystem.MULTIPART_UPLOAD_MAX_PART_SIZE:
            sync_fs._copy_object.assert_called_once_with(
                bucket1="bucket",
                key1="src",
                version_id1=None,
                bucket2="bucket",
                key2="dst",
                RequestPayer="requester",
                ContentType="text/csv",
            )
        else:
            sync_fs._create_multipart_upload.assert_called_once_with(
                bucket="bucket", key="dst", RequestPayer="requester", ContentType="text/csv"
            )
            # The part copies receive the parameters that they accept, and
            # max_workers limits how many run at once.
            # Two parts, the second with the 1-byte tail.
            assert sync_fs._upload_part_copy.call_count == 2
            assert all(
                c.kwargs["RequestPayer"] == "requester" and "ContentType" not in c.kwargs
                for c in sync_fs._upload_part_copy.call_args_list
            )
            assert max(concurrency) == 1
            assert (
                sync_fs._complete_multipart_upload.call_args.kwargs["RequestPayer"] == "requester"
            )

    def test_internal_file_system_not_cached(self):
        # GH-978: the internal S3FileSystem was kept in the fsspec instance
        # cache, so skip_instance_cache=True instances shared it.
        connection = mock.MagicMock()
        fs1 = AioS3FileSystem(connection=connection, skip_instance_cache=True)
        fs2 = AioS3FileSystem(connection=connection, skip_instance_cache=True)
        assert fs1._sync_fs is not fs2._sync_fs
        assert fs1.dircache is not fs2.dircache

        fs3 = AioS3FileSystem(connection=connection)
        assert AioS3FileSystem(connection=connection) is fs3
        for fs in (fs1, fs2, fs3):
            assert fs._sync_fs not in S3FileSystem._cache.values()

    @pytest.fixture(scope="class")
    def fs(self, request):
        if not hasattr(request, "param"):
            request.param = {}
        return AioS3FileSystem(connection=connect(), **request.param)

    @pytest.mark.parametrize(
        ("fs", "start", "end", "target_data"),
        list(
            chain(
                *[
                    [
                        ({"default_block_size": x}, 0, 5, b"01234"),
                        ({"default_block_size": x}, 2, 7, b"23456"),
                        ({"default_block_size": x}, 0, 10, b"0123456789"),
                    ]
                    for x in (S3FileSystem.DEFAULT_BLOCK_SIZE, 3)
                ]
            )
        ),
        indirect=["fs"],
    )
    def test_read(self, fs, start, end, target_data):
        # lowest level access: use _get_object
        data = fs._sync_fs._get_object(
            ENV.s3_staging_bucket, ENV.s3_filesystem_test_file_key, ranges=(start, end)
        )
        assert data == (start, target_data), data
        with fs.open(
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}", "rb"
        ) as file:
            # mid-level access: use _fetch_range
            data = file._fetch_range(start, end)
            assert data == target_data, data
            # high-level: use fileobj seek and read
            file.seek(start)
            data = file.read(end - start)
            assert data == target_data, data

    @pytest.mark.parametrize(
        ("base", "exp"),
        [
            (1, 2**10),
            (1, 2**20),
        ],
    )
    def test_write(self, fs, base, exp):
        data = b"a" * (base * exp)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_write/{uuid.uuid4()}"
        )
        with fs.open(path, "wb") as f:
            f.write(data)
        with fs.open(path, "rb") as f:
            actual = f.read()
            assert len(actual) == len(data)
            assert actual == data

    @pytest.mark.parametrize(
        "size",
        [
            2**10,  # < block size: one-shot PutObject path (the GH-719 regression)
            10 * 2**20,  # > block size (5 MiB): multipart path via the async executor
        ],
    )
    def test_write_transaction(self, fs, size):
        # GH-719 regression for the async filesystem: AioS3File inherits
        # _upload_chunk/commit from S3File, so the transaction fix must hold here
        # too, including the multipart path driven by the async executor.
        data = b"a" * size
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_write_transaction/{uuid.uuid4()}"
        )
        with fs.transaction, fs.open(path, "wb") as f:
            f.write(data)
        with fs.open(path, "rb") as f:
            actual = f.read()
            assert len(actual) == len(data)
            assert actual == data

    def test_write_transaction_rollback(self, fs):
        # Kept small-only on purpose: the multipart discard()/abort path is
        # already exercised by the sync test (AioS3File inherits discard()),
        # so there is no need to pay for another large upload here.
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_write_transaction_rollback/{uuid.uuid4()}"
        )

        def write_then_fail():
            with fs.transaction:
                f = fs.open(path, "wb")
                f.write(b"hello world")
                f.close()
                raise RuntimeError("rollback")

        with pytest.raises(RuntimeError):
            write_then_fail()
        fs.invalidate_cache(path)
        assert not fs.exists(path)

    @pytest.mark.parametrize(
        ("base", "exp"),
        [
            (1, 2**10),
            (1, 2**20),
        ],
    )
    def test_append(self, fs, base, exp):
        data = b"a" * (base * exp)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_append/{uuid.uuid4()}"
        )
        with fs.open(path, "ab") as f:
            f.write(data)
        extra = b"extra"
        with fs.open(path, "ab") as f:
            f.write(extra)
        with fs.open(path, "rb") as f:
            actual = f.read()
            assert len(actual) == len(data + extra)
            assert actual == data + extra

    @pytest.mark.asyncio
    async def test_ls_buckets(self, fs):
        fs.invalidate_cache()
        actual = await fs._ls("s3://")
        assert ENV.s3_staging_bucket in actual, actual

        fs.invalidate_cache()
        actual = await fs._ls("s3:///")
        assert ENV.s3_staging_bucket in actual, actual

        fs.invalidate_cache()
        actual = await fs._ls("s3://", detail=True)
        found = next(filter(lambda x: x.name == ENV.s3_staging_bucket, actual), None)
        assert found
        assert found.name == ENV.s3_staging_bucket

        fs.invalidate_cache()
        actual = await fs._ls("s3:///", detail=True)
        found = next(filter(lambda x: x.name == ENV.s3_staging_bucket, actual), None)
        assert found
        assert found.name == ENV.s3_staging_bucket

    @pytest.mark.asyncio
    async def test_ls_dirs(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_ls_dirs"
        )
        for i in range(5):
            await fs._pipe_file(f"{dir_}/prefix/test_{i}", bytes(i))
        await fs._touch(f"{dir_}/prefix2")

        assert len(await fs._ls(f"{dir_}/prefix")) == 5
        assert len(await fs._ls(f"{dir_}/prefix/")) == 5
        assert len(await fs._ls(f"{dir_}/prefix/test_")) == 0
        assert len(await fs._ls(f"{dir_}/prefix2")) == 1

        test_1 = await fs._ls(f"{dir_}/prefix/test_1")
        assert len(test_1) == 1
        assert test_1[0] == fs._strip_protocol(f"{dir_}/prefix/test_1")

        test_1_detail = await fs._ls(f"{dir_}/prefix/test_1", detail=True)
        assert len(test_1_detail) == 1
        assert test_1_detail[0].name == fs._strip_protocol(f"{dir_}/prefix/test_1")
        assert test_1_detail[0].size == 1

    @pytest.mark.asyncio
    async def test_info_bucket(self, fs):
        dir_ = f"s3://{ENV.s3_staging_bucket}"
        bucket, key, version_id = fs.parse_path(dir_)
        info = await fs._info(dir_)

        assert info.name == fs._strip_protocol(dir_)
        assert info.bucket == bucket
        assert info.key is None
        assert info.last_modified is None
        assert info.size == 0
        assert info.etag is None
        assert info.type == S3ObjectType.S3_OBJECT_TYPE_DIRECTORY
        assert info.storage_class == S3StorageClass.S3_STORAGE_CLASS_BUCKET
        assert info.version_id == version_id

        dir_ = f"s3://{ENV.s3_staging_bucket}/"
        bucket, key, version_id = fs.parse_path(dir_)
        info = await fs._info(dir_)

        assert info.name == fs._strip_protocol(dir_)
        assert info.bucket == bucket
        assert info.key is None
        assert info.last_modified is None
        assert info.size == 0
        assert info.etag is None
        assert info.type == S3ObjectType.S3_OBJECT_TYPE_DIRECTORY
        assert info.storage_class == S3StorageClass.S3_STORAGE_CLASS_BUCKET
        assert info.version_id == version_id

    @pytest.mark.asyncio
    async def test_info_dir(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_info_dir"
        )
        file = f"{dir_}/{uuid.uuid4()}"

        fs.invalidate_cache()
        with pytest.raises(FileNotFoundError):
            await fs._info(f"s3://{uuid.uuid4()}")

        await fs._pipe_file(file, b"a")
        bucket, key, version_id = fs.parse_path(dir_)
        fs.invalidate_cache()
        info = await fs._info(dir_)
        fs.invalidate_cache()

        assert info.name == fs._strip_protocol(dir_)
        assert info.bucket == bucket
        assert info.key == key.rstrip("/")
        assert info.last_modified is None
        assert info.size == 0
        assert info.etag is None
        assert info.type == S3ObjectType.S3_OBJECT_TYPE_DIRECTORY
        assert info.storage_class == S3StorageClass.S3_STORAGE_CLASS_DIRECTORY
        assert info.version_id == version_id

    @pytest.mark.asyncio
    async def test_info_file(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_info_file"
        )
        file = f"{dir_}/{uuid.uuid4()}"

        fs.invalidate_cache()
        with pytest.raises(FileNotFoundError):
            await fs._info(file)

        now = datetime.now(UTC)
        await fs._pipe_file(file, b"a")
        bucket, key, version_id = fs.parse_path(file)
        fs.invalidate_cache()
        info = await fs._info(file)
        fs.invalidate_cache()
        ls_info = (await fs._ls(file, detail=True))[0]

        assert info == ls_info
        assert info.name == fs._strip_protocol(file)
        assert info.bucket == bucket
        assert info.key == key
        assert info.last_modified >= now
        assert info.size == 1
        assert info.etag is not None
        assert info.type == S3ObjectType.S3_OBJECT_TYPE_FILE
        assert info.storage_class == S3StorageClass.S3_STORAGE_CLASS_STANDARD
        assert info.version_id == version_id

    @pytest.mark.asyncio
    async def test_find(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_find"
        )
        for i in range(5):
            await fs._pipe_file(f"{dir_}/prefix/test_{i}", bytes(i))
        await fs._touch(f"{dir_}/prefix2")

        result = await fs._find(f"{dir_}/prefix")
        assert len(result) == 5

        result = await fs._find(f"{dir_}/prefix/")
        assert len(result) == 5

        result = await fs._find(dir_, prefix="prefix")
        assert len(result) == 6

        result = await fs._find(f"{dir_}/prefix/test_")
        assert len(result) == 0

        result = await fs._find(f"{dir_}/prefix", prefix="test_")
        assert len(result) == 5

        result = await fs._find(f"{dir_}/prefix/", prefix="test_")
        assert len(result) == 5

        test_1 = await fs._find(f"{dir_}/prefix/test_1")
        assert len(test_1) == 1
        assert test_1[0] == fs._strip_protocol(f"{dir_}/prefix/test_1")

        test_1_detail = await fs._find(f"{dir_}/prefix/test_1", detail=True)
        assert len(test_1_detail) == 1
        assert test_1_detail[
            fs._strip_protocol(f"{dir_}/prefix/test_1")
        ].name == fs._strip_protocol(f"{dir_}/prefix/test_1")
        assert test_1_detail[fs._strip_protocol(f"{dir_}/prefix/test_1")].size == 1

    @pytest.mark.asyncio
    async def test_find_maxdepth(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_find_maxdepth"
        )
        # Create files at different depths
        await fs._touch(f"{dir_}/file0.txt")
        await fs._touch(f"{dir_}/level1/file1.txt")
        await fs._touch(f"{dir_}/level1/level2/file2.txt")
        await fs._touch(f"{dir_}/level1/level2/level3/file3.txt")

        # maxdepth must be at least 1, as in fsspec
        with pytest.raises(ValueError, match="maxdepth must be at least 1"):
            await fs._find(dir_, maxdepth=0)

        # Test maxdepth=1 (only files in the root)
        result = await fs._find(dir_, maxdepth=1)
        assert len(result) == 1
        assert fs._strip_protocol(f"{dir_}/file0.txt") in result

        # Test maxdepth=2 (files in root and level1)
        result = await fs._find(dir_, maxdepth=2)
        assert len(result) == 2
        assert fs._strip_protocol(f"{dir_}/file0.txt") in result
        assert fs._strip_protocol(f"{dir_}/level1/file1.txt") in result

        # Test maxdepth=3 (files in root, level1, and level2)
        result = await fs._find(dir_, maxdepth=3)
        assert len(result) == 3
        assert fs._strip_protocol(f"{dir_}/level1/level2/file2.txt") in result

        # Test no maxdepth (all files)
        result = await fs._find(dir_)
        assert len(result) == 4

    @pytest.mark.asyncio
    async def test_find_withdirs(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_find_withdirs"
        )
        # Create directory structure with files
        await fs._touch(f"{dir_}/file1.txt")
        await fs._touch(f"{dir_}/subdir1/file2.txt")
        await fs._touch(f"{dir_}/subdir1/subdir2/file3.txt")
        await fs._touch(f"{dir_}/subdir3/file4.txt")

        # Test default behavior (withdirs=False)
        result = await fs._find(dir_)
        assert len(result) == 4  # Only files
        for r in result:
            assert r.endswith(".txt")

        # Test withdirs=True
        result = await fs._find(dir_, withdirs=True)
        assert len(result) > 4  # Files and directories

        # Verify directories are included
        dirs = [r for r in result if not r.endswith(".txt")]
        assert len(dirs) > 0
        assert any("subdir1" in d for d in dirs)
        assert any("subdir2" in d for d in dirs)
        assert any("subdir3" in d for d in dirs)

        # Test withdirs=False explicitly
        result = await fs._find(dir_, withdirs=False)
        assert len(result) == 4  # Only files

    def test_du(self):
        # TODO
        pass

    @pytest.mark.asyncio
    async def test_glob(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_glob"
        )
        path = f"{dir_}/nested/test_{uuid.uuid4()}"
        await fs._touch(path)

        assert fs._strip_protocol(path) not in fs.glob(f"{dir_}/")
        assert fs._strip_protocol(path) not in fs.glob(f"{dir_}/*")
        assert fs._strip_protocol(path) not in fs.glob(f"{dir_}/nested")
        assert fs._strip_protocol(path) not in fs.glob(f"{dir_}/nested/")
        assert fs._strip_protocol(path) in fs.glob(f"{dir_}/nested/*")
        assert fs._strip_protocol(path) in fs.glob(f"{dir_}/nested/test_*")
        assert fs._strip_protocol(path) in fs.glob(f"{dir_}/*/*")

        with pytest.raises(ValueError):  # noqa: PT011
            fs.glob("*")

    @pytest.mark.asyncio
    async def test_exists_bucket(self, fs):
        assert await fs._exists("s3://")
        assert await fs._exists("s3:///")

        path = f"s3://{ENV.s3_staging_bucket}"
        assert await fs._exists(path)

        not_exists_path = f"s3://{uuid.uuid4()}"
        assert not await fs._exists(not_exists_path)

    @pytest.mark.asyncio
    async def test_exists_object(self, fs):
        path = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}"
        assert await fs._exists(path)

        not_exists_path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_exists/{uuid.uuid4()}"
        )
        assert not await fs._exists(not_exists_path)

    @pytest.mark.asyncio
    async def test_rm_file(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_rm_file"
        )
        file = f"{dir_}/{uuid.uuid4()}"
        await fs._touch(file)
        await fs._rm_file(file)

        assert not await fs._exists(file)
        assert not await fs._exists(dir_)

    @pytest.mark.asyncio
    async def test_rm(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_rm"
        )
        file = f"{dir_}/{uuid.uuid4()}"
        await fs._touch(file)
        await fs._rm(file)

        assert not await fs._exists(file)
        assert not await fs._exists(dir_)

    @pytest.mark.asyncio
    async def test_rm_recursive(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_rm_recursive"
        )

        files = [f"{dir_}/{uuid.uuid4()}" for _ in range(10)]
        for f in files:
            await fs._touch(f)

        await fs._rm(dir_)
        for f in files:
            assert await fs._exists(f)
        assert await fs._exists(dir_)

        await fs._rm(dir_, recursive=True)
        for f in files:
            assert not await fs._exists(f)
        assert not await fs._exists(dir_)

    @pytest.mark.asyncio
    async def test_touch(self, fs):
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_touch/{uuid.uuid4()}"
        )
        assert not await fs._exists(path)
        await fs._touch(path)
        assert await fs._exists(path)
        info = await fs._info(path)
        assert info.size == 0

        with fs.open(path, "wb") as f:
            f.write(b"data")
        info = await fs._info(path, refresh=True)
        assert info.size == 4
        await fs._touch(path, truncate=True)
        info = await fs._info(path, refresh=True)
        assert info.size == 0

        with fs.open(path, "wb") as f:
            f.write(b"data")
        info = await fs._info(path, refresh=True)
        assert info.size == 4
        with pytest.raises(ValueError, match="Cannot touch"):
            await fs._touch(path, truncate=False)
        info = await fs._info(path, refresh=True)
        assert info.size == 4

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("base", "exp"),
        [
            (1, 2**10),
            (1, 2**20),
        ],
    )
    async def test_pipe_cat(self, fs, base, exp):
        data = b"a" * (base * exp)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_pipe_cat/{uuid.uuid4()}"
        )
        await fs._pipe_file(path, data)
        assert await fs._cat_file(path) == data

    @pytest.mark.asyncio
    async def test_cat_ranges(self, fs):
        data = b"1234567890abcdefghijklmnopqrstuvwxyz"
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_cat_ranges/{uuid.uuid4()}"
        )
        await fs._pipe_file(path, data)

        assert await fs._cat_file(path) == data
        assert await fs._cat_file(path, start=5) == data[5:]
        assert await fs._cat_file(path, end=5) == data[:5]
        assert await fs._cat_file(path, start=1, end=-1) == data[1:-1]
        assert await fs._cat_file(path, start=-5) == data[-5:]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("base", "exp"),
        [
            (1, 2**10),
            (1, 2**20),
        ],
    )
    async def test_put(self, fs, base, exp):
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            data = b"a" * (base * exp)
            tmp.write(data)
            tmp.flush()

            rpath = (
                f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
                f"filesystem/test_async_put/{uuid.uuid4()}"
            )
            await fs._put_file(lpath=tmp.name, rpath=rpath)
            tmp.seek(0)
            assert await fs._cat_file(rpath) == tmp.read()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("base", "exp"),
        [
            (1, 2**10),
            (1, 2**20),
        ],
    )
    async def test_put_with_callback(self, fs, base, exp):
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            data = b"a" * (base * exp)
            tmp.write(data)
            tmp.flush()

            rpath = (
                f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
                f"filesystem/test_async_put_with_callback/{uuid.uuid4()}"
            )
            callback = Callback()
            await fs._put_file(lpath=tmp.name, rpath=rpath, callback=callback)
            tmp.seek(0)
            assert await fs._cat_file(rpath) == tmp.read()
            assert callback.size == os.stat(tmp.name).st_size
            assert callback.value == callback.size

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("base", "exp"),
        [
            (1, 2**10),
            (1, 2**20),
        ],
    )
    async def test_upload_cp_file(self, fs, base, exp):
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            data = b"a" * (base * exp)
            tmp.write(data)
            tmp.flush()

            rpath = (
                f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
                f"filesystem/test_async_upload_cp_file/{uuid.uuid4()}"
            )
            await fs._put_file(lpath=tmp.name, rpath=rpath)

            rpath_copy = (
                f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
                f"filesystem/test_async_upload_cp_file_copy/{uuid.uuid4()}"
            )
            await fs._cp_file(path1=rpath, path2=rpath_copy)
            tmp.seek(0)
            assert await fs._cat_file(rpath_copy) == tmp.read()
            assert await fs._cat_file(rpath_copy) == await fs._cat_file(rpath)

    @pytest.mark.asyncio
    async def test_move(self, fs):
        path1 = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_move/{uuid.uuid4()}"
        )
        path2 = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_move/{uuid.uuid4()}"
        )
        data = b"a"
        await fs._pipe_file(path1, data)
        fs.mv(path1, path2)
        assert await fs._cat_file(path2) == data
        assert not await fs._exists(path1)

    @pytest.mark.asyncio
    async def test_move_recursive(self, fs):
        # GH-974: the directory entries used to make mv() fail, after copying
        # the files.
        base = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_move_recursive/{uuid.uuid4()}"
        )
        await fs._pipe_file(f"{base}/src/a", b"a")
        await fs._pipe_file(f"{base}/src/sub/b", b"b")
        fs.mv(f"{base}/src", f"{base}/dst", recursive=True)
        assert await fs._cat_file(f"{base}/dst/a") == b"a"
        assert await fs._cat_file(f"{base}/dst/sub/b") == b"b"
        assert not await fs._exists(f"{base}/src/a")
        assert not await fs._exists(f"{base}/src/sub/b")

    @pytest.mark.asyncio
    async def test_get_file(self, fs):
        with tempfile.TemporaryDirectory() as tmp:
            rpath = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}"
            lpath = Path(f"{tmp}/{uuid.uuid4()}")
            callback = Callback()
            await fs._get_file(rpath=rpath, lpath=str(lpath), callback=callback)

            assert lpath.open("rb").read() == await fs._cat_file(rpath)
            assert callback.size == os.stat(lpath).st_size
            assert callback.value == callback.size

    def test_open_returns_aio_s3_file(self, fs):
        path = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}"
        with fs.open(path, "rb") as f:
            assert isinstance(f, AioS3File)
            data = f.read()
        assert data == b"0123456789"

    def test_checksum(self, fs):
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_checksum/{uuid.uuid4()}"
        )
        bucket, key, _ = fs.parse_path(path)

        fs.pipe_file(path, b"foo")
        checksum = fs.checksum(path)
        fs.ls(path)  # caching
        fs._sync_fs._put_object(bucket=bucket, key=key, body=b"bar")
        assert checksum == fs.checksum(path)
        assert checksum != fs.checksum(path, refresh=True)

        fs.pipe_file(path, b"foo")
        checksum = fs.checksum(path)
        fs.ls(path)  # caching
        fs._sync_fs._delete_object(bucket, key)
        assert checksum == fs.checksum(path)
        with pytest.raises(FileNotFoundError):
            fs.checksum(path, refresh=True)

    def test_sign(self, fs):
        path = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}"
        requested = time.time()
        time.sleep(1)
        url = fs.sign(path, expiration=100)
        parsed = urllib.parse.urlparse(url)
        query = urllib.parse.parse_qs(parsed.query)
        expires = int(query["Expires"][0])
        with urllib.request.urlopen(url) as r:
            data = r.read()

        assert "https" in url
        assert requested + 100 < expires
        assert data == b"0123456789"

    @pytest.mark.asyncio
    async def test_mkdir_and_rmdir(self, fs):
        # The bucket already exists.
        with pytest.raises(FileExistsError):
            await fs._mkdir(f"s3://{ENV.s3_staging_bucket}")
        await fs._makedirs(f"s3://{ENV.s3_staging_bucket}", exist_ok=True)

        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_rmdir/{uuid.uuid4()}"
        )
        await fs._pipe_file(path, b"data")
        # Only bucket paths can be removed.
        with pytest.raises(FileExistsError):
            await fs._rmdir(path)
        with pytest.raises(FileNotFoundError):
            fs.rmdir(f"{path}/nonexistent")

    @pytest.mark.asyncio
    async def test_metadata_and_tags(self, fs):
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_metadata/{uuid.uuid4()}"
        )
        await fs._pipe_file(path, b"data")

        assert fs.metadata(path) == {}
        fs.setxattr(path, attr1="value1")
        assert fs.metadata(path) == {"attr1": "value1"}
        assert fs.getxattr(path, "attr1") == "value1"
        assert fs.getxattr(path, "missing") is None

        assert fs.get_tags(path) == {}
        fs.put_tags(path, {"tag1": "value1"})
        assert fs.get_tags(path) == {"tag1": "value1"}

    def test_pandas_read_csv(self):
        import pandas

        df = pandas.read_csv(
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}",
            header=None,
            names=["col"],
        )
        assert [(row["col"],) for _, row in df.iterrows()] == [(123456789,)]

    @pytest.mark.parametrize(
        "line_count",
        [
            1 * 2**20,
        ],
    )
    def test_pandas_write_csv(self, line_count):
        import pandas

        with tempfile.NamedTemporaryFile("w+t") as tmp:
            tmp.write("col1")
            tmp.write("\n")
            for _ in range(line_count):
                tmp.write("a")
                tmp.write("\n")
            tmp.flush()

            tmp.seek(0)
            df = pandas.read_csv(tmp.name)
            path = (
                f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
                f"filesystem/test_async_pandas_write_csv/{uuid.uuid4()}.csv"
            )
            df.to_csv(path, index=False)

            actual = pandas.read_csv(path)
            pandas.testing.assert_frame_equal(actual, df)

    def test_sync_wrappers(self, fs):
        """Verify that mirror_sync_methods generates working sync wrappers."""
        actual = fs.ls(f"s3://{ENV.s3_staging_bucket}")
        assert isinstance(actual, list)
        assert len(actual) > 0

        assert fs.exists(f"s3://{ENV.s3_staging_bucket}")

    def test_invalidate_cache(self, fs):
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_invalidate_cache/{uuid.uuid4()}"
        )
        fs.pipe_file(path, b"data")
        fs.info(path)

        # Cache should be populated
        fs.invalidate_cache(path)
        # Should not raise after cache invalidation
        info = fs.info(path)
        assert info.size == 4


class TestAioS3File:
    def test_open_max_workers(self):
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        with fs.open("s3://bucket/key", "wb", max_workers=2) as f:
            assert isinstance(f, AioS3File)
            assert f.max_workers == 2

    @pytest.mark.parametrize("asynchronous", [False, True])
    @pytest.mark.asyncio
    async def test_open_parallel_requests(self, asynchronous):
        # GH-954: max_workers bounds the parallel part uploads and range
        # reads. GH-977: a filesystem created with asynchronous=True has no
        # event loop of its own and used to fail to run them.
        fs = AioS3FileSystem(
            connection=mock.MagicMock(), asynchronous=asynchronous, skip_instance_cache=True
        )
        block_size = S3FileSystem.MULTIPART_UPLOAD_MIN_PART_SIZE
        size = block_size * 4
        condition = threading.Condition()
        state = {"active": 0, "peak": 0}

        def track(result):
            def call(**kwargs):
                with condition:
                    state["active"] += 1
                    state["peak"] = max(state["peak"], state["active"])
                    condition.notify_all()
                    # Hold the call until another one overlaps it, then
                    # briefly for a third one, which only an executor
                    # without the limit runs.
                    condition.wait_for(lambda: state["active"] >= 2, timeout=5)
                    condition.wait_for(lambda: state["active"] >= 3, timeout=0.2)
                with condition:
                    state["active"] -= 1
                return result(**kwargs)

            return mock.MagicMock(side_effect=call)

        sync_fs = fs._sync_fs
        sync_fs._create_multipart_upload = mock.MagicMock(
            return_value=SimpleNamespace(upload_id="uploadid")
        )
        sync_fs._upload_part = track(
            lambda **kw: S3MultipartUploadPart(kw["part_number"], {"ETag": '"e"'})
        )
        sync_fs._complete_multipart_upload = mock.MagicMock()
        sync_fs._get_object = track(
            lambda **kw: (kw["ranges"][0], b"a" * (kw["ranges"][1] - kw["ranges"][0]))
        )
        sync_fs.info = mock.MagicMock(
            return_value=S3Object(
                init={"Key": "key"},
                type=S3ObjectType.S3_OBJECT_TYPE_FILE,
                bucket="bucket",
                key="key",
            )
        )
        sync_fs.info.return_value.size = size

        def write():
            with fs.open("s3://bucket/key", "wb", block_size=block_size, max_workers=2) as f:
                f.write(b"a" * size)

        await asyncio.to_thread(write)
        assert sync_fs._upload_part.call_count == 4
        assert state["peak"] == 2

        def read():
            with fs.open(
                "s3://bucket/key", "rb", block_size=block_size, cache_type="none", max_workers=2
            ) as f:
                return f.read()

        state["peak"] = 0
        assert await asyncio.to_thread(read) == b"a" * size
        assert sync_fs._get_object.call_count == 4
        assert state["peak"] == 2

    def test_open_invalid_max_workers(self):
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        with pytest.raises(ValueError, match="max_workers must be greater than 0"):
            fs.open("s3://bucket/key", "wb", max_workers=0)

    def test_open_version_id(self):
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs.info = mock.MagicMock(
            return_value=S3Object(
                init={"Key": "key"},
                type=S3ObjectType.S3_OBJECT_TYPE_FILE,
                bucket="bucket",
                key="key",
            )
        )
        fs._sync_fs.info.return_value.size = 4

        with fs.open("s3://bucket/key", "rb", version_id="v1") as f:
            assert isinstance(f, AioS3File)
            assert f.version_id == "v1"
            assert f.size == 4
        fs._sync_fs.info.assert_called_once_with("bucket/key?versionId=v1", version_id="v1")

    @pytest.mark.parametrize(
        ("objects", "target"),
        [
            ([(0, b"")], b""),
            ([(0, b"foo")], b"foo"),
            ([(0, b"foo"), (1, b"bar")], b"foobar"),
            ([(1, b"foo"), (0, b"bar")], b"barfoo"),
            ([(1, b""), (0, b"bar")], b"bar"),
            ([(1, b"foo"), (0, b"")], b"foo"),
            ([(2, b"foo"), (1, b"bar"), (3, b"baz")], b"barfoobaz"),
        ],
    )
    def test_merge_objects(self, objects, target):
        assert S3File._merge_objects(objects) == target

    @pytest.mark.parametrize(
        ("start", "end", "max_workers", "worker_block_size", "ranges"),
        [
            (42, 1337, 1, 999, [(42, 1337)]),  # single worker
            (42, 1337, 2, 999, [(42, 42 + 999), (42 + 999, 1337)]),  # more workers
            (
                42,
                1337,
                2,
                333,
                [
                    (42, 42 + 333),
                    (42 + 333, 42 + 666),
                    (42 + 666, 42 + 999),
                    (42 + 999, 1337),
                ],
            ),
            (42, 1337, 2, 1295, [(42, 1337)]),  # single block
            (42, 1337, 2, 1296, [(42, 1337)]),  # single block
            (42, 1337, 2, 1294, [(42, 1336), (1336, 1337)]),  # single block too small
        ],
    )
    def test_get_ranges(self, start, end, max_workers, worker_block_size, ranges):
        assert (
            S3File._get_ranges(
                start, end, max_workers=max_workers, worker_block_size=worker_block_size
            )
            == ranges
        )

    def test_format_ranges(self):
        assert S3File._format_ranges((0, 100)) == "bytes=0-99"
