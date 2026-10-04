import asyncio
import contextlib
import gzip
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
from botocore.stub import Stubber
from fsspec import Callback

from pyathena.filesystem.s3 import S3File, S3FileSystem
from pyathena.filesystem.s3_async import AioS3File, AioS3FileSystem
from pyathena.filesystem.s3_core import S3Core
from pyathena.filesystem.s3_object import (
    S3MultipartUpload,
    S3MultipartUploadPart,
    S3Object,
    S3ObjectType,
    S3StorageClass,
)
from pyathena.filesystem.s3_path import S3Path
from pyathena.util import RetryConfig
from tests import ENV
from tests.pyathena.conftest import connect
from tests.pyathena.util import (
    MULTIPART_COPY_BLOCK_SIZE,
    MULTIPART_COPY_KWARGS,
    stub_multipart_copy,
)


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
            AioS3FileSystem.parse_path("s3a://bucket?")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            AioS3FileSystem.parse_path("s3a://bucket?foo=bar")

        # GH-979: a "?" in a key that does not start a trailing version ID
        # query is part of the key.
        for path in ("s3://bucket/path/to/obj?foo=bar", "s3a://bucket/path/to/obj?foo=bar"):
            assert AioS3FileSystem.parse_path(path) == ("bucket", "path/to/obj?foo=bar", None)

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
        sync_fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "dst", "UploadId": "uploadid"}
            )
        )
        sync_fs.core.upload_part_copy = mock.MagicMock(
            side_effect=lambda **kw: SimpleNamespace(etag='"e"', part_number=kw["part_number"])
        )
        sync_fs.core.complete_multipart_upload = mock.MagicMock()
        # The HeadObject of the source.
        sync_fs._call = sync_fs._core.call = mock.MagicMock(
            return_value={"ContentLength": 5 * 2**30 + 2**20}
        )

        await fs._copy_object_with_multipart_upload(
            S3Path("bucket", "src"),
            S3Path("bucket", "dst"),
            # Copy without reading the metadata, tags and annotations of the
            # source (GH-973).
            MetadataDirective="REPLACE",
            TaggingDirective="REPLACE",
            AnnotationDirective="EXCLUDE",
        )

        parts = sorted(
            (c.kwargs["part_number"], c.kwargs["range_"])
            for c in sync_fs.core.upload_part_copy.call_args_list
        )
        assert parts == [
            (1, (0, 5 * 2**29 + 2**19)),
            (2, (5 * 2**29 + 2**19, 5 * 2**30 + 2**20)),
        ]

    @staticmethod
    async def _multipart_copy(fs=None, **kwargs):
        # max_workers=1 runs the stubbed requests in a deterministic order.
        fs = fs or AioS3FileSystem(
            key="dummy",
            secret="dummy",
            region_name="us-east-1",
            max_workers=1,
            skip_instance_cache=True,
        )
        with Stubber(fs._sync_fs._client) as stubber:
            stub_multipart_copy(stubber, **kwargs)
            try:
                await fs._copy_object_with_multipart_upload(
                    S3Path("bucket", "src"),
                    S3Path("bucket", "dst"),
                    block_size=MULTIPART_COPY_BLOCK_SIZE,
                    **MULTIPART_COPY_KWARGS,
                )
            finally:
                stubber.assert_no_pending_responses()

    @pytest.mark.parametrize("size", [0, 10])
    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_small_head_object_size(self, size):
        # GH-973: see
        # TestS3FileSystem.test_copy_object_with_multipart_upload_small_head_object_size.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        sync_fs = fs._sync_fs
        sync_fs._call = sync_fs._core.call = mock.MagicMock(
            return_value={"ContentLength": size, "VersionId": "v1"}
        )
        sync_fs.core.copy_object = mock.MagicMock()
        sync_fs.core.create_multipart_upload = mock.MagicMock()

        await fs._copy_object_with_multipart_upload(
            S3Path("bucket", "src"),
            S3Path("bucket", "dst"),
            ContentType="text/csv",
            RequestPayer="requester",
        )

        sync_fs.core.copy_object.assert_called_once_with(
            S3Path("bucket", "src", "v1"),
            S3Path("bucket", "dst"),
            ContentType="text/csv",
            RequestPayer="requester",
        )
        sync_fs.core.create_multipart_upload.assert_not_called()
        # Only HeadObject; the tags are not read for the multipart upload.
        assert sync_fs._call.call_count == 1

    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_copies_source(self):
        # GH-973: the same requests as S3FileSystem; see
        # TestS3FileSystem.test_copy_object_with_multipart_upload_copies_source.
        await self._multipart_copy()

    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_failed_listing(self):
        # GH-973: nothing is written when the annotations cannot be listed.
        with pytest.raises(PermissionError):
            await self._multipart_copy(fail_list=True)

    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_failed_part(self):
        # GH-973: a failed part copy aborts the upload, as in S3FileSystem.
        with pytest.raises(OSError, match="part failed"):
            await self._multipart_copy(fail_part=True)

    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_failed_annotation(self):
        # GH-973: a failed annotation copy is raised; the completed
        # destination is neither aborted nor deleted, and no other
        # annotation is copied.
        fs = AioS3FileSystem(
            key="dummy",
            secret="dummy",
            region_name="us-east-1",
            max_workers=1,
            skip_instance_cache=True,
        )
        sync_fs = fs._sync_fs
        with (
            mock.patch.object(
                sync_fs.core, "copy_object_annotation", wraps=sync_fs.core.copy_object_annotation
            ) as copy_annotation,
            pytest.raises(PermissionError),
        ):
            await self._multipart_copy(fs, fail_annotation=True)
        assert [c.args[0] for c in copy_annotation.call_args_list] == ["a1"]

    @pytest.mark.asyncio
    async def test_cp_file_failed_multipart_copy_invalidates_cache(self):
        # GH-973: see TestS3FileSystem.test_cp_file_failed_multipart_copy_invalidates_cache.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._info = mock.AsyncMock(
            return_value=S3Object(
                init={"ContentLength": S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE + 1},
                type=S3ObjectType.S3_OBJECT_TYPE_FILE,
                bucket="bucket",
                key="src",
            )
        )
        fs._copy_object_with_multipart_upload = mock.AsyncMock(side_effect=PermissionError)
        fs._sync_fs.dircache["bucket/dst"] = []

        with pytest.raises(PermissionError):
            await fs._cp_file("s3://bucket/src", "s3://bucket/dst")
        assert "bucket/dst" not in fs._sync_fs.dircache

    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_waits_for_running_parts(self):
        # GH-973: the abort waits for the part copies that are running when
        # one fails, and no part starts after the failure.
        fs = AioS3FileSystem(connection=mock.MagicMock(), max_workers=2, skip_instance_cache=True)
        sync_fs = fs._sync_fs
        sync_fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "dst", "UploadId": "uploadid"}
            )
        )
        events = []
        failed = threading.Event()

        def upload_part_copy(**kw):
            part_number = kw["part_number"]
            events.append(f"start {part_number}")
            if part_number == 1:
                failed.wait(5)
                time.sleep(0.05)
                events.append("end 1")
                return SimpleNamespace(etag='"e"', part_number=part_number)
            failed.set()
            raise OSError("part failed")

        sync_fs.core.upload_part_copy = mock.MagicMock(side_effect=upload_part_copy)
        sync_fs.core.complete_multipart_upload = mock.MagicMock()
        # The HeadObject of the source, with 3 parts of the default block size.
        size = 3 * S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE
        sync_fs._call = sync_fs._core.call = mock.MagicMock(return_value={"ContentLength": size})
        sync_fs._abort_multipart_upload = mock.MagicMock(
            side_effect=lambda *args: events.append("abort")
        )

        with pytest.raises(OSError, match="part failed"):
            await fs._copy_object_with_multipart_upload(
                S3Path("bucket", "src"),
                S3Path("bucket", "dst"),
                MetadataDirective="REPLACE",
                TaggingDirective="REPLACE",
                AnnotationDirective="EXCLUDE",
            )

        # Part 3 waits for a worker and is not started after the failure.
        assert sorted(events[:2]) == ["start 1", "start 2"]
        assert events[2:] == ["end 1", "abort"]
        sync_fs.core.complete_multipart_upload.assert_not_called()

    @pytest.mark.parametrize("cancellations", [1, 2])
    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_cancelled(self, cancellations):
        # GH-1046: a cancellation waits for the part copies that are running,
        # aborts the upload, and is re-raised, as S3FileSystem does on an
        # interrupt. A repeated cancellation returns without stopping the
        # cleanup.
        fs = AioS3FileSystem(connection=mock.MagicMock(), max_workers=2, skip_instance_cache=True)
        sync_fs = fs._sync_fs
        sync_fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "dst", "UploadId": "uploadid"}
            )
        )
        events = []
        lock = threading.Lock()
        started = threading.Semaphore(0)
        release = threading.Event()
        aborted = threading.Event()

        def upload_part_copy(**kw):
            part_number = kw["part_number"]
            with lock:
                events.append(f"start {part_number}")
            started.release()
            # The finally blocks of the test always release it.
            release.wait()
            with lock:
                events.append(f"end {part_number}")
            return SimpleNamespace(etag='"e"', part_number=part_number)

        def abort_multipart_upload(*args):
            events.append("abort")
            aborted.set()

        sync_fs.core.upload_part_copy = mock.MagicMock(side_effect=upload_part_copy)
        sync_fs.core.complete_multipart_upload = mock.MagicMock()
        # The HeadObject of the source, with 3 parts of the default block size.
        size = 3 * S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE
        sync_fs._call = sync_fs._core.call = mock.MagicMock(return_value={"ContentLength": size})
        sync_fs._abort_multipart_upload = mock.MagicMock(side_effect=abort_multipart_upload)

        task = asyncio.ensure_future(
            fs._copy_object_with_multipart_upload(
                S3Path("bucket", "src"),
                S3Path("bucket", "dst"),
                MetadataDirective="REPLACE",
                TaggingDirective="REPLACE",
                AnnotationDirective="EXCLUDE",
            )
        )
        try:
            for _ in range(2):
                assert await asyncio.to_thread(started.acquire, timeout=5)
            # The running parts are held until the copy has been cancelled.
            for _ in range(cancellations):
                task.cancel()
                # Lets the copy enter its cleanup.
                await asyncio.sleep(0)
            if cancellations > 1:
                # The repeated cancellation returns while the parts still run.
                assert task.done()
                assert events[2:] == []
            else:
                assert not task.done()
        except BaseException:
            task.cancel()
            raise
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await asyncio.to_thread(aborted.wait, 5)

        # Part 3 waits for a worker and is not started after the cancellation.
        assert sorted(events[:2]) == ["start 1", "start 2"]
        assert sorted(events[2:4]) == ["end 1", "end 2"]
        assert events[4:] == ["abort"]
        sync_fs.core.complete_multipart_upload.assert_not_called()

    @pytest.mark.parametrize("completion_fails", [False, True])
    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_cancelled_completion(self, completion_fails):
        # GH-1046: a cancellation during CompleteMultipartUpload waits for it,
        # aborts the upload only if it failed, and is re-raised.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        sync_fs = fs._sync_fs
        sync_fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "dst", "UploadId": "uploadid"}
            )
        )
        events = []
        started = threading.Event()
        release = threading.Event()

        def complete_multipart_upload(*args, **kw):
            started.set()
            # The finally blocks of the test always release it.
            release.wait()
            events.append("complete")
            if completion_fails:
                raise OSError("completion failed")
            return SimpleNamespace()

        sync_fs.core.upload_part_copy = mock.MagicMock(
            side_effect=lambda **kw: SimpleNamespace(etag='"e"', part_number=kw["part_number"])
        )
        sync_fs.core.complete_multipart_upload = mock.MagicMock(
            side_effect=complete_multipart_upload
        )
        # The HeadObject of the source, with 2 parts of the default block size.
        size = 2 * S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE
        sync_fs._call = sync_fs._core.call = mock.MagicMock(return_value={"ContentLength": size})
        sync_fs._abort_multipart_upload = mock.MagicMock(
            side_effect=lambda *args: events.append("abort")
        )

        task = asyncio.ensure_future(
            fs._copy_object_with_multipart_upload(
                S3Path("bucket", "src"),
                S3Path("bucket", "dst"),
                MetadataDirective="REPLACE",
                TaggingDirective="REPLACE",
                AnnotationDirective="EXCLUDE",
            )
        )
        try:
            assert await asyncio.to_thread(started.wait, 5)
            task.cancel()
            # Gives the cleanup time to abort early or to return, which it
            # must not do while the completion is held.
            await asyncio.sleep(0.1)
            assert not task.done()
            assert events == []
        except BaseException:
            task.cancel()
            raise
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert events == (["complete", "abort"] if completion_fails else ["complete"])

    @pytest.mark.parametrize("creation_fails", [False, True])
    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_cancelled_creation(self, creation_fails):
        # A cancellation during CreateMultipartUpload waits for it, aborts
        # the upload that it created before any part is copied, and is
        # re-raised. The created upload used to be left incomplete.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        sync_fs = fs._sync_fs
        events = []
        started = threading.Event()
        release = threading.Event()

        def create_multipart_upload(*args, **kw):
            started.set()
            # The finally blocks of the test always release it.
            release.wait()
            events.append("create")
            if creation_fails:
                raise OSError("creation failed")
            return S3MultipartUpload({"Bucket": "bucket", "Key": "dst", "UploadId": "uploadid"})

        sync_fs.core.create_multipart_upload = mock.MagicMock(side_effect=create_multipart_upload)
        sync_fs.core.upload_part_copy = mock.MagicMock()
        # The HeadObject of the source, with 2 parts of the default block size.
        size = 2 * S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE
        sync_fs._call = sync_fs._core.call = mock.MagicMock(return_value={"ContentLength": size})
        sync_fs._abort_multipart_upload = mock.MagicMock(
            side_effect=lambda upload, params: events.append(("abort", upload.upload_id))
        )

        task = asyncio.ensure_future(
            fs._copy_object_with_multipart_upload(
                S3Path("bucket", "src"),
                S3Path("bucket", "dst"),
                MetadataDirective="REPLACE",
                TaggingDirective="REPLACE",
                AnnotationDirective="EXCLUDE",
            )
        )
        try:
            assert await asyncio.to_thread(started.wait, 5)
            task.cancel()
            # Gives the cleanup time to return, which it must not do while
            # the creation is held.
            await asyncio.sleep(0.1)
            assert not task.done()
            assert events == []
        except BaseException:
            task.cancel()
            raise
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert events == (["create"] if creation_fails else ["create", ("abort", "uploadid")])
        sync_fs.core.upload_part_copy.assert_not_called()

    @pytest.mark.parametrize(
        "block_size",
        [
            S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE - 1,
            S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE + 1,
        ],
    )
    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_invalid_block_size(self, block_size):
        # GH-926: the message states the accepted range.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs._call = fs._sync_fs._core.call = mock.MagicMock()

        with pytest.raises(
            ValueError,
            match=r"between 5 MiB \(5242880 bytes\) and 5 GiB \(5368709120 bytes\), inclusive",
        ):
            await fs._copy_object_with_multipart_upload(
                S3Path("bucket", "src"), S3Path("bucket", "dst"), block_size=block_size
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
        fs._sync_fs._call = fs._sync_fs._core.call = mock.MagicMock()
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
        fs._sync_fs._core = S3Core(
            boto3.client(
                "s3",
                region_name="us-east-1",
                aws_access_key_id="dummy",
                aws_secret_access_key="dummy",
            )
        )
        fs._sync_fs.exists = mock.MagicMock(return_value=False)
        fs._sync_fs._call = fs._sync_fs._core.call = mock.MagicMock(return_value={"ETag": '"e"'})
        local = tmp_path / "local"
        local.write_bytes(b"a")

        fs.put_file(str(local), "s3://bucket/key", mode=mode)

        (call,) = fs._sync_fs._call.call_args_list
        assert "mode" not in call.kwargs
        assert call.kwargs.get("IfNoneMatch") == ("*" if mode == "create" else None)

    @pytest.mark.parametrize(
        ("path", "compression"),
        [
            ("s3://bucket/key", "gzip"),
            # Inferred from the path without the trailing slash, as open()
            # does.
            ("s3://bucket/key.gz/", "infer"),
        ],
    )
    @pytest.mark.parametrize("intrans", [False, True])
    def test_pipe_file_compression(self, path, compression, intrans):
        # GH-1037: the value is compressed before it is uploaded, also in a
        # transaction of this filesystem.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        put_object = fs._sync_fs._put_object = mock.MagicMock()

        with fs.transaction if intrans else contextlib.nullcontext():
            fs.pipe_file(path, b"data", compression=compression)

        ((_, kwargs),) = put_object.call_args_list
        assert "compression" not in kwargs
        assert gzip.decompress(kwargs["body"]) == b"data"

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

    @pytest.mark.parametrize("error", [RuntimeError, PermissionError])
    def test_transaction_put_file_failed_write(self, tmp_path, error):
        # GH-1014: in a transaction, a failed write or a local file that
        # cannot be read does not replace the object with the data written
        # so far, or with an empty one, when the transaction commits.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        put_object = fs._sync_fs._put_object = mock.MagicMock()
        local = tmp_path / "local"
        local.write_bytes(b"a")
        failing = tmp_path / "failing"
        failing.write_bytes(b"b")
        if error is PermissionError:
            if os.geteuid() == 0:
                pytest.skip("root can read a file without read permission.")
            failing.chmod(0)

        with fs.transaction:
            with (
                mock.patch.object(AioS3File, "write", side_effect=RuntimeError("write failed"))
                if error is RuntimeError
                else contextlib.nullcontext(),
                pytest.raises(error),
            ):
                fs.put_file(str(failing), "s3://bucket/k1")
            fs.put_file(str(local), "s3://bucket/k2")

        assert [(c.kwargs["key"], c.kwargs["body"]) for c in put_object.call_args_list] == [
            ("k2", b"a")
        ]

    @pytest.mark.parametrize("kwargs", [{"block_size": 4}, {}])
    def test_transaction_pipe_put_file_exceeding_max_parts(self, tmp_path, kwargs):
        # GH-953: in a transaction, as outside one, pipe_file() and put_file()
        # reject data that does not fit in the maximum number of parts before
        # opening the file.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs.core.MULTIPART_UPLOAD_MAX_PARTS = 3
        fs._sync_fs.default_block_size = 4
        fs._sync_fs._call = fs._sync_fs._core.call = mock.MagicMock()
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
        fs._sync_fs._call = fs._sync_fs._core.call = mock.MagicMock(return_value={"ETag": '"e"'})

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
        sync_fs._call = sync_fs._core.call = mock.MagicMock(return_value={})

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

        fs._sync_fs._call = fs._sync_fs._core.call = call
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

        fs._sync_fs._call = fs._sync_fs._core.call = call
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
        sync_fs._call = sync_fs._core.call = mock.MagicMock(return_value={})
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
        lpath = tmp_path / "data.csv"
        lpath.write_bytes(b"a")

        fs._put_file_in_transaction(
            str(lpath),
            "s3://bucket/key",
            Callback(),
            mode,
            block_size=S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE,
            max_workers=2,
            StorageClass="STANDARD_IA",
        )

        fs.open.assert_called_once_with(
            "s3://bucket/key",
            open_mode,
            block_size=S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE,
            max_workers=2,
            s3_additional_kwargs={"StorageClass": "STANDARD_IA", "ContentType": "text/csv"},
        )

    @pytest.mark.asyncio
    async def test_cp_file_directory(self):
        # GH-1008: recursive copy() passes the directories, which used to be
        # sent to CopyObject and fail with NoSuchKey.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._info = mock.AsyncMock(return_value=S3FileSystem._directory_object("bucket", "src"))
        fs._sync_fs._call = fs._sync_fs._core.call = mock.MagicMock()

        await fs._cp_file("s3://bucket/src", "s3://bucket/dst")
        fs._sync_fs._call.assert_not_called()

    @pytest.mark.asyncio
    async def test_copy_version(self):
        # GH-979: a version is copied to a destination named after its key,
        # not globbed with "?" as a wildcard.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs.isdir = mock.MagicMock(return_value=False)
        fs._cp_file = mock.AsyncMock()

        await fs._copy("s3://bucket/b?versionId=v1", "s3://bucket/d/", RequestPayer="requester")
        fs._cp_file.assert_awaited_once_with(
            "bucket/b?versionId=v1", "s3://bucket/d/b", RequestPayer="requester"
        )

    @pytest.mark.asyncio
    async def test_get_version_path_destination(self, tmp_path):
        # A Path destination is paired too: the version is downloaded to the
        # file, not into a directory of that name.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs.isdir = mock.MagicMock(return_value=False)
        fs._get_file = mock.AsyncMock()

        await fs._get("s3://bucket/b?versionId=v1", tmp_path / "out.bin")
        assert fs._get_file.await_args.args[:2] == (
            "bucket/b?versionId=v1",
            (tmp_path / "out.bin").as_posix(),
        )

    @pytest.mark.asyncio
    async def test_expand_path_glob_lists_stem_prefix(self):
        # Unversioned globs keep fsspec's async expansion, which lists only
        # the keys that start with the stem before the first wildcard.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs._find = mock.MagicMock(return_value=[])

        with pytest.raises(FileNotFoundError):
            await fs._expand_path("s3://bucket/reports/2026-*.csv")
        assert fs._sync_fs._find.call_args.kwargs["prefix"] == "2026-"

    @pytest.mark.asyncio
    async def test_get_version(self, tmp_path):
        # GH-979: a version is downloaded to a local path named after its key.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs.isdir = mock.MagicMock(return_value=False)
        fs._get_file = mock.AsyncMock()

        await fs._get("s3://bucket/b?versionId=v1", f"{tmp_path}/")
        assert fs._get_file.await_args.args[:2] == (
            "bucket/b?versionId=v1",
            f"{tmp_path.as_posix()}/b",
        )

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
        fs._sync_fs._call = fs._sync_fs._core.call = mock.MagicMock(return_value={})

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

    @pytest.mark.asyncio
    async def test_mv_copy_failure(self):
        # A failed copy is raised after the other copies have finished, and
        # nothing is deleted.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        finished = []

        async def copy_file(path1, path2, **kwargs):
            if path1 == "s3://bucket/a":
                raise OSError("copy failed")
            await asyncio.sleep(0.1)
            finished.append(path1)
            return True

        fs._copy_file = copy_file
        fs._sync_fs._call = fs._sync_fs._core.call = mock.MagicMock()

        with pytest.raises(OSError, match="copy failed"):
            await fs._mv(["s3://bucket/a", "s3://bucket/b"], ["s3://bucket/x/a", "s3://bucket/x/b"])
        assert finished == ["s3://bucket/b"]
        fs._sync_fs._call.assert_not_called()

    @pytest.mark.parametrize("status", [None, "Suspended", "Enabled"])
    @pytest.mark.asyncio
    async def test_mv_null_version_onto_key(self, status):
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs._call = mock.MagicMock(
            return_value={} if status is None else {"Status": status}
        )
        fs._copy_file = mock.AsyncMock(return_value=True)
        fs._delete_objects = mock.AsyncMock()
        sources = ["s3://bucket/a?versionId=null", "s3a://bucket/b?version_id=null"]
        destinations = ["s3://bucket/a", "s3://bucket/b"]

        await fs._mv(sources, destinations, MetadataDirective="COPY")

        fs._sync_fs._call.assert_called_once_with(
            fs._sync_fs._client.get_bucket_versioning, Bucket="bucket"
        )
        if status == "Enabled":
            assert fs._copy_file.await_args_list == [
                mock.call(source, dest, MetadataDirective="COPY")
                for source, dest in zip(sources, destinations, strict=True)
            ]
            fs._delete_objects.assert_awaited_once_with(sources)
        else:
            fs._copy_file.assert_not_awaited()
            fs._delete_objects.assert_awaited_once_with([])

    @pytest.mark.parametrize("status", [None, "Suspended", "Enabled"])
    @pytest.mark.asyncio
    async def test_mv_null_version_conflicts_depend_on_bucket_state(self, status):
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs._call = mock.MagicMock(
            return_value={} if status is None else {"Status": status}
        )
        fs._copy_file = mock.AsyncMock(return_value=True)
        fs._delete_objects = mock.AsyncMock()
        sources = ["s3://bucket/a", "s3://bucket/b?versionId=null"]
        destinations = ["s3a://bucket/b", "s3://bucket/out"]

        with (
            contextlib.nullcontext()
            if status == "Enabled"
            else pytest.raises(ValueError, match="another path that is moved")
        ):
            await fs._mv(sources, destinations)

        if status == "Enabled":
            assert fs._copy_file.await_args_list == [
                mock.call(source, dest) for source, dest in zip(sources, destinations, strict=True)
            ]
            fs._delete_objects.assert_awaited_once_with(sources)
        else:
            fs._copy_file.assert_not_awaited()
            fs._delete_objects.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_mv_null_version_directory_bucket_does_not_read_bucket_state(self):
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs._call = mock.MagicMock()
        fs._copy_file = mock.AsyncMock(return_value=True)
        fs._delete_objects = mock.AsyncMock()
        key = "s3://example--usw2-az1--x-s3/key"

        await fs._mv([f"{key}?versionId=null"], [key])

        fs._sync_fs._call.assert_not_called()
        fs._copy_file.assert_not_awaited()
        fs._delete_objects.assert_awaited_once_with([])

    @pytest.mark.parametrize("stage", ["lookup", "copy"])
    @pytest.mark.asyncio
    async def test_mv_null_version_failure_does_not_delete(self, stage):
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs._call = mock.MagicMock(return_value={"Status": "Enabled"})
        fs._copy_file = mock.AsyncMock(return_value=True)
        fs._delete_objects = mock.AsyncMock()
        if stage == "lookup":
            fs._sync_fs._call.side_effect = PermissionError("Access Denied")
        else:
            fs._copy_file.side_effect = PermissionError("Access Denied")

        with pytest.raises(PermissionError, match="Access Denied"):
            await fs._mv(
                ["s3://bucket/other", "s3://bucket/key?versionId=null"],
                ["s3://bucket/out", "s3://bucket/key"],
            )
        fs._delete_objects.assert_not_awaited()
        if stage == "lookup":
            fs._copy_file.assert_not_awaited()

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
        sync_fs.core.copy_object = mock.MagicMock()
        sync_fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"}
            )
        )
        running = []
        concurrency = []

        def upload_part_copy(**kw):
            running.append(kw["part_number"])
            concurrency.append(len(running))
            time.sleep(0.01)
            running.remove(kw["part_number"])
            return SimpleNamespace(etag='"e"', part_number=kw["part_number"])

        sync_fs.core.upload_part_copy = mock.MagicMock(side_effect=upload_part_copy)
        sync_fs.core.complete_multipart_upload = mock.MagicMock()
        # The HeadObject of the source.
        sync_fs._call = sync_fs._core.call = mock.MagicMock(return_value={"ContentLength": size})
        directives = {
            "MetadataDirective": "REPLACE",
            "TaggingDirective": "REPLACE",
            "AnnotationDirective": "EXCLUDE",
        }

        await fs._cp_file(
            "s3://bucket/src",
            "s3://bucket/dst",
            block_size=S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE // 2,
            max_workers=1,
            RequestPayer="requester",
            ContentType="text/csv",
            # Copy without reading the metadata, tags and annotations of the
            # source (GH-973).
            **directives,
        )

        if size <= S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE:
            sync_fs.core.copy_object.assert_called_once_with(
                S3Path("bucket", "src"),
                S3Path("bucket", "dst"),
                RequestPayer="requester",
                ContentType="text/csv",
                **directives,
            )
        else:
            sync_fs.core.create_multipart_upload.assert_called_once_with(
                S3Path("bucket", "dst"), RequestPayer="requester", ContentType="text/csv"
            )
            # The part copies receive the parameters that they accept, and
            # max_workers limits how many run at once.
            # Two parts, the second with the 1-byte tail.
            assert sync_fs.core.upload_part_copy.call_count == 2
            assert all(
                c.kwargs["RequestPayer"] == "requester" and "ContentType" not in c.kwargs
                for c in sync_fs.core.upload_part_copy.call_args_list
            )
            assert max(concurrency) == 1
            assert (
                sync_fs.core.complete_multipart_upload.call_args.kwargs["RequestPayer"]
                == "requester"
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

    @pytest.mark.parametrize(
        ("code", "exception"),
        [
            (None, None),
            ("NoSuchUpload", None),
            ("NoSuchBucket", FileNotFoundError),
            ("AccessDenied", PermissionError),
            ("InternalError", OSError),
        ],
    )
    def test_clear_multipart_uploads_race(self, code, exception):
        fs = AioS3FileSystem(
            key="dummy",
            secret="dummy",
            region_name="us-east-1",
            max_workers=1,
            retry_config=RetryConfig(attempt=1),
            skip_instance_cache=True,
        )
        with Stubber(fs.core.client) as stubber:
            stubber.add_response(
                "list_multipart_uploads",
                {
                    "Uploads": [
                        {"Key": "prefix/gone", "UploadId": "gone"},
                        {
                            "Key": "prefix/pending",
                            "UploadId": "pending",
                            "ChecksumAlgorithm": "CRC32",
                            "ChecksumType": "FULL_OBJECT",
                        },
                    ],
                    "IsTruncated": False,
                },
                {"Bucket": "bucket", "Prefix": "prefix/"},
            )
            request = {"Bucket": "bucket", "Key": "prefix/gone", "UploadId": "gone"}
            if code:
                stubber.add_client_error(
                    "abort_multipart_upload",
                    service_error_code=code,
                    http_status_code=404 if code.startswith("NoSuch") else 500,
                    expected_params=request,
                )
            else:
                stubber.add_response("abort_multipart_upload", {}, request)
            stubber.add_response(
                "abort_multipart_upload",
                {},
                {"Bucket": "bucket", "Key": "prefix/pending", "UploadId": "pending"},
            )
            expected = pytest.raises(exception) if exception else contextlib.nullcontext()
            with expected:
                fs.clear_multipart_uploads("s3://bucket/prefix/")
            stubber.assert_no_pending_responses()

    def test_clear_multipart_uploads_empty(self):
        fs = AioS3FileSystem(
            key="dummy",
            secret="dummy",
            region_name="us-east-1",
            max_workers=1,
            skip_instance_cache=True,
        )
        with Stubber(fs.core.client) as stubber:
            stubber.add_response(
                "list_multipart_uploads",
                {"Uploads": [], "IsTruncated": False},
                {"Bucket": "bucket", "Prefix": "prefix/"},
            )
            fs.clear_multipart_uploads("s3://bucket/prefix/")
            stubber.assert_no_pending_responses()

    @pytest.mark.parametrize(
        ("algorithm", "checksum_type"),
        [(None, None), ("SHA256", None), ("CRC32", None), ("CRC32", "FULL_OBJECT")],
    )
    @pytest.mark.asyncio
    async def test_multipart_copy_uses_creation_algorithm(self, algorithm, checksum_type):
        fs = AioS3FileSystem(
            key="dummy",
            secret="dummy",
            region_name="us-east-1",
            max_workers=1,
            skip_instance_cache=True,
        )
        block_size = 5 * 2**30
        size = 2 * block_size
        checksum_kwargs = {"ChecksumAlgorithm": algorithm} if algorithm else {}
        if checksum_type:
            checksum_kwargs["ChecksumType"] = checksum_type
        expected_parts = []
        with Stubber(fs.core.client) as stubber:
            stubber.add_response(
                "head_object", {"ContentLength": size}, {"Bucket": "bucket", "Key": "src"}
            )
            stubber.add_response(
                "create_multipart_upload",
                {"Bucket": "bucket", "Key": "dst", "UploadId": "u", **checksum_kwargs},
                {"Bucket": "bucket", "Key": "dst", **checksum_kwargs},
            )
            for number, range_ in (
                (1, f"bytes=0-{block_size - 1}"),
                (2, f"bytes={block_size}-{size - 1}"),
            ):
                result = {"ETag": f'"p{number}"', f"Checksum{algorithm or 'CRC32'}": "checksum"}
                stubber.add_response(
                    "upload_part_copy",
                    {"CopyPartResult": result},
                    {
                        "Bucket": "bucket",
                        "Key": "dst",
                        "UploadId": "u",
                        "PartNumber": number,
                        "CopySource": {"Bucket": "bucket", "Key": "src"},
                        "CopySourceRange": range_,
                    },
                )
                expected_parts.append(
                    {
                        "ETag": result["ETag"],
                        "PartNumber": number,
                        **({f"Checksum{algorithm}": "checksum"} if algorithm else {}),
                    }
                )
            stubber.add_response(
                "complete_multipart_upload",
                {"ETag": '"done"'},
                {
                    "Bucket": "bucket",
                    "Key": "dst",
                    "UploadId": "u",
                    "MultipartUpload": {"Parts": expected_parts},
                    **({"ChecksumType": checksum_type} if checksum_type else {}),
                },
            )
            kwargs = {
                "source": S3Path("bucket", "src"),
                "destination": S3Path("bucket", "dst"),
                "block_size": block_size,
                "MetadataDirective": "REPLACE",
                "TaggingDirective": "REPLACE",
                "AnnotationDirective": "EXCLUDE",
                **checksum_kwargs,
            }
            await fs._copy_object_with_multipart_upload(**kwargs)
            stubber.assert_no_pending_responses()

    @pytest.fixture(scope="class")
    def fs(self, request):
        if not hasattr(request, "param"):
            request.param = {}
        return AioS3FileSystem(connection=connect(), **request.param)

    @pytest.mark.parametrize(
        ("algorithm", "checksum_type"),
        [(None, None), ("SHA256", None), ("CRC32", None), ("CRC32", "FULL_OBJECT")],
    )
    def test_open_multipart_with_checksum(self, fs, algorithm, checksum_type):
        block_size = 5 * 2**20
        data = b"x" * (block_size + 1)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_open_multipart_with_checksum/{uuid.uuid4()}"
        )
        kwargs = {"ChecksumAlgorithm": algorithm} if algorithm else {}
        if checksum_type:
            kwargs["ChecksumType"] = checksum_type
        try:
            with fs.open(path, "wb", block_size=block_size, s3_additional_kwargs=kwargs) as file:
                file.write(data)
            assert fs.cat_file(path) == data
            assert fs.list_multipart_uploads(path) == []
        finally:
            fs.clear_multipart_uploads(path)
            if fs.exists(path):
                fs.rm(path)

    @pytest.mark.parametrize(
        ("algorithm", "checksum_type"),
        [(None, None), ("SHA256", None), ("CRC32", None), ("CRC32", "FULL_OBJECT")],
    )
    def test_put_file_multipart_with_checksum(self, fs, tmp_path, algorithm, checksum_type):
        block_size = 5 * 2**20
        data = b"x" * (block_size + 1)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_put_file_multipart_with_checksum/{uuid.uuid4()}"
        )
        kwargs = {"ChecksumAlgorithm": algorithm} if algorithm else {}
        if checksum_type:
            kwargs["ChecksumType"] = checksum_type
        try:
            lpath = tmp_path / "data"
            lpath.write_bytes(data)
            fs.put_file(str(lpath), path, block_size=block_size, s3_additional_kwargs=kwargs)
            assert fs.cat_file(path) == data
            assert fs.list_multipart_uploads(path) == []
        finally:
            fs.clear_multipart_uploads(path)
            if fs.exists(path):
                fs.rm(path)

    @pytest.mark.parametrize(
        ("algorithm", "checksum_type"),
        [(None, None), ("SHA256", None), ("CRC32", None), ("CRC32", "FULL_OBJECT")],
    )
    def test_pipe_file_multipart_with_checksum(self, fs, algorithm, checksum_type):
        block_size = 5 * 2**20
        data = b"x" * (block_size + 1)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_pipe_file_multipart_with_checksum/{uuid.uuid4()}"
        )
        kwargs = {"ChecksumAlgorithm": algorithm} if algorithm else {}
        if checksum_type:
            kwargs["ChecksumType"] = checksum_type
        try:
            fs.pipe_file(path, data, block_size=block_size, s3_additional_kwargs=kwargs)
            assert fs.cat_file(path) == data
            assert fs.list_multipart_uploads(path) == []
        finally:
            fs.clear_multipart_uploads(path)
            if fs.exists(path):
                fs.rm(path)

    @pytest.mark.parametrize(
        ("algorithm", "checksum_type"),
        [(None, None), ("SHA256", None), ("CRC32", None), ("CRC32", "FULL_OBJECT")],
    )
    def test_append_multipart_with_checksum(self, fs, algorithm, checksum_type):
        block_size = 5 * 2**20
        data = b"x" * (block_size + 1)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_append_multipart_with_checksum/{uuid.uuid4()}"
        )
        kwargs = {"ChecksumAlgorithm": algorithm} if algorithm else {}
        if checksum_type:
            kwargs["ChecksumType"] = checksum_type
        try:
            original = b"y" * block_size
            fs.pipe_file(path, original)
            with fs.open(path, "ab", block_size=block_size, s3_additional_kwargs=kwargs) as file:
                file.write(data)
            assert fs.cat_file(path) == original + data
            assert fs.list_multipart_uploads(path) == []
        finally:
            fs.clear_multipart_uploads(path)
            if fs.exists(path):
                fs.rm(path)

    def test_clear_multipart_uploads_after_listed_upload_is_aborted(self, fs):
        sync_fs = fs._sync_fs
        prefix = (
            f"{ENV.s3_staging_key}{ENV.schema}/filesystem/test_async_clear_multipart_race/"
            f"{uuid.uuid4()}/"
        )
        path = f"s3://{ENV.s3_staging_bucket}/{prefix}"
        gone_path = S3Path(ENV.s3_staging_bucket, f"{prefix}gone")
        gone = sync_fs.core.create_multipart_upload(gone_path)
        try:
            sync_fs.core.create_multipart_upload(S3Path(ENV.s3_staging_bucket, f"{prefix}pending"))
            list_uploads = sync_fs.list_multipart_uploads

            def list_then_abort(path):
                uploads = list_uploads(path)
                assert len(uploads) == 2
                assert any(upload.upload_id == gone.upload_id for upload in uploads)
                sync_fs.core.abort_multipart_upload(gone)
                return uploads

            with mock.patch.object(
                sync_fs, "list_multipart_uploads", side_effect=list_then_abort
            ) as list_mock:
                fs.clear_multipart_uploads(path)
                list_mock.assert_called_once_with(path)
            assert fs.list_multipart_uploads(path) == []
        finally:
            fs.clear_multipart_uploads(path)

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

    @pytest.mark.asyncio
    async def test_du(self, fs):
        """Disk usage reports file sizes, their total, and the requested depth."""
        directory = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/filesystem/test_async_du"
        first = f"{directory}/first"
        second = f"{directory}/nested/second"
        try:
            await fs._pipe_file(first, b"abc")
            await fs._pipe_file(second, b"12345")
            assert await fs._du(directory) == 8
            assert await fs._du(directory, total=False) == {
                fs._strip_protocol(first): 3,
                fs._strip_protocol(second): 5,
            }
            assert await fs._du(directory, maxdepth=1) == 3
            assert await fs._du(first) == 3
        finally:
            with contextlib.suppress(FileNotFoundError):
                await fs._rm(directory, recursive=True)

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
        assert fs._strip_protocol(f"{dir_}/nested") in fs.glob(f"{dir_}/nested/**")

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
        fs.core.delete_object(S3Path(bucket, key))
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
        block_size = S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE
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
        sync_fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"}
            )
        )
        sync_fs.core.upload_part = track(
            lambda **kw: S3MultipartUploadPart(kw["part_number"], {"ETag": '"e"'})
        )
        sync_fs.core.complete_multipart_upload = mock.MagicMock()
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
        assert sync_fs.core.upload_part.call_count == 4
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

    def test_open_lookup_parameters(self):
        # GH-1004: the lookup of the file sends its lookup parameters.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs.info = mock.MagicMock(
            return_value=S3Object(
                init={"ContentLength": 4},
                type=S3ObjectType.S3_OBJECT_TYPE_FILE,
                bucket="bucket",
                key="key",
            )
        )
        sse_c = {"SSECustomerAlgorithm": "AES256", "SSECustomerKey": "k" * 32}

        with fs.open("s3://bucket/key", "rb", ContentType="text/plain", **sse_c) as f:
            assert isinstance(f, AioS3File)
        fs._sync_fs.info.assert_called_once_with("bucket/key", version_id=None, **sse_c)

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
