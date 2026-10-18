import array
import asyncio
import contextlib
import gc
import gzip
import io
import os
import tempfile
import threading
import time
import traceback
import uuid
import weakref
from concurrent.futures import Future, ThreadPoolExecutor, wait
from types import SimpleNamespace
from unittest import mock

import boto3
import fsspec
import pytest
from botocore.stub import Stubber

from pyathena.filesystem.s3 import S3FileSystem
from pyathena.filesystem.s3_async import AioS3File, AioS3FileSystem, AioS3StreamedFile
from pyathena.filesystem.s3_core import S3Core, S3MultipartCopyPlan
from pyathena.filesystem.s3_executor import S3AioThreadPool, S3Executor
from pyathena.filesystem.s3_object import (
    S3MultipartUpload,
    S3MultipartUploadPart,
    S3Object,
    S3ObjectType,
    S3ObjectVersion,
)
from pyathena.filesystem.s3_path import S3Path
from pyathena.filesystem.s3_writer import S3MultipartStateError
from pyathena.util import RetryConfig
from tests import ENV
from tests.pyathena.conftest import connect
from tests.pyathena.util import (
    EVENT_TIMEOUT,
    MULTIPART_COPY_BLOCK_SIZE,
    MULTIPART_COPY_KWARGS,
    ThrottledOperation,
    await_bounded,
    overlapping_calls,
    retried,
    stub_multipart_copy,
    wait_until,
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
                await await_bounded(
                    fs._copy_object_with_multipart_upload(
                        S3Path("bucket", "src"),
                        S3Path("bucket", "dst"),
                        block_size=MULTIPART_COPY_BLOCK_SIZE,
                        **MULTIPART_COPY_KWARGS,
                    )
                )
            finally:
                stubber.assert_no_pending_responses()

    @pytest.mark.parametrize("size", [0, 10])
    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_small_head_object_size(self, size):
        # GH-973: see
        # TestS3FileSystem.test_copy_object_with_multipart_upload_small_head_object_size.
        # The async copy has its own single-request branch: the reported
        # version is copied with one CopyObject and nothing else is sent.
        fs = AioS3FileSystem(
            key="dummy", secret="dummy", region_name="us-east-1", skip_instance_cache=True
        )
        with Stubber(fs._sync_fs._client) as stubber:
            stubber.add_response("head_object", {"ContentLength": size, "VersionId": "v1"}, None)
            stubber.add_response(
                "copy_object",
                {},
                {
                    "CopySource": {"Bucket": "bucket", "Key": "src", "VersionId": "v1"},
                    "Bucket": "bucket",
                    "Key": "dst",
                    "ContentType": "text/csv",
                    "RequestPayer": "requester",
                },
            )
            await fs._copy_object_with_multipart_upload(
                S3Path("bucket", "src"),
                S3Path("bucket", "dst"),
                ContentType="text/csv",
                RequestPayer="requester",
            )
            stubber.assert_no_pending_responses()

    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_copies_source(self):
        # GH-973: the same requests as S3FileSystem; see
        # TestS3FileSystem.test_copy_object_with_multipart_upload_copies_source.
        await self._multipart_copy()

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
            await await_bounded(
                fs._copy_object_with_multipart_upload(
                    S3Path("bucket", "src"),
                    S3Path("bucket", "dst"),
                    MetadataDirective="REPLACE",
                    TaggingDirective="REPLACE",
                    AnnotationDirective="EXCLUDE",
                )
            )

        # Part 3 waits for a worker and is not started after the failure.
        assert sorted(events[:2]) == ["start 1", "start 2"]
        assert events[2:] == ["end 1", "abort"]
        sync_fs.core.complete_multipart_upload.assert_not_called()

    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_max_workers(self):
        # GH-1159: max_workers of the copy bounds its parallel part copies,
        # whatever the size of the default executor of the event loop, in
        # which they used to run.
        asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(max_workers=1))
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        sync_fs = fs._sync_fs
        sync_fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "dst", "UploadId": "uploadid"}
            )
        )
        condition = threading.Condition()
        state = {"calls": 0, "active": 0, "peak": 0}
        # The first three part copies run at once.
        first_three = threading.Barrier(3, timeout=EVENT_TIMEOUT)

        def upload_part_copy(**kw):
            with condition:
                state["calls"] += 1
                number = state["calls"]
                state["active"] += 1
                state["peak"] = max(state["peak"], state["active"])
                condition.notify_all()
            try:
                if number <= 3:
                    first_three.wait()
                    # Waits briefly for a fourth overlapping call, which only a
                    # copy that exceeds max_workers runs.
                    with condition:
                        condition.wait_for(lambda: state["active"] > 3, timeout=0.5)
            finally:
                with condition:
                    state["active"] -= 1
            return SimpleNamespace(etag='"e"', part_number=kw["part_number"])

        sync_fs.core.upload_part_copy = mock.MagicMock(side_effect=upload_part_copy)
        sync_fs.core.complete_multipart_upload = mock.MagicMock()
        # The HeadObject of the source, with 4 parts of the default block size.
        size = 4 * S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE
        sync_fs._call = sync_fs._core.call = mock.MagicMock(return_value={"ContentLength": size})

        await fs._copy_object_with_multipart_upload(
            S3Path("bucket", "src"),
            S3Path("bucket", "dst"),
            max_workers=3,
            MetadataDirective="REPLACE",
            TaggingDirective="REPLACE",
            AnnotationDirective="EXCLUDE",
        )

        assert sync_fs.core.upload_part_copy.call_count == 4
        assert state["peak"] == 3
        sync_fs.core.complete_multipart_upload.assert_called_once()

    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_shares_threads(self):
        # GH-1159: concurrent copies share the threads of the filesystem, so
        # that a bulk copy of large objects does not start max_workers threads
        # per object.
        fs = AioS3FileSystem(connection=mock.MagicMock(), max_workers=2, skip_instance_cache=True)
        sync_fs = fs._sync_fs
        sync_fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "dst", "UploadId": "uploadid"}
            )
        )
        # Waits briefly for a third overlapping call, which only copies
        # with threads of their own run.
        call, state = overlapping_calls(3, timeout=0.5)

        def upload_part_copy(**kw):
            call()
            return SimpleNamespace(etag='"e"', part_number=kw["part_number"])

        sync_fs.core.upload_part_copy = mock.MagicMock(side_effect=upload_part_copy)
        sync_fs.core.complete_multipart_upload = mock.MagicMock()
        size = 2 * S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE
        sync_fs._call = sync_fs._core.call = mock.MagicMock(return_value={"ContentLength": size})

        await asyncio.gather(
            *[
                fs._copy_object_with_multipart_upload(
                    S3Path("bucket", "src"),
                    S3Path("bucket", f"dst{i}"),
                    MetadataDirective="REPLACE",
                    TaggingDirective="REPLACE",
                    AnnotationDirective="EXCLUDE",
                )
                for i in range(2)
            ]
        )

        assert sync_fs.core.upload_part_copy.call_count == 4
        assert state["peak"] == 2

    @pytest.mark.asyncio
    async def test_blocking_calls_bypass_default_executor(self):
        # GH-1159: the blocking calls of the coroutines run in the threads of
        # the filesystem, so that a busy default executor of the event loop
        # does not hold them. They used to run in it.
        loop = asyncio.get_running_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs.info = mock.MagicMock(return_value={"name": "bucket/key"})
        release = threading.Event()
        # Held until it is released, after the lookup has had to finish.
        blocker = loop.run_in_executor(None, release.wait, 60)
        try:
            info = await asyncio.wait_for(fs._info("s3://bucket/key"), 5)
        finally:
            release.set()
            await blocker

        assert info == {"name": "bucket/key"}

    @staticmethod
    def _serve_keys(fs, keys):
        # Answer the S3 requests of the filesystem from a set of keys of
        # "bucket", which is returned, and record them by operation name. The
        # requests may come from several threads at once.
        store = set(keys)
        requests = []
        lock = threading.Lock()

        def call(method, **request):
            with lock:
                return answer(method if isinstance(method, str) else method.__name__, request)

        def answer(name, request):
            requests.append((name, request))
            key = request.get("Key")
            if name == "head_object":
                if key not in store:
                    raise FileNotFoundError(key)
                return {"ContentLength": 2, "ETag": '"e"'}
            if name == "get_object":
                return {"Body": io.BytesIO(b"aa"), "ContentLength": 2, "ETag": '"e"'}
            if name == "list_objects_v2":
                prefix, delimiter = request.get("Prefix", ""), request.get("Delimiter")
                listed = sorted(k for k in list(store) if k.startswith(prefix))
                directories = sorted(
                    {
                        prefix + k[len(prefix) :].split(delimiter)[0] + delimiter
                        for k in listed
                        if delimiter and delimiter in k[len(prefix) :]
                    }
                )
                files = [k for k in listed if not delimiter or delimiter not in k[len(prefix) :]]
                return {
                    "Contents": [{"Key": k, "Size": 2, "ETag": '"e"'} for k in files],
                    "CommonPrefixes": [{"Prefix": d} for d in directories],
                    "KeyCount": len(files) + len(directories),
                }
            if name == "copy_object" and request["CopySource"]["Key"] not in store:
                raise FileNotFoundError(request["CopySource"]["Key"])
            if name in {"put_object", "copy_object"}:
                store.add(key)
                return {"ETag": '"e"'}
            if name == "delete_object":
                store.discard(key)
            if name == "delete_objects":
                deleted = [o["Key"] for o in request["Delete"]["Objects"]]
                store.difference_update(deleted)
                return {"Deleted": [{"Key": k} for k in deleted]}
            if name == "get_object_tagging":
                return {"TagSet": [{"Key": "a", "Value": "1"}]}
            if name == "list_object_versions":
                return {
                    "Versions": [{"Key": "key", "VersionId": "v1", "IsLatest": False}],
                    "DeleteMarkers": [{"Key": "key", "VersionId": "m1", "IsLatest": True}],
                }
            return {}

        fs._sync_fs._call = fs._sync_fs._core.call = call
        return store, requests

    @pytest.mark.parametrize(
        ("method", "sync_method", "args", "kwargs"),
        [
            (
                "_info",
                "info",
                ("s3://bucket/key?versionId=v1",),
                {"ExpectedBucketOwner": "111122223333"},
            ),
            ("_ls", "ls", ("s3://bucket/dir",), {"detail": True}),
            (
                "_cat_file",
                "cat_file",
                ("s3://bucket/key",),
                {"start": 1, "end": 2, "ExpectedBucketOwner": "111122223333"},
            ),
            (
                "_exists",
                "exists",
                ("s3://bucket/key?versionId=v1",),
                {"ExpectedBucketOwner": "111122223333"},
            ),
            ("_rm_file", "rm_file", ("s3://bucket/key?versionId=v1",), {}),
            (
                "_pipe_file",
                "pipe_file",
                ("s3://bucket/new", b"a"),
                {"mode": "create", "ContentType": "text/csv"},
            ),
            (
                "_put_file",
                "put_file",
                ("local-source", "s3://bucket/new"),
                {"mode": "create", "ContentType": "text/csv"},
            ),
            (
                "_get_file",
                "get_file",
                ("s3://bucket/key?versionId=v1", "local-destination"),
                {"ExpectedBucketOwner": "111122223333"},
            ),
            ("_touch", "touch", ("s3://bucket/new",), {"ContentType": "text/csv"}),
            ("_find", "find", ("s3://bucket/dir",), {"maxdepth": 1, "withdirs": True}),
            (
                "_mv",
                "mv",
                ("s3://bucket/dir", "s3://bucket/out"),
                {"recursive": True, "maxdepth": 1, "MetadataDirective": "REPLACE"},
            ),
            (
                "metadata",
                "metadata",
                ("s3://bucket/key?versionId=v1",),
                {"ExpectedBucketOwner": "111122223333"},
            ),
            ("get_tags", "get_tags", ("s3://bucket/key?versionId=v1",), {}),
            ("put_tags", "put_tags", ("s3://bucket/key", {"b": "2"}), {"mode": "m"}),
            ("chmod", "chmod", ("s3://bucket/dir", "private"), {"recursive": True}),
            (
                "object_version_info",
                "object_version_info",
                ("s3://bucket/key",),
                {"delete_markers": True},
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_delegations_send_the_requests_of_the_sync_methods(
        self, tmp_path, method, sync_method, args, kwargs
    ):
        # The methods that run the synchronous filesystem's methods pass their
        # arguments through: each sends the requests, and returns the result,
        # of the synchronous method called with the same arguments.
        keys = {"key", "dir/a", "dir/sub/b"}
        source = tmp_path / "source"
        source.write_bytes(b"a")

        def comparable(value):
            # The listed versions have no equality of their own.
            if isinstance(value, list):
                return [comparable(v) for v in value]
            if isinstance(value, S3ObjectVersion):
                return vars(value)
            return value

        outcomes = []
        for run, name in (("sync", sync_method), ("async", method)):
            fs = AioS3FileSystem(
                key="dummy", secret="dummy", region_name="us-east-1", skip_instance_cache=True
            )
            store, requests = self._serve_keys(fs, keys)
            local = {"local-source": str(source), "local-destination": str(tmp_path / run)}
            call_args = [local.get(a, a) if isinstance(a, str) else a for a in args]
            if run == "sync":
                result = getattr(fs._sync_fs, name)(*call_args, **kwargs)
            else:
                result = getattr(fs, name)(*call_args, **kwargs)
                if asyncio.iscoroutine(result):
                    result = await await_bounded(result)
            outcomes.append((comparable(result), sorted(map(repr, requests)), sorted(store)))
        assert outcomes[0][1], "no request was sent"
        assert outcomes[1] == outcomes[0]

    @pytest.mark.asyncio
    async def test_max_workers_none(self):
        # None takes the default of ThreadPoolExecutor for the threads of the
        # filesystem and as the default max_workers of its files and copies.
        # The files and copies used to fail with it.
        fs = AioS3FileSystem(
            connection=mock.MagicMock(), max_workers=None, skip_instance_cache=True
        )
        sync_fs = fs._sync_fs
        sync_fs.info = mock.MagicMock(return_value={"name": "bucket/key"})

        assert await fs._info("s3://bucket/key") == {"name": "bucket/key"}
        # A multipart copy and a file without max_workers of their own.
        sync_fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "dst", "UploadId": "uploadid"}
            )
        )
        sync_fs.core.upload_part_copy = mock.MagicMock(
            side_effect=lambda **kw: SimpleNamespace(etag='"e"', part_number=kw["part_number"])
        )
        sync_fs.core.complete_multipart_upload = mock.MagicMock()
        sync_fs.core.put_object = mock.MagicMock()
        # The HeadObject of the source, with 2 parts of the default block size.
        sync_fs._call = sync_fs._core.call = mock.MagicMock(
            return_value={"ContentLength": 2 * S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE}
        )
        await fs._copy_object_with_multipart_upload(
            S3Path("bucket", "src"),
            S3Path("bucket", "dst"),
            MetadataDirective="REPLACE",
            TaggingDirective="REPLACE",
            AnnotationDirective="EXCLUDE",
        )
        assert sync_fs.core.upload_part_copy.call_count == 2
        sync_fs.core.complete_multipart_upload.assert_called_once()

        def write():
            with fs.open("s3://bucket/key", "wb") as f:
                f.write(b"a")

        await asyncio.to_thread(write)
        sync_fs.core.put_object.assert_called_once()

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

        logged = []
        asyncio.get_running_loop().set_exception_handler(
            lambda _, context: logged.append(context["message"])
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
        # The threads have run the callbacks of the requests once they are
        # shut down, and the event loop runs theirs on the next iterations.
        await asyncio.to_thread(fs._thread_pool.shutdown, True)
        gc.collect()
        for _ in range(3):
            await asyncio.sleep(0)

        assert events == (["complete", "abort"] if completion_fails else ["complete"])
        # GH-1186: a completion that fails after the cancellation is not
        # logged as never retrieved.
        assert logged == []

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

        logged = []
        asyncio.get_running_loop().set_exception_handler(
            lambda _, context: logged.append(context["message"])
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
        # The threads have run the callbacks of the requests once they are
        # shut down, and the event loop runs theirs on the next iterations.
        await asyncio.to_thread(fs._thread_pool.shutdown, True)
        gc.collect()
        for _ in range(3):
            await asyncio.sleep(0)

        assert events == (["create"] if creation_fails else ["create", ("abort", "uploadid")])
        # GH-1186: a creation that fails after the cancellation is not logged
        # as never retrieved.
        assert logged == []
        sync_fs.core.upload_part_copy.assert_not_called()

    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_cancelled_queued_completion(self):
        # GH-1186: a cancellation cancels a CompleteMultipartUpload request
        # that waits for a thread of the filesystem, and aborts the upload. The
        # request used to run after the cancellation and complete the copy.
        fs = AioS3FileSystem(connection=mock.MagicMock(), max_workers=1, skip_instance_cache=True)
        sync_fs = fs._sync_fs
        sync_fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "dst", "UploadId": "uploadid"}
            )
        )
        events = []
        release = threading.Event()

        def block():
            # The finally block of the test always releases it.
            release.wait()

        def upload_part_copy(**kw):
            if kw["part_number"] == 2:
                # Holds the only thread of the filesystem, so that the
                # completion waits for it.
                fs._thread_pool.submit(block)
            return SimpleNamespace(etag='"e"', part_number=kw["part_number"])

        sync_fs.core.upload_part_copy = mock.MagicMock(side_effect=upload_part_copy)
        sync_fs.core.complete_multipart_upload = mock.MagicMock(
            side_effect=lambda *args, **kw: events.append("complete")
        )
        # The HeadObject of the source, with 2 parts of the default block size.
        size = 2 * S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE
        sync_fs._call = sync_fs._core.call = mock.MagicMock(return_value={"ContentLength": size})
        sync_fs._abort_multipart_upload = mock.MagicMock(
            side_effect=lambda upload, params: events.append("abort")
        )

        submit = fs._thread_pool.submit
        completion_queued = threading.Event()

        def record_completion(fn, *args, **kwargs):
            future = submit(fn, *args, **kwargs)
            if fn is sync_fs.core.complete_multipart_upload:
                completion_queued.set()
            return future

        fs._thread_pool.submit = record_completion
        task = asyncio.ensure_future(
            fs._copy_object_with_multipart_upload(
                S3Path("bucket", "src"),
                S3Path("bucket", "dst"),
                max_workers=1,
                MetadataDirective="REPLACE",
                TaggingDirective="REPLACE",
                AnnotationDirective="EXCLUDE",
            )
        )
        try:
            # The completion waits for the thread, which block() holds.
            assert await asyncio.to_thread(completion_queued.wait, 5)
            task.cancel()
            # Lets the copy cancel the completion and schedule its cleanup.
            await asyncio.sleep(0.1)
        except BaseException:
            task.cancel()
            raise
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert events == ["abort"]

    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_failed_part_stops_running_parts(self):
        # GH-1203: a failed part copy stops the retries of the running ones
        # before the upload is aborted; their backoff wait would take at least
        # EVENT_TIMEOUT seconds.
        fs = AioS3FileSystem(connection=mock.MagicMock(), max_workers=4, skip_instance_cache=True)
        sync_fs = fs._sync_fs
        plan = S3MultipartCopyPlan(
            source=S3Path("bucket", "src"),
            destination=S3Path("bucket", "dst"),
            size=3,
            ranges=((0, 1), (1, 2), (2, 3)),
        )
        sync_fs.core.plan_multipart_copy = mock.MagicMock(return_value=plan)
        sync_fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload({"Bucket": "bucket", "Key": "dst", "UploadId": "id"})
        )
        sync_fs._abort_multipart_upload = mock.MagicMock()
        operation = ThrottledOperation(operation_name="UploadPartCopy")
        throttled = retried(operation, RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT))

        def upload_part_copy(part_number, **kwargs):
            if part_number == 1:
                assert operation.started.wait(EVENT_TIMEOUT)
                raise PermissionError("Access Denied")
            if part_number == 2:
                return throttled()
            return SimpleNamespace(etag='"e"', part_number=part_number)

        sync_fs.core.upload_part_copy = mock.MagicMock(side_effect=upload_part_copy)
        # The retries stop before the bound of the wait.
        with pytest.raises(PermissionError, match="Access Denied"):
            await await_bounded(
                fs._copy_object_with_multipart_upload(plan.source, plan.destination, max_workers=2)
            )
        assert operation.calls == 1
        sync_fs._abort_multipart_upload.assert_called_once()

    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_cancelled_creation_stops_retries(self):
        # GH-1203: cancelling a copy stops the retries of the creation of its
        # upload, which the cleanup waits for; their backoff wait would take at
        # least EVENT_TIMEOUT seconds.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        sync_fs = fs._sync_fs
        plan = S3MultipartCopyPlan(
            source=S3Path("bucket", "src"),
            destination=S3Path("bucket", "dst"),
            size=2,
            ranges=((0, 1), (1, 2)),
        )
        sync_fs.core.plan_multipart_copy = mock.MagicMock(return_value=plan)
        operation = ThrottledOperation(block=True, operation_name="CreateMultipartUpload")
        sync_fs.core.create_multipart_upload = retried(
            operation, RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT)
        )
        sync_fs._abort_multipart_upload = mock.MagicMock()
        task = asyncio.ensure_future(
            fs._copy_object_with_multipart_upload(plan.source, plan.destination)
        )
        try:
            assert await asyncio.to_thread(operation.started.wait, EVENT_TIMEOUT)
            task.cancel()
        finally:
            operation.release.set()
        started = time.monotonic()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert time.monotonic() - started < EVENT_TIMEOUT
        assert operation.calls == 1
        # No upload was created.
        sync_fs._abort_multipart_upload.assert_not_called()

    @pytest.mark.parametrize("failing", ["a1", "a2"])
    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_failed_annotation_stops_running_copies(
        self, failing
    ):
        # GH-1203: a failed annotation copy stops the retries of the running
        # ones, whose backoff wait would take at least EVENT_TIMEOUT seconds,
        # and its error is raised, whichever annotation comes first.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        sync_fs = fs._sync_fs
        plan = S3MultipartCopyPlan(
            source=S3Path("bucket", "src"),
            destination=S3Path("bucket", "dst"),
            size=1,
            ranges=((0, 1),),
            annotations=("a1", "a2"),
        )
        sync_fs.core.plan_multipart_copy = mock.MagicMock(return_value=plan)
        sync_fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload({"Bucket": "bucket", "Key": "dst", "UploadId": "id"})
        )
        sync_fs.core.upload_part_copy = mock.MagicMock(
            side_effect=lambda **kw: SimpleNamespace(etag='"e"', part_number=kw["part_number"])
        )
        sync_fs.core.complete_multipart_upload = mock.MagicMock(
            return_value=SimpleNamespace(version_id=None, etag='"e"')
        )
        operation = ThrottledOperation(operation_name="PutObjectAnnotation")
        throttled = retried(operation, RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT))

        def copy_object_annotation(name, *args, **kwargs):
            if name == failing:
                assert operation.started.wait(EVENT_TIMEOUT)
                raise PermissionError("Access Denied")
            return throttled()

        sync_fs.core.copy_object_annotation = mock.MagicMock(side_effect=copy_object_annotation)
        # The retries stop before the bound of the wait.
        with pytest.raises(PermissionError, match="Access Denied"):
            await await_bounded(
                fs._copy_object_with_multipart_upload(plan.source, plan.destination)
            )
        assert operation.calls == 1

    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_failed_annotation_skips_queued_copies(self):
        # GH-1203: the annotation copies that wait for a thread of the filesystem
        # when one fails are not sent.
        fs = AioS3FileSystem(connection=mock.MagicMock(), max_workers=1, skip_instance_cache=True)
        sync_fs = fs._sync_fs
        plan = S3MultipartCopyPlan(
            source=S3Path("bucket", "src"),
            destination=S3Path("bucket", "dst"),
            size=1,
            ranges=((0, 1),),
            annotations=("a1", "a2", "a3"),
        )
        sync_fs.core.plan_multipart_copy = mock.MagicMock(return_value=plan)
        sync_fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload({"Bucket": "bucket", "Key": "dst", "UploadId": "id"})
        )
        sync_fs.core.upload_part_copy = mock.MagicMock(
            side_effect=lambda **kw: SimpleNamespace(etag='"e"', part_number=kw["part_number"])
        )
        sync_fs.core.complete_multipart_upload = mock.MagicMock(
            return_value=SimpleNamespace(version_id=None, etag='"e"')
        )

        loop = asyncio.get_running_loop()

        def copy_object_annotation(name, *args, **kwargs):
            # The tasks of the three copies start in one pass of the event
            # loop, in which each submits its copy; a callback scheduled now
            # runs after that pass, so the other copies wait for the only
            # thread when this one fails.
            asyncio.run_coroutine_threadsafe(asyncio.sleep(0), loop).result(EVENT_TIMEOUT)
            raise PermissionError("Access Denied")

        sync_fs.core.copy_object_annotation = mock.MagicMock(side_effect=copy_object_annotation)
        with pytest.raises(PermissionError, match="Access Denied"):
            await await_bounded(
                fs._copy_object_with_multipart_upload(plan.source, plan.destination, max_workers=3)
            )
        sync_fs.core.copy_object_annotation.assert_called_once()

    @pytest.mark.asyncio
    async def test_copy_object_with_multipart_upload_cancelled_queued_parts(self):
        # GH-1186: a cancellation cancels the part copies that wait for a
        # thread of the filesystem, whose threads bound all its copies, before
        # it waits for the running ones and aborts the upload. The queued
        # part copies used to run before the abort.
        fs = AioS3FileSystem(connection=mock.MagicMock(), max_workers=1, skip_instance_cache=True)
        sync_fs = fs._sync_fs
        sync_fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "dst", "UploadId": "uploadid"}
            )
        )
        events = []
        started = threading.Event()
        release = threading.Event()

        def upload_part_copy(**kw):
            events.append(f"start {kw['part_number']}")
            started.set()
            # The finally block of the test always releases it.
            release.wait()
            events.append(f"end {kw['part_number']}")
            return SimpleNamespace(etag='"e"', part_number=kw["part_number"])

        sync_fs.core.upload_part_copy = mock.MagicMock(side_effect=upload_part_copy)
        sync_fs.core.complete_multipart_upload = mock.MagicMock()
        # The HeadObject of the source, with 2 parts of the default block size.
        size = 2 * S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE
        sync_fs._call = sync_fs._core.call = mock.MagicMock(return_value={"ContentLength": size})
        sync_fs._abort_multipart_upload = mock.MagicMock(
            side_effect=lambda upload, params: events.append("abort")
        )

        task = asyncio.ensure_future(
            fs._copy_object_with_multipart_upload(
                S3Path("bucket", "src"),
                S3Path("bucket", "dst"),
                max_workers=2,
                MetadataDirective="REPLACE",
                TaggingDirective="REPLACE",
                AnnotationDirective="EXCLUDE",
            )
        )
        try:
            assert await asyncio.to_thread(started.wait, 5)
            task.cancel()
            # Gives the cleanup time to cancel the queued part copy.
            await asyncio.sleep(0.1)
        except BaseException:
            task.cancel()
            raise
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert events == ["start 1", "end 1", "abort"]
        sync_fs.core.complete_multipart_upload.assert_not_called()

    @pytest.mark.parametrize(
        ("stage", "expected"),
        [
            ("create", ["start create", "end create", "abort"]),
            ("part", ["start part", "start part", "end part", "end part", "abort"]),
            ("complete", ["start complete", "end complete"]),
        ],
    )
    @pytest.mark.parametrize("cancelled", [False, True])
    def test_copy_object_with_multipart_upload_loop_shutdown(self, stage, expected, cancelled):
        # GH-1186: when the shutdown of the event loop at the end of
        # asyncio.run() cancels a copy, or a cleanup that a cancellation of
        # the copy has started, the cleanup still waits for the running
        # requests and aborts the upload unless it completed. The shutdown
        # also cancelled the tasks of the requests and of the cleanup, so the
        # cleanup left an upload created during the cancellation, aborted
        # before the running parts ended, or aborted during the completion.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        sync_fs = fs._sync_fs
        events = []
        lock = threading.Lock()
        copy: asyncio.Task[None] | None = None

        def request(name):
            with lock:
                events.append(f"start {name}")
            # Held until asyncio.run() has cancelled the copy, after the
            # cancellation of the test, if any.
            assert copy is not None
            deadline = time.monotonic() + 5
            while copy.cancelling() < (2 if cancelled else 1):
                assert time.monotonic() < deadline
                time.sleep(0.01)
            with lock:
                events.append(f"end {name}")

        def create_multipart_upload(*args, **kw):
            if stage == "create":
                request("create")
            return S3MultipartUpload({"Bucket": "bucket", "Key": "dst", "UploadId": "uploadid"})

        def upload_part_copy(**kw):
            if stage == "part":
                request("part")
            return SimpleNamespace(etag='"e"', part_number=kw["part_number"])

        def complete_multipart_upload(*args, **kw):
            request("complete")
            return SimpleNamespace()

        sync_fs.core.create_multipart_upload = mock.MagicMock(side_effect=create_multipart_upload)
        sync_fs.core.upload_part_copy = mock.MagicMock(side_effect=upload_part_copy)
        sync_fs.core.complete_multipart_upload = mock.MagicMock(
            side_effect=complete_multipart_upload
        )
        # The HeadObject of the source, with 2 parts of the default block size.
        size = 2 * S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE
        sync_fs._call = sync_fs._core.call = mock.MagicMock(return_value={"ContentLength": size})
        sync_fs._abort_multipart_upload = mock.MagicMock(
            side_effect=lambda upload, params: events.append("abort")
        )

        async def main():
            nonlocal copy
            copy = asyncio.ensure_future(
                fs._copy_object_with_multipart_upload(
                    S3Path("bucket", "src"),
                    S3Path("bucket", "dst"),
                    max_workers=2,
                    MetadataDirective="REPLACE",
                    TaggingDirective="REPLACE",
                    AnnotationDirective="EXCLUDE",
                )
            )
            # Returns once the held requests have started.
            started = expected.count(f"start {stage}")
            deadline = time.monotonic() + 5
            while len(events) < started:
                assert time.monotonic() < deadline
                await asyncio.sleep(0.01)
            if cancelled:
                copy.cancel()
                # Lets the copy start its cleanup.
                await asyncio.sleep(0.1)

        asyncio.run(main())
        # The cleanup may end after asyncio.run() returns.
        fs._thread_pool.shutdown(wait=True)

        assert events == expected

    @pytest.mark.parametrize("commit", [True, False])
    def test_transaction_pipe_put_file(self, tmp_path, commit):
        # GH-977: pipe_file() and put_file() join the transaction of this
        # filesystem; they used to write through the internal S3FileSystem,
        # which is not in the transaction, and were not rolled back.
        # A real client selects the request parameters of each operation.
        fs = AioS3FileSystem(
            key="dummy", secret="dummy", region_name="us-east-1", skip_instance_cache=True
        )
        put_object = fs._sync_fs.core.put_object = mock.MagicMock()
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
                (c.args[0].key, c.args[1], c.kwargs.get("ContentType"))
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
        put_object = fs._sync_fs.core.put_object = mock.MagicMock()

        with fs.transaction if intrans else contextlib.nullcontext():
            fs.pipe_file(path, b"data", compression=compression)

        (((_, body), kwargs),) = put_object.call_args_list
        assert "compression" not in kwargs
        assert gzip.decompress(body) == b"data"

    def test_transaction_pipe_file_write(self):
        # GH-997: in a transaction, a non-contiguous memoryview is written,
        # and a failed write does not replace the object with an empty one
        # when the transaction commits. GH-1211: the commit of the failed
        # write raises, and fsspec discards the rest of the transaction.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        put_object = fs._sync_fs.core.put_object = mock.MagicMock()

        def write_in_transaction():
            with fs.transaction:
                with (
                    mock.patch.object(AioS3File, "write", side_effect=RuntimeError("write failed")),
                    pytest.raises(RuntimeError, match="write failed"),
                ):
                    fs.pipe_file("s3://bucket/k1", b"data")
                fs.pipe_file("s3://bucket/k2", b"data")

        with pytest.raises(S3MultipartStateError):
            write_in_transaction()
        put_object.assert_not_called()

        with fs.transaction:
            fs.pipe_file("s3://bucket/k2", memoryview(b"ab" * 4)[::2])

        assert [(c.args[0].key, c.args[1]) for c in put_object.call_args_list] == [("k2", b"aaaa")]

    @pytest.mark.parametrize("error", [RuntimeError, PermissionError])
    def test_transaction_put_file_failed_write(self, tmp_path, error):
        # GH-1014: in a transaction, a failed write or a local file that
        # cannot be read does not replace the object with the data written
        # so far, or with an empty one, when the transaction commits.
        # GH-1211: the commit of the failed write raises, and fsspec discards
        # the rest of the transaction. A local file that cannot be read opens
        # no remote file.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        put_object = fs._sync_fs.core.put_object = mock.MagicMock()
        local = tmp_path / "local"
        local.write_bytes(b"a")
        failing = tmp_path / "failing"
        failing.write_bytes(b"b")
        if error is PermissionError:
            if os.geteuid() == 0:
                pytest.skip("root can read a file without read permission.")
            failing.chmod(0)

        def write_in_transaction():
            with fs.transaction:
                with (
                    mock.patch.object(AioS3File, "write", side_effect=RuntimeError("write failed"))
                    if error is RuntimeError
                    else contextlib.nullcontext(),
                    pytest.raises(error),
                ):
                    fs.put_file(str(failing), "s3://bucket/k1")
                fs.put_file(str(local), "s3://bucket/k2")

        if error is PermissionError:
            write_in_transaction()
            assert [(c.args[0].key, c.args[1]) for c in put_object.call_args_list] == [("k2", b"a")]
        else:
            with pytest.raises(S3MultipartStateError):
                write_in_transaction()
            put_object.assert_not_called()

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
            with pytest.raises(ValueError, match="in 3 parts with a block size of 4 bytes"):
                fs.pipe_file("s3://bucket/k1", b"a" * 13, **kwargs)
            with pytest.raises(ValueError, match="in 3 parts with a block size of 4 bytes"):
                fs.put_file(str(local), "s3://bucket/k2", **kwargs)
        fs.open.assert_not_called()
        fs._sync_fs._call.assert_not_called()

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
        # first path and dropped the DeleteObjects parameters. The async
        # filesystem sends the request of each bucket in parallel.
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
        # GH-962: _rm() did not pass maxdepth when expanding the path, so a
        # key below maxdepth was deleted too.
        fs = AioS3FileSystem(
            key="dummy", secret="dummy", region_name="us-east-1", skip_instance_cache=True
        )
        with Stubber(fs._sync_fs._client) as stubber:
            # One level of the directory: dir/sub/deep is not listed.
            stubber.add_response(
                "list_objects_v2",
                {
                    "Contents": [{"Key": "dir/a"}],
                    "CommonPrefixes": [{"Prefix": "dir/sub/"}],
                    "KeyCount": 2,
                },
                {"Bucket": "bucket", "Prefix": "dir/", "Delimiter": "/"},
            )
            stubber.add_response(
                "delete_objects",
                {},
                {
                    "Bucket": "bucket",
                    "Delete": {
                        "Objects": [{"Key": "dir"}, {"Key": "dir/a"}, {"Key": "dir/sub"}],
                        "Quiet": True,
                    },
                },
            )
            # batch_size is part of fsspec's async _rm() signature.
            await fs._rm("s3://bucket/dir", recursive=True, maxdepth=1, batch_size=10)
            stubber.assert_no_pending_responses()

    @pytest.mark.parametrize(("mode", "open_mode"), [("overwrite", "wb"), ("create", "xb")])
    def test_put_file_in_transaction_open_parameters(self, tmp_path, mode, open_mode):
        # GH-969: in a transaction, the open() parameters of put_file() go to
        # open(), and the other parameters, also in s3_additional_kwargs, to
        # S3. GH-972: fsspec's mode argument selects the mode of the file.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs.open = mock.MagicMock()
        lpath = tmp_path / "data.csv"
        lpath.write_bytes(b"a")

        with fs.transaction:
            fs.put_file(
                str(lpath),
                "s3://bucket/key",
                mode=mode,
                block_size=S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE,
                max_workers=2,
                StorageClass="STANDARD_IA",
            )
            # Without them, the file gets the defaults of the filesystem.
            fs.put_file(str(lpath), "s3://bucket/key", mode=mode)

        assert fs.open.call_args_list == [
            mock.call(
                "s3://bucket/key",
                open_mode,
                block_size=S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE,
                max_workers=2,
                s3_additional_kwargs={"StorageClass": "STANDARD_IA", "ContentType": "text/csv"},
            ),
            mock.call(
                "s3://bucket/key",
                open_mode,
                block_size=fs._sync_fs.default_block_size,
                max_workers=fs._sync_fs.max_workers,
                s3_additional_kwargs={"ContentType": "text/csv"},
            ),
        ]

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
    async def test_expand_path_glob_lists_stem_prefix(self):
        # Unversioned globs keep fsspec's async expansion, which lists only
        # the keys that start with the stem before the first wildcard.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        sync_fs = fs._sync_fs
        sync_fs._call = sync_fs._core.call = mock.MagicMock(
            side_effect=lambda method, **request: {"Contents": [], "KeyCount": 0}
        )

        with pytest.raises(FileNotFoundError):
            await fs._expand_path("s3://bucket/reports/2026-*.csv")
        listings = [
            c.kwargs
            for c in sync_fs._call.call_args_list
            if c.args[0] is sync_fs._client.list_objects_v2
        ]
        assert listings[0]["Prefix"] == "reports/2026-"

    @pytest.mark.parametrize(
        ("destination", "expected"),
        [
            # GH-979: a version is downloaded to a local path named after its
            # key.
            ("directory", "b"),
            # A Path destination is paired too: the version is downloaded to
            # the file, not into a directory of that name.
            ("path", "out.bin"),
        ],
        ids=["directory", "path"],
    )
    @pytest.mark.asyncio
    async def test_get_version(self, tmp_path, destination, expected):
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        fs._sync_fs.isdir = mock.MagicMock(return_value=False)
        fs._get_file = mock.AsyncMock()

        lpath = f"{tmp_path}/" if destination == "directory" else tmp_path / "out.bin"
        await fs._get("s3://bucket/b?versionId=v1", lpath)
        assert fs._get_file.await_args.args[:2] == (
            "bucket/b?versionId=v1",
            f"{tmp_path.as_posix()}/{expected}",
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

        await await_bounded(
            fs._cp_file(
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
            await await_bounded(fs._copy_object_with_multipart_upload(**kwargs))
            stubber.assert_no_pending_responses()

    @pytest.fixture(scope="class")
    def fs(self):
        return AioS3FileSystem(connection=connect())

    @pytest.mark.parametrize(
        "size",
        [
            10 * 2**20,  # > block size (5 MiB): multipart path via the async executor
        ],
    )
    def test_write_transaction(self, fs, size):
        # GH-719 regression for the async filesystem: the deferred commit of a
        # multipart write whose parts the async executor uploads. The
        # single-request path is S3File's, as in the sync test.
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

    @pytest.mark.parametrize(
        "size",
        [
            2**10,  # < block size: PutObject
            10 * 2**20,  # > block size (5 MiB): two parts of a multipart upload
        ],
    )
    @pytest.mark.asyncio
    async def test_open_async_write_read(self, fs, size):
        data = os.urandom(size)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_open_async/{uuid.uuid4()}"
        )
        async with await fs.open_async(path, "wb", block_size=5 * 2**20) as f:
            assert isinstance(f, AioS3StreamedFile)
            await f.write(data)
        async with await fs.open_async(path, "rb", block_size=5 * 2**20) as f:
            assert await f.read() == data
            f.seek(size // 2)
            assert await f.read(10) == data[size // 2 : size // 2 + 10]

    @pytest.mark.asyncio
    async def test_open_async_append_exclusive(self, fs):
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_open_async_append/{uuid.uuid4()}"
        )
        async with await fs.open_async(path, "xb") as f:
            await f.write(b"hello")
        with pytest.raises(FileExistsError):
            await fs.open_async(path, "xb")
        async with await fs.open_async(path, "ab") as f:
            await f.write(b" world")
        async with await fs.open_async(path, "rb") as f:
            assert await f.read() == b"hello world"

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

        # maxdepth reaches the listing: only prefix2 is one level down.
        result = await fs._find(dir_, maxdepth=1)
        assert result == [fs._strip_protocol(f"{dir_}/prefix2")]

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
    async def test_rm_recursive(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_rm_recursive"
        )

        files = [f"{dir_}/{uuid.uuid4()}" for _ in range(2)]
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
    @pytest.mark.parametrize(
        ("base", "exp"),
        [
            # Below the maximum part size, a single CopyObject request.
            (1, 2**10),
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

    @pytest.mark.skipif(
        os.getenv("AWS_ATHENA_S3_VERSIONING_TESTS") != "1",
        reason="Set AWS_ATHENA_S3_VERSIONING_TESTS=1 to create versioning test buckets.",
    )
    @pytest.mark.parametrize(
        "status",
        [
            pytest.param(None, id="async-None"),
            pytest.param("Enabled", id="async-Enabled"),
            pytest.param("Suspended", id="async-Suspended"),
        ],
    )
    @pytest.mark.asyncio
    async def test_move_null_version_onto_key(self, fs, versioning_buckets, status):
        client, buckets = versioning_buckets
        bucket = buckets[status]
        key = "async"
        path = f"s3://{bucket}/{key}"
        before = [
            v
            for v in client.list_object_versions(Bucket=bucket, Prefix=key)["Versions"]
            if v["Key"] == key
        ]
        assert any(v["VersionId"] == "null" for v in before)
        if status:
            assert not next(v for v in before if v["VersionId"] == "null")["IsLatest"]

        await fs._mv(f"{path}?versionId=null", path)

        with client.get_object(Bucket=bucket, Key=key)["Body"] as body:
            assert body.read() == (b"original" if status != "Suspended" else b"current")
        after = [
            v
            for v in client.list_object_versions(Bucket=bucket, Prefix=key)["Versions"]
            if v["Key"] == key
        ]
        if status == "Enabled":
            assert not any(v["VersionId"] == "null" for v in after)
            assert len(after) == len(before)
            latest = next(v for v in after if v["IsLatest"])
            assert latest["VersionId"] not in {v["VersionId"] for v in before}
        else:
            assert after == before

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

    def test_pandas_read_csv(self):
        import pandas

        df = pandas.read_csv(
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}",
            header=None,
            names=["col"],
        )
        assert [(row["col"],) for _, row in df.iterrows()] == [(123456789,)]


class TestAioS3File:
    @pytest.mark.parametrize("max_workers", [2, None])
    def test_open_max_workers(self, max_workers):
        # GH-1158: None takes the default of the filesystem. It used to raise
        # TypeError when the executor of the file was created.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        with fs.open("s3://bucket/key", "wb", max_workers=max_workers) as f:
            assert isinstance(f, AioS3File)
            assert f.max_workers == (max_workers or fs._sync_fs.max_workers)

    @pytest.mark.asyncio
    async def test_failed_read_frees_other_ranges(self):
        # GH-1186: a parallel read that fails frees the data of the other
        # ranges, also while its error is held, as with S3File.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        block_size = 2**10
        read = []
        others_read = threading.Semaphore(0)

        def get_object(path, range_, **kwargs):
            if range_[0] == 0:
                for _ in range(3):
                    assert others_read.acquire(timeout=5)
                raise PermissionError("Access Denied")
            # Unlike bytes, an array can be weakly referenced.
            data = array.array("B", bytes(range_[1] - range_[0]))
            read.append(weakref.ref(data))
            others_read.release()
            return data

        fs.core.get_object = mock.MagicMock(side_effect=get_object)
        info = S3Object(
            init={"Key": "key", "ETag": '"etag"'},
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="bucket",
            key="key",
        )
        info.size = block_size * 4
        fs._sync_fs.info = mock.MagicMock(return_value=info)
        # The executor of the file dispatches through the event loop, which
        # the blocking calls must not hold.
        f = await asyncio.to_thread(
            fs.open, "s3://bucket/key", "rb", block_size=block_size, cache_type="none"
        )
        with pytest.raises(PermissionError, match="Access Denied") as info_:
            await asyncio.to_thread(f.read)
        # Waits for the reads, whose threads may still set the results.
        await asyncio.to_thread(f.close)
        assert len(read) == 3
        assert [ref() for ref in read] == [None] * 3
        assert info_.value.__traceback__ is not None

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
            def call(*args, **kwargs):
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
                return result(*args, **kwargs)

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
        sync_fs.core.get_object = track(lambda path, range_, **kw: b"a" * (range_[1] - range_[0]))
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
        assert sync_fs.core.get_object.call_count == 4
        assert state["peak"] == 2

    @pytest.mark.asyncio
    async def test_open_parts_in_flight(self):
        # GH-1158: a write keeps at most max_workers parts in flight with the
        # S3AioExecutor of the file; the S3ThreadPoolExecutor of an
        # asynchronous filesystem is that of S3File. The parts waiting for a
        # worker used to be queued with their data.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        block_size = S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE
        gates: dict[int, threading.Event] = {}

        def upload_part(**kwargs):
            # Uploading until the test releases the part.
            assert gates[kwargs["part_number"]].wait(5)
            return S3MultipartUploadPart(kwargs["part_number"], {"ETag": '"e"'})

        sync_fs = fs._sync_fs
        sync_fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"}
            )
        )
        sync_fs.core.upload_part = mock.MagicMock(side_effect=upload_part)
        sync_fs.core.complete_multipart_upload = mock.MagicMock()
        submit_to = S3Executor.submit_to
        uploads = {}
        in_flight = []

        def record_upload(executor, future, fn, *args, **kwargs):
            if "body" not in kwargs:
                submit_to(executor, future, fn, *args, **kwargs)
                return
            gates[kwargs["part_number"]] = threading.Event()
            uploads[kwargs["part_number"]] = future
            in_flight.append(sum(not upload.done() for upload in uploads.values()))
            submit_to(executor, future, fn, *args, **kwargs)
            if in_flight[-1] >= 2:
                # Only the newest part in flight finishes, so a write that
                # waits for an older one times out.
                gates[kwargs["part_number"]].set()

        def write():
            with fs.open("s3://bucket/key", "wb", block_size=block_size, max_workers=2) as f:
                f.write(b"a" * block_size * 4)
                for gate in gates.values():
                    gate.set()

        with mock.patch.object(S3Executor, "submit_to", autospec=True, side_effect=record_upload):
            await asyncio.to_thread(write)

        assert sync_fs.core.upload_part.call_count == 4
        sync_fs.core.complete_multipart_upload.assert_called_once()
        assert in_flight == [1, 2, 2, 2]

    @pytest.mark.asyncio
    async def test_open_failed_part_while_waiting(self):
        # GH-1173: a part found failed when a write waits for a part stops the
        # write with its error with the S3AioExecutor of the file, as with
        # that of S3File. The write used to upload the rest of the data.
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        block_size = S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE
        sync_fs = fs._sync_fs
        sync_fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"}
            )
        )
        release = threading.Event()

        def upload_part(**kwargs):
            # Failing once max_workers parts are in flight.
            assert release.wait(5)
            raise PermissionError("Access Denied")

        sync_fs.core.upload_part = mock.MagicMock(side_effect=upload_part)
        sync_fs.core.abort_multipart_upload = mock.MagicMock()
        sync_fs.core.complete_multipart_upload = mock.MagicMock()
        submit_to = S3Executor.submit_to
        submitted = []

        def record_upload(executor, future, fn, *args, **kwargs):
            if "body" in kwargs:
                submitted.append(kwargs["part_number"])
            submit_to(executor, future, fn, *args, **kwargs)
            if len(submitted) == 2:
                release.set()

        def write():
            with fs.open("s3://bucket/key", "wb", block_size=block_size, max_workers=2) as f:
                f.write(b"a" * block_size * 3)

        with (
            mock.patch.object(S3Executor, "submit_to", autospec=True, side_effect=record_upload),
            pytest.raises(PermissionError, match="Access Denied"),
        ):
            await asyncio.to_thread(write)

        assert submitted == [1, 2]
        sync_fs.core.abort_multipart_upload.assert_called_once()
        sync_fs.core.complete_multipart_upload.assert_not_called()

    def test_open_invalid_max_workers(self):
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        with pytest.raises(ValueError, match="max_workers must be greater than 0"):
            fs.open("s3://bucket/key", "wb", max_workers=0)

    def test_open_lookup_parameters(self):
        # GH-1004: the lookup of the file sends its lookup parameters, and
        # looks up the version that is opened.
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

        with fs.open(
            "s3://bucket/key", "rb", version_id="v1", ContentType="text/plain", **sse_c
        ) as f:
            assert isinstance(f, AioS3File)
            assert f.version_id == "v1"
            assert f.size == 4
        fs._sync_fs.info.assert_called_once_with(
            "bucket/key?versionId=v1", version_id="v1", **sse_c
        )


class TestAioS3StreamedFile:
    @pytest.fixture
    def fs(self):
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        core = fs._sync_fs.core
        core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"}
            )
        )
        core.upload_part = mock.MagicMock(
            side_effect=lambda **kw: S3MultipartUploadPart(kw["part_number"], {"ETag": '"e"'})
        )
        core.complete_multipart_upload = mock.MagicMock()
        core.abort_multipart_upload = mock.MagicMock()
        core.put_object = mock.MagicMock()
        # The mocked client has no operation model to select the parameters.
        core.operation_params = mock.MagicMock(side_effect=lambda method, params: dict(params))
        return fs

    @pytest.mark.asyncio
    async def test_write_parts_in_flight(self, fs):
        # GH-1159: a write keeps at most max_workers parts in flight, and waits
        # for one to finish without blocking the event loop, on which the
        # parts are released here.
        block_size = S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE
        lock = threading.Lock()
        gates: dict[int, threading.Event] = {}
        active: list[int] = []
        uploads: dict[int, Future[S3MultipartUploadPart]] = {}
        in_flight: list[int] = []
        submit_to = S3AioThreadPool.submit_to

        def record_upload(pool, future, fn, *args, **kwargs):
            # The parts submitted and not finished, including those that
            # wait for a thread with their data.
            if "body" in kwargs:
                uploads[kwargs["part_number"]] = future
                in_flight.append(sum(not upload.done() for upload in uploads.values()))
            submit_to(pool, future, fn, *args, **kwargs)

        def upload_part(**kw):
            part_number = kw["part_number"]
            with lock:
                gate = gates.setdefault(part_number, threading.Event())
                active.append(part_number)
            if not gate.wait(5):
                raise TimeoutError(part_number)
            with lock:
                active.remove(part_number)
            return S3MultipartUploadPart(part_number, {"ETag": '"e"'})

        fs.core.upload_part = mock.MagicMock(side_effect=upload_part)
        written = asyncio.Event()

        async def release():
            deadline = time.monotonic() + EVENT_TIMEOUT
            while True:
                assert time.monotonic() < deadline
                with lock:
                    if written.is_set() or len(active) >= 2:
                        # Only the newest part finishes until the data is
                        # written, so a write that waits for the oldest one
                        # would time out.
                        for part_number in active if written.is_set() else active[-1:]:
                            gates.setdefault(part_number, threading.Event()).set()
                    if written.is_set() and not active:
                        return
                await asyncio.sleep(0.01)

        releaser = asyncio.ensure_future(release())
        f = await fs.open_async("s3://bucket/key", "wb", block_size=block_size, max_workers=2)
        with mock.patch.object(
            S3AioThreadPool, "submit_to", autospec=True, side_effect=record_upload
        ):
            await f.write(b"a" * block_size * 4)
            written.set()
            await f.close()
        await releaser

        assert len(in_flight) == 4
        assert max(in_flight) <= 2
        (_, parts), _ = fs.core.complete_multipart_upload.call_args
        assert [part.part_number for part in parts] == [1, 2, 3, 4]
        fs.core.abort_multipart_upload.assert_not_called()

    @pytest.mark.asyncio
    async def test_write_failed_part_while_waiting(self, fs):
        # A part found failed when a write waits for a part stops the write
        # with its error and aborts the upload.
        block_size = S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE
        # Both parts start before either fails; otherwise the cleanup after a
        # failure may cancel the other one before a thread takes it.
        started = threading.Barrier(2, timeout=5)

        def upload_part(**kwargs):
            started.wait()
            raise PermissionError("Access Denied")

        fs.core.upload_part = mock.MagicMock(side_effect=upload_part)
        f = await fs.open_async("s3://bucket/key", "wb", block_size=block_size, max_workers=2)

        with pytest.raises(PermissionError, match="Access Denied"):
            await f.write(b"a" * block_size * 3)

        assert f.closed
        assert sorted(c.kwargs["part_number"] for c in fs.core.upload_part.call_args_list) == [
            1,
            2,
        ]
        fs.core.abort_multipart_upload.assert_called_once()
        fs.core.complete_multipart_upload.assert_not_called()

    @pytest.mark.parametrize("blocks", [0, 2])
    @pytest.mark.asyncio
    async def test_exit_with_exception(self, fs, blocks):
        # Leaving the block with an exception does not commit the written
        # data, and aborts the multipart upload, if any.
        block_size = S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE
        f = await fs.open_async("s3://bucket/key", "wb", block_size=block_size)

        async def write_and_fail():
            async with f:
                await f.write(b"a" * (block_size * blocks + 1))
                raise RuntimeError("failed")

        with pytest.raises(RuntimeError, match="failed"):
            await write_and_fail()

        assert f.closed
        fs.core.put_object.assert_not_called()
        fs.core.complete_multipart_upload.assert_not_called()
        assert fs.core.abort_multipart_upload.call_count == (1 if blocks else 0)

    @pytest.mark.asyncio
    async def test_exit_with_exception_after_close(self, fs):
        # An exception raised after the file was closed propagates unchanged.
        f = await fs.open_async("s3://bucket/key", "wb")

        async def close_and_fail():
            async with f:
                await f.write(b"abc")
                await f.close()
                raise KeyError("failed")

        with pytest.raises(KeyError, match="failed"):
            await close_and_fail()

        fs.core.put_object.assert_called_once()
        fs.core.abort_multipart_upload.assert_not_called()

    @pytest.mark.asyncio
    async def test_sync_with(self, fs):
        # A synchronous with block cannot await close(), so it is rejected
        # instead of leaving the data uncommitted.
        f = await fs.open_async("s3://bucket/key", "wb")
        with pytest.raises(TypeError, match="async with"), f:
            pass
        await f.close()

    @pytest.mark.asyncio
    async def test_garbage_collected_unclosed(self, fs):
        # An unclosed file that is garbage collected warns, and aborts its
        # multipart upload instead of leaving it incomplete.
        block_size = S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE
        aborted = threading.Event()
        fs.core.abort_multipart_upload = mock.MagicMock(side_effect=lambda *a, **k: aborted.set())

        async def write_unclosed():
            # Created directly: the futures of open_async() reference the
            # file until the garbage collector breaks their cycle.
            f = AioS3StreamedFile(fs, "bucket/key", "wb", block_size=block_size)
            await f.write(b"a" * block_size * 2)

        # The file is released when the coroutine returns.
        with pytest.warns(ResourceWarning, match="was not closed"):
            await write_unclosed()
        assert await asyncio.to_thread(aborted.wait, 5)

        fs.core.complete_multipart_upload.assert_not_called()

    @pytest.mark.asyncio
    async def test_append_failed_part_copy(self, fs):
        # A part copy of an append found failed before a part is submitted
        # stops the write with its error, as with S3File (GH-1178).
        block_size = S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE
        info = S3Object(
            init={"Key": "key"},
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="bucket",
            key="key",
        )
        info.size = block_size
        fs._sync_fs.info = mock.MagicMock(return_value=info)
        release = threading.Event()

        def upload_part_copy(**kwargs):
            # Held until the first write returns, so that it does not see
            # the failure.
            release.wait(EVENT_TIMEOUT)
            raise PermissionError("Access Denied")

        fs.core.upload_part_copy = mock.MagicMock(side_effect=upload_part_copy)
        f = await fs.open_async("s3://bucket/key", "ab", block_size=block_size)
        try:
            await f.write(b"a" * block_size)
        finally:
            release.set()
        # The copy has failed before the next write submits a part.
        await asyncio.to_thread(wait, f._parts[:1], EVENT_TIMEOUT)

        with pytest.raises(PermissionError, match="Access Denied"):
            await f.write(b"a" * block_size)

        assert f.closed
        assert fs.core.upload_part.call_count == 1
        fs.core.abort_multipart_upload.assert_called_once()
        fs.core.complete_multipart_upload.assert_not_called()

    @pytest.mark.asyncio
    async def test_commit_failed_part_first(self, fs):
        # The commit raises a failed part without waiting for the parts
        # before it, and cancels those that have not started, as S3File does.
        f = await fs.open_async("s3://bucket/key", "wb")
        await f.write(b"a")
        f._writer.upload = S3MultipartUpload(
            {"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"}
        )
        failed: Future[S3MultipartUploadPart] = Future()
        failed.set_exception(PermissionError("Access Denied"))
        pending: list[Future[S3MultipartUploadPart]] = [Future(), Future()]
        f._parts = [pending[0], failed, pending[1]]

        try:
            with pytest.raises(PermissionError, match="Access Denied"):
                await asyncio.wait_for(asyncio.to_thread(f._complete_upload), 5)
        finally:
            # Releases a commit that waits for the parts in order.
            for part in pending:
                if not part.done():
                    part.set_result(S3MultipartUploadPart(1, {"ETag": '"e"'}))

        assert all(part.cancelled() for part in pending)
        fs.core.abort_multipart_upload.assert_called_once()
        fs.core.complete_multipart_upload.assert_not_called()
        # Closed without committing again.
        f.closed = True
        f._pool.shutdown()

    @pytest.mark.parametrize("cancellations", [1, 2])
    @pytest.mark.asyncio
    async def test_write_cancelled(self, fs, cancellations):
        # A cancelled write waits for the running parts in the threads of the
        # file and aborts the upload, also after a repeated cancellation.
        block_size = S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE
        events = []
        lock = threading.Lock()
        started = threading.Semaphore(0)
        release = threading.Event()
        aborted = threading.Event()

        def upload_part(**kw):
            part_number = kw["part_number"]
            with lock:
                events.append(f"start {part_number}")
            started.release()
            # The finally block of the test always releases it.
            release.wait()
            with lock:
                events.append(f"end {part_number}")
            return S3MultipartUploadPart(part_number, {"ETag": '"e"'})

        def abort_multipart_upload(*args, **kwargs):
            events.append("abort")
            aborted.set()

        fs.core.upload_part = mock.MagicMock(side_effect=upload_part)
        fs.core.abort_multipart_upload = mock.MagicMock(side_effect=abort_multipart_upload)
        f = await fs.open_async("s3://bucket/key", "wb", block_size=block_size, max_workers=2)
        task = asyncio.ensure_future(f.write(b"a" * block_size * 3))
        try:
            for _ in range(2):
                assert await asyncio.to_thread(started.acquire, timeout=5)
            for _ in range(cancellations):
                task.cancel()
                # Lets the write enter its cleanup.
                await asyncio.sleep(0)
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await asyncio.to_thread(aborted.wait, 5)

        assert f.closed
        assert sorted(events[:2]) == ["start 1", "start 2"]
        assert sorted(events[2:4]) == ["end 1", "end 2"]
        assert events[4:] == ["abort"]
        fs.core.complete_multipart_upload.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_cancelled(self, fs):
        # A commit that has started completes even if the task that closes
        # the file is cancelled, and the upload is not aborted.
        block_size = S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE
        started = threading.Event()
        release = threading.Event()
        completed = threading.Event()

        def complete_multipart_upload(*args, **kwargs):
            started.set()
            # The finally block of the test always releases it.
            release.wait()
            completed.set()

        fs.core.complete_multipart_upload = mock.MagicMock(side_effect=complete_multipart_upload)
        f = await fs.open_async("s3://bucket/key", "wb", block_size=block_size)
        await f.write(b"a" * block_size * 2)
        task = asyncio.ensure_future(f.close())
        try:
            assert await asyncio.to_thread(started.wait, 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()
        assert await asyncio.to_thread(completed.wait, 5)

        assert f.closed
        fs.core.complete_multipart_upload.assert_called_once()
        fs.core.abort_multipart_upload.assert_not_called()

    @pytest.mark.asyncio
    async def test_flush_cancelled_then_exit_with_exception(self, fs):
        # Leaving the block with an exception after a cancelled final flush
        # does not abort the upload that the running commit completes.
        block_size = S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE
        started = threading.Event()
        release = threading.Event()
        completed = threading.Event()

        def complete_multipart_upload(*args, **kwargs):
            started.set()
            # The finally block of the test always releases it.
            release.wait()
            completed.set()

        fs.core.complete_multipart_upload = mock.MagicMock(side_effect=complete_multipart_upload)
        f = await fs.open_async("s3://bucket/key", "wb", block_size=block_size)

        async def flush_and_exit():
            async with f:
                await f.write(b"a" * block_size * 2)
                flush = asyncio.ensure_future(f.flush(force=True))
                assert await asyncio.to_thread(started.wait, 5)
                flush.cancel()
                await flush

        try:
            with pytest.raises(asyncio.CancelledError):
                await flush_and_exit()
        finally:
            release.set()
        assert await asyncio.to_thread(completed.wait, 5)
        # Lets an abort that was submitted run.
        await asyncio.sleep(0.1)

        assert f.closed
        fs.core.complete_multipart_upload.assert_called_once()
        fs.core.abort_multipart_upload.assert_not_called()

    @pytest.mark.parametrize("data", [b"", b"abc"])
    @pytest.mark.asyncio
    async def test_close_small(self, fs, data):
        # Data smaller than a block is uploaded with PutObject, and no data
        # creates an empty object.
        fs._sync_fs.touch = mock.MagicMock()
        async with await fs.open_async("s3://bucket/key", "wb", ContentType="text/plain") as f:
            await f.write(data)

        if data:
            fs.core.put_object.assert_called_once_with(
                S3Path("bucket", "key"), data, ContentType="text/plain"
            )
            fs._sync_fs.touch.assert_not_called()
        else:
            fs._sync_fs.touch.assert_called_once_with("bucket/key", ContentType="text/plain")
            fs.core.put_object.assert_not_called()
        fs.core.create_multipart_upload.assert_not_called()

    @pytest.mark.asyncio
    async def test_failed_read_frees_other_ranges(self, fs):
        # GH-1186: a parallel read that fails cancels the reads that have not
        # finished and frees the data of the others, also while its error is
        # held, as with S3File.
        block_size = 2**10
        read = []
        others_read = threading.Semaphore(0)

        def get_object(path, range_, **kwargs):
            if range_[0] == 0:
                for _ in range(3):
                    assert others_read.acquire(timeout=5)
                raise PermissionError("Access Denied")
            # Unlike bytes, an array can be weakly referenced.
            data = array.array("B", bytes(range_[1] - range_[0]))
            read.append(weakref.ref(data))
            others_read.release()
            return data

        fs.core.get_object = mock.MagicMock(side_effect=get_object)
        info = S3Object(
            init={"Key": "key", "ETag": '"etag"'},
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="bucket",
            key="key",
        )
        info.size = block_size * 4
        fs._sync_fs.info = mock.MagicMock(return_value=info)
        f = await fs.open_async("s3://bucket/key", "rb", block_size=block_size, max_workers=4)
        with pytest.raises(PermissionError, match="Access Denied") as info_:
            await f.read()
        await f.close()
        # The threads have set the results once the pool of the file is shut
        # down, and the event loop runs their callbacks on the next iterations.
        await asyncio.to_thread(f._pool.shutdown, True)
        for _ in range(3):
            await asyncio.sleep(0)
        assert len(read) == 3
        assert [ref() for ref in read] == [None] * 3
        assert info_.value.__traceback__ is not None

    @pytest.mark.asyncio
    async def test_failed_read_stops_running_reads(self, fs):
        # GH-1203: a failed parallel read stops the retries of the running
        # reads, whose backoff wait would take at least EVENT_TIMEOUT seconds.
        block_size = 2**10
        operation = ThrottledOperation()
        throttled = retried(operation, RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT))

        def get_object(path, range_, **kwargs):
            if range_[0] == 0:
                assert operation.started.wait(EVENT_TIMEOUT)
                raise PermissionError("Access Denied")
            return throttled()

        fs.core.get_object = mock.MagicMock(side_effect=get_object)
        info = S3Object(
            init={"Key": "key", "ETag": '"etag"'},
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="bucket",
            key="key",
        )
        info.size = block_size * 2
        fs._sync_fs.info = mock.MagicMock(return_value=info)
        f = await fs.open_async("s3://bucket/key", "rb", block_size=block_size, max_workers=2)
        started = time.monotonic()
        with pytest.raises(PermissionError, match="Access Denied"):
            await f.read()
        await f.close()
        # Waits for the thread of the running read.
        await asyncio.to_thread(f._pool.shutdown, True)
        assert time.monotonic() - started < EVENT_TIMEOUT
        assert operation.calls == 1

    @pytest.mark.asyncio
    async def test_failed_part_stops_running_parts(self, fs):
        # GH-1203: a failed part stops the retries of the running parts before
        # the upload is aborted; their backoff wait would take at least
        # EVENT_TIMEOUT seconds.
        operation = ThrottledOperation(operation_name="UploadPart")
        throttled = retried(operation, RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT))

        def upload_part(part_number, **kwargs):
            if part_number == 1:
                assert operation.started.wait(EVENT_TIMEOUT)
                raise PermissionError("Access Denied")
            return throttled()

        fs.core.upload_part = mock.MagicMock(side_effect=upload_part)
        block_size = S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE
        f = await fs.open_async("s3://bucket/key", "wb", block_size=block_size, max_workers=2)
        await f.write(b"x" * (2 * block_size))
        started = time.monotonic()
        with pytest.raises(PermissionError, match="Access Denied"):
            await f.close()
        assert time.monotonic() - started < EVENT_TIMEOUT
        assert operation.calls == 1
        fs.core.abort_multipart_upload.assert_called_once()

    @pytest.mark.asyncio
    async def test_cancelled_creation_stops_retries(self, fs):
        # GH-1203: cancelling a write stops the retries of the creation of its
        # multipart upload, which the cleanup waits for; their backoff wait
        # would take at least EVENT_TIMEOUT seconds.
        operation = ThrottledOperation(block=True, operation_name="CreateMultipartUpload")
        fs.core.create_multipart_upload = retried(
            operation, RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT)
        )
        block_size = S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE
        # The creation holds the only thread, after which the cleanup waits.
        f = await fs.open_async("s3://bucket/key", "wb", block_size=block_size, max_workers=1)
        task = asyncio.ensure_future(f.write(b"x" * block_size))
        try:
            assert await asyncio.to_thread(operation.started.wait, EVENT_TIMEOUT)
            task.cancel()
        finally:
            operation.release.set()
        started = time.monotonic()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert time.monotonic() - started < EVENT_TIMEOUT
        assert operation.calls == 1
        assert f.closed

    @pytest.mark.asyncio
    async def test_cancelled_read_frees_ranges(self, fs):
        # GH-1186: a cancelled parallel read cancels the reads that have not
        # finished, which stops their retries, and frees the data of the
        # others, also while the cancellation is held, as a failed read does.
        block_size = 2**10
        loop = asyncio.get_running_loop()
        read = []
        all_read = threading.Barrier(4, timeout=EVENT_TIMEOUT)
        held = threading.Event()
        # The held range throttles and would retry after EVENT_TIMEOUT.
        operation = ThrottledOperation(failures=1)
        throttled = retried(operation, RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT))

        async def others_done():
            def running_reads():
                return [
                    t
                    for t in asyncio.all_tasks()
                    if t.get_coro().__qualname__ == "S3AioThreadPool.run" and not t.done()
                ]

            # At least the held read is found.
            assert running_reads()
            await wait_until(lambda: len(running_reads()) <= 1)

        def get_object(path, range_, **kwargs):
            if range_[0] == 0:
                all_read.wait()
                # Retrying when the read is cancelled, once the tasks of the
                # other ranges have taken their results.
                asyncio.run_coroutine_threadsafe(others_done(), loop).result(EVENT_TIMEOUT)
                held.set()
                try:
                    throttled()
                except Exception as e:
                    # The stopped retry keeps its error in a reference cycle
                    # through the frames of its traceback, which would keep
                    # this frame and the data, as S3Core.call() avoids.
                    traceback.clear_frames(e.__traceback__)
            # Unlike bytes, an array can be weakly referenced.
            data = array.array("B", bytes(range_[1] - range_[0]))
            read.append(weakref.ref(data))
            if range_[0] != 0:
                all_read.wait()
            return data

        fs.core.get_object = mock.MagicMock(side_effect=get_object)
        info = S3Object(
            init={"Key": "key", "ETag": '"etag"'},
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="bucket",
            key="key",
        )
        info.size = block_size * 4
        fs._sync_fs.info = mock.MagicMock(return_value=info)

        f = await fs.open_async("s3://bucket/key", "rb", block_size=block_size, max_workers=4)
        task = asyncio.ensure_future(f.read())
        assert await asyncio.to_thread(held.wait, EVENT_TIMEOUT)
        started = time.monotonic()
        task.cancel()
        with pytest.raises(asyncio.CancelledError) as info_:
            await asyncio.wait_for(task, EVENT_TIMEOUT)
        await f.close()
        await asyncio.wait_for(asyncio.to_thread(f._pool.shutdown, True), EVENT_TIMEOUT)
        # The retries of the held range stopped.
        assert time.monotonic() - started < EVENT_TIMEOUT
        assert operation.calls == 1
        for _ in range(3):
            await asyncio.sleep(0)
        assert len(read) == 4
        assert [ref() for ref in read] == [None] * 4
        assert info_.value.__traceback__ is not None

    @pytest.mark.asyncio
    async def test_read_parallel(self, fs):
        # A read runs at most max_workers range requests at once, conditional
        # on the ETag of the object.
        block_size = 2**10
        size = block_size * 4
        call, state = overlapping_calls(2)

        def get_object(path, range_, **kwargs):
            call()
            return b"a" * (range_[1] - range_[0])

        fs.core.get_object = mock.MagicMock(side_effect=get_object)
        info = S3Object(
            init={"Key": "key", "ETag": '"etag"'},
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="bucket",
            key="key",
        )
        info.size = size
        fs._sync_fs.info = mock.MagicMock(return_value=info)

        async with await fs.open_async(
            "s3://bucket/key", "rb", block_size=block_size, max_workers=2
        ) as f:
            assert await f.read() == b"a" * size
            assert await f.read() == b""

        assert fs.core.get_object.call_count == 4
        assert state["peak"] == 2
        for c in fs.core.get_object.call_args_list:
            assert c.kwargs == {"IfMatch": '"etag"'}

    @pytest.mark.parametrize(
        ("mode", "kwargs", "error"),
        [
            ("r", {}, ValueError),
            ("wb", {"compression": "gzip"}, ValueError),
            ("r+b", {}, NotImplementedError),
            ("wb", {"max_workers": 0}, ValueError),
            ("wb", {"block_size": 2**10}, ValueError),
            ("wb", {"version_id": "v"}, ValueError),
            ("wb", {"autocommit": False}, NotImplementedError),
        ],
    )
    @pytest.mark.asyncio
    async def test_open_async_invalid(self, fs, mode, kwargs, error):
        with pytest.raises(error):
            await fs.open_async("s3://bucket/key", mode, **kwargs)

    @pytest.mark.asyncio
    async def test_open_async_in_transaction(self, fs):
        # The file always commits when it is closed, so it cannot write in a
        # transaction, which defers the commit.
        with fs.transaction, pytest.raises(NotImplementedError, match="transaction"):
            await fs.open_async("s3://bucket/key", "wb")

    @pytest.mark.parametrize("exists", [False, True])
    @pytest.mark.asyncio
    async def test_exclusive_create(self, fs, exists):
        fs._sync_fs.exists = mock.MagicMock(return_value=exists)
        if exists:
            with pytest.raises(FileExistsError):
                await fs.open_async("s3://bucket/key", "xb")
            return
        async with await fs.open_async("s3://bucket/key", "xb") as f:
            await f.write(b"abc")

        fs.core.put_object.assert_called_once_with(S3Path("bucket", "key"), b"abc", IfNoneMatch="*")

    @pytest.mark.parametrize("size", [3, S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE])
    @pytest.mark.asyncio
    async def test_append(self, fs, size):
        # An existing object smaller than a part is rewritten from the buffer,
        # and a larger one is copied as the first part.
        info = S3Object(
            init={"Key": "key", "ContentType": "text/plain"},
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="bucket",
            key="key",
        )
        info.size = size
        fs._sync_fs.info = mock.MagicMock(return_value=info)
        fs._sync_fs.cat_file = mock.MagicMock(return_value=b"a" * size)
        fs.core.upload_part_copy = mock.MagicMock(
            side_effect=lambda **kw: S3MultipartUploadPart(kw["part_number"], {"ETag": '"c"'})
        )

        async with await fs.open_async("s3://bucket/key", "ab") as f:
            await f.write(b"b")

        if size < S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE:
            fs.core.put_object.assert_called_once()
            assert fs.core.put_object.call_args.args == (S3Path("bucket", "key"), b"aaab")
            # The rewritten object keeps the metadata of the existing one.
            assert fs.core.put_object.call_args.kwargs["ContentType"] == "text/plain"
            fs.core.upload_part_copy.assert_not_called()
        else:
            fs.core.put_object.assert_not_called()
            fs.core.upload_part_copy.assert_called_once()
            assert fs.core.upload_part_copy.call_args.kwargs["range_"] is None
            (_, parts), _ = fs.core.complete_multipart_upload.call_args
            assert [part.part_number for part in parts] == [1, 2]
