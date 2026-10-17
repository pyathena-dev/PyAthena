# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import gc
import io
import weakref
from concurrent.futures import CancelledError
from types import SimpleNamespace
from unittest import mock

import boto3
import pytest
from botocore.stub import Stubber

from pyathena.filesystem.s3_core import S3Core
from pyathena.filesystem.s3_executor import S3ThreadPoolExecutor
from pyathena.filesystem.s3_object import (
    S3CompleteMultipartUpload,
    S3MultipartUpload,
    S3MultipartUploadPart,
)
from pyathena.filesystem.s3_path import S3Path
from pyathena.filesystem.s3_writer import (
    S3MultipartStateError,
    S3MultipartWriter,
    _S3WriteSession,
)
from pyathena.util import RetryConfig


def _make_writer(block_size=4, **kwargs):
    client = boto3.client(
        "s3", region_name="us-east-1", aws_access_key_id="dummy", aws_secret_access_key="dummy"
    )
    core = S3Core(client, retry_config=RetryConfig(attempt=1))
    core.MULTIPART_UPLOAD_MIN_PART_SIZE = 4
    core.MULTIPART_UPLOAD_MAX_PART_SIZE = 8
    core.MULTIPART_UPLOAD_MAX_PARTS = 3
    return S3MultipartWriter(
        core, S3Path("bucket", "key"), block_size=block_size, **kwargs
    ), Stubber(client)


class TestS3MultipartWriter:
    @pytest.mark.parametrize(
        ("data", "block_size", "sizes"),
        [
            (b"", 4, []),
            (b"abc", 4, [3]),
            (b"abcdefghij", 4, [4, 6]),
            (b"abcdefghijkl", 4, [4, 4, 4]),
            (b"abcdefgh", 6, [4, 4]),
        ],
    )
    def test_writer_parts(self, data, block_size, sizes):
        writer, stubber = _make_writer(block_size)
        stream = io.BytesIO(b"skip" + data)
        stream.seek(4)
        with stubber:
            parts = list(writer.iter_parts(stream))
        assert [number for number, _ in parts] == list(range(1, len(sizes) + 1))
        assert [len(body) for _, body in parts] == sizes
        assert b"".join(body for _, body in parts) == data
        assert stream.tell() == len(data) + 4

    def test_writer_short_reads(self):
        class ShortReads(io.BytesIO):
            def read(self, size=-1):
                return super().read(min(size, 2))

        writer, _ = _make_writer()
        assert list(writer.iter_parts(ShortReads(b"abcdefghij"))) == [(1, b"abcd"), (2, b"efghij")]

    @pytest.mark.parametrize(
        ("size", "ranges"),
        [(0, []), (8, [(0, 8)]), (10, [(0, 5), (5, 10)]), (17, [(0, 8), (8, 12), (12, 17)])],
    )
    def test_writer_copy_parts(self, size, ranges):
        writer, stubber = _make_writer()
        with stubber:
            parts = list(writer.iter_copy_parts(size))
        assert parts == list(enumerate(ranges, 1))

    def test_writer_part_limit_includes_copies(self):
        writer, _ = _make_writer()
        copies = list(writer.iter_copy_parts(10))
        parts = writer.iter_parts(io.BytesIO(b"abcdefgh"), first_part_number=len(copies) + 1)
        assert next(parts) == (3, b"abcd")
        with pytest.raises(ValueError, match="including parts copied"):
            next(parts)

    def test_writer_empty_final_buffer_at_part_limit(self):
        writer, _ = _make_writer()
        assert list(writer.iter_parts(io.BytesIO(), first_part_number=4)) == []

    @pytest.mark.parametrize("method", ["iter_parts", "iter_copy_parts"])
    @pytest.mark.parametrize("number", [0, 4])
    def test_writer_invalid_first_number(self, method, number):
        writer, _ = _make_writer()
        data = io.BytesIO(b"abc") if method == "iter_parts" else 4
        with pytest.raises(ValueError, match=r"part_number|Cannot upload more"):
            list(getattr(writer, method)(data, first_part_number=number))

    @pytest.mark.parametrize("block_size", [0, 3, 9])
    def test_writer_invalid_block_size(self, block_size):
        with pytest.raises(ValueError, match="block_size"):
            _make_writer(block_size)

    @pytest.mark.parametrize("path", [S3Path("bucket"), S3Path("bucket", "key", "version")])
    def test_writer_invalid_destination(self, path):
        writer, _ = _make_writer()
        with pytest.raises(ValueError, match=r"has no key|Cannot write to a version"):
            S3MultipartWriter(writer._core, path, block_size=4)

    def test_writer_requests(self):
        params = {
            "ContentType": "text/plain",
            "RequestPayer": "requester",
            "ChecksumAlgorithm": "SHA256",
        }
        writer, stubber = _make_writer(request_kwargs=params)
        params["ContentType"] = "changed"
        identity = {"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"}
        stubber.add_response(
            "create_multipart_upload",
            {**identity, "ChecksumAlgorithm": "SHA256", "ChecksumType": "COMPOSITE"},
            {
                "Bucket": "bucket",
                "Key": "key",
                "ContentType": "text/plain",
                "RequestPayer": "requester",
                "ChecksumAlgorithm": "SHA256",
            },
        )
        stubber.add_response(
            "upload_part",
            {"ETag": '"one"', "ChecksumSHA256": "first"},
            {
                **identity,
                "PartNumber": 1,
                "Body": b"abcd",
                "ChecksumAlgorithm": "SHA256",
                "RequestPayer": "requester",
            },
        )
        stubber.add_response(
            "upload_part_copy",
            {"CopyPartResult": {"ETag": '"two"', "ChecksumSHA256": "second"}},
            {
                **identity,
                "PartNumber": 2,
                "CopySource": {"Bucket": "source", "Key": "old", "VersionId": "version"},
                "CopySourceRange": "bytes=0-3",
                "RequestPayer": "requester",
            },
        )
        stubber.add_response(
            "complete_multipart_upload",
            {"ETag": '"done"'},
            {
                **identity,
                "ChecksumType": "COMPOSITE",
                "RequestPayer": "requester",
                "MultipartUpload": {
                    "Parts": [
                        {"PartNumber": 1, "ETag": '"one"', "ChecksumSHA256": "first"},
                        {"PartNumber": 2, "ETag": '"two"', "ChecksumSHA256": "second"},
                    ]
                },
            },
        )
        with stubber:
            upload = writer.initiate()
            assert writer.initiate() is upload
            first = writer.upload_part(1, b"abcd", ContentType="filtered")
            second = writer.upload_part_copy(2, S3Path("source", "old", "version"), (0, 4))
            result = writer.complete([first, second])
            assert isinstance(first, S3MultipartUploadPart)
            assert isinstance(second, S3MultipartUploadPart)
            assert isinstance(result, S3CompleteMultipartUpload)
            assert result.etag == '"done"'
            assert writer.upload is upload
            stubber.assert_no_pending_responses()

    def test_writer_failed_abort_is_retryable(self):
        writer, stubber = _make_writer(
            request_kwargs={"ContentType": "text/plain", "RequestPayer": "requester"}
        )
        identity = {"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"}
        stubber.add_response(
            "create_multipart_upload",
            identity,
            {
                "Bucket": "bucket",
                "Key": "key",
                "ContentType": "text/plain",
                "RequestPayer": "requester",
            },
        )
        request = {**identity, "RequestPayer": "requester"}
        stubber.add_client_error(
            "abort_multipart_upload",
            service_error_code="AccessDenied",
            http_status_code=403,
            expected_params=request,
        )
        stubber.add_response("abort_multipart_upload", {}, request)
        with stubber:
            upload = writer.initiate()
            with pytest.raises(PermissionError):
                writer.abort()
            assert writer.upload is upload
            writer.abort()
            assert writer.upload is None
            writer.abort()
            stubber.assert_no_pending_responses()

    def test_writer_per_request_parameters_override_inherited_parameters(self):
        writer, stubber = _make_writer(request_kwargs={"ContentType": "text/plain"})
        stubber.add_response(
            "create_multipart_upload",
            {"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"},
            {"Bucket": "bucket", "Key": "key", "ContentType": "text/csv"},
        )
        with stubber:
            writer.initiate(ContentType="text/csv")
            stubber.assert_no_pending_responses()

    def test_writer_requires_upload(self):
        writer, stubber = _make_writer()
        with stubber:
            for request in (
                lambda: writer.upload_part(1, b"abcd"),
                lambda: writer.upload_part_copy(1, S3Path("bucket", "old")),
                lambda: writer.complete([]),
            ):
                with pytest.raises(RuntimeError, match="not initialized"):
                    request()
            writer.abort()


class _DroppingExecutor(S3ThreadPoolExecutor):
    """Drops requests unrun, as S3AioExecutor does when its event loop shuts down."""

    def __init__(self, drop) -> None:
        super().__init__(max_workers=1)
        self._drop = drop

    def submit_to(self, future, fn, *args, **kwargs):
        if self._drop(kwargs):
            # As _S3WorkItem.settle() does for a cancelled dispatch.
            future.cancel()
            future.set_running_or_notify_cancel()
        else:
            super().submit_to(future, fn, *args, **kwargs)


def _make_session(executor, part_errors=()):
    client = boto3.client(
        "s3", region_name="us-east-1", aws_access_key_id="dummy", aws_secret_access_key="dummy"
    )
    core = S3Core(client, retry_config=RetryConfig(attempt=1))
    core.MULTIPART_UPLOAD_MIN_PART_SIZE = 4
    core.MULTIPART_UPLOAD_MAX_PART_SIZE = 8
    completed = []
    errors = list(part_errors)

    def upload_part(upload, part_number, body, **params):
        if errors:
            raise errors.pop()
        return SimpleNamespace(part_number=part_number)

    core.create_multipart_upload = lambda path, **params: S3MultipartUpload(  # type: ignore[method-assign]
        {"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"}
    )
    core.upload_part = upload_part  # type: ignore[method-assign]
    core.complete_multipart_upload = lambda upload, parts, **params: completed.append(parts)  # type: ignore[method-assign]
    core.abort_multipart_upload = lambda upload, **params: None  # type: ignore[method-assign]
    session = _S3WriteSession(core, S3Path("bucket", "key"), executor, block_size=4, max_workers=1)
    return session, completed


class TestS3WriteSession:
    def test_failed_part_is_released(self):
        # GH-1211: the session drops the request of a failed part, whose body
        # the executor frees, and keeps no reference to its error once it is
        # raised, so that neither waits for the garbage collector.
        class Body:
            pass

        class AccessDeniedError(PermissionError):
            pass

        error = AccessDeniedError("Access Denied")
        error_ref = weakref.ref(error)
        gc.disable()
        try:
            with S3ThreadPoolExecutor(max_workers=1) as executor:
                session, _ = _make_session(executor, part_errors=[error])
                del error
                session.start()
                session.wait_ready()
                session.check()
                body = Body()
                body_ref = weakref.ref(body)
                session.submit_part(1, body)  # type: ignore[arg-type]
                del body
                # Returns once the part has failed, with max_workers=1.
                session.wait_ready()
            # The worker has returned from the request and its callbacks.
            assert body_ref() is None
            with pytest.raises(PermissionError) as info:
                session.check()
            del info
            assert error_ref() is None
            session.abort()
        finally:
            gc.enable()

    @pytest.mark.parametrize("dropped", ["creation", "part"])
    def test_dropped_request_fails_the_write(self, dropped):
        # A request that the executor drops without running it, as
        # S3AioExecutor does when its event loop shuts down, fails the write,
        # so that the upload is never completed without it.
        executor = _DroppingExecutor(
            lambda kwargs: dropped == "creation" or "part_number" in kwargs
        )
        with executor:
            session, completed = _make_session(executor)
            session.start()
            if dropped == "part":
                session.wait_ready()
                session.check()
                session.submit_part(1, b"abcd")
            with pytest.raises(CancelledError):
                session.complete()
            session.abort()
        assert completed == []

    @pytest.mark.parametrize("operation", ["complete", "abort"])
    def test_outcomes_before_callbacks(self, operation):
        # A future is done before its done callbacks run, so the completion
        # and the abort consume the finished requests themselves: the
        # completion includes every part, and a callback that runs after the
        # abort neither restores its part nor records its failure.
        with S3ThreadPoolExecutor(max_workers=1) as executor:
            session, completed = _make_session(
                executor,
                part_errors=[PermissionError("Access Denied")] if operation == "abort" else [],
            )
            session.start()
            session.wait_ready()
            session.check()
            on_part_done = session._on_part_done
            deferred = []
            with mock.patch.object(session, "_on_part_done", side_effect=deferred.append):
                session.submit_part(1, b"abcd")
                if operation == "complete":
                    session.complete()
                else:
                    session.abort()
            for request in deferred:
                on_part_done(request)
        if operation == "complete":
            assert [[part.part_number for part in parts] for parts in completed] == [[1]]
            assert [part.part_number for part in session.completed_parts] == [1]
        else:
            assert completed == []
            assert session.completed_parts == ()
            with pytest.raises(S3MultipartStateError, match="has been discarded"):
                session.complete()
