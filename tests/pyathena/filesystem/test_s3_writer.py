# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import gc
import io
import threading
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
    _S3WriteState,
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


class _Requests:
    """The S3 requests of a write session, recorded and controlled by a test."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.upload = S3MultipartUpload({"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"})
        # The requests that wait for an event before they respond.
        self.gates: dict[tuple, threading.Event] = {}
        # The requests that raise an error.
        self.errors: dict[tuple, BaseException] = {}
        self.started: dict[tuple, threading.Event] = {}

    def _respond(self, request: tuple, result):
        self.calls.append(request)
        self.started.setdefault(request, threading.Event()).set()
        if request in self.gates:
            assert self.gates[request].wait(5)
        if request in self.errors:
            raise self.errors.pop(request)
        return result

    def gate(self, *request) -> threading.Event:
        return self.gates.setdefault(request, threading.Event())

    def install(self, core: S3Core) -> None:
        core.create_multipart_upload = lambda path, **params: self._respond(  # type: ignore[method-assign]
            ("create",), self.upload
        )
        core.upload_part = lambda upload, part_number, body, **params: self._respond(  # type: ignore[method-assign]
            ("part", part_number), SimpleNamespace(part_number=part_number, etag=f'"{body!r}"')
        )
        core.upload_part_copy = lambda upload, part_number, source, range_, **params: (  # type: ignore[method-assign]
            self._respond(("copy", part_number), SimpleNamespace(part_number=part_number))
        )
        core.complete_multipart_upload = lambda upload, parts, **params: self._respond(  # type: ignore[method-assign]
            ("complete", tuple(part.part_number for part in parts)), "completed"
        )
        core.abort_multipart_upload = lambda upload, **params: self._respond(("abort",), None)  # type: ignore[method-assign]
        core.put_object = lambda path, body, **params: self._respond(("put", body), "put")  # type: ignore[method-assign]


class _DroppingExecutor(S3ThreadPoolExecutor):
    """Drops the requests, as S3AioExecutor does when its event loop shuts down."""

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


def _make_session(max_workers=2, executor_workers=4, executor=None):
    client = boto3.client(
        "s3", region_name="us-east-1", aws_access_key_id="dummy", aws_secret_access_key="dummy"
    )
    core = S3Core(client, retry_config=RetryConfig(attempt=1))
    core.MULTIPART_UPLOAD_MIN_PART_SIZE = 4
    core.MULTIPART_UPLOAD_MAX_PART_SIZE = 8
    requests = _Requests()
    requests.install(core)
    executor = executor or S3ThreadPoolExecutor(max_workers=executor_workers)
    session = _S3WriteSession(
        core, S3Path("bucket", "key"), executor, block_size=4, max_workers=max_workers
    )
    return session, requests, executor


def _start(session):
    session.start()
    assert session.ready().result(5) is None
    session.check()
    assert session.state is _S3WriteState.ACTIVE


class TestS3WriteSession:
    def test_invalid_max_workers(self):
        with pytest.raises(ValueError, match="max_workers"):
            _make_session(max_workers=0)

    def test_put(self):
        session, requests, executor = _make_session()
        with executor:
            assert session.put(b"data") == "put"
            assert session.state is _S3WriteState.COMPLETED
            # Committing again sends no request.
            assert session.put(b"data") == "put"
            session.abort()
            assert session.state is _S3WriteState.COMPLETED
            with pytest.raises(S3MultipartStateError, match="has been committed"):
                session.start()
        assert requests.calls == [("put", b"data")]
        assert not session.multipart

    def test_complete(self):
        session, requests, executor = _make_session()
        with executor:
            _start(session)
            assert list(session.iter_copy_parts(4)) == [(1, (0, 4))]
            session.submit_copy(1, S3Path("bucket", "key"), None)
            ((number, body),) = session.iter_parts(io.BytesIO(b"abcd"))
            assert number == 2
            session.submit_part(number, body)
            assert session.complete() == "completed"
            assert session.state is _S3WriteState.COMPLETED
            assert session.complete() == "completed"
            with pytest.raises(S3MultipartStateError, match="has been committed"):
                session.put(b"data")
            session.abort()
        assert requests.calls.count(("complete", (1, 2))) == 1
        assert ("abort",) not in requests.calls
        assert [part.part_number for part in session.completed_parts] == [1, 2]
        assert session.upload is requests.upload

    def test_ready_waits_for_creation_and_parts_in_flight(self):
        session, requests, executor = _make_session(max_workers=1)
        creation = requests.gate("create")
        part = requests.gate("part", 1)
        with executor:
            session.start()
            waiter = session.ready()
            assert requests.started.setdefault(("create",), threading.Event()).wait(5)
            assert not waiter.done()
            creation.set()
            assert waiter.result(5) is None
            session.submit_part(1, b"abcd")
            waiters = [session.ready(), session.ready()]
            # A waiter cancelled by its caller, such as by
            # asyncio.wrap_future(), does not affect the others.
            assert waiters[0].cancel()
            assert not waiters[1].done()
            part.set()
            assert waiters[1].result(5) is None
            assert session.complete() == "completed"

    def test_part_failure_is_raised_once(self):
        session, requests, executor = _make_session(max_workers=1)
        requests.errors[("part", 1)] = PermissionError("Access Denied")
        with executor:
            _start(session)
            session.submit_part(1, b"abcd")
            # A failure resolves the readiness, so that the caller finds it.
            assert session.ready().result(5) is None
            assert session.state is _S3WriteState.FAILED
            with pytest.raises(PermissionError, match="Access Denied"):
                session.check()
            for operation in (
                session.check,
                lambda: session.submit_part(2, b"abcd"),
                session.complete,
            ):
                with pytest.raises(
                    S3MultipartStateError,
                    match=r"has failed\. The first failure: PermissionError: Access Denied",
                ):
                    operation()
            session.abort()
            with pytest.raises(S3MultipartStateError, match="has been discarded"):
                session.complete()
        assert requests.calls[-1] == ("abort",)
        assert session.upload is None
        assert session.completed_parts == ()

    def test_failed_part_is_released(self):
        # The session drops the request of a failed part, whose body the
        # executor frees, and keeps no reference to its error once it is
        # raised, so neither waits for the garbage collector.
        class Body:
            pass

        class AccessDeniedError(PermissionError):
            pass

        session, requests, executor = _make_session(max_workers=1)
        error = AccessDeniedError("Access Denied")
        requests.errors[("part", 1)] = error
        error_ref = weakref.ref(error)
        del error
        gc.disable()
        try:
            with executor:
                _start(session)
                body = Body()
                body_ref = weakref.ref(body)
                session.submit_part(1, body)  # type: ignore[arg-type]
                del body
                assert session.ready().result(5) is None
            # The worker has returned from the request and its callbacks.
            assert body_ref() is None
            with pytest.raises(PermissionError) as info:
                session.check()
            del info
            assert error_ref() is None
            session.abort()
        finally:
            gc.enable()

    def test_creation_failure(self):
        session, requests, executor = _make_session()
        requests.errors[("create",)] = PermissionError("create failed")
        with executor:
            session.start()
            assert session.ready().result(5) is None
            with pytest.raises(PermissionError, match="create failed"):
                session.check()
            session.abort()
        assert session.state is _S3WriteState.ABORTED
        assert requests.calls == [("create",)]

    def test_abort_during_creation(self):
        # A running creation is waited for, and the upload that it creates is
        # aborted. The writes that wait for the creation are released.
        session, requests, executor = _make_session()
        creation = requests.gate("create")
        with executor:
            session.start()
            waiter = session.ready()
            assert requests.started.setdefault(("create",), threading.Event()).wait(5)
            aborting = threading.Thread(target=session.abort)
            aborting.start()
            assert waiter.result(5) is None
            assert session.state is _S3WriteState.ABORTING
            creation.set()
            aborting.join(5)
            assert not aborting.is_alive()
        assert requests.calls == [("create",), ("abort",)]
        assert session.state is _S3WriteState.ABORTED
        with pytest.raises(S3MultipartStateError, match="has been discarded"):
            session.check()

    def test_abort_cancels_queued_and_waits_for_running_parts(self):
        session, requests, executor = _make_session(executor_workers=1)
        running = requests.gate("part", 1)
        with executor:
            _start(session)
            session.submit_part(1, b"abcd")
            session.submit_part(2, b"efgh")
            assert requests.started.setdefault(("part", 1), threading.Event()).wait(5)
            aborting = threading.Thread(target=session.abort)
            aborting.start()
            # The abort waits for the running part.
            aborting.join(0.1)
            assert aborting.is_alive()
            running.set()
            aborting.join(5)
            assert not aborting.is_alive()
        assert requests.calls == [("create",), ("part", 1), ("abort",)]

    def test_abort_failure_is_retried(self):
        session, requests, executor = _make_session()
        requests.errors[("abort",)] = PermissionError("abort failed")
        with executor:
            _start(session)
            with pytest.raises(PermissionError, match="abort failed"):
                session.abort()
            assert session.state is _S3WriteState.ABORT_FAILED
            assert session.upload is requests.upload
            with pytest.raises(S3MultipartStateError, match="failed to abort"):
                session.complete()
            session.abort()
            session.abort()
        assert requests.calls == [("create",), ("abort",), ("abort",)]
        assert session.state is _S3WriteState.ABORTED
        assert session.upload is None

    def test_concurrent_aborts(self):
        # Another abort waits for the abort in progress.
        session, requests, executor = _make_session()
        aborted = requests.gate("abort")
        with executor:
            _start(session)
            first = threading.Thread(target=session.abort)
            first.start()
            assert requests.started.setdefault(("abort",), threading.Event()).wait(5)
            second = threading.Thread(target=session.abort)
            second.start()
            second.join(0.1)
            assert second.is_alive()
            aborted.set()
            first.join(5)
            second.join(5)
        assert requests.calls == [("create",), ("abort",)]

    def test_request_parameters_are_read_at_each_request(self):
        client = boto3.client(
            "s3", region_name="us-east-1", aws_access_key_id="dummy", aws_secret_access_key="dummy"
        )
        core = S3Core(client, retry_config=RetryConfig(attempt=1))
        sent = []
        core.put_object = lambda path, body, **params: sent.append(params)  # type: ignore[method-assign]
        request_kwargs = {"ContentType": "text/plain"}
        with S3ThreadPoolExecutor(max_workers=1) as executor:
            session = _S3WriteSession(
                core,
                S3Path("bucket", "key"),
                executor,
                block_size=5 * 2**20,
                max_workers=1,
                request_kwargs=request_kwargs,
            )
            request_kwargs["ContentType"] = "text/csv"
            request_kwargs["UploadId"] = "not accepted by PutObject"
            session.put(b"data")
        assert sent == [{"ContentType": "text/csv"}]

    def test_abort_before_start(self):
        session, requests, executor = _make_session()
        with executor:
            session.abort()
            with pytest.raises(
                S3MultipartStateError, match=r"Cannot commit: .* has been discarded"
            ):
                session.put(b"data")
        assert requests.calls == []

    def test_complete_failure(self):
        session, requests, executor = _make_session()
        requests.errors[("complete", ())] = RuntimeError("complete failed")
        with executor:
            _start(session)
            with pytest.raises(RuntimeError, match="complete failed"):
                session.complete()
            assert session.state is _S3WriteState.FAILED
            with pytest.raises(S3MultipartStateError, match="RuntimeError: complete failed"):
                session.complete()
            session.abort()
        assert requests.calls == [("create",), ("complete", ()), ("abort",)]

    def test_submit_requires_active_upload(self):
        session, _, executor = _make_session()
        with executor:
            with pytest.raises(S3MultipartStateError, match="has not started"):
                session.submit_part(1, b"abcd")
            _start(session)
            with pytest.raises(ValueError, match="Expected part number 1, got 2"):
                session.submit_part(2, b"abcd")
            session.abort()

    def test_submit_requires_capacity(self):
        # ready() is a hint; the submission itself refuses a request beyond
        # max_workers.
        session, requests, executor = _make_session(max_workers=1)
        release = requests.gate("part", 1)
        with executor:
            _start(session)
            session.submit_part(1, b"abcd")
            with pytest.raises(RuntimeError, match="wait for ready"):
                session.submit_part(2, b"abcd")
            release.set()
            assert session.ready().result(5) is None
            session.submit_part(2, b"abcd")
            assert session.complete() == "completed"

    def test_scheduling_failure(self):
        # A request that the executor refuses fails the write with the error
        # of the executor, and is no longer tracked, so the abort does not
        # wait for it.
        session, requests, executor = _make_session()
        with executor:
            _start(session)
        with pytest.raises(RuntimeError, match="shutdown"):
            session.submit_part(1, b"abcd")
        assert session.state is _S3WriteState.FAILED
        with pytest.raises(S3MultipartStateError, match="The first failure: RuntimeError"):
            session.check()
        session.abort()
        assert requests.calls == [("create",), ("abort",)]
        assert session.state is _S3WriteState.ABORTED

    def test_part_dropped_by_executor(self):
        # A part request that the executor drops without running it, such as
        # when the event loop of an S3AioExecutor shuts down, fails the write,
        # so that the upload is not completed without the part.
        session, requests, executor = _make_session(
            max_workers=1, executor=_DroppingExecutor(lambda kwargs: "part_number" in kwargs)
        )
        with executor:
            _start(session)
            session.submit_part(1, b"abcd")
            assert session.state is _S3WriteState.FAILED
            with pytest.raises(CancelledError, match="part request was cancelled"):
                session.complete()
            session.abort()
        assert requests.calls == [("create",), ("abort",)]

    def test_creation_dropped_by_executor(self):
        session, requests, executor = _make_session(executor=_DroppingExecutor(lambda kwargs: True))
        with executor:
            session.start()
            assert session.ready().result(5) is None
            with pytest.raises(CancelledError, match="creation of the multipart upload"):
                session.check()
            session.abort()
        assert requests.calls == []
        assert session.state is _S3WriteState.ABORTED

    def test_unrecorded_part_is_not_completed(self):
        # A part request whose done callback was stopped, such as by an
        # interrupt on the thread that cancelled it, is neither waited for
        # forever nor completed without its data.
        session, requests, executor = _make_session()
        with executor:
            _start(session)
            with mock.patch.object(session, "_on_part_done", return_value=None):
                session.submit_part(1, b"abcd")
            session.submit_part(2, b"efgh")
            with pytest.raises(RuntimeError, match=r"The parts \[1\] .* have not been uploaded"):
                session.complete()
            session.abort()
        assert ("complete", (2,)) not in requests.calls
        assert requests.calls[-1] == ("abort",)
