# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Synchronous multipart writing and part planning without fsspec."""

from __future__ import annotations

import contextlib
import enum
import threading
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import CancelledError, Future, wait
from typing import Any, BinaryIO, TypeVar, cast

from pyathena.filesystem.s3_core import S3Core
from pyathena.filesystem.s3_executor import S3Executor
from pyathena.filesystem.s3_object import (
    S3CompleteMultipartUpload,
    S3MultipartUpload,
    S3MultipartUploadPart,
    S3PutObject,
)
from pyathena.filesystem.s3_path import S3Path
from pyathena.util import stop_retries_on

T = TypeVar("T")


class S3MultipartWriter:
    """Plan parts and send the requests of a multipart write.

    The caller owns the buffer, schedules requests, collects part results in
    number order, and waits for running requests before completing or aborting.
    Initiation and finalization must be serialized; part requests may run in
    parallel after initiation. No request is sent by the constructor or planners.

    Attributes:
        upload: The identity returned by creation, retained after completion
            and after an abort failure. A successful abort clears it.
    """

    def __init__(
        self,
        core: S3Core,
        path: S3Path,
        *,
        block_size: int,
        request_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        """Initialize a writer without starting its upload.

        Args:
            core: The synchronous S3 operations to use.
            path: The destination, with a key and without a version ID.
            block_size: The nominal part size, between the core's minimum
                and maximum multipart part sizes, inclusive.
            request_kwargs: Parameters inherited by each request that accepts
                them. A copy is kept; parameters given to a request override it.

        Raises:
            ValueError: If the destination or block size is invalid.
        """
        if not path.key:
            raise ValueError(f"The path has no key: {path.uri}.")
        if path.version_id:
            raise ValueError(f"Cannot write to a version: {path.uri}.")
        if (
            not core.MULTIPART_UPLOAD_MIN_PART_SIZE
            <= block_size
            <= core.MULTIPART_UPLOAD_MAX_PART_SIZE
        ):
            raise ValueError("block_size must be between the minimum and maximum part sizes.")
        self._core = core
        self._path = path
        self._block_size = block_size
        self._request_kwargs = dict(request_kwargs or {})
        self.upload: S3MultipartUpload | None = None

    def _params(self, method: str, params: Mapping[str, Any]) -> dict[str, Any]:
        return self._core.operation_params(method, {**self._request_kwargs, **params})

    def _require_upload(self) -> S3MultipartUpload:
        if self.upload is None:
            raise RuntimeError("Multipart upload is not initialized.")
        return self.upload

    def _check_part_number(self, part_number: int) -> None:
        if part_number < 1:
            raise ValueError("part_number must be at least 1.")
        if part_number > self._core.MULTIPART_UPLOAD_MAX_PARTS:
            raise ValueError(
                f"Cannot upload more than {self._core.MULTIPART_UPLOAD_MAX_PARTS} "
                f"parts to {self._path.uri} with a block size of {self._block_size} bytes. "
                "Use a block_size large enough for all parts, including parts "
                "copied from the existing object in an append."
            )

    def _read_block(self, stream: BinaryIO) -> bytes:
        data = stream.read(self._block_size)
        if not data or len(data) == self._block_size:
            return data
        chunks = [data]
        size = len(data)
        while size < self._block_size:
            data = stream.read(self._block_size - size)
            if not data:
                break
            chunks.append(data)
            size += len(data)
        return chunks[0] if len(chunks) == 1 else b"".join(chunks)

    def iter_parts(
        self, stream: BinaryIO, first_part_number: int = 1
    ) -> Iterator[tuple[int, bytes]]:
        """Plan numbered parts from the stream's current position to EOF.

        A short trailing block is merged into the preceding block, splitting
        the result in half if it reaches the maximum part size. This keeps
        every part large enough for later writes, unless the entire stream
        is shorter than the minimum part size and must be the final part.
        Reads are bounded by the block size; the stream is not rewound.

        Args:
            stream: A blocking binary stream of buffered data.
            first_part_number: The first number, including any earlier copied
                or uploaded parts in the caller's count.

        Yields:
            The part number and bytes, in order, without sending requests.

        Raises:
            ValueError: If a generated part number is outside the S3 limit.
        """
        if first_part_number < 1:
            raise ValueError("part_number must be at least 1.")
        part_number = first_part_number
        data = self._read_block(stream)
        while data:
            next_data = self._read_block(stream)
            if 0 < len(next_data) < self._core.MULTIPART_UPLOAD_MIN_PART_SIZE:
                merged = data + next_data
                if len(merged) < self._core.MULTIPART_UPLOAD_MAX_PART_SIZE:
                    bodies = [merged]
                else:
                    split = len(merged) // 2
                    bodies = [merged[:split], merged[split:]]
                next_data = b""
            else:
                bodies = [data]
            for body in bodies:
                self._check_part_number(part_number)
                yield part_number, body
                part_number += 1
            data = next_data

    def iter_copy_parts(
        self, size: int, first_part_number: int = 1
    ) -> Iterator[tuple[int, tuple[int, int]]]:
        """Plan numbered copy ranges, with an exclusive end, without requests.

        Args:
            size: The nonnegative size of the existing object in bytes.
            first_part_number: The first part number to use.

        Yields:
            The part number and source range, in order. The ranges use the
            core's maximum part size and keep a short tail within S3 limits
            so that appended parts may follow, unless the whole source is
            smaller than the minimum part size and must be the final part.

        Raises:
            ValueError: If the size is negative or a part number exceeds the limit.
        """
        if size < 0:
            raise ValueError("size must be nonnegative.")
        if first_part_number < 1:
            raise ValueError("part_number must be at least 1.")
        if size == 0:
            return
        ranges = self._core.part_ranges(size, self._core.MULTIPART_UPLOAD_MAX_PART_SIZE)
        for part_number, range_ in enumerate(ranges, first_part_number):
            self._check_part_number(part_number)
            yield part_number, range_

    def initiate(self, **params: Any) -> S3MultipartUpload:
        """Create the upload, or return its already known identity.

        Args:
            **params: Parameters accepted by CreateMultipartUpload.

        Returns:
            The upload, stored before this method returns so that a caller
            can recover it after an interrupted wait for a scheduled request.
        """
        if self.upload is None:
            self.upload = self._core.create_multipart_upload(
                self._path, **self._params("create_multipart_upload", params)
            )
        return self.upload

    def upload_part(self, part_number: int, body: bytes, **params: Any) -> S3MultipartUploadPart:
        """Send an UploadPart request for the initialized upload.

        Args:
            part_number: The part number, between 1 and the core's maximum.
            body: The bytes to upload, with the size selected by the caller.
            **params: Parameters accepted by UploadPart.

        Returns:
            The uploaded part, retaining its number and checksums.

        Raises:
            RuntimeError: If no upload is initialized.
            ValueError: If the part number is outside the S3 limit.
        """
        self._check_part_number(part_number)
        return self._core.upload_part(
            upload=self._require_upload(),
            part_number=part_number,
            body=body,
            **self._params("upload_part", params),
        )

    def upload_part_copy(
        self,
        part_number: int,
        source: S3Path,
        range_: tuple[int, int] | None = None,
        **params: Any,
    ) -> S3MultipartUploadPart:
        """Send an UploadPartCopy request for the initialized upload.

        Args:
            part_number: The part number, between 1 and the core's maximum.
            source: The source object, with an optional version ID.
            range_: The source range with an exclusive end, or None to send
                no range of its own, as in ``S3Core.upload_part_copy()``.
            **params: Parameters accepted by UploadPartCopy.

        Returns:
            The copied part, retaining its number and checksums.

        Raises:
            RuntimeError: If no upload is initialized.
            ValueError: If the part number is outside the S3 limit.
        """
        self._check_part_number(part_number)
        return self._core.upload_part_copy(
            upload=self._require_upload(),
            part_number=part_number,
            source=source,
            range_=range_,
            **self._params("upload_part_copy", params),
        )

    def complete(
        self, parts: Sequence[S3MultipartUploadPart], **params: Any
    ) -> S3CompleteMultipartUpload:
        """Complete the upload after the caller has collected all its parts.

        Args:
            parts: Successfully uploaded parts in part-number order.
            **params: Parameters accepted by CompleteMultipartUpload.

        Returns:
            The completion result. The upload identity is retained.

        Raises:
            RuntimeError: If no upload is initialized. Request failures
                propagate; the caller must settle running work before aborting.
        """
        return self._core.complete_multipart_upload(
            self._require_upload(),
            parts,
            **self._params("complete_multipart_upload", params),
        )

    def abort(self, **params: Any) -> None:
        """Abort a known upload after the caller has settled running requests.

        Does nothing if no upload is known. A successful abort clears the
        identity; a failed or interrupted abort retains it for a later retry.

        Args:
            **params: Parameters accepted by AbortMultipartUpload.
        """
        if self.upload is not None:
            self._core.abort_multipart_upload(
                self.upload, **self._params("abort_multipart_upload", params)
            )
            self.upload = None


class S3MultipartStateError(RuntimeError):
    """An operation that the state of a write no longer allows.

    For example, committing a file whose upload was discarded, or whose
    write failed, or uploading a part after a part failed.
    """


class _S3WriteState(enum.Enum):
    """The states of a :class:`_S3WriteSession`."""

    #: No request has been sent.
    IDLE = "has not started a multipart upload"
    #: The multipart upload is being created.
    CREATING = "is creating its multipart upload"
    #: Parts may be submitted.
    ACTIVE = "is uploading parts"
    #: The write is being committed.
    COMPLETING = "is being committed"
    #: The write has been committed.
    COMPLETED = "has been committed"
    #: A request has failed; the write cannot be committed.
    FAILED = "has failed"
    #: The multipart upload is being aborted.
    ABORTING = "is being aborted"
    #: The write has been discarded, and its multipart upload, if any, aborted.
    ABORTED = "has been discarded"
    #: The abort failed or was interrupted; the upload is kept for a retry.
    ABORT_FAILED = "failed to abort its multipart upload"


#: The seconds after which a wait of the session checks the requests again,
#: in case the done callback of one was stopped, such as by an interrupt,
#: before it recorded the outcome and notified the wait.
_RECHECK_INTERVAL = 1.0

#: The states in which the caller may still write data.
_WRITABLE_STATES = frozenset({_S3WriteState.IDLE, _S3WriteState.CREATING, _S3WriteState.ACTIVE})


class _S3WriteSession:
    """Own the requests of one write of an object, and its state.

    A write is committed either with a single PutObject request, or as a
    multipart upload whose parts are uploaded from the written data or copied
    from an existing object. The session sends the requests, on the executor
    for the creation and the parts, and in the calling thread for the
    completion and the abort, so that those also work after the executor is
    shut down. It owns its :class:`S3MultipartWriter`.

    The caller buffers the data, plans the parts with :meth:`iter_parts` and
    :meth:`iter_copy_parts`, and submits them from one thread, after the
    future of :meth:`ready` resolves. The session keeps the parts in flight
    and consumes each request future when it finishes: a part becomes its
    metadata, and a failure is recorded, and the future is dropped. The first
    failure is raised once, by the next operation that the caller performs;
    afterwards, the operations that the failure prohibits raise
    :class:`S3MultipartStateError`. A failed write is never committed; the
    caller aborts it with :meth:`abort`.
    """

    def __init__(
        self,
        core: S3Core,
        path: S3Path,
        executor: S3Executor,
        *,
        block_size: int,
        max_workers: int,
        request_kwargs: Mapping[str, Any] | None = None,
        prepared_params: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        """Initialize a session without sending any request.

        Args:
            core: The synchronous S3 operations to use.
            path: The destination, with a key and without a version ID.
            executor: The executor of the creation and part requests.
            block_size: The nominal part size, as for :class:`S3MultipartWriter`.
            max_workers: The number of part requests in flight after which
                :meth:`ready` waits for one of them to finish.
            request_kwargs: Parameters inherited by each request that accepts
                them. The mapping is read when each request is sent, so later
                changes to it apply to the requests that follow.
            prepared_params: The parameters of the requests of some
                operations, by client method name, such as
                ``create_multipart_upload``. They are sent as given, instead
                of those selected from ``request_kwargs``, so that botocore
                rejects one that the operation does not accept.

        Raises:
            ValueError: If the destination, the block size, or
                ``max_workers`` is invalid.
        """
        if max_workers < 1:
            raise ValueError("max_workers must be greater than 0")
        self._core = core
        self._path = path
        self._executor = executor
        self._max_workers = max_workers
        self._request_kwargs: Mapping[str, Any] = (
            request_kwargs if request_kwargs is not None else {}
        )
        self._prepared_params = dict(prepared_params or {})
        # The requests pass the parameters themselves, as the writer would
        # copy the mapping that they are read from.
        self._writer = S3MultipartWriter(core, path, block_size=block_size)
        self._lock = threading.Lock()
        # Notified whenever a request settles or the state changes.
        self._changed = threading.Condition(self._lock)
        self._state = _S3WriteState.IDLE
        self._multipart = False
        self._creation: Future[S3MultipartUpload] | None = None
        # The part requests that have not settled, which may not have been
        # scheduled yet.
        self._requests: set[Future[S3MultipartUploadPart]] = set()
        self._next_part_number = 1
        self._parts: dict[int, S3MultipartUploadPart] = {}
        # The waiters of ready() and settled(), each with the number of part
        # requests in flight below which it resolves.
        self._waiters: list[tuple[Future[None], int]] = []
        # Set by abort(), so that the creation and the running part requests
        # make no further attempt before the upload is aborted.
        self._stop = threading.Event()
        # The first failure, until it is raised.
        self._failure: BaseException | None = None
        # A description of the first failure, kept after it is raised.
        self._failure_summary: str | None = None
        self._result: S3PutObject | S3CompleteMultipartUpload | None = None
        # The state that begin_complete() reserved the commit from, IDLE for
        # PutObject or ACTIVE for the completion, until put() or complete()
        # takes the reservation.
        self._reservation: _S3WriteState | None = None
        # Set by begin_abort() until abort() takes over the abort that it began.
        self._abort_pending = False

    @property
    def state(self) -> _S3WriteState:
        """The current state."""
        return self._state

    @property
    def multipart(self) -> bool:
        """Whether the write started a multipart upload."""
        return self._multipart

    @property
    def upload(self) -> S3MultipartUpload | None:
        """The multipart upload, from its creation until it is aborted."""
        return self._writer.upload

    @property
    def completed_parts(self) -> tuple[S3MultipartUploadPart, ...]:
        """The uploaded or copied parts in part-number order; empty after an abort."""
        with self._lock:
            return tuple(self._parts[number] for number in sorted(self._parts))

    def iter_parts(self, stream: BinaryIO) -> Iterator[tuple[int, bytes]]:
        """Plan the parts of the stream, numbered after the parts submitted so far.

        See :meth:`S3MultipartWriter.iter_parts`.

        Args:
            stream: A blocking binary stream of buffered data.

        Returns:
            An iterator of the part numbers and bytes.
        """
        return self._writer.iter_parts(stream, first_part_number=self._next_part_number)

    def iter_copy_parts(self, size: int) -> Iterator[tuple[int, tuple[int, int]]]:
        """Plan the copy ranges of an object, numbered after the parts submitted so far.

        See :meth:`S3MultipartWriter.iter_copy_parts`.

        Args:
            size: The size of the copied object in bytes.

        Returns:
            An iterator of the part numbers and source ranges.
        """
        return self._writer.iter_copy_parts(size, first_part_number=self._next_part_number)

    def start(self) -> None:
        """Schedule the creation of the multipart upload.

        The creation is tracked before it is scheduled, so that an interrupt
        while it is being scheduled still lets :meth:`abort` cancel it or wait
        for it.

        Raises:
            S3MultipartStateError: If a request has already been sent.
        """
        creation: Future[S3MultipartUpload] = Future()
        creation.add_done_callback(self._on_created)
        with self._lock:
            self._refuse_unless(_S3WriteState.IDLE, operation="start a multipart upload")
            self._state = _S3WriteState.CREATING
            self._multipart = True
            self._creation = creation
        try:
            with stop_retries_on(self._stop):
                self._executor.submit_to(
                    creation, self._initiate, self._params("create_multipart_upload")
                )
        except BaseException as e:
            # Recorded before the cancellation, which would record its own.
            with self._update():
                self._fail(e, raised=True)
            # A creation that was not scheduled never runs.
            creation.cancel()
            raise

    def ready(self) -> Future[None]:
        """Return a future that resolves when another part may be submitted.

        It resolves once the upload is created and fewer than ``max_workers``
        part requests are in flight, or when the write can no longer submit
        parts, such as after a failure, which the next operation raises. Each
        call returns a new future, which the caller may cancel, for example
        through ``asyncio.wrap_future()``, without affecting the session.

        Returns:
            A future whose result is None.
        """
        return self._waiter(self._max_workers)

    def settled(self) -> Future[None]:
        """Return a future that resolves when no part request is in flight.

        It resolves once the upload is created and its part requests have
        settled, or, as for :meth:`ready`, when the write can no longer submit
        parts, such as after a failure, which the next operation raises. Each
        call returns a new future, which the caller may cancel.

        Returns:
            A future whose result is None.
        """
        return self._waiter(1)

    def wait_ready(self) -> None:
        """Block until another part may be submitted; see :meth:`ready`.

        The requests are checked again every second, in case the done callback
        of one was stopped before it resolved the wait.
        """
        while True:
            waiter = self.ready()
            try:
                waiter.result(_RECHECK_INTERVAL)
                return
            except TimeoutError:
                waiter.cancel()

    def check(self) -> None:
        """Raise the error that keeps the write from continuing, if any.

        Raises:
            BaseException: The first failure of a request, if it has not been
                raised yet.
            S3MultipartStateError: If the write failed earlier, or has been
                committed or discarded.
        """
        if self._failure_summary is None and self._state in _WRITABLE_STATES:
            # Checked without the lock, as each write calls this. A failure
            # recorded meanwhile is raised by the next operation.
            return
        with self._lock:
            self._refuse_unless(*_WRITABLE_STATES, operation="write")

    def submit_part(self, part_number: int, body: bytes) -> None:
        """Schedule the upload of a part.

        Args:
            part_number: The number planned by :meth:`iter_parts`.
            body: The bytes of the part.

        Raises:
            BaseException: The first failure of a request, if it has not been
                raised yet.
            S3MultipartStateError: If the upload is not active.
            RuntimeError: If ``max_workers`` part requests are in flight;
                :meth:`ready` waits for one of them.
            ValueError: If the part number is not the next one.
        """
        self._submit(
            self._writer.upload_part,
            part_number=part_number,
            body=body,
            **self._params("upload_part"),
        )

    def submit_copy(self, part_number: int, source: S3Path, range_: tuple[int, int] | None) -> None:
        """Schedule the copy of a range of an object as a part.

        Args:
            part_number: The number planned by :meth:`iter_copy_parts`.
            source: The source object.
            range_: The source range with an exclusive end, or None to copy
                the whole object.

        Raises:
            BaseException: The first failure of a request, if it has not been
                raised yet.
            S3MultipartStateError: If the upload is not active.
            RuntimeError: If ``max_workers`` part requests are in flight;
                :meth:`ready` waits for one of them.
            ValueError: If the part number is not the next one.
        """
        self._submit(
            self._writer.upload_part_copy,
            part_number=part_number,
            source=source,
            range_=range_,
            **self._params("upload_part_copy"),
        )

    def put(self, body: bytes | None) -> S3PutObject:
        """Commit the write with a single PutObject request.

        Committing again after a successful request returns its result
        without another request.

        Args:
            body: The data of the object, or None for an empty object.

        Returns:
            The result of the request.

        Raises:
            S3MultipartStateError: If the write started a multipart upload,
                failed, or has been discarded.
        """
        return self._commit(
            _S3WriteState.IDLE,
            lambda: self._core.put_object(self._path, body, **self._params("put_object")),
        )

    def complete(self) -> S3CompleteMultipartUpload:
        """Commit the write by completing its multipart upload.

        Waits for the part requests, and raises the first failure among them
        without waiting for the others. A failed or interrupted completion
        leaves the session failed, and the caller aborts the upload.
        Completing again after a successful completion returns its result
        without another request.

        Returns:
            The result of the completion.

        Raises:
            BaseException: The first failure of a request, if it has not been
                raised yet, or the failure of the completion.
            S3MultipartStateError: If no upload is active, or the write failed
                earlier or has been discarded.
        """
        return self._commit(_S3WriteState.ACTIVE, self._complete_parts)

    def begin_complete(self) -> None:
        """Reserve the commit before the call that sends it is queued.

        Does not block. Afterwards, :meth:`abort` leaves the write to the
        commit, and the next :meth:`put` or :meth:`complete`, whichever the
        write allows, sends it, unless :meth:`withdraw_complete` withdraws the
        reservation first. Does nothing for a committed write.

        Raises:
            BaseException: The first failure of a request, if it has not been
                raised yet.
            S3MultipartStateError: If the write failed earlier, or has been
                discarded.
        """
        with self._lock:
            if self._state is _S3WriteState.COMPLETED:
                return
            state = _S3WriteState.ACTIVE if self._multipart else _S3WriteState.IDLE
            self._refuse_unless(state, operation="commit")
            # Set first, so that a COMPLETING state that this sets always has
            # its reservation for the commit to take.
            self._reservation = state
            self._state = _S3WriteState.COMPLETING

    def withdraw_complete(self) -> None:
        """Withdraw the commit reserved by :meth:`begin_complete`, unless it has been taken.

        Does not block. For a caller whose call that sends the commit does
        not run, such as one that an interrupt kept from being queued, or one
        cancelled before it started. The write then fails: :meth:`put` and
        :meth:`complete` refuse it, and :meth:`abort` discards it. Does
        nothing if no commit is reserved, or once :meth:`put` or
        :meth:`complete` has taken the reservation, whose commit runs on.
        """
        with self._update():
            if self._state is not _S3WriteState.COMPLETING or self._reservation is None:
                return
            # Failed first, so that an interrupt in between never leaves a
            # COMPLETING state without its reservation, which abort() leaves
            # to a commit that nothing sends.
            self._fail(
                S3MultipartStateError(
                    f"The commit of the write to {self._path.uri} was withdrawn."
                ),
                raised=True,
            )
            self._reservation = None

    def begin_abort(self) -> bool:
        """Begin discarding the write without blocking.

        Seals the write, cancels the creation and the part requests that have
        not started, and stops the retries of the running ones, so that they
        stop even while the call to :meth:`abort`, which waits for them, waits
        for a thread that they hold.

        Returns:
            Whether :meth:`abort` has to run to finish the abort, or to wait
            for one that is running. False if the write is being or has been
            committed, has been discarded, or never sent a request.
        """
        with self._lock:
            if self._state is _S3WriteState.IDLE:
                self._state = _S3WriteState.ABORTED
            if self._state in (
                _S3WriteState.COMPLETING,
                _S3WriteState.COMPLETED,
                _S3WriteState.ABORTED,
            ):
                return False
            if self._state is _S3WriteState.ABORTING:
                return True
            # Set first, so that an ABORTING state that this sets is always
            # left for abort() to take over, even after an interrupt.
            self._abort_pending = True
            self._state = _S3WriteState.ABORTING
            waiters = self._take_waiters()
        self._notify(waiters)
        self._stop_requests()
        return True

    def abort(self) -> None:
        """Discard the write, and abort its multipart upload, if any.

        The creation and the part requests that have not started are
        cancelled, and the running ones make no further attempt and are
        waited for first, since a part that is still uploading when the
        upload is aborted may be stored after it. The retries of the abort
        request itself are not stopped. While a running creation is waited
        for, KeyboardInterrupts are suppressed, so that its upload is
        recovered and aborted. Does nothing if the write is being or has been
        committed, or has been discarded; another call that is aborting the
        upload is waited for. Finishes an abort begun by :meth:`begin_abort`.

        Raises:
            BaseException: The failure of the abort request, or an interrupt
                of the wait for the parts. The upload is then kept, and a later
                call retries the abort.
        """
        owned = False
        try:
            with self._lock:
                while self._state is _S3WriteState.ABORTING and not self._abort_pending:
                    self._changed.wait()
                if self._state in (
                    _S3WriteState.COMPLETING,
                    _S3WriteState.COMPLETED,
                    _S3WriteState.ABORTED,
                ):
                    return
                if self._state is _S3WriteState.IDLE:
                    self._state = _S3WriteState.ABORTED
                    return
                # Set first, so that an interrupt anywhere after the state
                # changes leaves the write ABORT_FAILED, not ABORTING, which
                # a later abort() would wait for.
                owned = True
                self._abort_pending = False
                self._state = _S3WriteState.ABORTING
                waiters = self._take_waiters()
            self._notify(waiters)
            # Again after begin_abort(), which an interrupt may have stopped.
            self._stop_requests()
            self._settle_creation()
            with self._lock:
                self._consume_finished()
                while self._requests:
                    self._changed.wait(_RECHECK_INTERVAL)
                    self._consume_finished()
            self._writer.abort(**self._params("abort_multipart_upload"))
            with self._update():
                self._state = _S3WriteState.ABORTED
                # A discarded write reports its state, not a failure that it
                # was discarded after.
                self._failure = None
                self._parts.clear()
        except BaseException:
            if owned:
                with self._update():
                    self._state = _S3WriteState.ABORT_FAILED
            raise

    def _params(self, method: str) -> dict[str, Any]:
        """Select the parameters of a request of an S3 operation.

        Args:
            method: The name of the client method, such as ``upload_part``.

        Returns:
            The prepared parameters of the operation, if any, else the
            inherited parameters that it accepts.
        """
        prepared = self._prepared_params.get(method)
        if prepared is not None:
            return dict(prepared)
        return self._core.operation_params(method, self._request_kwargs)

    def _initiate(self, params: Mapping[str, Any]) -> S3MultipartUpload:
        """Create the multipart upload; the request of the creation.

        Sent through the core, as the writer would select the parameters
        again and drop the prepared ones that the operation does not accept.

        Args:
            params: The parameters of the request, sent as given.

        Returns:
            The upload, stored in the writer before this method returns, as
            :meth:`S3MultipartWriter.initiate` stores it.
        """
        self._writer.upload = self._core.create_multipart_upload(self._path, **params)
        return self._writer.upload

    def _submit(self, fn: Callable[..., S3MultipartUploadPart], **kwargs: Any) -> None:
        """Track a part request and schedule it.

        Args:
            fn: The writer method that sends the request.
            **kwargs: The arguments of the method, whose ``part_number`` must
                be the next one.
        """
        request: Future[S3MultipartUploadPart] = Future()
        # Added first, so that a cancellation of the tracked request always
        # stops tracking it.
        request.add_done_callback(self._on_part_done)
        with self._lock:
            self._consume_finished()
            self._refuse_unless(_S3WriteState.ACTIVE, operation="upload a part")
            if len(self._requests) >= self._max_workers:
                raise RuntimeError(
                    f"{self._max_workers} part requests are in flight; wait for ready() first."
                )
            if kwargs["part_number"] != self._next_part_number:
                raise ValueError(
                    f"Expected part number {self._next_part_number}, got {kwargs['part_number']}."
                )
            # Tracked before it is scheduled, so that an interrupt while it is
            # being scheduled still lets abort() cancel it or wait for it.
            self._requests.add(request)
            self._next_part_number += 1
        try:
            with stop_retries_on(self._stop):
                self._executor.submit_to(request, fn, **kwargs)
        except BaseException as e:
            # Recorded before the cancellation, which would record its own.
            with self._update():
                self._fail(e, raised=True)
            # A request that was not scheduled never runs. A started one is
            # tracked until it settles.
            request.cancel()
            raise

    def _commit(self, state: _S3WriteState, send: Callable[[], T]) -> T:
        """Reserve the commit, send its request, and record the outcome.

        Args:
            state: The state that allows the commit: IDLE for PutObject, or
                ACTIVE for the completion of the multipart upload.
            send: Sends the request and returns its result.

        Returns:
            The result of the request, or of the earlier successful commit.
        """
        reserved = False
        try:
            with self._lock:
                if self._state is _S3WriteState.COMPLETED and self._multipart is (
                    state is _S3WriteState.ACTIVE
                ):
                    return cast(T, self._result)
                if self._state is _S3WriteState.COMPLETING and self._reservation is state:
                    # Reserved by begin_complete().
                    reserved = True
                    self._reservation = None
                else:
                    self._refuse_unless(state, operation="commit")
                    # Set first, so that an interrupt right after the state
                    # changes leaves the write failed, for the caller to
                    # abort, rather than being committed.
                    reserved = True
                    self._state = _S3WriteState.COMPLETING
            result = send()
            with self._update():
                # The result first, as the state publishes it.
                self._result = cast(S3PutObject | S3CompleteMultipartUpload, result)
                self._state = _S3WriteState.COMPLETED
        except BaseException as e:
            if reserved and self._state is not _S3WriteState.COMPLETED:
                with self._update():
                    self._fail(e, raised=True)
            raise
        return result

    def _complete_parts(self) -> S3CompleteMultipartUpload:
        """Complete the upload once its part requests have settled.

        Returns:
            The result of the completion.

        Raises:
            BaseException: The first failure of a request, without waiting
                for the others.
            RuntimeError: If the outcome of a part was not recorded.
        """
        with self._lock:
            self._consume_finished()
            while self._requests and self._failure_summary is None:
                self._changed.wait(_RECHECK_INTERVAL)
                self._consume_finished()
            self._refuse_unless(_S3WriteState.COMPLETING, operation="commit")
            parts = [self._parts[number] for number in sorted(self._parts)]
        if len(parts) != self._next_part_number - 1:
            # Every finished request has been consumed, so this guards the
            # invariant that no part is completed without its data.
            missing = sorted(set(range(1, self._next_part_number)) - set(self._parts))
            raise RuntimeError(f"The parts {missing} of {self._path.uri} have not been uploaded.")
        return self._writer.complete(parts, **self._params("complete_multipart_upload"))

    def _on_created(self, creation: Future[S3MultipartUpload]) -> None:
        """Consume the finished creation; the done callback of its future.

        Args:
            creation: The finished future of the creation.
        """
        with self._update():
            self._consume_creation(creation)

    def _on_part_done(self, request: Future[S3MultipartUploadPart]) -> None:
        """Consume a finished part request; the done callback of its future.

        Args:
            request: The finished future of the part request.
        """
        with self._update():
            self._consume(request)

    def _consume_finished(self) -> None:
        """Consume the requests that have finished; called with the lock held.

        A future is done before its done callbacks run, and an interrupt may
        stop a callback, so the operations that depend on the outcomes
        consume them here too. Whichever comes first records an outcome.
        """
        if self._creation is not None and self._creation.done():
            self._consume_creation(self._creation)
        for request in [request for request in self._requests if request.done()]:
            self._consume(request)

    def _consume_creation(self, creation: Future[S3MultipartUpload]) -> None:
        """Record the outcome of the creation once; called with the lock held.

        Args:
            creation: The finished future of the creation.
        """
        if self._creation is not creation:
            return
        if self._state is _S3WriteState.CREATING:
            if creation.cancelled():
                # Such as by the shutdown of the event loop of an S3AioExecutor.
                self._fail(CancelledError("The creation of the multipart upload was cancelled."))
            elif (error := creation.exception()) is not None:
                self._fail(error)
            else:
                self._state = _S3WriteState.ACTIVE
        # Cleared after the outcome is recorded, so that a callback stopped
        # in between leaves the creation for the next consumption.
        self._creation = None

    def _consume(self, request: Future[S3MultipartUploadPart]) -> None:
        """Record the outcome of a part request once and stop tracking it.

        Called with the lock held. The outcome is recorded before the request
        is untracked, so that nothing completes the upload without it. A
        request finished during or after an abort records no failure.

        Args:
            request: The finished future of the part request.
        """
        if request not in self._requests:
            return
        if request.cancelled():
            # Not by abort(), but such as by the shutdown of the event loop of
            # an S3AioExecutor, the upload lacks the part.
            error: BaseException | None = CancelledError(
                "A part request was cancelled before it ran."
            )
        else:
            error = request.exception()
        if error is None:
            part = request.result()
            self._parts[part.part_number] = part
        elif self._state not in (_S3WriteState.ABORTING, _S3WriteState.ABORT_FAILED):
            self._fail(error)
        self._requests.discard(request)

    def _fail(self, error: BaseException, *, raised: bool = False) -> None:
        """Record a failure; called with the lock held.

        The first failure that a request reports is kept until it is raised.
        A request failure leaves a write that is being committed in that
        state, as its commit raises the failure.

        Args:
            error: The error of the failure.
            raised: Whether the caller raises the error itself, such as the
                failure of a commit or of the scheduling of a request. Only
                its description is kept.
        """
        if self._failure_summary is None:
            # The error first, as the summary marks the failure as recorded.
            if not raised:
                self._failure = error
            self._failure_summary = f"{type(error).__name__}: {error}"
        if self._state in _WRITABLE_STATES or (raised and self._state is _S3WriteState.COMPLETING):
            self._state = _S3WriteState.FAILED

    @contextlib.contextmanager
    def _update(self) -> Iterator[None]:
        """Change the session with the lock held, and notify the waits.

        The waiters of :meth:`ready` are resolved after the lock is released,
        as their callbacks run in this thread.

        Yields:
            Nothing; the body changes the session.
        """
        with self._lock:
            yield
            self._changed.notify_all()
            waiters = self._take_waiters()
        self._notify(waiters)

    def _refuse_unless(self, *states: _S3WriteState, operation: str) -> None:
        """Raise unless the write is in one of the states without a failure.

        Called with the lock held. The first failure is raised only once, and
        is no longer referenced by the session, whose frames its traceback
        keeps.

        Args:
            *states: The states that allow the operation.
            operation: The operation, for the message of a state error.

        Raises:
            BaseException: The first failure, if it has not been raised yet.
            S3MultipartStateError: Otherwise, if the write is not in one of
                the states or has failed.
        """
        if self._state in states and self._failure_summary is None:
            return
        error, self._failure = self._failure, None
        if error is None:
            message = f"Cannot {operation}: the write to {self._path.uri} {self._state.value}."
            if self._failure_summary is not None:
                message += f" The first failure: {self._failure_summary}"
            error = S3MultipartStateError(message)
        try:
            raise error
        finally:
            error = None

    def _settle_creation(self) -> None:
        """Cancel the creation, or wait for a running one to finish.

        KeyboardInterrupts are suppressed while waiting, so that an upload
        that the creation still creates is recovered for the abort.
        """
        with self._lock:
            creation = self._creation
        if creation is None or creation.cancel():
            return
        while not creation.done():
            try:
                wait([creation])
            except KeyboardInterrupt:
                continue

    def _stop_requests(self) -> None:
        """Cancel the requests that have not started, and stop the retries of the others.

        The requests that have not started are cancelled before the stop,
        which would let a thread that it frees take one of them. Their done
        callbacks, which stop tracking them, run in this thread, so they are
        cancelled without the lock held.
        """
        with self._lock:
            creation = self._creation
            requests = list(self._requests)
        if creation is not None:
            creation.cancel()
        for request in requests:
            request.cancel()
        self._stop.set()

    def _waiter(self, limit: int) -> Future[None]:
        """Return a future that resolves when fewer than ``limit`` part requests are in flight.

        See :meth:`ready`.

        Args:
            limit: The number of part requests in flight below which the
                future resolves.

        Returns:
            A future whose result is None.
        """
        waiter: Future[None] = Future()
        with self._update():
            self._consume_finished()
            self._drop_cancelled_waiters()
            if not self._is_ready(limit):
                self._waiters.append((waiter, limit))
                return waiter
        waiter.set_result(None)
        return waiter

    def _is_ready(self, limit: int) -> bool:
        """Whether a waiter resolves; called with the lock held.

        Args:
            limit: The number of part requests in flight below which the
                waiter resolves while the upload is active.

        Returns:
            Whether the waiter resolves.
        """
        if self._state is _S3WriteState.CREATING:
            return False
        if self._state is _S3WriteState.ACTIVE and self._failure_summary is None:
            return len(self._requests) < limit
        return True

    def _take_waiters(self) -> list[Future[None]]:
        """Remove the waiters to resolve; called with the lock held.

        Returns:
            The waiters that resolve in the current state.
        """
        # Taken, so that each waiter is resolved by one thread only.
        taken: list[Future[None]] = []
        waiting: list[tuple[Future[None], int]] = []
        for waiter, limit in self._waiters:
            if self._is_ready(limit):
                taken.append(waiter)
            else:
                waiting.append((waiter, limit))
        self._waiters = waiting
        return taken

    def _drop_cancelled_waiters(self) -> None:
        """Remove the waiters that their callers cancelled; called with the lock held.

        They are marked as notified, as an executor does with a cancelled
        future, so that ``concurrent.futures.wait()`` counts them as done.
        """
        waiting = []
        for waiter, limit in self._waiters:
            if waiter.cancelled():
                waiter.set_running_or_notify_cancel()
            else:
                waiting.append((waiter, limit))
        self._waiters = waiting

    @staticmethod
    def _notify(waiters: list[Future[None]]) -> None:
        """Resolve the waiters without the lock held, as their callbacks run here.

        Args:
            waiters: The waiters taken by :meth:`_take_waiters`.
        """
        for waiter in waiters:
            if waiter.set_running_or_notify_cancel():
                waiter.set_result(None)
