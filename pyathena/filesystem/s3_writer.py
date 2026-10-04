# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Synchronous multipart writing and part planning without fsspec."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any, BinaryIO

from pyathena.filesystem.s3_core import S3Core
from pyathena.filesystem.s3_object import (
    S3CompleteMultipartUpload,
    S3MultipartUpload,
    S3MultipartUploadPart,
)
from pyathena.filesystem.s3_path import S3Path


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
