# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""The typed S3 operations under the S3 filesystem."""

from __future__ import annotations

import logging
import math
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, cast

import botocore.exceptions
from botocore.client import BaseClient

from pyathena.filesystem.s3_errors import S3ClientError
from pyathena.filesystem.s3_object import (
    S3CompleteMultipartUpload,
    S3Metadata,
    S3MultipartUpload,
    S3MultipartUploadPart,
    S3ObjectVersion,
)
from pyathena.filesystem.s3_path import S3Path
from pyathena.util import RetryConfig, override, retry_api_call

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class S3ObjectSummary:
    """An object listed by ListObjectsV2 (a ``Contents`` entry).

    Attributes:
        bucket: The bucket of the object.
        key: The key of the object.
        size: The size of the object in bytes.
        etag: The entity tag of the object.
        last_modified: When the object was last modified.
        storage_class: The storage class of the object.
    """

    bucket: str
    key: str
    size: int | None = None
    etag: str | None = None
    last_modified: datetime | None = None
    storage_class: str | None = None

    @classmethod
    def from_response(cls, bucket: str, entry: Mapping[str, Any]) -> S3ObjectSummary:
        """Build the object from a ``Contents`` entry of ListObjectsV2.

        Args:
            bucket: The bucket that was listed.
            entry: The ``Contents`` entry.

        Returns:
            The listed object.
        """
        return cls(
            bucket=bucket,
            key=entry["Key"],
            size=entry.get("Size"),
            etag=entry.get("ETag"),
            last_modified=entry.get("LastModified"),
            storage_class=entry.get("StorageClass"),
        )

    @property
    def path(self) -> S3Path:
        """The path of the object."""
        return S3Path(self.bucket, self.key)


@dataclass(frozen=True)
class S3CommonPrefix:
    """A key prefix listed with a delimiter (a ``CommonPrefixes`` entry).

    Attributes:
        bucket: The bucket that was listed.
        prefix: The key prefix, ending with the delimiter.
    """

    bucket: str
    prefix: str

    @classmethod
    def from_response(cls, bucket: str, entry: Mapping[str, Any]) -> S3CommonPrefix:
        """Build the prefix from a ``CommonPrefixes`` entry.

        Args:
            bucket: The bucket that was listed.
            entry: The ``CommonPrefixes`` entry.

        Returns:
            The listed prefix.
        """
        return cls(bucket=bucket, prefix=entry["Prefix"])


@dataclass(frozen=True)
class S3Bucket:
    """A bucket, as listed by ListBuckets or found by HeadBucket.

    Attributes:
        name: The name of the bucket.
        creation_date: When the bucket was created, if returned.
        bucket_region: The region of the bucket, if returned.
    """

    name: str
    creation_date: datetime | None = None
    bucket_region: str | None = None

    @classmethod
    def from_response(cls, entry: Mapping[str, Any]) -> S3Bucket:
        """Build the bucket from a ``Buckets`` entry of ListBuckets.

        Args:
            entry: The ``Buckets`` entry.

        Returns:
            The listed bucket.
        """
        return cls(
            name=entry["Name"],
            creation_date=entry.get("CreationDate"),
            bucket_region=entry.get("BucketRegion"),
        )


@dataclass(frozen=True)
class S3ListObjectsPage:
    """One page of a ListObjectsV2 listing.

    Attributes:
        bucket: The bucket that was listed.
        objects: The listed objects (``Contents``).
        common_prefixes: The listed key prefixes (``CommonPrefixes``).
        key_count: The number of objects and prefixes of the page
            (``KeyCount``), if returned.
        is_truncated: Whether more pages follow.
        next_continuation_token: The token of the next page, if any.
    """

    bucket: str
    objects: tuple[S3ObjectSummary, ...] = ()
    common_prefixes: tuple[S3CommonPrefix, ...] = ()
    key_count: int | None = None
    is_truncated: bool = False
    next_continuation_token: str | None = None

    @classmethod
    def from_response(cls, bucket: str, response: Mapping[str, Any]) -> S3ListObjectsPage:
        """Build the page from a ListObjectsV2 response.

        Args:
            bucket: The bucket that was listed.
            response: The ListObjectsV2 response.

        Returns:
            The page.
        """
        return cls(
            bucket=bucket,
            objects=tuple(
                S3ObjectSummary.from_response(bucket, c) for c in response.get("Contents", [])
            ),
            common_prefixes=tuple(
                S3CommonPrefix.from_response(bucket, c) for c in response.get("CommonPrefixes", [])
            ),
            key_count=response.get("KeyCount"),
            is_truncated=response.get("IsTruncated", False),
            next_continuation_token=response.get("NextContinuationToken"),
        )


@dataclass(frozen=True)
class S3ListObjectVersionsPage:
    """One page of a ListObjectVersions listing.

    Attributes:
        bucket: The bucket that was listed.
        versions: The listed versions (``Versions``).
        delete_markers: The listed delete markers (``DeleteMarkers``).
        common_prefixes: The listed key prefixes (``CommonPrefixes``).
        is_truncated: Whether more pages follow.
        next_key_marker: The key marker of the next page, if any.
        next_version_id_marker: The version ID marker of the next page, if
            any.
    """

    bucket: str
    versions: tuple[S3ObjectVersion, ...] = ()
    delete_markers: tuple[S3ObjectVersion, ...] = ()
    common_prefixes: tuple[S3CommonPrefix, ...] = ()
    is_truncated: bool = False
    next_key_marker: str | None = None
    next_version_id_marker: str | None = None

    @classmethod
    def from_response(cls, bucket: str, response: Mapping[str, Any]) -> S3ListObjectVersionsPage:
        """Build the page from a ListObjectVersions response.

        Args:
            bucket: The bucket that was listed.
            response: The ListObjectVersions response.

        Returns:
            The page.
        """
        return cls(
            bucket=bucket,
            versions=tuple(
                S3ObjectVersion(bucket=bucket, is_delete_marker=False, response=v)
                for v in response.get("Versions", [])
            ),
            delete_markers=tuple(
                S3ObjectVersion(bucket=bucket, is_delete_marker=True, response=m)
                for m in response.get("DeleteMarkers", [])
            ),
            common_prefixes=tuple(
                S3CommonPrefix.from_response(bucket, c) for c in response.get("CommonPrefixes", [])
            ),
            is_truncated=response.get("IsTruncated", False),
            next_key_marker=response.get("NextKeyMarker"),
            next_version_id_marker=response.get("NextVersionIdMarker"),
        )


@dataclass(frozen=True)
class S3ListBucketsPage:
    """One page of a ListBuckets listing.

    Attributes:
        buckets: The listed buckets.
        continuation_token: The token of the next page, if any.
    """

    buckets: tuple[S3Bucket, ...] = ()
    continuation_token: str | None = None

    @classmethod
    def from_response(cls, response: Mapping[str, Any]) -> S3ListBucketsPage:
        """Build the page from a ListBuckets response.

        Args:
            response: The ListBuckets response.

        Returns:
            The page.
        """
        return cls(
            buckets=tuple(S3Bucket.from_response(b) for b in response.get("Buckets", [])),
            continuation_token=response.get("ContinuationToken"),
        )


@dataclass(frozen=True)
class S3DeleteBatch:
    """The objects of one bucket that one DeleteObjects request deletes.

    A batch is valid by construction: it has 1 to ``MAX_KEYS`` objects, each
    with a key in the bucket of the batch. A path with a version ID deletes
    that version.

    Attributes:
        bucket: The bucket of the objects.
        objects: The paths of the objects to delete.
        quiet: Whether S3 omits the deleted objects from the response.
    """

    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
    MAX_KEYS: ClassVar[int] = 1000

    bucket: str
    objects: tuple[S3Path, ...]
    quiet: bool = True

    def __post_init__(self) -> None:
        """Validate the objects of the batch.

        Raises:
            ValueError: If the batch has no objects or more than
                ``MAX_KEYS``, or an object has no key or another bucket.
        """
        if not 0 < len(self.objects) <= self.MAX_KEYS:
            raise ValueError(f"A batch has 1 to {self.MAX_KEYS} objects, not {len(self.objects)}.")
        for path in self.objects:
            if not path.key or path.bucket != self.bucket:
                raise ValueError(f"Not an object of the bucket {self.bucket}: {path.uri}.")

    @classmethod
    def from_paths(cls, paths: Iterable[S3Path], quiet: bool = True) -> list[S3DeleteBatch]:
        """Group the paths into batches by bucket, in the order of the paths.

        Args:
            paths: The paths of the objects to delete.
            quiet: Whether S3 omits the deleted objects from the responses.

        Returns:
            The batches of up to ``MAX_KEYS`` objects of one bucket each.

        Raises:
            ValueError: If a path has no key.
        """
        objects: dict[str, list[S3Path]] = {}
        for path in paths:
            objects.setdefault(path.bucket, []).append(path)
        return [
            cls(bucket=bucket, objects=tuple(paths_[i : i + cls.MAX_KEYS]), quiet=quiet)
            for bucket, paths_ in objects.items()
            for i in range(0, len(paths_), cls.MAX_KEYS)
        ]


@dataclass(frozen=True)
class S3DeleteError:
    """An object that DeleteObjects could not delete (an ``Errors`` entry).

    ``str()`` gives ``"path (code: message)"``.

    Attributes:
        path: The path of the object, with the version ID of the request, if
            any.
        code: The error code.
        message: The error message.
    """

    path: S3Path
    code: str | None = None
    message: str | None = None

    @classmethod
    def from_response(cls, bucket: str, entry: Mapping[str, Any]) -> S3DeleteError:
        """Build the error from an ``Errors`` entry of DeleteObjects.

        Args:
            bucket: The bucket of the request.
            entry: The ``Errors`` entry.

        Returns:
            The error.
        """
        return cls(
            path=S3Path(bucket, entry["Key"], entry.get("VersionId")),
            code=entry.get("Code"),
            message=entry.get("Message"),
        )

    @override
    def __str__(self) -> str:
        return f"{self.path} ({self.code}: {self.message})"


@dataclass(frozen=True)
class S3DeleteResult:
    """The result of a DeleteObjects request.

    S3 answers a request with 200 also when it could not delete some of the
    objects, and lists them in ``errors``.

    Attributes:
        bucket: The bucket of the request.
        deleted: The deleted objects, with the version ID of the request, if
            any. Empty for a quiet batch.
        errors: The objects that S3 could not delete.
    """

    bucket: str
    deleted: tuple[S3Path, ...] = ()
    errors: tuple[S3DeleteError, ...] = ()

    @classmethod
    def from_response(cls, bucket: str, response: Mapping[str, Any]) -> S3DeleteResult:
        """Build the result from a DeleteObjects response.

        Args:
            bucket: The bucket of the request.
            response: The DeleteObjects response.

        Returns:
            The result.
        """
        return cls(
            bucket=bucket,
            deleted=tuple(
                S3Path(bucket, d["Key"], d.get("VersionId")) for d in response.get("Deleted", [])
            ),
            errors=tuple(
                S3DeleteError.from_response(bucket, e) for e in response.get("Errors", [])
            ),
        )


class S3Core:
    """Typed S3 operations, one request each, on a boto3 S3 client.

    Each operation sends one request, or one per page for the iterators,
    with the retry policy, and translates S3 errors into ``OSError``
    subclasses (see :class:`~pyathena.filesystem.s3_errors.S3ClientError`):
    a missing bucket, or a missing object or version that an operation reads,
    raises ``FileNotFoundError``, and a denied request ``PermissionError``.
    As in S3, deleting a missing key is not an error. Nothing is cached.

    Example:
        >>> core = S3Core(boto3.client("s3"))
        >>> core.head_object(S3Path.parse("s3://bucket/key")).content_length
        >>> for page in core.list_objects("bucket", prefix="dir/", delimiter="/"):
        ...     print([o.key for o in page.objects])
    """

    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
    # The minimum size of a part in a multipart upload is 5MiB.
    MULTIPART_UPLOAD_MIN_PART_SIZE: int = 5 * 2**20  # 5MiB
    # The maximum size of a part in a multipart upload is 5GiB.
    MULTIPART_UPLOAD_MAX_PART_SIZE: int = 5 * 2**30  # 5GiB
    # The maximum number of parts per multipart upload is 10,000.
    MULTIPART_UPLOAD_MAX_PARTS: int = 10_000

    def __init__(
        self,
        client: BaseClient,
        retry_config: RetryConfig | None = None,
        request_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        """Create the operations on a client.

        Args:
            client: The boto3 S3 client to send the requests with.
            retry_config: The retry policy of the requests; defaults to
                ``RetryConfig()``.
            request_kwargs: Parameters sent with every operation that accepts
                them, such as ``{"RequestPayer": "requester"}``.
        """
        self._client = client
        self._retry_config = retry_config if retry_config else RetryConfig()
        self._request_kwargs = dict(request_kwargs) if request_kwargs else {}

    @property
    def client(self) -> BaseClient:
        """The boto3 S3 client."""
        return self._client

    @property
    def retry_config(self) -> RetryConfig:
        """The retry policy of the requests."""
        return self._retry_config

    @property
    def request_kwargs(self) -> dict[str, Any]:
        """A copy of the parameters sent with every operation that accepts them."""
        return dict(self._request_kwargs)

    def operation_params(self, method: str, params: Mapping[str, Any]) -> dict[str, Any]:
        """Select the parameters that an S3 operation accepts.

        Parameters inherited by several requests are filtered by the input
        shape of each operation, so that, e.g., ``ServerSideEncryption`` for
        writes is not sent with GetObject.

        Args:
            method: The name of the client method, such as ``get_object``.
            params: The parameters to select from.

        Returns:
            The parameters that the operation accepts. Empty for a method
            that is not an S3 API operation, such as
            ``generate_presigned_url``.
        """
        operation = self._client.meta.method_to_api_mapping.get(method)
        if not params or operation is None:
            return {}
        members = self._client.meta.service_model.operation_model(operation).input_shape.members
        return {k: v for k, v in params.items() if k in members}

    def call(self, method: str | Callable[..., Any], **request) -> dict[str, Any]:
        """Send a request with the retry policy and translate its errors.

        ``request_kwargs`` that the operation accepts are added; a parameter
        of the request takes precedence.

        Args:
            method: The name of the client method, or the method itself.
            **request: The request parameters, sent as given.

        Returns:
            The response.

        Raises:
            OSError: The translation of an S3 error response (see
                :class:`~pyathena.filesystem.s3_errors.S3ClientError`).
        """
        func = getattr(self._client, method) if isinstance(method, str) else method
        if self._request_kwargs:
            request = {**self.operation_params(func.__name__, self._request_kwargs), **request}
        try:
            response = retry_api_call(func, config=self._retry_config, logger=_logger, **request)
        except botocore.exceptions.ClientError as e:
            raise S3ClientError(e).os_error from e
        return cast(dict[str, Any], response)

    def head_object(self, path: S3Path, **params) -> S3Metadata:
        """Look up an object, or a version of it, with HeadObject.

        Args:
            path: The path of the object, with the version ID to look up, if
                any.
            **params: Additional request parameters, sent as given.

        Returns:
            The metadata of the object.

        Raises:
            ValueError: If the path has no key.
            FileNotFoundError: If the object or version does not exist.
        """
        if not path.key:
            raise ValueError(f"The path has no key: {path.uri}.")
        request: dict[str, Any] = {"Bucket": path.bucket, "Key": path.key}
        if path.version_id:
            request.update({"VersionId": path.version_id})
        response = self.call(self._client.head_object, **request, **params)
        return S3Metadata(response, path=path)

    def head_bucket(self, bucket: str, **params) -> S3Bucket:
        """Look up a bucket with HeadBucket.

        Args:
            bucket: The name of the bucket.
            **params: Additional request parameters, sent as given.

        Returns:
            The bucket, with its region if returned.

        Raises:
            FileNotFoundError: If the bucket does not exist.
        """
        response = self.call(self._client.head_bucket, Bucket=bucket, **params)
        return S3Bucket(name=bucket, bucket_region=response.get("BucketRegion"))

    def delete_object(self, path: S3Path, **params) -> None:
        """Delete an object, or a version of it, with DeleteObject.

        Args:
            path: The path of the object, with the version ID to delete, if
                any.
            **params: Additional request parameters, sent as given.

        Raises:
            ValueError: If the path has no key.
            FileNotFoundError: If the bucket does not exist.
        """
        if not path.key:
            raise ValueError(f"The path has no key: {path.uri}.")
        request: dict[str, Any] = {"Bucket": path.bucket, "Key": path.key}
        if path.version_id:
            request.update({"VersionId": path.version_id})
        self.call(self._client.delete_object, **request, **params)

    def delete_objects(self, batch: S3DeleteBatch, **params) -> S3DeleteResult:
        """Delete the objects of a batch with one DeleteObjects request.

        Args:
            batch: The objects to delete.
            **params: Additional request parameters, sent as given.

        Returns:
            The result, with the objects that S3 could not delete in its
            ``errors``.

        Raises:
            FileNotFoundError: If the bucket does not exist.
        """
        objects = []
        for path in batch.objects:
            object_ = {"Key": path.key}
            if path.version_id:
                object_.update({"VersionId": path.version_id})
            objects.append(object_)
        response = self.call(
            self._client.delete_objects,
            Bucket=batch.bucket,
            Delete={"Objects": objects, "Quiet": batch.quiet},
            **params,
        )
        return S3DeleteResult.from_response(batch.bucket, response)

    def create_multipart_upload(self, path: S3Path, **params) -> S3MultipartUpload:
        """Start a multipart upload to an object with CreateMultipartUpload.

        Args:
            path: The path of the object to write, without a version ID.
            **params: Additional request parameters. The bucket and key of
                the path take precedence over parameters of the same name.

        Returns:
            The multipart upload.

        Raises:
            ValueError: If the path has no key, or has a version ID, which a
                write cannot replace.
        """
        if not path.key:
            raise ValueError(f"The path has no key: {path.uri}.")
        if path.version_id:
            raise ValueError(f"Cannot write to a version: {path.uri}.")
        request: dict[str, Any] = {"Bucket": path.bucket, "Key": path.key}
        _logger.debug(f"Create multipart upload to {path.uri}.")
        response = self.call(self._client.create_multipart_upload, **{**params, **request})
        return S3MultipartUpload(response)

    def upload_part(
        self, path: S3Path, upload_id: str, part_number: int, body: bytes, **params
    ) -> S3MultipartUploadPart:
        """Upload a part of a multipart upload with UploadPart.

        Args:
            path: The path of the object that the upload writes.
            upload_id: The ID of the multipart upload.
            part_number: The number of the part, from 1.
            body: The data of the part.
            **params: Additional request parameters. The fields that the
                other arguments set take precedence over parameters of the
                same name.

        Returns:
            The uploaded part.

        Raises:
            ValueError: If the path has no key.
        """
        if not path.key:
            raise ValueError(f"The path has no key: {path.uri}.")
        request: dict[str, Any] = {
            "Bucket": path.bucket,
            "Key": path.key,
            "UploadId": upload_id,
            "PartNumber": part_number,
            "Body": body,
        }
        _logger.debug(f"Upload part of {upload_id} to {path.uri} as part {part_number}.")
        response = self.call(self._client.upload_part, **{**params, **request})
        return S3MultipartUploadPart(part_number, response)

    def upload_part_copy(
        self,
        path: S3Path,
        upload_id: str,
        part_number: int,
        source: S3Path,
        range_: tuple[int, int] | None = None,
        **params,
    ) -> S3MultipartUploadPart:
        """Copy a part of a multipart upload from an object with UploadPartCopy.

        Args:
            path: The path of the object that the upload writes.
            upload_id: The ID of the multipart upload.
            part_number: The number of the part, from 1.
            source: The path of the object to copy, with the version ID to
                copy, if any.
            range_: The ``(start, end)`` byte range of the source to copy,
                with an exclusive end; None copies the whole source.
            **params: Additional request parameters. The fields that the
                other arguments set take precedence over parameters of the
                same name.

        Returns:
            The copied part.

        Raises:
            ValueError: If the path or the source has no key.
        """
        if not path.key:
            raise ValueError(f"The path has no key: {path.uri}.")
        if not source.key:
            raise ValueError(f"The source has no key: {source.uri}.")
        copy_source: dict[str, Any] = {"Bucket": source.bucket, "Key": source.key}
        if source.version_id:
            copy_source.update({"VersionId": source.version_id})
        request: dict[str, Any] = {
            "Bucket": path.bucket,
            "Key": path.key,
            "CopySource": copy_source,
            "UploadId": upload_id,
            "PartNumber": part_number,
        }
        if range_:
            request.update({"CopySourceRange": f"bytes={range_[0]}-{range_[1] - 1}"})
        _logger.debug(f"Upload part copy from {source.uri} to {path.uri} as part {part_number}.")
        response = self.call(self._client.upload_part_copy, **{**params, **request})
        return S3MultipartUploadPart(part_number, response)

    def complete_multipart_upload(
        self, path: S3Path, upload_id: str, parts: Sequence[S3MultipartUploadPart], **params
    ) -> S3CompleteMultipartUpload:
        """Complete a multipart upload with CompleteMultipartUpload.

        Args:
            path: The path of the object that the upload writes.
            upload_id: The ID of the multipart upload.
            parts: The uploaded parts, in part-number order.
            **params: Additional request parameters. The fields that the
                other arguments set take precedence over parameters of the
                same name.

        Returns:
            The completed upload.

        Raises:
            ValueError: If the path has no key.
        """
        if not path.key:
            raise ValueError(f"The path has no key: {path.uri}.")
        request: dict[str, Any] = {
            "Bucket": path.bucket,
            "Key": path.key,
            "UploadId": upload_id,
            "MultipartUpload": {
                "Parts": [{"ETag": p.etag, "PartNumber": p.part_number} for p in parts]
            },
        }
        _logger.debug(f"Complete multipart upload {upload_id} to {path.uri}.")
        response = self.call(self._client.complete_multipart_upload, **{**params, **request})
        return S3CompleteMultipartUpload(response)

    def abort_multipart_upload(self, path: S3Path, upload_id: str, **params) -> None:
        """Abort a multipart upload with AbortMultipartUpload.

        Args:
            path: The path of the object that the upload writes.
            upload_id: The ID of the multipart upload.
            **params: Additional request parameters. The fields that the
                other arguments set take precedence over parameters of the
                same name.

        Raises:
            ValueError: If the path has no key.
            FileNotFoundError: If the upload does not exist, for example
                because it was completed or aborted.
        """
        if not path.key:
            raise ValueError(f"The path has no key: {path.uri}.")
        request: dict[str, Any] = {"Bucket": path.bucket, "Key": path.key, "UploadId": upload_id}
        self.call(self._client.abort_multipart_upload, **{**params, **request})

    def part_ranges(self, size: int, block_size: int) -> list[tuple[int, int]]:
        """Split an object into the source ranges of the parts that copy it.

        The object is split into ranges of ``block_size`` bytes, or of a
        larger size that splits it into at most
        ``MULTIPART_UPLOAD_MAX_PARTS`` ranges. A last range shorter than
        ``MULTIPART_UPLOAD_MIN_PART_SIZE`` is merged into the previous one,
        which is split in half if the result exceeds
        ``MULTIPART_UPLOAD_MAX_PART_SIZE``. Every range is then within the
        S3 part size limits, including the last one unless the whole object
        is smaller than the minimum part size, so that more parts can follow
        the copied ones, as in an append.

        Args:
            size: The size of the source object in bytes.
            block_size: The size in bytes to split the object by, between
                ``MULTIPART_UPLOAD_MIN_PART_SIZE`` and
                ``MULTIPART_UPLOAD_MAX_PART_SIZE``. It is raised to
                ``size`` divided by ``MULTIPART_UPLOAD_MAX_PARTS``, rounded
                up, if smaller. The range that a short last range is merged
                into can be longer, up to ``MULTIPART_UPLOAD_MAX_PART_SIZE``.

        Returns:
            The ``(start, end)`` byte ranges, with an exclusive end, that
            cover the whole object in order.
        """
        block_size = max(block_size, math.ceil(size / self.MULTIPART_UPLOAD_MAX_PARTS))
        starts = list(range(0, size, block_size))
        if len(starts) > 1 and size - starts[-1] < self.MULTIPART_UPLOAD_MIN_PART_SIZE:
            starts.pop()
            if size - starts[-1] > self.MULTIPART_UPLOAD_MAX_PART_SIZE:
                starts.append(starts[-1] + (size - starts[-1]) // 2)
        return list(zip(starts, [*starts[1:], size], strict=True))

    def list_objects_page(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str | None = None,
        max_keys: int | None = None,
        continuation_token: str | None = None,
        **params,
    ) -> S3ListObjectsPage:
        """List one page of objects with ListObjectsV2.

        Args:
            bucket: The bucket to list.
            prefix: The key prefix to list.
            delimiter: The delimiter to group keys by; None or ``""`` lists
                recursively.
            max_keys: The maximum number of keys of the page.
            continuation_token: The token of the page to list.
            **params: Additional request parameters, sent as given.

        Returns:
            The page.
        """
        request: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if delimiter is not None:
            request.update({"Delimiter": delimiter})
        if continuation_token:
            request.update({"ContinuationToken": continuation_token})
        if max_keys is not None:
            request.update({"MaxKeys": max_keys})
        response = self.call(self._client.list_objects_v2, **request, **params)
        return S3ListObjectsPage.from_response(bucket, response)

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str | None = None,
        max_keys: int | None = None,
        continuation_token: str | None = None,
        **params,
    ) -> Iterator[S3ListObjectsPage]:
        """List the pages of objects with ListObjectsV2, one request each.

        Args:
            bucket: The bucket to list.
            prefix: The key prefix to list.
            delimiter: The delimiter to group keys by; None or ``""`` lists
                recursively.
            max_keys: The maximum number of keys per page.
            continuation_token: The token of the first page to list.
            **params: Additional request parameters, sent as given.

        Yields:
            The pages, until one has no continuation token.

        Raises:
            TypeError: If ``params`` has ``ContinuationToken``, which the
                iterator advances; pass ``continuation_token`` instead.
        """
        if "ContinuationToken" in params:
            raise TypeError("Pass the first page's token as continuation_token.")
        while True:
            page = self.list_objects_page(
                bucket,
                prefix=prefix,
                delimiter=delimiter,
                max_keys=max_keys,
                continuation_token=continuation_token,
                **params,
            )
            yield page
            continuation_token = page.next_continuation_token
            if not continuation_token:
                return

    def list_object_versions_page(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str | None = None,
        key_marker: str | None = None,
        version_id_marker: str | None = None,
        **params,
    ) -> S3ListObjectVersionsPage:
        """List one page of versions with ListObjectVersions.

        Args:
            bucket: The bucket to list.
            prefix: The key prefix to list.
            delimiter: The delimiter to group keys by; None or ``""`` lists
                recursively.
            key_marker: The key marker of the page to list.
            version_id_marker: The version ID marker of the page to list. S3
                accepts it only with a key marker.
            **params: Additional request parameters, sent as given.

        Returns:
            The page.
        """
        request: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if delimiter is not None:
            request.update({"Delimiter": delimiter})
        if key_marker is not None:
            request.update({"KeyMarker": key_marker})
        if version_id_marker is not None:
            request.update({"VersionIdMarker": version_id_marker})
        response = self.call(self._client.list_object_versions, **request, **params)
        return S3ListObjectVersionsPage.from_response(bucket, response)

    def list_object_versions(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str | None = None,
        key_marker: str | None = None,
        version_id_marker: str | None = None,
        **params,
    ) -> Iterator[S3ListObjectVersionsPage]:
        """List the pages of versions with ListObjectVersions, one request each.

        Args:
            bucket: The bucket to list.
            prefix: The key prefix to list.
            delimiter: The delimiter to group keys by; None or ``""`` lists
                recursively.
            key_marker: The key marker of the first page to list.
            version_id_marker: The version ID marker of the first page to
                list, sent with ``key_marker``.
            **params: Additional request parameters, sent as given.

        Yields:
            The pages, until one is not truncated or has no key marker.

        Raises:
            TypeError: If ``params`` has ``KeyMarker`` or ``VersionIdMarker``,
                which the iterator advances; pass ``key_marker`` and
                ``version_id_marker`` instead.
        """
        if "KeyMarker" in params or "VersionIdMarker" in params:
            raise TypeError("Pass the first page's markers as key_marker and version_id_marker.")
        while True:
            page = self.list_object_versions_page(
                bucket,
                prefix=prefix,
                delimiter=delimiter,
                key_marker=key_marker,
                version_id_marker=version_id_marker,
                **params,
            )
            yield page
            if not page.is_truncated or not page.next_key_marker:
                return
            key_marker = page.next_key_marker
            version_id_marker = page.next_version_id_marker or ""

    def list_buckets_page(
        self, continuation_token: str | None = None, **params
    ) -> S3ListBucketsPage:
        """List one page of the buckets of the caller with ListBuckets.

        Args:
            continuation_token: The token of the page to list.
            **params: Additional request parameters, sent as given.

        Returns:
            The page.
        """
        request: dict[str, Any] = {}
        if continuation_token:
            request.update({"ContinuationToken": continuation_token})
        response = self.call(self._client.list_buckets, **request, **params)
        return S3ListBucketsPage.from_response(response)

    def list_buckets(
        self, continuation_token: str | None = None, **params
    ) -> Iterator[S3ListBucketsPage]:
        """List the pages of the buckets of the caller, one request each.

        Args:
            continuation_token: The token of the first page to list.
            **params: Additional request parameters, sent as given.

        Yields:
            The pages, until one has no continuation token.

        Raises:
            TypeError: If ``params`` has ``ContinuationToken``, which the
                iterator advances; pass ``continuation_token`` instead.
        """
        if "ContinuationToken" in params:
            raise TypeError("Pass the first page's token as continuation_token.")
        while True:
            page = self.list_buckets_page(continuation_token=continuation_token, **params)
            yield page
            continuation_token = page.continuation_token
            if not continuation_token:
                return
