# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""The typed S3 operations under the S3 filesystem."""

from __future__ import annotations

import contextlib
import logging
import math
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar, cast
from urllib.parse import urlencode

import botocore.exceptions
from botocore.client import BaseClient

from pyathena.filesystem.s3_errors import S3ClientError
from pyathena.filesystem.s3_object import (
    S3CompleteMultipartUpload,
    S3Metadata,
    S3MultipartUpload,
    S3MultipartUploadPart,
    S3ObjectVersion,
    S3PutObject,
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
class S3ListMultipartUploadsPage:
    """One page of a ListMultipartUploads listing.

    Attributes:
        bucket: The bucket that was listed.
        uploads: The listed in-progress multipart uploads (``Uploads``),
            with the listed bucket as their bucket.
        is_truncated: Whether more pages follow.
        next_key_marker: The key marker of the next page, if any.
        next_upload_id_marker: The upload ID marker of the next page, if any.
    """

    bucket: str
    uploads: tuple[S3MultipartUpload, ...] = ()
    is_truncated: bool = False
    next_key_marker: str | None = None
    next_upload_id_marker: str | None = None

    @classmethod
    def from_response(cls, bucket: str, response: Mapping[str, Any]) -> S3ListMultipartUploadsPage:
        """Build the page from a ListMultipartUploads response.

        Args:
            bucket: The bucket that was listed.
            response: The ListMultipartUploads response.

        Returns:
            The page.
        """
        return cls(
            bucket=bucket,
            uploads=tuple(
                S3MultipartUpload({**u, "Bucket": bucket}) for u in response.get("Uploads", [])
            ),
            is_truncated=response.get("IsTruncated", False),
            next_key_marker=response.get("NextKeyMarker"),
            next_upload_id_marker=response.get("NextUploadIdMarker"),
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


@dataclass(frozen=True)
class S3MultipartCopyPlan:
    """The requests of a copy with a multipart upload, as CopyObject copies.

    :meth:`S3Core.plan_multipart_copy` reads the source and builds the plan;
    the caller schedules the requests: CreateMultipartUpload with
    ``create_params``, one UploadPartCopy per range with ``part_params``,
    then CompleteMultipartUpload with ``complete_params``, or
    AbortMultipartUpload with ``abort_params`` after a failure, and finally
    the copy of each annotation with :meth:`S3Core.copy_object_annotation`.
    If ``fits_single_request`` is true, the source is copied with
    :meth:`S3Core.copy_object` instead, and ``ranges``, the parameters and
    ``annotations`` are empty.

    Attributes:
        source: The object to copy, with the version that HeadObject
            reported unless the path has one or the version is ``null``.
        destination: The object that the copy writes.
        size: The size in bytes of the source, from HeadObject.
        ranges: The ``(start, end)`` byte ranges of the source that the parts
            copy, with an exclusive end, in part-number order.
        create_params: The parameters of CreateMultipartUpload, with the
            metadata and the tags that CopyObject would write.
        part_params: The parameters of each UploadPartCopy.
        complete_params: The parameters of CompleteMultipartUpload.
        abort_params: The parameters of AbortMultipartUpload.
        annotations: The names of the annotations to copy; empty if the
            directive excludes them or the source cannot have any.
        fits_single_request: Whether the source fits in a single CopyObject
            request.
    """

    source: S3Path
    destination: S3Path
    size: int
    ranges: tuple[tuple[int, int], ...] = ()
    create_params: Mapping[str, Any] = field(default_factory=dict)
    part_params: Mapping[str, Any] = field(default_factory=dict)
    complete_params: Mapping[str, Any] = field(default_factory=dict)
    abort_params: Mapping[str, Any] = field(default_factory=dict)
    annotations: tuple[str, ...] = ()
    fits_single_request: bool = False


class S3Core:
    """Typed S3 operations on a boto3 S3 client.

    Each operation sends one request, or one per page for the iterators,
    except those that say otherwise, such as :meth:`plan_multipart_copy`,
    :meth:`copy_object_annotation` and :meth:`generate_presigned_url`. The
    requests are sent with the retry policy, and S3 errors are translated
    into ``OSError`` subclasses (see
    :class:`~pyathena.filesystem.s3_errors.S3ClientError`): a missing bucket
    or multipart upload, or a missing object or version that an operation
    reads, raises ``FileNotFoundError``, and a denied request
    ``PermissionError``.
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
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html
    # The metadata that CopyObject copies from the source with the COPY
    # metadata directive, which ignores the values given in the request.
    _COPY_METADATA_PARAMS: ClassVar[tuple[str, ...]] = (
        "CacheControl",
        "ContentDisposition",
        "ContentEncoding",
        "ContentLanguage",
        "ContentType",
        "Expires",
        "Metadata",
    )
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html
    # The CopyObject parameters that set the encryption of the copy.
    _SSE_COPY_PARAMS: ClassVar[frozenset[str]] = frozenset(
        {
            "ServerSideEncryption",
            "SSEKMSKeyId",
            "SSEKMSEncryptionContext",
            "BucketKeyEnabled",
            "SSECustomerAlgorithm",
            "SSECustomerKey",
            "SSECustomerKeyMD5",
        }
    )
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl
    # The canned ACLs that an object accepts.
    OBJECT_ACLS: ClassVar[frozenset[str]] = frozenset(
        {
            "private",
            "public-read",
            "public-read-write",
            "authenticated-read",
            "aws-exec-read",
            "bucket-owner-read",
            "bucket-owner-full-control",
        }
    )
    # The canned ACLs that a bucket accepts.
    BUCKET_ACLS: ClassVar[frozenset[str]] = frozenset(
        {"private", "public-read", "public-read-write", "authenticated-read"}
    )

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

    def get_object(
        self, path: S3Path, range_: tuple[int, int | None] | None = None, **params
    ) -> bytes:
        """Read an object, a version of it, or a byte range of it with GetObject.

        The body of the response is read whole and then closed, also when
        the read fails. A failure to read the body is neither translated nor
        retried: the botocore exception, such as ``ReadTimeoutError`` or
        ``IncompleteReadError``, propagates.

        Args:
            path: The path of the object, with the version ID to read, if
                any.
            range_: The ``(start, end)`` byte range to read, with an exclusive
                end, or None as the end to read to the end of the object. A
                negative start without an end reads the last ``-start``
                bytes, or the whole object if it is shorter. None sends no
                range of its own, so the whole object is read unless
                ``params`` or ``request_kwargs`` have ``Range``.
            **params: Additional request parameters. The fields that the
                other arguments set take precedence over parameters of the
                same name.

        Returns:
            The bytes read.

        Raises:
            ValueError: If the path has no key, or the range is empty or has
                both a negative start and an end, which S3 would ignore and
                return the whole object for.
            FileNotFoundError: If the object or version does not exist.
            OSError: If the range starts at or past the end of the object.
                Its ``__cause__`` is the ``ClientError`` with the
                ``InvalidRange`` code.
        """
        if not path.key:
            raise ValueError(f"The path has no key: {path.uri}.")
        request: dict[str, Any] = {"Bucket": path.bucket, "Key": path.key}
        if path.version_id:
            request.update({"VersionId": path.version_id})
        if range_ is not None:
            start, end = range_
            if end is None:
                request.update({"Range": f"bytes={start}" if start < 0 else f"bytes={start}-"})
            elif start < 0 or start >= end:
                raise ValueError(f"Invalid range: {range_}.")
            else:
                request.update({"Range": f"bytes={start}-{end - 1}"})
        request = {**params, **request}
        _logger.debug(f"Get object: {path.uri} range={request.get('Range')}")
        response = self.call(self._client.get_object, **request)
        # Read through the StreamingBody, which verifies the length and the
        # checksum of the data; entering it would return the raw stream.
        with contextlib.closing(response["Body"]) as body:
            return cast(bytes, body.read())

    def put_object(self, path: S3Path, body: bytes | None = None, **params) -> S3PutObject:
        """Write an object with PutObject.

        Args:
            path: The path of the object to write, without a version ID.
            body: The data to write. None or empty bytes send no body of
                their own, so an empty object is written unless ``params`` or
                ``request_kwargs`` have ``Body``.
            **params: Additional request parameters. The fields that the
                other arguments set take precedence over parameters of the
                same name.

        Returns:
            The result of the write.

        Raises:
            ValueError: If the path has no key, or has a version ID, which a
                write cannot replace.
        """
        if not path.key:
            raise ValueError(f"The path has no key: {path.uri}.")
        if path.version_id:
            raise ValueError(f"Cannot write to a version: {path.uri}.")
        request: dict[str, Any] = {"Bucket": path.bucket, "Key": path.key}
        if body:
            request.update({"Body": body})
        _logger.debug(f"Put object: {path.uri}")
        response = self.call(self._client.put_object, **{**params, **request})
        return S3PutObject(response)

    def create_bucket(
        self,
        bucket: str,
        acl: str | None = None,
        region_name: str | None = None,
        **params,
    ) -> None:
        """Create a bucket with CreateBucket.

        A call creates the bucket; the ``allow_bucket_creation`` option of
        the filesystem applies only to the filesystem's methods.

        Args:
            bucket: The name of the bucket.
            acl: The canned ACL of the bucket, one of ``BUCKET_ACLS``. None
                or an empty string sends no ACL.
            region_name: The region to create the bucket in. None or an
                empty string uses the region of the client. A location
                constraint is sent for every region except ``us-east-1``,
                which does not accept one.
            **params: Additional request parameters, sent as given. A
                ``CreateBucketConfiguration`` given here is sent instead of
                the location constraint of ``region_name``, in every region.

        Raises:
            ValueError: If the ACL is not a canned ACL of buckets.
        """
        if acl and acl not in self.BUCKET_ACLS:
            raise ValueError(f"ACL not in {self.BUCKET_ACLS}.")
        request: dict[str, Any] = {"Bucket": bucket}
        if acl:
            request.update({"ACL": acl})
        region_name = region_name or self._client.meta.region_name
        if "CreateBucketConfiguration" not in params and region_name and region_name != "us-east-1":
            request.update({"CreateBucketConfiguration": {"LocationConstraint": region_name}})
        _logger.debug(f"Create bucket: s3://{bucket}")
        self.call(self._client.create_bucket, **request, **params)

    def delete_bucket(self, bucket: str, **params) -> None:
        """Delete a bucket, which must be empty, with DeleteBucket.

        A call deletes the bucket; the ``allow_bucket_deletion`` option of
        the filesystem applies only to the filesystem's methods.

        Args:
            bucket: The name of the bucket.
            **params: Additional request parameters, sent as given.

        Raises:
            FileNotFoundError: If the bucket does not exist.
        """
        _logger.debug(f"Delete bucket: s3://{bucket}")
        self.call(self._client.delete_bucket, Bucket=bucket, **params)

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
            The upload retaining the path's bucket and key, including an
            access point alias or ARN, and the response's upload ID and
            checksum configuration.

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
        # S3 returns the bucket name even when creation uses an access point.
        # Keep the request identity for every subsequent upload operation.
        return S3MultipartUpload({**response, "Bucket": path.bucket, "Key": path.key})

    @staticmethod
    def _multipart_upload_request(upload: S3MultipartUpload) -> dict[str, Any]:
        """Return the upload identity, rejecting incomplete upload objects."""
        if not upload.bucket:
            raise ValueError("The multipart upload has no bucket.")
        if not upload.key:
            raise ValueError("The multipart upload has no key.")
        if not upload.upload_id:
            raise ValueError("The multipart upload has no upload ID.")
        return {"Bucket": upload.bucket, "Key": upload.key, "UploadId": upload.upload_id}

    def upload_part(
        self, upload: S3MultipartUpload, part_number: int, body: bytes, **params
    ) -> S3MultipartUploadPart:
        """Upload a part of a multipart upload with UploadPart.

        Args:
            upload: The multipart upload returned by creation. Its checksum
                algorithm is passed to the SDK to calculate the part checksum.
            part_number: The number of the part, from 1.
            body: The data of the part.
            **params: Additional request parameters. The fields that the
                other arguments set take precedence over parameters of the
                same name.

        Returns:
            The uploaded part.

        Raises:
            ValueError: If the upload has no bucket, key, or upload ID.
        """
        request: dict[str, Any] = {
            **self._multipart_upload_request(upload),
            "PartNumber": part_number,
            "Body": body,
        }
        if upload.checksum_algorithm is not None:
            request["ChecksumAlgorithm"] = upload.checksum_algorithm
        _logger.debug(f"Upload part of {upload.upload_id} as part {part_number}.")
        response = self.call(self._client.upload_part, **{**params, **request})
        return S3MultipartUploadPart(part_number, response)

    def upload_part_copy(
        self,
        upload: S3MultipartUpload,
        part_number: int,
        source: S3Path,
        range_: tuple[int, int] | None = None,
        **params,
    ) -> S3MultipartUploadPart:
        """Copy a part of a multipart upload from an object with UploadPartCopy.

        Args:
            upload: The multipart upload returned by creation.
            part_number: The number of the part, from 1.
            source: The path of the object to copy, with the version ID to
                copy, if any.
            range_: The ``(start, end)`` byte range of the source to copy,
                with an exclusive end. None sends no range of its own, so the
                whole source is copied unless ``params`` or ``request_kwargs``
                have ``CopySourceRange``.
            **params: Additional request parameters. The fields that the
                other arguments set take precedence over parameters of the
                same name.

        Returns:
            The copied part.

        Raises:
            ValueError: If the upload has no bucket, key, or upload ID, or the
                source has no key.
        """
        request = self._multipart_upload_request(upload)
        if not source.key:
            raise ValueError(f"The source has no key: {source.uri}.")
        copy_source: dict[str, Any] = {"Bucket": source.bucket, "Key": source.key}
        if source.version_id:
            copy_source.update({"VersionId": source.version_id})
        request.update(
            {
                "CopySource": copy_source,
                "PartNumber": part_number,
            }
        )
        if range_:
            request.update({"CopySourceRange": f"bytes={range_[0]}-{range_[1] - 1}"})
        _logger.debug(
            f"Copy part from {source.uri} to upload {upload.upload_id} as part {part_number}."
        )
        response = self.call(self._client.upload_part_copy, **{**params, **request})
        return S3MultipartUploadPart(part_number, response)

    def complete_multipart_upload(
        self,
        upload: S3MultipartUpload,
        parts: Sequence[S3MultipartUploadPart],
        **params,
    ) -> S3CompleteMultipartUpload:
        """Complete a multipart upload with CompleteMultipartUpload.

        Args:
            upload: The multipart upload returned by creation. Its checksum
                algorithm selects the matching part checksum, and its checksum
                type is sent when present. Without an algorithm, only the ETag
                and part number are sent, even if the SDK added a part checksum.
            parts: The uploaded parts, in part-number order.
            **params: Additional request parameters. The fields that the
                other arguments set take precedence over parameters of the
                same name.

        Returns:
            The completed upload.

        Raises:
            ValueError: If the upload has no bucket, key, or upload ID.
        """
        request = self._multipart_upload_request(upload)
        part_fields = {"ETag", "PartNumber"}
        if upload.checksum_algorithm is not None:
            part_fields.add(f"Checksum{upload.checksum_algorithm}")
        if upload.checksum_type is not None:
            request["ChecksumType"] = upload.checksum_type
        request["MultipartUpload"] = {
            "Parts": [
                {key: value for key, value in part.to_api_repr().items() if key in part_fields}
                for part in parts
            ]
        }
        _logger.debug(f"Complete multipart upload {upload.upload_id}.")
        response = self.call(self._client.complete_multipart_upload, **{**params, **request})
        return S3CompleteMultipartUpload(response)

    def abort_multipart_upload(self, upload: S3MultipartUpload, **params) -> None:
        """Abort a multipart upload with AbortMultipartUpload.

        Args:
            upload: The multipart upload returned by creation or listing.
            **params: Additional request parameters. The fields that the
                other arguments set take precedence over parameters of the
                same name.

        Raises:
            ValueError: If the upload has no bucket, key, or upload ID.
            FileNotFoundError: If the upload does not exist, for example
                because it was completed or aborted.
        """
        request = self._multipart_upload_request(upload)
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

    def check_multipart_upload_size(self, size: int, block_size: int) -> None:
        """Check that data fits in a multipart upload before it is uploaded.

        Sends no request.

        Args:
            size: The size of the data in bytes.
            block_size: The size in bytes of the parts that upload the data.

        Raises:
            ValueError: If the data takes more than
                ``MULTIPART_UPLOAD_MAX_PARTS`` blocks. The message gives the
                minimum block size, which is at least
                ``MULTIPART_UPLOAD_MIN_PART_SIZE``.
        """
        if size > block_size * self.MULTIPART_UPLOAD_MAX_PARTS:
            min_block_size = max(
                math.ceil(size / self.MULTIPART_UPLOAD_MAX_PARTS),
                self.MULTIPART_UPLOAD_MIN_PART_SIZE,
            )
            raise ValueError(
                f"Cannot upload {size} bytes in {self.MULTIPART_UPLOAD_MAX_PARTS} parts "
                f"with a block size of {block_size} bytes. "
                f"Use a block size of at least {min_block_size} bytes."
            )

    def copy_object(self, source: S3Path, destination: S3Path, **params) -> None:
        """Copy an object, or a version of it, with CopyObject.

        Args:
            source: The path of the object to copy, with the version ID to
                copy, if any.
            destination: The path of the object to write, without a version
                ID.
            **params: Additional request parameters, sent as given.

        Raises:
            ValueError: If the source or the destination has no key, or the
                destination has a version ID, which a write cannot replace.
        """
        if not source.key:
            raise ValueError(f"The source has no key: {source.uri}.")
        if not destination.key:
            raise ValueError(f"The path has no key: {destination.uri}.")
        if destination.version_id:
            raise ValueError(f"Cannot write to a version: {destination.uri}.")
        copy_source: dict[str, Any] = {"Bucket": source.bucket, "Key": source.key}
        if source.version_id:
            copy_source.update({"VersionId": source.version_id})
        request: dict[str, Any] = {
            "CopySource": copy_source,
            "Bucket": destination.bucket,
            "Key": destination.key,
        }
        _logger.debug(f"Copy object from {source.uri} to {destination.uri}.")
        self.call(self._client.copy_object, **request, **params)

    def plan_multipart_copy(
        self,
        source: S3Path,
        destination: S3Path,
        block_size: int | None = None,
        **params,
    ) -> S3MultipartCopyPlan:
        """Plan a copy with a multipart upload that copies as CopyObject does.

        The source is read with HeadObject. Without a version in the path, a
        version ID other than ``null`` that it reports is the version to
        copy, so that the parts, the tags and the annotations come from the
        same object even if the source is replaced during the copy. A
        ``null`` version, which a write can replace, is not pinned. If the
        reported size fits in a single CopyObject request, nothing else is
        read and the plan says so.

        No multipart request accepts the directives of CopyObject, so the
        plan applies them as CopyObject does. With the COPY metadata
        directive (the default), the content headers and the user-defined
        metadata of the source are used, and the values of ``params`` are
        ignored. With the COPY tagging directive (the default), the tags are
        read with GetObjectTagging, and the ``Tagging`` of ``params`` is
        ignored. REPLACE uses the values of ``params`` instead. With the COPY
        annotation directive (the default), the annotations are listed with
        ListObjectAnnotations, unless the source is encrypted with SSE-C or
        is in a directory bucket, which cannot have annotations; an object in
        a directory bucket has no tags either. The requests that read the
        source receive ``RequestPayer``, and the source's expected bucket
        owner and SSE-C parameters (``ExpectedSourceBucketOwner`` and
        ``CopySourceSSECustomer*``) under their names in those requests.

        Args:
            source: The path of the object to copy, with the version ID to
                copy, if any.
            destination: The path of the object to write, without a version
                ID.
            block_size: The size in bytes of the copied ranges, between
                ``MULTIPART_UPLOAD_MIN_PART_SIZE`` and
                ``MULTIPART_UPLOAD_MAX_PART_SIZE`` (the default); see
                :meth:`part_ranges`.
            **params: The CopyObject parameters of the copy. Each request
                receives those that it accepts; CreateMultipartUpload also
                receives those that CopyObject does not accept either, so
                that botocore rejects them as it would for CopyObject.

        Returns:
            The plan.

        Raises:
            ValueError: If the source or the destination has no key, the
                destination has a version ID, ``block_size`` is out of the
                part size limits, a directive has a value that CopyObject
                does not accept, or HeadObject reports no size.
        """
        if not source.key:
            raise ValueError(f"The source has no key: {source.uri}.")
        if not destination.key:
            raise ValueError(f"The path has no key: {destination.uri}.")
        if destination.version_id:
            raise ValueError(f"Cannot write to a version: {destination.uri}.")
        block_size = block_size if block_size else self.MULTIPART_UPLOAD_MAX_PART_SIZE
        if (
            block_size < self.MULTIPART_UPLOAD_MIN_PART_SIZE
            or block_size > self.MULTIPART_UPLOAD_MAX_PART_SIZE
        ):
            raise ValueError(
                "Block size must be between "
                f"5 MiB ({self.MULTIPART_UPLOAD_MIN_PART_SIZE} bytes) and "
                f"5 GiB ({self.MULTIPART_UPLOAD_MAX_PART_SIZE} bytes), "
                f"inclusive: {block_size}."
            )
        metadata_directive = params.get("MetadataDirective", "COPY")
        tagging_directive = params.get("TaggingDirective", "COPY")
        annotation_directive = params.get("AnnotationDirective", "COPY")
        if metadata_directive not in ("COPY", "REPLACE"):
            raise ValueError(f"Invalid MetadataDirective: {metadata_directive}.")
        if tagging_directive not in ("COPY", "REPLACE"):
            raise ValueError(f"Invalid TaggingDirective: {tagging_directive}.")
        if annotation_directive not in ("COPY", "EXCLUDE"):
            raise ValueError(f"Invalid AnnotationDirective: {annotation_directive}.")

        source_params = self._copy_source_params(params)
        _logger.debug(f"Head object to copy: {source.uri}")
        head = self.head_object(source, **self.operation_params("head_object", source_params))
        if head.content_length is None:
            raise ValueError(f"HeadObject reported no size for {source.uri}.")
        if not source.version_id and head.version_id and head.version_id != "null":
            source = source.with_version_id(head.version_id)
        if head.content_length <= self.MULTIPART_UPLOAD_MAX_PART_SIZE:
            # Copied with CopyObject instead, which applies the directives.
            return S3MultipartCopyPlan(
                source=source,
                destination=destination,
                size=head.content_length,
                fits_single_request=True,
            )

        request = dict(params)
        if metadata_directive == "COPY":
            for name in self._COPY_METADATA_PARAMS:
                request.pop(name, None)
            copied = {
                "CacheControl": head.cache_control,
                "ContentDisposition": head.content_disposition,
                "ContentEncoding": head.content_encoding,
                "ContentLanguage": head.content_language,
                "ContentType": head.content_type,
                "Expires": head.expires,
                "Metadata": head.user_metadata,
            }
            request.update({k: v for k, v in copied.items() if v is not None})
        if tagging_directive == "COPY":
            request.pop("Tagging", None)
            # Directory buckets do not support GetObjectTagging, and their
            # objects have no tags.
            if not self._is_directory_bucket(source.bucket):
                tags = self.get_object_tagging(
                    source, **self.operation_params("get_object_tagging", source_params)
                )
                if tags:
                    request.update({"Tagging": urlencode(tags)})
        copy_params = self.operation_params("copy_object", request)
        create_params = {
            **self.operation_params("create_multipart_upload", request),
            # A parameter that CopyObject does not accept either is sent as
            # is, so that botocore rejects it as it does for CopyObject.
            **{k: v for k, v in request.items() if k not in copy_params},
        }
        ranges = tuple(self.part_ranges(head.content_length, block_size))
        # The annotations are listed before the caller writes anything, so
        # that a missing permission fails first.
        annotations = (
            tuple(
                self.list_object_annotations(
                    source, **self.operation_params("list_object_annotations", source_params)
                )
            )
            if annotation_directive == "COPY"
            and "CopySourceSSECustomerAlgorithm" not in params
            and not self._is_directory_bucket(source.bucket)
            else ()
        )
        return S3MultipartCopyPlan(
            source=source,
            destination=destination,
            size=head.content_length,
            ranges=ranges,
            create_params=create_params,
            part_params=self.operation_params("upload_part_copy", params),
            complete_params=self.operation_params("complete_multipart_upload", params),
            abort_params=self.operation_params("abort_multipart_upload", params),
            annotations=annotations,
        )

    def list_object_annotations(self, path: S3Path, **params) -> list[str]:
        """List the names of the annotations of an object with ListObjectAnnotations.

        Sends one request per page.

        Args:
            path: The path of the object, with the version ID to list, if
                any.
            **params: Additional request parameters. The fields that the
                other arguments set take precedence over parameters of the
                same name.

        Returns:
            The annotation names, across all pages.

        Raises:
            ValueError: If the path has no key.
        """
        if not path.key:
            raise ValueError(f"The path has no key: {path.uri}.")
        request: dict[str, Any] = {**params, "Bucket": path.bucket, "Key": path.key}
        if path.version_id:
            request.update({"VersionId": path.version_id})
        names: list[str] = []
        while True:
            _logger.debug(f"List object annotations: {path.uri}")
            response = self.call(self._client.list_object_annotations, **request)
            names.extend(a["AnnotationName"] for a in response.get("Annotations", []))
            token = response.get("NextContinuationToken")
            if not token:
                return names
            request.update({"ContinuationToken": token})

    def copy_object_annotation(
        self,
        name: str,
        source: S3Path,
        destination: S3Path,
        version_id: str | None,
        etag: str | None,
        **params,
    ) -> None:
        """Copy an annotation of an object onto the object that a copy wrote.

        Reads the annotation with GetObjectAnnotation and writes it with
        PutObjectAnnotation. The annotation is written to the version that
        the copy created, if the bucket is versioned, and only if the
        destination still has the ETag of the copy, so that it is not
        attached to an object written over the copy.

        Args:
            name: The annotation name.
            source: The path of the copied object, with the version ID that
                was copied, if any.
            destination: The path of the object that the copy wrote, without
                a version ID.
            version_id: The version ID that the copy created, if any.
            etag: The ETag of the object that the copy wrote, if any.
            **params: The CopyObject parameters of the copy.
                GetObjectAnnotation receives those of the source, mapped as
                for :meth:`plan_multipart_copy`, and PutObjectAnnotation
                those that it accepts; the fields that the other arguments
                set take precedence.

        Raises:
            ValueError: If the source or the destination has no key.
        """
        if not source.key:
            raise ValueError(f"The source has no key: {source.uri}.")
        if not destination.key:
            raise ValueError(f"The path has no key: {destination.uri}.")
        get_request: dict[str, Any] = {
            "Bucket": source.bucket,
            "Key": source.key,
            "AnnotationName": name,
        }
        if source.version_id:
            get_request.update({"VersionId": source.version_id})
        _logger.debug(f"Copy object annotation {name} from {source.uri} to {destination.uri}.")
        response = self.call(
            self._client.get_object_annotation,
            **self.operation_params("get_object_annotation", self._copy_source_params(params)),
            **get_request,
        )
        put_request: dict[str, Any] = {
            "Bucket": destination.bucket,
            "Key": destination.key,
            "AnnotationName": name,
            "AnnotationPayload": response["AnnotationPayload"].read(),
        }
        if version_id:
            put_request.update({"VersionId": version_id})
        if etag:
            put_request.update({"ObjectIfMatch": etag})
        self.call(
            self._client.put_object_annotation,
            **{**self.operation_params("put_object_annotation", params), **put_request},
        )

    def get_object_tagging(self, path: S3Path, **params) -> dict[str, str]:
        """Get the tags of an object, or of a version of it, with GetObjectTagging.

        Args:
            path: The path of the object, with the version ID to read, if
                any, including ``null``.
            **params: Additional request parameters, sent as given.

        Returns:
            The tags, mapping each key to its value, in the order of the
            response.

        Raises:
            ValueError: If the path has no key.
            FileNotFoundError: If the object or version does not exist.
        """
        if not path.key:
            raise ValueError(f"The path has no key: {path.uri}.")
        request: dict[str, Any] = {"Bucket": path.bucket, "Key": path.key}
        if path.version_id:
            request.update({"VersionId": path.version_id})
        _logger.debug(f"Get object tagging: {path.uri}")
        response = self.call(self._client.get_object_tagging, **request, **params)
        return {t["Key"]: t["Value"] for t in response["TagSet"]}

    def put_object_tagging(self, path: S3Path, tags: Mapping[str, str], **params) -> None:
        """Replace the tags of an object, or of a version of it, with PutObjectTagging.

        Args:
            path: The path of the object, with the version ID to tag, if any,
                including ``null``.
            tags: The tags, mapping each key to its value. They replace all
                the existing tags.
            **params: Additional request parameters, sent as given.

        Raises:
            ValueError: If the path has no key.
            FileNotFoundError: If the object or version does not exist.
        """
        if not path.key:
            raise ValueError(f"The path has no key: {path.uri}.")
        request: dict[str, Any] = {
            "Bucket": path.bucket,
            "Key": path.key,
            "Tagging": {"TagSet": [{"Key": k, "Value": v} for k, v in tags.items()]},
        }
        if path.version_id:
            request.update({"VersionId": path.version_id})
        _logger.debug(f"Put object tagging: {path.uri}")
        self.call(self._client.put_object_tagging, **request, **params)

    def put_object_acl(self, path: S3Path, acl: str, **params) -> None:
        """Apply a canned ACL to an object, or to a version of it, with PutObjectAcl.

        Args:
            path: The path of the object, with the version ID to apply the
                ACL to, if any, including ``null``.
            acl: The canned ACL, one of ``OBJECT_ACLS``.
            **params: Additional request parameters, sent as given.

        Raises:
            ValueError: If the path has no key, or the ACL is not in
                ``OBJECT_ACLS``.
            FileNotFoundError: If the object or version does not exist.
        """
        if not path.key:
            raise ValueError(f"The path has no key: {path.uri}.")
        if acl not in self.OBJECT_ACLS:
            raise ValueError(f"ACL not in {self.OBJECT_ACLS}.")
        request: dict[str, Any] = {"Bucket": path.bucket, "Key": path.key, "ACL": acl}
        if path.version_id:
            request.update({"VersionId": path.version_id})
        _logger.debug(f"Put object acl: {path.uri}")
        self.call(self._client.put_object_acl, **request, **params)

    def put_bucket_acl(self, bucket: str, acl: str, **params) -> None:
        """Apply a canned ACL to a bucket with PutBucketAcl.

        Args:
            bucket: The name of the bucket.
            acl: The canned ACL, one of ``BUCKET_ACLS``.
            **params: Additional request parameters, sent as given.

        Raises:
            ValueError: If the ACL is not in ``BUCKET_ACLS``.
            FileNotFoundError: If the bucket does not exist.
        """
        if acl not in self.BUCKET_ACLS:
            raise ValueError(f"ACL not in {self.BUCKET_ACLS}.")
        _logger.debug(f"Put bucket acl: s3://{bucket}")
        self.call(self._client.put_bucket_acl, Bucket=bucket, ACL=acl, **params)

    def replace_object_metadata(
        self, path: S3Path, head: S3Metadata, metadata: Mapping[str, str], **params
    ) -> None:
        """Replace the user-defined metadata of an object by copying it onto itself.

        S3 does not update the metadata of an object in place, so the object
        is copied onto itself with CopyObject and the REPLACE metadata
        directive, which writes a new object, or a new version in a
        versioned bucket. With that directive, S3 does not copy what the
        request omits, so the request also sends the content headers,
        ``Expires``, ``WebsiteRedirectLocation`` and ``StorageClass`` of
        ``head``, and, unless ``params`` set an encryption parameter, its
        ``ServerSideEncryption``, ``SSEKMSKeyId`` and ``BucketKeyEnabled``.
        Fields that are None in ``head`` are not sent; note that
        :class:`~pyathena.filesystem.s3_object.S3Metadata` reports
        ``STANDARD`` when HeadObject omits the storage class. HeadObject does
        not return the KMS encryption context, so it is not retained.

        Args:
            path: The path of the object, without a version ID.
            head: The HeadObject result of ``path`` (see :meth:`head_object`),
                whose fields are retained.
            metadata: The user-defined metadata of the copy, which replaces
                all the existing user-defined metadata.
            **params: Additional CopyObject parameters. They take precedence
                over the retained fields; one that the copy itself sets,
                such as ``Metadata``, ``MetadataDirective`` or ``Key``,
                raises ``TypeError``.

        Raises:
            ValueError: If the path has no key or has a version ID, which a
                write cannot replace.
        """
        if not path.key:
            raise ValueError(f"The path has no key: {path.uri}.")
        if path.version_id:
            raise ValueError(f"Cannot write to a version: {path.uri}.")
        _logger.debug(f"Replace object metadata: {path.uri}")
        retained: dict[str, Any] = {
            "CacheControl": head.cache_control,
            "ContentDisposition": head.content_disposition,
            "ContentEncoding": head.content_encoding,
            "ContentLanguage": head.content_language,
            "ContentType": head.content_type,
            "Expires": head.expires,
            "WebsiteRedirectLocation": head.website_redirect_location,
            "StorageClass": head.storage_class,
        }
        if not self._SSE_COPY_PARAMS.intersection(params):
            retained.update(
                {
                    "ServerSideEncryption": head.server_side_encryption,
                    "SSEKMSKeyId": head.sse_kms_key_id,
                    "BucketKeyEnabled": head.bucket_key_enabled,
                }
            )
        self.copy_object(
            path,
            path,
            # botocore accepts only a dict, not another mapping such as an
            # S3Metadata.
            Metadata=dict(metadata),
            MetadataDirective="REPLACE",
            **{
                **{k: v for k, v in retained.items() if v is not None},
                **params,
            },
        )

    def generate_presigned_url(
        self,
        path: S3Path,
        client_method: str = "get_object",
        expires_in: int = 3600,
        **params,
    ) -> str:
        """Generate a presigned URL for a request on an object.

        The URL is signed locally with the client's credentials; no request
        is sent. ``request_kwargs`` are not added to the signed parameters.

        Args:
            path: The path of the object, with the version ID to sign for,
                if any. A path without a key is not rejected; botocore
                validates the parameters of the method.
            client_method: The name of the client method to sign, such as
                ``get_object`` or ``put_object``.
            expires_in: The number of seconds for which the URL is valid.
            **params: Parameters of the method to sign. They take precedence
                over the bucket, key and version ID of the path.

        Returns:
            The presigned URL.
        """
        request: dict[str, Any] = {"Bucket": path.bucket, "Key": path.key}
        if path.version_id:
            request.update({"VersionId": path.version_id})
        _logger.debug(f"Generate signed url: {path.uri}")
        return cast(
            str,
            self.call(
                self._client.generate_presigned_url,
                ClientMethod=client_method,
                Params={**request, **params},
                ExpiresIn=expires_in,
            ),
        )

    @staticmethod
    def _is_directory_bucket(bucket: str) -> bool:
        """Return whether the bucket is a directory bucket (S3 Express One Zone).

        Directory bucket names end with ``--x-s3``.

        Args:
            bucket: S3 bucket name.

        Returns:
            True if the bucket is a directory bucket.
        """
        return bucket.endswith("--x-s3")

    @staticmethod
    def _copy_source_params(params: Mapping[str, Any]) -> dict[str, Any]:
        """Map the parameters of a copy to those of the requests that read its source.

        Args:
            params: The CopyObject parameters of the copy.

        Returns:
            ``RequestPayer``, and the source's expected bucket owner and SSE-C
            parameters under the names of the requests that read the source
            (``ExpectedBucketOwner`` and ``SSECustomer*``), where given.
        """
        source_params = {
            "RequestPayer": params.get("RequestPayer"),
            "ExpectedBucketOwner": params.get("ExpectedSourceBucketOwner"),
            "SSECustomerAlgorithm": params.get("CopySourceSSECustomerAlgorithm"),
            "SSECustomerKey": params.get("CopySourceSSECustomerKey"),
            "SSECustomerKeyMD5": params.get("CopySourceSSECustomerKeyMD5"),
        }
        return {k: v for k, v in source_params.items() if v is not None}

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

    def list_multipart_uploads_page(
        self,
        bucket: str,
        prefix: str | None = None,
        key_marker: str | None = None,
        upload_id_marker: str | None = None,
        **params,
    ) -> S3ListMultipartUploadsPage:
        """List one page of in-progress multipart uploads with ListMultipartUploads.

        Args:
            bucket: The bucket to list.
            prefix: The key prefix to list, which S3 matches as a plain string
                prefix of the keys. None sends no prefix.
            key_marker: The key marker of the page to list.
            upload_id_marker: The upload ID marker of the page to list. S3
                accepts it only with a key marker.
            **params: Additional request parameters, sent as given.

        Returns:
            The page.

        Raises:
            FileNotFoundError: If the bucket does not exist.
        """
        request: dict[str, Any] = {"Bucket": bucket}
        if prefix is not None:
            request.update({"Prefix": prefix})
        if key_marker is not None:
            request.update({"KeyMarker": key_marker})
        if upload_id_marker is not None:
            request.update({"UploadIdMarker": upload_id_marker})
        response = self.call(self._client.list_multipart_uploads, **request, **params)
        return S3ListMultipartUploadsPage.from_response(bucket, response)

    def list_multipart_uploads(
        self,
        bucket: str,
        prefix: str | None = None,
        key_marker: str | None = None,
        upload_id_marker: str | None = None,
        **params,
    ) -> Iterator[S3ListMultipartUploadsPage]:
        """List the pages of in-progress multipart uploads, one request each.

        Args:
            bucket: The bucket to list.
            prefix: The key prefix to list, which S3 matches as a plain string
                prefix of the keys. None sends no prefix.
            key_marker: The key marker of the first page to list.
            upload_id_marker: The upload ID marker of the first page to list,
                sent with ``key_marker``.
            **params: Additional request parameters, sent as given.

        Yields:
            The pages, until one is not truncated or lacks either next marker.

        Raises:
            TypeError: If ``params`` has ``KeyMarker`` or ``UploadIdMarker``,
                which the iterator advances; pass ``key_marker`` and
                ``upload_id_marker`` instead.
            FileNotFoundError: If the bucket does not exist.
        """
        if "KeyMarker" in params or "UploadIdMarker" in params:
            raise TypeError("Pass the first page's markers as key_marker and upload_id_marker.")
        while True:
            page = self.list_multipart_uploads_page(
                bucket,
                prefix=prefix,
                key_marker=key_marker,
                upload_id_marker=upload_id_marker,
                **params,
            )
            yield page
            if not page.is_truncated or not page.next_key_marker or not page.next_upload_id_marker:
                return
            key_marker = page.next_key_marker
            upload_id_marker = page.next_upload_id_marker

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
