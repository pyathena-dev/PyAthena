# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""The typed S3 operations under the S3 filesystem."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

import botocore.exceptions
from botocore.client import BaseClient

from pyathena.filesystem.s3_errors import S3ClientError
from pyathena.filesystem.s3_object import S3Metadata, S3ObjectVersion
from pyathena.filesystem.s3_path import S3Path
from pyathena.util import RetryConfig, retry_api_call

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


class S3Core:
    """Typed S3 operations, one request each, on a boto3 S3 client.

    Each operation sends one request, or one per page for the iterators,
    with the retry policy, and translates S3 errors into ``OSError``
    subclasses (see :class:`~pyathena.filesystem.s3_errors.S3ClientError`):
    a missing object, version or bucket raises ``FileNotFoundError``, and a
    denied request ``PermissionError``. Nothing is cached.

    Example:
        >>> core = S3Core(boto3.client("s3"))
        >>> core.head_object(S3Path.parse("s3://bucket/key")).content_length
        >>> for page in core.list_objects("bucket", prefix="dir/", delimiter="/"):
        ...     print([o.key for o in page.objects])
    """

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
            delimiter: The delimiter to group keys by, or None to list
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
            delimiter: The delimiter to group keys by, or None to list
                recursively.
            max_keys: The maximum number of keys per page.
            continuation_token: The token of the first page to list.
            **params: Additional request parameters, sent as given.

        Yields:
            The pages, until one has no continuation token.
        """
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
            delimiter: The delimiter to group keys by, or None to list
                recursively.
            key_marker: The key marker of the page to list.
            version_id_marker: The version ID marker of the page to list,
                sent with ``key_marker`` unless it is None.
            **params: Additional request parameters, sent as given.

        Returns:
            The page.
        """
        request: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if delimiter:
            request.update({"Delimiter": delimiter})
        if key_marker:
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
            delimiter: The delimiter to group keys by, or None to list
                recursively.
            key_marker: The key marker of the first page to list.
            version_id_marker: The version ID marker of the first page to
                list, sent with ``key_marker``.
            **params: Additional request parameters, sent as given.

        Yields:
            The pages, until one is not truncated or has no key marker.
        """
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
        """
        while True:
            page = self.list_buckets_page(continuation_token=continuation_token, **params)
            yield page
            continuation_token = page.continuation_token
            if not continuation_token:
                return
