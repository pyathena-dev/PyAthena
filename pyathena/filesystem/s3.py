"""fsspec filesystem and file implementations for Amazon S3."""

from __future__ import annotations

import contextlib
import logging
import math
import mimetypes
import os.path
import re
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import Future, as_completed, wait
from copy import deepcopy
from datetime import datetime
from io import BytesIO
from multiprocessing import cpu_count
from re import Pattern
from typing import Any, cast
from urllib.parse import unquote_plus

import botocore.exceptions
from boto3 import Session
from botocore import UNSIGNED
from botocore.client import BaseClient, Config
from fsspec import AbstractFileSystem
from fsspec.callbacks import _DEFAULT_CALLBACK
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import isfilelike, tokenize

import pyathena
from pyathena.connection import Connection
from pyathena.filesystem.s3_errors import S3ClientError
from pyathena.filesystem.s3_executor import S3Executor, S3ThreadPoolExecutor
from pyathena.filesystem.s3_object import (
    S3CompleteMultipartUpload,
    S3Metadata,
    S3MultipartUpload,
    S3MultipartUploadPart,
    S3Object,
    S3ObjectType,
    S3ObjectVersion,
    S3PutObject,
    S3StorageClass,
)
from pyathena.util import RetryConfig, retry_api_call

_logger = logging.getLogger(__name__)


class S3FileSystem(AbstractFileSystem):
    """A filesystem interface for Amazon S3 that implements the fsspec protocol.

    This class provides a file-system like interface to Amazon S3, allowing you to
    use familiar file operations (ls, open, cp, rm, etc.) with S3 objects. It's
    designed to be compatible with s3fs while offering PyAthena-specific optimizations.

    The filesystem supports standard S3 operations including:

    - Listing objects and directories
    - Reading and writing files
    - Copying and moving objects
    - Reading and writing object metadata, tags, and canned ACLs
    - Multipart uploads for large files, including management of
      incomplete uploads
    - Version-aware reads and object version listing (see ``version_aware``)
    - Creating and removing buckets (disabled by default; see
      ``allow_bucket_creation`` / ``allow_bucket_deletion``)
    - Various S3 storage classes and encryption options
    - Translating S3 error responses into standard Python exceptions
      (e.g., ``404`` -> ``FileNotFoundError``, ``403`` -> ``PermissionError``)

    Attributes:
        allow_bucket_creation: Whether mkdir/makedirs may create buckets.
            Defaults to False.
        allow_bucket_deletion: Whether rmdir may delete buckets.
            Defaults to False.
        version_aware: Whether reads pin the object version observed at
            open time and ls may list all versions. Requires the
            s3:GetObjectVersion / s3:ListBucketVersions permissions.
            Defaults to False.

    Example:
        >>> from pyathena.filesystem.s3 import S3FileSystem
        >>> fs = S3FileSystem()
        >>>
        >>> # List objects in a bucket
        >>> files = fs.ls('s3://my-bucket/data/')
        >>>
        >>> # Read a file
        >>> with fs.open('s3://my-bucket/data/file.csv', 'r') as f:
        ...     content = f.read()
        >>>
        >>> # Write a file
        >>> with fs.open('s3://my-bucket/output/result.txt', 'w') as f:
        ...     f.write('Hello, S3!')
        >>>
        >>> # Copy files
        >>> fs.cp('s3://source-bucket/file.txt', 's3://dest-bucket/file.txt')

    Note:
        This filesystem is used internally by PyAthena for handling query results
        stored in S3, but can also be used independently for S3 file operations.
    """

    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
    # The minimum size of a part in a multipart upload is 5MiB.
    MULTIPART_UPLOAD_MIN_PART_SIZE: int = 5 * 2**20  # 5MiB
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
    # The maximum size of a part in a multipart upload is 5GiB.
    MULTIPART_UPLOAD_MAX_PART_SIZE: int = 5 * 2**30  # 5GiB
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
    # The maximum number of parts per multipart upload is 10,000.
    MULTIPART_UPLOAD_MAX_PARTS: int = 10_000
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
    DELETE_OBJECTS_MAX_KEYS: int = 1000
    DEFAULT_BLOCK_SIZE: int = 5 * 2**20  # 5MiB
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl
    OBJECT_ACLS: frozenset[str] = frozenset(
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
    BUCKET_ACLS: frozenset[str] = frozenset(
        {"private", "public-read", "public-read-write", "authenticated-read"}
    )
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html
    # The CopyObject parameters that set the encryption of the copy.
    _SSE_COPY_PARAMS: frozenset[str] = frozenset(
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
    PATTERN_PATH: Pattern[str] = re.compile(
        r"(^s3://|^s3a://|^)(?P<bucket>[a-zA-Z0-9.\-_]+)(/(?P<key>[^?]+)|/)?"
        r"($|\?version(Id|ID|id|_id)=(?P<version_id>.+)$)"
    )

    protocol = ("s3", "s3a")
    _extra_tokenize_attributes = ("default_block_size",)

    def __init__(
        self,
        connection: Connection[Any] | None = None,
        default_block_size: int | None = None,
        default_cache_type: str | None = None,
        max_workers: int = (cpu_count() or 1) * 5,
        s3_additional_kwargs=None,
        allow_bucket_creation: bool = False,
        allow_bucket_deletion: bool = False,
        version_aware: bool = False,
        *args,
        **kwargs,
    ) -> None:
        """Create a filesystem for Amazon S3.

        Args:
            connection: A PyAthena connection whose session, region, config and
                retry policy the S3 client uses. Without one, the client is built
                from s3fs-compatible arguments in ``kwargs``.
            default_block_size: The block size for reads and writes; defaults to
                ``DEFAULT_BLOCK_SIZE``.
            default_cache_type: The fsspec cache type for reads; defaults to
                ``"bytes"``.
            max_workers: The number of threads for parallel transfers.
            s3_additional_kwargs: Extra arguments for the object requests of
                ``open()`` and ``pipe_file()``; listings and other requests do
                not use them. Each request receives those that its operation
                accepts, and the parameters of a call take precedence.
            allow_bucket_creation: Whether ``mkdir``/``makedirs`` may create a
                bucket.
            allow_bucket_deletion: Whether ``rmdir`` may delete a bucket.
            version_aware: Whether reads pin the object version observed at open
                time.
            *args: Passed to ``fsspec.AbstractFileSystem``.
            **kwargs: Passed to ``fsspec.AbstractFileSystem``; without a
                ``connection``, also s3fs-compatible client arguments.
                ``requester_pays=True`` sends requester-pays requests with
                the operations that accept ``RequestPayer``.
        """
        super().__init__(*args, **kwargs)
        if connection:
            self._client = connection.session.client(
                "s3",
                region_name=connection.region_name,
                config=connection.config,
                **connection._client_kwargs,
            )
            self._retry_config = connection.retry_config
        else:
            self._client = self._get_client_compatible_with_s3fs(**kwargs)
            self._retry_config = RetryConfig()
        self.default_block_size = (
            default_block_size if default_block_size else self.DEFAULT_BLOCK_SIZE
        )
        self.default_cache_type = default_cache_type if default_cache_type else "bytes"
        self.max_workers = max_workers
        self.s3_additional_kwargs = s3_additional_kwargs if s3_additional_kwargs else {}
        self.allow_bucket_creation = allow_bucket_creation
        self.allow_bucket_deletion = allow_bucket_deletion
        self.version_aware = version_aware

        requester_pays = kwargs.pop("requester_pays", False)
        self.request_kwargs: dict[str, Any] = (
            {"RequestPayer": "requester"} if requester_pays else {}
        )

    def _get_client_compatible_with_s3fs(self, **kwargs) -> BaseClient:
        """Build a boto3 S3 client from s3fs-compatible constructor arguments.

        Accepts the constructor arguments that s3fs users pass through fsspec
        storage options — ``key``/``username``, ``secret``/``password``,
        ``token``, ``profile``, ``anon``, ``use_ssl``, ``endpoint_url``,
        ``connect_timeout``/``read_timeout``, and the ``client_kwargs`` /
        ``config_kwargs`` dictionaries — in addition to boto3 session
        arguments such as ``region_name`` and ``profile_name``. ``profile``
        is used as ``profile_name`` when ``profile_name`` is not given.

        Args:
            **kwargs: The filesystem constructor arguments.

        Returns:
            A boto3 S3 client configured from the arguments.
        """
        config_kwargs = deepcopy(kwargs.pop("config_kwargs", {}))
        client_kwargs = deepcopy(kwargs.pop("client_kwargs", {}))

        user_agent_extra = config_kwargs.pop("user_agent_extra", None)
        if user_agent_extra and pyathena.user_agent_extra not in user_agent_extra:
            user_agent_extra = f"{pyathena.user_agent_extra} {user_agent_extra}"
        config_kwargs.update({"user_agent_extra": user_agent_extra or pyathena.user_agent_extra})
        if connect_timeout := kwargs.pop("connect_timeout", None):
            config_kwargs.update({"connect_timeout": connect_timeout})
        if read_timeout := kwargs.pop("read_timeout", None):
            config_kwargs.update({"read_timeout": read_timeout})

        use_ssl = kwargs.pop("use_ssl", None)
        if use_ssl is not None:
            client_kwargs.update({"use_ssl": use_ssl})
        if endpoint_url := kwargs.pop("endpoint_url", None):
            client_kwargs.update({"endpoint_url": endpoint_url})
        if kwargs.pop("anon", False):
            config_kwargs.update({"signature_version": UNSIGNED})
        else:
            creds = {
                key: value
                for key, value in {
                    "aws_access_key_id": kwargs.pop("key", kwargs.pop("username", None)),
                    "aws_secret_access_key": kwargs.pop("secret", kwargs.pop("password", None)),
                    "aws_session_token": kwargs.pop("token", None),
                }.items()
                if value is not None
            }
            kwargs.update(creds)
            client_kwargs.update(creds)
        if profile := kwargs.pop("profile", None):
            kwargs.setdefault("profile_name", profile)

        session = Session(
            **{k: v for k, v in kwargs.items() if k in Connection._SESSION_PASSING_ARGS}
        )
        return session.client(
            "s3",
            config=Config(**config_kwargs),
            **{k: v for k, v in client_kwargs.items() if k in Connection._CLIENT_PASSING_ARGS},
        )

    @staticmethod
    def parse_path(path: str) -> tuple[str, str | None, str | None]:
        """Parse an S3 path into its bucket, key and version ID.

        The path may have an ``s3://`` or ``s3a://`` scheme and a version ID
        query (``?versionId=``, ``?versionID=``, ``?versionid=`` or
        ``?version_id=``).

        Args:
            path: The S3 path (e.g., "s3://bucket/key?versionId=...").

        Returns:
            Tuple of the bucket, the key (None for a bucket path) and the
            version ID (None if the path has none).

        Raises:
            ValueError: If the path is not a valid S3 path.
        """
        match = S3FileSystem.PATTERN_PATH.search(path)
        if match:
            return match.group("bucket"), match.group("key"), match.group("version_id")
        raise ValueError(f"Invalid S3 path format {path}.")

    @staticmethod
    def _directory_object(bucket: str, key: str | None, version_id: str | None = None) -> S3Object:
        """Build an S3Object representing a directory entry."""
        return S3Object(
            init={
                "ContentLength": 0,
                "ContentType": None,
                "StorageClass": S3StorageClass.S3_STORAGE_CLASS_DIRECTORY,
                "ETag": None,
                "LastModified": None,
            },
            type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
            bucket=bucket,
            key=key,
            version_id=version_id,
        )

    @staticmethod
    def _versioned_file_object(bucket: str, version: dict[str, Any]) -> S3Object:
        """Build an S3Object from a ListObjectVersions Versions entry."""
        return S3Object(
            init=version,
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket=bucket,
            key=version["Key"],
            version_id=version.get("VersionId"),
            is_latest=version.get("IsLatest", False),
        )

    def _head_bucket(self, bucket, refresh: bool = False) -> S3Object | None:
        """Get the bucket as a directory object with HeadBucket.

        The result is cached under the bucket name. A missing bucket evicts
        its entry and the cached bucket listing that still lists it.

        Args:
            bucket: The bucket name.
            refresh: If True, bypass the cache and call HeadBucket.

        Returns:
            The bucket object, or None if the bucket does not exist.
        """
        file = None if refresh else self.dircache.get(bucket)
        if file is None:
            try:
                self._call(
                    self._client.head_bucket,
                    Bucket=bucket,
                )
            except FileNotFoundError:
                self._evict_cache(bucket)
                # Evict the cached bucket listing only if it still lists the bucket.
                buckets = self.dircache.get("")
                if buckets and any(b.name == bucket for b in buckets):
                    self._evict_cache("")
                return None
            file = S3Object(
                init={
                    "ContentLength": 0,
                    "ContentType": None,
                    "StorageClass": S3StorageClass.S3_STORAGE_CLASS_BUCKET,
                    "ETag": None,
                    "LastModified": None,
                },
                type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                bucket=bucket,
                key=None,
                version_id=None,
            )
            self.dircache[bucket] = file
        return file

    def _head_object(
        self, path: str, version_id: str | None = None, refresh: bool = False
    ) -> S3Object | None:
        """Get the object with HeadObject.

        The result is cached under the path, or under the version-qualified
        path for an explicit version. An explicitly requested ``"null"``
        version is not cached. A missing object evicts its entry.

        Args:
            path: The object path, optionally with a versionId query.
            version_id: The version to get when the path has no version.
            refresh: If True, bypass the cache and call HeadObject.

        Returns:
            The object, or None if it does not exist.
        """
        bucket, key, path_version_id = self.parse_path(path)
        version_id = path_version_id if path_version_id else version_id
        if version_id and not path_version_id:
            # Cache an explicit version under its version-qualified path so
            # that it neither reuses nor replaces the entry of another version.
            path = f"{path}?versionId={version_id}"
        # Writes invalidate only the path without the version, and an
        # overwrite replaces the "null" version of a bucket without
        # versioning, so that version is looked up every time.
        cacheable = version_id != "null"
        file = None if refresh else self.dircache.get(path)
        if file is None:
            try:
                request = {
                    "Bucket": bucket,
                    "Key": key,
                }
                if version_id:
                    request.update({"VersionId": version_id})
                response = self._call(
                    self._client.head_object,
                    **request,
                )
            except FileNotFoundError:
                self._evict_cache(path)
                return None
            if self.version_aware and not version_id:
                # Pin the version of the object so that subsequent reads see
                # the version observed here even if the object is overwritten.
                version_id = response.get("VersionId")
            file = S3Object(
                init=response,
                type=S3ObjectType.S3_OBJECT_TYPE_FILE,
                bucket=bucket,
                key=key,
                version_id=version_id,
            )
            if cacheable:
                self.dircache[path] = file
        return file

    def _ls_buckets(self, refresh: bool = False) -> list[S3Object]:
        """List the buckets with ListBuckets.

        The listing is cached under ``""``.

        Args:
            refresh: If True, bypass the cache and call ListBuckets.

        Returns:
            The buckets as directory objects.
        """
        buckets = None if refresh else self.dircache.get("")
        if buckets is None:
            response = self._call(
                self._client.list_buckets,
            )
            buckets = [
                S3Object(
                    init={
                        "ContentLength": 0,
                        "ContentType": None,
                        "StorageClass": S3StorageClass.S3_STORAGE_CLASS_BUCKET,
                        "ETag": None,
                        "LastModified": None,
                    },
                    type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                    bucket=b["Name"],
                    key=None,
                    version_id=None,
                )
                for b in response["Buckets"]
            ]
            self.dircache[""] = buckets
        return buckets

    def _ls_dirs(
        self,
        path: str,
        prefix: str = "",
        delimiter: str = "/",
        next_token: str | None = None,
        max_keys: int | None = None,
        refresh: bool = False,
    ) -> list[S3Object]:
        """List the objects and common prefixes under a path.

        A complete, non-empty listing of the path is cached under
        ``(path, delimiter)``, and an empty one evicts it.
        ``invalidate_cache`` drops it when the path or a path under it is
        invalidated.

        Args:
            path: The bucket or directory path to list.
            prefix: Key prefix to filter by, relative to the path. A prefixed
                listing is neither read from nor written to the cache.
            delimiter: Delimiter to group keys by; ``""`` lists recursively.
            next_token: Continuation token to start listing from. A listing
                that starts from a token is neither read from nor written to
                the cache.
            max_keys: Maximum number of keys per ListObjectsV2 request.
            refresh: If True, bypass the cache and list from S3.

        Returns:
            The listed directories and files.
        """
        bucket, key, version_id = self.parse_path(path)
        use_cache = not prefix and not next_token
        if key:
            prefix = f"{key}/{prefix if prefix else ''}"

        cache_key = (path, delimiter)
        cached = self.dircache.get(cache_key) if use_cache and not refresh else None
        if cached is not None:
            return cast(list[S3Object], cached)

        files: list[S3Object] = []
        while True:
            request: dict[Any, Any] = {
                "Bucket": bucket,
                "Prefix": prefix,
                "Delimiter": delimiter,
            }
            if next_token:
                request.update({"ContinuationToken": next_token})
            if max_keys:
                request.update({"MaxKeys": max_keys})
            response = self._call(
                self._client.list_objects_v2,
                **request,
            )
            files.extend(
                self._directory_object(bucket, c["Prefix"][:-1].rstrip("/"), version_id)
                for c in response.get("CommonPrefixes", [])
            )
            files.extend(
                S3Object(
                    init=c,
                    type=S3ObjectType.S3_OBJECT_TYPE_FILE,
                    bucket=bucket,
                    key=c["Key"],
                )
                for c in response.get("Contents", [])
            )
            next_token = response.get("NextContinuationToken")
            if not next_token:
                break
        if use_cache:
            if files:
                self.dircache[cache_key] = files
            else:
                self._evict_cache(cache_key)
        return files

    def ls(
        self, path: str, detail: bool = False, refresh: bool = False, **kwargs
    ) -> list[S3Object] | list[str]:
        """List contents of an S3 path.

        Lists buckets (when path is root) or objects within a bucket/prefix.
        Compatible with fsspec interface for filesystem operations.

        Args:
            path: S3 path to list (e.g., "s3://bucket" or "s3://bucket/prefix").
            detail: If True, return S3Object instances; if False, return paths as strings.
            refresh: If True, bypass cache and fetch fresh results from S3.
            **kwargs: Additional arguments including:
                versions: If True, list all versions of the objects. Requires
                    the filesystem to be constructed with ``version_aware=True``.

        Returns:
            List of S3Object instances (if detail=True) or paths as strings (if detail=False).

        Example:
            >>> fs = S3FileSystem()
            >>> fs.ls("s3://my-bucket")  # List objects in bucket
            >>> fs.ls("s3://my-bucket/", detail=True)  # Get detailed object info
        """
        versions = kwargs.pop("versions", False)
        if versions and not self.version_aware:
            raise ValueError(
                "Cannot list the object versions unless the filesystem is version aware."
            )
        path = self._strip_protocol(path).rstrip("/")
        if path in ["", "/"]:
            files = self._ls_buckets(refresh)
        elif versions:
            files = self._ls_object_versions(path)
        else:
            files = self._ls_dirs(path, refresh=refresh)
            if not files and "/" in path:
                file = self._head_object(path, refresh=refresh)
                if file:
                    files = [file]
        return list(files) if detail else [f.name for f in files]

    def _ls_object_versions(self, path: str) -> list[S3Object]:
        """List a prefix including all versions of the objects.

        The listing is always fetched from S3 and is not cached, because the
        dircache stores the current view of a path.
        """
        bucket, key, _ = self.parse_path(path)
        prefix = f"{key}/" if key else ""

        files: list[S3Object] = []
        for response in self._list_object_versions_pages(bucket, prefix=prefix, delimiter="/"):
            files.extend(
                self._directory_object(bucket, c["Prefix"][:-1].rstrip("/"))
                for c in response.get("CommonPrefixes", [])
            )
            files.extend(
                self._versioned_file_object(bucket, v) for v in response.get("Versions", [])
            )

        if not files and key:
            # The path may point at an object rather than a key prefix.
            files = [
                self._versioned_file_object(bucket, v)
                for response in self._list_object_versions_pages(bucket, prefix=key, delimiter="/")
                for v in response.get("Versions", [])
                if v["Key"] == key
            ]
        return files

    def _list_object_versions_pages(
        self, bucket: str, prefix: str, delimiter: str | None = None, **kwargs
    ) -> Iterator[dict[str, Any]]:
        """Iterate over the pages of a ListObjectVersions request."""
        next_key_marker: str | None = None
        next_version_id_marker: str | None = None
        while True:
            request: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
            if delimiter:
                request.update({"Delimiter": delimiter})
            if next_key_marker:
                request.update(
                    {
                        "KeyMarker": next_key_marker,
                        "VersionIdMarker": next_version_id_marker,
                    }
                )
            response = self._call(
                self._client.list_object_versions,
                **request,
                **kwargs,
            )
            yield response
            if not response.get("IsTruncated"):
                break
            next_key_marker = response.get("NextKeyMarker")
            next_version_id_marker = response.get("NextVersionIdMarker", "")
            if not next_key_marker:
                break

    def info(self, path: str, **kwargs) -> S3Object:
        """Return information about an S3 path.

        Uses the directory cache first: a cached entry for the path is
        returned, a cached listing of the path itself makes it a directory,
        and a cached listing of its parent without it means it does not exist.
        Otherwise, a key path is looked up with HeadObject and, if no object
        exists, with a ListObjectsV2 request (``Delimiter="/"``,
        ``MaxKeys=1``) that checks whether it is a key prefix; a bucket path
        is looked up with HeadBucket. With ``version_aware``, a cached file
        entry without a version ID is looked up again. With an explicit
        version, the cached entries of the path are skipped, and the
        HeadObject result is cached under the version-qualified path apart
        from other versions, except for the ``null`` version, which an
        overwrite replaces.

        Args:
            path: S3 path (e.g., "s3://bucket" or "s3://bucket/key").
            **kwargs: Additional arguments including:
                refresh: If True, bypass the cache and query S3.
                version_id: The version ID to look up when the path has none.

        Returns:
            S3Object describing the bucket, directory, or file.

        Raises:
            FileNotFoundError: If the path does not exist.
        """
        refresh = kwargs.pop("refresh", False)
        path = self._strip_protocol(path)
        bucket, key, path_version_id = self.parse_path(path)
        version_id = path_version_id if path_version_id else kwargs.pop("version_id", None)
        if path in ["/", ""]:
            return S3Object(
                init={
                    "ContentLength": 0,
                    "ContentType": None,
                    "StorageClass": S3StorageClass.S3_STORAGE_CLASS_BUCKET,
                    "ETag": None,
                    "LastModified": None,
                },
                type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                bucket=bucket,
                key=None,
                version_id=None,
            )
        # Cached entries describe the current version of a path, so an
        # explicit version uses only the HeadObject cache of that version.
        if not refresh and not version_id:
            caches: list[S3Object] | S3Object | None = self._ls_from_cache(path)
            if caches is not None:
                if isinstance(caches, list):
                    cache = next((c for c in caches if c.name == path), None)
                elif caches.name == path:
                    cache = caches
                else:
                    cache = None

                if cache:
                    if (
                        self.version_aware
                        and cache.get("type") == S3ObjectType.S3_OBJECT_TYPE_FILE
                        and not cache.get("version_id")
                    ):
                        # A version-aware lookup needs the version to pin;
                        # treat a version-less cached entry (e.g., populated
                        # by a listing) as stale and head the object again.
                        refresh = True
                    else:
                        return cache
                else:
                    return self._directory_object(
                        bucket, key.rstrip("/") if key else None, version_id
                    )
        if key:
            object_info = self._head_object(path, refresh=refresh, version_id=version_id)
            if object_info:
                return object_info
        else:
            bucket_info = self._head_bucket(path, refresh=refresh)
            if bucket_info:
                return bucket_info
            raise FileNotFoundError(path)

        response = self._call(
            self._client.list_objects_v2,
            Bucket=bucket,
            Prefix=f"{key.rstrip('/')}/" if key else "",
            Delimiter="/",
            MaxKeys=1,
        )
        if (
            response.get("KeyCount", 0) > 0
            or response.get("Contents", [])
            or response.get("CommonPrefixes", [])
        ):
            return self._directory_object(bucket, key.rstrip("/") if key else None, version_id)
        raise FileNotFoundError(path)

    def _extract_parent_directories(
        self, files: list[S3Object], bucket: str, base_key: str | None
    ) -> list[S3Object]:
        """Extract parent directory objects from file paths.

        When listing files without delimiter, S3 doesn't return directory entries.
        This method creates directory objects by analyzing file paths.

        Args:
            files: List of S3Object instances representing files.
            bucket: S3 bucket name.
            base_key: Base key path to calculate relative paths from.

        Returns:
            List of S3Object instances representing directories.
        """
        dirs = set()
        base_key = base_key.rstrip("/") if base_key else ""

        for f in files:
            if f.key and f.type == S3ObjectType.S3_OBJECT_TYPE_FILE:
                # Extract directory paths from file paths
                f_key = f.key
                if base_key and f_key.startswith(base_key + "/"):
                    relative_path = f_key[len(base_key) + 1 :]
                elif not base_key:
                    relative_path = f_key
                else:
                    continue

                # Get all parent directories
                parts = relative_path.split("/")
                for i in range(1, len(parts)):
                    if base_key:
                        dir_path = base_key + "/" + "/".join(parts[:i])
                    else:
                        dir_path = "/".join(parts[:i])
                    dirs.add(dir_path)

        return [self._directory_object(bucket, dir_path) for dir_path in dirs]

    def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool | None = None,
        **kwargs,
    ) -> list[S3Object]:
        """List the objects below a path, as described in ``find``.

        Args:
            path: S3 path to search under.
            maxdepth: Maximum number of levels to descend, at least 1
                (None for unlimited).
            withdirs: Whether to include directories in the result.
            **kwargs: Additional arguments including ``prefix`` and
                ``refresh``, as described in ``find``.

        Returns:
            The objects found, and the directories if ``withdirs`` is True.

        Raises:
            ValueError: If ``maxdepth`` is less than 1 or the path is the root.
        """
        if maxdepth is not None and maxdepth < 1:
            raise ValueError("maxdepth must be at least 1")
        path = self._strip_protocol(path)
        if path in ["", "/"]:
            raise ValueError("Cannot traverse all files in S3.")
        bucket, key, _ = self.parse_path(path)
        prefix = kwargs.pop("prefix", "")
        # Keep refresh in kwargs so that the recursive calls also refresh.
        refresh = kwargs.get("refresh", False)

        # When maxdepth is specified, use a recursive approach with delimiter
        if maxdepth is not None:
            result: list[S3Object] = []

            # List files and directories at current level
            current_items = self._ls_dirs(path, prefix=prefix, delimiter="/", refresh=refresh)

            for item in current_items:
                if item.type == S3ObjectType.S3_OBJECT_TYPE_FILE:
                    # Add files
                    result.append(item)
                elif item.type == S3ObjectType.S3_OBJECT_TYPE_DIRECTORY:
                    # Add directory if withdirs is True
                    if withdirs:
                        result.append(item)

                    # Recursively explore subdirectory if depth allows
                    if maxdepth > 1:
                        sub_path = f"s3://{bucket}/{item.key}"
                        sub_results = self._find(
                            sub_path, maxdepth=maxdepth - 1, withdirs=withdirs, **kwargs
                        )
                        result.extend(sub_results)

            return result

        # For unlimited depth, use the original approach (get all files at once)
        files = self._ls_dirs(path, prefix=prefix, delimiter="", refresh=refresh)
        if not files and key:
            try:
                files = [self.info(path, refresh=refresh)]
            except FileNotFoundError:
                files = []

        # If withdirs is True, we need to derive directories from file paths
        if withdirs:
            # Build a new list; files may be the cached listing.
            files = files + self._extract_parent_directories(files, bucket, key)

        # Filter directories if withdirs is False (default)
        if withdirs is False or withdirs is None:
            files = [f for f in files if f.type != S3ObjectType.S3_OBJECT_TYPE_DIRECTORY]

        return files

    def find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool | None = None,
        detail: bool = False,
        **kwargs,
    ) -> dict[str, S3Object] | list[str]:
        """Find all files below a given S3 path.

        Recursively searches for files under the specified path, with optional
        depth limiting and directory inclusion. Uses efficient S3 list operations
        with delimiter handling for performance.

        Args:
            path: S3 path to search under (e.g., "s3://bucket/prefix").
            maxdepth: Maximum number of levels to descend, at least 1
                (None for unlimited). With 1, only the entries directly under
                the path are listed.
            withdirs: Whether to include directories in results (None = default behavior).
            detail: If True, return dict of {path: S3Object}; if False, return list of paths.
            **kwargs: Additional arguments including:
                prefix: Key prefix, relative to the path, to filter the listed keys
                    by. Without maxdepth, if nothing is listed and the path itself is
                    an object, that object is returned regardless of the prefix.
                refresh: If True, bypass the cache and list from S3.

        Returns:
            Dictionary mapping paths to S3Objects (if detail=True) or
            list of paths (if detail=False).

        Raises:
            ValueError: If ``maxdepth`` is less than 1 or the path is the root.

        Example:
            >>> fs = S3FileSystem()
            >>> fs.find("s3://bucket/data/", maxdepth=2)  # Limit depth
            >>> fs.find("s3://bucket/", withdirs=True)    # Include directories
        """
        files = self._find(path=path, maxdepth=maxdepth, withdirs=withdirs, **kwargs)
        if detail:
            return {f.name: f for f in files}
        return [f.name for f in files]

    def exists(self, path: str, **kwargs) -> bool:
        """Check if an S3 path exists.

        Determines whether a bucket, object, or prefix exists in S3.
        Uses caching and efficient head operations to minimize API calls.

        Args:
            path: S3 path to check (e.g., "s3://bucket" or "s3://bucket/key").
            **kwargs: Additional arguments including:
                refresh: If True, bypass the cache and query S3.

        Returns:
            True if the path exists, False otherwise.

        Example:
            >>> fs = S3FileSystem()
            >>> fs.exists("s3://my-bucket/file.txt")
            >>> fs.exists("s3://my-bucket/")
        """
        refresh = kwargs.pop("refresh", False)
        path = self._strip_protocol(path)
        if path in ["", "/"]:
            # The root always exists.
            return True
        bucket, key, _ = self.parse_path(path)
        if key:
            try:
                if not refresh and self._ls_from_cache(path):
                    return True
                info = self.info(path, refresh=refresh)
                return bool(info)
            except FileNotFoundError:
                return False
        if not refresh:
            if self.dircache.get(bucket, False):
                return True
            try:
                if self._ls_from_cache(bucket):
                    return True
            except FileNotFoundError:
                pass
        file = self._head_bucket(bucket, refresh=refresh)
        return bool(file)

    def rm_file(self, path: str, **kwargs) -> None:
        """Delete an S3 object with DeleteObject.

        Does nothing for a bucket path. If the path has a version ID, that
        version is deleted.

        Args:
            path: S3 path (s3://bucket/key) of the object to delete.
            **kwargs: Accepted for fsspec compatibility; not used in the
                request.
        """
        bucket, key, version_id = self.parse_path(path)
        if not key:
            return
        self._delete_object(bucket=bucket, key=key, version_id=version_id, **kwargs)
        self.invalidate_cache(path)

    def rm(self, path, recursive=False, maxdepth=None, **kwargs) -> None:
        """Delete objects with DeleteObjects requests.

        Expands the paths with ``expand_path`` and deletes the matched objects
        in parallel requests of up to ``DELETE_OBJECTS_MAX_KEYS`` keys each,
        one set of requests per bucket. A path with a version ID deletes that
        version without expansion.

        Args:
            path: S3 path (s3://bucket/key) or list of paths to delete.
            recursive: Whether to delete all objects below the paths.
            maxdepth: Maximum depth to expand when ``recursive`` is True.
            **kwargs: Additional parameters passed to the DeleteObjects API.
                ``Quiet`` (default True) sets the quiet mode of the requests.

        Raises:
            ValueError: If a path is a bucket.
            OSError: If S3 could not delete some of the objects.
        """
        paths = self._expand_delete_paths(path, recursive=recursive, maxdepth=maxdepth)
        self._delete_objects(paths, **kwargs)

    def _expand_delete_paths(
        self, path: str | list[str], recursive: bool = False, maxdepth: int | None = None
    ) -> list[str]:
        """Expand the paths that ``rm`` deletes.

        Args:
            path: S3 path or list of paths.
            recursive: Whether to include all objects below the paths.
            maxdepth: Maximum depth to expand when ``recursive`` is True.

        Returns:
            The paths with a version ID as given, followed by the expansion
            of the other paths by ``expand_path``.

        Raises:
            ValueError: If a path is a bucket.
        """
        paths = [path] if isinstance(path, str) else list(path)
        versioned_paths, unversioned_paths = [], []
        for p in paths:
            _, key, version_id = self.parse_path(p)
            # expand_path strips the slashes of "bucket//" to the bucket.
            if not key or not key.strip("/"):
                raise ValueError("Cannot delete the bucket.")
            if version_id:
                versioned_paths.append(p)
            else:
                unversioned_paths.append(p)

        if unversioned_paths:
            # expand_path treats "?" as a wildcard, so versioned paths skip it.
            unversioned_paths = self.expand_path(
                unversioned_paths, recursive=recursive, maxdepth=maxdepth
            )
        return versioned_paths + unversioned_paths

    def _delete_object(
        self, bucket: str, key: str, version_id: str | None = None, **kwargs
    ) -> None:
        request = {
            "Bucket": bucket,
            "Key": key,
        }
        if version_id:
            request.update({"VersionId": version_id})

        _logger.debug(f"Delete object: s3://{bucket}/{key}?versionId={version_id}")
        self._call(
            self._client.delete_object,
            **request,
        )

    def _create_executor(self, max_workers: int) -> S3Executor:
        """Create an executor strategy for parallel operations.

        Subclasses can override to provide alternative execution strategies
        (e.g., asyncio-based execution).

        Args:
            max_workers: Maximum number of parallel workers.

        Returns:
            An S3Executor instance.
        """
        return S3ThreadPoolExecutor(max_workers=max_workers)

    def _delete_objects(self, paths: list[str], max_workers: int | None = None, **kwargs) -> None:
        """Delete objects with DeleteObjects requests grouped by bucket.

        Args:
            paths: Paths of the objects to delete. Bucket paths are skipped.
            max_workers: Maximum number of parallel requests. Defaults to
                ``self.max_workers``.
            **kwargs: Additional parameters passed to the DeleteObjects API.
                ``Quiet`` (default True) sets the quiet mode of the requests.

        Raises:
            OSError: If S3 could not delete some of the objects.
        """
        requests = self._delete_objects_requests(paths, **kwargs)
        if not requests:
            return

        max_workers = max_workers if max_workers else self.max_workers
        with self._create_executor(max_workers=max_workers) as executor:
            fs = [executor.submit(self._delete_objects_request, request) for request in requests]
        # The executor has waited for every request, also after a failure.
        self._raise_delete_objects_errors(requests, [f.exception() or f.result() for f in fs])

    def _delete_objects_requests(self, paths: list[str], **kwargs) -> list[dict[str, Any]]:
        """Build the DeleteObjects requests that delete the objects.

        Args:
            paths: Paths of the objects to delete. Bucket paths are skipped.
            **kwargs: Additional parameters of the requests. ``Quiet``
                (default True) sets the quiet mode of the requests.

        Returns:
            Requests of up to ``DELETE_OBJECTS_MAX_KEYS`` keys of one bucket each.

        Raises:
            TypeError: If kwargs has ``Bucket`` or ``Delete``.
        """
        for name in ("Bucket", "Delete"):
            if name in kwargs:
                raise TypeError(f"rm() got an unexpected keyword argument '{name}'")
        quiet = kwargs.pop("Quiet", True)
        delete_objects: dict[str, list[dict[str, str]]] = {}
        for p in paths:
            bucket, key, version_id = self.parse_path(p)
            if key:
                object_ = {"Key": key}
                if version_id:
                    object_.update({"VersionId": version_id})
                delete_objects.setdefault(bucket, []).append(object_)
        return [
            {
                "Bucket": bucket,
                "Delete": {
                    "Objects": objects[i : i + self.DELETE_OBJECTS_MAX_KEYS],
                    "Quiet": quiet,
                },
                **kwargs,
            }
            for bucket, objects in delete_objects.items()
            for i in range(0, len(objects), self.DELETE_OBJECTS_MAX_KEYS)
        ]

    def _delete_objects_request(self, request: dict[str, Any]) -> dict[str, Any]:
        """Send a DeleteObjects request and invalidate the cache of its objects.

        The cache is invalidated when the request has finished, also when it
        fails, because S3 may have deleted some of the objects.

        Args:
            request: The DeleteObjects request.

        Returns:
            The DeleteObjects response.
        """
        try:
            return self._call(self._client.delete_objects, **request)
        finally:
            for object_ in request["Delete"]["Objects"]:
                self.invalidate_cache(self._delete_objects_path(request["Bucket"], object_))

    @staticmethod
    def _delete_objects_path(bucket: str, object_: dict[str, Any]) -> str:
        """Build the path of a DeleteObjects object or error entry.

        Args:
            bucket: The bucket of the request.
            object_: An entry with ``Key`` and an optional ``VersionId``.

        Returns:
            The path, with a ``?versionId=`` query if the entry has a version.
        """
        path = f"{bucket}/{object_['Key']}"
        if object_.get("VersionId"):
            path += f"?versionId={object_['VersionId']}"
        return path

    @staticmethod
    def _raise_delete_objects_errors(
        requests: list[dict[str, Any]], results: list[dict[str, Any] | BaseException]
    ) -> None:
        """Raise an error for the DeleteObjects requests that failed.

        S3 reports the objects it could not delete in the ``Errors`` of a
        successful response.

        Args:
            requests: The DeleteObjects requests.
            results: The response or the exception of each request, in the
                order of the requests.

        Raises:
            BaseException: The first exception of the requests, with a note
                that lists the objects of ``Errors``, if any.
            OSError: If no request raised and a response has errors.
        """
        exceptions = []
        errors = []
        for request, result in zip(requests, results, strict=True):
            if isinstance(result, BaseException):
                exceptions.append(result)
                continue
            for error in result.get("Errors", []):
                path = S3FileSystem._delete_objects_path(request["Bucket"], error)
                errors.append(f"{path} ({error.get('Code')}: {error.get('Message')})")
        message = f"Failed to delete objects: {', '.join(sorted(errors))}" if errors else None
        if exceptions:
            if message:
                exceptions[0].add_note(message)
            raise exceptions[0]
        if message:
            raise OSError(message)

    def mkdir(self, path: str, create_parents: bool = True, **kwargs) -> None:
        """Create an S3 bucket.

        S3 has no real directories below the bucket level; creating a key
        prefix requires no operation. This method creates the bucket when the
        path points at a bucket (or when ``create_parents`` is True and the
        bucket does not exist yet), and does nothing for key prefixes under
        an existing bucket.

        Bucket lifecycle operations are disabled by default because they are
        infrastructure-level changes; pass ``allow_bucket_creation=True`` to
        the filesystem constructor to enable bucket creation.

        Args:
            path: S3 path (e.g., "s3://bucket" or "s3://bucket/prefix").
            create_parents: If True, create the bucket when it does not exist,
                even if the path contains a key prefix.
            **kwargs: Additional arguments including:
                acl: Canned ACL to apply to the bucket.
                region_name: Region to create the bucket in. Defaults to the
                    client's region.

        Raises:
            FileExistsError: If the path is a bucket that already exists.
            FileNotFoundError: If the bucket does not exist and
                ``create_parents`` is False.
            PermissionError: If the bucket would be created but bucket
                creation is not enabled on this filesystem instance.
            ValueError: If the ACL is invalid or the path is empty.
        """
        path = self._strip_protocol(path).rstrip("/")
        if not path:
            raise ValueError("Cannot create the root directory.")
        bucket, key, _ = self.parse_path(path)
        if self.exists(bucket):
            if not key:
                # Requested to create a bucket, but the bucket already exists.
                raise FileExistsError(bucket)
            # Do nothing as the bucket already exists.
        elif not key or create_parents:
            if not self.allow_bucket_creation:
                raise PermissionError(
                    "Bucket creation is disabled. "
                    "Set allow_bucket_creation=True on the filesystem to enable it."
                )
            acl = kwargs.pop("acl", "")
            if acl and acl not in self.BUCKET_ACLS:
                raise ValueError(f"ACL not in {self.BUCKET_ACLS}.")
            request: dict[str, Any] = {"Bucket": bucket}
            if acl:
                request.update({"ACL": acl})
            region_name = kwargs.pop("region_name", None) or self._client.meta.region_name
            if region_name and region_name != "us-east-1":
                # us-east-1 does not accept a location constraint.
                request.update({"CreateBucketConfiguration": {"LocationConstraint": region_name}})

            _logger.debug(f"Create bucket: s3://{bucket}")
            try:
                self._call(
                    self._client.create_bucket,
                    **request,
                )
            except botocore.exceptions.ParamValidationError as e:
                raise ValueError(f"Bucket create failed {bucket!r}: {e}") from e
            # invalidate_cache walks parent paths and never pops the root
            # entry itself, so evict the cached bucket listing directly.
            self._evict_cache("")
            self.invalidate_cache(bucket)
        else:
            # exists() has already confirmed the bucket does not exist,
            # and it is not requested to be created.
            raise FileNotFoundError(bucket)

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Recursively create a directory, creating the bucket if necessary.

        Creating the bucket requires ``allow_bucket_creation=True`` on the
        filesystem constructor; see :meth:`mkdir`.

        Args:
            path: S3 path (e.g., "s3://bucket" or "s3://bucket/prefix").
            exist_ok: If False, raise FileExistsError when the path is a
                bucket that already exists.

        Raises:
            FileExistsError: If the path is a bucket that already exists and
                ``exist_ok`` is False.
            PermissionError: If the bucket would be created but bucket
                creation is not enabled on this filesystem instance.
        """
        try:
            self.mkdir(path, create_parents=True)
        except FileExistsError:
            if not exist_ok:
                raise

    def rmdir(self, path: str) -> None:
        """Remove an S3 bucket, which must be empty.

        S3 has no real directories below the bucket level, so only bucket
        paths can be removed.

        Bucket lifecycle operations are disabled by default because they are
        infrastructure-level changes; pass ``allow_bucket_deletion=True`` to
        the filesystem constructor to enable bucket deletion.

        Args:
            path: S3 bucket path (e.g., "s3://bucket").

        Raises:
            FileExistsError: If the path contains a key that exists. The user
                may have meant ``rm(path, recursive=True)``.
            FileNotFoundError: If the path contains a key that does not exist,
                or the bucket does not exist.
            PermissionError: If bucket deletion is not enabled on this
                filesystem instance.
            OSError: If the bucket is not empty.
        """
        path = self._strip_protocol(path).rstrip("/")
        bucket, key, _ = self.parse_path(path)
        if key:
            if self.exists(path):
                # The user may have meant rm(path, recursive=True).
                raise FileExistsError(path)
            raise FileNotFoundError(path)
        if not self.allow_bucket_deletion:
            raise PermissionError(
                "Bucket deletion is disabled. "
                "Set allow_bucket_deletion=True on the filesystem to enable it."
            )

        _logger.debug(f"Delete bucket: s3://{bucket}")
        self._call(
            self._client.delete_bucket,
            Bucket=bucket,
        )
        self.invalidate_cache(bucket)
        # invalidate_cache walks parent paths and never pops the root
        # entry itself, so evict the cached bucket listing directly.
        self._evict_cache("")

    def touch(self, path: str, truncate: bool = True, **kwargs) -> dict[str, Any]:
        """Create an empty object with PutObject.

        Args:
            path: S3 path (s3://bucket/key) of the object.
            truncate: If True, replace an existing object with an empty one;
                if False, raise if the object exists.
            **kwargs: Additional parameters passed to the PutObject API.

        Returns:
            The PutObject response as a dictionary (see
            :meth:`S3PutObject.to_dict`).

        Raises:
            ValueError: If the path has a version ID, is a bucket, or exists
                while ``truncate`` is False.
        """
        bucket, key, version_id = self.parse_path(path)
        if version_id:
            raise ValueError("Cannot touch the file with the version specified.")
        if not truncate and self.exists(path):
            raise ValueError("Cannot touch the existing file without specifying truncate.")
        if not key:
            raise ValueError("Cannot touch the bucket.")

        object_ = self._put_object(bucket=bucket, key=key, body=None, **kwargs)
        self.invalidate_cache(path)
        return object_.to_dict()

    def cp_file(
        self, path1: str, path2: str, recursive=False, maxdepth=None, on_error=None, **kwargs
    ):
        """Copy an S3 object to another S3 location.

        Performs server-side copy of S3 objects, which is more efficient than
        downloading and re-uploading. Automatically chooses between simple copy
        and multipart copy based on object size.

        Args:
            path1: Source S3 path (s3://bucket/key).
            path2: Destination S3 path (s3://bucket/key).
            recursive: Unused parameter for fsspec compatibility.
            maxdepth: Unused parameter for fsspec compatibility.
            on_error: Unused parameter for fsspec compatibility.
            **kwargs: Additional S3 copy parameters (e.g., metadata, storage
                class). The ``block_size`` and ``max_workers`` parameters
                control a multipart copy and are not sent to S3.

        Raises:
            ValueError: If trying to copy to a versioned file or copy buckets.

        Note:
            Uses multipart copy for objects larger than the maximum part size
            to optimize performance for large files. The copy operation is
            performed entirely on the S3 service without data transfer.
        """
        # fsspec < 2026.6.0: AbstractFileSystem.mv() passed the typo'd
        # "onerror" keyword (instead of "on_error", which copy() consumes),
        # so it leaked through copy(**kwargs) into cp_file and must not
        # reach the S3 API. Remove this once the fsspec requirement is
        # >= 2026.6.0, where mv() passes on_error correctly.
        # https://github.com/fsspec/filesystem_spec/commit/346a589fef9308550ffa3d0d510f2db67281bb05
        kwargs.pop("onerror", None)
        # Parameters of the multipart copy, not of the S3 requests.
        block_size = kwargs.pop("block_size", None)
        max_workers = kwargs.pop("max_workers", None)
        bucket1, key1, version_id1 = self.parse_path(path1)
        bucket2, key2, version_id2 = self.parse_path(path2)
        if version_id2:
            raise ValueError("Cannot copy to a versioned file.")
        if not key1 or not key2:
            raise ValueError("Cannot copy buckets.")

        info1 = self.info(path1)
        size1 = info1.get("size", 0)
        if size1 <= self.MULTIPART_UPLOAD_MAX_PART_SIZE:
            self._copy_object(
                bucket1=bucket1,
                key1=key1,
                version_id1=version_id1,
                bucket2=bucket2,
                key2=key2,
                **kwargs,
            )
        else:
            self._copy_object_with_multipart_upload(
                bucket1=bucket1,
                key1=key1,
                version_id1=version_id1,
                size1=size1,
                bucket2=bucket2,
                key2=key2,
                max_workers=max_workers,
                block_size=block_size,
                **kwargs,
            )
        self.invalidate_cache(path2)

    def _copy_object(
        self,
        bucket1: str,
        key1: str,
        version_id1: str | None,
        bucket2: str,
        key2: str,
        **kwargs,
    ) -> None:
        copy_source = {
            "Bucket": bucket1,
            "Key": key1,
        }
        if version_id1:
            copy_source.update({"VersionId": version_id1})
        request = {
            "CopySource": copy_source,
            "Bucket": bucket2,
            "Key": key2,
        }

        _logger.debug(
            f"Copy object from s3://{bucket1}/{key1}?versionId={version_id1} "
            f"to s3://{bucket2}/{key2}."
        )
        self._call(self._client.copy_object, **request, **kwargs)

    def _copy_object_with_multipart_upload(
        self,
        bucket1: str,
        key1: str,
        size1: int,
        bucket2: str,
        key2: str,
        max_workers: int | None = None,
        block_size: int | None = None,
        version_id1: str | None = None,
        **kwargs,
    ) -> None:
        max_workers = max_workers if max_workers else self.max_workers
        block_size = block_size if block_size else self.MULTIPART_UPLOAD_MAX_PART_SIZE
        if (
            block_size < self.MULTIPART_UPLOAD_MIN_PART_SIZE
            or block_size > self.MULTIPART_UPLOAD_MAX_PART_SIZE
        ):
            raise ValueError(
                "Block size must be between "
                f"5 MiB ({self.MULTIPART_UPLOAD_MIN_PART_SIZE} bytes) and "
                f"5 GiB ({self.MULTIPART_UPLOAD_MAX_PART_SIZE} bytes), inclusive: {block_size}."
            )

        copy_source = {
            "Bucket": bucket1,
            "Key": key1,
        }
        if version_id1:
            copy_source.update({"VersionId": version_id1})

        ranges = self._get_copy_ranges(size1, block_size)
        multipart_upload = self._create_multipart_upload(
            bucket=bucket2,
            key=key2,
            **kwargs,
        )
        with self._create_executor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    self._upload_part_copy,
                    bucket=bucket2,
                    key=key2,
                    copy_source=copy_source,
                    upload_id=cast(str, multipart_upload.upload_id),
                    part_number=i + 1,
                    copy_source_ranges=range_,
                    **self._get_operation_kwargs("upload_part_copy", kwargs),
                )
                for i, range_ in enumerate(ranges)
            ]
            self._finish_multipart_upload(
                bucket=bucket2,
                key=key2,
                upload_id=cast(str, multipart_upload.upload_id),
                futures=futures,
                request_kwargs=kwargs,
            )

    def _get_copy_ranges(self, size: int, block_size: int) -> list[tuple[int, int]]:
        """Split an object into the source ranges of a multipart copy.

        The object is split into ranges of ``block_size`` bytes, whatever the
        number of workers, or of a larger size that splits it into at most
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

    def _check_multipart_upload_size(self, path: str, size: int, block_size: int) -> None:
        """Check that data fits in a multipart upload before uploading it.

        Args:
            path: The path that the data is written to.
            size: The size of the data in bytes.
            block_size: The block size of the write in bytes.

        Raises:
            ValueError: If the data takes more than
                ``MULTIPART_UPLOAD_MAX_PARTS`` blocks.
        """
        if size > block_size * self.MULTIPART_UPLOAD_MAX_PARTS:
            min_block_size = max(
                math.ceil(size / self.MULTIPART_UPLOAD_MAX_PARTS),
                self.MULTIPART_UPLOAD_MIN_PART_SIZE,
            )
            raise ValueError(
                f"Cannot upload {size} bytes to {path} in "
                f"{self.MULTIPART_UPLOAD_MAX_PARTS} parts with a block size of "
                f"{block_size} bytes. Write the file with a block_size, or a "
                "default_block_size of the filesystem, of at least "
                f"{min_block_size} bytes."
            )

    @staticmethod
    def _write_and_close(f: S3File, value: bytes | bytearray | memoryview) -> None:
        """Write the whole value to a file opened for writing and close it.

        Unlike a ``with`` block, a failed write closes the file without
        committing it, so the existing object is left unchanged.

        Args:
            f: The file to write to.
            value: The bytes to write.
        """
        try:
            if isinstance(value, memoryview) and not value.c_contiguous:
                # The buffer of the file cannot write a non-contiguous memoryview.
                value = value.tobytes()
            f.write(value)
        except BaseException:
            f._close_without_commit()
            raise
        f.close()

    def pipe_file(
        self, path: str, value: bytes | bytearray | memoryview, mode: str = "overwrite", **kwargs
    ) -> None:
        """Write bytes into the path.

        Writes data up to the block size with a single PutObject request
        instead of the inherited ``open()`` + ``write()`` path. Larger data
        and writes inside an fsspec transaction go through the buffered
        path, which uploads the data as a parallel multipart upload and
        keeps the deferred-commit semantics of transactions. A write that
        fails on that path leaves the existing object unchanged.

        Args:
            path: S3 path (s3://bucket/key) to write to.
            value: The bytes to write.
            mode: "overwrite" (default) or "create". With "create", raise
                FileExistsError when the object already exists, including
                one created during the write, which is not replaced.
            **kwargs: Additional parameters passed to the PutObject API
                (e.g., ContentType, StorageClass) on the single-request
                path. The ``block_size``, ``max_workers``, and
                ``s3_additional_kwargs`` parameters of the ``open()`` path
                are also accepted.

        Raises:
            FileExistsError: If the mode is "create" and the path already
                exists, or an object is created at it before the write is
                committed.
            ValueError: If the path does not contain a key or specifies a
                version, or if the data takes more than
                ``MULTIPART_UPLOAD_MAX_PARTS`` blocks.
        """
        block_size = kwargs.get("block_size") or self.default_block_size
        # The size in bytes; the length of a memoryview counts its items.
        self._check_multipart_upload_size(path, memoryview(value).nbytes, block_size)
        if self._intrans or len(value) > min(block_size, self.MULTIPART_UPLOAD_MAX_PART_SIZE):
            # Defer to the buffered open() path, which keeps the
            # deferred-commit semantics of fsspec transactions and uploads
            # large data as a parallel multipart upload.
            self._write_and_close(
                self.open(path, "xb" if mode == "create" else "wb", **kwargs), value
            )
            return
        bucket, key, version_id = self.parse_path(path)
        if version_id:
            raise ValueError("Cannot write to the file with the version specified.")
        if not key:
            raise ValueError("Cannot write to a bucket.")
        if mode == "create":
            # Checked up front, as open() does in "xb" mode, and with
            # IfNoneMatch for an object created since.
            if self.exists(path):
                raise FileExistsError(path)
            kwargs["IfNoneMatch"] = "*"
        if not isinstance(value, bytes):
            # Accept bytes-like values (bytearray, memoryview) as the
            # buffered path does.
            value = bytes(value)

        kwargs.pop("block_size", None)
        kwargs.pop("max_workers", None)
        request_kwargs = {
            **self._get_operation_kwargs("put_object", self.s3_additional_kwargs),
            **kwargs.pop("s3_additional_kwargs", {}),
            **kwargs,
        }
        self._put_object(bucket=bucket, key=key, body=value, **request_kwargs)
        self.invalidate_cache(path)

    def _finish_multipart_upload(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        futures: list[Future[S3MultipartUploadPart]],
        request_kwargs: Mapping[str, Any] | None = None,
    ) -> S3CompleteMultipartUpload:
        """Collect the uploaded parts and complete the multipart upload.

        When any part or the completion fails, the parts that have not
        started are cancelled, the running ones are waited for, and the
        multipart upload is aborted so that no incomplete upload or part is
        left behind. The original error is then re-raised.

        Args:
            bucket: S3 bucket name.
            key: Object key being uploaded.
            upload_id: Unique identifier for the multipart upload.
            futures: Futures of the part uploads, in part-number order.
            request_kwargs: Parameters of the upload, such as
                ``RequestPayer`` or the SSE-C parameters; the completion and
                the abort receive those that they accept.

        Returns:
            S3CompleteMultipartUpload of the completed upload.
        """
        request_kwargs = request_kwargs or {}
        try:
            # The futures are in part-number order.
            results = [future.result() for future in futures]
            parts = [{"ETag": r.etag, "PartNumber": r.part_number} for r in results]
            return self._complete_multipart_upload(
                bucket=bucket,
                key=key,
                upload_id=upload_id,
                parts=parts,
                **self._get_operation_kwargs("complete_multipart_upload", request_kwargs),
            )
        except Exception:
            # A part that is still uploading when the upload is aborted may
            # be stored after the abort, so wait for the parts that could not
            # be cancelled first.
            wait([future for future in futures if not future.cancel()])
            try:
                self._call(
                    self._client.abort_multipart_upload,
                    **{
                        **self._get_operation_kwargs("abort_multipart_upload", request_kwargs),
                        "Bucket": bucket,
                        "Key": key,
                        "UploadId": upload_id,
                    },
                )
            except Exception:
                _logger.exception(
                    f"Failed to abort multipart upload {upload_id} to s3://{bucket}/{key}."
                )
            raise

    def cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs
    ) -> bytes:
        """Read the contents of an S3 object with GetObject.

        ``start`` and ``end`` select bytes like a slice of the object: an
        empty range, or one that starts at or past the end of the object,
        returns ``b""``, and an end past the object reads up to its end.
        Non-negative offsets are sent to S3 as they are, and so is a negative
        ``start`` without an ``end``, as a suffix range of the last bytes.
        Other negative offsets are resolved against the size from
        :meth:`info`, which also checks that the object exists for an empty
        range.

        Args:
            path: S3 path (s3://bucket/key) of the object.
            start: Byte offset to start reading at. A negative value counts
                from the end of the object.
            end: Byte offset to stop reading at (exclusive). A negative value
                counts from the end of the object.
            **kwargs: Additional parameters passed to the GetObject API,
                except ``version_id``: the version ID to read when the path
                has none.

        Returns:
            The bytes read from the object.

        Raises:
            FileNotFoundError: If the path has no key or the key does not
                exist.
        """
        bucket, key, path_version_id = self.parse_path(path)
        if not key:
            raise FileNotFoundError(path)
        version_id = kwargs.pop("version_id", None)
        if path_version_id:
            version_id = path_version_id
        ranges: tuple[int, int | None] | None = None
        if start is not None and start < 0 and end is None:
            # S3 returns the last bytes, or the whole object when it is
            # shorter, without the size of the object.
            ranges = (start, None)
        else:
            if (start is not None and start < 0) or (
                end is not None and (end < 0 or (start or 0) >= end)
            ):
                # A negative offset needs the size of the object, and an
                # empty range sends no GetObject request that would report a
                # missing object.
                info = self.info(path, version_id=version_id)
                if info.get("type") == S3ObjectType.S3_OBJECT_TYPE_DIRECTORY or info.key != key:
                    # There is no object to read, as GetObject reports for
                    # the other ranges, or info() describes the key without
                    # the trailing slash of this one.
                    raise FileNotFoundError(path)
                start, end, _ = slice(start, end).indices(info.get("size", 0))
            if start is not None or end is not None:
                start = start or 0
                if end is not None and start >= end:
                    # S3 would return the whole object for an empty range.
                    return b""
                ranges = (start, end)
        try:
            return self._get_object(
                bucket=bucket,
                key=key,
                ranges=ranges,
                version_id=version_id,
                **kwargs,
            )[1]
        except OSError as e:
            if (
                ranges
                and isinstance(e.__cause__, botocore.exceptions.ClientError)
                and S3ClientError(e.__cause__).code == "InvalidRange"
            ):
                # The range starts at or past the end of the object.
                return b""
            raise

    def put_file(
        self,
        lpath: str,
        rpath: str,
        callback=_DEFAULT_CALLBACK,
        mode: str = "overwrite",
        **kwargs,
    ):
        """Upload a local file to S3.

        Uploads a file from the local filesystem to an S3 location. Supports
        automatic content type detection based on file extension and provides
        progress callback functionality.

        Args:
            lpath: Local file path to upload.
            rpath: S3 destination path (s3://bucket/key).
            callback: Progress callback for tracking upload progress.
            mode: "overwrite" (default) or "create". With "create", the file
                is written as with ``open()`` in ``xb`` mode: raise
                FileExistsError when the object already exists, including
                one created during the upload, which is not replaced.
            **kwargs: Additional S3 parameters (e.g., ContentType, StorageClass).
                The ``block_size``, ``max_workers``, and ``s3_additional_kwargs``
                parameters of ``open()`` are also accepted.

        Raises:
            FileExistsError: If the mode is "create" and the path already
                exists, or an object is created at it before the upload is
                committed.
            ValueError: If the file takes more than
                ``MULTIPART_UPLOAD_MAX_PARTS`` blocks.

        Note:
            Directories are not supported for upload. If lpath is a directory,
            the method returns without performing any operation. Bucket-only
            destinations (without key) are also not supported.
        """
        if os.path.isdir(lpath):
            # No support for directory uploads.
            return

        bucket, key, _ = self.parse_path(rpath)
        if not key:
            # No support for bucket copy.
            return

        size = os.path.getsize(lpath)
        block_size = kwargs.pop("block_size", None) or self.default_block_size
        max_workers = kwargs.pop("max_workers", self.max_workers)
        # The other parameters are S3 request parameters, as in pipe_file().
        s3_additional_kwargs = {**kwargs.pop("s3_additional_kwargs", {}), **kwargs}
        self._check_multipart_upload_size(rpath, size, block_size)
        callback.set_size(size)
        if "ContentType" not in {**self.s3_additional_kwargs, **s3_additional_kwargs}:
            content_type, _ = mimetypes.guess_type(lpath)
            if content_type is not None:
                s3_additional_kwargs["ContentType"] = content_type

        with (
            self.open(
                rpath,
                "xb" if mode == "create" else "wb",
                block_size=block_size,
                max_workers=max_workers,
                s3_additional_kwargs=s3_additional_kwargs,
            ) as remote,
            open(lpath, "rb") as local,
        ):
            while data := local.read(remote.blocksize):
                remote.write(data)
                callback.relative_update(len(data))

        self.invalidate_cache(rpath)

    def get_file(self, rpath: str, lpath=None, callback=_DEFAULT_CALLBACK, outfile=None, **kwargs):
        """Download an S3 file to local filesystem.

        Downloads a file from S3 to the local filesystem with progress tracking.
        Reads the file in chunks to handle large files efficiently.

        As with fsspec's ``AbstractFileSystem.get_file()``, a directory
        ``rpath`` creates the local directory ``lpath``, and the parent
        directories of a local file ``lpath`` are created as needed.

        Args:
            rpath: S3 source path (s3://bucket/key).
            lpath: Local destination path, or a file-like object to write to.
                Not needed when ``outfile`` is given.
            callback: Progress callback for tracking download progress.
            outfile: A file-like object to write to instead of ``lpath``.
            **kwargs: Additional S3 parameters passed to open().
        """
        _, _, path_version_id = self.parse_path(self._strip_protocol(rpath))
        if outfile is None and isfilelike(lpath):
            outfile = lpath
        elif (
            outfile is None
            and not (path_version_id or kwargs.get("version_id"))
            and self.isdir(rpath)
        ):
            # A requested version always names an object, while isdir()
            # would look up the latest version, or the prefix of the same
            # name when the version does not exist.
            os.makedirs(lpath, exist_ok=True)
            return

        # The remote file is opened first so that no local file is created
        # when open() finds no object at the path.
        with contextlib.ExitStack() as stack:
            remote = stack.enter_context(self.open(rpath, "rb", **kwargs))
            if outfile is None:
                # Not abspath(), which would resolve ".." before symlinks.
                if parent := os.path.dirname(lpath):
                    os.makedirs(parent, exist_ok=True)
                outfile = stack.enter_context(open(lpath, "wb"))
            callback.set_size(remote.size)
            while data := remote.read(remote.blocksize):
                outfile.write(data)
                callback.relative_update(len(data))

    def checksum(self, path: str, **kwargs):
        """Get checksum for S3 object or directory.

        Computes a checksum for the specified S3 path. For individual objects,
        returns the ETag converted to an integer. For directories, returns a
        checksum based on the directory's tokenized representation.

        Args:
            path: S3 path (s3://bucket/key) to get checksum for.
            **kwargs: Additional arguments including:
                refresh: If True, refresh cached info before computing checksum.

        Returns:
            Integer checksum value derived from S3 ETag or directory token.

        Note:
            For multipart uploads, ETag format is different and only the first
            part before the dash is used for checksum calculation.
        """
        refresh = kwargs.pop("refresh", False)
        info = self.info(path, refresh=refresh)
        if info.get("type") != S3ObjectType.S3_OBJECT_TYPE_DIRECTORY:
            return int(info.get("etag").strip('"').split("-")[0], 16)
        return int(tokenize(info), 16)

    def sign(self, path: str, expiration: int = 3600, **kwargs):
        """Generate a presigned URL for S3 object access.

        Creates a presigned URL that allows temporary access to an S3 object
        without requiring AWS credentials. Useful for sharing files or providing
        time-limited access to resources.

        Args:
            path: S3 path (s3://bucket/key) to generate URL for.
            expiration: URL expiration time in seconds. Defaults to 3600 (1 hour).
            **kwargs: Additional parameters including:
                client_method: S3 operation ('get_object', 'put_object', etc.).
                             Defaults to 'get_object'.
                Additional parameters passed to the S3 operation.

        Returns:
            Presigned URL string that provides temporary access to the S3 object.

        Example:
            >>> fs = S3FileSystem()
            >>> url = fs.sign("s3://my-bucket/file.txt", expiration=7200)
            >>> # URL valid for 2 hours
            >>>
            >>> # Generate upload URL
            >>> upload_url = fs.sign(
            ...     "s3://my-bucket/upload.txt",
            ...     client_method="put_object"
            ... )
        """
        bucket, key, version_id = self.parse_path(path)
        client_method = kwargs.pop("client_method", "get_object")
        params = {"Bucket": bucket, "Key": key}
        if version_id:
            params.update({"VersionId": version_id})
        if kwargs:
            params.update(kwargs)
        request = {
            "ClientMethod": client_method,
            "Params": params,
            "ExpiresIn": expiration,
        }

        _logger.debug(f"Generate signed url: s3://{bucket}/{key}?versionId={version_id}")
        return self._call(
            self._client.generate_presigned_url,
            **request,
        )

    def metadata(self, path: str, **kwargs) -> S3Metadata:
        """Return the metadata of the path.

        Args:
            path: S3 path (s3://bucket/key) to get metadata for.
            **kwargs: Additional parameters passed to the HeadObject API.

        Returns:
            S3Metadata, which behaves as a read-only mapping of the
            user-defined metadata (``x-amz-meta-*``) and exposes the
            system-defined metadata (content type, encryption settings,
            etc.) as typed properties.
        """
        bucket, key, version_id = self.parse_path(path)
        if not key:
            raise ValueError("Cannot get metadata of a bucket.")
        request: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if version_id:
            request.update({"VersionId": version_id})

        _logger.debug(f"Head object metadata: s3://{bucket}/{key}?versionId={version_id}")
        response = self._call(
            self._client.head_object,
            **request,
            **kwargs,
        )
        return S3Metadata(response)

    def getxattr(self, path: str, attr_name: str, **kwargs) -> str | None:
        """Get an attribute from the user-defined metadata of the path.

        Args:
            path: S3 path (s3://bucket/key) to get the attribute for.
            attr_name: The name of the attribute.
            **kwargs: Additional parameters passed to :meth:`metadata`.

        Returns:
            The value of the attribute, or None if the attribute is not set.
        """
        return self.metadata(path, **kwargs).get(attr_name)

    def setxattr(self, path: str, copy_kwargs: dict[str, Any] | None = None, **kw_args) -> None:
        """Set the user-defined metadata of the path.

        S3 does not allow updating the metadata of an existing object in
        place, so the object is copied onto itself with the REPLACE metadata
        directive. Note that this rewrites the object and updates its
        last-modified time. The system-defined metadata (e.g.,
        ``ContentType`` and ``CacheControl``), the storage class, and the
        server-side encryption algorithm and KMS key of the object are kept.
        HeadObject does not return the KMS encryption context, and an
        ``Expires`` value that is not a valid HTTP date is not kept.

        Args:
            path: S3 path (s3://bucket/key) to set metadata for. A path with
                a version ID is rejected, since copying a version onto the
                key would replace the current object with it.
            copy_kwargs: Additional parameters to use for the underlying
                CopyObject API call. They take precedence over the kept
                system-defined metadata and storage class. Any encryption
                parameter replaces all kept encryption settings.
            **kw_args: Key-value pairs to set, where the values must be
                strings. The keys are used as-is; names that are not valid
                Python identifiers (e.g., containing hyphens) can be passed
                by unpacking a dictionary. Does not alter existing fields,
                unless the field appears here - if the value is None, delete
                the field.

        Example:
            >>> fs = S3FileSystem()
            >>> fs.setxattr("s3://bucket/key", attribute1="value1")
            >>> fs.setxattr("s3://bucket/key", **{"attribute-2": "value2"})

        Raises:
            ValueError: If the path is a bucket or has a version ID.
        """
        bucket, key, version_id = self.parse_path(path)
        if not key:
            raise ValueError("Cannot set metadata of a bucket.")
        if version_id:
            raise ValueError("Cannot set metadata of a version.")
        head = self.metadata(path)
        metadata = dict(head)
        for k, v in kw_args.items():
            if v is None:
                metadata.pop(k, None)
            else:
                metadata[k] = v

        # With the REPLACE directive, S3 does not copy what the request
        # omits: the system-defined metadata is dropped, and the copy is
        # written as STANDARD with the default encryption of the bucket.
        kept: dict[str, Any] = {
            "CacheControl": head.cache_control,
            "ContentDisposition": head.content_disposition,
            "ContentEncoding": head.content_encoding,
            "ContentLanguage": head.content_language,
            "ContentType": head.content_type,
            "Expires": head.expires,
            "WebsiteRedirectLocation": head.website_redirect_location,
            "StorageClass": head.storage_class,
        }
        copy_kwargs = copy_kwargs if copy_kwargs else {}
        if not self._SSE_COPY_PARAMS.intersection(copy_kwargs):
            kept.update(
                {
                    "ServerSideEncryption": head.server_side_encryption,
                    "SSEKMSKeyId": head.sse_kms_key_id,
                    "BucketKeyEnabled": head.bucket_key_enabled,
                }
            )

        _logger.debug(f"Set object metadata: s3://{bucket}/{key}")
        self._call(
            self._client.copy_object,
            CopySource={"Bucket": bucket, "Key": key},
            Bucket=bucket,
            Key=key,
            Metadata=metadata,
            MetadataDirective="REPLACE",
            **{
                **{k: v for k, v in kept.items() if v is not None},
                **copy_kwargs,
            },
        )
        self.invalidate_cache(path)

    def get_tags(self, path: str) -> dict[str, str]:
        """Retrieve the tag key/values for the given path.

        Args:
            path: S3 path (s3://bucket/key) to get tags for.

        Returns:
            Dictionary mapping tag keys to tag values.
        """
        bucket, key, version_id = self.parse_path(path)
        if not key:
            raise ValueError("Cannot get tags of a bucket.")
        request: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if version_id:
            request.update({"VersionId": version_id})

        _logger.debug(f"Get object tagging: s3://{bucket}/{key}?versionId={version_id}")
        response = self._call(
            self._client.get_object_tagging,
            **request,
        )
        return {v["Key"]: v["Value"] for v in response["TagSet"]}

    def put_tags(self, path: str, tags: dict[str, str], mode: str = "o") -> None:
        """Set the tags for the given existing key.

        Tags are a str:str mapping that can be attached to any key, distinct
        from the user-defined metadata, which is usually set at key creation
        time. See
        https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html

        Args:
            path: S3 path (s3://bucket/key) of the existing key to attach
                tags to.
            tags: Tags to apply.
            mode: One of 'o' or 'm'. 'o' will over-write any existing tags.
                'm' will merge in new tags with existing tags, which incurs
                two remote calls.
        """
        bucket, key, version_id = self.parse_path(path)
        if not key:
            raise ValueError("Cannot put tags of a bucket.")
        if mode == "m":
            existing_tags = self.get_tags(path)
            existing_tags.update(tags)
            new_tags = [{"Key": k, "Value": v} for k, v in existing_tags.items()]
        elif mode == "o":
            new_tags = [{"Key": k, "Value": v} for k, v in tags.items()]
        else:
            raise ValueError(f"Mode must be {{'o', 'm'}}, not {mode}.")
        request: dict[str, Any] = {
            "Bucket": bucket,
            "Key": key,
            "Tagging": {"TagSet": new_tags},
        }
        if version_id:
            request.update({"VersionId": version_id})

        _logger.debug(f"Put object tagging: s3://{bucket}/{key}?versionId={version_id}")
        self._call(
            self._client.put_object_tagging,
            **request,
        )

    def chmod(self, path: str, acl: str, recursive: bool = False, **kwargs) -> None:
        """Set the Access Control on a bucket/key.

        See https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl

        Args:
            path: S3 path (s3://bucket or s3://bucket/key) to set the ACL on.
            acl: The value of the canned ACL to apply.
            recursive: Whether to apply the ACL to all keys below the given
                path too.
            **kwargs: Additional parameters passed to the PutObjectAcl or
                PutBucketAcl API.
        """
        bucket, key, version_id = self.parse_path(path)
        # Validate before any ACL is applied so that a recursive call cannot
        # partially apply object ACLs and then fail on the bucket ACL.
        if not key and acl not in self.BUCKET_ACLS:
            raise ValueError(f"ACL not in {self.BUCKET_ACLS}.")
        if key and acl not in self.OBJECT_ACLS:
            raise ValueError(f"ACL not in {self.OBJECT_ACLS}.")
        if recursive:
            with self._create_executor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(self.chmod, p, acl, recursive=False, **kwargs)
                    for p in self.find(path, withdirs=False)
                ]
                for future in as_completed(futures):
                    future.result()
            if key:
                # A key prefix is not an object itself; only the objects
                # below it have ACLs.
                return
        if key:
            request: dict[str, Any] = {"Bucket": bucket, "Key": key, "ACL": acl}
            if version_id:
                request.update({"VersionId": version_id})

            _logger.debug(f"Put object acl: s3://{bucket}/{key}?versionId={version_id}")
            self._call(
                self._client.put_object_acl,
                **request,
                **kwargs,
            )
        else:
            _logger.debug(f"Put bucket acl: s3://{bucket}")
            self._call(
                self._client.put_bucket_acl,
                Bucket=bucket,
                ACL=acl,
                **kwargs,
            )

    def list_multipart_uploads(self, path: str) -> list[S3MultipartUpload]:
        """List in-progress (incomplete) multipart uploads in a bucket.

        Incomplete multipart uploads continue to accrue storage costs until
        they are completed or aborted. Use :meth:`clear_multipart_uploads`
        to abort all of them.

        Args:
            path: S3 bucket or key path (e.g., "bucket", "s3://bucket" or
                "s3://bucket/prefix"). If the path contains a key, only the
                uploads to that key and to the keys under ``key/`` are
                listed, not those to sibling keys that merely start with the
                same characters (e.g., ``prefix2/a``).

        Returns:
            List of S3MultipartUpload instances describing the in-progress
            multipart uploads.
        """
        bucket, key, _ = self.parse_path(path)
        # S3 matches Prefix as a plain string, so the uploads are filtered to
        # the key itself and the keys under it.
        prefix = f"{key.rstrip('/')}/" if key else ""

        _logger.debug(f"List multipart uploads: s3://{bucket}/{key}")
        uploads: list[S3MultipartUpload] = []
        next_key_marker: str | None = None
        next_upload_id_marker: str | None = None
        while True:
            request: dict[str, Any] = {"Bucket": bucket}
            if key:
                request.update({"Prefix": key})
            if next_key_marker:
                request.update(
                    {"KeyMarker": next_key_marker, "UploadIdMarker": next_upload_id_marker}
                )
            response = self._call(
                self._client.list_multipart_uploads,
                **request,
            )
            uploads.extend(
                S3MultipartUpload({**u, "Bucket": bucket})
                for u in response.get("Uploads", [])
                if u["Key"] == key or u["Key"].startswith(prefix)
            )
            if not response.get("IsTruncated"):
                break
            next_key_marker = response.get("NextKeyMarker")
            next_upload_id_marker = response.get("NextUploadIdMarker")
            if not next_key_marker or not next_upload_id_marker:
                break
        return uploads

    def object_version_info(
        self, path: str, delete_markers: bool = False, **kwargs
    ) -> list[S3ObjectVersion]:
        """List the versions of the object or of the objects under the path.

        A key path without a trailing slash selects that key if it has any
        versions or delete markers, and otherwise the keys under ``key/``.
        The choice does not depend on ``delete_markers``, so a key that has
        only delete markers yields no versions without them. A key path with
        a trailing slash selects the keys under it, and a bucket path selects
        all the keys in the bucket. Sibling keys that merely start with the
        same characters (e.g., ``key.bak``) are never included.

        Args:
            path: S3 path (s3://bucket/key or a key prefix) to list the
                versions for.
            delete_markers: Whether to include delete markers in the result.
            **kwargs: Additional parameters passed to the ListObjectVersions
                API.

        Returns:
            List of S3ObjectVersion instances describing the versions.
        """
        bucket, key, _ = self.parse_path(path)
        # S3 matches Prefix as a plain string, so the versions are filtered to
        # the key itself or the keys under it.
        prefix = f"{key.rstrip('/')}/" if key else ""

        _logger.debug(f"List object versions: s3://{bucket}/{key}")
        versions: list[S3ObjectVersion] = []
        for response in self._list_object_versions_pages(bucket, prefix=key or "", **kwargs):
            versions.extend(
                S3ObjectVersion(bucket=bucket, is_delete_marker=False, response=v)
                for v in response.get("Versions", [])
            )
            # Delete markers are kept until the key is chosen, so that the
            # choice is the same with and without them.
            versions.extend(
                S3ObjectVersion(bucket=bucket, is_delete_marker=True, response=m)
                for m in response.get("DeleteMarkers", [])
            )
        # botocore decodes the keys only when it sets EncodingType itself, so
        # the keys of an explicit EncodingType="url" are decoded for matching.
        url_encoded = kwargs.get("EncodingType") == "url"
        keys = [unquote_plus(v.key) if url_encoded else v.key for v in versions]
        if key and not key.endswith("/") and key in keys:
            selected = [v for v, k in zip(versions, keys, strict=True) if k == key]
        else:
            selected = [v for v, k in zip(versions, keys, strict=True) if k.startswith(prefix)]
        return [v for v in selected if delete_markers or not v.is_delete_marker]

    def clear_multipart_uploads(self, path: str) -> None:
        """Abort any incomplete multipart uploads in the bucket.

        Args:
            path: S3 bucket or key path (e.g., "bucket", "s3://bucket" or
                "s3://bucket/prefix"). If the path contains a key, only the
                uploads to that key and to the keys under ``key/`` are
                aborted, as listed by :meth:`list_multipart_uploads`.
        """
        uploads = self.list_multipart_uploads(path)
        if not uploads:
            return
        with self._create_executor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(
                    self._call,
                    self._client.abort_multipart_upload,
                    Bucket=upload.bucket,
                    Key=upload.key,
                    UploadId=upload.upload_id,
                )
                for upload in uploads
            ]
            for future in as_completed(futures):
                future.result()

    def created(self, path: str) -> datetime:
        """Return the creation time of the path.

        Returns the same value as :meth:`modified`.

        Args:
            path: S3 path (s3://bucket/key).

        Returns:
            The last-modified time of the object.
        """
        return self.modified(path)

    def modified(self, path: str) -> datetime:
        """Return the last-modified time of the path.

        Args:
            path: S3 path (s3://bucket/key).

        Returns:
            The ``last_modified`` field from :meth:`info`, which is None for
            buckets and directories.
        """
        info = self.info(path)
        return cast(datetime, info.get("last_modified"))

    def invalidate_cache(self, path: str | None = None) -> None:
        """Remove the cached entries of the path and its parent paths.

        A version-qualified path invalidates the version under every query
        spelling that ``parse_path`` accepts, and also the object path without
        the version, because deleting or copying a version can change the
        current version of the object.

        Args:
            path: The path to invalidate. If None, clear the whole cache.
        """
        if path is None:
            self.dircache.clear()
        else:
            path = self._strip_protocol(path)
            while path:
                # parse_path does not accept "?" in keys, so it starts the
                # versionId query.
                base, _, query = path.partition("?")
                cache_paths = [path]
                if query:
                    version_id = query.partition("=")[2]
                    cache_paths.extend(
                        f"{base}?{name}={version_id}"
                        for name in ("versionId", "versionID", "versionid", "version_id")
                    )
                for cache_path in cache_paths:
                    # _ls_dirs caches listings under (path, delimiter).
                    for cache_key in (cache_path, (cache_path, "/"), (cache_path, "")):
                        self._evict_cache(cache_key)
                # A version-qualified path continues with the path without
                # the version.
                path = self._strip_protocol(base) if query else self._parent(path)

    def _evict_cache(self, key: str | tuple[str, str]) -> None:
        """Remove a dircache entry if it exists.

        ``DirCache.pop()`` reads and then deletes the entry, so it raises
        KeyError when another thread removes the same entry in between,
        such as the request threads of ``rm()`` invalidating a shared parent
        at once. A single ``del`` raises KeyError only when the entry is
        already gone, which this ignores.

        Args:
            key: The dircache key, a path or a ``(path, delimiter)`` listing
                key.
        """
        with contextlib.suppress(KeyError):
            del self.dircache[key]

    def _ls_from_cache(self, path: str) -> list[S3Object] | S3Object | None:
        """Check the dircache for a cached entry of the path.

        fsspec's implementation assumes every dircache value is a listing,
        but S3FileSystem also caches a single S3Object under the object's own
        path (HeadObject/HeadBucket results). Guard the parent lookup so that
        looking up a child path of a cached object does not fail, and fall
        through to the S3 API instead.
        """
        cache = self.dircache.get(path.rstrip("/"))
        if cache is not None:
            return cast("list[S3Object] | S3Object", cache)
        parent_cache = self.dircache.get(self._parent(path))
        if isinstance(parent_cache, list):
            files = [
                f
                for f in parent_cache
                if f["name"] == path
                or (
                    f["name"] == path.rstrip("/")
                    and f["type"] == S3ObjectType.S3_OBJECT_TYPE_DIRECTORY
                )
            ]
            if files:
                return files
            raise FileNotFoundError(path)
        return None

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        cache_type: str | None = None,
        autocommit: bool = True,
        cache_options: dict[Any, Any] | None = None,
        **kwargs,
    ) -> S3File:
        if block_size is None:
            block_size = self.default_block_size
        if cache_type is None:
            cache_type = self.default_cache_type
        max_workers = kwargs.pop("max_workers", self.max_workers)
        # The parameters of the call take precedence over those of the
        # filesystem; the caller's dictionary is not modified.
        s3_additional_kwargs = {
            **self.s3_additional_kwargs,
            **kwargs.pop("s3_additional_kwargs", {}),
        }

        return S3File(
            self,
            path,
            mode,
            max_workers=max_workers,
            executor=self._create_executor(max_workers=max_workers),
            block_size=block_size,
            cache_type=cache_type,
            autocommit=autocommit,
            cache_options=cache_options,
            s3_additional_kwargs=s3_additional_kwargs,
            **kwargs,
        )

    def _get_object(
        self,
        bucket: str,
        key: str,
        ranges: tuple[int, int | None] | None = None,
        version_id: str | None = None,
        **kwargs,
    ) -> tuple[int, bytes]:
        """Read an object or a byte range of it with GetObject.

        Args:
            bucket: The bucket name.
            key: The object key.
            ranges: The ``(start, end)`` byte range to read, with an exclusive
                end or ``None`` to read to the end of the object (the last
                ``-start`` bytes for a negative start), or ``None`` to read
                the whole object.
            version_id: The version ID to read, or ``None`` for the latest.
            **kwargs: Additional parameters passed to the GetObject API.

        Returns:
            Tuple of the start of the range as given (0 for the whole
            object) and the bytes read.

        Raises:
            ValueError: If the range is empty. S3 ignores a range whose last
                byte precedes its first byte and returns the whole object.
        """
        request = {"Bucket": bucket, "Key": key}
        if ranges:
            if ranges[1] is not None and ranges[0] >= ranges[1]:
                raise ValueError(f"Invalid empty range: {ranges}.")
            range_ = S3File._format_ranges(ranges)
            request.update({"Range": range_})
        else:
            ranges = (0, 0)
            range_ = "bytes=0-"
        if version_id:
            request.update({"VersionId": version_id})

        _logger.debug(f"Get object: s3://{bucket}/{key}?versionId={version_id}&range={range_}")
        response = self._call(
            self._client.get_object,
            # The fields of the request take precedence over inherited
            # parameters of the same name.
            **{**kwargs, **request},
        )
        return ranges[0], cast(bytes, response["Body"].read())

    def _put_object(self, bucket: str, key: str, body: bytes | None, **kwargs) -> S3PutObject:
        request: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if body:
            request.update({"Body": body})

        _logger.debug(f"Put object: s3://{bucket}/{key}")
        response = self._call(
            self._client.put_object,
            # The fields of the request take precedence over inherited
            # parameters of the same name.
            **{**kwargs, **request},
        )
        return S3PutObject(response)

    def _create_multipart_upload(self, bucket: str, key: str, **kwargs) -> S3MultipartUpload:
        request = {
            "Bucket": bucket,
            "Key": key,
        }

        _logger.debug(f"Create multipart upload to s3://{bucket}/{key}.")
        response = self._call(
            self._client.create_multipart_upload,
            # The fields of the request take precedence over inherited
            # parameters of the same name.
            **{**kwargs, **request},
        )
        return S3MultipartUpload(response)

    def _upload_part_copy(
        self,
        bucket: str,
        key: str,
        copy_source: str | dict[str, Any],
        upload_id: str,
        part_number: int,
        copy_source_ranges: tuple[int, int] | None = None,
        **kwargs,
    ) -> S3MultipartUploadPart:
        request = {
            "Bucket": bucket,
            "Key": key,
            "CopySource": copy_source,
            "UploadId": upload_id,
            "PartNumber": part_number,
        }
        if copy_source_ranges:
            range_ = S3File._format_ranges(copy_source_ranges)
            request.update({"CopySourceRange": range_})
        _logger.debug(
            f"Upload part copy from {copy_source} to s3://{bucket}/{key} as part {part_number}."
        )
        response = self._call(
            self._client.upload_part_copy,
            # The fields of the request take precedence over inherited
            # parameters of the same name.
            **{**kwargs, **request},
        )
        return S3MultipartUploadPart(part_number, response)

    def _upload_part(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        body: bytes,
        **kwargs,
    ) -> S3MultipartUploadPart:
        request = {
            "Bucket": bucket,
            "Key": key,
            "UploadId": upload_id,
            "PartNumber": part_number,
            "Body": body,
        }

        _logger.debug(f"Upload part of {upload_id} to s3://{bucket}/{key} as part {part_number}.")
        response = self._call(
            self._client.upload_part,
            # The fields of the request take precedence over inherited
            # parameters of the same name.
            **{**kwargs, **request},
        )
        return S3MultipartUploadPart(part_number, response)

    def _complete_multipart_upload(
        self, bucket: str, key: str, upload_id: str, parts: list[dict[str, Any]], **kwargs
    ) -> S3CompleteMultipartUpload:
        request = {
            "Bucket": bucket,
            "Key": key,
            "UploadId": upload_id,
            "MultipartUpload": {"Parts": parts},
        }

        _logger.debug(f"Complete multipart upload {upload_id} to s3://{bucket}/{key}.")
        response = self._call(
            self._client.complete_multipart_upload,
            # The fields of the request take precedence over inherited
            # parameters of the same name.
            **{**kwargs, **request},
        )
        return S3CompleteMultipartUpload(response)

    def _get_operation_kwargs(self, method: str, kwargs: Mapping[str, Any]) -> dict[str, Any]:
        """Select the parameters that an S3 operation accepts.

        Parameters that are inherited by several requests (the
        ``requester_pays`` parameter, ``s3_additional_kwargs``, or the
        parameters of a file or a multipart copy) are filtered by the input
        shape of each operation, so that, e.g., ``ServerSideEncryption`` for
        writes is not sent with GetObject.

        Args:
            method: The name of the client method, such as ``get_object``.
            kwargs: The parameters to select from.

        Returns:
            The parameters that the operation accepts. Empty for a method
            that is not an S3 API operation, such as
            ``generate_presigned_url``.
        """
        operation = self._client.meta.method_to_api_mapping.get(method)
        if not kwargs or operation is None:
            return {}
        members = self._client.meta.service_model.operation_model(operation).input_shape.members
        return {k: v for k, v in kwargs.items() if k in members}

    def _call(self, method: str | Callable[..., Any], **kwargs) -> dict[str, Any]:
        func = getattr(self._client, method) if isinstance(method, str) else method
        # The requester_pays parameter goes only to the operations that
        # accept it, and a parameter of the call takes precedence.
        request = (
            {**self._get_operation_kwargs(func.__name__, self.request_kwargs), **kwargs}
            if self.request_kwargs
            else kwargs
        )
        try:
            response = retry_api_call(func, config=self._retry_config, logger=_logger, **request)
        except botocore.exceptions.ClientError as e:
            raise S3ClientError(e).os_error from e
        return cast(dict[str, Any], response)


class S3File(AbstractBufferedFile):
    """A buffered file object for reading and writing an S3 object.

    Instances are returned by ``S3FileSystem.open()``.
    """

    fs: S3FileSystem
    buffer: BytesIO | None

    def __init__(
        self,
        fs: S3FileSystem,
        path: str,
        mode: str = "rb",
        version_id: str | None = None,
        max_workers: int = (cpu_count() or 1) * 5,
        executor: S3Executor | None = None,
        block_size: int = S3FileSystem.DEFAULT_BLOCK_SIZE,
        cache_type: str = "bytes",
        autocommit: bool = True,
        cache_options: dict[Any, Any] | None = None,
        size: int | None = None,
        s3_additional_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """Initialize the file for the path and mode.

        In read mode, the object is looked up with ``info()`` and the reads
        are made conditional on its ETag (``IfMatch``). In append mode, an
        existing object smaller than ``MULTIPART_UPLOAD_MIN_PART_SIZE`` is
        read into the write buffer; a larger one is copied with
        ``UploadPartCopy`` as the first parts of a multipart upload, whatever
        the block size. In exclusive-create mode, the object must not exist
        when the file is opened, and the upload is committed with
        ``IfNoneMatch="*"`` so that it does not replace an object created in
        the meantime.

        Args:
            fs: The filesystem that the file belongs to.
            path: S3 path (s3://bucket/key) of the file.
            mode: The file mode: ``rb``, ``wb``, ``ab``, or ``xb``.
            version_id: The version ID to read. Must match the version ID in
                the path if both are given. A version cannot be given, in
                either form, for writing or appending.
            max_workers: The number of parallel workers for range reads and
                part copies.
            executor: The executor for parallel operations. If None, a new
                ``S3ThreadPoolExecutor`` is created.
            block_size: The block size for reads and writes. Must be between
                ``MULTIPART_UPLOAD_MIN_PART_SIZE`` and
                ``MULTIPART_UPLOAD_MAX_PART_SIZE``, inclusive, unless reading.
            cache_type: The fsspec cache type for reads.
            autocommit: Whether to commit the written data when the file is
                closed. If False, :meth:`commit` must be called.
            cache_options: Options for the fsspec cache.
            size: The size of the object, if known. Passed to
                ``fsspec.spec.AbstractBufferedFile``.
            s3_additional_kwargs: Additional parameters for the S3 requests of
                the file, such as ``ContentType`` or ``RequestPayer``. Each
                request receives those that its operation accepts.
            **kwargs: Additional parameters for the S3 requests of the file,
                which take precedence over ``s3_additional_kwargs``.

        Raises:
            FileExistsError: If an object exists at the path in
                exclusive-create mode.
            FileNotFoundError: If no object exists at the path when reading,
                including when the path is a prefix.
            ValueError: If the path has no key, the version IDs do not match,
                a version is given for writing, or the block size is not
                between ``MULTIPART_UPLOAD_MIN_PART_SIZE`` and
                ``MULTIPART_UPLOAD_MAX_PART_SIZE`` for writing.
        """
        self.max_workers = max_workers
        # A new dictionary, so that the caller's is not modified.
        self.s3_additional_kwargs: dict[str, Any] = {**(s3_additional_kwargs or {}), **kwargs}

        # The arguments are validated, and the objects looked up, before the
        # base class initializer: a file that fails here is never opened, so
        # its garbage collection does not close (flush and commit) it.
        bucket, key, path_version_id = S3FileSystem.parse_path(path)
        self.bucket = bucket
        if not key:
            raise ValueError("The path does not contain a key.")
        self.key = key
        if version_id and path_version_id:
            if version_id != path_version_id:
                raise ValueError(
                    f"The version_id: {version_id} specified in the argument and "
                    f"the version_id: {path_version_id} specified in the path do not match."
                )
            self.version_id: str | None = version_id
        elif path_version_id:
            self.version_id = path_version_id
        else:
            self.version_id = version_id
        if self.version_id and "r" not in mode:
            raise ValueError("Cannot write to the file with the version specified.")
        if self.version_id and not path_version_id:
            # Carry the version in the path, as with the ?versionId= suffix,
            # so that a reopened (e.g., unpickled) file reads the same version.
            path = f"{path}?versionId={self.version_id}"
        if "r" not in mode and not (
            fs.MULTIPART_UPLOAD_MIN_PART_SIZE <= block_size <= fs.MULTIPART_UPLOAD_MAX_PART_SIZE
        ):
            # When writing, every full block is uploaded as a part of a
            # multipart upload.
            raise ValueError(
                "Block size for writing must be between "
                f"5 MiB ({fs.MULTIPART_UPLOAD_MIN_PART_SIZE} bytes) and "
                f"5 GiB ({fs.MULTIPART_UPLOAD_MAX_PART_SIZE} bytes), inclusive: {block_size}."
            )

        self._details: S3Object | dict[str, Any] = {}
        append_info: S3Object | None = None
        append_data: bytes | None = None
        if "r" in mode:
            # Looked up before the base class initializer, which would
            # otherwise take the size from the latest version of the object.
            info = fs.info(path, version_id=self.version_id)
            if info.get("type") == S3ObjectType.S3_OBJECT_TYPE_DIRECTORY:
                # A prefix has no object to read.
                raise FileNotFoundError(path)
            if fs.version_aware and not self.version_id:
                # Pin the version observed at open time so that reads are
                # consistent even if the object is overwritten. info() heads
                # the object when the cached entry carries no version.
                self.version_id = info.get("version_id")
            if etag := info.get("etag"):
                self.s3_additional_kwargs.update({"IfMatch": etag})
            self._details = info
            if size is None:
                size = info.get("size")
        elif "a" in mode and fs.exists(path):
            append_info = fs.info(path)
            if append_info.get("size", 0) < fs.MULTIPART_UPLOAD_MIN_PART_SIZE:
                # Too small to be a part of a multipart upload: rewritten
                # from the buffer.
                append_data = fs.cat(path)
        elif "x" in mode:
            # Checked up front so that no data is uploaded for an existing
            # object, and on commit with IfNoneMatch for one created since.
            if fs.exists(path):
                raise FileExistsError(path)
            self.s3_additional_kwargs.update({"IfNoneMatch": "*"})

        self._executor: S3Executor = executor or S3ThreadPoolExecutor(max_workers=max_workers)
        super().__init__(
            fs=fs,
            path=path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            size=size,
        )

        self.append_block = False
        self.multipart_upload: S3MultipartUpload | None = None
        self.multipart_upload_parts: list[Future[S3MultipartUploadPart]] = []
        if append_info is not None:
            if append_data is not None:
                self.write(append_data)
            else:
                # Copied with UploadPartCopy as the leading part(s).
                self.append_block = True
            self.loc = append_info.get("size", 0)
            self.s3_additional_kwargs.update(append_info.to_api_repr())
            self._details = append_info

    def _get_request_kwargs(self, method: str) -> dict[str, Any]:
        """Select the parameters of the file that an S3 operation accepts.

        Args:
            method: The name of the client method, such as ``upload_part``.

        Returns:
            The parameters in ``s3_additional_kwargs`` that the operation
            accepts.
        """
        return self.fs._get_operation_kwargs(method, self.s3_additional_kwargs)

    def close(self) -> None:
        """Close the file, flushing any written data, and shut down its executor."""
        try:
            super().close()
        finally:
            # The executor is shut down even if the final flush fails.
            self._executor.shutdown()

    def _close_without_commit(self) -> None:
        """Close the file without uploading the written data.

        Drops the buffered data, so that neither close() nor a deferred
        commit() uploads it, and aborts the multipart upload, if any. An
        abort failure is logged instead of raised, so it does not mask the
        error that the caller is handling. Even if the abort fails or is
        interrupted, commit() does not complete the upload afterwards. The
        executor is shut down here, as fsspec does not close a closed file
        again when it is garbage collected.
        """
        self.buffer = None
        self.closed = True
        try:
            self.discard()
        except Exception:
            _logger.exception(f"Failed to abort multipart upload to s3://{self.bucket}/{self.key}.")
        finally:
            self.multipart_upload = None
            self.multipart_upload_parts = []
            self._executor.shutdown()

    def _initiate_upload(self) -> None:
        if not self.append_block and self.tell() < self.blocksize:
            # Files smaller than block size in size cannot be multipart uploaded.
            # An append to an object copied with UploadPartCopy always uses
            # a multipart upload, whatever the block size.
            return

        self.multipart_upload = self.fs._create_multipart_upload(
            bucket=self.bucket,
            key=self.key,
            **self._get_request_kwargs("create_multipart_upload"),
        )
        if self.append_block:
            if self.tell() > self.fs.MULTIPART_UPLOAD_MAX_PART_SIZE:
                info = self.fs.info(self.path, version_id=self.version_id)
                ranges = self.fs._get_copy_ranges(
                    # Set copy source file byte size
                    info.get("size", 0),
                    self.fs.MULTIPART_UPLOAD_MAX_PART_SIZE,
                )
                for i, range_ in enumerate(ranges):
                    self.multipart_upload_parts.append(
                        self._executor.submit(
                            self.fs._upload_part_copy,
                            bucket=self.bucket,
                            key=self.key,
                            copy_source=self.path,
                            upload_id=cast(str, self.multipart_upload.upload_id),
                            part_number=i + 1,
                            copy_source_ranges=range_,
                            **self._get_request_kwargs("upload_part_copy"),
                        )
                    )
            else:
                self.multipart_upload_parts.append(
                    self._executor.submit(
                        self.fs._upload_part_copy,
                        bucket=self.bucket,
                        key=self.key,
                        copy_source=self.path,
                        upload_id=cast(str, self.multipart_upload.upload_id),
                        part_number=1,
                        **self._get_request_kwargs("upload_part_copy"),
                    )
                )

    def _upload_chunk(self, final: bool = False) -> bool:
        # The return value controls whether fsspec's flush() resets self.buffer
        # afterwards: it does so only when this returns a value other than False.
        # Returning ``not final`` keeps the buffer intact on the final flush so a
        # deferred commit() (autocommit=False, i.e. inside an fsspec transaction)
        # can still read the bytes; resetting it there would upload an empty
        # object for small files. Mid-stream chunks (final=False) return True so
        # fsspec clears the already-uploaded buffer between parts.
        if not self.append_block and self.tell() < self.blocksize:
            # Files smaller than block size in size cannot be multipart uploaded.
            if self.autocommit and final:
                self.commit()
            return not final

        if not self.multipart_upload:
            raise RuntimeError("Multipart upload is not initialized.")

        # fsspec's flush() never calls this on a closed file, whose buffer
        # may have been dropped.
        buffer = cast(BytesIO, self.buffer)
        part_number = len(self.multipart_upload_parts)
        buffer.seek(0)
        data = buffer.read(self.blocksize)
        while data:
            # Only the last part of a multipart upload may be smaller than the
            # minimum part size, and more data may follow a mid-stream chunk.
            # A single write() can leave several blocks in the buffer, so look
            # ahead one block and merge a short last block into this one.
            next_data = buffer.read(self.blocksize)
            next_data_size = len(next_data)
            if 0 < next_data_size < self.fs.MULTIPART_UPLOAD_MIN_PART_SIZE:
                upload_data = data + next_data
                upload_data_size = len(upload_data)
                if upload_data_size < self.fs.MULTIPART_UPLOAD_MAX_PART_SIZE:
                    uploads = [upload_data]
                else:
                    split_size = upload_data_size // 2
                    uploads = [upload_data[:split_size], upload_data[split_size:]]
                next_data = b""
            else:
                uploads = [data]

            for upload in uploads:
                if part_number >= self.fs.MULTIPART_UPLOAD_MAX_PARTS:
                    self._close_without_commit()
                    raise ValueError(
                        f"Cannot upload more than {self.fs.MULTIPART_UPLOAD_MAX_PARTS} "
                        f"parts to s3://{self.bucket}/{self.key} with a block size of "
                        f"{self.blocksize} bytes. Write the file with a block_size, or "
                        "a default_block_size of the filesystem, large enough for it to "
                        f"fit in {self.fs.MULTIPART_UPLOAD_MAX_PARTS} parts, including "
                        "the parts copied from the existing object in an append."
                    )
                part_number += 1
                self.multipart_upload_parts.append(
                    self._executor.submit(
                        self.fs._upload_part,
                        bucket=self.bucket,
                        key=self.key,
                        upload_id=cast(str, self.multipart_upload.upload_id),
                        part_number=part_number,
                        body=upload,
                        **self._get_request_kwargs("upload_part"),
                    )
                )

            data = next_data

        if self.autocommit and final:
            self.commit()
        return not final

    def commit(self) -> None:
        """Complete the upload of the written data.

        Creates an empty object if nothing was written, uploads the buffered
        data with PutObject if no multipart upload part was submitted, and
        otherwise completes the multipart upload, which is aborted if the
        completion fails. Invalidates the cache of the path afterwards.

        Raises:
            FileExistsError: If an object was created at the path after the
                file was opened in exclusive-create mode.
            RuntimeError: If parts were submitted but no multipart upload is
                initialized.
        """
        if self.tell() == 0:
            if self.buffer is not None:
                self.discard()
                self.fs.touch(self.path, **self._get_request_kwargs("put_object"))
        elif not self.multipart_upload_parts:
            if self.buffer is not None:
                # Upload files smaller than block size.
                self.buffer.seek(0)
                data = self.buffer.read()
                self.fs._put_object(
                    bucket=self.bucket,
                    key=self.key,
                    body=data,
                    **self._get_request_kwargs("put_object"),
                )
        else:
            if not self.multipart_upload:
                raise RuntimeError("Multipart upload is not initialized.")

            try:
                self.fs._finish_multipart_upload(
                    bucket=self.bucket,
                    key=self.key,
                    upload_id=cast(str, self.multipart_upload.upload_id),
                    futures=self.multipart_upload_parts,
                    request_kwargs=self.s3_additional_kwargs,
                )
            except Exception:
                # The multipart upload has been aborted by the helper;
                # prevent discard() from aborting it again.
                self.multipart_upload = None
                self.multipart_upload_parts = []
                raise

        self.fs.invalidate_cache(self.path)

    def discard(self) -> None:
        """Abort the multipart upload, if any.

        The part uploads that have not started are cancelled, and the
        running ones are waited for before the abort.
        """
        if self.multipart_upload:
            # A part that is still uploading when the upload is aborted may
            # be stored after the abort, so wait for the parts that could not
            # be cancelled first.
            wait([f for f in self.multipart_upload_parts if not f.cancel()])
            self.fs._call(
                "abort_multipart_upload",
                **{
                    **self._get_request_kwargs("abort_multipart_upload"),
                    "Bucket": self.bucket,
                    "Key": self.key,
                    "UploadId": self.multipart_upload.upload_id,
                },
            )

        self.multipart_upload = None
        self.multipart_upload_parts = []

    def url(self, expiration: int = 3600, **kwargs) -> str:
        """Generate a presigned HTTP URL to read this file (if it already exists).

        Args:
            expiration: URL expiration time in seconds. Defaults to 3600 (1 hour).
            **kwargs: Additional parameters passed to :meth:`S3FileSystem.sign`.

        Returns:
            Presigned URL string that provides temporary access to the S3 object.
        """
        return cast(str, self.fs.sign(self.path, expiration=expiration, **kwargs))

    def metadata(self, **kwargs) -> S3Metadata:
        """Return the metadata of the file.

        See :meth:`S3FileSystem.metadata`.

        Args:
            **kwargs: Additional parameters passed to the HeadObject API.

        Returns:
            S3Metadata, which behaves as a read-only mapping of the
            user-defined metadata and exposes the system-defined metadata
            as typed properties.
        """
        return self.fs.metadata(self.path, **kwargs)

    def getxattr(self, xattr_name: str, **kwargs) -> str | None:
        """Get an attribute from the user-defined metadata of the file.

        See :meth:`S3FileSystem.getxattr`.

        Args:
            xattr_name: The name of the attribute.
            **kwargs: Additional parameters passed to the HeadObject API.

        Returns:
            The value of the attribute, or None if the attribute is not set.
        """
        return self.fs.getxattr(self.path, xattr_name, **kwargs)

    def setxattr(self, copy_kwargs: dict[str, Any] | None = None, **kwargs) -> None:
        """Set the user-defined metadata of the file.

        See :meth:`S3FileSystem.setxattr`.

        Args:
            copy_kwargs: Additional parameters to use for the underlying
                CopyObject API call.
            **kwargs: Key-value pairs of metadata to set.
        """
        if self.writable():
            raise NotImplementedError("Cannot update metadata while the file is open for writing.")
        self.fs.setxattr(self.path, copy_kwargs=copy_kwargs, **kwargs)

    def _fetch_range(self, start: int, end: int) -> bytes:
        """Read a byte range of the object for the fsspec cache.

        The range is clamped to the size of the object, since fsspec caches
        may request a range that is empty or reaches past the end of the
        object. S3 would answer the former with the whole object and a range
        starting past the end with an ``InvalidRange`` error.

        Args:
            start: The offset of the first byte to read.
            end: The offset to stop reading at (exclusive).

        Returns:
            The bytes read, empty if the clamped range is empty.
        """
        end = min(end, self.size)
        if start >= end:
            return b""
        ranges = self._get_ranges(
            start, end, max_workers=self.max_workers, worker_block_size=self.blocksize
        )
        if len(ranges) > 1:
            futures = [
                self._executor.submit(
                    self.fs._get_object,
                    bucket=self.bucket,
                    key=self.key,
                    ranges=r,
                    version_id=self.version_id,
                    **self._get_request_kwargs("get_object"),
                )
                for r in ranges
            ]
            object_ = self._merge_objects([f.result() for f in as_completed(futures)])
        else:
            object_ = self.fs._get_object(
                self.bucket,
                self.key,
                ranges[0],
                self.version_id,
                **self._get_request_kwargs("get_object"),
            )[1]
        return object_

    @staticmethod
    def _format_ranges(ranges: tuple[int, int | None]) -> str:
        """Format a byte range as the value of an HTTP ``Range`` header.

        Args:
            ranges: The ``(start, end)`` byte range, with an exclusive end or
                ``None`` for the end of the object. A negative start with no
                end selects the last ``-start`` bytes.

        Returns:
            The range, such as ``bytes=0-99``, ``bytes=100-`` or ``bytes=-8``.
        """
        start, end = ranges
        if end is None:
            return f"bytes={start}" if start < 0 else f"bytes={start}-"
        return f"bytes={start}-{end - 1}"

    @staticmethod
    def _get_ranges(
        start: int, end: int, max_workers: int, worker_block_size: int
    ) -> list[tuple[int, int]]:
        ranges = []
        range_size = end - start
        if max_workers > 1 and range_size > worker_block_size:
            range_start = start
            while True:
                range_end = range_start + worker_block_size
                if range_end >= end:
                    # Also when the size is an exact multiple of the block
                    # size, so that no empty trailing range is generated.
                    ranges.append((range_start, end))
                    break
                ranges.append((range_start, range_end))
                range_start += worker_block_size
        else:
            ranges.append((start, end))
        return ranges

    @staticmethod
    def _merge_objects(objects: list[tuple[int, bytes]]) -> bytes:
        objects.sort(key=lambda x: x[0])
        return b"".join([obj for start, obj in objects])
