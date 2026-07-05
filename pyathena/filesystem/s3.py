from __future__ import annotations

import logging
import mimetypes
import os.path
import re
from collections.abc import Callable
from concurrent.futures import Future, as_completed
from copy import deepcopy
from datetime import datetime
from multiprocessing import cpu_count
from re import Pattern
from typing import TYPE_CHECKING, Any, cast

import botocore.exceptions
from boto3 import Session
from botocore import UNSIGNED
from botocore.client import BaseClient, Config
from fsspec import AbstractFileSystem
from fsspec.callbacks import _DEFAULT_CALLBACK
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import tokenize

import pyathena
from pyathena.filesystem.s3_errors import S3ClientError
from pyathena.filesystem.s3_executor import S3Executor, S3ThreadPoolExecutor
from pyathena.filesystem.s3_object import (
    S3CompleteMultipartUpload,
    S3MultipartUpload,
    S3MultipartUploadPart,
    S3Object,
    S3ObjectType,
    S3PutObject,
    S3StorageClass,
)
from pyathena.util import RetryConfig, retry_api_call

if TYPE_CHECKING:
    from pyathena.connection import Connection

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
    - Creating and removing directories
    - Multipart uploads for large files
    - Various S3 storage classes and encryption options

    Attributes:
        session: The boto3 session used for S3 operations.
        client: The S3 client for direct API calls.
        config: Boto3 configuration for the client.
        retry_config: Configuration for retry behavior on failed operations.

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
        *args,
        **kwargs,
    ) -> None:
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

        requester_pays = kwargs.pop("requester_pays", False)
        self.request_kwargs = {"RequestPayer": "requester"} if requester_pays else {}

    def _get_client_compatible_with_s3fs(self, **kwargs) -> BaseClient:
        """
        https://github.com/fsspec/s3fs/blob/2023.4.0/s3fs/core.py#L457-L535
        """
        from pyathena.connection import Connection

        config_kwargs = deepcopy(kwargs.pop("config_kwargs", {}))
        user_agent_extra = config_kwargs.pop("user_agent_extra", None)
        if user_agent_extra:
            if pyathena.user_agent_extra not in user_agent_extra:
                config_kwargs.update(
                    {"user_agent_extra": f"{pyathena.user_agent_extra} {user_agent_extra}"}
                )
            else:
                config_kwargs.update({"user_agent_extra": user_agent_extra})
        else:
            config_kwargs.update({"user_agent_extra": pyathena.user_agent_extra})
        connect_timeout = kwargs.pop("connect_timeout", None)
        if connect_timeout:
            config_kwargs.update({"connect_timeout": connect_timeout})
        read_timeout = kwargs.pop("read_timeout", None)
        if read_timeout:
            config_kwargs.update({"read_timeout": read_timeout})

        client_kwargs = deepcopy(kwargs.pop("client_kwargs", {}))
        use_ssl = kwargs.pop("use_ssl", None)
        if use_ssl:
            client_kwargs.update({"use_ssl": use_ssl})
        endpoint_url = kwargs.pop("endpoint_url", None)
        if endpoint_url:
            client_kwargs.update({"endpoint_url": endpoint_url})
        anon = kwargs.pop("anon", False)
        if anon:
            config_kwargs.update({"signature_version": UNSIGNED})
        else:
            creds = {
                "aws_access_key_id": kwargs.pop("key", kwargs.pop("username", None)),
                "aws_secret_access_key": kwargs.pop("secret", kwargs.pop("password", None)),
                "aws_session_token": kwargs.pop("token", None),
            }
            kwargs.update(**creds)
            client_kwargs.update(**creds)

        config = Config(**config_kwargs)
        session = Session(
            **{k: v for k, v in kwargs.items() if k in Connection._SESSION_PASSING_ARGS}
        )
        return session.client(
            "s3",
            config=config,
            **{k: v for k, v in client_kwargs.items() if k in Connection._CLIENT_PASSING_ARGS},
        )

    @staticmethod
    def parse_path(path: str) -> tuple[str, str | None, str | None]:
        match = S3FileSystem.PATTERN_PATH.search(path)
        if match:
            return match.group("bucket"), match.group("key"), match.group("version_id")
        raise ValueError(f"Invalid S3 path format {path}.")

    def _head_bucket(self, bucket, refresh: bool = False) -> S3Object | None:
        if bucket not in self.dircache or refresh:
            try:
                self._call(
                    self._client.head_bucket,
                    Bucket=bucket,
                )
            except FileNotFoundError:
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
        else:
            file = self.dircache[bucket]
        return file

    def _head_object(
        self, path: str, version_id: str | None = None, refresh: bool = False
    ) -> S3Object | None:
        bucket, key, path_version_id = self.parse_path(path)
        version_id = path_version_id if path_version_id else version_id
        if path not in self.dircache or refresh:
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
                return None
            file = S3Object(
                init=response,
                type=S3ObjectType.S3_OBJECT_TYPE_FILE,
                bucket=bucket,
                key=key,
                version_id=version_id,
            )
            self.dircache[path] = file
        else:
            file = self.dircache[path]
        return file

    def _ls_buckets(self, refresh: bool = False) -> list[S3Object]:
        if "" not in self.dircache or refresh:
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
        else:
            buckets = self.dircache[""]
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
        bucket, key, version_id = self.parse_path(path)
        if key:
            prefix = f"{key}/{prefix if prefix else ''}"

        # Create a cache key that includes the delimiter
        cache_key = (path, delimiter)
        if cache_key in self.dircache and not refresh:
            return cast(list[S3Object], self.dircache[cache_key])

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
                S3Object(
                    init={
                        "ContentLength": 0,
                        "ContentType": None,
                        "StorageClass": S3StorageClass.S3_STORAGE_CLASS_DIRECTORY,
                        "ETag": None,
                        "LastModified": None,
                    },
                    type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                    bucket=bucket,
                    key=c["Prefix"][:-1].rstrip("/"),
                    version_id=version_id,
                )
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
        if files:
            self.dircache[cache_key] = files
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
            **kwargs: Additional arguments (ignored for S3).

        Returns:
            List of S3Object instances (if detail=True) or paths as strings (if detail=False).

        Example:
            >>> fs = S3FileSystem()
            >>> fs.ls("s3://my-bucket")  # List objects in bucket
            >>> fs.ls("s3://my-bucket/", detail=True)  # Get detailed object info
        """
        path = self._strip_protocol(path).rstrip("/")
        if path in ["", "/"]:
            files = self._ls_buckets(refresh)
        else:
            files = self._ls_dirs(path, refresh=refresh)
            if not files and "/" in path:
                file = self._head_object(path, refresh=refresh)
                if file:
                    files = [file]
        return list(files) if detail else [f.name for f in files]

    def info(self, path: str, **kwargs) -> S3Object:
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
        if not refresh:
            caches: list[S3Object] | S3Object | None = self._ls_from_cache(path)
            if caches is not None:
                if isinstance(caches, list):
                    cache = next((c for c in caches if c.name == path), None)
                elif caches.name == path:
                    cache = caches
                else:
                    cache = None

                if cache:
                    return cache
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
                    key=key.rstrip("/") if key else None,
                    version_id=version_id,
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
                key=key.rstrip("/") if key else None,
                version_id=version_id,
            )
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

        # Create S3Object instances for directories
        directory_objects = []
        for dir_path in dirs:
            dir_obj = S3Object(
                init={
                    "ContentLength": 0,
                    "ContentType": None,
                    "StorageClass": S3StorageClass.S3_STORAGE_CLASS_DIRECTORY,
                    "ETag": None,
                    "LastModified": None,
                },
                type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                bucket=bucket,
                key=dir_path,
                version_id=None,
            )
            directory_objects.append(dir_obj)

        return directory_objects

    def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool | None = None,
        **kwargs,
    ) -> list[S3Object]:
        path = self._strip_protocol(path)
        if path in ["", "/"]:
            raise ValueError("Cannot traverse all files in S3.")
        bucket, key, _ = self.parse_path(path)
        prefix = kwargs.pop("prefix", "")

        # When maxdepth is specified, use a recursive approach with delimiter
        if maxdepth is not None:
            result: list[S3Object] = []

            # List files and directories at current level
            current_items = self._ls_dirs(path, prefix=prefix, delimiter="/")

            for item in current_items:
                if item.type == S3ObjectType.S3_OBJECT_TYPE_FILE:
                    # Add files
                    result.append(item)
                elif item.type == S3ObjectType.S3_OBJECT_TYPE_DIRECTORY:
                    # Add directory if withdirs is True
                    if withdirs:
                        result.append(item)

                    # Recursively explore subdirectory if depth allows
                    if maxdepth > 0:
                        sub_path = f"s3://{bucket}/{item.key}"
                        sub_results = self._find(
                            sub_path, maxdepth=maxdepth - 1, withdirs=withdirs, **kwargs
                        )
                        result.extend(sub_results)

            return result

        # For unlimited depth, use the original approach (get all files at once)
        files = self._ls_dirs(path, prefix=prefix, delimiter="")
        if not files and key:
            try:
                files = [self.info(path)]
            except FileNotFoundError:
                files = []

        # If withdirs is True, we need to derive directories from file paths
        if withdirs:
            files.extend(self._extract_parent_directories(files, bucket, key))

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
            maxdepth: Maximum depth to recurse (None for unlimited).
            withdirs: Whether to include directories in results (None = default behavior).
            detail: If True, return dict of {path: S3Object}; if False, return list of paths.
            **kwargs: Additional arguments.

        Returns:
            Dictionary mapping paths to S3Objects (if detail=True) or
            list of paths (if detail=False).

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
            **kwargs: Additional arguments (unused).

        Returns:
            True if the path exists, False otherwise.

        Example:
            >>> fs = S3FileSystem()
            >>> fs.exists("s3://my-bucket/file.txt")
            >>> fs.exists("s3://my-bucket/")
        """
        path = self._strip_protocol(path)
        if path in ["", "/"]:
            # The root always exists.
            return True
        bucket, key, _ = self.parse_path(path)
        if key:
            try:
                if self._ls_from_cache(path):
                    return True
                info = self.info(path)
                return bool(info)
            except FileNotFoundError:
                return False
        elif self.dircache.get(bucket, False):
            return True
        else:
            try:
                if self._ls_from_cache(bucket):
                    return True
            except FileNotFoundError:
                pass
            file = self._head_bucket(bucket)
            return bool(file)

    def rm_file(self, path: str, **kwargs) -> None:
        bucket, key, version_id = self.parse_path(path)
        if not key:
            return
        self._delete_object(bucket=bucket, key=key, version_id=version_id, **kwargs)
        self.invalidate_cache(path)

    def rm(self, path, recursive=False, maxdepth=None, **kwargs) -> None:
        bucket, key, version_id = self.parse_path(path)
        if not key:
            raise ValueError("Cannot delete the bucket.")

        expand_path = self.expand_path(path, recursive=recursive, maxdepth=maxdepth)
        self._delete_objects(bucket, expand_path, **kwargs)
        for p in expand_path:
            self.invalidate_cache(p)

    def _delete_object(
        self, bucket: str, key: str, version_id: str | None = None, **kwargs
    ) -> None:
        request = {
            "Bucket": bucket,
            "Key": key,
        }
        if version_id:
            request.update({"VersionId": version_id})

        _logger.debug("Delete object: s3://%s/%s?versionId=%s", bucket, key, version_id)
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

    def _delete_objects(
        self, bucket: str, paths: list[str], max_workers: int | None = None, **kwargs
    ) -> None:
        if not paths:
            return

        max_workers = max_workers if max_workers else self.max_workers
        quiet = kwargs.pop("Quiet", True)
        delete_objects = []
        for p in paths:
            bucket, key, version_id = self.parse_path(p)
            if key:
                object_ = {"Key": key}
                if version_id:
                    object_.update({"VersionId": version_id})
                delete_objects.append(object_)

        with self._create_executor(max_workers=max_workers) as executor:
            fs = []
            for delete in [
                delete_objects[i : i + self.DELETE_OBJECTS_MAX_KEYS]
                for i in range(0, len(delete_objects), self.DELETE_OBJECTS_MAX_KEYS)
            ]:
                request = {
                    "Bucket": bucket,
                    "Delete": {
                        "Objects": delete,
                        "Quiet": quiet,
                    },
                }
                fs.append(
                    executor.submit(self._call, self._client.delete_objects, **request, **kwargs)
                )
            for f in as_completed(fs):
                f.result()

    def mkdir(self, path: str, create_parents: bool = True, **kwargs) -> None:
        """Create an S3 bucket.

        S3 has no real directories below the bucket level; creating a key
        prefix requires no operation. This method creates the bucket when the
        path points at a bucket (or when ``create_parents`` is True and the
        bucket does not exist yet), and does nothing for key prefixes under
        an existing bucket.

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

            _logger.debug("Create bucket: s3://%s", bucket)
            try:
                self._call(
                    self._client.create_bucket,
                    **request,
                )
            except botocore.exceptions.ParamValidationError as e:
                raise ValueError(f"Bucket create failed {bucket!r}: {e}") from e
            # invalidate_cache walks parent paths and never pops the root
            # entry itself, so evict the cached bucket listing directly.
            self.dircache.pop("", None)
            self.invalidate_cache(bucket)
        else:
            # exists() has already confirmed the bucket does not exist,
            # and it is not requested to be created.
            raise FileNotFoundError(bucket)

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Recursively create a directory, creating the bucket if necessary.

        Args:
            path: S3 path (e.g., "s3://bucket" or "s3://bucket/prefix").
            exist_ok: If False, raise FileExistsError when the path is a
                bucket that already exists.
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

        Args:
            path: S3 bucket path (e.g., "s3://bucket").

        Raises:
            FileExistsError: If the path contains a key that exists. The user
                may have meant ``rm(path, recursive=True)``.
            FileNotFoundError: If the path contains a key that does not exist,
                or the bucket does not exist.
            OSError: If the bucket is not empty.
        """
        path = self._strip_protocol(path).rstrip("/")
        bucket, key, _ = self.parse_path(path)
        if key:
            if self.exists(path):
                # The user may have meant rm(path, recursive=True).
                raise FileExistsError(path)
            raise FileNotFoundError(path)

        _logger.debug("Delete bucket: s3://%s", bucket)
        self._call(
            self._client.delete_bucket,
            Bucket=bucket,
        )
        self.invalidate_cache(bucket)
        # invalidate_cache walks parent paths and never pops the root
        # entry itself, so evict the cached bucket listing directly.
        self.dircache.pop("", None)

    def touch(self, path: str, truncate: bool = True, **kwargs) -> dict[str, Any]:
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
            **kwargs: Additional S3 copy parameters (e.g., metadata, storage class).

        Raises:
            ValueError: If trying to copy to a versioned file or copy buckets.

        Note:
            Uses multipart copy for objects larger than the maximum part size
            to optimize performance for large files. The copy operation is
            performed entirely on the S3 service without data transfer.
        """
        # TODO: Delete the value that seems to be a typo, onerror=false.
        # https://github.com/fsspec/filesystem_spec/commit/346a589fef9308550ffa3d0d510f2db67281bb05
        # https://github.com/fsspec/filesystem_spec/blob/2024.10.0/fsspec/spec.py#L1185
        # https://github.com/fsspec/filesystem_spec/blob/2024.10.0/fsspec/spec.py#L1077
        kwargs.pop("onerror", None)
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
            "Copy object from s3://%s/%s?versionId=%s to s3://%s/%s.",
            bucket1,
            key1,
            version_id1,
            bucket2,
            key2,
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
            raise ValueError("Block size must be greater than 5MiB and less than 5GiB.")

        copy_source = {
            "Bucket": bucket1,
            "Key": key1,
        }
        if version_id1:
            copy_source.update({"VersionId": version_id1})

        ranges = S3File._get_ranges(
            0,
            size1,
            max_workers,
            block_size,
        )
        multipart_upload = self._create_multipart_upload(
            bucket=bucket2,
            key=key2,
            **kwargs,
        )
        parts = []
        with self._create_executor(max_workers=max_workers) as executor:
            fs = [
                executor.submit(
                    self._upload_part_copy,
                    bucket=bucket2,
                    key=key2,
                    copy_source=copy_source,
                    upload_id=cast(str, multipart_upload.upload_id),
                    part_number=i + 1,
                    copy_source_ranges=range_,
                )
                for i, range_ in enumerate(ranges)
            ]
            for f in as_completed(fs):
                result = f.result()
                parts.append(
                    {
                        "ETag": result.etag,
                        "PartNumber": result.part_number,
                    }
                )

        parts.sort(key=lambda x: x["PartNumber"])  # type: ignore[arg-type, return-value]
        self._complete_multipart_upload(
            bucket=bucket2,
            key=key2,
            upload_id=cast(str, multipart_upload.upload_id),
            parts=parts,
        )

    def cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs
    ) -> bytes:
        bucket, key, version_id = self.parse_path(path)
        if start is not None or end is not None:
            size = self.info(path).get("size", 0)
            if start is None:
                range_start = 0
            elif start < 0:
                range_start = size + start
            else:
                range_start = start

            if end is None:
                range_end = size
            elif end < 0:
                range_end = size + end
            else:
                range_end = end

            ranges = (range_start, range_end)
        else:
            ranges = None

        return self._get_object(
            bucket=bucket,
            key=cast(str, key),
            ranges=ranges,
            version_id=version_id,
            **kwargs,
        )[1]

    def put_file(self, lpath: str, rpath: str, callback=_DEFAULT_CALLBACK, **kwargs):
        """Upload a local file to S3.

        Uploads a file from the local filesystem to an S3 location. Supports
        automatic content type detection based on file extension and provides
        progress callback functionality.

        Args:
            lpath: Local file path to upload.
            rpath: S3 destination path (s3://bucket/key).
            callback: Progress callback for tracking upload progress.
            **kwargs: Additional S3 parameters (e.g., ContentType, StorageClass).

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
        callback.set_size(size)
        if "ContentType" not in kwargs:
            content_type, _ = mimetypes.guess_type(lpath)
            if content_type is not None:
                kwargs["ContentType"] = content_type

        with (
            self.open(rpath, "wb", s3_additional_kwargs=kwargs) as remote,
            open(lpath, "rb") as local,
        ):
            while data := local.read(remote.blocksize):
                remote.write(data)
                callback.relative_update(len(data))

        self.invalidate_cache(rpath)

    def get_file(self, rpath: str, lpath: str, callback=_DEFAULT_CALLBACK, outfile=None, **kwargs):
        """Download an S3 file to local filesystem.

        Downloads a file from S3 to the local filesystem with progress tracking.
        Reads the file in chunks to handle large files efficiently.

        Args:
            rpath: S3 source path (s3://bucket/key).
            lpath: Local destination file path.
            callback: Progress callback for tracking download progress.
            outfile: Unused parameter for fsspec compatibility.
            **kwargs: Additional S3 parameters passed to open().

        Note:
            If lpath is a directory, the method returns without performing
            any operation.
        """
        if os.path.isdir(lpath):
            return

        with open(lpath, "wb") as local, self.open(rpath, "rb", **kwargs) as remote:
            callback.set_size(remote.size)
            while data := remote.read(remote.blocksize):
                local.write(data)
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

        _logger.debug("Generate signed url: s3://%s/%s?versionId=%s", bucket, key, version_id)
        return self._call(
            self._client.generate_presigned_url,
            **request,
        )

    def metadata(self, path: str, **kwargs) -> dict[str, Any]:
        """Return the user-defined metadata (``x-amz-meta-*``) of the path.

        Args:
            path: S3 path (s3://bucket/key) to get metadata for.
            **kwargs: Additional parameters passed to the HeadObject API.

        Returns:
            Dictionary of the user-defined metadata. The keys are returned
            as stored in S3 (S3 normalizes them to lowercase).
        """
        bucket, key, version_id = self.parse_path(path)
        if not key:
            raise ValueError("Cannot get metadata of a bucket.")
        request: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if version_id:
            request.update({"VersionId": version_id})

        _logger.debug("Head object metadata: s3://%s/%s?versionId=%s", bucket, key, version_id)
        response = self._call(
            self._client.head_object,
            **request,
            **kwargs,
        )
        return cast(dict[str, Any], response["Metadata"])

    def getxattr(self, path: str, attr_name: str, **kwargs) -> Any:
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
        last-modified time.

        Args:
            path: S3 path (s3://bucket/key) to set metadata for.
            copy_kwargs: Additional parameters to use for the underlying
                CopyObject API call.
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
        """
        bucket, key, version_id = self.parse_path(path)
        if not key:
            raise ValueError("Cannot set metadata of a bucket.")
        metadata = self.metadata(path)
        for k, v in kw_args.items():
            if v is None:
                metadata.pop(k, None)
            else:
                metadata[k] = v

        copy_source: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if version_id:
            copy_source.update({"VersionId": version_id})

        _logger.debug("Set object metadata: s3://%s/%s?versionId=%s", bucket, key, version_id)
        self._call(
            self._client.copy_object,
            CopySource=copy_source,
            Bucket=bucket,
            Key=key,
            Metadata=metadata,
            MetadataDirective="REPLACE",
            **(copy_kwargs if copy_kwargs else {}),
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

        _logger.debug("Get object tagging: s3://%s/%s?versionId=%s", bucket, key, version_id)
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

        _logger.debug("Put object tagging: s3://%s/%s?versionId=%s", bucket, key, version_id)
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

            _logger.debug("Put object acl: s3://%s/%s?versionId=%s", bucket, key, version_id)
            self._call(
                self._client.put_object_acl,
                **request,
                **kwargs,
            )
        else:
            _logger.debug("Put bucket acl: s3://%s", bucket)
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
            path: S3 bucket or prefix path (e.g., "bucket", "s3://bucket" or
                "s3://bucket/prefix"). If the path contains a key prefix,
                only the uploads under that prefix are listed.

        Returns:
            List of S3MultipartUpload instances describing the in-progress
            multipart uploads.
        """
        bucket, key, _ = self.parse_path(path)

        _logger.debug("List multipart uploads: s3://%s/%s", bucket, key)
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
                S3MultipartUpload({**u, "Bucket": bucket}) for u in response.get("Uploads", [])
            )
            if not response.get("IsTruncated"):
                break
            next_key_marker = response.get("NextKeyMarker")
            next_upload_id_marker = response.get("NextUploadIdMarker")
            if not next_key_marker or not next_upload_id_marker:
                break
        return uploads

    def clear_multipart_uploads(self, path: str) -> None:
        """Abort any incomplete multipart uploads in the bucket.

        Args:
            path: S3 bucket or prefix path (e.g., "bucket", "s3://bucket" or
                "s3://bucket/prefix"). If the path contains a key prefix,
                only the uploads under that prefix are aborted.
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
        return self.modified(path)

    def modified(self, path: str) -> datetime:
        info = self.info(path)
        return cast(datetime, info.get("last_modified"))

    def invalidate_cache(self, path: str | None = None) -> None:
        if path is None:
            self.dircache.clear()
        else:
            path = self._strip_protocol(path)
            while path:
                self.dircache.pop(path, None)
                path = self._parent(path)

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
        max_workers = kwargs.pop("max_worker", self.max_workers)
        s3_additional_kwargs = kwargs.pop("s3_additional_kwargs", {})
        s3_additional_kwargs.update(self.s3_additional_kwargs)

        return S3File(
            self,
            path,
            mode,
            version_id=None,
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
        ranges: tuple[int, int] | None = None,
        version_id: str | None = None,
        **kwargs,
    ) -> tuple[int, bytes]:
        request = {"Bucket": bucket, "Key": key}
        if ranges:
            range_ = S3File._format_ranges(ranges)
            request.update({"Range": range_})
        else:
            ranges = (0, 0)
            range_ = "bytes=0-"
        if version_id:
            request.update({"VersionId": version_id})

        _logger.debug(
            "Get object: s3://%s/%s?versionId=%s&range=%s",
            bucket,
            key,
            version_id,
            range_,
        )
        response = self._call(
            self._client.get_object,
            **request,
            **kwargs,
        )
        return ranges[0], cast(bytes, response["Body"].read())

    def _put_object(self, bucket: str, key: str, body: bytes | None, **kwargs) -> S3PutObject:
        request: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if body:
            request.update({"Body": body})

        _logger.debug("Put object: s3://%s/%s", bucket, key)
        response = self._call(
            self._client.put_object,
            **request,
            **kwargs,
        )
        return S3PutObject(response)

    def _create_multipart_upload(self, bucket: str, key: str, **kwargs) -> S3MultipartUpload:
        request = {
            "Bucket": bucket,
            "Key": key,
        }

        _logger.debug("Create multipart upload to s3://%s/%s.", bucket, key)
        response = self._call(
            self._client.create_multipart_upload,
            **request,
            **kwargs,
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
            "Upload part copy from %s to s3://%s/%s as part %s.",
            copy_source,
            bucket,
            key,
            part_number,
        )
        response = self._call(
            self._client.upload_part_copy,
            **request,
            **kwargs,
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

        _logger.debug(
            "Upload part of %s to s3://%s/%s as part %s.",
            upload_id,
            bucket,
            key,
            part_number,
        )
        response = self._call(
            self._client.upload_part,
            **request,
            **kwargs,
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

        _logger.debug("Complete multipart upload %s to s3://%s/%s.", upload_id, bucket, key)
        response = self._call(
            self._client.complete_multipart_upload,
            **request,
            **kwargs,
        )
        return S3CompleteMultipartUpload(response)

    def _call(self, method: str | Callable[..., Any], **kwargs) -> dict[str, Any]:
        func = getattr(self._client, method) if isinstance(method, str) else method
        try:
            response = retry_api_call(
                func, config=self._retry_config, logger=_logger, **kwargs, **self.request_kwargs
            )
        except botocore.exceptions.ClientError as e:
            raise S3ClientError(e).os_error from e
        return cast(dict[str, Any], response)


class S3File(AbstractBufferedFile):
    fs: S3FileSystem

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
        self.max_workers = max_workers
        self._executor: S3Executor = executor or S3ThreadPoolExecutor(max_workers=max_workers)
        self.s3_additional_kwargs = s3_additional_kwargs if s3_additional_kwargs else {}

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
        if "r" not in mode and block_size < self.fs.MULTIPART_UPLOAD_MIN_PART_SIZE:
            # When writing occurs, the block size should not be smaller
            # than the minimum size of a part in a multipart upload.
            raise ValueError(f"Block size must be >= {self.fs.MULTIPART_UPLOAD_MIN_PART_SIZE}MB.")

        self.append_block = False
        self._details: S3Object | dict[str, Any]
        if "r" in mode:
            info = self.fs.info(self.path, version_id=self.version_id)
            if etag := info.get("etag"):
                self.s3_additional_kwargs.update({"IfMatch": etag})
            self._details = info
        elif "a" in mode and self.fs.exists(path):
            self.append_block = True
            info = self.fs.info(self.path, version_id=self.version_id)
            loc = info.get("size", 0)
            if loc < self.fs.MULTIPART_UPLOAD_MIN_PART_SIZE:
                self.write(self.fs.cat(self.path))
            self.loc = loc
            self.s3_additional_kwargs.update(info.to_api_repr())
            self._details = info
        else:
            self._details = {}

        self.multipart_upload: S3MultipartUpload | None = None
        self.multipart_upload_parts: list[Future[S3MultipartUploadPart]] = []

    def close(self) -> None:
        super().close()
        self._executor.shutdown()

    def _initiate_upload(self) -> None:
        if self.tell() < self.blocksize:
            # Files smaller than block size in size cannot be multipart uploaded.
            return

        self.multipart_upload = self.fs._create_multipart_upload(
            bucket=self.bucket,
            key=self.key,
            **self.s3_additional_kwargs,
        )
        if self.append_block:
            if self.tell() > S3FileSystem.MULTIPART_UPLOAD_MAX_PART_SIZE:
                info = self.fs.info(self.path, version_id=self.version_id)
                ranges = self._get_ranges(
                    0,
                    # Set copy source file byte size
                    info.get("size", 0),
                    self.max_workers,
                    S3FileSystem.MULTIPART_UPLOAD_MAX_PART_SIZE,
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
        if self.tell() < self.blocksize:
            # Files smaller than block size in size cannot be multipart uploaded.
            if self.autocommit and final:
                self.commit()
            return not final

        if not self.multipart_upload:
            raise RuntimeError("Multipart upload is not initialized.")

        part_number = len(self.multipart_upload_parts)
        self.buffer.seek(0)
        while data := self.buffer.read(self.blocksize):
            # The last part of a multipart request should be adjusted
            # to be larger than the minimum part size.
            next_data = self.buffer.read(self.blocksize)
            next_data_size = len(next_data)
            if 0 < next_data_size < self.fs.MULTIPART_UPLOAD_MIN_PART_SIZE:
                upload_data = data + next_data
                upload_data_size = len(upload_data)
                if upload_data_size < self.fs.MULTIPART_UPLOAD_MAX_PART_SIZE:
                    uploads = [upload_data]
                else:
                    split_size = upload_data_size // 2
                    uploads = [upload_data[:split_size], upload_data[split_size:]]
            else:
                uploads = [data]
                if next_data:
                    uploads.append(next_data)

            for upload in uploads:
                part_number += 1
                self.multipart_upload_parts.append(
                    self._executor.submit(
                        self.fs._upload_part,
                        bucket=self.bucket,
                        key=self.key,
                        upload_id=cast(str, self.multipart_upload.upload_id),
                        part_number=part_number,
                        body=upload,
                    )
                )

            if not next_data:
                break

        if self.autocommit and final:
            self.commit()
        return not final

    def commit(self) -> None:
        if self.tell() == 0:
            if self.buffer is not None:
                self.discard()
                self.fs.touch(self.path, **self.s3_additional_kwargs)
        elif not self.multipart_upload_parts:
            if self.buffer is not None:
                # Upload files smaller than block size.
                self.buffer.seek(0)
                data = self.buffer.read()
                self.fs._put_object(
                    bucket=self.bucket,
                    key=self.key,
                    body=data,
                    **self.s3_additional_kwargs,
                )
        else:
            if not self.multipart_upload:
                raise RuntimeError("Multipart upload is not initialized.")

            parts: list[dict[str, Any]] = []
            for f in as_completed(self.multipart_upload_parts):
                result = f.result()
                parts.append(
                    {
                        "ETag": result.etag,
                        "PartNumber": result.part_number,
                    }
                )
            parts.sort(key=lambda x: x["PartNumber"])
            self.fs._complete_multipart_upload(
                bucket=self.bucket,
                key=self.key,
                upload_id=cast(str, self.multipart_upload.upload_id),
                parts=parts,
            )

        self.fs.invalidate_cache(self.path)

    def discard(self) -> None:
        if self.multipart_upload:
            for f in self.multipart_upload_parts:
                f.cancel()
            self.fs._call(
                "abort_multipart_upload",
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self.multipart_upload.upload_id,
                **self.s3_additional_kwargs,
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

    def metadata(self, **kwargs) -> dict[str, Any]:
        """Return the user-defined metadata of the file.

        See :meth:`S3FileSystem.metadata`.

        Args:
            **kwargs: Additional parameters passed to the HeadObject API.

        Returns:
            Dictionary of the user-defined metadata.
        """
        return self.fs.metadata(self.path, **kwargs)

    def getxattr(self, xattr_name: str, **kwargs) -> Any:
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
                    **self.s3_additional_kwargs,
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
                **self.s3_additional_kwargs,
            )[1]
        return object_

    @staticmethod
    def _format_ranges(ranges: tuple[int, int]):
        return f"bytes={ranges[0]}-{ranges[1] - 1}"

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
                if range_end > end:
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
