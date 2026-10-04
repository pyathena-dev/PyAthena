import asyncio
import bz2
import contextlib
import functools
import gc
import gzip
import io
import lzma
import os
import re
import signal
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import uuid
import weakref
from base64 import b64encode
from concurrent.futures import Future, ThreadPoolExecutor, wait
from datetime import UTC, datetime
from itertools import chain
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from zlib import crc32

import boto3
import botocore.exceptions
import pytest
from botocore.stub import Stubber
from fsspec import Callback
from fsspec.compression import compr
from fsspec.dircache import DirCache
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.implementations.memory import MemoryFileSystem

import pyathena
from pyathena.filesystem import register_s3_filesystem
from pyathena.filesystem.s3 import CompressedBuffer, S3File, S3FileSystem
from pyathena.filesystem.s3_core import S3Core, S3DeleteBatch
from pyathena.filesystem.s3_errors import S3ClientError
from pyathena.filesystem.s3_executor import S3AioExecutor, S3ThreadPoolExecutor
from pyathena.filesystem.s3_object import S3MultipartUpload, S3Object, S3ObjectType, S3StorageClass
from pyathena.filesystem.s3_path import S3Path
from pyathena.filesystem.s3_path_pairing import S3PathPairing
from pyathena.util import RetryConfig
from tests import ENV
from tests.pyathena.conftest import connect
from tests.pyathena.util import (
    MULTIPART_COPY_BLOCK_SIZE,
    MULTIPART_COPY_KWARGS,
    MULTIPART_COPY_SIZE,
    stub_multipart_copy,
)

# A client that sends no requests; its service model selects the parameters
# that each S3 operation accepts.
S3_CLIENT = boto3.client(
    "s3", region_name="us-east-1", aws_access_key_id="dummy", aws_secret_access_key="dummy"
)


@pytest.fixture(scope="class")
def register_filesystem():
    register_s3_filesystem()


@pytest.mark.usefixtures("register_filesystem")
class TestS3FileSystem:
    def test_parse_path(self):
        actual = S3FileSystem.parse_path("s3://bucket")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = S3FileSystem.parse_path("s3://bucket/")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = S3FileSystem.parse_path("s3://bucket/path/to/obj")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] is None

        actual = S3FileSystem.parse_path("s3://bucket/path/to/obj?versionId=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = S3FileSystem.parse_path("s3a://bucket")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = S3FileSystem.parse_path("s3a://bucket/")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = S3FileSystem.parse_path("s3a://bucket/path/to/obj")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] is None

        actual = S3FileSystem.parse_path("s3a://bucket/path/to/obj?versionId=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = S3FileSystem.parse_path("bucket")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = S3FileSystem.parse_path("bucket/")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = S3FileSystem.parse_path("bucket/path/to/obj")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] is None

        actual = S3FileSystem.parse_path("bucket/path/to/obj?versionId=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = S3FileSystem.parse_path("bucket/path/to/obj?versionID=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = S3FileSystem.parse_path("bucket/path/to/obj?versionid=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = S3FileSystem.parse_path("bucket/path/to/obj?version_id=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

    def test_parse_path_invalid(self):
        with pytest.raises(ValueError, match="Invalid S3 path format"):
            S3FileSystem.parse_path("http://bucket")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            S3FileSystem.parse_path("s3://bucket?")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            S3FileSystem.parse_path("s3://bucket?foo=bar")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            S3FileSystem.parse_path("s3a://bucket?")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            S3FileSystem.parse_path("s3a://bucket?foo=bar")

        # GH-979: a "?" in a key that does not start a trailing version ID
        # query is part of the key.
        for path in ("s3://bucket/path/to/obj?foo=bar", "s3a://bucket/path/to/obj?foo=bar"):
            assert S3FileSystem.parse_path(path) == ("bucket", "path/to/obj?foo=bar", None)

    @staticmethod
    def _make_fs():
        # Build a minimal S3FileSystem without touching AWS, bypassing
        # __init__ which would require a boto3 client.
        fs = S3FileSystem.__new__(S3FileSystem)
        fs.dircache = DirCache()
        client = mock.MagicMock()
        client.meta.method_to_api_mapping = S3_CLIENT.meta.method_to_api_mapping
        client.meta.service_model = S3_CLIENT.meta.service_model
        fs._core = S3Core(client, retry_config=RetryConfig())
        # The requests of the core and of the filesystem go to one mock.
        fs._call = fs._core.call = mock.MagicMock()
        fs.max_workers = 4
        fs.default_block_size = S3FileSystem.DEFAULT_BLOCK_SIZE
        fs.allow_bucket_creation = False
        fs.allow_bucket_deletion = False
        fs.s3_additional_kwargs = {}
        fs._intrans = False
        fs.version_aware = False
        return fs

    @staticmethod
    def _file_object(key):
        # Build a listed file entry in the bucket named "bucket".
        return S3Object(
            init={"Key": key},
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="bucket",
            key=key,
        )

    @staticmethod
    def _barrier_dircache(key):
        # Build a DirCache that holds every reader of the key until two
        # threads have read it, so that both read before either deletes.
        barrier = threading.Barrier(2, timeout=5)

        class BarrierDirCache(DirCache):
            def __getitem__(self, item):
                value = super().__getitem__(item)
                if item == key:
                    barrier.wait()
                return value

        return BarrierDirCache()

    def test_get_client_compatible_with_s3fs(self):
        # Only constructs a boto3 client; no AWS access.
        fs = S3FileSystem(
            key="test_access_key",
            secret="test_secret_key",
            region_name="us-east-1",
            use_ssl=False,
            skip_instance_cache=True,
        )
        # use_ssl=False is honored (previously dropped by a truthiness check).
        assert fs._client.meta.endpoint_url.startswith("http://")
        assert pyathena.user_agent_extra in fs._client.meta.config.user_agent_extra

        fs = S3FileSystem(
            key="test_access_key",
            secret="test_secret_key",
            region_name="us-east-1",
            endpoint_url="http://localhost:9000",
            skip_instance_cache=True,
        )
        assert fs._client.meta.endpoint_url == "http://localhost:9000"

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            ({}, "DEFAULTKEY"),
            # s3fs names the boto3 profile_name argument "profile".
            ({"profile": "other"}, "OTHERKEY"),
            ({"profile_name": "other"}, "OTHERKEY"),
            ({"profile": "other", "profile_name": "default"}, "DEFAULTKEY"),
        ],
    )
    def test_get_client_compatible_with_s3fs_profile(self, monkeypatch, tmp_path, kwargs, expected):
        # Only constructs a boto3 client from local profile files; no AWS access.
        config = tmp_path / "config"
        config.write_text("[default]\n[profile other]\n")
        credentials = tmp_path / "credentials"
        credentials.write_text(
            "[default]\naws_access_key_id = DEFAULTKEY\naws_secret_access_key = secret\n"
            "[other]\naws_access_key_id = OTHERKEY\naws_secret_access_key = secret\n"
        )
        for name in (
            "AWS_PROFILE",
            "AWS_DEFAULT_PROFILE",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
        monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials))

        fs = S3FileSystem(region_name="us-east-1", skip_instance_cache=True, **kwargs)
        assert fs._client._request_signer._credentials.access_key == expected

    def test_ls_from_cache_with_cached_object(self):
        fs = self._make_fs()
        obj = S3Object(
            init={
                "ContentLength": 4,
                "ContentType": None,
                "StorageClass": S3StorageClass.S3_STORAGE_CLASS_STANDARD,
                "ETag": '"etag"',
                "LastModified": None,
            },
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="bucket",
            key="key",
        )
        fs.dircache["bucket/key"] = obj

        assert fs._ls_from_cache("bucket/key") is obj
        # A child path of a cached object entry must not fail; it falls
        # through to the S3 API instead (fsspec's implementation assumes
        # every cache value is a listing and raises TypeError here).
        assert fs._ls_from_cache("bucket/key/child") is None

    def test_info_uses_cached_listings(self):
        # GH-965: listings cached under (path, delimiter) answer info() and
        # exists() without HeadObject or ListObjectsV2 requests.
        fs = self._make_fs()
        fs._call.return_value = {
            "CommonPrefixes": [{"Prefix": "d/sub/"}],
            "Contents": [{"Key": "d/direct", "Size": 4, "ETag": '"etag"'}],
        }
        fs.ls("s3://bucket/d")
        fs._call.reset_mock()

        file = fs.info("s3://bucket/d/direct")
        assert (file.type, file.size, file.etag) == (S3ObjectType.S3_OBJECT_TYPE_FILE, 4, '"etag"')
        assert fs.isdir("s3://bucket/d/sub")
        with pytest.raises(FileNotFoundError):
            fs.info("s3://bucket/d/missing")
        assert not fs.exists("s3://bucket/d/missing")
        fs._call.assert_not_called()

    def test_info_prefers_listed_object_to_prefix_of_same_name(self):
        # A key that is both an object and a key prefix is an object, as the
        # uncached lookup with HeadObject finds.
        fs = self._make_fs()
        fs.dircache[("bucket", "/")] = [
            fs._directory_object("bucket", "d"),
            self._file_object("d"),
        ]

        assert fs.isfile("s3://bucket/d")
        fs._call.assert_not_called()

    def test_info_does_not_use_listing_of_path(self):
        # The listing of the path cannot tell whether an object of the same
        # name exists.
        fs = self._make_fs()
        fs.dircache[("bucket/d", "/")] = [self._file_object("d/direct")]
        fs._call.return_value = {"ContentLength": 4}

        assert fs.isfile("s3://bucket/d")
        fs._call.assert_called_once_with(fs._client.head_object, Bucket="bucket", Key="d")

    @pytest.mark.parametrize(
        ("version_aware", "lookup"),
        [
            pytest.param(
                False, lambda fs: fs.exists("s3://bucket/d/key", refresh=True), id="exists"
            ),
            pytest.param(False, lambda fs: fs.ls("s3://bucket/d/key", refresh=True), id="ls"),
            # The listed entry has no version, so it is looked up again.
            pytest.param(True, lambda fs: fs.isfile("s3://bucket/d/key"), id="version_aware"),
        ],
    )
    def test_missing_object_drops_cached_parent_listing(self, version_aware, lookup):
        # A lookup that finds a listed object deleted is not contradicted by
        # the listing afterwards.
        fs = self._make_fs()
        fs.version_aware = version_aware
        fs.dircache[("bucket/d", "/")] = [self._file_object("d/key")]

        def call(method, **kwargs):
            if method == fs._client.head_object:
                raise FileNotFoundError
            return {}

        fs._call.side_effect = call

        lookup(fs)
        assert ("bucket/d", "/") not in fs.dircache
        assert not fs.exists("s3://bucket/d/key")

    def test_refreshed_prefix_drops_cached_parent_listing(self):
        # A key prefix created after the parent was listed is not reported
        # missing by the listing after a refreshed lookup finds it.
        fs = self._make_fs()
        fs.dircache[("bucket/d", "/")] = [self._file_object("d/key")]

        def call(method, **kwargs):
            if method == fs._client.head_object:
                raise FileNotFoundError
            return {"KeyCount": 1}

        fs._call.side_effect = call

        assert fs.isdir("s3://bucket/d/new") is False
        assert (
            fs.info("s3://bucket/d/new", refresh=True).type == S3ObjectType.S3_OBJECT_TYPE_DIRECTORY
        )
        assert fs.isdir("s3://bucket/d/new")

    def test_missing_object_keeps_cached_parent_listing_without_it(self):
        fs = self._make_fs()
        fs.dircache[("bucket/d", "/")] = [self._file_object("d/key")]
        fs._call.side_effect = FileNotFoundError

        assert fs._head_object("bucket/d/other", refresh=True) is None
        assert ("bucket/d", "/") in fs.dircache

    def test_info_version_aware_heads_listed_file(self):
        fs = self._make_fs()
        fs.version_aware = True
        fs.dircache[("bucket/d", "/")] = [self._file_object("d/direct")]
        fs._call.return_value = {"ContentLength": 4, "VersionId": "v1"}

        # The listed entry has no version to pin.
        assert fs.info("s3://bucket/d/direct").version_id == "v1"
        fs._call.assert_called_once_with(fs._client.head_object, Bucket="bucket", Key="d/direct")

    def test_exists_version_ignores_cached_parent_listing(self):
        # The listing describes the current versions, so a version missing
        # from it is looked up with HeadObject.
        fs = self._make_fs()
        fs.dircache[("bucket/d", "/")] = [self._file_object("d/other")]
        fs._call.return_value = {"ContentLength": 4}

        assert fs.exists("s3://bucket/d/direct?versionId=v1")
        fs._call.assert_called_once_with(
            fs._client.head_object, Bucket="bucket", Key="d/direct", VersionId="v1"
        )

    def test_info_bucket_missing_from_bucket_listing(self):
        # GH-980: the bucket listing holds only the buckets of the caller.
        fs = self._make_fs()
        fs.dircache[""] = [fs._directory_object("mine", None)]
        fs._call.return_value = {}

        info = fs.info("s3://other-account-bucket")
        assert info.storage_class == S3StorageClass.S3_STORAGE_CLASS_BUCKET
        fs._call.assert_called_once_with(fs._client.head_bucket, Bucket="other-account-bucket")
        fs._call.reset_mock()
        assert fs.isdir("s3://other-account-bucket")
        assert fs.isdir("s3://mine")
        fs._call.assert_not_called()

    @pytest.mark.parametrize("path", ["", "/", "s3://"])
    def test_info_root(self, path):
        fs = self._make_fs()

        info = fs.info(path)
        assert (info.name, info.type, info.size) == ("", S3ObjectType.S3_OBJECT_TYPE_DIRECTORY, 0)
        assert fs.isdir(path)
        assert not fs.isfile(path)
        assert fs.size(path) == 0
        fs._call.assert_not_called()

    @pytest.mark.parametrize("path", ["", "/", "s3://"])
    def test_invalidate_cache_root_drops_bucket_listing(self, path):
        fs = self._make_fs()
        fs.dircache[""] = [fs._directory_object("bucket", None)]
        fs.dircache["bucket"] = fs._directory_object("bucket", None)

        fs.invalidate_cache(path)
        assert list(fs.dircache) == ["bucket"]

    def test_exists_bucket_access_denied(self):
        # GH-980: HeadBucket answers 403 for a bucket that exists but that
        # the caller may not access.
        fs = self._make_fs()
        fs._call.side_effect = PermissionError

        assert fs.exists("s3://not-my-bucket")
        fs.makedirs("s3://not-my-bucket/prefix", exist_ok=True)
        assert (
            fs._call.call_args_list
            == [
                mock.call(fs._client.head_bucket, Bucket="not-my-bucket"),
            ]
            * 2
        )

    def test_invalidate_cache_drops_listings_of_path_and_parents(self):
        fs = self._make_fs()
        invalidated = [
            "bucket/a/b/c.txt",
            ("bucket/a/b", "/"),
            ("bucket/a/b", ""),
            ("bucket/a", "/"),
            ("bucket/a", ""),
            ("bucket", "/"),
            ("bucket", ""),
        ]
        kept = ["", ("bucket/a/x", "/")]
        for cache_key in invalidated + kept:
            fs.dircache[cache_key] = []

        fs.invalidate_cache("s3://bucket/a/b/c.txt")
        assert list(fs.dircache) == kept

    @pytest.mark.parametrize(
        ("path", "cache_key"),
        [
            ("s3://bucket/a/c.txt?versionId=v1", "bucket/a/c.txt?versionId=v1"),
            (Path("bucket/a/c.txt?versionId=v1"), "bucket/a/c.txt?versionId=v1"),
            # A directory marker object keeps the trailing slash before the query.
            ("s3://bucket/a/c.txt/?versionId=v1", "bucket/a/c.txt/?versionId=v1"),
            # parse_path accepts other spellings of the query.
            ("s3://bucket/a/c.txt?version_id=v1", "bucket/a/c.txt?versionId=v1"),
            ("s3://bucket/a/c.txt?versionId=v1", "bucket/a/c.txt?versionid=v1"),
        ],
    )
    def test_invalidate_cache_version_drops_object_path(self, path, cache_key):
        fs = self._make_fs()
        invalidated = [
            cache_key,
            (cache_key, "/"),
            "bucket/a/c.txt",
            ("bucket/a", "/"),
            ("bucket", "/"),
        ]
        # Other versions of the object do not change.
        kept = ["bucket/a/c.txt?versionId=v2"]
        for key in invalidated + kept:
            fs.dircache[key] = []

        fs.invalidate_cache(path)
        assert list(fs.dircache) == kept

    def test_rm_file_version_invalidates_object_path(self):
        fs = self._make_fs()
        fs.dircache["bucket/a/c.txt"] = self._file_object("a/c.txt")

        fs.rm_file("s3://bucket/a/c.txt?versionId=v1")
        fs._call.assert_called_once_with(
            fs._client.delete_object, Bucket="bucket", Key="a/c.txt", VersionId="v1"
        )

        # The deleted version was the only one: HeadObject and the prefix
        # listing find nothing, instead of the cached object answering.
        fs._call.side_effect = [FileNotFoundError("bucket/a/c.txt"), {}]
        assert not fs.exists("s3://bucket/a/c.txt")

    @pytest.mark.parametrize("name", ["bucket", "key", "version_id"])
    def test_rm_file_path_kwargs(self, name):
        # The path gives these; rm_file() must not delete another version
        # than the one asked for.
        fs = self._make_fs()

        with pytest.raises(TypeError, match=f"multiple values for keyword argument '{name}'"):
            fs.rm_file("s3://bucket/a", **{name: "v1"})
        fs._call.assert_not_called()

    @staticmethod
    def _sent_delete_objects(fs):
        return sorted(
            (c.kwargs["Bucket"], c.kwargs["Delete"]["Objects"])
            for c in fs._call.call_args_list
            if c.args == (fs._client.delete_objects,)
        )

    def test_rm_paths_across_buckets(self):
        # GH-971: a list of paths was rejected, and every request went to the
        # bucket of one path.
        fs = self._make_fs()
        fs._call.return_value = {}

        fs.rm(["s3://b1/a", "s3://b2/b", "s3://b1/c"], ExpectedBucketOwner="111122223333")
        assert self._sent_delete_objects(fs) == [
            ("b1", [{"Key": "a"}, {"Key": "c"}]),
            ("b2", [{"Key": "b"}]),
        ]
        for c in fs._call.call_args_list:
            assert c.kwargs["ExpectedBucketOwner"] == "111122223333"
            assert c.kwargs["Delete"]["Quiet"] is True

    def test_rm_version(self):
        # GH-971: expand_path treated "?" in the version query as a wildcard.
        fs = self._make_fs()
        fs._call.return_value = {}
        fs.dircache["bucket/a.csv"] = self._file_object("a.csv")

        fs.rm("s3://bucket/a.csv?versionId=v1", recursive=True)
        assert self._sent_delete_objects(fs) == [
            ("bucket", [{"Key": "a.csv", "VersionId": "v1"}]),
        ]
        assert "bucket/a.csv" not in fs.dircache

        fs._call.reset_mock()
        fs.rm(["s3://bucket/a.csv?versionId=v1", "s3://bucket/b.csv"])
        assert self._sent_delete_objects(fs) == [
            ("bucket", [{"Key": "a.csv", "VersionId": "v1"}, {"Key": "b.csv"}]),
        ]

    @pytest.mark.parametrize(
        "path",
        [
            "s3://bucket",
            "s3://bucket/",
            "s3://bucket?versionId=v1",
            # expand_path strips the slashes to the bucket.
            "s3://bucket//",
            ["s3://bucket/a", "s3://bucket"],
        ],
    )
    def test_rm_bucket(self, path):
        fs = self._make_fs()
        fs._call.return_value = {}

        with pytest.raises(ValueError, match="Cannot delete the bucket"):
            fs.rm(path, recursive=True)
        fs._call.assert_not_called()

    def test_rm_errors(self):
        # GH-971: S3 reports the objects it could not delete in a successful
        # response, which rm() ignored.
        fs = self._make_fs()
        fs._call.return_value = {
            "Errors": [{"Key": "locked", "Code": "AccessDenied", "Message": "Access Denied"}]
        }
        with pytest.raises(OSError, match=r"bucket/locked \(AccessDenied: Access Denied\)"):
            fs.rm("s3://bucket/locked")

        fs._call.return_value = {
            "Errors": [
                {"Key": "locked", "Code": "AccessDenied", "Message": "Access Denied"},
                {"Key": "a", "VersionId": "v1", "Code": "InternalError", "Message": "Error"},
            ]
        }
        fs.dircache["bucket/b"] = self._file_object("b")

        with pytest.raises(OSError, match="Failed to delete objects: ") as exc_info:
            fs.rm(["s3://bucket/locked", "s3://bucket/a?versionId=v1", "s3://bucket/b"])
        assert str(exc_info.value) == (
            "Failed to delete objects: "
            "bucket/a?versionId=v1 (InternalError: Error), "
            "bucket/locked (AccessDenied: Access Denied)"
        )
        # The deleted object is not left in the cache.
        assert "bucket/b" not in fs.dircache

    @pytest.mark.parametrize("name", ["Bucket", "Delete"])
    def test_rm_request_target_kwargs(self, name):
        fs = self._make_fs()

        with pytest.raises(TypeError, match=f"unexpected keyword argument '{name}'"):
            fs.rm("s3://bucket/a", **{name: "other"})
        fs._call.assert_not_called()

    def test_rm_request_error_keeps_errors(self):
        fs = self._make_fs()

        def call(method, **request):
            if request["Bucket"] == "b1":
                raise PermissionError("Access Denied")
            return {"Errors": [{"Key": "b", "Code": "AccessDenied", "Message": "Access Denied"}]}

        fs._call.side_effect = call
        with pytest.raises(PermissionError, match="Access Denied") as exc_info:
            fs.rm(["s3://b1/a", "s3://b2/b"])
        assert exc_info.value.__notes__ == [
            "Failed to delete objects: b2/b (AccessDenied: Access Denied)"
        ]

    def test_rm_requests_invalidate_shared_parent(self):
        # The request threads invalidate the shared parent "bucket/dir" at
        # once. DirCache.pop() reads before it deletes, so the second delete
        # raised KeyError when both threads had read the entry.
        fs = self._make_fs()
        fs._call.return_value = {}
        fs.dircache = self._barrier_dircache("bucket/dir")
        fs.dircache["bucket/dir"] = []

        with mock.patch.object(S3DeleteBatch, "MAX_KEYS", 1):
            fs.rm(["s3://bucket/dir/a", "s3://bucket/dir/b"])
        assert fs._call.call_count == 2
        assert "bucket/dir" not in fs.dircache._cache

    @pytest.mark.parametrize(
        ("key", "listing", "read"),
        [
            ("bucket", False, lambda fs: fs._head_bucket("bucket")),
            ("bucket/key", False, lambda fs: fs._head_object("bucket/key")),
            ("", True, lambda fs: fs._ls_buckets()),
            (("bucket/dir", "/"), True, lambda fs: fs._ls_dirs("bucket/dir")),
        ],
    )
    def test_cache_read_with_concurrent_invalidation(self, key, listing, read):
        # Another thread invalidates the entry right after this thread looks
        # it up. Checking the key and then reading it raised KeyError.
        class InvalidatingDirCache(DirCache):
            def __getitem__(self, item):
                value = super().__getitem__(item)
                if item == key:
                    self._cache.pop(item, None)
                return value

        fs = self._make_fs()
        fs.dircache = InvalidatingDirCache()
        cached = [self._file_object("dir/a")] if listing else self._file_object("key")
        fs.dircache[key] = cached

        assert read(fs) is cached
        fs._call.assert_not_called()

    @pytest.mark.parametrize(
        ("key", "call", "evict"),
        [
            (
                "bucket",
                {"side_effect": FileNotFoundError("bucket")},
                lambda fs: fs._head_bucket("bucket", refresh=True),
            ),
            (
                "bucket/key",
                {"side_effect": FileNotFoundError("bucket/key")},
                lambda fs: fs._head_object("bucket/key", refresh=True),
            ),
            (
                ("bucket/dir", "/"),
                {"return_value": {}},
                lambda fs: fs._ls_dirs("bucket/dir", refresh=True),
            ),
        ],
    )
    def test_cache_eviction_with_concurrent_invalidation(self, key, call, evict):
        # Two threads evict the same entry at once. DirCache.pop() reads
        # before it deletes, so the second delete raised KeyError when both
        # threads had read the entry.
        fs = self._make_fs()
        fs._call.configure_mock(**call)
        fs.dircache = self._barrier_dircache(key)
        fs.dircache[key] = []

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(evict, fs) for _ in range(2)]
            for future in futures:
                future.result()
        assert key not in fs.dircache._cache

    def test_rm_request_error_invalidates_cache(self):
        fs = self._make_fs()
        fs._call.side_effect = PermissionError("Access Denied")
        fs.dircache["bucket/a"] = self._file_object("a")

        with pytest.raises(PermissionError, match="Access Denied"):
            fs.rm("s3://bucket/a")
        assert "bucket/a" not in fs.dircache

    @pytest.mark.parametrize(
        ("prefix", "next_token"),
        [
            ("test_", None),
            ("", "token"),
        ],
    )
    def test_ls_dirs_partial_listing_bypasses_cache(self, prefix, next_token):
        fs = self._make_fs()
        cached = self._file_object("dir/cached")
        fs.dircache[("bucket/dir", "")] = [cached]
        fs._call.return_value = {"Contents": [{"Key": "dir/test_1"}]}

        files = fs._ls_dirs("bucket/dir", prefix=prefix, delimiter="", next_token=next_token)
        assert [f.name for f in files] == ["bucket/dir/test_1"]
        assert fs.dircache[("bucket/dir", "")] == [cached]

        # A complete listing of the path is still served from the cache.
        fs._call.reset_mock()
        assert fs._ls_dirs("bucket/dir", delimiter="") == [cached]
        fs._call.assert_not_called()

    def test_ls_dirs_empty_refresh_evicts_cached_listing(self):
        fs = self._make_fs()
        fs.dircache[("bucket/dir", "/")] = [self._file_object("dir/deleted")]
        fs._call.return_value = {}

        assert fs._ls_dirs("bucket/dir", refresh=True) == []
        # The next listing must not return the deleted object from the cache.
        fs._call.reset_mock()
        assert fs._ls_dirs("bucket/dir") == []
        fs._call.assert_called_once()

    def test_find_withdirs_does_not_modify_cached_listing(self):
        fs = self._make_fs()
        fs.dircache[("bucket/dir", "")] = [self._file_object("dir/sub/file")]

        expected = ["bucket/dir", "bucket/dir/sub", "bucket/dir/sub/file"]
        assert sorted(fs.find("s3://bucket/dir", withdirs=True)) == expected
        assert sorted(fs.find("s3://bucket/dir", withdirs=True)) == expected
        assert fs.find("s3://bucket/dir") == ["bucket/dir/sub/file"]
        fs._call.assert_not_called()

    def test_find_refresh_bypasses_cached_listings(self):
        fs = self._make_fs()
        fs.dircache[("bucket/dir", "")] = [self._file_object("dir/old")]
        fs.dircache[("bucket/dir", "/")] = [self._file_object("dir/old")]
        fs.dircache[("bucket/dir/sub", "/")] = [self._file_object("dir/sub/old")]
        responses = {
            ("dir/", ""): {"Contents": [{"Key": "dir/sub/new"}]},
            ("dir/", "/"): {"CommonPrefixes": [{"Prefix": "dir/sub/"}]},
            ("dir/sub/", "/"): {"Contents": [{"Key": "dir/sub/new"}]},
        }
        fs._call.side_effect = lambda method, **kwargs: responses[
            (kwargs["Prefix"], kwargs["Delimiter"])
        ]

        assert fs.find("s3://bucket/dir", refresh=True) == ["bucket/dir/sub/new"]
        # The subdirectory listings of maxdepth are refreshed as well.
        assert fs.find("s3://bucket/dir", maxdepth=2, refresh=True) == ["bucket/dir/sub/new"]

    FIND_KEYS = ("dir/direct", "dir/sub/nested", "dir/sub/deep/file")

    @staticmethod
    def _serve_keys(fs, keys):
        # Answer the ListObjectsV2, HeadObject, CopyObject, and DeleteObjects
        # requests of "bucket" from a set of the given keys, which is returned.
        keys = set(keys)

        def call(method, **kwargs):
            if method is fs._client.get_bucket_versioning:
                return {}
            if method is fs._client.head_object:
                if kwargs["Key"] not in keys:
                    raise FileNotFoundError(kwargs["Key"])
                return {"ContentLength": 0}
            if method is fs._client.copy_object:
                if kwargs["CopySource"]["Key"] not in keys:
                    raise FileNotFoundError(kwargs["CopySource"]["Key"])
                keys.add(kwargs["Key"])
                return {}
            if method is fs._client.delete_objects:
                keys.difference_update(o["Key"] for o in kwargs["Delete"]["Objects"])
                return {}
            prefix, delimiter = kwargs["Prefix"], kwargs["Delimiter"]
            contents, prefixes = [], set()
            for key in sorted(keys):
                if not key.startswith(prefix):
                    continue
                rest = key[len(prefix) :]
                if delimiter and delimiter in rest:
                    prefixes.add(prefix + rest.split(delimiter)[0] + delimiter)
                else:
                    contents.append({"Key": key})
            return {
                "Contents": contents,
                "CommonPrefixes": [{"Prefix": p} for p in sorted(prefixes)],
                "KeyCount": len(contents) + len(prefixes),
            }

        fs._call.side_effect = call
        return keys

    @staticmethod
    def _memory_fs(keys):
        # Build an fsspec MemoryFileSystem with the keys under "/bucket".
        memory = MemoryFileSystem(skip_instance_cache=True)
        memory.store = {}
        memory.pseudo_dirs = [""]
        for key in keys:
            memory.pipe(f"/bucket/{key}", b"")
        return memory

    def test_find_maxdepth_counts_levels_like_fsspec(self):
        fs = self._make_fs()
        self._serve_keys(fs, self.FIND_KEYS)

        with pytest.raises(ValueError, match="maxdepth must be at least 1"):
            fs.find("s3://bucket/dir", maxdepth=0)
        fs._call.assert_not_called()

        assert fs.find("s3://bucket/dir", maxdepth=1) == ["bucket/dir/direct"]
        assert sorted(fs.find("s3://bucket/dir", maxdepth=2)) == [
            "bucket/dir/direct",
            "bucket/dir/sub/nested",
        ]
        assert sorted(fs.find("s3://bucket/dir", maxdepth=3)) == [
            "bucket/dir/direct",
            "bucket/dir/sub/deep/file",
            "bucket/dir/sub/nested",
        ]

    @pytest.mark.parametrize(
        ("path", "maxdepth", "withdirs"),
        [
            ("dir", None, True),
            ("dir", None, False),
            ("dir", 1, True),
            ("dir", 2, True),
            ("dir", 1, False),
            ("dir/sub", 1, True),
            ("dir/direct", None, True),
            ("dir/direct", 1, True),
            ("dir/direct", 1, False),
            ("missing", None, True),
            ("missing", 1, True),
        ],
    )
    def test_find_matches_fsspec(self, path, maxdepth, withdirs):
        fs = self._make_fs()
        self._serve_keys(fs, self.FIND_KEYS)
        memory = self._memory_fs(self.FIND_KEYS)

        expected = [p.lstrip("/") for p in memory.find(f"/bucket/{path}", maxdepth, withdirs)]
        assert sorted(fs.find(f"s3://bucket/{path}", maxdepth, withdirs)) == expected

    @pytest.mark.parametrize(
        ("pattern", "maxdepth"),
        [
            ("dir/**", None),
            ("dir/**", 1),
            ("dir/**", 2),
            ("dir/*", None),
            ("dir/*/*", None),
            ("dir/s*", None),
            ("dir/**/file", None),
            ("missing/*", None),
        ],
    )
    def test_glob_matches_fsspec(self, pattern, maxdepth):
        fs = self._make_fs()
        self._serve_keys(fs, self.FIND_KEYS)
        memory = self._memory_fs(self.FIND_KEYS)

        expected = [p.lstrip("/") for p in memory.glob(f"/bucket/{pattern}", maxdepth=maxdepth)]
        assert sorted(fs.glob(f"s3://bucket/{pattern}", maxdepth=maxdepth)) == expected

    def test_find_withdirs_omits_bucket(self):
        fs = self._make_fs()
        self._serve_keys(fs, self.FIND_KEYS)

        # A recursive copy of the expanded paths cannot copy a bucket.
        assert "bucket" not in fs.find("s3://bucket", withdirs=True)
        assert "bucket" not in fs.find("s3://bucket", maxdepth=1, withdirs=True)
        assert "bucket" not in fs.expand_path("s3://bucket/**", recursive=True)

    def test_find_directory_without_extra_requests(self):
        fs = self._make_fs()
        self._serve_keys(fs, self.FIND_KEYS)

        assert "bucket/dir" in fs.find("s3://bucket/dir", withdirs=True)
        assert fs._call.call_count == 1
        fs._call.reset_mock()
        assert "bucket/dir" in fs.find("s3://bucket/dir", maxdepth=1, withdirs=True)
        assert fs._call.call_count == 1
        # Only a subdirectory is listed; it is dropped without withdirs, but
        # the path is a directory, so it is not looked up as an object.
        fs._call.reset_mock()
        assert fs.find("s3://bucket/dir", maxdepth=1, prefix="s") == []
        assert fs._call.call_count == 1

    def test_find_maxdepth_listings_follow_invalidation(self):
        fs = self._make_fs()
        self._serve_keys(fs, ("dir/direct", "dir/sub/nested"))
        assert sorted(fs.find("s3://bucket/dir", maxdepth=2)) == [
            "bucket/dir/direct",
            "bucket/dir/sub/nested",
        ]

        # A write below the subdirectory invalidates its cached listing.
        self._serve_keys(fs, ("dir/direct", "dir/sub/nested", "dir/sub/new"))
        fs.invalidate_cache("s3://bucket/dir/sub/new")
        assert sorted(fs.find("s3://bucket/dir", maxdepth=2)) == [
            "bucket/dir/direct",
            "bucket/dir/sub/nested",
            "bucket/dir/sub/new",
        ]

    def test_find_prefix_counts_levels_from_path(self):
        fs = self._make_fs()
        self._serve_keys(fs, self.FIND_KEYS)

        # Nothing directly under dir/ starts with "sub/deep/".
        assert fs.find("s3://bucket/dir", maxdepth=1, prefix="sub/deep/") == []
        assert fs.find("s3://bucket/dir", maxdepth=2, prefix="sub/deep/") == []
        assert fs.find("s3://bucket/dir", maxdepth=3, prefix="sub/deep/") == [
            "bucket/dir/sub/deep/file"
        ]
        assert sorted(fs.find("s3://bucket/dir", maxdepth=2, prefix="sub/")) == [
            "bucket/dir/sub/nested"
        ]
        # The directories above the prefix do not start with it, with or
        # without maxdepth.
        expected = [
            "bucket/dir",
            "bucket/dir/sub/deep",
            "bucket/dir/sub/deep/file",
            "bucket/dir/sub/nested",
        ]
        assert sorted(fs.find("s3://bucket/dir", maxdepth=3, prefix="sub/", withdirs=True)) == (
            expected
        )
        assert sorted(fs.find("s3://bucket/dir", prefix="sub/", withdirs=True)) == expected

    @pytest.mark.parametrize("maxdepth", [None, 1])
    def test_find_object_path_ignores_prefix(self, maxdepth):
        fs = self._make_fs()
        self._serve_keys(fs, self.FIND_KEYS)

        # As in fsspec, the object itself is returned when nothing is listed.
        assert fs.find("s3://bucket/dir/direct", maxdepth=maxdepth, prefix="x") == [
            "bucket/dir/direct"
        ]

    def test_refresh_evicts_cached_object_and_bucket_not_found(self):
        fs = self._make_fs()
        fs.dircache["bucket/key"] = self._file_object("key")
        fs.dircache["bucket"] = fs._directory_object("bucket", None)
        fs.dircache[""] = [fs._directory_object("bucket", None)]

        def call(method, **kwargs):
            if method in (fs._client.head_object, fs._client.head_bucket):
                raise FileNotFoundError
            return {}

        fs._call.side_effect = call

        assert fs.ls("s3://bucket/key", refresh=True) == []
        # The next lookups must not return the deleted object and bucket from the cache.
        assert fs.ls("s3://bucket/key") == []
        assert not fs.exists("s3://bucket/key")
        with pytest.raises(FileNotFoundError):
            fs.info("s3://bucket", refresh=True)
        assert not fs.exists("s3://bucket")

    def test_exists_refresh_bypasses_cache(self):
        fs = self._make_fs()
        fs.dircache["bucket/key"] = self._file_object("key")
        fs.dircache["bucket"] = fs._directory_object("bucket", None)
        fs.dircache[""] = [fs._directory_object("bucket", None)]

        def call(method, **kwargs):
            if method in (fs._client.head_object, fs._client.head_bucket):
                raise FileNotFoundError
            return {}

        fs._call.side_effect = call

        assert fs.exists("s3://bucket/key")
        assert fs.exists("s3://bucket")
        fs._call.assert_not_called()

        assert not fs.exists("s3://bucket/key", refresh=True)
        assert not fs.exists("s3://bucket", refresh=True)

    def test_missing_bucket_keeps_bucket_listing_without_it(self):
        fs = self._make_fs()
        fs.dircache[""] = [fs._directory_object("bucket", None)]
        fs._call.side_effect = FileNotFoundError

        assert not fs.exists("s3://missing")
        # Other buckets are still answered from the cached bucket listing.
        fs._call.reset_mock()
        assert fs.exists("s3://bucket")
        fs._call.assert_not_called()

    def test_mkdir_creates_bucket(self):
        fs = self._make_fs()
        fs.allow_bucket_creation = True
        fs.exists = mock.MagicMock(return_value=False)
        fs._client.meta.region_name = "ap-northeast-1"
        fs.dircache[""] = ["stale-bucket-listing"]

        fs.mkdir("s3://new-bucket")
        fs._call.assert_called_once_with(
            fs._client.create_bucket,
            Bucket="new-bucket",
            CreateBucketConfiguration={"LocationConstraint": "ap-northeast-1"},
        )
        # The cached bucket listing must be evicted.
        assert "" not in fs.dircache

    def test_mkdir_creates_bucket_in_us_east_1_without_location_constraint(self):
        fs = self._make_fs()
        fs.allow_bucket_creation = True
        fs.exists = mock.MagicMock(return_value=False)
        fs._client.meta.region_name = "us-east-1"

        fs.mkdir("s3://new-bucket")
        fs._call.assert_called_once_with(fs._client.create_bucket, Bucket="new-bucket")

    def test_mkdir_creates_bucket_with_acl(self):
        fs = self._make_fs()
        fs.allow_bucket_creation = True
        fs.exists = mock.MagicMock(return_value=False)
        fs._client.meta.region_name = "us-east-1"

        fs.mkdir("s3://new-bucket", acl="private")
        fs._call.assert_called_once_with(
            fs._client.create_bucket, Bucket="new-bucket", ACL="private"
        )

        with pytest.raises(ValueError, match="ACL not in"):
            fs.mkdir("s3://another-bucket", acl="invalid-acl")

    def test_mkdir_bucket_creation_disabled(self):
        # Bucket creation is disabled by default.
        fs = self._make_fs()
        fs.exists = mock.MagicMock(return_value=False)

        with pytest.raises(PermissionError, match="Bucket creation is disabled"):
            fs.mkdir("s3://new-bucket")
        fs._call.assert_not_called()

    def test_rmdir_deletes_bucket(self):
        fs = self._make_fs()
        fs.allow_bucket_deletion = True
        fs.dircache[""] = ["stale-bucket-listing"]

        fs.rmdir("s3://bucket")
        fs._call.assert_called_once_with(fs._client.delete_bucket, Bucket="bucket")
        # The cached bucket listing must be evicted.
        assert "" not in fs.dircache

    def test_rmdir_bucket_deletion_disabled(self):
        # Bucket deletion is disabled by default.
        fs = self._make_fs()

        with pytest.raises(PermissionError, match="Bucket deletion is disabled"):
            fs.rmdir("s3://bucket")
        fs._call.assert_not_called()

    def test_touch_put_object(self):
        fs = self._make_fs()
        fs._call.return_value = {"ETag": '"e"'}

        assert fs.touch("s3://bucket/key", ContentType="text/plain")["_etag"] == '"e"'
        fs._call.assert_called_once_with(
            fs._client.put_object, Bucket="bucket", Key="key", ContentType="text/plain"
        )
        # touch() writes no data, so a body is rejected.
        fs._call.reset_mock()
        with pytest.raises(TypeError, match="body"):
            fs.touch("s3://bucket/key", body=b"data")
        fs._call.assert_not_called()

    def test_pipe_file_small_uses_put_object(self):
        fs = self._make_fs()
        fs.default_block_size = S3FileSystem.DEFAULT_BLOCK_SIZE
        fs.s3_additional_kwargs = {"ServerSideEncryption": "AES256"}
        fs.core.put_object = mock.MagicMock()

        # The filesystem-level s3_additional_kwargs are merged with the
        # call-level kwargs, as in the open() path.
        fs.pipe_file("s3://bucket/key", b"data", ContentType="text/plain")
        fs.core.put_object.assert_called_once_with(
            S3Path("bucket", "key"),
            b"data",
            ServerSideEncryption="AES256",
            ContentType="text/plain",
        )

    def test_requester_pays(self):
        # GH-969: RequestPayer is sent only with the operations that accept
        # it, and one given to a call does not conflict with it (GH-946).
        fs = S3FileSystem(
            key="dummy",
            secret="dummy",
            region_name="us-east-1",
            requester_pays=True,
            skip_instance_cache=True,
        )
        head_object = {"ContentLength": 1, "ETag": '"e"'}
        with Stubber(fs._client) as stubber:
            stubber.add_response("head_bucket", {}, {"Bucket": "bucket"})
            stubber.add_response(
                "head_object",
                head_object,
                {"Bucket": "bucket", "Key": "key", "RequestPayer": "requester"},
            )
            stubber.add_response(
                "head_object",
                head_object,
                {"Bucket": "bucket", "Key": "key2", "RequestPayer": "requester"},
            )
            fs.info("s3://bucket")
            fs.metadata("s3://bucket/key")
            fs.metadata("s3://bucket/key2", RequestPayer="requester")
            stubber.assert_no_pending_responses()
        assert fs.sign("s3://bucket/key").startswith("https://")

    def test_open_s3_additional_kwargs(self):
        # GH-969: the parameters of the call take precedence over those of
        # the filesystem, keyword parameters are added to them, each request
        # receives those that it accepts, and the caller's dictionary is not
        # modified.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs.s3_additional_kwargs = {"ServerSideEncryption": "AES256", "StorageClass": "STANDARD"}
        fs.info = mock.MagicMock(
            return_value=S3Object(
                init={"ContentLength": 3, "ETag": '"e"'},
                type=S3ObjectType.S3_OBJECT_TYPE_FILE,
                bucket="bucket",
                key="key",
            )
        )
        fs.core.get_object = mock.MagicMock(return_value=b"abc")
        fs.core.put_object = mock.MagicMock()
        kwargs = {"StorageClass": "GLACIER_IR", "ExpectedBucketOwner": "111122223333"}

        with fs.open("s3://bucket/key", "rb", s3_additional_kwargs=kwargs) as f:
            assert f.read() == b"abc"
        with fs.open(
            "s3://bucket/key", "wb", s3_additional_kwargs=kwargs, ContentType="text/csv"
        ) as f:
            f.write(b"x")

        assert kwargs == {"StorageClass": "GLACIER_IR", "ExpectedBucketOwner": "111122223333"}
        fs.core.get_object.assert_called_once_with(
            S3Path("bucket", "key"), (0, 3), ExpectedBucketOwner="111122223333", IfMatch='"e"'
        )
        fs.core.put_object.assert_called_once_with(
            S3Path("bucket", "key"),
            b"x",
            ServerSideEncryption="AES256",
            StorageClass="GLACIER_IR",
            ExpectedBucketOwner="111122223333",
            ContentType="text/csv",
        )

    @pytest.mark.parametrize("transaction", [False, True])
    def test_pipe_file_buffered_s3_parameters(self, transaction):
        # GH-969: the parameters of the call reach the upload when the data
        # goes through the buffered path, as on the single-request path.
        fs = S3FileSystem(
            key="dummy", secret="dummy", region_name="us-east-1", skip_instance_cache=True
        )
        fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"}
            )
        )
        fs.core.upload_part = mock.MagicMock(
            side_effect=lambda **kw: SimpleNamespace(etag='"e"', part_number=kw["part_number"])
        )
        fs.core.complete_multipart_upload = mock.MagicMock()
        fs.core.put_object = mock.MagicMock()
        data = b"x" * (fs.core.MULTIPART_UPLOAD_MIN_PART_SIZE + 1)

        if transaction:
            with fs.transaction:
                fs.pipe_file("s3://bucket/key", b"x", ContentType="text/csv")
            fs.core.put_object.assert_called_once_with(
                S3Path("bucket", "key"), b"x", ContentType="text/csv"
            )
        else:
            fs.pipe_file("s3://bucket/key", data, ContentType="text/csv")
            fs.core.create_multipart_upload.assert_called_once_with(
                S3Path("bucket", "key"), ContentType="text/csv"
            )

    def test_put_file_open_parameters(self, tmp_path):
        # GH-969: the open() parameters of put_file() go to open(), and the
        # other parameters, also in s3_additional_kwargs, to S3.
        fs = S3FileSystem(
            key="dummy", secret="dummy", region_name="us-east-1", skip_instance_cache=True
        )
        lpath = tmp_path / "data.csv"
        lpath.write_bytes(b"a")
        block_size = fs.core.MULTIPART_UPLOAD_MIN_PART_SIZE

        with (
            mock.patch.object(fs, "open", wraps=fs.open) as open_,
            Stubber(fs._client) as stubber,
        ):
            stubber.add_response(
                "put_object",
                {"ETag": '"e"'},
                {
                    "Bucket": "bucket",
                    "Key": "key",
                    "Body": b"a",
                    "ContentType": "text/csv",
                    "StorageClass": "STANDARD_IA",
                },
            )
            fs.put_file(
                str(lpath),
                "s3://bucket/key",
                block_size=block_size,
                max_workers=2,
                s3_additional_kwargs={"StorageClass": "STANDARD_IA"},
            )
            stubber.assert_no_pending_responses()

        open_.assert_called_once_with(
            "s3://bucket/key",
            "wb",
            block_size=block_size,
            max_workers=2,
            s3_additional_kwargs={"StorageClass": "STANDARD_IA", "ContentType": "text/csv"},
        )

    @staticmethod
    def _record_requests(fs, precondition_failed=False, exists=True):
        # Record the S3 requests of the filesystem by operation name, with an
        # object of 2 bytes at every key if it exists. With
        # precondition_failed, the conditional writes fail as S3 fails them
        # when an object exists.
        requests = []

        def call(method, **request):
            name = method if isinstance(method, str) else method._extract_mock_name()
            name = name.split(".")[-1]
            requests.append((name, request))
            if name == "head_object":
                if not exists:
                    raise FileNotFoundError(request["Key"])
                return {"ContentLength": 2, "ETag": '"e"'}
            if name == "get_object":
                return {"Body": io.BytesIO(b"aa")}
            if precondition_failed and name in {"put_object", "complete_multipart_upload"}:
                error = botocore.exceptions.ClientError(
                    {
                        "Error": {
                            "Code": "PreconditionFailed",
                            "Message": "At least one of the pre-conditions you specified "
                            "did not hold",
                            "Condition": "If-None-Match",
                        },
                        "ResponseMetadata": {"HTTPStatusCode": 412},
                    },
                    name,
                )
                raise S3ClientError(error).os_error from error
            if name == "create_multipart_upload":
                return {"Bucket": request["Bucket"], "Key": request["Key"], "UploadId": "uploadid"}
            return {"ETag": '"e"'}

        fs._call.side_effect = call
        return requests

    @pytest.mark.parametrize(
        ("size", "expected"),
        [
            # An empty file is created by touch().
            (0, ["put_object"]),
            (1, ["put_object"]),
            (
                S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE + 1,
                ["create_multipart_upload", "upload_part", "complete_multipart_upload"],
            ),
        ],
    )
    def test_open_exclusive_create(self, size, expected):
        # GH-972: "xb" used to replace an existing object. The upload is
        # committed only if no object exists, with IfNoneMatch on the
        # requests that accept it.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs.exists = mock.MagicMock(return_value=False)
        requests = self._record_requests(fs)

        with fs.open(
            "s3://bucket/key", "xb", block_size=fs.core.MULTIPART_UPLOAD_MIN_PART_SIZE
        ) as f:
            f.write(b"a" * size)

        fs.exists.assert_called_once()
        assert [name for name, _ in requests] == expected
        for name, request in requests:
            conditional = name in {"put_object", "complete_multipart_upload"}
            assert request.get("IfNoneMatch") == ("*" if conditional else None)

    def test_open_exclusive_create_existing(self):
        # GH-972: an existing object is found when the file is opened, before
        # any data is uploaded.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs.exists = mock.MagicMock(return_value=True)

        with pytest.raises(FileExistsError):
            fs.open("s3://bucket/key", "xb")
        fs._call.assert_not_called()

    @pytest.mark.parametrize(
        ("size", "expected"),
        [
            (1, ["put_object"]),
            (
                S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE + 1,
                [
                    "create_multipart_upload",
                    "upload_part",
                    "complete_multipart_upload",
                    "abort_multipart_upload",
                ],
            ),
        ],
    )
    def test_open_exclusive_create_created_since(self, size, expected):
        # GH-972: an object created after the file was opened is not
        # replaced. S3 rejects the conditional write, which raises
        # FileExistsError, and the multipart upload is aborted.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs.exists = mock.MagicMock(return_value=False)
        requests = self._record_requests(fs, precondition_failed=True)

        with (
            pytest.raises(FileExistsError),
            fs.open(
                "s3://bucket/key", "xb", block_size=fs.core.MULTIPART_UPLOAD_MIN_PART_SIZE
            ) as f,
        ):
            f.write(b"a" * size)

        assert [name for name, _ in requests] == expected

    @pytest.mark.parametrize(("mode", "open_mode"), [("overwrite", "wb"), ("create", "xb")])
    def test_put_file_mode(self, tmp_path, mode, open_mode):
        # GH-972: fsspec's mode argument used to be sent to PutObject. It
        # selects the mode of the remote file instead.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs.exists = mock.MagicMock(return_value=False)
        requests = self._record_requests(fs)
        lpath = tmp_path / "data"
        lpath.write_bytes(b"a")

        with mock.patch.object(fs, "open", wraps=fs.open) as open_:
            fs.put_file(str(lpath), "s3://bucket/key", mode=mode)

        assert open_.call_args.args == ("s3://bucket/key", open_mode)
        ((name, request),) = requests
        assert name == "put_object"
        assert "mode" not in request
        assert request.get("IfNoneMatch") == ("*" if mode == "create" else None)

    def test_put_file_create_existing(self, tmp_path):
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs.exists = mock.MagicMock(return_value=True)
        lpath = tmp_path / "data"
        lpath.write_bytes(b"a")

        with pytest.raises(FileExistsError):
            fs.put_file(str(lpath), "s3://bucket/key", mode="create")
        fs._call.assert_not_called()

    @pytest.mark.parametrize(
        ("size", "conditional"),
        [
            (1, "put_object"),
            (S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE + 1, "complete_multipart_upload"),
        ],
    )
    def test_pipe_file_create_created_since(self, size, conditional):
        # GH-972: pipe_file(mode="create") also writes conditionally, on the
        # single-request path as on the buffered one, so that an object
        # created after the existence check is not replaced.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs.default_block_size = fs.core.MULTIPART_UPLOAD_MIN_PART_SIZE
        fs.exists = mock.MagicMock(return_value=False)
        requests = self._record_requests(fs, precondition_failed=True)

        with pytest.raises(FileExistsError):
            fs.pipe_file("s3://bucket/key", b"a" * size, mode="create")

        fs.exists.assert_called_once()
        assert [name for name, request in requests if request.get("IfNoneMatch") == "*"] == [
            conditional
        ]

    @pytest.mark.parametrize("fail", [False, True])
    def test_open_parameters_named_as_request_fields(self, fail):
        # Parameters of a file named like the fields that a request sets
        # itself do not replace them, so the parts, the completion and the
        # abort use the upload of the file.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        requests = []

        def call(method, **request):
            name = method if isinstance(method, str) else method._extract_mock_name()
            name = name.split(".")[-1]
            requests.append((name, request))
            if name == "upload_part" and fail:
                raise OSError("upload failed")
            return {
                "Bucket": request["Bucket"],
                "Key": request["Key"],
                "UploadId": "uploadid",
                "ETag": '"e"',
            }

        fs._call.side_effect = call
        block_size = fs.core.MULTIPART_UPLOAD_MIN_PART_SIZE

        with (
            pytest.raises(OSError, match="upload failed") if fail else contextlib.nullcontext(),
            fs.open(
                "s3://bucket/key",
                "wb",
                block_size=block_size,
                Key="other",
                UploadId="other",
                PartNumber=99,
            ) as f,
        ):
            f.write(b"x" * (block_size + 1))

        names = [name for name, _ in requests]
        expected = "abort_multipart_upload" if fail else "complete_multipart_upload"
        assert names == ["create_multipart_upload", "upload_part", expected]
        for name, request in requests:
            assert request["Key"] == "key"
            if name != "create_multipart_upload":
                assert request["UploadId"] == "uploadid"
        assert requests[1][1]["PartNumber"] == 1

    def test_finish_multipart_upload_request_parameters(self):
        # GH-946: the completion and the abort receive the parameters of the
        # upload that they accept.
        fs = self._make_fs()
        upload = S3MultipartUpload({"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"})
        fs.core.complete_multipart_upload = mock.MagicMock()
        kwargs = {
            "ContentType": "text/csv",
            "RequestPayer": "requester",
            "SSECustomerAlgorithm": "AES256",
        }
        part: Future[SimpleNamespace] = Future()
        part.set_result(SimpleNamespace(etag='"e1"', part_number=1))

        fs._finish_multipart_upload(upload=upload, futures=[part], request_kwargs=kwargs)
        failed: Future[SimpleNamespace] = Future()
        failed.set_exception(RuntimeError("upload failed"))
        with pytest.raises(RuntimeError, match="upload failed"):
            fs._finish_multipart_upload(
                upload=upload,
                futures=[failed],
                request_kwargs=kwargs,
            )

        fs.core.complete_multipart_upload.assert_called_once_with(
            upload,
            [part.result()],
            RequestPayer="requester",
            SSECustomerAlgorithm="AES256",
        )
        fs._call.assert_called_once_with(
            fs._client.abort_multipart_upload,
            Bucket="bucket",
            Key="key",
            UploadId="uploadid",
            RequestPayer="requester",
        )

    def test_cp_file_directory(self):
        # GH-1008: recursive copy() passes the directories, which used to be
        # sent to CopyObject and fail with NoSuchKey.
        fs = self._make_fs()
        fs.info = mock.MagicMock(return_value=S3FileSystem._directory_object("bucket", "src"))
        fs.core.copy_object = mock.MagicMock()

        fs.cp_file("s3://bucket/src", "s3://bucket/dst")
        fs.core.copy_object.assert_not_called()
        fs._call.assert_not_called()

    @pytest.mark.parametrize(
        ("path1", "path2"),
        [
            # Several sources with the same destination.
            (["s3://bucket/a", "s3://bucket/b"], ["s3://bucket/c", "s3a://bucket/c"]),
            # A destination that is another source.
            (["s3://bucket/a", "s3://bucket/b"], ["s3://bucket/b", "s3://bucket/a"]),
            # A destination that is a source left in place.
            (["s3://bucket/a", "s3://bucket/b"], ["s3://bucket/b", "s3://bucket/b"]),
            # A destination whose "null" version is another source.
            (
                ["s3://bucket/a", "s3://bucket/b?versionId=null"],
                ["s3://bucket/b", "s3://bucket/out"],
            ),
        ],
    )
    def test_mv_conflicting_destinations(self, path1, path2):
        fs = self._make_fs()
        fs.info = mock.MagicMock()
        fs._call.return_value = {}

        with pytest.raises(ValueError, match="Cannot move"):
            fs.mv(path1, path2, recursive=True)
        fs.info.assert_not_called()
        if any(S3Path.parse(p).version_id == "null" for p in path1):
            fs._call.assert_called_once_with(fs._client.get_bucket_versioning, Bucket="bucket")
        else:
            fs._call.assert_not_called()

    def test_mv_conflicting_destinations_of_listed_sources(self):
        # A list of sources is mapped without looking up the destination.
        fs = self._make_fs()
        fs.expand_path = mock.MagicMock(return_value=["bucket/x/a", "bucket/y/a"])
        fs.isdir = mock.MagicMock()

        with pytest.raises(ValueError, match="same destination"):
            fs.mv(["s3://bucket/x/a", "s3://bucket/y/a"], "s3://bucket/out", recursive=True)
        fs.isdir.assert_not_called()
        fs._call.assert_not_called()

    def test_mv_keeps_given_paths(self):
        # The destination keeps its trailing slash, as with copy().
        fs = self._make_fs()
        fs._copy_file = mock.MagicMock(return_value=True)
        fs._delete_objects = mock.MagicMock()

        fs.mv(["s3://bucket/src"], ["s3://bucket/dst/"])
        fs._copy_file.assert_called_once_with("s3://bucket/src", "s3://bucket/dst/")
        fs._delete_objects.assert_called_once_with(["s3://bucket/src"])

    @pytest.mark.parametrize(
        ("keys", "path1", "path2", "expected"),
        [
            # The directory itself, which find() includes, is not copied.
            ({"d/a", "d/b"}, "s3://bucket/d/**", "s3://bucket/out/", {"out/a", "out/b"}),
            # The directory moved onto an existing subdirectory is no conflict.
            (
                {"src/a", "src/archive/x"},
                "s3://bucket/src/**",
                "s3://bucket/src/archive/",
                {"src/archive/a", "src/archive/archive/x"},
            ),
            # A directory shares its destination with an object.
            (
                {"d/x", "e/y"},
                ["s3://bucket/d", "s3://bucket/d/x", "s3://bucket/e/y"],
                ["s3://bucket/e", "s3://bucket/e", "s3://bucket/out"],
                {"e", "out"},
            ),
        ],
    )
    def test_mv_glob_with_directories(self, keys, path1, path2, expected):
        fs = self._make_fs()
        store = self._serve_keys(fs, keys)

        fs.mv(path1, path2, recursive=True)
        assert store == expected

    @pytest.mark.parametrize(
        ("keys", "path2"),
        [
            # Onto another source that is only an object.
            ({"d", "d/x", "e"}, ["e", "o/x", "o/e"]),
            # Onto another source that also has keys below it.
            ({"d", "d/x", "d/x/y"}, ["d/x", "o", "o/y"]),
        ],
    )
    def test_mv_objects_with_keys_below_conflict(self, keys, path2):
        # An object that also has keys below it is copied, so moving it onto
        # another source raises before anything is copied.
        fs = self._make_fs()
        store = self._serve_keys(fs, keys)

        with pytest.raises(ValueError, match="another path that is moved"):
            fs.mv(
                [f"s3://bucket/{k}" for k in sorted(keys)],
                [f"s3://bucket/{k}" for k in path2],
                recursive=True,
            )
        assert store == keys
        methods = {c.args[0] for c in fs._call.call_args_list}
        assert fs._client.copy_object not in methods
        assert fs._client.delete_objects not in methods

    def test_mv_version_with_keys_below_conflicts(self):
        # A version names an object even with keys below its key, so it is
        # not taken for a directory when no current object exists at the key.
        fs = self._make_fs()
        self._serve_keys(fs, {"d/x", "a"})

        with pytest.raises(ValueError, match="same destination"):
            fs.mv(
                ["s3://bucket/d?versionId=null", "s3://bucket/d/x", "s3://bucket/a"],
                ["s3://bucket/out", "s3://bucket/x", "s3://bucket/out"],
            )
        methods = {c.args[0] for c in fs._call.call_args_list}
        assert fs._client.copy_object not in methods
        assert fs._client.delete_objects not in methods

    def test_mv_versions_onto_their_key(self):
        fs = self._make_fs()
        self._serve_keys(fs, {"b"})

        # In this unversioned bucket, the "null" version names the key and stays in place.
        fs.mv(["s3://bucket/b?versionId=null"], ["s3://bucket/b"])
        fs._call.assert_called_once_with(fs._client.get_bucket_versioning, Bucket="bucket")

        # Another version is copied onto the key, and then deleted.
        fs.mv(["s3://bucket/b?versionId=v1"], ["s3://bucket/b"])
        copies = [c.kwargs for c in fs._call.call_args_list if c.args[0] is fs._client.copy_object]
        assert [(c["CopySource"].get("VersionId"), c["Key"]) for c in copies] == [("v1", "b")]
        deletes = [
            c.kwargs["Delete"]["Objects"]
            for c in fs._call.call_args_list
            if c.args[0] is fs._client.delete_objects
        ]
        assert deletes == [[{"Key": "b", "VersionId": "v1"}]]

    @pytest.mark.parametrize("status", [None, "Suspended", "Enabled"])
    def test_mv_null_version_onto_key(self, status):
        fs = self._make_fs()
        fs._call.return_value = {} if status is None else {"Status": status}
        fs._copy_file = mock.MagicMock(return_value=True)
        fs._delete_objects = mock.MagicMock()
        sources = ["s3://bucket/a?versionId=null", "s3a://bucket/b?version_id=null"]
        destinations = ["s3://bucket/a", "s3://bucket/b"]

        fs.mv(sources, destinations, MetadataDirective="COPY")

        fs._call.assert_called_once_with(fs._client.get_bucket_versioning, Bucket="bucket")
        if status == "Enabled":
            assert fs._copy_file.call_args_list == [
                mock.call(source, dest, MetadataDirective="COPY")
                for source, dest in zip(sources, destinations, strict=True)
            ]
            fs._delete_objects.assert_called_once_with(sources)
        else:
            fs._copy_file.assert_not_called()
            fs._delete_objects.assert_called_once_with([])

    def test_mv_null_version_request_versions(self):
        fs = S3FileSystem(
            key="dummy", secret="dummy", region_name="us-east-1", skip_instance_cache=True
        )
        with Stubber(fs._client) as stubber:
            stubber.add_response(
                "get_bucket_versioning", {"Status": "Enabled"}, {"Bucket": "bucket"}
            )
            stubber.add_response(
                "head_object",
                {"ContentLength": 3, "VersionId": "null"},
                {"Bucket": "bucket", "Key": "key", "VersionId": "null"},
            )
            stubber.add_response(
                "copy_object",
                {"CopyObjectResult": {"ETag": '"copied"'}, "VersionId": "new"},
                {
                    "Bucket": "bucket",
                    "Key": "key",
                    "CopySource": {"Bucket": "bucket", "Key": "key", "VersionId": "null"},
                },
            )
            stubber.add_response(
                "delete_objects",
                {},
                {
                    "Bucket": "bucket",
                    "Delete": {"Objects": [{"Key": "key", "VersionId": "null"}], "Quiet": True},
                },
            )

            fs.mv(["s3://bucket/key?versionId=null"], ["s3://bucket/key"])
            stubber.assert_no_pending_responses()

    @pytest.mark.parametrize("status", [None, "Suspended", "Enabled"])
    def test_mv_null_version_conflicts_depend_on_bucket_state(self, status):
        fs = self._make_fs()
        fs._call.return_value = {} if status is None else {"Status": status}
        fs._copy_file = mock.MagicMock(return_value=True)
        fs._delete_objects = mock.MagicMock()
        sources = ["s3://bucket/a", "s3://bucket/b?versionId=null"]
        destinations = ["s3a://bucket/b", "s3://bucket/out"]

        with (
            contextlib.nullcontext()
            if status == "Enabled"
            else pytest.raises(ValueError, match="another path that is moved")
        ):
            fs.mv(sources, destinations)

        fs._call.assert_called_once_with(fs._client.get_bucket_versioning, Bucket="bucket")
        if status == "Enabled":
            assert fs._copy_file.call_args_list == [
                mock.call(source, dest) for source, dest in zip(sources, destinations, strict=True)
            ]
            fs._delete_objects.assert_called_once_with(sources)
        else:
            fs._copy_file.assert_not_called()
            fs._delete_objects.assert_not_called()

    @pytest.mark.parametrize(
        ("source", "dest"),
        [
            ("s3://bucket/a", "s3://bucket/b"),
            ("s3://bucket/a?versionId=v1", "s3://bucket/a"),
            ("s3://bucket/a?versionId=null", "s3://bucket/b"),
            ("s3://bucket/a?versionId=null", "s3a://bucket/a?version_id=null"),
        ],
    )
    def test_mv_without_null_key_comparison_does_not_read_bucket_state(self, source, dest):
        fs = self._make_fs()
        fs._copy_file = mock.MagicMock(return_value=True)
        fs._delete_objects = mock.MagicMock()

        fs.mv([source], [dest])
        fs._call.assert_not_called()

    @pytest.mark.parametrize("same_key", [False, True])
    def test_mv_null_version_directory_bucket_does_not_read_bucket_state(self, same_key):
        fs = self._make_fs()
        fs._copy_file = mock.MagicMock(return_value=True)
        fs._delete_objects = mock.MagicMock()
        key = "s3://example--usw2-az1--x-s3/key"
        sources = [f"{key}?versionId=null"] if same_key else [f"{key}?versionId=null", key]
        destinations = [key] if same_key else [f"{key}-copy1", f"{key}-copy2"]

        fs.mv(sources, destinations)

        fs._call.assert_not_called()
        if same_key:
            fs._copy_file.assert_not_called()
            fs._delete_objects.assert_called_once_with([])
        else:
            assert fs._copy_file.call_args_list == [
                mock.call(source, dest) for source, dest in zip(sources, destinations, strict=True)
            ]
            fs._delete_objects.assert_called_once_with(sources)

    def test_mv_null_version_reads_each_bucket(self):
        fs = self._make_fs()
        fs._call.side_effect = lambda method, **request: (
            {"Status": "Enabled"} if request["Bucket"] == "enabled" else {"Status": "Suspended"}
        )
        fs._copy_file = mock.MagicMock(return_value=True)
        fs._delete_objects = mock.MagicMock()

        fs.mv(
            ["s3://enabled/key?versionId=null", "s3://suspended/key?versionId=null"],
            ["s3://enabled/key", "s3://suspended/key"],
        )
        assert fs._call.call_count == 2
        fs._call.assert_has_calls(
            [
                mock.call(fs._client.get_bucket_versioning, Bucket="enabled"),
                mock.call(fs._client.get_bucket_versioning, Bucket="suspended"),
            ],
            any_order=True,
        )
        fs._copy_file.assert_called_once_with("s3://enabled/key?versionId=null", "s3://enabled/key")
        fs._delete_objects.assert_called_once_with(["s3://enabled/key?versionId=null"])

    def test_mv_null_version_does_not_cache_bucket_state(self):
        fs = self._make_fs()
        fs._call.side_effect = [{}, {"Status": "Enabled"}, {"Status": "Suspended"}]
        fs._copy_file = mock.MagicMock(return_value=True)
        fs._delete_objects = mock.MagicMock()
        source = "s3://bucket/key?versionId=null"

        for _ in range(3):
            fs.mv([source], ["s3://bucket/key"])
        assert fs._call.call_count == 3
        fs._copy_file.assert_called_once_with(source, "s3://bucket/key")
        assert fs._delete_objects.call_args_list == [
            mock.call([]),
            mock.call([source]),
            mock.call([]),
        ]

    @pytest.mark.parametrize("stage", ["lookup", "copy"])
    def test_mv_null_version_failure_does_not_delete(self, stage):
        fs = self._make_fs()
        fs._call.return_value = {"Status": "Enabled"}
        fs._copy_file = mock.MagicMock(return_value=True)
        fs._delete_objects = mock.MagicMock()
        if stage == "lookup":
            fs._call.side_effect = PermissionError("Access Denied")
        else:
            fs._copy_file.side_effect = PermissionError("Access Denied")

        with pytest.raises(PermissionError, match="Access Denied"):
            fs.mv(
                ["s3://bucket/other", "s3://bucket/key?versionId=null"],
                ["s3://bucket/out", "s3://bucket/key"],
            )
        fs._delete_objects.assert_not_called()
        if stage == "lookup":
            fs._copy_file.assert_not_called()

    def test_question_mark_keys(self):
        # GH-979: keys containing "?" are keys, not version ID queries.
        fs = self._make_fs()
        keys = self._serve_keys(fs, {"dir/a.txt", "dir/what?.txt", "dir/q?version_id"})

        assert fs.ls("s3://bucket/dir") == [
            "bucket/dir/a.txt",
            "bucket/dir/q?version_id",
            "bucket/dir/what?.txt",
        ]
        assert fs.info("s3://bucket/dir/what?.txt")["name"] == "bucket/dir/what?.txt"
        fs.invalidate_cache()
        assert fs.exists("s3://bucket/dir/q?version_id")
        heads = [c.kwargs for c in fs._call.call_args_list if c.args[0] is fs._client.head_object]
        assert heads[-1] == {"Bucket": "bucket", "Key": "dir/q?version_id"}

        fs.rm("s3://bucket/dir", recursive=True)
        assert keys == set()

    def test_invalidate_cache_question_mark_key(self):
        # A "?" that does not start a trailing version ID query is part of
        # the key, whose parents are invalidated.
        fs = self._make_fs()
        for key in ("bucket/dir/what?.txt", ("bucket/dir", "/"), "bucket/dir/what"):
            fs.dircache[key] = []

        fs.invalidate_cache("s3://bucket/dir/what?.txt")
        assert list(fs.dircache) == ["bucket/dir/what"]

    @pytest.mark.parametrize(
        "kwargs",
        [{"path": "s3://bucket/dir?versionId=v1"}, {"path": "s3://bucket/dir", "version_id": "v1"}],
    )
    def test_info_missing_version_is_not_a_prefix(self, kwargs):
        # GH-979: a version names an object, so a missing version is not
        # found even if its key is a key prefix, which info() used to return
        # as a directory.
        fs = self._make_fs()
        self._serve_keys(fs, {"dir/child"})

        with pytest.raises(FileNotFoundError):
            fs.info(**kwargs)
        assert not fs.exists("s3://bucket/dir?versionId=v1")
        methods = {c.args[0] for c in fs._call.call_args_list}
        assert fs._client.list_objects_v2 not in methods

    @pytest.mark.parametrize("recursive", [False, True])
    def test_expand_path_version(self, recursive):
        # GH-979: "?" of a version ID query is not a glob character, and a
        # version is not expanded below its key.
        fs = self._make_fs()
        self._serve_keys(fs, {"b", "bc", "c"})

        assert fs.expand_path("s3://bucket/b?versionId=v1", recursive=recursive) == [
            "bucket/b?versionId=v1"
        ]
        assert fs.expand_path(
            ["s3://bucket/b?versionId=v1", "s3://bucket/c*"], recursive=recursive
        ) == ["bucket/b?versionId=v1", "bucket/c"]
        with pytest.raises(ValueError, match="maxdepth"):
            fs.expand_path("s3://bucket/b?versionId=v1", maxdepth=0)

    def test_expand_path_recursive_version_lookup_error(self):
        # A failed lookup is raised, not taken for a missing version.
        fs = self._make_fs()
        self._serve_keys(fs, {"a", "b"})
        serve = fs._call.side_effect

        def call(method, **kwargs):
            if method is fs._client.head_object and kwargs["Key"] == "b":
                raise PermissionError("b")
            return serve(method, **kwargs)

        fs._call.side_effect = call
        with pytest.raises(PermissionError):
            fs.expand_path(
                ["s3://bucket/a?versionId=v1", "s3://bucket/b?versionId=v2"], recursive=True
            )

    def test_expand_path_recursive_missing_version(self):
        # With recursive, a version is included only if it is a file, as
        # fsspec includes a path that exists; a key prefix of the same name
        # is not a version.
        fs = self._make_fs()
        self._serve_keys(fs, {"b", "dir/child"})

        assert fs.expand_path(
            ["s3://bucket/b?versionId=v1", "s3://bucket/dir?versionId=v2"], recursive=True
        ) == ["bucket/b?versionId=v1"]
        with pytest.raises(FileNotFoundError):
            fs.expand_path("s3://bucket/dir?versionId=v2", recursive=True)

    @pytest.mark.parametrize(
        ("path1", "path2", "expected"),
        [
            ("s3://bucket/b?versionId=v1", "s3://bucket/out", "out"),
            ("s3://bucket/b?version_id=v1", "s3://bucket/d/", "d/b"),
            (["s3://bucket/b?versionId=v1"], "s3://bucket/d", "d/b"),
            # The key may contain "?" too.
            ("s3://bucket/q?x?versionId=v1", "s3://bucket/d/", "d/q?x"),
        ],
    )
    @pytest.mark.parametrize("method", ["copy", "mv"])
    def test_copy_version(self, method, path1, path2, expected):
        # GH-979: a version is copied to a destination named after its key,
        # not globbed with "?" as a wildcard.
        fs = self._make_fs()
        self._serve_keys(fs, {"b", "q?x", "d/x"})

        getattr(fs, method)(path1, path2)
        copies = [c.kwargs for c in fs._call.call_args_list if c.args[0] is fs._client.copy_object]
        source = S3Path.parse(path1 if isinstance(path1, str) else path1[0])
        assert [(c["CopySource"], c["Key"]) for c in copies] == [
            ({"Bucket": "bucket", "Key": source.key, "VersionId": "v1"}, expected)
        ]

    @pytest.mark.parametrize(
        ("rpath", "lpath", "expected"),
        [
            ("s3://bucket/key?versionId=v1", "f.txt", "f.txt"),
            ("s3://bucket/key?versionId=v1", "d/", "d/key"),
            (["s3://bucket/key?versionId=v1"], "d", "d/key"),
            (Path("bucket/key?versionId=v1"), "d/", "d/key"),
            ("s3://bucket/key?versionId=v1", Path("f.txt"), "f.txt"),
            # A sequence of destinations is paired by fsspec, as before.
            ("s3://bucket/key?versionId=v1", ("f.txt",), "f.txt"),
        ],
    )
    def test_get_version(self, tmp_path, monkeypatch, rpath, lpath, expected):
        # Path sources and destinations are accepted as fsspec accepts them.
        # GH-979: a version is downloaded to a local path named after its
        # key, which used to be the version-qualified name of the source.
        monkeypatch.chdir(tmp_path)
        (tmp_path / "d").mkdir()
        fs, _ = self._make_object_fs(b"data")

        fs.get(rpath, lpath)
        assert sorted(str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*")) == sorted(
            {"d", expected}
        )
        assert (tmp_path / expected).read_bytes() == b"data"

    @pytest.mark.parametrize(
        "rpath",
        [
            "s3://bucket/a/..?versionId=v1",
            # The sources without a version are named by the same pairing.
            ["s3://bucket/x/..", "s3://bucket/key?versionId=v1"],
        ],
    )
    def test_get_version_outside_destination(self, tmp_path, rpath):
        # A destination named after a key must stay under lpath, which
        # fsspec checks for the destinations that it names.
        (tmp_path / "d").mkdir()
        fs, _ = self._make_object_fs(b"data")

        with pytest.raises(ValueError, match="outside"):
            fs.get(rpath, f"{tmp_path}/d/")
        assert sorted(p.name for p in tmp_path.rglob("*")) == ["d"]

    @pytest.mark.parametrize(
        ("path2", "lookups", "expected"),
        [
            # The source is checked for a directory; the destination decides
            # the pairing, so it is looked up too.
            ("s3://bucket/d", ["bucket/b?versionId=v1", "s3://bucket/d"], "s3://bucket/d/b"),
            # A trailing slash decides it without a lookup.
            ("s3://bucket/d/", ["bucket/b?versionId=v1"], "s3://bucket/d/b"),
        ],
    )
    def test_copy_pairs_destination_lookup(self, path2, lookups, expected):
        fs = self._make_fs()
        self._serve_keys(fs, {"b"})
        # Only the destination is a directory.
        fs.isdir = mock.MagicMock(side_effect=lambda p: p.rstrip("/").endswith("/d"))

        pairs = fs._copy_pairs(S3PathPairing("s3://bucket/b?versionId=v1", path2))

        assert pairs == [("bucket/b?versionId=v1", expected)]
        assert [c.args[0] for c in fs.isdir.call_args_list] == lookups

    def test_move_pairs_looks_up_only_conflict_candidates(self):
        # Only a source that may be a directory and whose destination
        # conflicts is looked up with HeadObject.
        fs = self._make_fs()
        self._serve_keys(fs, {"d/x", "e/y", "f"})
        fs._head_object = mock.MagicMock(return_value=None)

        pairs = fs._move_pairs(
            ["s3://bucket/d", "s3://bucket/d/x", "s3://bucket/e/y", "s3://bucket/f"],
            ["s3://bucket/e", "s3://bucket/e", "s3://bucket/out", "s3://bucket/g"],
        )

        assert len(pairs) == 4
        fs._head_object.assert_called_once_with("bucket/d")

    def test_freed_by_reference_counting(self):
        # The filesystem holds no reference cycle, so a filesystem that is
        # not cached, such as the internal one of a cursor (GH-978), is freed
        # as soon as it is unused.
        fs = self._stubbed_fs()
        ref = weakref.ref(fs)
        gc.disable()
        try:
            del fs
            assert ref() is None
        finally:
            gc.enable()

    def test_mv_nothing_within_maxdepth(self):
        # Only directories within maxdepth: nothing is moved, as with copy().
        fs = self._make_fs()
        fs.expand_path = mock.MagicMock(return_value=["bucket/src/sub"])
        fs.isdir = mock.MagicMock(return_value=True)

        fs.mv("s3://bucket/src", "s3://bucket/dst/", recursive=True, maxdepth=1)
        fs._call.assert_not_called()

    @pytest.mark.parametrize("size", [10, 5 * 2**30 + 1])
    def test_cp_file_multipart_parameters(self, size):
        # GH-967: block_size and max_workers control a multipart copy and are
        # not sent to S3, whatever the size of the object.
        fs = self._make_fs()
        fs.info = mock.MagicMock(
            return_value=S3Object(
                init={"ContentLength": size},
                type=S3ObjectType.S3_OBJECT_TYPE_FILE,
                bucket="bucket",
                key="src",
            )
        )
        fs.core.copy_object = mock.MagicMock()
        fs._copy_object_with_multipart_upload = mock.MagicMock()

        fs.cp_file(
            "s3://bucket/src",
            "s3://bucket/dst",
            block_size=fs.core.MULTIPART_UPLOAD_MIN_PART_SIZE,
            max_workers=2,
            RequestPayer="requester",
        )

        if size <= fs.core.MULTIPART_UPLOAD_MAX_PART_SIZE:
            fs.core.copy_object.assert_called_once_with(
                S3Path("bucket", "src"), S3Path("bucket", "dst"), RequestPayer="requester"
            )
        else:
            fs._copy_object_with_multipart_upload.assert_called_once_with(
                S3Path("bucket", "src"),
                S3Path("bucket", "dst"),
                max_workers=2,
                block_size=fs.core.MULTIPART_UPLOAD_MIN_PART_SIZE,
                RequestPayer="requester",
            )

    def test_copy_object_with_multipart_upload_request_parameters(self):
        # GH-946: the part copies, the completion and the abort receive the
        # parameters of the copy that they accept.
        fs = self._make_fs()
        fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "dst", "UploadId": "uploadid"}
            )
        )
        fs.core.upload_part_copy = mock.MagicMock(
            side_effect=lambda **kw: SimpleNamespace(etag='"e"', part_number=kw["part_number"])
        )
        fs._finish_multipart_upload = mock.MagicMock()
        fs._call.return_value = {"ContentLength": 5 * 2**30 + 2**20}
        # The directives make the copy use the given values without reading
        # the source's metadata, tags and annotations (GH-973).
        directives = {
            "MetadataDirective": "REPLACE",
            "TaggingDirective": "REPLACE",
            "AnnotationDirective": "EXCLUDE",
        }
        kwargs = {"ContentType": "text/csv", "RequestPayer": "requester", **directives}

        fs._copy_object_with_multipart_upload(
            S3Path("bucket", "src"), S3Path("bucket", "dst"), **kwargs
        )

        fs.core.create_multipart_upload.assert_called_once_with(
            S3Path("bucket", "dst"), ContentType="text/csv", RequestPayer="requester"
        )
        # Only the HeadObject of the source, for its version.
        assert fs._call.call_count == 1
        assert all(
            c.kwargs["RequestPayer"] == "requester" and "ContentType" not in c.kwargs
            for c in fs.core.upload_part_copy.call_args_list
        )
        assert fs._finish_multipart_upload.call_args.kwargs["request_kwargs"] == {
            "RequestPayer": "requester"
        }

    @staticmethod
    def _stubbed_copy_fs(**kwargs):
        # max_workers=1 copies the parts in the order of the stubbed
        # responses.
        return S3FileSystem(
            key="dummy",
            secret="dummy",
            region_name="us-east-1",
            skip_instance_cache=True,
            max_workers=1,
            **kwargs,
        )

    @staticmethod
    def _multipart_copy(fs, source_bucket="bucket", **kwargs):
        fs._copy_object_with_multipart_upload(
            S3Path(source_bucket, "src"),
            S3Path("bucket", "dst"),
            block_size=MULTIPART_COPY_BLOCK_SIZE,
            **kwargs,
        )

    def test_copy_object_with_multipart_upload_copies_source(self):
        # GH-973: as CopyObject does by default, the multipart copy copies
        # the content headers, the user-defined metadata, the tags and the
        # annotations of the source, ignoring the values of the copy; the
        # source condition goes to the part copies, and the source's lookups
        # get the source's expected bucket owner.
        fs = self._stubbed_copy_fs()
        with Stubber(fs._client) as stubber:
            stub_multipart_copy(stubber)
            self._multipart_copy(fs, **MULTIPART_COPY_KWARGS)
            stubber.assert_no_pending_responses()

    def test_copy_object_with_multipart_upload_failed_listing(self):
        # GH-973: the annotations are listed before the upload is created, so
        # a caller without s3:ListObjectAnnotations fails before anything is
        # written.
        fs = self._stubbed_copy_fs()
        with Stubber(fs._client) as stubber:
            stub_multipart_copy(stubber, fail_list=True)
            with pytest.raises(PermissionError):
                self._multipart_copy(fs, **MULTIPART_COPY_KWARGS)
            stubber.assert_no_pending_responses()

    def test_copy_object_with_multipart_upload_failed_part(self):
        fs = self._stubbed_copy_fs()
        with Stubber(fs._client) as stubber:
            stub_multipart_copy(stubber, fail_part=True)
            with pytest.raises(OSError, match="part failed"):
                self._multipart_copy(fs, **MULTIPART_COPY_KWARGS)
            stubber.assert_no_pending_responses()

    def test_copy_object_with_multipart_upload_failed_annotation(self):
        # GH-973: a failed annotation copy is raised; the completed
        # destination is neither aborted nor deleted.
        fs = self._stubbed_copy_fs()
        with Stubber(fs._client) as stubber:
            stub_multipart_copy(stubber, fail_annotation=True)
            with pytest.raises(PermissionError):
                self._multipart_copy(fs, **MULTIPART_COPY_KWARGS)
            stubber.assert_no_pending_responses()

    def test_cp_file_failed_multipart_copy_invalidates_cache(self):
        # GH-973: a multipart copy that fails to copy an annotation has
        # written the destination, so its cached entries are removed.
        fs = self._make_fs()
        fs.info = mock.MagicMock(return_value=self._file_object("src"))
        fs.info.return_value.size = S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE + 1
        fs._copy_object_with_multipart_upload = mock.MagicMock(side_effect=PermissionError)
        fs.dircache["bucket/dst"] = [self._file_object("dst")]

        with pytest.raises(PermissionError):
            fs.cp_file("s3://bucket/src", "s3://bucket/dst")
        assert "bucket/dst" not in fs.dircache

    def test_copy_object_with_multipart_upload_head_object_size(self):
        # GH-973: the ranges cover the size that HeadObject reports for the
        # copied object, not a cached size, and the "null" version of a
        # bucket with versioning suspended is not pinned.
        fs = self._make_fs()
        block_size = MULTIPART_COPY_BLOCK_SIZE
        fs._call.return_value = {
            "ContentLength": 2 * block_size + S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE,
            "VersionId": "null",
        }
        fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "dst", "UploadId": "uploadid"}
            )
        )
        fs.core.upload_part_copy = mock.MagicMock()
        fs._finish_multipart_upload = mock.MagicMock()

        self._multipart_copy(
            fs,
            MetadataDirective="REPLACE",
            TaggingDirective="REPLACE",
            AnnotationDirective="EXCLUDE",
        )

        # The parts are copied in parallel, in any order.
        parts = sorted(
            (c.kwargs["range_"], c.kwargs["source"])
            for c in fs.core.upload_part_copy.call_args_list
        )
        source = S3Path("bucket", "src")
        assert parts == [
            ((0, block_size), source),
            ((block_size, 2 * block_size), source),
            ((2 * block_size, fs._call.return_value["ContentLength"]), source),
        ]

    @pytest.mark.parametrize("size", [0, 10])
    def test_copy_object_with_multipart_upload_small_head_object_size(self, size):
        # GH-973: when a cached size over 5 GiB is stale and HeadObject
        # reports a size that fits in a single CopyObject request, including
        # an empty object, the reported version is copied with CopyObject.
        fs = self._make_fs()
        fs._call.return_value = {"ContentLength": size, "VersionId": "v1"}
        fs.core.copy_object = mock.MagicMock()
        fs.core.create_multipart_upload = mock.MagicMock()

        self._multipart_copy(fs, ContentType="text/csv", RequestPayer="requester")

        fs.core.copy_object.assert_called_once_with(
            S3Path("bucket", "src", "v1"),
            S3Path("bucket", "dst"),
            ContentType="text/csv",
            RequestPayer="requester",
        )
        fs.core.create_multipart_upload.assert_not_called()
        # Only HeadObject; the tags are not read for the multipart upload.
        assert fs._call.call_count == 1

    def test_copy_object_with_multipart_upload_replace_directives(self):
        # GH-973: REPLACE uses the values of the copy without reading the
        # source, and EXCLUDE skips the annotations.
        fs = self._stubbed_copy_fs()
        with Stubber(fs._client) as stubber:
            # Read only for the version, which a bucket without versioning
            # does not report.
            stubber.add_response(
                "head_object",
                {"ContentLength": MULTIPART_COPY_SIZE, "ContentType": "text/csv"},
                None,
            )
            stubber.add_response(
                "create_multipart_upload",
                {"Bucket": "bucket", "Key": "dst", "UploadId": "u"},
                {"Bucket": "bucket", "Key": "dst", "ContentType": "text/plain", "Tagging": "a=1"},
            )
            for _ in (1, 2):
                stubber.add_response("upload_part_copy", {"CopyPartResult": {"ETag": '"p"'}}, None)
            stubber.add_response("complete_multipart_upload", {"ETag": '"dst"'}, None)
            self._multipart_copy(
                fs,
                ContentType="text/plain",
                Tagging="a=1",
                MetadataDirective="REPLACE",
                TaggingDirective="REPLACE",
                AnnotationDirective="EXCLUDE",
            )
            stubber.assert_no_pending_responses()

    @pytest.mark.parametrize(
        "directive",
        [
            {"MetadataDirective": "EXCLUDE"},
            {"TaggingDirective": "copy"},
            {"AnnotationDirective": "REPLACE"},
        ],
    )
    def test_copy_object_with_multipart_upload_invalid_directive(self, directive):
        fs = self._stubbed_copy_fs()
        with Stubber(fs._client), pytest.raises(ValueError, match="Invalid"):
            self._multipart_copy(fs, **directive)

    def test_copy_object_with_multipart_upload_sse_c_source(self):
        # GH-973: the source's SSE-C key reaches its HeadObject, and an SSE-C
        # object, which cannot have annotations, is not listed for them.
        fs = self._stubbed_copy_fs()
        sse_c = {"CopySourceSSECustomerAlgorithm": "AES256", "CopySourceSSECustomerKey": "k" * 32}
        with Stubber(fs._client) as stubber:
            stubber.add_response(
                "head_object",
                {"ContentLength": MULTIPART_COPY_SIZE, "ContentType": "text/csv"},
                {
                    "Bucket": "bucket",
                    "Key": "src",
                    "SSECustomerAlgorithm": "AES256",
                    "SSECustomerKey": "k" * 32,
                },
            )
            stubber.add_response(
                "create_multipart_upload",
                {"Bucket": "bucket", "Key": "dst", "UploadId": "u"},
                {"Bucket": "bucket", "Key": "dst", "ContentType": "text/csv", "Metadata": {}},
            )
            for _ in (1, 2):
                stubber.add_response("upload_part_copy", {"CopyPartResult": {"ETag": '"p"'}}, None)
            stubber.add_response("complete_multipart_upload", {"ETag": '"dst"'}, None)
            self._multipart_copy(fs, TaggingDirective="REPLACE", **sse_c)
            stubber.assert_no_pending_responses()

    def test_copy_object_with_multipart_upload_directory_bucket_source(self):
        # GH-973: objects in a directory bucket have neither tags nor
        # annotations, and the bucket supports neither GetObjectTagging nor
        # ListObjectAnnotations.
        fs = self._stubbed_copy_fs()
        bucket = "bucket--usw2-az1--x-s3"
        with Stubber(fs._client) as stubber:
            stubber.add_response(
                "head_object",
                {"ContentLength": MULTIPART_COPY_SIZE, "ContentType": "text/csv"},
                {"Bucket": bucket, "Key": "src"},
            )
            stubber.add_response(
                "create_multipart_upload",
                {"Bucket": "bucket", "Key": "dst", "UploadId": "u"},
                {"Bucket": "bucket", "Key": "dst", "ContentType": "text/csv", "Metadata": {}},
            )
            for _ in (1, 2):
                stubber.add_response("upload_part_copy", {"CopyPartResult": {"ETag": '"p"'}}, None)
            stubber.add_response("complete_multipart_upload", {"ETag": '"dst"'}, None)
            self._multipart_copy(fs, source_bucket=bucket)
            stubber.assert_no_pending_responses()

    def test_pipe_file_invalid_path_raises(self):
        fs = self._make_fs()
        with pytest.raises(ValueError, match="Cannot write to a bucket"):
            fs.pipe_file("s3://bucket", b"data")
        with pytest.raises(ValueError, match="version"):
            fs.pipe_file("s3://bucket/key?versionId=12345abcde", b"data")

    def test_pipe_file_non_contiguous_memoryview(self):
        # A non-contiguous memoryview within the block size, 4 items of 2
        # bytes here, is uploaded with PutObject.
        fs = self._make_fs()
        fs.core.put_object = mock.MagicMock()
        value = memoryview(b"ab" * 8).cast("H")[::2]

        fs.pipe_file("s3://bucket/key", value, block_size=8)

        fs.core.put_object.assert_called_once_with(S3Path("bucket", "key"), b"ab" * 4)

    @pytest.mark.parametrize("intrans", [False, True])
    @pytest.mark.parametrize("size", [1, S3FileSystem.DEFAULT_BLOCK_SIZE + 1])
    def test_pipe_file_trailing_slash(self, intrans, size):
        # GH-1037: a path with a trailing slash is written without it, as
        # open() writes it, whatever the size of the value. The single
        # request used to write the key with the trailing slash.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs._transaction = None
        fs.core.put_object = mock.MagicMock()
        fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"}
            )
        )
        fs.core.upload_part = mock.MagicMock(
            side_effect=lambda **kw: SimpleNamespace(etag='"e"', part_number=kw["part_number"])
        )
        fs.core.complete_multipart_upload = mock.MagicMock()

        with fs.transaction if intrans else contextlib.nullcontext():
            fs.pipe_file("s3://bucket/dir/key/", b"a" * size)

        keys = [c.args[0].key for c in fs.core.put_object.call_args_list] + [
            c.args[0].key for c in fs.core.create_multipart_upload.call_args_list
        ]
        assert keys == ["dir/key"]

    def test_pipe_file_memoryview_routed_by_bytes(self):
        # A memoryview larger than the block size in bytes, but not in items,
        # is uploaded as a multipart upload. Its item count used to route it
        # to PutObject, which accepts at most 5 GiB.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs.core.put_object = mock.MagicMock()
        fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"}
            )
        )
        fs.core.upload_part = mock.MagicMock(
            side_effect=lambda **kw: SimpleNamespace(etag='"e"', part_number=kw["part_number"])
        )
        fs.core.complete_multipart_upload = mock.MagicMock()
        data = b"a" * (S3FileSystem.DEFAULT_BLOCK_SIZE + 4)

        fs.pipe_file("s3://bucket/key", memoryview(data).cast("I"))

        fs.core.put_object.assert_not_called()
        assert b"".join(c.kwargs["body"] for c in fs.core.upload_part.call_args_list) == data
        fs.core.complete_multipart_upload.assert_called_once()

    def test_pipe_file_small_drops_max_workers(self):
        fs = self._make_fs()
        fs.core.put_object = mock.MagicMock()

        # max_workers is an open() parameter and is not sent to PutObject.
        fs.pipe_file("s3://bucket/key", b"data", max_workers=2)
        fs.core.put_object.assert_called_once_with(S3Path("bucket", "key"), b"data")

    def test_pipe_file_buffered_non_contiguous_memoryview(self):
        # GH-997: a non-contiguous memoryview larger than the block size is
        # uploaded as a multipart upload; the buffer of the file used to
        # raise BufferError for it.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"}
            )
        )
        fs.core.upload_part = mock.MagicMock(
            side_effect=lambda **kw: SimpleNamespace(etag='"e"', part_number=kw["part_number"])
        )
        fs.core.complete_multipart_upload = mock.MagicMock()
        size = S3FileSystem.DEFAULT_BLOCK_SIZE + 1

        fs.pipe_file("s3://bucket/key", memoryview(b"ab" * size)[::2])

        assert b"".join(c.kwargs["body"] for c in fs.core.upload_part.call_args_list) == b"a" * size
        fs.core.complete_multipart_upload.assert_called_once()
        fs._call.assert_not_called()

    @pytest.mark.parametrize("intrans", [False, True])
    def test_pipe_file_failed_write(self, intrans):
        # GH-997: a write that fails on the buffered path leaves the existing
        # object unchanged. The file used to be committed when the failure
        # left the with block of fsspec's pipe_file(), which replaced the
        # object with an empty one, also later in a transaction.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs._transaction = None
        fs.core.put_object = mock.MagicMock()

        with (
            mock.patch.object(S3File, "write", side_effect=RuntimeError("write failed")),
            fs.transaction if intrans else contextlib.nullcontext(),
            pytest.raises(RuntimeError, match="write failed"),
        ):
            fs.pipe_file("s3://bucket/key", b"a" * (S3FileSystem.DEFAULT_BLOCK_SIZE + 1))

        fs.core.put_object.assert_not_called()
        fs._call.assert_not_called()

    @pytest.mark.parametrize(
        ("path", "compression", "key"),
        [
            ("s3://bucket/key", "gzip", "key"),
            ("s3://bucket/key.gz", "infer", "key.gz"),
            # Inferred from, and written to, the path without the trailing
            # slash, as open() does.
            ("s3://bucket/key.gz/", "infer", "key.gz"),
        ],
    )
    @pytest.mark.parametrize("intrans", [False, True])
    @pytest.mark.parametrize("size", [1, S3FileSystem.DEFAULT_BLOCK_SIZE + 1])
    def test_pipe_file_compression(self, path, compression, key, intrans, size):
        # GH-1037: the value is compressed before it is uploaded, on every
        # path. The single-request path used to send compression to
        # PutObject, which botocore rejects.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs._transaction = None
        fs.core.put_object = mock.MagicMock()
        value = b"a" * size

        with fs.transaction if intrans else contextlib.nullcontext():
            fs.pipe_file(path, value, compression=compression)

        # The compressed value fits in one block.
        (((s3_path, body), kwargs),) = fs.core.put_object.call_args_list
        assert s3_path.key == key
        assert "compression" not in kwargs
        assert gzip.decompress(body) == value

    @pytest.mark.parametrize("intrans", [False, True])
    def test_pipe_file_compression_multipart(self, intrans):
        # Compressed data larger than the block size is uploaded as a
        # multipart upload.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs._transaction = None
        fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"}
            )
        )
        fs.core.upload_part = mock.MagicMock(
            side_effect=lambda **kw: SimpleNamespace(etag='"e"', part_number=kw["part_number"])
        )
        fs.core.complete_multipart_upload = mock.MagicMock()
        # Random bytes stay larger than the block size when compressed.
        value = os.urandom(S3FileSystem.DEFAULT_BLOCK_SIZE + 1)

        with fs.transaction if intrans else contextlib.nullcontext():
            fs.pipe_file("s3://bucket/key", value, compression="gzip")

        body = b"".join(c.kwargs["body"] for c in fs.core.upload_part.call_args_list)
        assert gzip.decompress(body) == value
        fs.core.complete_multipart_upload.assert_called_once()

    def test_pipe_file_compression_non_contiguous_memoryview(self):
        fs = self._make_fs()
        fs.core.put_object = mock.MagicMock()

        fs.pipe_file("s3://bucket/key", memoryview(b"ab" * 4)[::2], compression="gzip")

        assert gzip.decompress(fs.core.put_object.call_args.args[1]) == b"aaaa"

    def test_pipe_file_compression_inferred_none(self):
        # "infer" uploads the value as it is for a path without the
        # extension of a codec, as open() does.
        fs = self._make_fs()
        fs.core.put_object = mock.MagicMock()

        fs.pipe_file("s3://bucket/key.txt", b"a", compression="infer")

        (((_, body), kwargs),) = fs.core.put_object.call_args_list
        assert "compression" not in kwargs
        assert body == b"a"

    def test_pipe_file_unsupported_compression(self):
        fs = self._make_fs()

        with pytest.raises(ValueError, match="not supported"):
            fs.pipe_file("s3://bucket/key", b"a", compression="unknown")
        fs._call.assert_not_called()

    @pytest.mark.parametrize("intrans", [False, True])
    def test_pipe_file_compression_failed_write(self, intrans):
        # GH-1037: a failed write of a compressed value leaves the existing
        # object unchanged. open() used to return a compression wrapper,
        # without _close_without_commit(), and the object was replaced with
        # an empty compressed one.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs._transaction = None
        fs.core.put_object = mock.MagicMock()
        # Random bytes stay larger than the block size when compressed, so
        # that the buffered path also writes them outside a transaction.
        value = b"a" if intrans else os.urandom(S3FileSystem.DEFAULT_BLOCK_SIZE + 1)

        with (
            mock.patch.object(S3File, "write", side_effect=RuntimeError("write failed")),
            fs.transaction if intrans else contextlib.nullcontext(),
            pytest.raises(RuntimeError, match="write failed"),
        ):
            fs.pipe_file("s3://bucket/key", value, compression="gzip")
        gc.collect()

        fs.core.put_object.assert_not_called()
        fs._call.assert_not_called()

    def test_pipe_file_failed_write_aborts_multipart_upload(self):
        # GH-997: a write that fails after its multipart upload has started
        # aborts the upload instead of completing it.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"}
            )
        )
        fs.core.complete_multipart_upload = mock.MagicMock()
        executor = mock.MagicMock()

        def submit(fn, *args, **kwargs):
            if not fs.core.create_multipart_upload.called:
                creation = Future()
                creation.set_result(fn(*args, **kwargs))
                return creation
            raise RuntimeError("submit failed")

        executor.submit.side_effect = submit
        fs._create_executor = mock.MagicMock(return_value=executor)

        with pytest.raises(RuntimeError, match="submit failed"):
            fs.pipe_file("s3://bucket/key", b"a" * (3 * S3FileSystem.DEFAULT_BLOCK_SIZE))

        fs.core.complete_multipart_upload.assert_not_called()
        fs._call.assert_called_once_with(
            fs._client.abort_multipart_upload, Bucket="bucket", Key="key", UploadId="uploadid"
        )
        executor.shutdown.assert_called_once()

    @pytest.mark.parametrize("intrans", [False, True])
    @pytest.mark.parametrize("error", [RuntimeError, KeyboardInterrupt, PermissionError])
    def test_put_file_failed_write(self, tmp_path, intrans, error):
        # GH-1014: a failure inside the write loop, or a local file that
        # cannot be read, leaves the existing object unchanged. The remote
        # file used to be committed when the failure left the with block,
        # which replaced the object with the data written so far or with an
        # empty one, also later in a transaction.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs._transaction = None
        fs.core.put_object = mock.MagicMock()
        lpath = tmp_path / "data"
        lpath.write_bytes(b"a")
        callback = Callback()
        if error is PermissionError:
            if os.geteuid() == 0:
                pytest.skip("root can read a file without read permission.")
            lpath.chmod(0)
        else:
            callback.relative_update = mock.MagicMock(side_effect=error("callback failed"))

        with (
            fs.transaction if intrans else contextlib.nullcontext(),
            pytest.raises(error),
        ):
            fs.put_file(str(lpath), "s3://bucket/key", callback=callback)

        fs.core.put_object.assert_not_called()
        fs._call.assert_not_called()

    @pytest.mark.parametrize("intrans", [False, True])
    def test_put_file_failed_write_aborts_multipart_upload(self, tmp_path, intrans):
        # GH-1014: a failure after the first block was uploaded aborts the
        # multipart upload instead of completing it with that block only.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs._transaction = None
        fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"}
            )
        )
        fs.core.upload_part = mock.MagicMock(
            side_effect=lambda **kw: SimpleNamespace(etag='"e"', part_number=kw["part_number"])
        )
        fs.core.complete_multipart_upload = mock.MagicMock()
        callback = Callback()
        callback.relative_update = mock.MagicMock(side_effect=RuntimeError("callback failed"))
        lpath = tmp_path / "data"
        lpath.write_bytes(b"a" * (2 * S3FileSystem.DEFAULT_BLOCK_SIZE + 1))

        with (
            fs.transaction if intrans else contextlib.nullcontext(),
            pytest.raises(RuntimeError, match="callback failed"),
        ):
            fs.put_file(str(lpath), "s3://bucket/key", callback=callback)

        fs.core.complete_multipart_upload.assert_not_called()
        fs._call.assert_called_once_with(
            fs._client.abort_multipart_upload, Bucket="bucket", Key="key", UploadId="uploadid"
        )

    @pytest.mark.parametrize("kwargs", [{"block_size": 4}, {}])
    def test_put_file_exceeding_max_parts(self, tmp_path, kwargs):
        # GH-953: a file that does not fit in the maximum number of parts is
        # rejected before anything is uploaded.
        fs = self._make_fs()
        fs.core.MULTIPART_UPLOAD_MAX_PARTS = 3
        fs.default_block_size = 4
        fs.open = mock.MagicMock()
        lpath = tmp_path / "data"
        lpath.write_bytes(b"a" * 13)

        with pytest.raises(ValueError, match="in 3 parts with a block size of 4 bytes"):
            fs.put_file(str(lpath), "s3://bucket/key", **kwargs)
        fs.open.assert_not_called()
        fs._call.assert_not_called()

    def test_put_file_block_size(self, tmp_path):
        # block_size and max_workers are passed to open() instead of the S3
        # API.
        fs = self._make_fs()
        fs.open = mock.MagicMock()
        lpath = tmp_path / "data"
        lpath.write_bytes(b"a" * 13)

        fs.put_file(str(lpath), "s3://bucket/key", block_size=8)

        fs.open.assert_called_once_with(
            "s3://bucket/key",
            "wb",
            block_size=8,
            max_workers=fs.max_workers,
            s3_additional_kwargs={},
        )

    @pytest.mark.parametrize(
        ("filesystem_kwargs", "kwargs", "expected"),
        [
            ({}, {}, {"ContentType": "text/csv"}),
            ({}, {"ContentType": "text/plain"}, {"ContentType": "text/plain"}),
            # An explicit ContentType of the filesystem takes precedence over
            # the one guessed from the file extension.
            ({"ContentType": "application/octet-stream"}, {}, {}),
        ],
    )
    def test_put_file_content_type(self, tmp_path, filesystem_kwargs, kwargs, expected):
        fs = self._make_fs()
        fs.s3_additional_kwargs = filesystem_kwargs
        fs.open = mock.MagicMock()
        lpath = tmp_path / "data.csv"
        lpath.write_bytes(b"a")

        fs.put_file(str(lpath), "s3://bucket/key", **kwargs)

        assert fs.open.call_args.kwargs["s3_additional_kwargs"] == expected

    @pytest.mark.parametrize(
        ("value", "kwargs"),
        [
            (b"a" * 13, {"block_size": 4}),
            (b"a" * 13, {}),
            # The size of a memoryview is counted in bytes, not items.
            (memoryview(b"a" * 16).cast("I"), {}),
        ],
    )
    def test_pipe_file_exceeding_max_parts(self, value, kwargs):
        # GH-953: data that does not fit in the maximum number of parts is
        # rejected before anything is uploaded.
        fs = self._make_fs()
        fs.core.MULTIPART_UPLOAD_MAX_PARTS = 3
        fs.default_block_size = 4
        fs.open = mock.MagicMock()
        fs.core.put_object = mock.MagicMock()

        with pytest.raises(ValueError, match="in 3 parts with a block size of 4 bytes"):
            fs.pipe_file("s3://bucket/key", value, **kwargs)
        fs.open.assert_not_called()
        fs.core.put_object.assert_not_called()
        fs._call.assert_not_called()

    def test_open_max_workers(self):
        fs = self._make_fs()
        fs.default_cache_type = "bytes"

        with fs.open("s3://bucket/key", "wb", max_workers=2) as f:
            assert f.max_workers == 2

    def test_open_version_id(self):
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs.info = mock.MagicMock(return_value=self._file_object("key"))
        fs.info.return_value.size = 4

        with fs.open("s3://bucket/key", "rb", version_id="v1") as f:
            assert f.version_id == "v1"
            # The size is that of the requested version, not the latest one.
            assert f.size == 4
        fs.info.assert_called_once_with("bucket/key?versionId=v1", version_id="v1")
        # The version is carried in the path, which fsspec reopens an
        # unpickled file with.
        assert f.path == "bucket/key?versionId=v1"
        assert f.__reduce__()[1][1] == "bucket/key?versionId=v1"
        # The argument must match the version in the path.
        with pytest.raises(ValueError, match="do not match"):
            fs.open("s3://bucket/key?versionId=v2", "rb", version_id="v1")

    def test_open_version_aware_pins_version_in_path(self):
        # GH-979: the version observed at open time is carried in the path as
        # an explicit version is, so that the metadata, attributes and URL of
        # the file describe that version, not the latest one.
        fs = self._make_fs()
        fs.version_aware = True
        fs.default_cache_type = "bytes"
        fs._call.side_effect = lambda method, **request: (
            "https://signed"
            if method is fs._client.generate_presigned_url
            else {"ContentLength": 4, "ETag": '"e"', "VersionId": "v1", "Metadata": {"a": "1"}}
        )

        with fs.open("s3://bucket/key", "rb") as f:
            assert f.version_id == "v1"
            assert f.path == "bucket/key?versionId=v1"
            assert f.metadata()["a"] == "1"
            assert f.getxattr("a") == "1"
            assert f.url() == "https://signed"
        requests = [c.kwargs for c in fs._call.call_args_list]
        # The open-time lookup, then metadata(), getxattr() and url().
        assert requests[1:3] == [{"Bucket": "bucket", "Key": "key", "VersionId": "v1"}] * 2
        assert requests[3]["Params"] == {"Bucket": "bucket", "Key": "key", "VersionId": "v1"}
        # A reopened (e.g., unpickled) file reads the same version.
        assert f.__reduce__()[1][1] == "bucket/key?versionId=v1"

    @pytest.mark.parametrize("mode", ["wb", "ab", "xb"])
    @pytest.mark.parametrize(
        ("path", "kwargs"),
        [
            ("s3://bucket/key", {"version_id": "v1"}),
            ("s3://bucket/key?versionId=v1", {}),
        ],
    )
    def test_open_version_id_for_writing(self, mode, path, kwargs):
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs._call.side_effect = AssertionError("No request is expected.")

        with pytest.raises(ValueError, match="version specified"):
            fs.open(path, mode, **kwargs)

    @pytest.mark.parametrize("mode", ["wb", "ab", "xb"])
    @pytest.mark.parametrize(
        ("path", "block_size", "match"),
        [
            # GH-926: the message states the accepted range.
            (
                "s3://bucket/key",
                S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE - 1,
                r"between 5 MiB \(5242880 bytes\) and 5 GiB \(5368709120 bytes\), inclusive",
            ),
            # GH-952: a part cannot be larger than the maximum part size.
            ("s3://bucket/key", S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE + 1, "between"),
            ("s3://bucket", S3FileSystem.DEFAULT_BLOCK_SIZE, "does not contain a key"),
        ],
    )
    def test_open_invalid_for_writing(self, monkeypatch, mode, path, block_size, match):
        # GH-976: an open() that fails validation sends no request and leaves
        # no half-initialized file, whose garbage collection would close it.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs._call.side_effect = AssertionError("No request is expected.")
        unraisable = []
        monkeypatch.setattr(sys, "unraisablehook", unraisable.append)

        with pytest.raises(ValueError, match=match):
            fs.open(path, mode, block_size=block_size)
        gc.collect()

        assert unraisable == []

    def test_open_append_keeps_metadata_of_listed_object(self):
        # A cached listing entry lacks the metadata that the rewritten object
        # keeps, so the append looks up the object.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs.dircache[("bucket", "/")] = [self._file_object("key")]
        fs._call.return_value = {
            "ContentLength": 2,
            "ContentType": "text/plain",
            "Metadata": {"k": "v"},
        }
        fs.cat_file = mock.MagicMock(return_value=b"aa")
        fs.core.put_object = mock.MagicMock()

        with fs.open("s3://bucket/key", "ab") as f:
            f.write(b"bb")
        fs._call.assert_called_once_with(fs._client.head_object, Bucket="bucket", Key="key")
        (_, body), request = fs.core.put_object.call_args
        assert (body, request["ContentType"], request["Metadata"]) == (
            b"aabb",
            "text/plain",
            {"k": "v"},
        )

    def test_open_append_lookup_failure(self, monkeypatch):
        # GH-976: an append whose lookup of the existing object fails leaves
        # no half-initialized file, whose garbage collection would close it.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"

        def info(path, **kwargs):
            # A new exception each time: one kept by a mock would keep its
            # traceback, and the file, alive.
            raise PermissionError("denied")

        fs.info = info
        unraisable = []
        monkeypatch.setattr(sys, "unraisablehook", unraisable.append)

        with pytest.raises(PermissionError, match="denied"):
            fs.open("s3://bucket/key", "ab")
        gc.collect()

        assert unraisable == []
        fs._call.assert_not_called()

    LOOKUP_KWARGS = {
        "ExpectedBucketOwner": "111122223333",
        "RequestPayer": "requester",
        "SSECustomerAlgorithm": "AES256",
        "SSECustomerKey": "k" * 32,
    }

    @pytest.mark.parametrize("mode", ["rb", "ab", "xb"])
    def test_open_lookup_parameters(self, mode):
        # GH-1004: the lookups made while opening a file did not send its
        # parameters, so an object encrypted with a customer-provided key, or
        # in a requester-pays bucket, could not be opened.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        requests = self._record_requests(fs, exists=mode != "xb")

        with fs.open(
            "s3://bucket/key",
            mode,
            ContentType="text/plain",
            Range="bytes=0-0",
            **self.LOOKUP_KWARGS,
        ) as f:
            if mode == "rb":
                assert f.read() == b"aa"
            else:
                f.write(b"bb")

        lookups = {name: request for name, request in requests if name != "put_object"}
        # Only the parameters on which the authorization of a lookup depends.
        assert lookups.pop("head_object") == {
            "Bucket": "bucket",
            "Key": "key",
            **self.LOOKUP_KWARGS,
        }
        if mode == "xb":
            assert lookups.pop("list_objects_v2") == {
                "Bucket": "bucket",
                "Prefix": "key/",
                "Delimiter": "/",
                "MaxKeys": 1,
                "ExpectedBucketOwner": "111122223333",
                "RequestPayer": "requester",
            }
        else:
            get_object = lookups.pop("get_object")
            assert self.LOOKUP_KWARGS.items() <= get_object.items()
            if mode == "ab":
                # The whole existing object is read.
                assert "Range" not in get_object
        assert lookups == {}

    def test_cache_lookup_concurrent_parameters(self):
        # GH-1004: caching a lookup with some parameters does not replace a
        # result cached in the meantime for other parameters.
        fs = self._make_fs()
        path = "bucket/key"
        lookup_kwargs = self.LOOKUP_KWARGS
        other_key = {**lookup_kwargs, "SSECustomerKey": "j" * 32}
        stale, fresh, other = (fs._directory_object("bucket", "key") for _ in range(3))
        fs._cache_lookup(path, lookup_kwargs, stale)

        class InterleavedDirCache(DirCache):
            interleaved = False

            def get(self, key, default=None):
                value = super().get(key, default)
                if not self.interleaved:
                    # Another thread refreshes the lookup in between.
                    self.interleaved = True
                    fs._cache_lookup(path, lookup_kwargs, fresh)
                return value

        cache = InterleavedDirCache()
        cache.update(fs.dircache)
        fs.dircache = cache
        fs._cache_lookup(path, other_key, other)

        assert fs._get_cached_lookup(path, lookup_kwargs) is fresh
        assert fs._get_cached_lookup(path, other_key) is other

    def test_cache_lookup_expiry(self, monkeypatch):
        # GH-1004: the cached lookups with parameters expire after the
        # listings_expiry_time of the dircache, each on its own.
        fs = self._make_fs()
        fs.dircache = DirCache(listings_expiry_time=60)
        now = [0.0]
        monkeypatch.setattr("fsspec.dircache.time.time", lambda: now[0])
        path = "bucket/key"
        stale, fresh = (fs._directory_object("bucket", "key") for _ in range(2))

        other_key = {**self.LOOKUP_KWARGS, "SSECustomerKey": "j" * 32}

        fs._cache_lookup(path, self.LOOKUP_KWARGS, stale)
        now[0] = 59.0
        fs._cache_lookup(path, self.LOOKUP_KWARGS, fresh)
        now[0] = 61.0
        assert fs._get_cached_lookup(path, self.LOOKUP_KWARGS) is fresh

        # Each result expires on its own, also while the results of other
        # parameters keep renewing the entry of the path.
        fs._cache_lookup(path, other_key, stale)
        now[0] = 120.0
        assert fs._get_cached_lookup(path, other_key) is stale
        assert fs._get_cached_lookup(path, self.LOOKUP_KWARGS) is None

    def test_info_lookup_parameters_cache(self):
        # GH-1004: a cached result serves only lookups with the same lookup
        # parameters, on which the authorization of the requests depends.
        fs = self._make_fs()
        fs._call.return_value = {"ContentLength": 2, "ETag": '"e"'}
        fs.dircache[("bucket", "/")] = [self._file_object("key")]
        path = "s3://bucket/key"
        other_key = {**self.LOOKUP_KWARGS, "SSECustomerKey": "j" * 32}

        for _ in range(2):
            assert fs.info(path).size == 0
            assert fs.info(path, **self.LOOKUP_KWARGS).size == 2
            assert fs.info(path, IfMatch='"x"', **other_key).size == 2
            assert fs.exists(path, **self.LOOKUP_KWARGS)
        # The listing serves only the lookups without the parameters, and the
        # other parameters are not sent.
        assert [c.kwargs for c in fs._call.call_args_list] == [
            {"Bucket": "bucket", "Key": "key", **self.LOOKUP_KWARGS},
            {"Bucket": "bucket", "Key": "key", **other_key},
        ]
        # The cache does not keep the customer-provided keys.
        assert "k" * 32 not in repr(dict(fs.dircache))
        assert "j" * 32 not in repr(dict(fs.dircache))

        fs.invalidate_cache(path)
        fs.info(path, **self.LOOKUP_KWARGS)
        assert fs._call.call_count == 3

    def test_info_lookup_parameters_missing_object(self):
        # GH-1004: a missing object evicts the cached results of the lookups
        # with parameters, and the request that checks for a key prefix
        # receives those that ListObjectsV2 accepts.
        fs = self._make_fs()
        fs._call.side_effect = [
            {"ContentLength": 2, "ETag": '"e"'},
            FileNotFoundError("key"),
            {"KeyCount": 0},
        ]
        path = "s3://bucket/key"

        fs.info(path, **self.LOOKUP_KWARGS)
        with pytest.raises(FileNotFoundError):
            fs.info(path, refresh=True, **self.LOOKUP_KWARGS)

        assert fs.dircache == {}
        assert fs._call.call_args.kwargs == {
            "Bucket": "bucket",
            "Prefix": "key/",
            "Delimiter": "/",
            "MaxKeys": 1,
            "ExpectedBucketOwner": "111122223333",
            "RequestPayer": "requester",
        }

    def test_exists_bucket_lookup_parameters(self):
        # GH-1004: a bucket lookup with parameters uses neither the cached
        # bucket listing nor the result of a lookup without them.
        fs = self._make_fs()
        fs._call.return_value = {}
        fs.dircache[""] = [fs._directory_object("bucket", None)]

        for _ in range(2):
            assert fs.exists("s3://bucket")
            assert fs.exists("s3://bucket", **self.LOOKUP_KWARGS)
            assert fs.info("s3://bucket", **self.LOOKUP_KWARGS).type == (
                S3ObjectType.S3_OBJECT_TYPE_DIRECTORY
            )
        fs._call.assert_called_once_with(
            fs._client.head_bucket, Bucket="bucket", ExpectedBucketOwner="111122223333"
        )

    def test_pipe_file_create_lookup_parameters(self):
        # GH-1004: the existence check of pipe_file(mode="create") sends the
        # lookup parameters of the write, as open() does in "xb" mode.
        fs = self._make_fs()
        requests = self._record_requests(fs, exists=False)

        fs.pipe_file("s3://bucket/key", b"a", mode="create", **self.LOOKUP_KWARGS)

        assert requests[0] == (
            "head_object",
            {"Bucket": "bucket", "Key": "key", **self.LOOKUP_KWARGS},
        )
        assert requests[-1][0] == "put_object"
        assert requests[-1][1]["IfNoneMatch"] == "*"

    def test_cat_file_range_lookup_parameters(self):
        # GH-1004: the lookup that resolves negative offsets sends the lookup
        # parameters of the read.
        fs = self._make_fs()
        requests = self._record_requests(fs)

        assert fs.cat_file("s3://bucket/key", start=-2, end=-1, **self.LOOKUP_KWARGS) == b"aa"

        assert [(name, self.LOOKUP_KWARGS.items() <= r.items()) for name, r in requests] == [
            ("head_object", True),
            ("get_object", True),
        ]

    @pytest.mark.parametrize(
        "block_size",
        [S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE, S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE],
    )
    def test_open_block_size_limits_for_writing(self, block_size):
        fs = self._make_fs()
        fs.default_cache_type = "bytes"

        with fs.open("s3://bucket/key", "wb", block_size=block_size) as f:
            assert f.blocksize == block_size

    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("s3://bucket/key", "v1"),
            # The version in the path takes precedence, as in info().
            ("s3://bucket/key?versionId=v2", "v2"),
        ],
    )
    def test_cat_file_version_id(self, path, expected):
        fs = self._make_fs()
        fs.info = mock.MagicMock(return_value=self._file_object("key"))
        fs.info.return_value.size = 10

        fs._call.return_value = {"Body": io.BytesIO(b"data")}
        assert fs.cat_file(path, version_id="v1") == b"data"
        fs._call.assert_called_once_with(
            fs._client.get_object, Bucket="bucket", Key="key", VersionId=expected
        )

        # A negative offset is resolved against the size of the same version.
        fs._call.reset_mock()
        fs._call.return_value = {"Body": io.BytesIO(b"ta")}
        assert fs.cat_file(path, start=-8, end=-6, version_id="v1") == b"ta"
        fs.info.assert_called_once_with(path, version_id=expected)
        fs._call.assert_called_once_with(
            fs._client.get_object,
            Bucket="bucket",
            Key="key",
            Range="bytes=2-3",
            VersionId=expected,
        )

    def _make_object_fs(self, data):
        # A filesystem holding one object at s3://bucket/key whose client
        # answers GetObject like S3: the last bytes for a suffix range,
        # InvalidRange when the range starts at or past the end of the
        # object, and a failure on a range that S3 would answer with the
        # whole object (last byte before the first).
        # info() reports the given size, and the requested ranges are
        # recorded.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        # The real request function, on the client mock.
        fs._core.call = functools.partial(S3Core.call, fs._core)
        fs._call = functools.partial(S3FileSystem._call, fs)
        fs.info = mock.MagicMock(return_value=self._file_object("key"))
        fs.info.return_value.size = len(data)
        ranges = []

        def get_object(**request):
            range_ = request.get("Range")
            ranges.append(range_)
            if range_ is None:
                return {"Body": io.BytesIO(data)}
            if suffix := re.fullmatch(r"bytes=-(\d+)", range_):
                return {"Body": io.BytesIO(data[-int(suffix[1]) :])}
            match = re.fullmatch(r"bytes=(\d+)-(\d*)", range_)
            assert match, range_
            first = int(match[1])
            last = int(match[2]) if match[2] else len(data) - 1
            assert not match[2] or first <= last, range_
            if first >= len(data):
                raise botocore.exceptions.ClientError(
                    {
                        "Error": {"Code": "InvalidRange", "Message": "Not satisfiable"},
                        "ResponseMetadata": {"HTTPStatusCode": 416},
                    },
                    "GetObject",
                )
            return {"Body": io.BytesIO(data[first : last + 1])}

        fs._client.get_object.side_effect = get_object
        return fs, ranges

    @pytest.mark.parametrize(
        ("start", "end"),
        [
            (None, 5),
            (5, None),
            (1, -1),
            (-5, None),
            (-10, None),
            (-100, None),
            (5, 100),
            (-100, 5),
            # Empty ranges.
            (0, 0),
            (5, 5),
            (7, 3),
            (10, None),
            (12, None),
            (12, 20),
            (None, -20),
            (-3, -5),
        ],
    )
    def test_cat_file_range(self, start, end):
        data = b"0123456789"
        fs, ranges = self._make_object_fs(data)

        # The range selects bytes like a slice.
        assert fs.cat_file("s3://bucket/key", start=start, end=end) == data[start:end]
        suffix = (start or 0) < 0 and end is None
        negative = (start or 0) < 0 or (end or 0) < 0
        empty = end is not None and 0 <= end <= (start or 0)
        # Only a negative offset with an end, a negative end, or an empty
        # range looks up the object.
        assert fs.info.called == ((negative and not suffix) or empty)
        if suffix:
            assert ranges == [f"bytes={start}"]
        if empty:
            assert ranges == []

    def test_cat_file_range_stale_size(self):
        fs, ranges = self._make_object_fs(b"0123456789abcdefghij")
        # A cached entry from before the object grew.
        fs.info.return_value.size = 10

        assert fs.cat_file("s3://bucket/key", start=10, end=20) == b"abcdefghij"
        assert fs.cat_file("s3://bucket/key", start=15) == b"fghij"
        assert fs.cat_file("s3://bucket/key", start=-5) == b"fghij"
        assert ranges == ["bytes=10-19", "bytes=15-", "bytes=-5"]

    def test_cat_file_suffix_range_key_ending_in_slash(self):
        fs, _ = self._make_object_fs(b"abc")
        # info() reports a key ending in "/" as a directory.
        fs.info.return_value = S3FileSystem._directory_object("bucket", "dir")

        assert fs.cat_file("s3://bucket/dir/", start=-2) == b"bc"
        fs._client.get_object.assert_called_once_with(Bucket="bucket", Key="dir/", Range="bytes=-2")
        fs.info.assert_not_called()

    def test_cat_file_range_errors(self):
        fs = self._make_fs()
        # The real request function, on the client mock.
        fs._core.call = functools.partial(S3Core.call, fs._core)
        fs._call = functools.partial(S3FileSystem._call, fs)
        fs._client.get_object.side_effect = botocore.exceptions.ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "No such key"}}, "GetObject"
        )

        # Errors other than InvalidRange are raised.
        with pytest.raises(FileNotFoundError):
            fs.cat_file("s3://bucket/dir", start=0, end=5)
        fs._client.get_object.side_effect = botocore.exceptions.ClientError(
            {
                "Error": {"Code": "InvalidRange", "Message": "Not satisfiable"},
                "ResponseMetadata": {"HTTPStatusCode": 416},
            },
            "GetObject",
        )
        # InvalidRange without a range is not taken as an empty read.
        with pytest.raises(OSError, match="Not satisfiable"):
            fs.cat_file("s3://bucket/key")

    @pytest.mark.parametrize(("start", "end"), [(0, 0), (5, 3), (None, 0)])
    def test_cat_file_empty_range_missing(self, start, end):
        fs = self._make_fs()
        fs.info = mock.MagicMock(side_effect=FileNotFoundError("bucket/missing"))

        # An empty range of a missing object is not read as empty.
        with pytest.raises(FileNotFoundError):
            fs.cat_file("s3://bucket/missing", start=start, end=end)
        fs._call.assert_not_called()

    @pytest.mark.parametrize(("start", "end"), [(-5, 3), (0, -1), (5, 5)])
    def test_cat_file_range_directory(self, start, end):
        fs = self._make_fs()
        fs.info = mock.MagicMock(return_value=S3FileSystem._directory_object("bucket", "dir"))

        # A prefix is not read as an empty object.
        with pytest.raises(FileNotFoundError):
            fs.cat_file("s3://bucket/dir", start=start, end=end)
        fs._call.assert_not_called()

    @pytest.mark.parametrize(("start", "end"), [(None, None), (-1, None), (0, 5)])
    def test_cat_file_bucket(self, start, end):
        fs = self._make_fs()

        with pytest.raises(FileNotFoundError):
            fs.cat_file("s3://bucket/", start=start, end=end)
        fs._call.assert_not_called()

    @pytest.mark.parametrize(("start", "end"), [(-2, -1), (0, -1), (5, 5)])
    def test_cat_file_range_key_ending_in_slash(self, start, end):
        fs, ranges = self._make_object_fs(b"0123456789")
        # info() of "dir/" describes the object "dir" when both exist.
        fs.info.return_value = self._file_object("dir")

        # The size of "dir" is not used for the range of "dir/".
        with pytest.raises(FileNotFoundError):
            fs.cat_file("s3://bucket/dir/", start=start, end=end)
        assert ranges == []

    @pytest.mark.parametrize("key", ["dir", None])
    def test_get_file_directory(self, tmp_path, key):
        # GH-974: recursive get() passes directories, including the bucket,
        # which become local directories as with fsspec's get_file().
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs.info = mock.MagicMock(return_value=S3FileSystem._directory_object("bucket", key))
        rpath = f"s3://bucket/{key}" if key else "s3://bucket"
        lpath = tmp_path / "out" / "dir"

        fs.get_file(rpath, str(lpath))
        assert lpath.is_dir()
        # Existing directories are kept.
        fs.get_file(rpath, str(lpath))
        assert lpath.is_dir()
        fs._call.assert_not_called()

    def test_get_file_missing(self, tmp_path):
        # A missing object leaves no local file or parent directory.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs.info = mock.MagicMock(side_effect=FileNotFoundError("bucket/key"))

        with pytest.raises(FileNotFoundError):
            fs.get_file("s3://bucket/key", str(tmp_path / "new" / "key"))
        assert list(tmp_path.iterdir()) == []

    def test_get_file_creates_parent_directories(self, tmp_path):
        # GH-974: the parent directories used to raise FileNotFoundError.
        fs, _ = self._make_object_fs(b"data")
        lpath = tmp_path / "new" / "dir" / "key"
        callback = Callback()

        fs.get_file("s3://bucket/key", str(lpath), callback=callback)
        assert lpath.read_bytes() == b"data"
        assert callback.size == callback.value == 4

    def test_get_file_parent_through_symlink(self, tmp_path):
        # The parent is created as open() resolves it: "link/.." is the
        # parent of the symlink's target, not tmp_path / "a".
        fs, _ = self._make_object_fs(b"data")
        (tmp_path / "b" / "sub").mkdir(parents=True)
        (tmp_path / "a").mkdir()
        (tmp_path / "a" / "link").symlink_to(tmp_path / "b" / "sub")
        (tmp_path / "a" / "out").touch()

        fs.get_file("s3://bucket/key", str(tmp_path / "a" / "link" / ".." / "out" / "key"))
        assert (tmp_path / "b" / "out" / "key").read_bytes() == b"data"

    @pytest.mark.parametrize(
        ("rpath", "kwargs"),
        [
            ("s3://bucket/key", {"version_id": "v1"}),
            ("s3://bucket/key?versionId=v1", {}),
        ],
    )
    def test_get_file_version_id(self, tmp_path, rpath, kwargs):
        # A requested version is looked up only by open(), not as a possible
        # directory: isdir() would look up the latest version, or the prefix
        # of the same name when the version does not exist.
        fs, _ = self._make_object_fs(b"data")
        lpath = tmp_path / "key"

        fs.get_file(rpath, str(lpath), **kwargs)
        assert lpath.read_bytes() == b"data"
        assert fs.info.call_count == 1

    def test_get_file_file_like(self, tmp_path):
        # GH-974: a file-like lpath used to raise TypeError, and outfile was
        # ignored in favor of the local file lpath.
        fs, _ = self._make_object_fs(b"data")
        lpath = io.BytesIO()
        fs.get_file("s3://bucket/key", lpath)
        assert lpath.getvalue() == b"data"
        assert not lpath.closed

        outfile = io.BytesIO()
        fs.get_file("s3://bucket/key", str(tmp_path / "key"), outfile=outfile)
        assert outfile.getvalue() == b"data"
        assert not outfile.closed
        assert not (tmp_path / "key").exists()

        # As with fsspec, lpath may be omitted when outfile is given.
        outfile = io.BytesIO()
        fs.get_file("s3://bucket/key", outfile=outfile)
        assert outfile.getvalue() == b"data"

    def test_cat_ranges_range(self):
        fs, ranges = self._make_object_fs(b"0123456789")

        assert fs.cat_ranges(["s3://bucket/key"] * 4, [5, 0, -100, 12], [5, 3, 5, 20]) == [
            b"",
            b"012",
            b"01234",
            b"",
        ]
        assert sorted(ranges) == ["bytes=0-2", "bytes=0-4", "bytes=12-19"]

    @pytest.mark.parametrize(
        ("size", "offset", "open_kwargs"),
        [
            # FirstChunkCache fetches an empty range at the end of the object.
            (10, 10, {"cache_type": "first"}),
            # MMapCache fetches an empty last block.
            (32, 16, {"cache_type": "mmap", "block_size": 16}),
            # A read past the end is split into ranges for parallel requests.
            (40, 20, {"cache_type": "none", "block_size": 16, "max_workers": 4}),
        ],
    )
    def test_read_to_end(self, size, offset, open_kwargs):
        data = bytes(range(size))
        fs, _ = self._make_object_fs(data)

        with fs.open("s3://bucket/key", "rb", **open_kwargs) as f:
            assert f.read(offset) == data[:offset]
            assert f.read(size) == data[offset:]
            assert f.read(size) == b""

    def test_read_parallel_ranges_in_order(self):
        # The ranges of a parallel read are joined in their order, also when
        # the first range finishes last.
        data = bytes(range(64))
        fs, ranges = self._make_object_fs(data)
        get_object = fs._client.get_object.side_effect
        others_done = threading.Semaphore(0)

        def answer_first_range_last(**request):
            if request["Range"].startswith("bytes=0-"):
                for _ in range(3):
                    assert others_done.acquire(timeout=5)
                return get_object(**request)
            try:
                return get_object(**request)
            finally:
                others_done.release()

        fs._client.get_object.side_effect = answer_first_range_last
        with fs.open("s3://bucket/key", "rb", cache_type="none", block_size=16, max_workers=4) as f:
            assert f.read() == data
        assert sorted(ranges) == ["bytes=0-15", "bytes=16-31", "bytes=32-47", "bytes=48-63"]

    @pytest.mark.parametrize("cache_type", ["bytes", "all", "first"])
    def test_open_directory(self, cache_type):
        fs = self._make_fs()
        fs.info = mock.MagicMock(return_value=S3FileSystem._directory_object("bucket", "dir"))

        # A prefix is not read as an empty object.
        with pytest.raises(FileNotFoundError):
            fs.open("s3://bucket/dir", "rb", cache_type=cache_type)
        fs._call.assert_not_called()

    def test_finish_multipart_upload(self):
        fs = self._make_fs()
        upload = S3MultipartUpload({"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"})
        fs.core.complete_multipart_upload = mock.MagicMock()
        futures = []
        for part_number in (1, 2):
            future: Future[SimpleNamespace] = Future()
            future.set_result(SimpleNamespace(etag=f'"e{part_number}"', part_number=part_number))
            futures.append(future)

        fs._finish_multipart_upload(upload=upload, futures=futures)
        fs.core.complete_multipart_upload.assert_called_once_with(
            upload,
            [f.result() for f in futures],
        )
        fs._call.assert_not_called()

    # GH-1014: an interrupt while waiting for the parts used to leave the
    # multipart upload behind.
    @pytest.mark.parametrize("error", [RuntimeError, KeyboardInterrupt])
    def test_finish_multipart_upload_aborts_on_failure(self, error):
        fs = self._make_fs()
        upload = S3MultipartUpload({"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"})
        fs.core.complete_multipart_upload = mock.MagicMock()
        future: Future[SimpleNamespace] = Future()
        future.set_exception(error("upload failed"))

        with pytest.raises(error, match="upload failed"):
            fs._finish_multipart_upload(upload=upload, futures=[future])
        fs.core.complete_multipart_upload.assert_not_called()
        fs._call.assert_called_once_with(
            fs._client.abort_multipart_upload,
            Bucket="bucket",
            Key="key",
            UploadId="uploadid",
        )

    def test_finish_multipart_upload_without_abort(self):
        # A caller that aborts the upload itself, as S3File.commit() does,
        # gets the original error with the parts and the upload left alone.
        fs = self._make_fs()
        upload = S3MultipartUpload({"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"})
        fs.core.complete_multipart_upload = mock.MagicMock()
        failed: Future[SimpleNamespace] = Future()
        failed.set_exception(RuntimeError("upload failed"))
        pending: Future[SimpleNamespace] = Future()

        with pytest.raises(RuntimeError, match="upload failed"):
            fs._finish_multipart_upload(
                upload=upload,
                futures=[failed, pending],
                abort=False,
            )
        assert not pending.cancelled()
        fs._call.assert_not_called()

    def test_finish_multipart_upload_abort_failure_does_not_mask_the_original_error(self, caplog):
        fs = self._make_fs()
        upload = S3MultipartUpload({"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"})
        fs.core.complete_multipart_upload = mock.MagicMock()
        # The abort is sent through the core, whose call is the same mock.
        fs._call.side_effect = RuntimeError("abort failed")
        future: Future[SimpleNamespace] = Future()
        future.set_exception(RuntimeError("upload failed"))

        # The abort failure is logged, and the original error propagates.
        with pytest.raises(RuntimeError, match="upload failed"):
            fs._finish_multipart_upload(upload=upload, futures=[future])
        fs._call.assert_called_once_with(
            fs._client.abort_multipart_upload, Bucket="bucket", Key="key", UploadId="uploadid"
        )
        assert "Failed to abort multipart upload uploadid to s3://bucket/key." in caplog.text

    def test_finish_multipart_upload_waits_for_running_parts(self):
        # GH-976: a part that is still uploading when the upload is aborted
        # may be stored after the abort, so the abort waits for it. The
        # parts that have not started are cancelled.
        fs = self._make_fs()
        upload = S3MultipartUpload({"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"})
        fs.core.complete_multipart_upload = mock.MagicMock()
        events = []
        fs._call.side_effect = lambda *args, **kwargs: events.append("abort")
        failed: Future[SimpleNamespace] = Future()
        failed.set_exception(RuntimeError("upload failed"))
        started = threading.Event()
        release = threading.Event()

        def upload_part():
            started.set()
            # Uploading until the abort waits for it, so that an abort that
            # does not wait comes first.
            release.wait(5)
            events.append("part 2 stored")

        def wait_parts(futures):
            release.set()
            return wait(futures)

        with (
            ThreadPoolExecutor(max_workers=1) as executor,
            mock.patch("pyathena.filesystem.s3.wait", side_effect=wait_parts) as waited,
        ):
            running = executor.submit(upload_part)
            pending = executor.submit(events.append, "part 3 stored")
            started.wait(5)
            with pytest.raises(RuntimeError, match="upload failed"):
                fs._finish_multipart_upload(
                    upload=upload,
                    futures=[failed, running, pending],
                )

        assert events == ["part 2 stored", "abort"]
        waited.assert_called_once_with([failed, running])
        assert pending.cancelled()

    def test_finish_multipart_upload_does_not_wait_for_cancelled_parts(self):
        # GH-976: a cancelled part is not waited for, as nothing may
        # acknowledge its cancellation, e.g., an event loop blocked by the
        # caller.
        fs = self._make_fs()
        upload = S3MultipartUpload({"Bucket": "bucket", "Key": "key", "UploadId": "uploadid"})
        fs.core.complete_multipart_upload = mock.MagicMock()
        failed: Future[SimpleNamespace] = Future()
        failed.set_exception(RuntimeError("upload failed"))
        never_started: Future[SimpleNamespace] = Future()
        errors = []

        def finish():
            try:
                fs._finish_multipart_upload(
                    upload=upload,
                    futures=[failed, never_started],
                )
            except RuntimeError as e:
                errors.append(e)

        thread = threading.Thread(target=finish, daemon=True)
        thread.start()
        thread.join(5)

        assert not thread.is_alive()
        assert [str(e) for e in errors] == ["upload failed"]
        assert never_started.cancelled()
        fs._call.assert_called_once()

    @pytest.mark.parametrize("max_workers", [1, 4])
    def test_copy_object_with_multipart_upload_part_sizes(self, max_workers):
        # GH-951: the parts are within the S3 part size limits whatever the
        # number of workers; a single worker used to copy the whole object
        # as one part larger than 5 GiB.
        fs = self._make_fs()
        fs.core.create_multipart_upload = mock.MagicMock(
            return_value=S3MultipartUpload(
                {"Bucket": "bucket", "Key": "dst", "UploadId": "uploadid"}
            )
        )
        fs.core.upload_part_copy = mock.MagicMock()
        fs._finish_multipart_upload = mock.MagicMock()
        # The HeadObject of the source.
        fs._call.return_value = {"ContentLength": 5 * 2**30 + 2**20}

        fs._copy_object_with_multipart_upload(
            S3Path("bucket", "src"),
            S3Path("bucket", "dst"),
            max_workers=max_workers,
            # Copy without reading the metadata, tags and annotations of the
            # source (GH-973).
            MetadataDirective="REPLACE",
            TaggingDirective="REPLACE",
            AnnotationDirective="EXCLUDE",
        )

        parts = sorted(
            (c.kwargs["part_number"], c.kwargs["range_"])
            for c in fs.core.upload_part_copy.call_args_list
        )
        assert parts == [
            (1, (0, 5 * 2**29 + 2**19)),
            (2, (5 * 2**29 + 2**19, 5 * 2**30 + 2**20)),
        ]

    @pytest.mark.skipif(
        threading.current_thread() is not threading.main_thread(),
        reason="SIGINT interrupts the main thread.",
    )
    def test_copy_object_with_multipart_upload_interrupted_creation(self):
        # An interrupt during CreateMultipartUpload waits for it, aborts the
        # upload that it created before any part is copied, and is re-raised.
        # The created upload used to be left incomplete.
        fs = self._make_fs()
        started = threading.Event()
        waiting = threading.Event()
        interrupted = threading.Event()

        def create_multipart_upload(*args, **kw):
            started.set()
            # Still running when the interrupt arrives, which releases it.
            interrupted.wait(30)
            return S3MultipartUpload({"Bucket": "bucket", "Key": "dst", "UploadId": "uploadid"})

        fs.core.create_multipart_upload = mock.MagicMock(side_effect=create_multipart_upload)
        fs.core.upload_part_copy = mock.MagicMock()
        # The HeadObject of the source.
        fs._call.return_value = {"ContentLength": 2 * S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE}
        fs._abort_multipart_upload = mock.MagicMock()
        executor = S3ThreadPoolExecutor(max_workers=2)
        submit = executor.submit

        def submit_creation(fn, *args, **kwargs):
            future = submit(fn, *args, **kwargs)
            if fn is fs.core.create_multipart_upload:
                result = future.result

                def wait_for_result(timeout=None):
                    # The interrupt is sent once the copy waits for the creation.
                    waiting.set()
                    return result(timeout)

                future.result = wait_for_result  # type: ignore[method-assign]
            return future

        executor.submit = submit_creation  # type: ignore[method-assign]
        fs._create_executor = mock.MagicMock(return_value=executor)

        def handle_interrupt(signum, frame):
            interrupted.set()
            raise KeyboardInterrupt

        def interrupt():
            # Sent only while the creation is running and the copy waits for
            # it, which the creation cannot stop doing before the interrupt.
            if started.wait(5) and waiting.wait(5):
                signal.pthread_kill(threading.main_thread().ident, signal.SIGINT)

        thread = threading.Thread(target=interrupt, daemon=True)
        previous_handler = signal.signal(signal.SIGINT, handle_interrupt)
        try:
            thread.start()
            with pytest.raises(KeyboardInterrupt):
                fs._copy_object_with_multipart_upload(
                    S3Path("bucket", "src"),
                    S3Path("bucket", "dst"),
                    MetadataDirective="REPLACE",
                    TaggingDirective="REPLACE",
                    AnnotationDirective="EXCLUDE",
                )
        finally:
            # A late interrupt is ignored instead of reaching a later test.
            signal.signal(signal.SIGINT, lambda signum, frame: None)
            started.set()
            waiting.set()
            if thread.ident is not None:
                thread.join()
            signal.signal(signal.SIGINT, previous_handler)
            interrupted.set()

        fs._abort_multipart_upload.assert_called_once()
        upload, params = fs._abort_multipart_upload.call_args.args
        assert (upload.bucket, upload.key, upload.upload_id, params) == (
            "bucket",
            "dst",
            "uploadid",
            {},
        )
        fs.core.upload_part_copy.assert_not_called()

    @pytest.mark.parametrize(
        "block_size",
        [
            S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE - 1,
            S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE + 1,
        ],
    )
    def test_copy_object_with_multipart_upload_invalid_block_size(self, block_size):
        # GH-926: the message states the accepted range.
        fs = self._make_fs()

        with pytest.raises(
            ValueError,
            match=r"between 5 MiB \(5242880 bytes\) and 5 GiB \(5368709120 bytes\), inclusive",
        ):
            fs._copy_object_with_multipart_upload(
                S3Path("bucket", "src"), S3Path("bucket", "dst"), block_size=block_size
            )
        fs._call.assert_not_called()

    def test_ls_sparse_entries_keep_defaults(self):
        # Listed entries without Size or StorageClass get S3Object's defaults,
        # as they did when they were built from the response.
        fs = self._make_fs()
        fs.version_aware = True
        fs._call.side_effect = [
            {"Contents": [{"Key": "k"}]},
            {"Versions": [{"Key": "k", "VersionId": "v1"}]},
        ]

        for files in (
            fs.ls("s3://bucket", detail=True),
            fs.ls("s3://bucket", detail=True, versions=True),
        ):
            assert (files[0]["size"], files[0]["content_length"], files[0]["storage_class"]) == (
                0,
                0,
                S3StorageClass.S3_STORAGE_CLASS_STANDARD,
            )

    def test_object_version_info_from_markers(self):
        # Explicit markers start the listing, and the next page follows the
        # returned markers instead of sending them twice.
        fs = self._make_fs()
        fs._call.side_effect = [
            {"IsTruncated": True, "NextKeyMarker": "m", "NextVersionIdMarker": "w"},
            {"IsTruncated": False},
        ]

        fs.object_version_info("s3://bucket", KeyMarker="k", VersionIdMarker="v")
        assert [c.kwargs for c in fs._call.call_args_list] == [
            {"Bucket": "bucket", "Prefix": "", "KeyMarker": "k", "VersionIdMarker": "v"},
            {"Bucket": "bucket", "Prefix": "", "KeyMarker": "m", "VersionIdMarker": "w"},
        ]

    def test_ls_buckets_follows_pages(self):
        # GH-1059: ListBuckets returns pages, which _ls_buckets() used to
        # ignore beyond the first.
        fs = self._make_fs()
        fs._call.side_effect = [
            {"Buckets": [{"Name": "a"}], "ContinuationToken": "t1"},
            {"Buckets": [{"Name": "b"}]},
        ]

        assert fs.ls("s3://") == ["a", "b"]
        assert fs._call.call_args_list == [
            mock.call(fs._client.list_buckets),
            mock.call(fs._client.list_buckets, ContinuationToken="t1"),
        ]

    def test_info_keeps_head_object_fields(self):
        # The entry built from the typed HeadObject result has the fields
        # that it had when it was built from the response.
        fs = self._make_fs()
        retain_until = datetime(2027, 1, 1, tzinfo=UTC)
        fs._call.return_value = {
            "ContentLength": 4,
            "ETag": '"e"',
            "ObjectLockMode": "GOVERNANCE",
            "ObjectLockRetainUntilDate": retain_until,
            "ObjectLockLegalHoldStatus": "ON",
            "BucketKeyEnabled": False,
            "Metadata": {"a": "1"},
        }

        info = fs.info("s3://bucket/key")
        assert dict(info) == {
            "content_length": 4,
            "size": 4,
            "etag": '"e"',
            "object_lock_mode": "GOVERNANCE",
            "object_lock_retain_until_date": retain_until,
            "object_lock_legal_hold_status": "ON",
            "bucket_key_enabled": False,
            "metadata": {"a": "1"},
            "storage_class": S3StorageClass.S3_STORAGE_CLASS_STANDARD,
            "type": S3ObjectType.S3_OBJECT_TYPE_FILE,
            "bucket": "bucket",
            "key": "key",
            "version_id": None,
            "name": "bucket/key",
        }

    def test_head_object_version_aware(self):
        fs = self._make_fs()
        fs._call.return_value = {"ContentLength": 4, "ETag": '"etag"', "VersionId": "v1"}

        # The observed version is pinned only in version-aware mode.
        assert fs._head_object("bucket/key").version_id is None

        fs = self._make_fs()
        fs.version_aware = True
        fs._call.return_value = {"ContentLength": 4, "ETag": '"etag"', "VersionId": "v1"}
        assert fs._head_object("bucket/key").version_id == "v1"

    @pytest.mark.parametrize("version_aware", [False, True])
    def test_info_caches_each_version_separately(self, version_aware):
        fs = self._make_fs()
        fs.version_aware = version_aware
        responses = {
            None: {"ContentLength": 3, "ETag": '"e3"', "VersionId": "v3"},
            "v1": {"ContentLength": 1, "ETag": '"e1"', "VersionId": "v1"},
            "v2": {"ContentLength": 2, "ETag": '"e2"', "VersionId": "v2"},
        }
        fs._call.side_effect = lambda _, **kwargs: responses[kwargs.get("VersionId")]

        for _ in range(2):
            assert fs.info("s3://bucket/key", version_id="v1").size == 1
            assert fs.info("s3://bucket/key", version_id="v2").size == 2
            assert fs.info("s3://bucket/key?versionId=v1").size == 1
            assert fs.info("s3://bucket/key").size == 3
        # The second round is served from the cache.
        assert fs._call.call_count == 3

    @pytest.mark.parametrize("lookup", [False, True])
    def test_info_version_spellings_share_cache(self, lookup):
        # A version looked up with any spelling of the query is cached once,
        # so a missing version evicts it for every spelling.
        fs = self._make_fs()
        kwargs = self.LOOKUP_KWARGS if lookup else {}
        # A missing version is not looked up as a key prefix.
        fs._call.side_effect = [
            {"ContentLength": 4, "ETag": '"etag"', "VersionId": "v1"},
            FileNotFoundError("key"),
            FileNotFoundError("key"),
        ]

        assert fs.info("s3://bucket/key?versionId=v1", **kwargs).size == 4
        assert fs.info("s3://bucket/key?version_id=v1", **kwargs).size == 4
        assert fs.info("s3://bucket/key", version_id="v1", **kwargs).size == 4
        assert fs._call.call_count == 1
        with pytest.raises(FileNotFoundError):
            fs.info("s3://bucket/key?version_id=v1", refresh=True, **kwargs)
        with pytest.raises(FileNotFoundError):
            fs.info("s3://bucket/key?versionId=v1", **kwargs)
        assert fs._call.call_count == 3

    def test_info_does_not_cache_null_version(self):
        fs = self._make_fs()
        fs._call.return_value = {"ContentLength": 4, "ETag": '"etag"', "VersionId": "null"}

        for _ in range(2):
            assert fs.info("s3://bucket/key", version_id="null").size == 4
            assert fs.info("s3://bucket/key?versionId=null").size == 4
        # An overwrite can replace the null version, so it is looked up every time.
        assert fs._call.call_count == 4

    def test_object_version_info_paginates(self):
        fs = self._make_fs()
        fs._call.side_effect = [
            {
                "Versions": [
                    {"Key": "key", "VersionId": "v2", "IsLatest": True, "Size": 4},
                ],
                "DeleteMarkers": [
                    {"Key": "key", "VersionId": "m1", "IsLatest": False},
                ],
                "IsTruncated": True,
                "NextKeyMarker": "key",
                "NextVersionIdMarker": "v2",
            },
            {
                "Versions": [
                    {"Key": "key", "VersionId": "v1", "IsLatest": False, "Size": 2},
                ],
                "IsTruncated": False,
            },
        ]

        actual = fs.object_version_info("s3://bucket/key")
        assert [(v.key, v.version_id, v.is_latest, v.size) for v in actual] == [
            ("key", "v2", True, 4),
            ("key", "v1", False, 2),
        ]
        assert all(not v.is_delete_marker for v in actual)
        fs._call.assert_any_call(fs._client.list_object_versions, Bucket="bucket", Prefix="key")
        fs._call.assert_any_call(
            fs._client.list_object_versions,
            Bucket="bucket",
            Prefix="key",
            KeyMarker="key",
            VersionIdMarker="v2",
        )

    def test_object_version_info_with_delete_markers(self):
        fs = self._make_fs()
        fs._call.return_value = {
            "Versions": [{"Key": "key", "VersionId": "v1", "IsLatest": False}],
            "DeleteMarkers": [{"Key": "key", "VersionId": "m1", "IsLatest": True}],
            "IsTruncated": False,
        }

        actual = fs.object_version_info("s3://bucket/key", delete_markers=True)
        assert [(v.version_id, v.is_delete_marker) for v in actual] == [
            ("v1", False),
            ("m1", True),
        ]

    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            # The key itself wins over the keys under it.
            ("s3://bucket/a.csv", ["a.csv"]),
            # Without an object, the keys under the path are returned.
            ("s3://bucket/dir", ["dir/", "dir/x"]),
            ("s3://bucket/dir/", ["dir/", "dir/x"]),
            # A trailing slash selects the keys under the path even if the
            # key without it exists.
            ("s3://bucket/a.csv/", ["a.csv/x"]),
            ("s3://bucket", ["a.csv", "a.csv.bak", "a.csv/x", "dir/", "dir/x", "dir2/y"]),
        ],
    )
    def test_object_version_info_excludes_sibling_keys(self, path, expected):
        fs = self._make_fs()
        keys = ["a.csv", "a.csv.bak", "a.csv/x", "dir/", "dir/x", "dir2/y"]
        # S3 matches Prefix as a plain string prefix.
        fs._call.side_effect = lambda _, **request: {
            "Versions": [
                {"Key": k, "VersionId": f"v-{k}", "IsLatest": True}
                for k in keys
                if k.startswith(request["Prefix"])
            ],
            "DeleteMarkers": [
                {"Key": k, "VersionId": f"m-{k}", "IsLatest": False}
                for k in keys
                if k.startswith(request["Prefix"])
            ],
            "IsTruncated": False,
        }

        actual = fs.object_version_info(path)
        assert [v.key for v in actual] == expected
        actual = fs.object_version_info(path, delete_markers=True)
        assert sorted(v.key for v in actual if not v.is_delete_marker) == expected
        assert sorted(v.key for v in actual if v.is_delete_marker) == expected

    @pytest.mark.parametrize(
        ("delete_markers", "expected"),
        [
            (False, []),
            (True, [("dir", "m1", True)]),
        ],
    )
    def test_object_version_info_chooses_key_with_only_delete_markers(
        self, delete_markers, expected
    ):
        fs = self._make_fs()
        # "dir" is both a deleted object, with only a delete marker left, and
        # a folder.
        fs._call.return_value = {
            "Versions": [{"Key": "dir/x", "VersionId": "v1", "IsLatest": True}],
            "DeleteMarkers": [{"Key": "dir", "VersionId": "m1", "IsLatest": True}],
            "IsTruncated": False,
        }

        actual = fs.object_version_info("s3://bucket/dir", delete_markers=delete_markers)
        assert [(v.key, v.version_id, v.is_delete_marker) for v in actual] == expected

    def test_object_version_info_matches_url_encoded_keys(self):
        fs = self._make_fs()
        # With an explicit EncodingType="url", botocore leaves the keys encoded.
        fs._call.return_value = {
            "Versions": [
                {"Key": "a+b", "VersionId": "v1", "IsLatest": True},
                {"Key": "a+b.bak", "VersionId": "v2", "IsLatest": True},
            ],
            "IsTruncated": False,
        }

        actual = fs.object_version_info("s3://bucket/a b", EncodingType="url")
        assert [(v.key, v.version_id) for v in actual] == [("a+b", "v1")]

    def test_ls_versions_requires_version_aware(self):
        fs = self._make_fs()
        with pytest.raises(ValueError, match="version aware"):
            fs.ls("s3://bucket/path", versions=True)

    def test_ls_versions(self):
        fs = self._make_fs()
        fs.version_aware = True
        fs._call.return_value = {
            "CommonPrefixes": [{"Prefix": "path/dir/"}],
            "Versions": [
                {"Key": "path/key", "VersionId": "v2", "IsLatest": True, "Size": 4},
                {"Key": "path/key", "VersionId": "v1", "IsLatest": False, "Size": 2},
                {"Key": "path/other", "VersionId": "null", "IsLatest": True, "Size": 1},
            ],
            "IsTruncated": False,
        }

        actual = fs.ls("s3://bucket/path", detail=True, versions=True)
        fs._call.assert_called_once_with(
            fs._client.list_object_versions, Bucket="bucket", Prefix="path/", Delimiter="/"
        )
        # GH-979: each version is named so that it can be addressed, except
        # the "null" version, which a write to the key replaces.
        assert [(f.name, f.version_id, f.is_latest) for f in actual] == [
            ("bucket/path/dir", None, None),
            ("bucket/path/key?versionId=v2", "v2", True),
            ("bucket/path/key?versionId=v1", "v1", False),
            ("bucket/path/other", "null", True),
        ]
        assert actual[1].size == 4
        assert actual[2].size == 2
        assert fs.ls("s3://bucket/path", versions=True) == [f.name for f in actual]

    def test_ls_versions_object_path_falls_back_to_the_key(self):
        fs = self._make_fs()
        fs.version_aware = True
        fs._call.side_effect = [
            # The prefix listing returns nothing: the path is an object.
            {"IsTruncated": False},
            {
                "Versions": [
                    {"Key": "path/key", "VersionId": "v2", "IsLatest": True, "Size": 4},
                    {"Key": "path/key", "VersionId": "v1", "IsLatest": False, "Size": 2},
                    # A sibling key sharing the prefix is excluded.
                    {"Key": "path/key2", "VersionId": "x1", "IsLatest": True, "Size": 1},
                ],
                "IsTruncated": False,
            },
        ]

        actual = fs.ls("s3://bucket/path/key", detail=True, versions=True)
        fs._call.assert_any_call(
            fs._client.list_object_versions, Bucket="bucket", Prefix="path/key", Delimiter="/"
        )
        assert [(f.name, f.version_id, f.size) for f in actual] == [
            ("bucket/path/key?versionId=v2", "v2", 4),
            ("bucket/path/key?versionId=v1", "v1", 2),
        ]

    def test_dir_filesystem(self):
        # DirFileSystem copies every entry with copy() before renaming it.
        fs = self._make_fs()
        fs._call.return_value = {
            "CommonPrefixes": [{"Prefix": "path/dir/"}],
            "Contents": [{"Key": "path/key", "Size": 4}],
            "IsTruncated": False,
        }
        dir_fs = DirFileSystem(path="bucket/path", fs=fs)

        actual = dir_fs.ls("", detail=True)
        assert [(f["name"], f["type"]) for f in actual] == [("dir", "directory"), ("key", "file")]
        assert all(isinstance(f, S3Object) for f in actual)
        actual = dir_fs.info("key")
        assert isinstance(actual, S3Object)
        assert (actual.name, actual.size) == ("key", 4)
        # The cached entries keep their full names.
        assert [f.name for f in fs.ls("bucket/path", detail=True)] == [
            "bucket/path/dir",
            "bucket/path/key",
        ]
        assert fs.info("bucket/path/key").name == "bucket/path/key"
        # info() answers from the cached listing.
        fs._call.assert_called_once()

    def test_metadata_with_version_id(self):
        fs = self._make_fs()
        fs._call.return_value = {"Metadata": {}}

        assert fs.metadata("s3://bucket/key?versionId=12345abcde") == {}
        fs._call.assert_called_once_with(
            fs._client.head_object, Bucket="bucket", Key="key", VersionId="12345abcde"
        )

    def test_chmod_object(self):
        fs = self._make_fs()

        fs.chmod("s3://bucket/key", "bucket-owner-full-control")
        fs._call.assert_called_once_with(
            fs._client.put_object_acl,
            Bucket="bucket",
            Key="key",
            ACL="bucket-owner-full-control",
        )

    def test_chmod_bucket(self):
        fs = self._make_fs()

        fs.chmod("s3://bucket", "private")
        fs._call.assert_called_once_with(fs._client.put_bucket_acl, Bucket="bucket", ACL="private")

    def test_chmod_recursive(self):
        fs = self._make_fs()
        fs.find = mock.MagicMock(return_value=["bucket/key1", "bucket/key2"])

        fs.chmod("s3://bucket/path", "private", recursive=True)
        assert fs._call.call_count == 2
        fs._call.assert_any_call(
            fs._client.put_object_acl, Bucket="bucket", Key="key1", ACL="private"
        )
        fs._call.assert_any_call(
            fs._client.put_object_acl, Bucket="bucket", Key="key2", ACL="private"
        )

    def test_put_tags_merge(self):
        fs = self._make_fs()
        fs._call.side_effect = [
            {"TagSet": [{"Key": "a", "Value": "1"}, {"Key": "b", "Value": "2"}]},
            {},
        ]

        fs.put_tags("s3://bucket/key?versionId=v1", {"b": "3", "c": "4"}, mode="m")
        request = {"Bucket": "bucket", "Key": "key", "VersionId": "v1"}
        assert fs._call.call_args_list == [
            mock.call(fs._client.get_object_tagging, **request),
            mock.call(
                fs._client.put_object_tagging,
                **request,
                Tagging={
                    "TagSet": [
                        {"Key": "a", "Value": "1"},
                        {"Key": "b", "Value": "3"},
                        {"Key": "c", "Value": "4"},
                    ]
                },
            ),
        ]

    def test_put_tags_invalid_mode(self):
        fs = self._make_fs()

        with pytest.raises(ValueError, match="Mode must be"):
            fs.put_tags("s3://bucket/key", {"a": "1"}, mode="x")
        fs._call.assert_not_called()

    def test_list_multipart_uploads_paginates(self):
        fs = self._make_fs()
        fs._call.side_effect = [
            {
                "Uploads": [{"Key": "key1", "UploadId": "upload1"}],
                "IsTruncated": True,
                "NextKeyMarker": "key1",
                "NextUploadIdMarker": "upload1",
            },
            {
                "Uploads": [{"Key": "key2", "UploadId": "upload2"}],
                "IsTruncated": False,
            },
        ]

        actual = fs.list_multipart_uploads("s3://bucket")
        assert [(u.bucket, u.key, u.upload_id) for u in actual] == [
            ("bucket", "key1", "upload1"),
            ("bucket", "key2", "upload2"),
        ]
        fs._call.assert_any_call(fs._client.list_multipart_uploads, Bucket="bucket")
        fs._call.assert_any_call(
            fs._client.list_multipart_uploads,
            Bucket="bucket",
            KeyMarker="key1",
            UploadIdMarker="upload1",
        )

    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("s3://bucket/data", ["data", "data/part.csv"]),
            ("s3://bucket/data/", ["data/part.csv"]),
            ("s3://bucket", ["data", "data.csv", "data/part.csv", "data2/other.csv"]),
        ],
    )
    def test_list_and_clear_multipart_uploads_exclude_sibling_keys(self, path, expected):
        fs = self._make_fs()
        keys = ["data", "data.csv", "data/part.csv", "data2/other.csv"]
        aborted = []

        def call(method, **request):
            if method is fs._client.abort_multipart_upload:
                aborted.append(request["Key"])
                return {}
            # S3 matches Prefix as a plain string prefix.
            return {
                "Uploads": [
                    {"Key": k, "UploadId": f"u-{k}"}
                    for k in keys
                    if k.startswith(request.get("Prefix", ""))
                ],
                "IsTruncated": False,
            }

        fs._call.side_effect = call

        assert [u.key for u in fs.list_multipart_uploads(path)] == expected
        fs.clear_multipart_uploads(path)
        assert sorted(aborted) == expected

    @pytest.mark.parametrize(
        ("code", "exception"),
        [
            (None, None),
            ("NoSuchUpload", None),
            ("NoSuchBucket", FileNotFoundError),
            ("AccessDenied", PermissionError),
            ("InternalError", OSError),
        ],
    )
    def test_clear_multipart_uploads_race(self, code, exception):
        fs = S3FileSystem(
            key="dummy",
            secret="dummy",
            region_name="us-east-1",
            max_workers=1,
            retry_config=RetryConfig(attempt=1),
            skip_instance_cache=True,
        )
        with Stubber(fs.core.client) as stubber:
            stubber.add_response(
                "list_multipart_uploads",
                {
                    "Uploads": [
                        {"Key": "prefix/gone", "UploadId": "gone"},
                        {
                            "Key": "prefix/pending",
                            "UploadId": "pending",
                            "ChecksumAlgorithm": "CRC32",
                            "ChecksumType": "FULL_OBJECT",
                        },
                    ],
                    "IsTruncated": False,
                },
                {"Bucket": "bucket", "Prefix": "prefix/"},
            )
            request = {"Bucket": "bucket", "Key": "prefix/gone", "UploadId": "gone"}
            if code:
                stubber.add_client_error(
                    "abort_multipart_upload",
                    service_error_code=code,
                    http_status_code=404 if code.startswith("NoSuch") else 500,
                    expected_params=request,
                )
            else:
                stubber.add_response("abort_multipart_upload", {}, request)
            stubber.add_response(
                "abort_multipart_upload",
                {},
                {"Bucket": "bucket", "Key": "prefix/pending", "UploadId": "pending"},
            )
            expected = pytest.raises(exception) if exception else contextlib.nullcontext()
            with expected:
                fs.clear_multipart_uploads("s3://bucket/prefix/")
            stubber.assert_no_pending_responses()

    def test_clear_multipart_uploads_empty(self):
        fs = S3FileSystem(
            key="dummy",
            secret="dummy",
            region_name="us-east-1",
            max_workers=1,
            skip_instance_cache=True,
        )
        with Stubber(fs.core.client) as stubber:
            stubber.add_response(
                "list_multipart_uploads",
                {"Uploads": [], "IsTruncated": False},
                {"Bucket": "bucket", "Prefix": "prefix/"},
            )
            fs.clear_multipart_uploads("s3://bucket/prefix/")
            stubber.assert_no_pending_responses()

    def test_clear_multipart_uploads_checks_all_results(self, monkeypatch):
        fs = S3FileSystem(
            key="dummy", secret="dummy", region_name="us-east-1", skip_instance_cache=True
        )
        fs.list_multipart_uploads = mock.MagicMock(
            return_value=[
                S3MultipartUpload({"Bucket": "bucket", "Key": f"prefix/{n}", "UploadId": str(n)})
                for n in range(3)
            ]
        )
        error = PermissionError("denied")
        futures = [mock.Mock(), mock.Mock(), mock.Mock()]
        futures[0].result.side_effect = error
        futures[2].result.side_effect = FileNotFoundError("unclassified missing resource")
        executor = mock.MagicMock()
        executor.__enter__.return_value.submit.side_effect = futures
        monkeypatch.setattr(fs, "_create_executor", mock.Mock(return_value=executor))
        monkeypatch.setattr("pyathena.filesystem.s3.as_completed", lambda pending: iter(pending))
        with pytest.raises(PermissionError) as raised:
            fs.clear_multipart_uploads("s3://bucket/prefix/")
        assert raised.value is error
        for future in futures:
            future.result.assert_called_once_with()

    def test_clear_multipart_uploads_preserves_unclassified_file_not_found(self):
        fs = S3FileSystem(
            key="dummy", secret="dummy", region_name="us-east-1", skip_instance_cache=True
        )
        fs.list_multipart_uploads = mock.MagicMock(
            return_value=[
                S3MultipartUpload({"Bucket": "bucket", "Key": "prefix/key", "UploadId": "u"})
            ]
        )
        error = FileNotFoundError("unclassified missing resource")
        fs.core.abort_multipart_upload = mock.MagicMock(side_effect=error)
        with pytest.raises(FileNotFoundError) as raised:
            fs.clear_multipart_uploads("s3://bucket/prefix/")
        assert raised.value is error

    @pytest.mark.parametrize(
        ("algorithm", "checksum_type"),
        [(None, None), ("SHA256", None), ("CRC32", None), ("CRC32", "FULL_OBJECT")],
    )
    def test_multipart_copy_uses_creation_algorithm(self, algorithm, checksum_type):
        fs = S3FileSystem(
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
            fs._copy_object_with_multipart_upload(**kwargs)
            stubber.assert_no_pending_responses()

    @pytest.fixture(scope="class")
    def fs(self, request):
        if not hasattr(request, "param"):
            request.param = {}
        return S3FileSystem(connect(), **request.param)

    @pytest.mark.parametrize(
        ("algorithm", "checksum_type"),
        [(None, None), ("SHA256", None), ("CRC32", None), ("CRC32", "FULL_OBJECT")],
    )
    def test_open_multipart_with_checksum(self, fs, algorithm, checksum_type):
        block_size = 5 * 2**20
        data = b"x" * (block_size + 1)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_open_multipart_with_checksum/{uuid.uuid4()}"
        )
        kwargs = {"ChecksumAlgorithm": algorithm} if algorithm else {}
        if checksum_type:
            kwargs["ChecksumType"] = checksum_type
        try:
            with fs.open(path, "wb", block_size=block_size, s3_additional_kwargs=kwargs) as file:
                file.write(data)
            assert fs.cat_file(path) == data
            assert fs.list_multipart_uploads(path) == []
        finally:
            fs.clear_multipart_uploads(path)
            if fs.exists(path):
                fs.rm(path)

    @pytest.mark.parametrize(
        ("algorithm", "checksum_type"),
        [(None, None), ("SHA256", None), ("CRC32", None), ("CRC32", "FULL_OBJECT")],
    )
    def test_put_file_multipart_with_checksum(self, fs, tmp_path, algorithm, checksum_type):
        block_size = 5 * 2**20
        data = b"x" * (block_size + 1)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_put_file_multipart_with_checksum/{uuid.uuid4()}"
        )
        kwargs = {"ChecksumAlgorithm": algorithm} if algorithm else {}
        if checksum_type:
            kwargs["ChecksumType"] = checksum_type
        try:
            lpath = tmp_path / "data"
            lpath.write_bytes(data)
            fs.put_file(str(lpath), path, block_size=block_size, s3_additional_kwargs=kwargs)
            assert fs.cat_file(path) == data
            assert fs.list_multipart_uploads(path) == []
        finally:
            fs.clear_multipart_uploads(path)
            if fs.exists(path):
                fs.rm(path)

    @pytest.mark.parametrize(
        ("algorithm", "checksum_type"),
        [(None, None), ("SHA256", None), ("CRC32", None), ("CRC32", "FULL_OBJECT")],
    )
    def test_pipe_file_multipart_with_checksum(self, fs, algorithm, checksum_type):
        block_size = 5 * 2**20
        data = b"x" * (block_size + 1)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_pipe_file_multipart_with_checksum/{uuid.uuid4()}"
        )
        kwargs = {"ChecksumAlgorithm": algorithm} if algorithm else {}
        if checksum_type:
            kwargs["ChecksumType"] = checksum_type
        try:
            fs.pipe_file(path, data, block_size=block_size, s3_additional_kwargs=kwargs)
            assert fs.cat_file(path) == data
            assert fs.list_multipart_uploads(path) == []
        finally:
            fs.clear_multipart_uploads(path)
            if fs.exists(path):
                fs.rm(path)

    @pytest.mark.parametrize(
        ("algorithm", "checksum_type"),
        [(None, None), ("SHA256", None), ("CRC32", None), ("CRC32", "FULL_OBJECT")],
    )
    def test_append_multipart_with_checksum(self, fs, algorithm, checksum_type):
        block_size = 5 * 2**20
        data = b"x" * (block_size + 1)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_append_multipart_with_checksum/{uuid.uuid4()}"
        )
        kwargs = {"ChecksumAlgorithm": algorithm} if algorithm else {}
        if checksum_type:
            kwargs["ChecksumType"] = checksum_type
        try:
            original = b"y" * block_size
            fs.pipe_file(path, original)
            with fs.open(path, "ab", block_size=block_size, s3_additional_kwargs=kwargs) as file:
                file.write(data)
            assert fs.cat_file(path) == original + data
            assert fs.list_multipart_uploads(path) == []
        finally:
            fs.clear_multipart_uploads(path)
            if fs.exists(path):
                fs.rm(path)

    @pytest.mark.parametrize(
        ("algorithm", "checksum_type"),
        [(None, None), ("SHA256", "COMPOSITE"), ("CRC32", "FULL_OBJECT")],
    )
    @pytest.mark.parametrize("copy", [False, True])
    def test_core_multipart_upload_with_checksum(self, fs, algorithm, checksum_type, copy):
        prefix = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_core_multipart_with_checksum/{uuid.uuid4()}/"
        )
        destination = S3Path.parse(f"{prefix}destination")
        source = S3Path.parse(f"{prefix}source")
        data = b"x" * S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE
        kwargs = (
            {"ChecksumAlgorithm": algorithm, "ChecksumType": checksum_type} if algorithm else {}
        )
        try:
            if copy:
                fs.pipe_file(source.uri, data)
            upload = fs.core.create_multipart_upload(destination, **kwargs)
            if algorithm:
                assert upload.checksum_algorithm == algorithm
                assert upload.checksum_type == checksum_type
            listed = fs.list_multipart_uploads(destination.uri)
            assert len(listed) == 1
            assert listed[0].upload_id == upload.upload_id
            assert listed[0].checksum_algorithm == upload.checksum_algorithm
            assert listed[0].checksum_type == upload.checksum_type
            if copy:
                first = fs.core.upload_part_copy(upload, 1, source)
            else:
                first = fs.core.upload_part(upload, 1, data)
            last = fs.core.upload_part(upload, 2, b"end")
            fs.core.complete_multipart_upload(upload, [first, last])
            if checksum_type == "FULL_OBJECT":
                metadata = fs.core.call(
                    "head_object", Bucket=upload.bucket, Key=upload.key, ChecksumMode="ENABLED"
                )
                assert metadata["ChecksumType"] == "FULL_OBJECT"
                assert (
                    metadata["ChecksumCRC32"]
                    == b64encode(crc32(data + b"end").to_bytes(4, "big")).decode()
                )
            fs.invalidate_cache(destination.uri)
            assert fs.cat_file(destination.uri) == data + b"end"
            assert fs.list_multipart_uploads(destination.uri) == []
        finally:
            fs.clear_multipart_uploads(prefix)
            for path in (source, destination):
                if fs.exists(path.uri):
                    fs.rm(path.uri)

    def test_clear_multipart_uploads_after_listed_upload_is_aborted(self, fs):
        prefix = (
            f"{ENV.s3_staging_key}{ENV.schema}/filesystem/test_clear_multipart_race/{uuid.uuid4()}/"
        )
        path = f"s3://{ENV.s3_staging_bucket}/{prefix}"
        gone_path = S3Path(ENV.s3_staging_bucket, f"{prefix}gone")
        gone = fs.core.create_multipart_upload(gone_path)
        try:
            fs.core.create_multipart_upload(S3Path(ENV.s3_staging_bucket, f"{prefix}pending"))
            list_uploads = fs.list_multipart_uploads

            def list_then_abort(path):
                uploads = list_uploads(path)
                assert len(uploads) == 2
                assert any(upload.upload_id == gone.upload_id for upload in uploads)
                fs.core.abort_multipart_upload(gone)
                return uploads

            with mock.patch.object(
                fs, "list_multipart_uploads", side_effect=list_then_abort
            ) as list_mock:
                fs.clear_multipart_uploads(path)
                list_mock.assert_called_once_with(path)
            assert fs.list_multipart_uploads(path) == []
        finally:
            fs.clear_multipart_uploads(path)

    @pytest.mark.parametrize(
        ("fs", "start", "end", "target_data"),
        list(
            chain(
                *[
                    [
                        ({"default_block_size": x}, 0, 5, b"01234"),
                        ({"default_block_size": x}, 2, 7, b"23456"),
                        ({"default_block_size": x}, 0, 10, b"0123456789"),
                    ]
                    for x in (S3FileSystem.DEFAULT_BLOCK_SIZE, 3)
                ]
            )
        ),
        indirect=["fs"],
    )
    def test_read(self, fs, start, end, target_data):
        # lowest level access: use the core
        data = fs.core.get_object(
            S3Path(ENV.s3_staging_bucket, ENV.s3_filesystem_test_file_key), (start, end)
        )
        assert data == target_data, data
        with fs.open(
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}", "rb"
        ) as file:
            # mid-level access: use _fetch_range
            data = file._fetch_range(start, end)
            assert data == target_data, data
            # high-level: use fileobj seek and read
            file.seek(start)
            data = file.read(end - start)
            assert data == target_data, data

    @pytest.mark.parametrize(
        ("base", "exp"),
        [
            # TODO: Comment out some test cases because of the high cost of AWS for testing.
            (1, 2**10),
            # (10, 2**10),
            # (100, 2**10),
            (1, 2**20),
            # (10, 2**20),
            # (100, 2**20),
            # (1024, 2**20),
            # TODO: Perhaps OOM is occurring and the worker is shutting down.
            #   The runner has received a shutdown signal.
            #   This can happen when the runner service is stopped,
            #   or a manually started runner is canceled.
            # (5 * 1024 + 1, 2**20),
        ],
    )
    def test_write(self, fs, base, exp):
        data = b"a" * (base * exp)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_write/{uuid.uuid4()}"
        )
        with fs.open(path, "wb") as f:
            f.write(data)
        with fs.open(path, "rb") as f:
            actual = f.read()
            assert len(actual) == len(data)
            assert actual == data

    def test_write_multiple_blocks_then_more(self, fs):
        # GH-942: a single write() of more than two blocks with a short tail,
        # followed by more data, must not leave a part smaller than the
        # minimum part size before the last part.
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_write_multiple_blocks_then_more/{uuid.uuid4()}"
        )
        size = 2 * fs.default_block_size + 2**20
        with fs.open(path, "wb") as f:
            f.write(b"a" * size)
            f.write(b"b")
        assert fs.info(path).get("size") == size + 1

    @pytest.mark.parametrize(
        "size",
        [
            2**10,  # < block size: one-shot PutObject path (the GH-719 regression)
            10 * 2**20,  # > block size (5 MiB): multipart CompleteMultipartUpload path
        ],
    )
    def test_write_transaction(self, fs, size):
        # Regression test for GH-719: files written inside an fsspec transaction
        # (autocommit=False) must round-trip with their real content (small files
        # were previously committed as empty objects).
        data = b"a" * size
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_write_transaction/{uuid.uuid4()}"
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
            2**10,  # < block size: small-file discard() is a no-op
            10 * 2**20,  # > block size: discard() aborts the multipart upload
        ],
    )
    def test_write_transaction_rollback(self, fs, size):
        # Raising inside the transaction must leave no object behind.
        data = b"a" * size
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_write_transaction_rollback/{uuid.uuid4()}"
        )

        def write_then_fail():
            with fs.transaction:
                f = fs.open(path, "wb")
                f.write(data)
                f.close()
                raise RuntimeError("rollback")

        with pytest.raises(RuntimeError):
            write_then_fail()
        fs.invalidate_cache(path)
        assert not fs.exists(path)

    @pytest.mark.parametrize(
        ("base", "exp"),
        [
            # TODO: Comment out some test cases because of the high cost of AWS for testing.
            (1, 2**10),
            # (10, 2**10),
            # (100, 2**10),
            (1, 2**20),
            # (10, 2**20),
            # (100, 2**20),
            # (1024, 2**20),
            # TODO: Perhaps OOM is occurring and the worker is shutting down.
            #   The runner has received a shutdown signal.
            #   This can happen when the runner service is stopped,
            #   or a manually started runner is canceled.
            # (5 * 1024 + 1, 2**20),
        ],
    )
    def test_append(self, fs, base, exp):
        # TODO: Check the metadata is kept.
        data = b"a" * (base * exp)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_append/{uuid.uuid4()}"
        )
        with fs.open(path, "ab") as f:
            f.write(data)
        extra = b"extra"
        with fs.open(path, "ab") as f:
            f.write(extra)
        with fs.open(path, "rb") as f:
            actual = f.read()
            assert len(actual) == len(data + extra)
            assert actual == data + extra

    @pytest.mark.parametrize(
        ("size", "extra_size", "block_size"),
        [
            # GH-921: an existing object of at least 5 MiB, appended within a
            # larger block size, is copied with UploadPartCopy.
            (6 * 2**20, 5, 16 * 2**20),
            # An existing object smaller than 5 MiB is rewritten from the
            # buffer, not copied as well, when the append crosses the block size.
            (2**10, 5 * 2**20, None),
        ],
    )
    def test_append_with_block_size(self, fs, size, extra_size, block_size):
        data = b"a" * size
        extra = b"b" * extra_size
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_append_with_block_size/{uuid.uuid4()}"
        )
        fs.pipe_file(path, data)
        with fs.open(path, "ab", block_size=block_size) as f:
            f.write(extra)
        # Check the size and the bytes at the ends and around the boundary
        # instead of reading the whole object back, to keep the transfer small.
        assert fs.info(path, refresh=True).size == size + extra_size
        assert fs.cat_file(path, start=0, end=1) == b"a"
        assert fs.cat_file(path, start=size - 1, end=size + 1) == b"ab"
        assert fs.cat_file(path, start=-1) == b"b"

    @pytest.mark.parametrize("block_size", [None, 16 * 2**20])
    def test_append_transaction_rollback(self, fs, block_size):
        # Raising inside the transaction aborts the multipart upload that
        # copies the existing object and leaves the object unchanged.
        data = b"a" * (6 * 2**20)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_append_transaction_rollback/{uuid.uuid4()}"
        )
        fs.pipe_file(path, data)
        before = fs.info(path, refresh=True)

        def append_then_fail():
            with fs.transaction:
                f = fs.open(path, "ab", block_size=block_size)
                f.write(b"b" * 5)
                f.close()
                raise RuntimeError("rollback")

        with pytest.raises(RuntimeError):
            append_then_fail()
        # A committed append (a multipart upload, or the appended bytes alone)
        # would change the ETag and the size, so the object is not read back.
        after = fs.info(path, refresh=True)
        assert (after.etag, after.last_modified, after.size) == (
            before.etag,
            before.last_modified,
            before.size,
        )
        assert not fs.list_multipart_uploads(path)

    def test_ls_buckets(self, fs):
        fs.invalidate_cache()
        actual = fs.ls("s3://")
        assert ENV.s3_staging_bucket in actual, actual

        fs.invalidate_cache()
        actual = fs.ls("s3:///")
        assert ENV.s3_staging_bucket in actual, actual

        fs.invalidate_cache()
        ls = fs.ls("s3://", detail=True)
        actual = next(filter(lambda x: x.name == ENV.s3_staging_bucket, ls), None)
        assert actual
        assert actual.name == ENV.s3_staging_bucket

        fs.invalidate_cache()
        ls = fs.ls("s3:///", detail=True)
        actual = next(filter(lambda x: x.name == ENV.s3_staging_bucket, ls), None)
        assert actual
        assert actual.name == ENV.s3_staging_bucket

    def test_ls_dirs(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/filesystem/test_ls_dirs"
        )
        for i in range(5):
            fs.pipe(f"{dir_}/prefix/test_{i}", bytes(i))
        fs.touch(f"{dir_}/prefix2")

        assert len(fs.ls(f"{dir_}/prefix")) == 5
        assert len(fs.ls(f"{dir_}/prefix/")) == 5
        assert len(fs.ls(f"{dir_}/prefix/test_")) == 0
        assert len(fs.ls(f"{dir_}/prefix2")) == 1

        test_1 = fs.ls(f"{dir_}/prefix/test_1")
        assert len(test_1) == 1
        assert test_1[0] == fs._strip_protocol(f"{dir_}/prefix/test_1")

        test_1_detail = fs.ls(f"{dir_}/prefix/test_1", detail=True)
        assert len(test_1_detail) == 1
        assert test_1_detail[0].name == fs._strip_protocol(f"{dir_}/prefix/test_1")
        assert test_1_detail[0].size == 1

    def test_ls_and_find_reflect_changes_through_the_filesystem(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_ls_and_find_reflect_changes/{uuid.uuid4()}"
        )
        path = fs._strip_protocol(dir_)
        fs.touch(f"{dir_}/a.txt")
        fs.touch(f"{dir_}/b.txt")
        assert sorted(fs.ls(dir_)) == [f"{path}/a.txt", f"{path}/b.txt"]
        assert sorted(fs.find(dir_)) == [f"{path}/a.txt", f"{path}/b.txt"]

        fs.rm(f"{dir_}/a.txt")
        assert fs.ls(dir_) == [f"{path}/b.txt"]
        assert fs.find(dir_) == [f"{path}/b.txt"]

        fs.touch(f"{dir_}/c.txt")
        assert sorted(fs.ls(dir_)) == [f"{path}/b.txt", f"{path}/c.txt"]
        assert sorted(fs.find(dir_)) == [f"{path}/b.txt", f"{path}/c.txt"]
        # A prefixed find must not be served from the unprefixed listing.
        assert fs.find(dir_, prefix="c") == [f"{path}/c.txt"]

        fs.rm(dir_, recursive=True)

    def test_info_bucket(self, fs):
        dir_ = f"s3://{ENV.s3_staging_bucket}"
        bucket, key, version_id = fs.parse_path(dir_)
        info = fs.info(dir_)

        assert info.name == fs._strip_protocol(dir_)
        assert info.bucket == bucket
        assert info.key is None
        assert info.last_modified is None
        assert info.size == 0
        assert info.etag is None
        assert info.type == S3ObjectType.S3_OBJECT_TYPE_DIRECTORY
        assert info.storage_class == S3StorageClass.S3_STORAGE_CLASS_BUCKET
        assert info.version_id == version_id

        dir_ = f"s3://{ENV.s3_staging_bucket}/"
        bucket, key, version_id = fs.parse_path(dir_)
        info = fs.info(dir_)

        assert info.name == fs._strip_protocol(dir_)
        assert info.bucket == bucket
        assert info.key is None
        assert info.last_modified is None
        assert info.size == 0
        assert info.etag is None
        assert info.type == S3ObjectType.S3_OBJECT_TYPE_DIRECTORY
        assert info.storage_class == S3StorageClass.S3_STORAGE_CLASS_BUCKET
        assert info.version_id == version_id

    def test_info_dir(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_info_dir"
        )
        file = f"{dir_}/{uuid.uuid4()}"

        fs.invalidate_cache()
        with pytest.raises(FileNotFoundError):
            fs.info(f"s3://{uuid.uuid4()}")

        fs.pipe(file, b"a")
        bucket, key, version_id = fs.parse_path(dir_)
        fs.invalidate_cache()
        info = fs.info(dir_)
        fs.invalidate_cache()

        assert info.name == fs._strip_protocol(dir_)
        assert info.bucket == bucket
        assert info.key == key.rstrip("/")
        assert info.last_modified is None
        assert info.size == 0
        assert info.etag is None
        assert info.type == S3ObjectType.S3_OBJECT_TYPE_DIRECTORY
        assert info.storage_class == S3StorageClass.S3_STORAGE_CLASS_DIRECTORY
        assert info.version_id == version_id

    def test_info_file(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_info_file"
        )
        file = f"{dir_}/{uuid.uuid4()}"

        fs.invalidate_cache()
        with pytest.raises(FileNotFoundError):
            fs.info(file)

        now = datetime.now(UTC)
        fs.pipe(file, b"a")
        bucket, key, version_id = fs.parse_path(file)
        fs.invalidate_cache()
        info = fs.info(file)
        fs.invalidate_cache()
        ls_info = fs.ls(file, detail=True)[0]

        assert info == ls_info
        assert info.name == fs._strip_protocol(file)
        assert info.bucket == bucket
        assert info.key == key
        assert info.last_modified >= now
        assert info.size == 1
        assert info.etag is not None
        assert info.type == S3ObjectType.S3_OBJECT_TYPE_FILE
        assert info.storage_class == S3StorageClass.S3_STORAGE_CLASS_STANDARD
        assert info.version_id == version_id

    def test_find(self, fs):
        dir_ = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/filesystem/test_find"
        for i in range(5):
            fs.pipe(f"{dir_}/prefix/test_{i}", bytes(i))
        fs.touch(f"{dir_}/prefix2")

        assert len(fs.find(f"{dir_}/prefix")) == 5
        assert len(fs.find(f"{dir_}/prefix/")) == 5
        assert len(fs.find(dir_, prefix="prefix")) == 6
        assert len(fs.find(f"{dir_}/prefix/test_")) == 0
        assert len(fs.find(f"{dir_}/prefix", prefix="test_")) == 5
        assert len(fs.find(f"{dir_}/prefix/", prefix="test_")) == 5

        test_1 = fs.find(f"{dir_}/prefix/test_1")
        assert len(test_1) == 1
        assert test_1[0] == fs._strip_protocol(f"{dir_}/prefix/test_1")

        test_1_detail = fs.find(f"{dir_}/prefix/test_1", detail=True)
        assert len(test_1_detail) == 1
        assert test_1_detail[
            fs._strip_protocol(f"{dir_}/prefix/test_1")
        ].name == fs._strip_protocol(f"{dir_}/prefix/test_1")
        assert test_1_detail[fs._strip_protocol(f"{dir_}/prefix/test_1")].size == 1

    def test_find_maxdepth(self, fs):
        dir_ = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/filesystem/test_find_maxdepth"
        # Create files at different depths
        fs.touch(f"{dir_}/file0.txt")
        fs.touch(f"{dir_}/level1/file1.txt")
        fs.touch(f"{dir_}/level1/level2/file2.txt")
        fs.touch(f"{dir_}/level1/level2/level3/file3.txt")

        # maxdepth must be at least 1, as in fsspec
        with pytest.raises(ValueError, match="maxdepth must be at least 1"):
            fs.find(dir_, maxdepth=0)

        # Test maxdepth=1 (only files in the root)
        result = fs.find(dir_, maxdepth=1)
        assert len(result) == 1
        assert fs._strip_protocol(f"{dir_}/file0.txt") in result

        # Test maxdepth=2 (files in root and level1)
        result = fs.find(dir_, maxdepth=2)
        assert len(result) == 2
        assert fs._strip_protocol(f"{dir_}/file0.txt") in result
        assert fs._strip_protocol(f"{dir_}/level1/file1.txt") in result

        # Test maxdepth=3 (files in root, level1, and level2)
        result = fs.find(dir_, maxdepth=3)
        assert len(result) == 3
        assert fs._strip_protocol(f"{dir_}/level1/level2/file2.txt") in result

        # Test no maxdepth (all files)
        result = fs.find(dir_)
        assert len(result) == 4

        # Each slash in the prefix counts as one level
        assert fs.find(dir_, maxdepth=1, prefix="level1/") == []
        assert fs.find(dir_, maxdepth=2, prefix="level1/") == [
            fs._strip_protocol(f"{dir_}/level1/file1.txt")
        ]

        # An object path returns the object itself
        assert fs.find(f"{dir_}/file0.txt", maxdepth=1) == [fs._strip_protocol(f"{dir_}/file0.txt")]

    def test_find_withdirs(self, fs):
        dir_ = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/filesystem/test_find_withdirs"
        # Create directory structure with files
        fs.touch(f"{dir_}/file1.txt")
        fs.touch(f"{dir_}/subdir1/file2.txt")
        fs.touch(f"{dir_}/subdir1/subdir2/file3.txt")
        fs.touch(f"{dir_}/subdir3/file4.txt")

        # Test default behavior (withdirs=False)
        result = fs.find(dir_)
        assert len(result) == 4  # Only files
        for r in result:
            assert r.endswith(".txt")

        # Test withdirs=True
        result = fs.find(dir_, withdirs=True)
        assert len(result) > 4  # Files and directories
        assert fs._strip_protocol(dir_) in result
        assert fs._strip_protocol(dir_) in fs.find(dir_, maxdepth=1, withdirs=True)

        # Verify directories are included
        dirs = [r for r in result if not r.endswith(".txt")]
        assert len(dirs) > 0
        assert any("subdir1" in d for d in dirs)
        assert any("subdir2" in d for d in dirs)
        assert any("subdir3" in d for d in dirs)

        # Test withdirs=False explicitly
        result = fs.find(dir_, withdirs=False)
        assert len(result) == 4  # Only files

    def test_du(self, fs):
        """Disk usage reports file sizes, their total, and the requested depth."""
        directory = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/filesystem/test_du"
        )
        first = f"{directory}/first"
        second = f"{directory}/nested/second"
        try:
            fs.pipe_file(first, b"abc")
            fs.pipe_file(second, b"12345")
            assert fs.du(directory) == 8
            assert fs.du(directory, total=False) == {
                fs._strip_protocol(first): 3,
                fs._strip_protocol(second): 5,
            }
            assert fs.du(directory, maxdepth=1) == 3
            assert fs.du(first) == 3
        finally:
            with contextlib.suppress(FileNotFoundError):
                fs.rm(directory, recursive=True)

    def test_glob(self, fs):
        dir_ = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/filesystem/test_glob"
        path = f"{dir_}/nested/test_{uuid.uuid4()}"
        fs.touch(path)

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

    def test_exists_bucket(self, fs):
        assert fs.exists("s3://")
        assert fs.exists("s3:///")

        path = f"s3://{ENV.s3_staging_bucket}"
        assert fs.exists(path)

        not_exists_path = f"s3://{uuid.uuid4()}"
        assert not fs.exists(not_exists_path)

    def test_exists_object(self, fs):
        path = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}"
        assert fs.exists(path)

        not_exists_path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_exists/{uuid.uuid4()}"
        )
        assert not fs.exists(not_exists_path)

    def test_rm_file(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/filesystem/test_rm_rile"
        )
        file = f"{dir_}/{uuid.uuid4()}"
        fs.touch(file)
        fs.rm_file(file)

        assert not fs.exists(file)
        assert not fs.exists(dir_)

    def test_rm(self, fs):
        dir_ = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/filesystem/test_rm"
        file = f"{dir_}/{uuid.uuid4()}"
        fs.touch(file)
        fs.rm(file)

        assert not fs.exists(file)
        assert not fs.exists(dir_)

    def test_rm_recursive(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_rm_recursive"
        )

        files = [f"{dir_}/{uuid.uuid4()}" for _ in range(10)]
        for f in files:
            fs.touch(f)

        fs.rm(dir_)
        for f in files:
            assert fs.exists(f)
        assert fs.exists(dir_)

        fs.rm(dir_, recursive=True)
        for f in files:
            assert not fs.exists(f)
        assert not fs.exists(dir_)

    def test_touch(self, fs):
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_touch/{uuid.uuid4()}"
        )
        assert not fs.exists(path)
        fs.touch(path)
        assert fs.exists(path)
        assert fs.size(path) == 0

        with fs.open(path, "wb") as f:
            f.write(b"data")
        assert fs.size(path) == 4
        fs.touch(path, truncate=True)
        assert fs.size(path) == 0

        with fs.open(path, "wb") as f:
            f.write(b"data")
        assert fs.size(path) == 4
        with pytest.raises(ValueError, match="Cannot touch"):
            fs.touch(path, truncate=False)
        assert fs.size(path) == 4

    @pytest.mark.parametrize(
        ("base", "exp"),
        [
            # TODO: Comment out some test cases because of the high cost of AWS for testing.
            (1, 2**10),
            # (10, 2**10),
            # (100, 2**10),
            (1, 2**20),  # < block size (5 MiB): single PutObject
            (6, 2**20),  # > block size (5 MiB): parallel multipart upload
            # (10, 2**20),
            # (100, 2**20),
            # (1024, 2**20),
            # TODO: Perhaps OOM is occurring and the worker is shutting down.
            #   The runner has received a shutdown signal.
            #   This can happen when the runner service is stopped,
            #   or a manually started runner is canceled.
            # (5 * 1024 + 1, 2**20),
        ],
    )
    def test_pipe_cat(self, fs, base, exp):
        data = b"a" * (base * exp)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_pipe_file/{uuid.uuid4()}"
        )
        fs.pipe(path, data)
        assert fs.cat(path) == data

    def test_exclusive_create(self, fs, tmp_path):
        # GH-972: "xb" and put_file(mode="create") used to replace an
        # existing object.
        prefix = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_exclusive_create/{uuid.uuid4()}"
        )
        path = f"{prefix}/existing"
        with fs.open(path, "xb") as f:
            f.write(b"old")
        lpath = tmp_path / "data"
        lpath.write_bytes(b"new")
        with pytest.raises(FileExistsError):
            fs.open(path, "xb")
        with pytest.raises(FileExistsError):
            fs.put_file(str(lpath), path, mode="create")
        assert fs.cat(path) == b"old"

        # S3 rejects the conditional write of an object created after the
        # file was opened.
        path = f"{prefix}/created_since"
        f = fs.open(path, "xb")
        f.write(b"new")
        fs.pipe_file(path, b"old")
        with pytest.raises(FileExistsError):
            f.close()
        assert fs.cat(path) == b"old"

    def test_pipe_file_create_mode_and_kwargs(self, fs):
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_pipe_file_create/{uuid.uuid4()}"
        )
        data = b"0123456789"
        fs.pipe_file(path, data, ContentType="text/plain")
        assert fs.cat(path) == data
        assert fs.metadata(path).content_type == "text/plain"

        with pytest.raises(FileExistsError):
            fs.pipe_file(path, data, mode="create")

    def test_pipe_file_transaction(self, fs):
        # Inside an fsspec transaction the write is deferred to the commit.
        data = b"0123456789"
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_pipe_file_transaction/{uuid.uuid4()}"
        )
        with fs.transaction:
            fs.pipe_file(path, data)
        assert fs.cat(path) == data

        # Raising inside the transaction must leave no object behind.
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_pipe_file_transaction/{uuid.uuid4()}"
        )

        def write_then_fail():
            with fs.transaction:
                fs.pipe_file(path, data)
                raise RuntimeError("rollback")

        with pytest.raises(RuntimeError):
            write_then_fail()
        fs.invalidate_cache(path)
        assert not fs.exists(path)

    def test_cat_ranges(self, fs):
        data = b"1234567890abcdefghijklmnopqrstuvwxyz"
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_cat_ranges/{uuid.uuid4()}"
        )
        fs.pipe(path, data)

        assert fs.cat_file(path) == data
        assert fs.cat_file(path, start=5) == data[5:]
        assert fs.cat_file(path, end=5) == data[:5]
        assert fs.cat_file(path, start=1, end=-1) == data[1:-1]
        assert fs.cat_file(path, start=-5) == data[-5:]

    @pytest.mark.parametrize(
        ("base", "exp"),
        [
            # TODO: Comment out some test cases because of the high cost of AWS for testing.
            (1, 2**10),
            # (10, 2**10),
            # (100, 2**10),
            (1, 2**20),
            # (10, 2**20),
            # (100, 2**20),
            # (1024, 2**20),
            # TODO: Perhaps OOM is occurring and the worker is shutting down.
            #   The runner has received a shutdown signal.
            #   This can happen when the runner service is stopped,
            #   or a manually started runner is canceled.
            # (5 * 1024 + 1, 2**20),
        ],
    )
    def test_put(self, fs, base, exp):
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            data = b"a" * (base * exp)
            tmp.write(data)
            tmp.flush()

            # put
            rpath = (
                f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
                f"filesystem/test_put/{uuid.uuid4()}"
            )
            fs.put(lpath=tmp.name, rpath=rpath)
            tmp.seek(0)
            assert fs.cat(rpath) == tmp.read()

    @pytest.mark.parametrize(
        ("base", "exp"),
        [
            # TODO: Comment out some test cases because of the high cost of AWS for testing.
            (1, 2**10),
            # (10, 2**10),
            # (100, 2**10),
            (1, 2**20),
            # (10, 2**20),
            # (100, 2**20),
            # (1024, 2**20),
            # TODO: Perhaps OOM is occurring and the worker is shutting down.
            #   The runner has received a shutdown signal.
            #   This can happen when the runner service is stopped,
            #   or a manually started runner is canceled.
            # (5 * 1024 + 1, 2**20),
        ],
    )
    def test_put_with_callback(self, fs, base, exp):
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            data = b"a" * (base * exp)
            tmp.write(data)
            tmp.flush()

            # put_file
            rpath = (
                f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
                f"filesystem/test_put_with_callback/{uuid.uuid4()}"
            )
            callback = Callback()
            fs.put_file(lpath=tmp.name, rpath=rpath, callback=callback)
            tmp.seek(0)
            assert fs.cat(rpath) == tmp.read()
            assert callback.size == os.stat(tmp.name).st_size
            assert callback.value == callback.size

    @pytest.mark.parametrize(
        ("base", "exp"),
        [
            # TODO: Comment out some test cases because of the high cost of AWS for testing.
            (1, 2**10),
            # (10, 2**10),
            # (100, 2**10),
            (1, 2**20),
            # (10, 2**20),
            # (100, 2**20),
            # (1024, 2**20),
            # TODO: Perhaps OOM is occurring and the worker is shutting down.
            #   The runner has received a shutdown signal.
            #   This can happen when the runner service is stopped,
            #   or a manually started runner is canceled.
            # (5 * 1024 + 1, 2**20),
        ],
    )
    def test_upload_cp_file(self, fs, base, exp):
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            data = b"a" * (base * exp)
            tmp.write(data)
            tmp.flush()

            rpath = (
                f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
                f"filesystem/test_upload_copy_file/{uuid.uuid4()}"
            )
            fs.upload(lpath=tmp.name, rpath=rpath)

            rpath_copy = (
                f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
                f"filesystem/test_put_file_copy_file/{uuid.uuid4()}"
            )
            fs.cp_file(path1=rpath, path2=rpath_copy)
            tmp.seek(0)
            assert fs.cat(rpath_copy) == tmp.read()
            assert fs.cat(rpath_copy) == fs.cat(rpath)

    @pytest.mark.skipif(
        os.getenv("AWS_ATHENA_S3_VERSIONING_TESTS") != "1",
        reason="Set AWS_ATHENA_S3_VERSIONING_TESTS=1 to create versioning test buckets.",
    )
    @pytest.mark.parametrize(
        "status",
        [
            pytest.param(None, id="sync-None"),
            pytest.param("Enabled", id="sync-Enabled"),
            pytest.param("Suspended", id="sync-Suspended"),
        ],
    )
    def test_move_null_version_onto_key(self, fs, versioning_buckets, status):
        client, buckets = versioning_buckets
        bucket = buckets[status]
        key = "sync"
        path = f"s3://{bucket}/{key}"
        before = [
            v
            for v in client.list_object_versions(Bucket=bucket, Prefix=key)["Versions"]
            if v["Key"] == key
        ]
        assert any(v["VersionId"] == "null" for v in before)
        if status:
            assert not next(v for v in before if v["VersionId"] == "null")["IsLatest"]

        fs.mv(f"{path}?versionId=null", path)

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

    def test_move(self, fs):
        path1 = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_move/{uuid.uuid4()}"
        )
        path2 = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_move/{uuid.uuid4()}"
        )
        data = b"a"
        fs.pipe(path1, data)
        fs.move(path1, path2)
        assert fs.cat(path2) == data
        assert not fs.exists(path1)

    @pytest.mark.parametrize(
        ("files", "path1", "path2", "expected", "kwargs"),
        [
            # GH-974: the directory entries used to make mv() fail.
            (
                ["src/a", "src/sub/b"],
                "src",
                "dst",
                {"dst/a": "src/a", "dst/sub/b": "src/sub/b"},
                {},
            ),
            # GH-1008: mv() removed the source by expanding it again, which
            # also removed the copies made under it.
            (
                ["src/a", "src/sub/b"],
                "src",
                "src/archive",
                {"src/archive/a": "src/a", "src/archive/sub/b": "src/sub/b"},
                {},
            ),
            (
                ["src/a", "src/b"],
                "src/*",
                "src/archive/",
                {"src/archive/a": "src/a", "src/archive/b": "src/b"},
                {},
            ),
            # Files whose destination is the file itself are kept.
            (
                ["data/x.csv", "data/y.csv"],
                "data/*.csv",
                "data/",
                {"data/x.csv": "data/x.csv", "data/y.csv": "data/y.csv"},
                {},
            ),
            # Files below maxdepth are neither copied nor deleted.
            (
                ["src/a", "src/sub/b"],
                "src",
                "dst/",
                {"dst/a": "src/a", "src/sub/b": "src/sub/b"},
                {"maxdepth": 1},
            ),
        ],
    )
    def test_move_recursive(self, fs, files, path1, path2, expected, kwargs):
        base = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_move_recursive/{uuid.uuid4()}"
        )
        for f in files:
            fs.pipe(f"{base}/{f}", f.encode())
        fs.mv(f"{base}/{path1}", f"{base}/{path2}", recursive=True, **kwargs)
        fs.invalidate_cache(base)
        prefix = f"{fs._strip_protocol(base)}/"
        assert {p.removeprefix(prefix): fs.cat(p).decode() for p in fs.find(base)} == expected

    def test_get_recursive(self, fs, tmp_path):
        # GH-974: the directory entries used to be written as empty files.
        base = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_get_recursive/{uuid.uuid4()}"
        )
        fs.pipe(f"{base}/a", b"a")
        fs.pipe(f"{base}/sub/b", b"b")
        fs.get(base, str(tmp_path / "out"), recursive=True)
        assert (tmp_path / "out" / "a").read_bytes() == b"a"
        assert (tmp_path / "out" / "sub" / "b").read_bytes() == b"b"

    def test_get_file(self, fs):
        with tempfile.TemporaryDirectory() as tmp:
            rpath = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}"
            lpath = Path(f"{tmp}/{uuid.uuid4()}")
            callback = Callback()
            fs.get_file(rpath=rpath, lpath=str(lpath), callback=callback)

            assert lpath.open("rb").read() == fs.cat(rpath)
            assert callback.size == os.stat(lpath).st_size
            assert callback.value == callback.size

    def test_checksum(self, fs):
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_checksum/{uuid.uuid4()}"
        )
        bucket, key, _ = fs.parse_path(path)

        fs.pipe_file(path, b"foo")
        checksum = fs.checksum(path)
        fs.ls(path)  # caching
        fs.core.put_object(S3Path(bucket, key), b"bar")
        assert checksum == fs.checksum(path)
        assert checksum != fs.checksum(path, refresh=True)

        fs.pipe_file(path, b"foo")
        checksum = fs.checksum(path)
        fs.ls(path)  # caching
        fs.core.delete_object(S3Path(bucket, key))
        assert checksum == fs.checksum(path)
        with pytest.raises(FileNotFoundError):
            fs.checksum(path, refresh=True)

    def test_sign(self, fs):
        path = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}"
        requested = time.time()
        time.sleep(1)
        url = fs.sign(path, expiration=100)
        parsed = urllib.parse.urlparse(url)
        query = urllib.parse.parse_qs(parsed.query)
        expires = int(query["Expires"][0])
        with urllib.request.urlopen(url) as r:
            data = r.read()

        assert "https" in url
        assert requested + 100 < expires
        assert data == b"0123456789"

    def test_mkdir_and_rmdir(self, fs):
        # The bucket already exists; raised regardless of the flags.
        with pytest.raises(FileExistsError):
            fs.mkdir(f"s3://{ENV.s3_staging_bucket}")
        # exist_ok suppresses the error.
        fs.makedirs(f"s3://{ENV.s3_staging_bucket}", exist_ok=True)
        with pytest.raises(FileExistsError):
            fs.makedirs(f"s3://{ENV.s3_staging_bucket}", exist_ok=False)
        # Creating a key prefix under an existing bucket requires no operation.
        fs.mkdir(
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_mkdir/{uuid.uuid4()}"
        )

        # Bucket creation/deletion are disabled by default.
        nonexistent = f"s3://pyathena-test-{uuid.uuid4()}"
        with pytest.raises(PermissionError, match="Bucket creation is disabled"):
            fs.mkdir(nonexistent)
        with pytest.raises(PermissionError, match="Bucket deletion is disabled"):
            fs.rmdir(f"s3://{ENV.s3_staging_bucket}")
        # The bucket does not exist, and it is not requested to be created.
        with pytest.raises(FileNotFoundError):
            fs.mkdir(f"{nonexistent}/dir", create_parents=False)

        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_rmdir/{uuid.uuid4()}"
        )
        fs.pipe(path, b"data")
        # Only bucket paths can be removed.
        with pytest.raises(FileExistsError):
            fs.rmdir(path)
        with pytest.raises(FileNotFoundError):
            fs.rmdir(f"{path}/nonexistent")

    def test_metadata_getxattr_setxattr(self, fs):
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_metadata/{uuid.uuid4()}"
        )
        data = b"0123456789"
        fs.pipe(path, data, ContentType="text/csv", CacheControl="max-age=60")
        assert fs.metadata(path) == {}

        # The keys are stored as-is; hyphenated names can be passed by
        # unpacking a dictionary.
        fs.setxattr(path, attr1="value1", **{"attr-2": "value2"})
        assert fs.metadata(path) == {"attr1": "value1", "attr-2": "value2"}
        # GH-975: the system-defined metadata is kept.
        assert fs.metadata(path).content_type == "text/csv"
        assert fs.metadata(path).cache_control == "max-age=60"
        assert fs.getxattr(path, "attr1") == "value1"
        assert fs.getxattr(path, "attr-2") == "value2"
        assert fs.getxattr(path, "missing") is None

        # Setting a field to None deletes it, and the content is preserved.
        fs.setxattr(path, attr3="value3", **{"attr-2": None})
        assert fs.metadata(path) == {"attr1": "value1", "attr3": "value3"}
        assert fs.cat(path) == data

        # copy_kwargs are passed to the underlying CopyObject API. The system
        # metadata is exposed as typed properties on the returned S3Metadata,
        # and the user-defined metadata through its mapping interface.
        fs.setxattr(path, copy_kwargs={"ContentType": "text/plain"}, attr4="value4")
        metadata = fs.metadata(path)
        assert metadata.content_type == "text/plain"
        assert metadata["attr4"] == "value4"
        assert metadata.user_metadata == {"attr1": "value1", "attr3": "value3", "attr4": "value4"}
        assert metadata.content_length == len(data)
        assert metadata.etag
        assert fs.getxattr(path, "attr4") == "value4"

        with pytest.raises(FileNotFoundError):
            fs.metadata(
                f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
                f"filesystem/test_metadata/nonexistent-{uuid.uuid4()}"
            )
        with pytest.raises(ValueError, match="Cannot get metadata"):
            fs.metadata(f"s3://{ENV.s3_staging_bucket}")
        with pytest.raises(ValueError, match="Cannot set metadata"):
            fs.setxattr(f"s3://{ENV.s3_staging_bucket}", attr1="value1")

    @staticmethod
    def _stubbed_fs():
        return S3FileSystem(
            key="dummy", secret="dummy", region_name="us-east-1", skip_instance_cache=True
        )

    def test_setxattr_keeps_system_metadata(self):
        # GH-975: the REPLACE directive drops what the CopyObject request
        # omits, so the system-defined metadata, the storage class and the
        # encryption are sent from the HeadObject response.
        fs = self._stubbed_fs()
        expires = datetime(2030, 1, 1, tzinfo=UTC)
        head_object = {
            "ContentLength": 10,
            "CacheControl": "max-age=60",
            "ContentDisposition": "attachment",
            "ContentEncoding": "gzip",
            "ContentLanguage": "en",
            "ContentType": "text/csv",
            "Expires": expires,
            "WebsiteRedirectLocation": "/other",
            "StorageClass": "STANDARD_IA",
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": "arn:aws:kms:us-east-1:111122223333:key/k",
            "BucketKeyEnabled": True,
            "Metadata": {"a": "1"},
        }
        kept = {
            "CacheControl": "max-age=60",
            "ContentDisposition": "attachment",
            "ContentEncoding": "gzip",
            "ContentLanguage": "en",
            "ContentType": "text/csv",
            "Expires": expires,
            "WebsiteRedirectLocation": "/other",
            "StorageClass": "STANDARD_IA",
        }
        request = {
            "CopySource": {"Bucket": "bucket", "Key": "key.csv"},
            "Bucket": "bucket",
            "Key": "key.csv",
            "Metadata": {"a": "1", "b": "2"},
            "MetadataDirective": "REPLACE",
        }
        with Stubber(fs._client) as stubber:
            stubber.add_response("head_object", head_object, {"Bucket": "bucket", "Key": "key.csv"})
            stubber.add_response(
                "copy_object",
                {},
                {
                    **request,
                    **kept,
                    "ServerSideEncryption": "aws:kms",
                    "SSEKMSKeyId": "arn:aws:kms:us-east-1:111122223333:key/k",
                    "BucketKeyEnabled": True,
                },
            )
            fs.setxattr("s3://bucket/key.csv", b="2")

            # copy_kwargs take precedence, and an encryption parameter
            # replaces all kept encryption settings.
            stubber.add_response("head_object", head_object, {"Bucket": "bucket", "Key": "key.csv"})
            stubber.add_response(
                "copy_object",
                {},
                {
                    **request,
                    **kept,
                    "ContentType": "text/plain",
                    "ServerSideEncryption": "AES256",
                },
            )
            fs.setxattr(
                "s3://bucket/key.csv",
                copy_kwargs={"ContentType": "text/plain", "ServerSideEncryption": "AES256"},
                b="2",
            )
            stubber.assert_no_pending_responses()

    def test_setxattr_omits_unset_system_metadata(self):
        # S3 omits StorageClass for STANDARD objects; unset headers are not sent.
        fs = self._stubbed_fs()
        with Stubber(fs._client) as stubber:
            stubber.add_response(
                "head_object", {"ContentLength": 1}, {"Bucket": "bucket", "Key": "key"}
            )
            stubber.add_response(
                "copy_object",
                {},
                {
                    "CopySource": {"Bucket": "bucket", "Key": "key"},
                    "Bucket": "bucket",
                    "Key": "key",
                    "Metadata": {"a": "1"},
                    "MetadataDirective": "REPLACE",
                    "StorageClass": "STANDARD",
                },
            )
            fs.setxattr("s3://bucket/key", a="1")
            stubber.assert_no_pending_responses()

    @pytest.mark.parametrize(
        "path",
        ["s3://bucket/key?versionId=OLD", "s3://bucket/key?version_id=OLD"],
    )
    def test_setxattr_version_path(self, path):
        # GH-975: copying a version onto the key would replace the current
        # object with it, so a version path is rejected without requests.
        fs = self._stubbed_fs()
        with (
            Stubber(fs._client),
            pytest.raises(ValueError, match="Cannot set metadata of a version"),
        ):
            fs.setxattr(path, a="1")

    def test_get_and_put_tags(self, fs):
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_tags/{uuid.uuid4()}"
        )
        fs.pipe(path, b"data")
        assert fs.get_tags(path) == {}

        fs.put_tags(path, {"tag1": "value1"})
        assert fs.get_tags(path) == {"tag1": "value1"}

        # Merge mode keeps the existing tags.
        fs.put_tags(path, {"tag2": "value2"}, mode="m")
        assert fs.get_tags(path) == {"tag1": "value1", "tag2": "value2"}

        # Overwrite mode replaces the existing tags.
        fs.put_tags(path, {"tag3": "value3"}, mode="o")
        assert fs.get_tags(path) == {"tag3": "value3"}

        with pytest.raises(ValueError, match="Mode must be"):
            fs.put_tags(path, {"tag4": "value4"}, mode="x")

    def test_chmod_validation(self, fs):
        # Canned ACLs are validated before any API call; applying ACLs is
        # not integration-tested because the test bucket has ACLs disabled.
        with pytest.raises(ValueError, match="ACL not in"):
            fs.chmod(f"s3://{ENV.s3_staging_bucket}/key", "invalid-acl")
        with pytest.raises(ValueError, match="ACL not in"):
            # A valid object ACL that is not valid for buckets.
            fs.chmod(f"s3://{ENV.s3_staging_bucket}", "bucket-owner-full-control")
        with pytest.raises(ValueError, match="ACL not in"):
            # A recursive call must validate before applying any ACL.
            fs.chmod(f"s3://{ENV.s3_staging_bucket}", "bucket-owner-full-control", recursive=True)

    def test_error_translation_permission_error(self):
        # Unsigned access to a private object: 403 -> PermissionError.
        anon_fs = S3FileSystem(anon=True, skip_instance_cache=True)
        with pytest.raises(PermissionError):
            anon_fs.info(f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}")

    @pytest.mark.parametrize(
        ("algorithm", "checksum_type"),
        [(None, None), ("SHA256", "COMPOSITE"), ("CRC32", "FULL_OBJECT")],
    )
    def test_list_and_clear_multipart_uploads(self, fs, algorithm, checksum_type):
        # Scope the list/clear to a unique prefix so that parallel test
        # workers' in-flight multipart uploads in the shared bucket are
        # not aborted.
        bucket = ENV.s3_staging_bucket
        prefix = (
            f"{ENV.s3_staging_key}{ENV.schema}/filesystem/test_multipart_uploads/{uuid.uuid4()}"
        )
        prefix_path = f"s3://{bucket}/{prefix}"
        key = f"{prefix}/file"
        kwargs = (
            {"ChecksumAlgorithm": algorithm, "ChecksumType": checksum_type} if algorithm else {}
        )
        upload = fs.core.create_multipart_upload(S3Path(bucket, key), **kwargs)
        # A sibling key that starts with the same characters as the prefix.
        sibling = fs.core.create_multipart_upload(S3Path(bucket, f"{prefix}2/file"))
        try:
            uploads = fs.list_multipart_uploads(prefix_path)
            listed = next((u for u in uploads if u.upload_id == upload.upload_id), None)
            assert listed
            assert listed.bucket == bucket
            assert listed.key == key
            assert listed.initiated
            assert listed.checksum_algorithm == upload.checksum_algorithm
            assert listed.checksum_type == upload.checksum_type
            assert not any(u.upload_id == sibling.upload_id for u in uploads)

            fs.clear_multipart_uploads(prefix_path)
            uploads = fs.list_multipart_uploads(prefix_path)
            assert not any(u.upload_id == upload.upload_id for u in uploads)
            uploads = fs.list_multipart_uploads(f"{prefix_path}2")
            assert any(u.upload_id == sibling.upload_id for u in uploads)
        finally:
            fs.clear_multipart_uploads(prefix_path)
            fs.clear_multipart_uploads(f"{prefix_path}2")

    def test_object_version_info(self, fs):
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_object_version_info/{uuid.uuid4()}"
        )
        fs.pipe(path, b"data")
        # A sibling key that starts with the same characters as the path.
        fs.pipe(f"{path}.bak", b"backup")

        versions = fs.object_version_info(path)
        assert len(versions) == 1
        version = versions[0]
        bucket, key, _ = fs.parse_path(path)
        assert version.bucket == bucket
        assert version.key == key
        assert version.name == f"{bucket}/{key}"
        assert version.is_latest
        assert not version.is_delete_marker
        assert version.size == 4
        # An unversioned bucket reports the "null" version.
        assert version.version_id

    def test_read_version_id(self, fs):
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_read_version_id/{uuid.uuid4()}"
        )
        data = b"0123456789"
        fs.pipe(path, data)
        # An unversioned bucket reports the "null" version, which can be
        # read explicitly.
        version_id = fs.object_version_info(path)[0].version_id

        assert fs.cat_file(path, version_id=version_id) == data
        assert fs.cat_file(path, start=2, end=5, version_id=version_id) == data[2:5]
        with fs.open(path, "rb", version_id=version_id) as f:
            assert f.read() == data
        # The version reaches S3, which rejects an unknown one.
        with pytest.raises(OSError, match="Invalid version id"):
            fs.cat_file(path, version_id="invalid")

    @pytest.mark.parametrize("fs", [{"version_aware": True}], indirect=True)
    def test_version_aware_read(self, fs):
        # On an unversioned bucket, the version-aware mode is a no-op for
        # reads: HeadObject returns no version to pin.
        path = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}"
        with fs.open(path, "rb") as f:
            assert f.read() == b"0123456789"

    def test_read_null_version(self, fs):
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_read_null_version/{uuid.uuid4()}"
        )
        # An unversioned bucket stores each object as the "null" version,
        # which an overwrite replaces.
        for data in (b"1", b"22"):
            fs.pipe(path, data)
            for _ in range(2):
                with fs.open(f"{path}?versionId=null", "rb") as f:
                    assert f.read() == data

    def test_question_mark_keys_and_null_version(self, fs, tmp_path):
        # GH-979: keys containing "?" can be written, listed, read and
        # deleted, and a version path, here the "null" version of an
        # unversioned bucket, is copied and downloaded under its key.
        base = (
            f"{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_question_mark_keys/{uuid.uuid4()}"
        )
        fs.pipe(f"s3://{base}/what?.txt", b"1")
        assert fs.ls(f"s3://{base}") == [f"{base}/what?.txt"]
        assert fs.info(f"s3://{base}/what?.txt")["size"] == 1
        assert fs.cat(f"s3://{base}/what?.txt") == b"1"

        fs.copy(f"s3://{base}/what?.txt?versionId=null", f"s3://{base}/copy/")
        assert fs.cat(f"s3://{base}/copy/what?.txt") == b"1"
        fs.get(f"s3://{base}/what?.txt?versionId=null", f"{tmp_path}/")
        assert (tmp_path / "what?.txt").read_bytes() == b"1"

        fs.rm(f"s3://{base}", recursive=True)
        assert not fs.exists(f"s3://{base}/what?.txt")
        assert not fs.exists(f"s3://{base}/copy/what?.txt")

    def test_file_url_metadata_getxattr_setxattr(self, fs):
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_file_helpers/{uuid.uuid4()}"
        )
        data = b"0123456789"
        fs.pipe(path, data)
        fs.setxattr(path, attr1="value1")

        with fs.open(path, "rb") as f:
            assert f.metadata() == {"attr1": "value1"}
            assert f.getxattr("attr1") == "value1"
            url = f.url(expiration=100)
            with urllib.request.urlopen(url) as r:
                assert r.read() == data

        with fs.open(path, "wb") as f:
            with pytest.raises(NotImplementedError):
                f.setxattr(attr2="value2")
            f.write(data)

    def test_pandas_read_csv(self):
        import pandas

        df = pandas.read_csv(
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}",
            header=None,
            names=["col"],
        )
        assert [(row["col"],) for _, row in df.iterrows()] == [(123456789,)]

    @pytest.mark.parametrize(
        "line_count",
        [
            # TODO: Comment out some test cases because of the high cost of AWS for testing.
            1 * 2**20,  # Generates files of about 2 MB.
            # (2 * (2**20),),  # 4MB
            # (3 * (2**20),),  # 6MB
            # (4 * (2**20),),  # 8MB
            # (5 * (2**20),),  # 10MB
            # (6 * (2**20),),  # 12MB
        ],
    )
    def test_pandas_write_csv(self, line_count):
        import pandas

        with tempfile.NamedTemporaryFile("w+t") as tmp:
            tmp.write("col1")
            tmp.write("\n")
            for _ in range(line_count):
                tmp.write("a")
                tmp.write("\n")
            tmp.flush()

            tmp.seek(0)
            df = pandas.read_csv(tmp.name)
            path = (
                f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
                f"filesystem/test_pandas_write_csv/{uuid.uuid4()}.csv"
            )
            df.to_csv(path, index=False)

            actual = pandas.read_csv(path)
            pandas.testing.assert_frame_equal(actual, df)


class TestS3File:
    @pytest.mark.parametrize("operation", ["write", "append", "exclusive", "pipe", "put"])
    @pytest.mark.parametrize("abort_fails", [False, True])
    @pytest.mark.skipif(
        threading.current_thread() is not threading.main_thread()
        or not hasattr(signal, "pthread_kill"),
        reason="Requires SIGINT delivery to the main thread.",
    )
    def test_interrupted_creation(self, tmp_path, operation, abort_fails):
        # GH-1077: creation finishes after SIGINT interrupts the writer's wait.
        # Its ID is recovered and aborted before the interrupt is re-raised.
        fs = self._make_append_fs(b"a" * 6 if operation == "append" else b"")
        fs.exists.return_value = False
        fs._intrans = False
        fs.default_block_size = 4
        fs.max_workers = 1
        fs.s3_additional_kwargs = {}
        fs._strip_protocol.side_effect = S3FileSystem._strip_protocol
        started = threading.Event()
        waiting = threading.Event()
        interrupted = threading.Event()
        upload = fs.core.create_multipart_upload.return_value

        def create(*args, **kwargs):
            started.set()
            if threading.current_thread() is threading.main_thread():
                # The old implementation sent creation on the main thread.
                waiting.set()
            assert interrupted.wait(5)
            return upload

        fs.core.create_multipart_upload.side_effect = create
        if abort_fails:
            fs._call.side_effect = PermissionError("abort failed")
        executor = S3ThreadPoolExecutor(max_workers=1)
        submit = executor.submit

        def submit_creation(fn, *args, **kwargs):
            future = submit(fn, *args, **kwargs)
            result = future.result

            def wait_for_result(timeout=None):
                waiting.set()
                return result(timeout)

            future.result = wait_for_result  # type: ignore[method-assign]
            return future

        executor.submit = submit_creation  # type: ignore[method-assign]
        mode = {"append": "ab", "exclusive": "xb"}.get(operation, "wb")
        file = S3File(
            fs,
            "s3://bucket/key.txt",
            mode=mode,
            block_size=4,
            autocommit=False,
            executor=executor,
        )
        fs.open.return_value = file
        if operation == "pipe":
            perform = functools.partial(
                S3FileSystem.pipe_file, fs, file.path, b"x" * 8, block_size=4
            )
        elif operation == "put":
            local = tmp_path / "input"
            local.write_bytes(b"x" * 8)
            perform = functools.partial(
                S3FileSystem.put_file, fs, str(local), file.path, block_size=4
            )
        else:
            perform = functools.partial(file.write, b"x" * 8)

        def handle_interrupt(signum, frame):
            interrupted.set()
            raise KeyboardInterrupt

        def interrupt():
            if started.wait(5) and waiting.wait(5):
                signal.pthread_kill(threading.main_thread().ident, signal.SIGINT)

        thread = threading.Thread(target=interrupt, daemon=True)
        previous_handler = signal.signal(signal.SIGINT, handle_interrupt)
        try:
            thread.start()
            with pytest.raises(KeyboardInterrupt):
                perform()
            fs._call.assert_called_once_with(
                S3_CLIENT.abort_multipart_upload,
                Bucket="bucket",
                Key="key.txt",
                UploadId="uploadid",
            )
            assert file.closed
            assert file.buffer is None
            assert (file.multipart_upload is upload) is abort_fails
            file.close()
            file.commit()
            fs.core.upload_part.assert_not_called()
            fs.core.upload_part_copy.assert_not_called()
            fs.core.complete_multipart_upload.assert_not_called()
            fs.core.put_object.assert_not_called()
            fs.touch.assert_not_called()
            if abort_fails:
                fs._call.side_effect = None
                file.discard()
                assert fs._call.call_count == 2
                assert file.multipart_upload is None
        finally:
            signal.signal(signal.SIGINT, lambda signum, frame: None)
            started.set()
            waiting.set()
            interrupted.set()
            if thread.ident is not None:
                thread.join(5)
            signal.signal(signal.SIGINT, previous_handler)
            fs._call.side_effect = None
            file._close_without_commit()

    @pytest.mark.parametrize("autocommit", [False, True])
    @pytest.mark.parametrize("abort_fails", [False, True])
    def test_repeated_creation_interrupt(self, autocommit, abort_fails):
        fs = self._make_append_fs(b"")
        started = threading.Event()
        release = threading.Event()
        upload = fs.core.create_multipart_upload.return_value

        def create(*args, **kwargs):
            started.set()
            assert release.wait(5)
            return upload

        fs.core.create_multipart_upload.side_effect = create
        if abort_fails:
            fs._call.side_effect = PermissionError("abort failed")
        executor = S3ThreadPoolExecutor(max_workers=1)
        submit = executor.submit

        def submit_creation(fn, *args, **kwargs):
            future = submit(fn, *args, **kwargs)

            def interrupt_result(timeout=None):
                assert started.wait(5)
                raise KeyboardInterrupt("first interrupt")

            future.result = interrupt_result  # type: ignore[method-assign]
            return future

        executor.submit = submit_creation  # type: ignore[method-assign]
        file = S3File(
            fs,
            "s3://bucket/key.txt",
            mode="wb",
            block_size=4,
            autocommit=autocommit,
            executor=executor,
        )
        closed_at_recovery = []

        def interrupt_recovery(futures):
            if not release.is_set():
                closed_at_recovery.append(file.closed and file.buffer is None)
                release.set()
                raise KeyboardInterrupt("second interrupt")
            return wait(futures)

        try:
            with (
                mock.patch("pyathena.filesystem.s3.wait", side_effect=interrupt_recovery),
                pytest.raises(KeyboardInterrupt, match="first interrupt"),
            ):
                file.write(b"x" * 8)

            assert closed_at_recovery == [True]
            assert file.closed
            assert file.buffer is None
            fs._call.assert_called_once_with(
                S3_CLIENT.abort_multipart_upload,
                Bucket="bucket",
                Key="key.txt",
                UploadId="uploadid",
            )
            assert (file.multipart_upload is upload) is abort_fails
            file.close()
            file.commit()
            fs.core.upload_part.assert_not_called()
            fs.core.complete_multipart_upload.assert_not_called()
            fs.core.put_object.assert_not_called()
            fs.touch.assert_not_called()
            if abort_fails:
                fs._call.side_effect = None
                file.discard()
                assert fs._call.call_count == 2
                assert file.multipart_upload is None
        finally:
            release.set()
            executor.shutdown()
            fs._call.side_effect = None
            file._close_without_commit()

    def test_creation_cancelled_before_start(self):
        fs = self._make_append_fs(b"")
        creation = Future()
        creation.result = mock.MagicMock(side_effect=KeyboardInterrupt)  # type: ignore[method-assign]
        executor = mock.MagicMock()
        executor.submit.return_value = creation
        file = S3File(fs, "s3://bucket/key.txt", mode="wb", block_size=4, executor=executor)

        with pytest.raises(KeyboardInterrupt):
            file.write(b"x" * 8)

        assert creation.cancelled()
        assert file.closed
        assert file.buffer is None
        assert file.multipart_upload is None
        executor.shutdown.assert_called_once()
        fs.core.create_multipart_upload.assert_not_called()
        fs._call.assert_not_called()
        file.commit()
        fs.core.put_object.assert_not_called()

    def test_creation_failure(self):
        fs = self._make_append_fs(b"")
        fs.core.create_multipart_upload.side_effect = PermissionError("create failed")
        file = S3File(fs, "s3://bucket/key.txt", mode="wb", block_size=4)

        with pytest.raises(PermissionError, match="create failed"):
            file.write(b"x" * 8)

        assert file.closed
        assert file.buffer is None
        assert file.multipart_upload is None
        fs._call.assert_not_called()
        file.commit()
        fs.core.put_object.assert_not_called()

    async def test_cancelled_async_write_finishes(self):
        # Cancelling to_thread does not interrupt the buffered writer's thread.
        fs = self._make_append_fs(b"")
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        upload = fs.core.create_multipart_upload.return_value

        def create(*args, **kwargs):
            started.set()
            assert release.wait(5)
            return upload

        fs.core.create_multipart_upload.side_effect = create
        file = S3File(
            fs,
            "s3://bucket/key.txt",
            mode="wb",
            block_size=4,
            executor=S3AioExecutor(asyncio.get_running_loop(), max_workers=1),
        )

        def write():
            try:
                file._write_and_close(b"x" * 8)
            finally:
                finished.set()

        task = asyncio.create_task(asyncio.to_thread(write))
        try:
            assert await asyncio.to_thread(started.wait, 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()
            assert await asyncio.to_thread(finished.wait, 5)
        fs.core.complete_multipart_upload.assert_called_once()
        fs._call.assert_not_called()
        assert file.closed

    @staticmethod
    def _make_mock_fs():
        # A mocked filesystem that selects the request parameters of each
        # operation as the real one does.
        fs = mock.MagicMock(spec=S3FileSystem)
        fs._client = S3_CLIENT
        fs.core = S3Core(S3_CLIENT)
        # The requests of the core go to the mocked _call, and the
        # uploads whose results the tests build are mocked.
        fs.core.call = fs._call
        for name in (
            "put_object",
            "create_multipart_upload",
            "upload_part",
            "upload_part_copy",
            "complete_multipart_upload",
        ):
            setattr(fs.core, name, mock.MagicMock())
        fs._get_lookup_kwargs.side_effect = S3FileSystem._get_lookup_kwargs
        return fs

    @staticmethod
    def _make_write_file(data: bytes, autocommit: bool):
        # Build a minimal write-mode S3File without touching AWS, bypassing
        # __init__ which would require a real connection.
        file = S3File.__new__(S3File)
        file.fs = TestS3File._make_mock_fs()
        file.path = "s3://bucket/key.txt"
        file.bucket = "bucket"
        file.key = "key.txt"
        file.s3_additional_kwargs = {}
        file.autocommit = autocommit
        file.blocksize = S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE
        file.fs.core.MULTIPART_UPLOAD_MAX_PARTS = S3Core.MULTIPART_UPLOAD_MAX_PARTS
        file.append_block = False
        file._multipart_writer = None
        file.multipart_upload = None
        file.multipart_upload_parts = []
        file.buffer = io.BytesIO(data)
        file.loc = len(data)  # tell() returns self.loc
        file._executor = mock.MagicMock()
        return file

    @staticmethod
    def _make_multipart_write_file(data: bytes, autocommit: bool):
        # A write-mode S3File set up to take the multipart branch (blocksize <
        # len(data)), with the executor and S3 part calls mocked so no AWS
        # access is needed.
        file = TestS3File._make_write_file(data, autocommit=autocommit)
        file.blocksize = 4
        file.fs.core.MULTIPART_UPLOAD_MIN_PART_SIZE = 4
        file.fs.core.MULTIPART_UPLOAD_MAX_PART_SIZE = 8
        file.multipart_upload = S3MultipartUpload(
            {"Bucket": "bucket", "Key": "key.txt", "UploadId": "uploadid"}
        )
        file._executor = ThreadPoolExecutor(max_workers=1)
        file.fs.core.upload_part.side_effect = lambda **kw: SimpleNamespace(
            etag=f'"e{kw["part_number"]}"', part_number=kw["part_number"]
        )
        return file

    @staticmethod
    def _make_append_fs(existing: bytes):
        # A mocked filesystem holding an existing object, with a minimum part
        # size of 4 bytes so that the write and append paths can be exercised
        # with tiny data and no AWS access.
        fs = TestS3File._make_mock_fs()
        fs.core.MULTIPART_UPLOAD_MIN_PART_SIZE = 4
        fs.core.MULTIPART_UPLOAD_MAX_PART_SIZE = 64
        fs.core.MULTIPART_UPLOAD_MAX_PARTS = S3Core.MULTIPART_UPLOAD_MAX_PARTS
        fs.info.return_value = S3Object(
            init={"ContentLength": len(existing)},
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="bucket",
            key="key.txt",
        )
        fs.cat_file.return_value = existing
        fs.core.create_multipart_upload.return_value = S3MultipartUpload(
            {"Bucket": "bucket", "Key": "key.txt", "UploadId": "uploadid"}
        )

        def part(**kw):
            return SimpleNamespace(etag=f'"e{kw["part_number"]}"', part_number=kw["part_number"])

        fs.core.upload_part.side_effect = part
        fs.core.upload_part_copy.side_effect = part
        return fs

    @staticmethod
    def _uploaded_object(fs, existing: bytes) -> bytes:
        # Rebuild the object S3 would store from the mocked upload calls.
        # A part copy without a range copies the whole existing object.
        if fs.core.put_object.called:
            fs.core.create_multipart_upload.assert_not_called()
            return fs.core.put_object.call_args.args[1]
        fs.core.complete_multipart_upload.assert_called_once()
        parts = []
        for c in fs.core.upload_part_copy.call_args_list:
            start, end = c.kwargs.get("range_") or (0, len(existing))
            parts.append((c.kwargs["part_number"], existing[start:end]))
        parts += [
            (c.kwargs["part_number"], c.kwargs["body"]) for c in fs.core.upload_part.call_args_list
        ]
        part_numbers = sorted(n for n, _ in parts)
        assert part_numbers == list(range(1, len(parts) + 1))
        return b"".join(body for _, body in sorted(parts))

    @pytest.mark.parametrize(
        ("existing", "appended", "multipart", "part_copy"),
        [
            # Smaller than the minimum part size: read into the buffer.
            (b"aa", b"bb", False, False),
            # GH-921: an existing object of at least the minimum part size is
            # copied with UploadPartCopy even when the block size is larger
            # than the whole object.
            (b"a" * 6, b"bb", True, True),
            (b"a" * 6, b"", True, True),
            # An existing object read into the buffer is not copied again
            # when the append crosses the block size.
            (b"aa", b"b" * 16, True, False),
        ],
    )
    def test_append(self, existing, appended, multipart, part_copy):
        fs = self._make_append_fs(existing)

        with S3File(fs, "s3://bucket/key.txt", mode="ab", block_size=16) as f:
            f.write(appended)

        assert self._uploaded_object(fs, existing) == existing + appended
        assert fs.core.create_multipart_upload.called is multipart
        assert fs.core.upload_part_copy.called is part_copy
        fs.touch.assert_not_called()

    @pytest.mark.parametrize("max_workers", [1, 4])
    def test_append_part_copy_ranges(self, max_workers):
        # GH-951: an existing object larger than the maximum part size is
        # copied in parts within the part size limits whatever the number of
        # workers. A short remainder used to be copied as its own part,
        # which is not the last one when data is appended.
        existing = b"a" * 129
        fs = self._make_append_fs(existing)

        with S3File(
            fs, "s3://bucket/key.txt", mode="ab", block_size=16, max_workers=max_workers
        ) as f:
            f.write(b"b")

        assert self._uploaded_object(fs, existing) == existing + b"b"
        ranges = sorted(
            (c.kwargs["part_number"], c.kwargs["range_"])
            for c in fs.core.upload_part_copy.call_args_list
        )
        assert ranges == [(1, (0, 64)), (2, (64, 96)), (3, (96, 129))]

    @pytest.mark.parametrize(
        ("writes", "block_size"),
        [
            # GH-942: a single write() that leaves more than two blocks with a
            # short tail in the buffer, followed by more data.
            ([b"a" * 11, b"b"], 4),
            ([b"a" * 9, b"b"], 4),
            ([b"a" * 11], 4),
            ([b"a" * 12, b"b" * 3], 4),
            ([b"a" * 3, b"b" * 10, b"c" * 2, b"d"], 4),
            # A merged tail that reaches the maximum part size is split in half.
            ([b"a" * 127, b"b"], 62),
        ],
    )
    def test_write_part_sizes(self, writes, block_size):
        fs = self._make_append_fs(b"")

        with S3File(fs, "s3://bucket/key.txt", mode="wb", block_size=block_size) as f:
            for data in writes:
                f.write(data)

        assert self._uploaded_object(fs, b"") == b"".join(writes)
        parts = sorted(
            (c.kwargs["part_number"], len(c.kwargs["body"]))
            for c in fs.core.upload_part.call_args_list
        )
        sizes = [size for _, size in parts]
        assert all(size >= fs.core.MULTIPART_UPLOAD_MIN_PART_SIZE for size in sizes[:-1])
        assert all(size <= fs.core.MULTIPART_UPLOAD_MAX_PART_SIZE for size in sizes)

    @staticmethod
    def _write_and_close(f, writes: list[bytes]) -> None:
        with f:
            for data in writes:
                f.write(data)

    @pytest.mark.parametrize(
        ("existing", "mode", "writes"),
        [
            # The data fills the maximum number of parts.
            (b"", "wb", [b"a" * 4] * 3),
            (b"", "wb", [b"a" * 14]),
            # The parts copied from the existing object in an append count
            # toward the maximum.
            (b"a" * 6, "ab", [b"b" * 4] * 2),
        ],
    )
    @pytest.mark.parametrize("autocommit", [True, False])
    def test_write_max_parts(self, existing, mode, writes, autocommit):
        fs = self._make_append_fs(existing)
        fs.core.MULTIPART_UPLOAD_MAX_PARTS = 3

        f = S3File(fs, "s3://bucket/key.txt", mode=mode, block_size=4, autocommit=autocommit)
        self._write_and_close(f, writes)
        if not autocommit:
            f.commit()

        assert self._uploaded_object(fs, existing) == existing + b"".join(writes)
        assert fs.core.upload_part_copy.call_count + fs.core.upload_part.call_count == 3

    @pytest.mark.parametrize(
        ("existing", "mode", "writes"),
        [
            # GH-953: the part after the maximum is not uploaded, whether it is
            # flushed by a write()
            (b"", "wb", [b"a" * 4] * 4),
            # or by close(),
            (b"", "wb", [b"a" * 12, b"b" * 3]),
            # including after the parts copied in an append.
            (b"a" * 6, "ab", [b"b" * 4] * 3),
        ],
    )
    @pytest.mark.parametrize("autocommit", [True, False])
    def test_write_exceeding_max_parts(self, existing, mode, writes, autocommit):
        fs = self._make_append_fs(existing)
        fs.core.MULTIPART_UPLOAD_MAX_PARTS = 3

        executor = mock.MagicMock(wraps=S3ThreadPoolExecutor(max_workers=1))
        f = S3File(
            fs,
            "s3://bucket/key.txt",
            mode=mode,
            block_size=4,
            autocommit=autocommit,
            executor=executor,
        )
        with pytest.raises(ValueError, match="block_size"):
            self._write_and_close(f, writes)
        # The upload is aborted, and committing a deferred write afterwards
        # uploads nothing.
        if not autocommit:
            f.commit()

        assert f.closed
        # The submitted parts, some of which the abort may have cancelled.
        assert [
            c.kwargs["part_number"]
            for c in executor.submit.call_args_list
            if "part_number" in c.kwargs
        ] == [1, 2, 3]
        executor.shutdown.assert_called()
        fs._call.assert_called_once_with(
            S3_CLIENT.abort_multipart_upload, Bucket="bucket", Key="key.txt", UploadId="uploadid"
        )
        fs.core.complete_multipart_upload.assert_not_called()
        fs.core.put_object.assert_not_called()

    @pytest.mark.parametrize("autocommit", [True, False])
    def test_write_exceeding_max_parts_abort_failure(self, caplog, autocommit):
        # An abort failure is logged; the part limit error propagates, and
        # neither closing the file nor committing a deferred write retries
        # the upload or completes it. GH-945: the upload is kept, so that
        # discard() retries the abort.
        fs = self._make_append_fs(b"")
        fs.core.MULTIPART_UPLOAD_MAX_PARTS = 3
        fs._call.side_effect = PermissionError("abort failed")

        executor = mock.MagicMock(wraps=S3ThreadPoolExecutor(max_workers=1))
        f = S3File(
            fs,
            "s3://bucket/key.txt",
            mode="wb",
            block_size=4,
            autocommit=autocommit,
            executor=executor,
        )
        with pytest.raises(ValueError, match="block_size"):
            self._write_and_close(f, [b"a" * 4] * 4)
        if not autocommit:
            f.commit()

        assert f.closed
        assert [
            c.kwargs["part_number"]
            for c in executor.submit.call_args_list
            if "part_number" in c.kwargs
        ] == [1, 2, 3]
        executor.shutdown.assert_called()
        fs._call.assert_called_once()
        fs.core.complete_multipart_upload.assert_not_called()
        fs.core.put_object.assert_not_called()
        assert "Failed to abort multipart upload uploadid to s3://bucket/key.txt." in caplog.text

        assert f.multipart_upload is not None
        fs._call.side_effect = None
        f.discard()
        assert fs._call.call_count == 2
        assert fs._call.call_args_list[1] == fs._call.call_args_list[0]
        assert f.multipart_upload is None
        assert f.multipart_upload_parts == []

    def test_write_exceeding_max_parts_abort_interrupted(self):
        # GH-997: an interrupted abort propagates, and a deferred commit
        # still does not complete the upload; the executor is shut down.
        # GH-945: the upload is kept, so that discard() retries the abort.
        fs = self._make_append_fs(b"")
        fs.core.MULTIPART_UPLOAD_MAX_PARTS = 3
        fs._call.side_effect = KeyboardInterrupt

        executor = mock.MagicMock(wraps=S3ThreadPoolExecutor(max_workers=1))
        f = S3File(
            fs,
            "s3://bucket/key.txt",
            mode="wb",
            block_size=4,
            autocommit=False,
            executor=executor,
        )
        with pytest.raises(KeyboardInterrupt):
            self._write_and_close(f, [b"a" * 4] * 4)
        f.commit()

        assert f.closed
        executor.shutdown.assert_called()
        fs._call.assert_called_once()
        fs.core.complete_multipart_upload.assert_not_called()
        fs.core.put_object.assert_not_called()

        assert f.multipart_upload is not None
        fs._call.side_effect = None
        f.discard()
        assert fs._call.call_count == 2
        assert fs._call.call_args_list[1] == fs._call.call_args_list[0]
        assert f.multipart_upload is None
        assert f.multipart_upload_parts == []

    def test_write_exceeding_max_parts_without_close(self):
        # The executor of the closed file is shut down, as fsspec does not
        # close it again when it is garbage collected.
        fs = self._make_append_fs(b"")
        fs.core.MULTIPART_UPLOAD_MAX_PARTS = 3
        executor = mock.MagicMock(wraps=S3ThreadPoolExecutor(max_workers=1))
        f = S3File(fs, "s3://bucket/key.txt", mode="wb", block_size=4, executor=executor)

        for _ in range(3):
            f.write(b"a" * 4)
        with pytest.raises(ValueError, match="block_size"):
            f.write(b"a" * 4)

        assert f.closed
        executor.shutdown.assert_called_once()

    def test_multipart_write_request_parameters(self):
        # GH-946: the parts receive the parameters of the file that they
        # accept, such as RequestPayer and SSE-C, as does the completion.
        fs = self._make_append_fs(b"")
        kwargs = {
            "ContentType": "text/csv",
            "RequestPayer": "requester",
            "SSECustomerAlgorithm": "AES256",
            "SSECustomerKey": "key",
        }

        with S3File(
            fs, "s3://bucket/key.txt", mode="wb", block_size=4, s3_additional_kwargs=kwargs
        ) as f:
            f.write(b"x" * 8)

        fs.core.create_multipart_upload.assert_called_once_with(
            S3Path("bucket", "key.txt"), **kwargs
        )
        assert fs.core.upload_part.call_count == 2
        for c in fs.core.upload_part.call_args_list:
            assert {k: v for k, v in c.kwargs.items() if k[0].isupper()} == {
                "RequestPayer": "requester",
                "SSECustomerAlgorithm": "AES256",
                "SSECustomerKey": "key",
            }
        assert fs.core.complete_multipart_upload.call_args.kwargs == fs.core.operation_params(
            "complete_multipart_upload", kwargs
        )

    @pytest.mark.parametrize(
        "parameter", ["key", "upload", "parts", "body", "part_number", "source", "range_", "fn"]
    )
    def test_multipart_write_keyword_named_as_argument(self, parameter):
        # A keyword parameter of the file named like a helper argument
        # is ignored when filtering parameters before calling writer methods.
        fs = self._make_append_fs(b"")

        with S3File(
            fs, "s3://bucket/key.txt", mode="wb", block_size=4, **{parameter: "other"}
        ) as f:
            f.write(b"x" * 8)

        fs.core.create_multipart_upload.assert_called_once_with(S3Path("bucket", "key.txt"))
        assert (
            fs.core.complete_multipart_upload.call_args.args[0]
            is fs.core.create_multipart_upload.return_value
        )
        assert fs.core.complete_multipart_upload.call_args.kwargs == {}

    def test_append_discard(self):
        # Rolling back an append aborts its multipart upload without the
        # existing object's metadata, which AbortMultipartUpload rejects,
        # but with the request parameters it accepts.
        fs = self._make_append_fs(b"a" * 6)
        f = S3File(
            fs,
            "s3://bucket/key.txt",
            mode="ab",
            block_size=16,
            autocommit=False,
            s3_additional_kwargs={"RequestPayer": "requester", "ExpectedBucketOwner": "123"},
        )
        f.write(b"bb")
        f.close()

        f.discard()

        fs._call.assert_called_once_with(
            S3_CLIENT.abort_multipart_upload,
            Bucket="bucket",
            Key="key.txt",
            UploadId="uploadid",
            RequestPayer="requester",
            ExpectedBucketOwner="123",
        )
        fs.core.complete_multipart_upload.assert_not_called()
        fs.core.put_object.assert_not_called()

    @pytest.mark.parametrize(
        ("start", "end", "max_workers", "worker_block_size", "ranges"),
        [
            (42, 1337, 1, 999, [(42, 1337)]),  # single worker
            (42, 1337, 2, 999, [(42, 42 + 999), (42 + 999, 1337)]),  # more workers
            (
                42,
                1337,
                2,
                333,
                [
                    (42, 42 + 333),
                    (42 + 333, 42 + 666),
                    (42 + 666, 42 + 999),
                    (42 + 999, 1337),
                ],
            ),
            (42, 1337, 2, 1295, [(42, 1337)]),  # single block
            (42, 1337, 2, 1296, [(42, 1337)]),  # single block
            (42, 1337, 2, 1294, [(42, 1336), (1336, 1337)]),  # single block too small
            # The size is an exact multiple of the block size:
            # no empty trailing range is generated.
            (0, 10, 2, 5, [(0, 5), (5, 10)]),
            (42, 2632, 2, 1295, [(42, 1337), (1337, 2632)]),
        ],
    )
    def test_get_ranges(self, start, end, max_workers, worker_block_size, ranges):
        assert (
            S3File._get_ranges(
                start, end, max_workers=max_workers, worker_block_size=worker_block_size
            )
            == ranges
        )

    @pytest.mark.parametrize("autocommit", [True, False])
    def test_upload_chunk_small_file(self, autocommit):
        # Single (one-shot PutObject) upload via _upload_chunk + commit.
        # autocommit=True  -> normal open(), committed immediately.
        # autocommit=False -> inside a transaction, commit() is deferred to
        #                     Transaction.complete(). This is the GH-719 case:
        #                     _upload_chunk must return False so fsspec.flush()
        #                     keeps self.buffer for the deferred commit, instead
        #                     of resetting it and uploading an empty object.
        data = b"hello world"
        file = self._make_write_file(data, autocommit=autocommit)

        assert file._upload_chunk(final=True) is False
        if not autocommit:
            # Deferred: nothing is uploaded until commit() runs.
            file.fs.core.put_object.assert_not_called()
            file.commit()
        file.fs.core.put_object.assert_called_once_with(S3Path("bucket", "key.txt"), data)

    def test_upload_chunk_empty_file_touches(self):
        # An intentionally empty file (tell() == 0) is created via touch(),
        # never via a PutObject request of the file.
        file = self._make_write_file(b"", autocommit=True)

        assert file._upload_chunk(final=True) is False
        file.fs.touch.assert_called_once()
        file.fs.core.put_object.assert_not_called()

    @pytest.mark.parametrize("multipart", [False, True])
    def test_discard(self, multipart):
        # Rollback (Transaction.complete(commit=False)) never creates the object:
        # a single small file is a no-op, while a multipart upload is aborted.
        if multipart:
            file = self._make_multipart_write_file(b"x" * 16, autocommit=False)
        else:
            file = self._make_write_file(b"hello world", autocommit=False)
        assert file._upload_chunk(final=True) is False

        file.discard()

        if multipart:
            file.fs._call.assert_called_once()
            assert file.fs._call.call_args.args[0] == S3_CLIENT.abort_multipart_upload
        else:
            file.fs._call.assert_not_called()
        file.fs.core.put_object.assert_not_called()
        assert file.multipart_upload is None
        assert file.multipart_upload_parts == []

    def test_discard_waits_for_running_parts(self):
        # GH-976: a part that is still uploading when the upload is aborted
        # may be stored after the abort, so the abort waits for it. The
        # parts that have not started are cancelled.
        file = self._make_write_file(b"", autocommit=False)
        file.multipart_upload = S3MultipartUpload(
            {"Bucket": "bucket", "Key": "key.txt", "UploadId": "uploadid"}
        )
        events = []
        file.fs._call.side_effect = lambda *args, **kwargs: events.append("abort")
        started = threading.Event()
        release = threading.Event()

        def upload_part():
            started.set()
            # Uploading until the abort waits for it, so that an abort that
            # does not wait comes first.
            release.wait(5)
            events.append("part 1 stored")

        def wait_parts(futures):
            release.set()
            return wait(futures)

        with (
            ThreadPoolExecutor(max_workers=1) as executor,
            mock.patch("pyathena.filesystem.s3.wait", side_effect=wait_parts) as waited,
        ):
            running = executor.submit(upload_part)
            pending = executor.submit(events.append, "part 2 stored")
            file.multipart_upload_parts = [running, pending]
            started.wait(5)
            file.discard()

        assert events == ["part 1 stored", "abort"]
        waited.assert_called_once_with([running])
        assert pending.cancelled()

    @pytest.mark.parametrize("abort_fails", [False, True])
    @pytest.mark.parametrize("error", [RuntimeError, KeyboardInterrupt])
    def test_commit_failure_and_discard(self, caplog, error, abort_fails):
        # GH-1014: a failed or interrupted completion is aborted by commit(),
        # so a later discard(), such as a transaction rollback, does not
        # abort the upload again. GH-945: if the abort also fails, the upload
        # is kept so that discard() retries the abort.
        file = self._make_multipart_write_file(b"x" * 16, autocommit=False)
        file._upload_chunk(final=True)
        file.fs.core.complete_multipart_upload.side_effect = error("complete failed")
        if abort_fails:
            file.fs._call.side_effect = [PermissionError("abort failed"), None]

        # The abort failure is logged, and the original error propagates.
        with pytest.raises(error, match="complete failed"):
            file.commit()
        assert (file.multipart_upload is not None) is abort_fails
        assert bool(file.multipart_upload_parts) is abort_fails
        assert (
            "Failed to abort multipart upload uploadid to s3://bucket/key.txt." in caplog.text
        ) is abort_fails
        file.discard()

        assert file.fs._call.call_args_list == [
            mock.call(
                S3_CLIENT.abort_multipart_upload,
                Bucket="bucket",
                Key="key.txt",
                UploadId="uploadid",
            )
        ] * (2 if abort_fails else 1)
        assert file.multipart_upload is None
        assert file.multipart_upload_parts == []

    def test_commit_failure_and_interrupted_abort(self):
        # GH-945: if the abort after a failed completion is interrupted, the
        # interrupt propagates and the upload is kept so that discard()
        # retries the abort.
        file = self._make_multipart_write_file(b"x" * 16, autocommit=False)
        file._upload_chunk(final=True)
        file.fs.core.complete_multipart_upload.side_effect = RuntimeError("complete failed")
        file.fs._call.side_effect = [KeyboardInterrupt, None]

        with pytest.raises(KeyboardInterrupt):
            file.commit()
        assert file.multipart_upload is not None
        assert file.multipart_upload_parts
        file.discard()

        assert file.fs._call.call_count == 2
        assert file.multipart_upload is None
        assert file.multipart_upload_parts == []

    def test_discard_on_event_loop_thread(self):
        # GH-976: the parts that have not started are cancelled and not
        # waited for, so a rollback on the thread of the event loop that
        # would run them does not block.
        file = self._make_write_file(b"", autocommit=False)
        file.multipart_upload = S3MultipartUpload(
            {"Bucket": "bucket", "Key": "key.txt", "UploadId": "uploadid"}
        )
        parts = []

        async def rollback():
            executor = S3AioExecutor(loop=asyncio.get_running_loop())
            parts.extend(executor.submit(file.fs.core.upload_part) for _ in range(2))
            file.multipart_upload_parts = list(parts)
            file.discard()

        thread = threading.Thread(target=asyncio.run, args=(rollback(),), daemon=True)
        thread.start()
        thread.join(5)

        assert not thread.is_alive()
        assert all(part.cancelled() for part in parts)
        file.fs.core.upload_part.assert_not_called()
        file.fs._call.assert_called_once()
        assert file.fs._call.call_args.args[0] == S3_CLIENT.abort_multipart_upload

    @pytest.mark.parametrize("autocommit", [True, False])
    def test_upload_chunk_multipart(self, autocommit):
        # Multipart upload (CompleteMultipartUpload), completed from the uploaded
        # parts; the buffer is never read for the body and no one-shot PutObject
        # is issued. autocommit=True completes inside _upload_chunk; autocommit=
        # False (transaction) defers completion to commit().
        #
        # The mid-stream assertion also guards why _upload_chunk returns
        # `not final` rather than a plain False (as s3fs does): PyAthena delegates
        # mid-stream buffer management to fsspec, so a non-final chunk MUST return
        # True to have fsspec reset the buffer between parts. Returning False
        # mid-stream would re-upload the already-sent buffer.
        file = self._make_multipart_write_file(b"x" * 16, autocommit=autocommit)

        assert file._upload_chunk(final=False) is True  # mid-stream -> buffer reset
        assert file._upload_chunk(final=True) is False  # final -> buffer kept
        if not autocommit:
            # Deferred: the multipart upload is completed by commit().
            file.fs.core.complete_multipart_upload.assert_not_called()
            file.commit()
        file.fs.core.complete_multipart_upload.assert_called_once()
        file.fs.core.put_object.assert_not_called()


class TestCompressedBuffer:
    @pytest.mark.parametrize(
        ("compression", "decompress"),
        [("gzip", gzip.decompress), ("bz2", bz2.decompress), ("xz", lzma.decompress)],
    )
    def test_compress(self, compression, decompress):
        assert decompress(CompressedBuffer.compress(b"a" * 100, compression)) == b"a" * 100

    def test_compress_non_contiguous_memoryview(self):
        value = memoryview(b"ab" * 4)[::2]

        assert gzip.decompress(CompressedBuffer.compress(value, "gzip")) == b"aaaa"

    @pytest.mark.parametrize("compression", ["unknown", "infer"])
    def test_compress_unsupported(self, compression):
        # "infer" is resolved from a path by the caller, as open() does.
        with pytest.raises(ValueError, match="not supported"):
            CompressedBuffer.compress(b"a", compression)

    def test_compress_codec_closing_its_file(self):
        # GH-1037: some codecs, such as the zstandard stream writer, close the
        # file that they write to when they are closed.
        def closing_gzip(f, mode):
            g = gzip.GzipFile(fileobj=f, mode=mode)
            close = g.close
            g.close = lambda: (close(), f.close())
            return g

        with mock.patch.dict(compr, {"closing": closing_gzip}):
            assert gzip.decompress(CompressedBuffer.compress(b"a", "closing")) == b"a"
