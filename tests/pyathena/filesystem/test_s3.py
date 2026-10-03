import asyncio
import functools
import gc
import io
import os
import re
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import Future, ThreadPoolExecutor, wait
from datetime import UTC, datetime
from itertools import chain, pairwise
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import botocore.exceptions
import pytest
from fsspec import Callback
from fsspec.dircache import DirCache
from fsspec.implementations.dirfs import DirFileSystem

import pyathena
from pyathena.filesystem import register_s3_filesystem
from pyathena.filesystem.s3 import S3File, S3FileSystem
from pyathena.filesystem.s3_executor import S3AioExecutor, S3ThreadPoolExecutor
from pyathena.filesystem.s3_object import S3Object, S3ObjectType, S3StorageClass
from pyathena.util import RetryConfig
from tests import ENV
from tests.pyathena.conftest import connect


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
            S3FileSystem.parse_path("s3://bucket/path/to/obj?foo=bar")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            S3FileSystem.parse_path("s3a://bucket?")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            S3FileSystem.parse_path("s3a://bucket?foo=bar")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            S3FileSystem.parse_path("s3a://bucket/path/to/obj?foo=bar")

    @staticmethod
    def _make_fs():
        # Build a minimal S3FileSystem without touching AWS, bypassing
        # __init__ which would require a boto3 client.
        fs = S3FileSystem.__new__(S3FileSystem)
        fs.dircache = {}
        fs._client = mock.MagicMock()
        fs._call = mock.MagicMock()
        fs._retry_config = RetryConfig()
        fs.request_kwargs = {}
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
        fs.DELETE_OBJECTS_MAX_KEYS = 1
        fs.dircache = self._barrier_dircache("bucket/dir")
        fs.dircache["bucket/dir"] = []

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

        expected = ["bucket/dir/sub", "bucket/dir/sub/file"]
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

    def test_find_maxdepth_counts_levels_like_fsspec(self):
        fs = self._make_fs()
        responses = {
            "dir/": {
                "Contents": [{"Key": "dir/direct"}],
                "CommonPrefixes": [{"Prefix": "dir/sub/"}],
            },
            "dir/sub/": {
                "Contents": [{"Key": "dir/sub/nested"}],
                "CommonPrefixes": [{"Prefix": "dir/sub/deep/"}],
            },
            "dir/sub/deep/": {"Contents": [{"Key": "dir/sub/deep/file"}]},
        }
        fs._call.side_effect = lambda method, **kwargs: responses[kwargs["Prefix"]]

        with pytest.raises(ValueError, match="maxdepth must be at least 1"):
            fs.find("s3://bucket/dir", maxdepth=0)
        fs._call.assert_not_called()

        assert fs.find("s3://bucket/dir", maxdepth=1) == ["bucket/dir/direct"]
        assert sorted(fs.find("s3://bucket/dir", maxdepth=1, withdirs=True)) == [
            "bucket/dir/direct",
            "bucket/dir/sub",
        ]
        assert sorted(fs.find("s3://bucket/dir", maxdepth=2)) == [
            "bucket/dir/direct",
            "bucket/dir/sub/nested",
        ]
        assert sorted(fs.find("s3://bucket/dir", maxdepth=3)) == [
            "bucket/dir/direct",
            "bucket/dir/sub/deep/file",
            "bucket/dir/sub/nested",
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

    def test_pipe_file_small_uses_put_object(self):
        fs = self._make_fs()
        fs.default_block_size = S3FileSystem.DEFAULT_BLOCK_SIZE
        fs.s3_additional_kwargs = {"ServerSideEncryption": "AES256"}
        fs._put_object = mock.MagicMock()

        # The filesystem-level s3_additional_kwargs are merged with the
        # call-level kwargs, as in the open() path.
        fs.pipe_file("s3://bucket/key", b"data", ContentType="text/plain")
        fs._put_object.assert_called_once_with(
            bucket="bucket",
            key="key",
            body=b"data",
            ServerSideEncryption="AES256",
            ContentType="text/plain",
        )

    def test_pipe_file_invalid_path_raises(self):
        fs = self._make_fs()
        with pytest.raises(ValueError, match="Cannot write to a bucket"):
            fs.pipe_file("s3://bucket", b"data")
        with pytest.raises(ValueError, match="version"):
            fs.pipe_file("s3://bucket/key?versionId=12345abcde", b"data")

    def test_pipe_file_non_contiguous_memoryview(self):
        # A non-contiguous memoryview within the block size in items, 4 items
        # of 8 bytes here, is uploaded with PutObject, as the buffered path
        # cannot write it.
        fs = self._make_fs()
        fs._put_object = mock.MagicMock()
        value = memoryview(b"ab" * 8).cast("H")[::2]

        fs.pipe_file("s3://bucket/key", value, block_size=6)

        fs._put_object.assert_called_once_with(bucket="bucket", key="key", body=b"ab" * 4)

    def test_pipe_file_small_drops_max_workers(self):
        fs = self._make_fs()
        fs._put_object = mock.MagicMock()

        # max_workers is an open() parameter and is not sent to PutObject.
        fs.pipe_file("s3://bucket/key", b"data", max_workers=2)
        fs._put_object.assert_called_once_with(bucket="bucket", key="key", body=b"data")

    @pytest.mark.parametrize(
        ("size", "block_size", "min_block_size"),
        [
            # The data fits in the maximum number of parts.
            (12, 4, None),
            # GH-953: more data is rejected with the minimum block size,
            (13, 4, 5),
            # which is at least the minimum part size.
            (5, 1, 4),
        ],
    )
    def test_check_multipart_upload_size(self, size, block_size, min_block_size):
        fs = self._make_fs()
        fs.MULTIPART_UPLOAD_MIN_PART_SIZE = 4
        fs.MULTIPART_UPLOAD_MAX_PARTS = 3

        if min_block_size is None:
            fs._check_multipart_upload_size("s3://bucket/key", size, block_size)
        else:
            with pytest.raises(ValueError, match=f"at least {min_block_size} bytes"):
                fs._check_multipart_upload_size("s3://bucket/key", size, block_size)

    @pytest.mark.parametrize("kwargs", [{"block_size": 4}, {}])
    def test_put_file_exceeding_max_parts(self, tmp_path, kwargs):
        # GH-953: a file that does not fit in the maximum number of parts is
        # rejected before anything is uploaded.
        fs = self._make_fs()
        fs.MULTIPART_UPLOAD_MAX_PARTS = 3
        fs.default_block_size = 4
        fs.open = mock.MagicMock()
        lpath = tmp_path / "data"
        lpath.write_bytes(b"a" * 13)

        with pytest.raises(ValueError, match="block_size"):
            fs.put_file(str(lpath), "s3://bucket/key", **kwargs)
        fs.open.assert_not_called()
        fs._call.assert_not_called()

    def test_put_file_block_size(self, tmp_path):
        # block_size is passed to open() instead of the S3 API.
        fs = self._make_fs()
        fs.open = mock.MagicMock()
        fs.open.return_value.__enter__.return_value.blocksize = 8
        lpath = tmp_path / "data"
        lpath.write_bytes(b"a" * 13)

        fs.put_file(str(lpath), "s3://bucket/key", block_size=8)

        fs.open.assert_called_once_with(
            "s3://bucket/key", "wb", block_size=8, s3_additional_kwargs={}
        )

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
        fs.MULTIPART_UPLOAD_MAX_PARTS = 3
        fs.default_block_size = 4
        fs.open = mock.MagicMock()
        fs._put_object = mock.MagicMock()

        with pytest.raises(ValueError, match="block_size"):
            fs.pipe_file("s3://bucket/key", value, **kwargs)
        fs.open.assert_not_called()
        fs._put_object.assert_not_called()
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
                S3FileSystem.MULTIPART_UPLOAD_MIN_PART_SIZE - 1,
                r"between 5 MiB \(5242880 bytes\) and 5 GiB \(5368709120 bytes\), inclusive",
            ),
            # GH-952: a part cannot be larger than the maximum part size.
            ("s3://bucket/key", S3FileSystem.MULTIPART_UPLOAD_MAX_PART_SIZE + 1, "between"),
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

    def test_open_append_lookup_failure(self, monkeypatch):
        # GH-976: an append whose lookup of the existing object fails leaves
        # no half-initialized file, whose garbage collection would close it.
        fs = self._make_fs()
        fs.default_cache_type = "bytes"

        def exists(path):
            # A new exception each time: one kept by a mock would keep its
            # traceback, and the file, alive.
            raise PermissionError("denied")

        fs.exists = exists
        unraisable = []
        monkeypatch.setattr(sys, "unraisablehook", unraisable.append)

        with pytest.raises(PermissionError, match="denied"):
            fs.open("s3://bucket/key", "ab")
        gc.collect()

        assert unraisable == []
        fs._call.assert_not_called()

    @pytest.mark.parametrize(
        "block_size",
        [S3FileSystem.MULTIPART_UPLOAD_MIN_PART_SIZE, S3FileSystem.MULTIPART_UPLOAD_MAX_PART_SIZE],
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

    def test_get_file_directory(self, tmp_path):
        fs = self._make_fs()
        fs.default_cache_type = "bytes"
        fs.info = mock.MagicMock(return_value=S3FileSystem._directory_object("bucket", "dir"))

        with pytest.raises(FileNotFoundError):
            fs.get_file("s3://bucket/dir", str(tmp_path / "dir"))
        assert list(tmp_path.iterdir()) == []

    def test_cat_ranges_range(self):
        fs, ranges = self._make_object_fs(b"0123456789")

        assert fs.cat_ranges(["s3://bucket/key"] * 4, [5, 0, -100, 12], [5, 3, 5, 20]) == [
            b"",
            b"012",
            b"01234",
            b"",
        ]
        assert sorted(ranges) == ["bytes=0-2", "bytes=0-4", "bytes=12-19"]

    def test_get_object_empty_range(self):
        fs = self._make_fs()

        with pytest.raises(ValueError, match="empty range"):
            fs._get_object("bucket", "key", ranges=(5, 5))
        fs._call.assert_not_called()

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
        fs._complete_multipart_upload = mock.MagicMock()
        futures = []
        for part_number in (1, 2):
            future: Future[SimpleNamespace] = Future()
            future.set_result(SimpleNamespace(etag=f'"e{part_number}"', part_number=part_number))
            futures.append(future)

        fs._finish_multipart_upload(
            bucket="bucket", key="key", upload_id="uploadid", futures=futures
        )
        fs._complete_multipart_upload.assert_called_once_with(
            bucket="bucket",
            key="key",
            upload_id="uploadid",
            parts=[
                {"ETag": '"e1"', "PartNumber": 1},
                {"ETag": '"e2"', "PartNumber": 2},
            ],
        )
        fs._call.assert_not_called()

    def test_finish_multipart_upload_aborts_on_failure(self):
        fs = self._make_fs()
        fs._complete_multipart_upload = mock.MagicMock()
        future: Future[SimpleNamespace] = Future()
        future.set_exception(RuntimeError("upload failed"))

        with pytest.raises(RuntimeError, match="upload failed"):
            fs._finish_multipart_upload(
                bucket="bucket", key="key", upload_id="uploadid", futures=[future]
            )
        fs._complete_multipart_upload.assert_not_called()
        fs._call.assert_called_once_with(
            fs._client.abort_multipart_upload,
            Bucket="bucket",
            Key="key",
            UploadId="uploadid",
        )

    def test_finish_multipart_upload_abort_failure_does_not_mask_the_original_error(self):
        fs = self._make_fs()
        fs._complete_multipart_upload = mock.MagicMock()
        fs._call = mock.MagicMock(side_effect=RuntimeError("abort failed"))
        future: Future[SimpleNamespace] = Future()
        future.set_exception(RuntimeError("upload failed"))

        # The abort failure is logged, and the original error propagates.
        with pytest.raises(RuntimeError, match="upload failed"):
            fs._finish_multipart_upload(
                bucket="bucket", key="key", upload_id="uploadid", futures=[future]
            )

    def test_finish_multipart_upload_waits_for_running_parts(self):
        # GH-976: a part that is still uploading when the upload is aborted
        # may be stored after the abort, so the abort waits for it. The
        # parts that have not started are cancelled.
        fs = self._make_fs()
        fs._complete_multipart_upload = mock.MagicMock()
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
                    bucket="bucket",
                    key="key",
                    upload_id="uploadid",
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
        fs._complete_multipart_upload = mock.MagicMock()
        failed: Future[SimpleNamespace] = Future()
        failed.set_exception(RuntimeError("upload failed"))
        never_started: Future[SimpleNamespace] = Future()
        errors = []

        def finish():
            try:
                fs._finish_multipart_upload(
                    bucket="bucket",
                    key="key",
                    upload_id="uploadid",
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

    @pytest.mark.parametrize(
        ("size", "block_size", "ranges"),
        [
            # A single range.
            (5 * 2**20, 5 * 2**20, [(0, 5 * 2**20)]),
            # The size is an exact multiple of the block size.
            (10 * 2**30, 5 * 2**30, [(0, 5 * 2**30), (5 * 2**30, 10 * 2**30)]),
            # A last range of the minimum part size is kept.
            (
                5 * 2**30 + 5 * 2**20,
                5 * 2**30,
                [(0, 5 * 2**30), (5 * 2**30, 5 * 2**30 + 5 * 2**20)],
            ),
            # GH-951: a last range shorter than the minimum part size is
            # merged into the previous one,
            (15 * 2**20 - 1, 5 * 2**20, [(0, 5 * 2**20), (5 * 2**20, 15 * 2**20 - 1)]),
            # which is split in half if it exceeds the maximum part size.
            (
                5 * 2**30 + 2**20,
                5 * 2**30,
                [(0, 5 * 2**29 + 2**19), (5 * 2**29 + 2**19, 5 * 2**30 + 2**20)],
            ),
        ],
    )
    def test_get_copy_ranges(self, size, block_size, ranges):
        assert self._make_fs()._get_copy_ranges(size, block_size) == ranges

    @pytest.mark.parametrize(
        ("size", "num_ranges"),
        [
            # The block size splits the object into the maximum number of parts.
            (10_000 * 5 * 2**20, 10_000),
            # GH-953: a larger object is split by a larger size instead of
            # into more parts than the maximum,
            (10_000 * 5 * 2**20 + 1, 9_999),
            (50 * 2**30, 10_000),
            # including the maximum object size.
            (5 * 2**40, 10_000),
        ],
    )
    def test_get_copy_ranges_max_parts(self, size, num_ranges):
        fs = self._make_fs()
        ranges = fs._get_copy_ranges(size, 5 * 2**20)

        assert len(ranges) == num_ranges
        assert ranges[0][0] == 0
        assert ranges[-1][1] == size
        assert all(end == start for (_, end), (start, _) in pairwise(ranges))
        assert all(
            fs.MULTIPART_UPLOAD_MIN_PART_SIZE <= end - start <= fs.MULTIPART_UPLOAD_MAX_PART_SIZE
            for start, end in ranges
        )

    @pytest.mark.parametrize("max_workers", [1, 4])
    def test_copy_object_with_multipart_upload_part_sizes(self, max_workers):
        # GH-951: the parts are within the S3 part size limits whatever the
        # number of workers; a single worker used to copy the whole object
        # as one part larger than 5 GiB.
        fs = self._make_fs()
        fs._create_multipart_upload = mock.MagicMock(
            return_value=SimpleNamespace(upload_id="uploadid")
        )
        fs._upload_part_copy = mock.MagicMock()
        fs._finish_multipart_upload = mock.MagicMock()

        fs._copy_object_with_multipart_upload(
            bucket1="bucket",
            key1="src",
            size1=5 * 2**30 + 2**20,
            bucket2="bucket",
            key2="dst",
            max_workers=max_workers,
        )

        parts = sorted(
            (c.kwargs["part_number"], c.kwargs["copy_source_ranges"])
            for c in fs._upload_part_copy.call_args_list
        )
        assert parts == [
            (1, (0, 5 * 2**29 + 2**19)),
            (2, (5 * 2**29 + 2**19, 5 * 2**30 + 2**20)),
        ]

    @pytest.mark.parametrize(
        "block_size",
        [
            S3FileSystem.MULTIPART_UPLOAD_MIN_PART_SIZE - 1,
            S3FileSystem.MULTIPART_UPLOAD_MAX_PART_SIZE + 1,
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
                bucket1="bucket",
                key1="src",
                size1=5 * 2**30 + 2**20,
                bucket2="bucket",
                key2="dst",
                block_size=block_size,
            )
        fs._call.assert_not_called()

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
            ],
            "IsTruncated": False,
        }

        actual = fs.ls("s3://bucket/path", detail=True, versions=True)
        fs._call.assert_called_once_with(
            fs._client.list_object_versions, Bucket="bucket", Prefix="path/", Delimiter="/"
        )
        assert [(f.name, f.version_id, f.is_latest) for f in actual] == [
            ("bucket/path/dir", None, None),
            ("bucket/path/key", "v2", True),
            ("bucket/path/key", "v1", False),
        ]
        assert actual[1].size == 4
        assert actual[2].size == 2

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
            ("bucket/path/key", "v2", 4),
            ("bucket/path/key", "v1", 2),
        ]

    def test_dir_filesystem(self):
        # DirFileSystem copies every entry with copy() before renaming it.
        fs = self._make_fs()
        fs._call.side_effect = [
            {
                "CommonPrefixes": [{"Prefix": "path/dir/"}],
                "Contents": [{"Key": "path/key", "Size": 4}],
                "IsTruncated": False,
            },
            {"ContentLength": 4, "ETag": '"etag"'},
        ]
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
        assert fs._call.call_count == 2

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

    @pytest.fixture(scope="class")
    def fs(self, request):
        if not hasattr(request, "param"):
            request.param = {}
        return S3FileSystem(connect(), **request.param)

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
        # lowest level access: use _get_object
        data = fs._get_object(
            ENV.s3_staging_bucket, ENV.s3_filesystem_test_file_key, ranges=(start, end)
        )
        assert data == (start, target_data), data
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

        # Verify directories are included
        dirs = [r for r in result if not r.endswith(".txt")]
        assert len(dirs) > 0
        assert any("subdir1" in d for d in dirs)
        assert any("subdir2" in d for d in dirs)
        assert any("subdir3" in d for d in dirs)

        # Test withdirs=False explicitly
        result = fs.find(dir_, withdirs=False)
        assert len(result) == 4  # Only files

    def test_du(self):
        # TODO
        pass

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

    # TODO Recursive directory traversal currently requires wildcards,
    #  but with the recursive option, recursive directory traversal
    #  must be possible without wildcards.
    # def test_move_recursive(self, fs):
    #     dir1 = (
    #         f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
    #         f"filesystem/test_move_recursive/"
    #     )
    #     dir2 = (
    #         f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
    #         f"filesystem/test_move_recursive_copy/"
    #     )
    #
    #     for i in range(10):
    #         fs.pipe(f"{dir1}test_{i}", bytes(i))
    #     fs.move(dir1, dir2, recursive=True)
    #     for i in range(10):
    #         assert fs.cat(f"{dir2}test_{i}") == bytes(i)
    #         assert not fs.exists(f"{dir1}test_{i}")

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
        fs._put_object(bucket=bucket, key=key, body=b"bar")
        assert checksum == fs.checksum(path)
        assert checksum != fs.checksum(path, refresh=True)

        fs.pipe_file(path, b"foo")
        checksum = fs.checksum(path)
        fs.ls(path)  # caching
        fs._delete_object(bucket, key)
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
        fs.pipe(path, data)
        assert fs.metadata(path) == {}

        # The keys are stored as-is; hyphenated names can be passed by
        # unpacking a dictionary.
        fs.setxattr(path, attr1="value1", **{"attr-2": "value2"})
        assert fs.metadata(path) == {"attr1": "value1", "attr-2": "value2"}
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

    def test_list_and_clear_multipart_uploads(self, fs):
        # Scope the list/clear to a unique prefix so that parallel test
        # workers' in-flight multipart uploads in the shared bucket are
        # not aborted.
        bucket = ENV.s3_staging_bucket
        prefix = (
            f"{ENV.s3_staging_key}{ENV.schema}/filesystem/test_multipart_uploads/{uuid.uuid4()}"
        )
        prefix_path = f"s3://{bucket}/{prefix}"
        key = f"{prefix}/file"
        upload = fs._create_multipart_upload(bucket=bucket, key=key)

        uploads = fs.list_multipart_uploads(prefix_path)
        listed = next((u for u in uploads if u.upload_id == upload.upload_id), None)
        assert listed
        assert listed.bucket == bucket
        assert listed.key == key
        assert listed.initiated

        fs.clear_multipart_uploads(prefix_path)
        uploads = fs.list_multipart_uploads(prefix_path)
        assert not any(u.upload_id == upload.upload_id for u in uploads)

    def test_object_version_info(self, fs):
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_object_version_info/{uuid.uuid4()}"
        )
        fs.pipe(path, b"data")

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
    @staticmethod
    def _make_write_file(data: bytes, autocommit: bool):
        # Build a minimal write-mode S3File without touching AWS, bypassing
        # __init__ which would require a real connection.
        file = S3File.__new__(S3File)
        file.fs = mock.MagicMock(spec=S3FileSystem)
        file.path = "s3://bucket/key.txt"
        file.bucket = "bucket"
        file.key = "key.txt"
        file.s3_additional_kwargs = {}
        file.autocommit = autocommit
        file.blocksize = S3FileSystem.MULTIPART_UPLOAD_MIN_PART_SIZE
        file.fs.MULTIPART_UPLOAD_MAX_PARTS = S3FileSystem.MULTIPART_UPLOAD_MAX_PARTS
        file.append_block = False
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
        file.fs.MULTIPART_UPLOAD_MIN_PART_SIZE = 4
        file.fs.MULTIPART_UPLOAD_MAX_PART_SIZE = 8
        file.multipart_upload = SimpleNamespace(upload_id="uploadid")
        file._executor = ThreadPoolExecutor(max_workers=1)
        file.fs._upload_part.side_effect = lambda **kw: SimpleNamespace(
            etag=f'"e{kw["part_number"]}"', part_number=kw["part_number"]
        )
        return file

    @staticmethod
    def _make_append_fs(existing: bytes):
        # A mocked filesystem holding an existing object, with a minimum part
        # size of 4 bytes so that the write and append paths can be exercised
        # with tiny data and no AWS access.
        fs = mock.MagicMock(spec=S3FileSystem)
        fs.MULTIPART_UPLOAD_MIN_PART_SIZE = 4
        fs.MULTIPART_UPLOAD_MAX_PART_SIZE = 64
        fs.MULTIPART_UPLOAD_MAX_PARTS = S3FileSystem.MULTIPART_UPLOAD_MAX_PARTS
        fs.exists.return_value = True
        fs.info.return_value = S3Object(
            init={"ContentLength": len(existing)},
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="bucket",
            key="key.txt",
        )
        fs.cat.return_value = existing
        fs._create_multipart_upload.return_value = SimpleNamespace(upload_id="uploadid")

        def part(**kw):
            return SimpleNamespace(etag=f'"e{kw["part_number"]}"', part_number=kw["part_number"])

        fs._upload_part.side_effect = part
        fs._upload_part_copy.side_effect = part
        fs._get_copy_ranges.side_effect = functools.partial(S3FileSystem._get_copy_ranges, fs)
        return fs

    @staticmethod
    def _uploaded_object(fs, existing: bytes) -> bytes:
        # Rebuild the object S3 would store from the mocked upload calls.
        # A part copy without a range copies the whole existing object.
        if fs._put_object.called:
            fs._create_multipart_upload.assert_not_called()
            return fs._put_object.call_args.kwargs["body"]
        fs._finish_multipart_upload.assert_called_once()
        parts = []
        for c in fs._upload_part_copy.call_args_list:
            start, end = c.kwargs.get("copy_source_ranges", (0, len(existing)))
            parts.append((c.kwargs["part_number"], existing[start:end]))
        parts += [
            (c.kwargs["part_number"], c.kwargs["body"]) for c in fs._upload_part.call_args_list
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
        assert fs._create_multipart_upload.called is multipart
        assert fs._upload_part_copy.called is part_copy
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
            (c.kwargs["part_number"], c.kwargs["copy_source_ranges"])
            for c in fs._upload_part_copy.call_args_list
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
            (c.kwargs["part_number"], len(c.kwargs["body"])) for c in fs._upload_part.call_args_list
        )
        sizes = [size for _, size in parts]
        assert all(size >= fs.MULTIPART_UPLOAD_MIN_PART_SIZE for size in sizes[:-1])
        assert all(size <= fs.MULTIPART_UPLOAD_MAX_PART_SIZE for size in sizes)

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
        fs.MULTIPART_UPLOAD_MAX_PARTS = 3

        f = S3File(fs, "s3://bucket/key.txt", mode=mode, block_size=4, autocommit=autocommit)
        self._write_and_close(f, writes)
        if not autocommit:
            f.commit()

        assert self._uploaded_object(fs, existing) == existing + b"".join(writes)
        assert fs._upload_part_copy.call_count + fs._upload_part.call_count == 3

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
        fs.MULTIPART_UPLOAD_MAX_PARTS = 3

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
        assert [c.kwargs["part_number"] for c in executor.submit.call_args_list] == [1, 2, 3]
        executor.shutdown.assert_called()
        fs._call.assert_called_once_with(
            "abort_multipart_upload", Bucket="bucket", Key="key.txt", UploadId="uploadid"
        )
        fs._finish_multipart_upload.assert_not_called()
        fs._put_object.assert_not_called()

    @pytest.mark.parametrize("autocommit", [True, False])
    def test_write_exceeding_max_parts_abort_failure(self, autocommit):
        # An abort failure is logged; the part limit error propagates, and
        # neither closing the file nor committing a deferred write retries
        # the upload or completes it.
        fs = self._make_append_fs(b"")
        fs.MULTIPART_UPLOAD_MAX_PARTS = 3
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
        assert [c.kwargs["part_number"] for c in executor.submit.call_args_list] == [1, 2, 3]
        executor.shutdown.assert_called()
        fs._call.assert_called_once()
        fs._finish_multipart_upload.assert_not_called()
        fs._put_object.assert_not_called()

    def test_write_exceeding_max_parts_without_close(self):
        # The executor of the closed file is shut down, as fsspec does not
        # close it again when it is garbage collected.
        fs = self._make_append_fs(b"")
        fs.MULTIPART_UPLOAD_MAX_PARTS = 3
        executor = mock.MagicMock(wraps=S3ThreadPoolExecutor(max_workers=1))
        f = S3File(fs, "s3://bucket/key.txt", mode="wb", block_size=4, executor=executor)

        for _ in range(3):
            f.write(b"a" * 4)
        with pytest.raises(ValueError, match="block_size"):
            f.write(b"a" * 4)

        assert f.closed
        executor.shutdown.assert_called_once()

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
            "abort_multipart_upload",
            Bucket="bucket",
            Key="key.txt",
            UploadId="uploadid",
            RequestPayer="requester",
            ExpectedBucketOwner="123",
        )
        fs._finish_multipart_upload.assert_not_called()
        fs._put_object.assert_not_called()

    @pytest.mark.parametrize(
        ("objects", "target"),
        [
            ([(0, b"")], b""),
            ([(0, b"foo")], b"foo"),
            ([(0, b"foo"), (1, b"bar")], b"foobar"),
            ([(1, b"foo"), (0, b"bar")], b"barfoo"),
            ([(1, b""), (0, b"bar")], b"bar"),
            ([(1, b"foo"), (0, b"")], b"foo"),
            ([(2, b"foo"), (1, b"bar"), (3, b"baz")], b"barfoobaz"),
        ],
    )
    def test_merge_objects(self, objects, target):
        assert S3File._merge_objects(objects) == target

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

    def test_format_ranges(self):
        assert S3File._format_ranges((0, 100)) == "bytes=0-99"
        assert S3File._format_ranges((100, None)) == "bytes=100-"
        assert S3File._format_ranges((-8, None)) == "bytes=-8"

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
            file.fs._put_object.assert_not_called()
            file.commit()
        file.fs._put_object.assert_called_once_with(bucket="bucket", key="key.txt", body=data)

    def test_upload_chunk_empty_file_touches(self):
        # An intentionally empty file (tell() == 0) is created via touch(),
        # never via _put_object.
        file = self._make_write_file(b"", autocommit=True)

        assert file._upload_chunk(final=True) is False
        file.fs.touch.assert_called_once()
        file.fs._put_object.assert_not_called()

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
            assert file.fs._call.call_args.args[0] == "abort_multipart_upload"
        else:
            file.fs._call.assert_not_called()
        file.fs._put_object.assert_not_called()
        assert file.multipart_upload is None
        assert file.multipart_upload_parts == []

    def test_discard_waits_for_running_parts(self):
        # GH-976: a part that is still uploading when the upload is aborted
        # may be stored after the abort, so the abort waits for it. The
        # parts that have not started are cancelled.
        file = self._make_write_file(b"", autocommit=False)
        file.multipart_upload = SimpleNamespace(upload_id="uploadid")
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

    def test_discard_on_event_loop_thread(self):
        # GH-976: the parts that have not started are cancelled and not
        # waited for, so a rollback on the thread of the event loop that
        # would run them does not block.
        file = self._make_write_file(b"", autocommit=False)
        file.multipart_upload = SimpleNamespace(upload_id="uploadid")
        parts = []

        async def rollback():
            executor = S3AioExecutor(loop=asyncio.get_running_loop())
            parts.extend(executor.submit(file.fs._upload_part) for _ in range(2))
            file.multipart_upload_parts = list(parts)
            file.discard()

        thread = threading.Thread(target=asyncio.run, args=(rollback(),), daemon=True)
        thread.start()
        thread.join(5)

        assert not thread.is_alive()
        assert all(part.cancelled() for part in parts)
        file.fs._upload_part.assert_not_called()
        file.fs._call.assert_called_once()
        assert file.fs._call.call_args.args[0] == "abort_multipart_upload"

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
            file.fs._finish_multipart_upload.assert_not_called()
            file.commit()
        file.fs._finish_multipart_upload.assert_called_once()
        file.fs._put_object.assert_not_called()
