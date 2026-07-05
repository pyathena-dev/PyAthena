import io
import logging
import os
import tempfile
import time
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from itertools import chain
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import fsspec
import pytest
from fsspec import Callback
from fsspec.registry import _registry, known_implementations

from pyathena.filesystem import register_s3_filesystem
from pyathena.filesystem.s3 import S3File, S3FileSystem
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
        fs.allow_bucket_creation = False
        fs.allow_bucket_deletion = False
        fs.s3_additional_kwargs = {}
        fs._intrans = False
        fs.version_aware = False
        return fs

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

    def test_pipe_file_large_uses_multipart_upload(self):
        fs = self._make_fs()
        fs.default_block_size = 5
        # Shrink the part-size limits so the test data is split without
        # allocating real 5 MiB payloads.
        fs.MULTIPART_UPLOAD_MIN_PART_SIZE = 5
        fs.MULTIPART_UPLOAD_MAX_PART_SIZE = 2**30
        fs._put_object = mock.MagicMock()
        fs._create_multipart_upload = mock.MagicMock(
            return_value=SimpleNamespace(upload_id="uploadid")
        )
        uploaded: dict[int, bytes] = {}

        def upload_part(**kwargs):
            uploaded[kwargs["part_number"]] = kwargs["body"]
            return SimpleNamespace(
                etag=f'"e{kwargs["part_number"]}"', part_number=kwargs["part_number"]
            )

        fs._upload_part = mock.MagicMock(side_effect=upload_part)
        fs._complete_multipart_upload = mock.MagicMock()

        fs.pipe_file("s3://bucket/key", b"0123456789ABCDEF", ContentType="text/plain")

        fs._put_object.assert_not_called()
        fs._create_multipart_upload.assert_called_once_with(
            bucket="bucket", key="key", ContentType="text/plain"
        )
        # The data is split into block-size parts and reassembled in order.
        assert uploaded == {1: b"01234", 2: b"56789", 3: b"ABCDE", 4: b"F"}
        fs._complete_multipart_upload.assert_called_once_with(
            bucket="bucket",
            key="key",
            upload_id="uploadid",
            parts=[{"ETag": f'"e{i}"', "PartNumber": i} for i in range(1, 5)],
        )

    def test_pipe_file_aborts_multipart_upload_on_failure(self):
        fs = self._make_fs()
        fs.default_block_size = 5
        fs.MULTIPART_UPLOAD_MIN_PART_SIZE = 5
        fs.MULTIPART_UPLOAD_MAX_PART_SIZE = 2**30
        fs._create_multipart_upload = mock.MagicMock(
            return_value=SimpleNamespace(upload_id="uploadid")
        )
        fs._upload_part = mock.MagicMock(side_effect=RuntimeError("upload failed"))
        fs._complete_multipart_upload = mock.MagicMock()

        with pytest.raises(RuntimeError, match="upload failed"):
            fs.pipe_file("s3://bucket/key", b"0123456789ABCDEF")
        fs._complete_multipart_upload.assert_not_called()
        fs._call.assert_called_once_with(
            fs._client.abort_multipart_upload,
            Bucket="bucket",
            Key="key",
            UploadId="uploadid",
        )

    def test_pipe_file_abort_failure_does_not_mask_the_original_error(self):
        fs = self._make_fs()
        fs.default_block_size = 5
        fs.MULTIPART_UPLOAD_MIN_PART_SIZE = 5
        fs.MULTIPART_UPLOAD_MAX_PART_SIZE = 2**30
        fs._create_multipart_upload = mock.MagicMock(
            return_value=SimpleNamespace(upload_id="uploadid")
        )
        fs._upload_part = mock.MagicMock(side_effect=RuntimeError("upload failed"))
        fs._complete_multipart_upload = mock.MagicMock()
        fs._call = mock.MagicMock(side_effect=RuntimeError("abort failed"))

        # The abort failure is logged, and the original error propagates.
        with pytest.raises(RuntimeError, match="upload failed"):
            fs.pipe_file("s3://bucket/key", b"0123456789ABCDEF")

    def test_head_object_version_aware(self):
        fs = self._make_fs()
        fs._call.return_value = {"ContentLength": 4, "ETag": '"etag"', "VersionId": "v1"}

        # The observed version is pinned only in version-aware mode.
        assert fs._head_object("bucket/key").version_id is None

        fs = self._make_fs()
        fs.version_aware = True
        fs._call.return_value = {"ContentLength": 4, "ETag": '"etag"', "VersionId": "v1"}
        assert fs._head_object("bucket/key").version_id == "v1"

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

        now = datetime.now(timezone.utc)
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

        # Test maxdepth=0 (only files in the root)
        result = fs.find(dir_, maxdepth=0)
        assert len(result) == 1
        assert fs._strip_protocol(f"{dir_}/file0.txt") in result

        # Test maxdepth=1 (files in root and level1)
        result = fs.find(dir_, maxdepth=1)
        assert len(result) == 2
        assert fs._strip_protocol(f"{dir_}/file0.txt") in result
        assert fs._strip_protocol(f"{dir_}/level1/file1.txt") in result

        # Test maxdepth=2 (files in root, level1, and level2)
        result = fs.find(dir_, maxdepth=2)
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

    @pytest.mark.parametrize("fs", [{"version_aware": True}], indirect=True)
    def test_version_aware_read(self, fs):
        # On an unversioned bucket, the version-aware mode is a no-op for
        # reads: HeadObject returns no version to pin.
        path = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}"
        with fs.open(path, "rb") as f:
            assert f.read() == b"0123456789"

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


class _DummyFileSystem:
    pass


@pytest.fixture
def registry_state():
    registry = dict(_registry)
    known = {k: dict(v) for k, v in known_implementations.items()}
    yield
    _registry.clear()
    _registry.update(registry)
    known_implementations.clear()
    known_implementations.update(known)


def test_register_s3_filesystem(registry_state):
    _registry.pop("s3", None)
    _registry.pop("s3a", None)

    register_s3_filesystem()
    assert fsspec.registry["s3"] is S3FileSystem
    assert fsspec.registry["s3a"] is S3FileSystem


def test_register_s3_filesystem_overwrites_existing_registration(registry_state, caplog):
    fsspec.register_implementation("s3", _DummyFileSystem, clobber=True)

    with caplog.at_level(logging.WARNING, logger="pyathena.filesystem"):
        register_s3_filesystem()
    # An explicitly registered filesystem class is overwritten with a warning.
    assert fsspec.registry["s3"] is S3FileSystem
    assert "will be overwritten" in caplog.text

    # Re-registering PyAthena's own filesystem does not warn.
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="pyathena.filesystem"):
        register_s3_filesystem()
    assert not caplog.text
