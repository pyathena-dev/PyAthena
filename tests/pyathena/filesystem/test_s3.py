import os
import tempfile
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from itertools import chain
from pathlib import Path

import fsspec
import pytest
from fsspec import Callback

from pyathena.filesystem.s3 import S3File, S3FileSystem
from pyathena.filesystem.s3_object import S3ObjectType, S3StorageClass
from tests import ENV
from tests.pyathena.conftest import connect


@pytest.fixture(scope="class")
def register_filesystem():
    fsspec.register_implementation("s3", "pyathena.filesystem.s3.S3FileSystem", clobber=True)
    fsspec.register_implementation("s3a", "pyathena.filesystem.s3.S3FileSystem", clobber=True)


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
    def test_pipe_cat(self, fs, base, exp):
        data = b"a" * (base * exp)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_pipe_file/{uuid.uuid4()}"
        )
        fs.pipe(path, data)
        assert fs.cat(path) == data

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
