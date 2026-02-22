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
from pyathena.filesystem.s3_async import AioS3File, AioS3FileSystem
from pyathena.filesystem.s3_object import S3ObjectType, S3StorageClass
from tests import ENV
from tests.pyathena.conftest import connect


@pytest.fixture(scope="class")
def register_async_filesystem():
    fsspec.register_implementation(
        "s3", "pyathena.filesystem.s3_async.AioS3FileSystem", clobber=True
    )
    fsspec.register_implementation(
        "s3a", "pyathena.filesystem.s3_async.AioS3FileSystem", clobber=True
    )


@pytest.mark.usefixtures("register_async_filesystem")
class TestAioS3FileSystem:
    def test_parse_path(self):
        actual = AioS3FileSystem.parse_path("s3://bucket")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = AioS3FileSystem.parse_path("s3://bucket/")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = AioS3FileSystem.parse_path("s3://bucket/path/to/obj")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] is None

        actual = AioS3FileSystem.parse_path("s3://bucket/path/to/obj?versionId=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = AioS3FileSystem.parse_path("s3a://bucket")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = AioS3FileSystem.parse_path("s3a://bucket/")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = AioS3FileSystem.parse_path("s3a://bucket/path/to/obj")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] is None

        actual = AioS3FileSystem.parse_path("s3a://bucket/path/to/obj?versionId=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = AioS3FileSystem.parse_path("bucket")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = AioS3FileSystem.parse_path("bucket/")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = AioS3FileSystem.parse_path("bucket/path/to/obj")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] is None

        actual = AioS3FileSystem.parse_path("bucket/path/to/obj?versionId=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = AioS3FileSystem.parse_path("bucket/path/to/obj?versionID=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = AioS3FileSystem.parse_path("bucket/path/to/obj?versionid=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = AioS3FileSystem.parse_path("bucket/path/to/obj?version_id=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

    def test_parse_path_invalid(self):
        with pytest.raises(ValueError, match="Invalid S3 path format"):
            AioS3FileSystem.parse_path("http://bucket")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            AioS3FileSystem.parse_path("s3://bucket?")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            AioS3FileSystem.parse_path("s3://bucket?foo=bar")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            AioS3FileSystem.parse_path("s3://bucket/path/to/obj?foo=bar")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            AioS3FileSystem.parse_path("s3a://bucket?")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            AioS3FileSystem.parse_path("s3a://bucket?foo=bar")

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            AioS3FileSystem.parse_path("s3a://bucket/path/to/obj?foo=bar")

    @pytest.fixture(scope="class")
    def fs(self, request):
        if not hasattr(request, "param"):
            request.param = {}
        return AioS3FileSystem(connection=connect(), **request.param)

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
        data = fs._sync_fs._get_object(
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
            (1, 2**10),
            (1, 2**20),
        ],
    )
    def test_write(self, fs, base, exp):
        data = b"a" * (base * exp)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_write/{uuid.uuid4()}"
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
            (1, 2**10),
            (1, 2**20),
        ],
    )
    def test_append(self, fs, base, exp):
        data = b"a" * (base * exp)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_append/{uuid.uuid4()}"
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

    @pytest.mark.asyncio
    async def test_ls_buckets(self, fs):
        fs.invalidate_cache()
        actual = await fs._ls("s3://")
        assert ENV.s3_staging_bucket in actual, actual

        fs.invalidate_cache()
        actual = await fs._ls("s3:///")
        assert ENV.s3_staging_bucket in actual, actual

        fs.invalidate_cache()
        actual = await fs._ls("s3://", detail=True)
        found = next(filter(lambda x: x.name == ENV.s3_staging_bucket, actual), None)
        assert found
        assert found.name == ENV.s3_staging_bucket

        fs.invalidate_cache()
        actual = await fs._ls("s3:///", detail=True)
        found = next(filter(lambda x: x.name == ENV.s3_staging_bucket, actual), None)
        assert found
        assert found.name == ENV.s3_staging_bucket

    @pytest.mark.asyncio
    async def test_ls_dirs(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_ls_dirs"
        )
        for i in range(5):
            await fs._pipe_file(f"{dir_}/prefix/test_{i}", bytes(i))
        await fs._touch(f"{dir_}/prefix2")

        assert len(await fs._ls(f"{dir_}/prefix")) == 5
        assert len(await fs._ls(f"{dir_}/prefix/")) == 5
        assert len(await fs._ls(f"{dir_}/prefix/test_")) == 0
        assert len(await fs._ls(f"{dir_}/prefix2")) == 1

        test_1 = await fs._ls(f"{dir_}/prefix/test_1")
        assert len(test_1) == 1
        assert test_1[0] == fs._strip_protocol(f"{dir_}/prefix/test_1")

        test_1_detail = await fs._ls(f"{dir_}/prefix/test_1", detail=True)
        assert len(test_1_detail) == 1
        assert test_1_detail[0].name == fs._strip_protocol(f"{dir_}/prefix/test_1")
        assert test_1_detail[0].size == 1

    @pytest.mark.asyncio
    async def test_info_bucket(self, fs):
        dir_ = f"s3://{ENV.s3_staging_bucket}"
        bucket, key, version_id = fs.parse_path(dir_)
        info = await fs._info(dir_)

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
        info = await fs._info(dir_)

        assert info.name == fs._strip_protocol(dir_)
        assert info.bucket == bucket
        assert info.key is None
        assert info.last_modified is None
        assert info.size == 0
        assert info.etag is None
        assert info.type == S3ObjectType.S3_OBJECT_TYPE_DIRECTORY
        assert info.storage_class == S3StorageClass.S3_STORAGE_CLASS_BUCKET
        assert info.version_id == version_id

    @pytest.mark.asyncio
    async def test_info_dir(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_info_dir"
        )
        file = f"{dir_}/{uuid.uuid4()}"

        fs.invalidate_cache()
        with pytest.raises(FileNotFoundError):
            await fs._info(f"s3://{uuid.uuid4()}")

        await fs._pipe_file(file, b"a")
        bucket, key, version_id = fs.parse_path(dir_)
        fs.invalidate_cache()
        info = await fs._info(dir_)
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

    @pytest.mark.asyncio
    async def test_info_file(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_info_file"
        )
        file = f"{dir_}/{uuid.uuid4()}"

        fs.invalidate_cache()
        with pytest.raises(FileNotFoundError):
            await fs._info(file)

        now = datetime.now(timezone.utc)
        await fs._pipe_file(file, b"a")
        bucket, key, version_id = fs.parse_path(file)
        fs.invalidate_cache()
        info = await fs._info(file)
        fs.invalidate_cache()
        ls_info = (await fs._ls(file, detail=True))[0]

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

    @pytest.mark.asyncio
    async def test_find(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_find"
        )
        for i in range(5):
            await fs._pipe_file(f"{dir_}/prefix/test_{i}", bytes(i))
        await fs._touch(f"{dir_}/prefix2")

        result = await fs._find(f"{dir_}/prefix")
        assert len(result) == 5

        result = await fs._find(f"{dir_}/prefix/")
        assert len(result) == 5

        result = await fs._find(dir_, prefix="prefix")
        assert len(result) == 6

        result = await fs._find(f"{dir_}/prefix/test_")
        assert len(result) == 0

        result = await fs._find(f"{dir_}/prefix", prefix="test_")
        assert len(result) == 5

        result = await fs._find(f"{dir_}/prefix/", prefix="test_")
        assert len(result) == 5

        test_1 = await fs._find(f"{dir_}/prefix/test_1")
        assert len(test_1) == 1
        assert test_1[0] == fs._strip_protocol(f"{dir_}/prefix/test_1")

        test_1_detail = await fs._find(f"{dir_}/prefix/test_1", detail=True)
        assert len(test_1_detail) == 1
        assert test_1_detail[
            fs._strip_protocol(f"{dir_}/prefix/test_1")
        ].name == fs._strip_protocol(f"{dir_}/prefix/test_1")
        assert test_1_detail[fs._strip_protocol(f"{dir_}/prefix/test_1")].size == 1

    @pytest.mark.asyncio
    async def test_find_maxdepth(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_find_maxdepth"
        )
        # Create files at different depths
        await fs._touch(f"{dir_}/file0.txt")
        await fs._touch(f"{dir_}/level1/file1.txt")
        await fs._touch(f"{dir_}/level1/level2/file2.txt")
        await fs._touch(f"{dir_}/level1/level2/level3/file3.txt")

        # Test maxdepth=0 (only files in the root)
        result = await fs._find(dir_, maxdepth=0)
        assert len(result) == 1
        assert fs._strip_protocol(f"{dir_}/file0.txt") in result

        # Test maxdepth=1 (files in root and level1)
        result = await fs._find(dir_, maxdepth=1)
        assert len(result) == 2
        assert fs._strip_protocol(f"{dir_}/file0.txt") in result
        assert fs._strip_protocol(f"{dir_}/level1/file1.txt") in result

        # Test maxdepth=2 (files in root, level1, and level2)
        result = await fs._find(dir_, maxdepth=2)
        assert len(result) == 3
        assert fs._strip_protocol(f"{dir_}/level1/level2/file2.txt") in result

        # Test no maxdepth (all files)
        result = await fs._find(dir_)
        assert len(result) == 4

    @pytest.mark.asyncio
    async def test_find_withdirs(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_find_withdirs"
        )
        # Create directory structure with files
        await fs._touch(f"{dir_}/file1.txt")
        await fs._touch(f"{dir_}/subdir1/file2.txt")
        await fs._touch(f"{dir_}/subdir1/subdir2/file3.txt")
        await fs._touch(f"{dir_}/subdir3/file4.txt")

        # Test default behavior (withdirs=False)
        result = await fs._find(dir_)
        assert len(result) == 4  # Only files
        for r in result:
            assert r.endswith(".txt")

        # Test withdirs=True
        result = await fs._find(dir_, withdirs=True)
        assert len(result) > 4  # Files and directories

        # Verify directories are included
        dirs = [r for r in result if not r.endswith(".txt")]
        assert len(dirs) > 0
        assert any("subdir1" in d for d in dirs)
        assert any("subdir2" in d for d in dirs)
        assert any("subdir3" in d for d in dirs)

        # Test withdirs=False explicitly
        result = await fs._find(dir_, withdirs=False)
        assert len(result) == 4  # Only files

    def test_du(self):
        # TODO
        pass

    @pytest.mark.asyncio
    async def test_glob(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_glob"
        )
        path = f"{dir_}/nested/test_{uuid.uuid4()}"
        await fs._touch(path)

        assert fs._strip_protocol(path) not in fs.glob(f"{dir_}/")
        assert fs._strip_protocol(path) not in fs.glob(f"{dir_}/*")
        assert fs._strip_protocol(path) not in fs.glob(f"{dir_}/nested")
        assert fs._strip_protocol(path) not in fs.glob(f"{dir_}/nested/")
        assert fs._strip_protocol(path) in fs.glob(f"{dir_}/nested/*")
        assert fs._strip_protocol(path) in fs.glob(f"{dir_}/nested/test_*")
        assert fs._strip_protocol(path) in fs.glob(f"{dir_}/*/*")

        with pytest.raises(ValueError):  # noqa: PT011
            fs.glob("*")

    @pytest.mark.asyncio
    async def test_exists_bucket(self, fs):
        assert await fs._exists("s3://")
        assert await fs._exists("s3:///")

        path = f"s3://{ENV.s3_staging_bucket}"
        assert await fs._exists(path)

        not_exists_path = f"s3://{uuid.uuid4()}"
        assert not await fs._exists(not_exists_path)

    @pytest.mark.asyncio
    async def test_exists_object(self, fs):
        path = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}"
        assert await fs._exists(path)

        not_exists_path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_exists/{uuid.uuid4()}"
        )
        assert not await fs._exists(not_exists_path)

    @pytest.mark.asyncio
    async def test_rm_file(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_rm_file"
        )
        file = f"{dir_}/{uuid.uuid4()}"
        await fs._touch(file)
        await fs._rm_file(file)

        assert not await fs._exists(file)
        assert not await fs._exists(dir_)

    @pytest.mark.asyncio
    async def test_rm(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_rm"
        )
        file = f"{dir_}/{uuid.uuid4()}"
        await fs._touch(file)
        await fs._rm(file)

        assert not await fs._exists(file)
        assert not await fs._exists(dir_)

    @pytest.mark.asyncio
    async def test_rm_recursive(self, fs):
        dir_ = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_rm_recursive"
        )

        files = [f"{dir_}/{uuid.uuid4()}" for _ in range(10)]
        for f in files:
            await fs._touch(f)

        await fs._rm(dir_)
        for f in files:
            assert await fs._exists(f)
        assert await fs._exists(dir_)

        await fs._rm(dir_, recursive=True)
        for f in files:
            assert not await fs._exists(f)
        assert not await fs._exists(dir_)

    @pytest.mark.asyncio
    async def test_touch(self, fs):
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_touch/{uuid.uuid4()}"
        )
        assert not await fs._exists(path)
        await fs._touch(path)
        assert await fs._exists(path)
        info = await fs._info(path)
        assert info.size == 0

        with fs.open(path, "wb") as f:
            f.write(b"data")
        info = await fs._info(path, refresh=True)
        assert info.size == 4
        await fs._touch(path, truncate=True)
        info = await fs._info(path, refresh=True)
        assert info.size == 0

        with fs.open(path, "wb") as f:
            f.write(b"data")
        info = await fs._info(path, refresh=True)
        assert info.size == 4
        with pytest.raises(ValueError, match="Cannot touch"):
            await fs._touch(path, truncate=False)
        info = await fs._info(path, refresh=True)
        assert info.size == 4

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("base", "exp"),
        [
            (1, 2**10),
            (1, 2**20),
        ],
    )
    async def test_pipe_cat(self, fs, base, exp):
        data = b"a" * (base * exp)
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_pipe_cat/{uuid.uuid4()}"
        )
        await fs._pipe_file(path, data)
        assert await fs._cat_file(path) == data

    @pytest.mark.asyncio
    async def test_cat_ranges(self, fs):
        data = b"1234567890abcdefghijklmnopqrstuvwxyz"
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_cat_ranges/{uuid.uuid4()}"
        )
        await fs._pipe_file(path, data)

        assert await fs._cat_file(path) == data
        assert await fs._cat_file(path, start=5) == data[5:]
        assert await fs._cat_file(path, end=5) == data[:5]
        assert await fs._cat_file(path, start=1, end=-1) == data[1:-1]
        assert await fs._cat_file(path, start=-5) == data[-5:]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("base", "exp"),
        [
            (1, 2**10),
            (1, 2**20),
        ],
    )
    async def test_put(self, fs, base, exp):
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            data = b"a" * (base * exp)
            tmp.write(data)
            tmp.flush()

            rpath = (
                f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
                f"filesystem/test_async_put/{uuid.uuid4()}"
            )
            await fs._put_file(lpath=tmp.name, rpath=rpath)
            tmp.seek(0)
            assert await fs._cat_file(rpath) == tmp.read()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("base", "exp"),
        [
            (1, 2**10),
            (1, 2**20),
        ],
    )
    async def test_put_with_callback(self, fs, base, exp):
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            data = b"a" * (base * exp)
            tmp.write(data)
            tmp.flush()

            rpath = (
                f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
                f"filesystem/test_async_put_with_callback/{uuid.uuid4()}"
            )
            callback = Callback()
            await fs._put_file(lpath=tmp.name, rpath=rpath, callback=callback)
            tmp.seek(0)
            assert await fs._cat_file(rpath) == tmp.read()
            assert callback.size == os.stat(tmp.name).st_size
            assert callback.value == callback.size

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("base", "exp"),
        [
            (1, 2**10),
            (1, 2**20),
        ],
    )
    async def test_upload_cp_file(self, fs, base, exp):
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            data = b"a" * (base * exp)
            tmp.write(data)
            tmp.flush()

            rpath = (
                f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
                f"filesystem/test_async_upload_cp_file/{uuid.uuid4()}"
            )
            await fs._put_file(lpath=tmp.name, rpath=rpath)

            rpath_copy = (
                f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
                f"filesystem/test_async_upload_cp_file_copy/{uuid.uuid4()}"
            )
            await fs._cp_file(path1=rpath, path2=rpath_copy)
            tmp.seek(0)
            assert await fs._cat_file(rpath_copy) == tmp.read()
            assert await fs._cat_file(rpath_copy) == await fs._cat_file(rpath)

    @pytest.mark.asyncio
    async def test_move(self, fs):
        path1 = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_move/{uuid.uuid4()}"
        )
        path2 = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_move/{uuid.uuid4()}"
        )
        data = b"a"
        await fs._pipe_file(path1, data)
        fs.mv(path1, path2)
        assert await fs._cat_file(path2) == data
        assert not await fs._exists(path1)

    @pytest.mark.asyncio
    async def test_get_file(self, fs):
        with tempfile.TemporaryDirectory() as tmp:
            rpath = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}"
            lpath = Path(f"{tmp}/{uuid.uuid4()}")
            callback = Callback()
            await fs._get_file(rpath=rpath, lpath=str(lpath), callback=callback)

            assert lpath.open("rb").read() == await fs._cat_file(rpath)
            assert callback.size == os.stat(lpath).st_size
            assert callback.value == callback.size

    def test_open_returns_aio_s3_file(self, fs):
        path = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}"
        with fs.open(path, "rb") as f:
            assert isinstance(f, AioS3File)
            data = f.read()
        assert data == b"0123456789"

    def test_checksum(self, fs):
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_checksum/{uuid.uuid4()}"
        )
        bucket, key, _ = fs.parse_path(path)

        fs.pipe_file(path, b"foo")
        checksum = fs.checksum(path)
        fs.ls(path)  # caching
        fs._sync_fs._put_object(bucket=bucket, key=key, body=b"bar")
        assert checksum == fs.checksum(path)
        assert checksum != fs.checksum(path, refresh=True)

        fs.pipe_file(path, b"foo")
        checksum = fs.checksum(path)
        fs.ls(path)  # caching
        fs._sync_fs._delete_object(bucket, key)
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
            1 * 2**20,
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
                f"filesystem/test_async_pandas_write_csv/{uuid.uuid4()}.csv"
            )
            df.to_csv(path, index=False)

            actual = pandas.read_csv(path)
            pandas.testing.assert_frame_equal(actual, df)

    def test_sync_wrappers(self, fs):
        """Verify that mirror_sync_methods generates working sync wrappers."""
        actual = fs.ls(f"s3://{ENV.s3_staging_bucket}")
        assert isinstance(actual, list)
        assert len(actual) > 0

        assert fs.exists(f"s3://{ENV.s3_staging_bucket}")

    def test_invalidate_cache(self, fs):
        path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_async_invalidate_cache/{uuid.uuid4()}"
        )
        fs.pipe_file(path, b"data")
        fs.info(path)

        # Cache should be populated
        fs.invalidate_cache(path)
        # Should not raise after cache invalidation
        info = fs.info(path)
        assert info.size == 4


class TestAioS3File:
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
