# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""fsspec's conformance tests, run against the S3 filesystems on real S3."""

import contextlib
import functools
import posixpath
import uuid

import fsspec.tests.abstract as abstract
import pytest

from pyathena.filesystem.s3 import S3FileSystem
from pyathena.filesystem.s3_async import AioS3FileSystem
from tests import ENV
from tests.pyathena.conftest import connect


def _xfail_missing_ls(test):
    """Mark an inherited conformance test as failing on GH-1330.

    The test is expected to fail with pytest's ``Failed``, as the
    ``pytest.raises(FileNotFoundError)`` around ``ls()`` does when nothing is
    raised, so other errors still fail it. The mark goes on a wrapper, so the
    test function of fsspec's class stays unmarked.

    Args:
        test: The test function of fsspec's abstract class.

    Returns:
        The wrapped test, expected to fail.
    """

    @functools.wraps(test)
    def wrapper(*args, **kwargs):
        return test(*args, **kwargs)

    return pytest.mark.xfail(
        reason="GH-1330: ls() of a missing key prefix returns [] instead of raising "
        "FileNotFoundError",
        raises=pytest.fail.Exception,
        strict=True,
    )(wrapper)


class S3Fixtures(abstract.AbstractFixtures):
    @pytest.fixture(scope="class")
    def fs(self):
        return S3FileSystem(connect())

    @pytest.fixture
    def fs_join(self):
        # S3 keys use "/" on every platform.
        return posixpath.join

    @pytest.fixture
    def fs_path(self, fs):
        # Each test writes below its own prefix, so parallel tests do not share
        # objects.
        path = (
            f"{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/"
            f"filesystem/test_fsspec_conformance/{uuid.uuid4()}"
        )
        yield path
        # fsspec's fixtures remove only their own scenarios, and
        # test_get_directory_recursive leaves its source below the path.
        with contextlib.suppress(FileNotFoundError):
            fs.rm(path, recursive=True)

    @pytest.fixture
    def supports_empty_directories(self):
        return False


# AioS3FileSystem runs pipe_file() and a small open() through the code of
# S3FileSystem, so only its copy, get, and put have classes of their own.
class AioS3Fixtures(S3Fixtures):
    @pytest.fixture(scope="class")
    def fs(self):
        fs = AioS3FileSystem(connection=connect())
        yield fs
        fs.close()


class TestS3FileSystemCopy(abstract.AbstractCopyTests, S3Fixtures):
    test_copy_directory_to_new_directory = _xfail_missing_ls(
        abstract.AbstractCopyTests.test_copy_directory_to_new_directory
    )


class TestS3FileSystemGet(abstract.AbstractGetTests, S3Fixtures):
    pass


class TestS3FileSystemPut(abstract.AbstractPutTests, S3Fixtures):
    test_put_directory_to_new_directory = _xfail_missing_ls(
        abstract.AbstractPutTests.test_put_directory_to_new_directory
    )


class TestS3FileSystemPipe(abstract.AbstractPipeTests, S3Fixtures):
    pass


class TestS3FileSystemOpen(abstract.AbstractOpenTests, S3Fixtures):
    pass


class TestAioS3FileSystemCopy(abstract.AbstractCopyTests, AioS3Fixtures):
    test_copy_directory_to_new_directory = _xfail_missing_ls(
        abstract.AbstractCopyTests.test_copy_directory_to_new_directory
    )


class TestAioS3FileSystemGet(abstract.AbstractGetTests, AioS3Fixtures):
    pass


class TestAioS3FileSystemPut(abstract.AbstractPutTests, AioS3Fixtures):
    test_put_directory_to_new_directory = _xfail_missing_ls(
        abstract.AbstractPutTests.test_put_directory_to_new_directory
    )
