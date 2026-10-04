# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from unittest import mock

import pytest

from pyathena.filesystem.s3_async import AioS3FileSystem
from pyathena.filesystem.s3_path_pairing import S3PathPairing
from tests.pyathena.filesystem import test_s3


def _make_fs(keys):
    # An S3FileSystem whose requests are answered from the keys of "bucket".
    fs = test_s3.TestS3FileSystem._make_fs()
    test_s3.TestS3FileSystem._serve_keys(fs, keys)
    return fs


class TestS3PathPairing:
    def test_filesystems_share_one_pairing(self):
        fs = AioS3FileSystem(connection=mock.MagicMock(), skip_instance_cache=True)
        assert isinstance(fs.pairing, S3PathPairing)
        assert fs.pairing is fs._sync_fs.pairing
        assert fs._sync_fs.pairing is fs._sync_fs.pairing

    @pytest.mark.parametrize(
        ("path1", "path2", "kwargs", "expected"),
        [
            # The directory and the objects below it, as fsspec's copy() pairs
            # them.
            (
                "s3://bucket/d",
                "s3://bucket/out",
                {"recursive": True},
                [
                    ("bucket/d", "s3://bucket/out"),
                    ("bucket/d/a", "s3://bucket/out/a"),
                    ("bucket/d/b", "s3://bucket/out/b"),
                ],
            ),
            # A version is copied to a destination named after its key.
            (
                "s3://bucket/b?versionId=v1",
                "s3://bucket/d/",
                {},
                [("bucket/b?versionId=v1", "s3://bucket/d/b")],
            ),
            # Without recursive, a directory is not copied.
            ("s3://bucket/d", "s3://bucket/out", {}, []),
            # Lists are paired as given.
            (
                ["s3://bucket/b", "s3://bucket/d/a"],
                ["s3://bucket/x", "s3://bucket/y"],
                {},
                [("s3://bucket/b", "s3://bucket/x"), ("s3://bucket/d/a", "s3://bucket/y")],
            ),
        ],
    )
    def test_copy_pairs(self, path1, path2, kwargs, expected):
        fs = _make_fs({"d/a", "d/b", "b"})
        assert fs.pairing.copy_pairs(path1, path2, **kwargs) == expected

    def test_copy_pairs_isdir(self):
        # The given isdir, such as the local filesystem's for get(), decides
        # whether the destination is a directory.
        fs = _make_fs({"b"})
        isdir = mock.MagicMock(return_value=True)

        pairs = fs.pairing.copy_pairs("s3://bucket/b?versionId=v1", "/tmp/out", isdir=isdir)

        assert pairs == [("bucket/b?versionId=v1", "/tmp/out/b")]
        isdir.assert_called_once_with("/tmp/out")

    def test_move_pairs(self):
        # The "null" version of a key moved onto the key stays in place.
        fs = _make_fs({"b", "d/a"})

        pairs = fs.pairing.move_pairs(
            ["s3://bucket/b?versionId=null", "s3://bucket/d/a"],
            ["s3://bucket/b", "s3://bucket/z"],
        )

        assert pairs == [("s3://bucket/d/a", "s3://bucket/z")]

    @pytest.mark.parametrize(
        ("path2", "match"),
        [
            (["s3://bucket/o", "s3://bucket/o"], "same destination"),
            (["s3://bucket/z", "s3://bucket/n"], "another path that is moved"),
        ],
    )
    def test_move_pairs_conflicts(self, path2, match):
        fs = _make_fs({"b", "z"})
        with pytest.raises(ValueError, match=match):
            fs.pairing.move_pairs(["s3://bucket/b", "s3://bucket/z"], path2)

    def test_delete_paths(self):
        # The versions are deleted as given, before the expanded paths.
        fs = _make_fs({"d/a", "d/b"})

        paths = fs.pairing.delete_paths(
            ["s3://bucket/d", "s3://bucket/b?versionId=v1"], recursive=True
        )

        assert paths == ["s3://bucket/b?versionId=v1", "bucket/d", "bucket/d/a", "bucket/d/b"]

    @pytest.mark.parametrize("path", ["s3://bucket", "s3://bucket/", "s3://bucket//"])
    def test_delete_paths_bucket(self, path):
        fs = _make_fs(set())
        with pytest.raises(ValueError, match="Cannot delete the bucket"):
            fs.pairing.delete_paths(path)
        fs._call.assert_not_called()
