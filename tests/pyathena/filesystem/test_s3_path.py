# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import pytest

from pyathena.filesystem.s3_path import S3Path


class TestS3Path:
    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("s3://bucket", S3Path("bucket")),
            ("s3a://bucket/", S3Path("bucket")),
            ("bucket", S3Path("bucket")),
            ("s3://bucket/path/to/obj", S3Path("bucket", "path/to/obj")),
            ("s3a://bucket/path/to/obj", S3Path("bucket", "path/to/obj")),
            ("bucket/path/to/dir/", S3Path("bucket", "path/to/dir/")),
            ("bucket//", S3Path("bucket", "/")),
            ("s3://bucket/obj?versionId=v1", S3Path("bucket", "obj", "v1")),
            ("bucket/obj?versionID=v1", S3Path("bucket", "obj", "v1")),
            ("bucket/obj?versionid=v1", S3Path("bucket", "obj", "v1")),
            ("bucket/obj?version_id=v1", S3Path("bucket", "obj", "v1")),
            ("bucket?versionId=v1", S3Path("bucket", None, "v1")),
        ],
    )
    def test_parse(self, path, expected):
        assert S3Path.parse(path) == expected

    @pytest.mark.parametrize("path", ["", "s3://", "http://bucket", "bucket/obj?x=1"])
    def test_parse_invalid(self, path):
        with pytest.raises(ValueError, match="Invalid S3 path format"):
            S3Path.parse(path)

    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            (S3Path("bucket"), True),
            (S3Path("bucket", "/"), True),
            (S3Path("bucket", "//"), True),
            (S3Path("bucket", "key"), False),
            (S3Path("bucket", "dir/"), False),
        ],
    )
    def test_is_bucket(self, path, expected):
        assert path.is_bucket is expected

    @pytest.mark.parametrize(
        ("path", "name", "string", "uri"),
        [
            (S3Path("bucket"), "bucket", "bucket", "s3://bucket"),
            (S3Path("bucket", "dir/"), "bucket/dir/", "bucket/dir/", "s3://bucket/dir/"),
            (
                S3Path("bucket", "key", "v1"),
                "bucket/key",
                "bucket/key?versionId=v1",
                "s3://bucket/key?versionId=v1",
            ),
        ],
    )
    def test_names(self, path, name, string, uri):
        assert path.name == name
        assert str(path) == string
        assert path.uri == uri

    def test_str_spells_the_version_query_as_version_id(self):
        assert str(S3Path.parse("s3a://bucket/key?version_id=v1")) == "bucket/key?versionId=v1"

    def test_with_version_id(self):
        path = S3Path("bucket", "key", "v1")
        assert path.with_version_id("v2") == S3Path("bucket", "key", "v2")
        assert path.with_version_id(None) == S3Path("bucket", "key")
        # The path is a frozen value.
        assert path == S3Path("bucket", "key", "v1")
        with pytest.raises(AttributeError):
            path.key = "other"  # type: ignore[misc]
