# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from pathlib import Path

import pytest

from pyathena.filesystem.s3_path import S3Path


class TestS3Path:
    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("s3://bucket", S3Path("bucket")),
            ("s3://bucket/", S3Path("bucket")),
            ("s3a://bucket", S3Path("bucket")),
            ("s3a://bucket/", S3Path("bucket")),
            ("bucket", S3Path("bucket")),
            ("bucket/", S3Path("bucket")),
            ("s3://bucket/path/to/obj", S3Path("bucket", "path/to/obj")),
            ("s3a://bucket/path/to/obj", S3Path("bucket", "path/to/obj")),
            ("bucket/path/to/obj", S3Path("bucket", "path/to/obj")),
            ("bucket/path/to/dir/", S3Path("bucket", "path/to/dir/")),
            ("bucket//", S3Path("bucket", "/")),
            ("s3://bucket/obj?versionId=v1", S3Path("bucket", "obj", "v1")),
            ("s3a://bucket/obj?versionId=v1", S3Path("bucket", "obj", "v1")),
            ("bucket/obj?versionId=v1", S3Path("bucket", "obj", "v1")),
            ("bucket/obj?versionID=v1", S3Path("bucket", "obj", "v1")),
            ("bucket/obj?versionid=v1", S3Path("bucket", "obj", "v1")),
            ("bucket/obj?version_id=v1", S3Path("bucket", "obj", "v1")),
            ("bucket?versionId=v1", S3Path("bucket", None, "v1")),
            # Only a trailing version ID query is a version; any other "?" is
            # part of the key.
            ("bucket/dir/what?.txt", S3Path("bucket", "dir/what?.txt")),
            ("bucket/obj?x=1", S3Path("bucket", "obj?x=1")),
            ("s3://bucket/obj?foo=bar", S3Path("bucket", "obj?foo=bar")),
            ("s3a://bucket/obj?foo=bar", S3Path("bucket", "obj?foo=bar")),
            ("bucket/a?b?versionId=v1", S3Path("bucket", "a?b", "v1")),
            ("bucket/obj?versionId=", S3Path("bucket", "obj?versionId=")),
            ("bucket/obj?versionId=a?versionId=b", S3Path("bucket", "obj?versionId=a", "b")),
            ("bucket/?", S3Path("bucket", "?")),
            # A version right after the bucket names a version of the bucket path.
            ("bucket/?versionId=v1", S3Path("bucket", None, "v1")),
            # Keys may contain newlines, also at the end.
            ("bucket/a\nb", S3Path("bucket", "a\nb")),
            ("bucket/a\n", S3Path("bucket", "a\n")),
            ("bucket/a\n?versionId=v1", S3Path("bucket", "a\n", "v1")),
        ],
    )
    def test_parse(self, path, expected):
        assert S3Path.parse(path) == expected

    @pytest.mark.parametrize(
        "path",
        [
            "",
            "s3://",
            "http://bucket",
            "bucket?x=1",
            "s3://bucket?",
            "s3://bucket?foo=bar",
            "s3a://bucket?",
            "s3a://bucket?foo=bar",
        ],
    )
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
        ("path", "expected"),
        [
            (S3Path("bucket--usw2-az1--x-s3", "key"), True),
            (S3Path("bucket--usw2-az1--x-s3"), True),
            (S3Path("bucket", "key--x-s3"), False),
            (S3Path("bucket--x-s3-other"), False),
        ],
    )
    def test_is_directory_bucket(self, path, expected):
        assert path.is_directory_bucket is expected

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

    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("bucket/key", ("bucket/key", None)),
            ("s3://bucket/key?versionId=v1", ("s3://bucket/key", "v1")),
            ("bucket/dir/?version_id=v1", ("bucket/dir/", "v1")),
            ("bucket/what?.txt", ("bucket/what?.txt", None)),
            ("bucket/a?b?versionID=v1", ("bucket/a?b", "v1")),
            # Not an S3 path, which parse() would reject.
            ("", ("", None)),
            ("name?versionid=v1", ("name", "v1")),
        ],
    )
    def test_split_version_id(self, path, expected):
        assert S3Path.split_version_id(path) == expected

    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("bucket/key", False),
            ("bucket/key?versionId=v1", True),
            ("bucket/what?.txt", False),
            (Path("bucket/key"), False),
            (Path("bucket/key?version_id=v1"), True),
        ],
    )
    def test_has_version_id(self, path, expected):
        assert S3Path.has_version_id(path) is expected

    def test_with_version_id(self):
        path = S3Path("bucket", "key", "v1")
        assert path.with_version_id("v2") == S3Path("bucket", "key", "v2")
        assert path.with_version_id(None) == S3Path("bucket", "key")
        # The path is a frozen value.
        assert path == S3Path("bucket", "key", "v1")
        with pytest.raises(AttributeError):
            path.key = "other"  # type: ignore[misc]

    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            # A write to the key replaces its "null" version.
            (S3Path("bucket", "key", "null"), S3Path("bucket", "key")),
            # Any other path is its own target.
            (S3Path("bucket", "key", "v1"), S3Path("bucket", "key", "v1")),
            (S3Path("bucket", "key"), S3Path("bucket", "key")),
            (S3Path("bucket"), S3Path("bucket")),
        ],
    )
    def test_target(self, path, expected):
        assert path.target == expected
