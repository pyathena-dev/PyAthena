# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import pytest

from pyathena.filesystem.s3_path_pairing import S3PathPairing


class TestS3PathPairing:
    @pytest.mark.parametrize(
        ("path1", "path2", "expected"),
        [
            ("s3://bucket/a", "s3://bucket/b", True),
            (["s3://bucket/a"], "s3://bucket/b", True),
            ("s3://bucket/a", ["s3://bucket/b"], True),
            # Lists are paired as given.
            (["s3://bucket/a"], ["s3://bucket/b"], False),
        ],
    )
    def test_expands(self, path1, path2, expected):
        assert S3PathPairing.expands(path1, path2) is expected

    @pytest.mark.parametrize(
        ("path1", "recursive", "maxdepth", "expected"),
        [
            ("s3://bucket/d", False, None, True),
            ("s3://bucket/d", True, None, False),
            ("s3://bucket/d", True, 1, True),
            (["s3://bucket/d"], False, None, False),
        ],
    )
    def test_skips_directories(self, path1, recursive, maxdepth, expected):
        assert S3PathPairing.skips_directories(path1, recursive, maxdepth) is expected

    @pytest.mark.parametrize(
        ("path1", "path2", "expected"),
        [
            ("s3://bucket/a", "s3://bucket/b", True),
            # A version ID query is not a glob pattern.
            ("s3://bucket/a?versionId=v1", "s3://bucket/b", True),
            # A trailing slash decides the pairing without a lookup.
            ("s3://bucket/a/", "s3://bucket/b", False),
            ("s3://bucket/a", "s3://bucket/b/", False),
            # So does a glob pattern.
            ("s3://bucket/a*", "s3://bucket/b", False),
            (["s3://bucket/a"], "s3://bucket/b", False),
            ("s3://bucket/a", ["s3://bucket/b"], False),
        ],
    )
    def test_looks_up_destination(self, path1, path2, expected):
        assert S3PathPairing.looks_up_destination(path1, path2) is expected

    @pytest.mark.parametrize(
        ("path1", "path2", "sources", "destination_is_dir", "expected"),
        [
            # The directory and the objects below it, as fsspec's copy()
            # pairs them.
            (
                "s3://bucket/d",
                "s3://bucket/out",
                ["bucket/d", "bucket/d/a", "bucket/d/b"],
                False,
                [
                    ("bucket/d", "s3://bucket/out"),
                    ("bucket/d/a", "s3://bucket/out/a"),
                    ("bucket/d/b", "s3://bucket/out/b"),
                ],
            ),
            # A version is copied into a directory under its key,
            (
                "s3://bucket/b?versionId=v1",
                "s3://bucket/d",
                ["bucket/b?versionId=v1"],
                True,
                [("bucket/b?versionId=v1", "s3://bucket/d/b")],
            ),
            # or to the destination itself.
            (
                "s3://bucket/b?versionId=v1",
                "s3://bucket/c",
                ["bucket/b?versionId=v1"],
                False,
                [("bucket/b?versionId=v1", "s3://bucket/c")],
            ),
            # A trailing slash needs no lookup.
            (
                "s3://bucket/b?versionId=v1",
                "s3://bucket/d/",
                ["bucket/b?versionId=v1"],
                None,
                [("bucket/b?versionId=v1", "s3://bucket/d/b")],
            ),
            # Lists are paired as given, up to the end of the shorter one.
            (
                ["s3://bucket/a", "s3://bucket/b", "s3://bucket/c"],
                ["s3://bucket/x", "s3://bucket/y"],
                (),
                None,
                [("s3://bucket/a", "s3://bucket/x"), ("s3://bucket/b", "s3://bucket/y")],
            ),
            # Nothing to copy.
            ("s3://bucket/d", "s3://bucket/out", [], None, []),
        ],
    )
    def test_copy_pairs(self, path1, path2, sources, destination_is_dir, expected):
        assert S3PathPairing.copy_pairs(path1, path2, sources, destination_is_dir) == expected

    def test_copy_pairs_needs_destination_lookup(self):
        with pytest.raises(ValueError, match="destination_is_dir"):
            S3PathPairing.copy_pairs("s3://bucket/a", "s3://bucket/b", ["bucket/a"])

    def test_copy_pairs_needs_sources(self):
        # Not passing the expansion is not the same as expanding to nothing.
        with pytest.raises(ValueError, match="sources"):
            S3PathPairing.copy_pairs("s3://bucket/a/", "s3://bucket/b/")

    def test_move_pairs(self):
        # The "null" version of a key moved onto the key stays in place.
        pairs = [("s3://bucket/b?versionId=null", "s3://bucket/b"), ("bucket/d/a", "s3://bucket/z")]

        assert S3PathPairing.conflict_candidates(pairs) == []
        assert S3PathPairing.move_pairs(pairs) == [("bucket/d/a", "s3://bucket/z")]

    @pytest.mark.parametrize(
        ("pairs", "match"),
        [
            (
                [("s3://bucket/b", "s3://bucket/o"), ("s3://bucket/z", "s3://bucket/o")],
                "same destination",
            ),
            (
                [("s3://bucket/b", "s3://bucket/z"), ("s3://bucket/z", "s3://bucket/n")],
                "another path that is moved",
            ),
        ],
    )
    def test_move_pairs_conflicts(self, pairs, match):
        with pytest.raises(ValueError, match=match):
            S3PathPairing.move_pairs(pairs)

    def test_move_pairs_directory_without_object(self):
        # A source with another source below it may be a directory; one
        # without an object at its key is not copied, so its destination
        # does not conflict.
        pairs = [
            ("s3://bucket/d", "s3://bucket/e"),
            ("s3://bucket/d/x", "s3://bucket/e"),
            ("s3://bucket/e/y", "s3://bucket/out"),
        ]

        assert S3PathPairing.conflict_candidates(pairs) == ["bucket/d"]
        with pytest.raises(ValueError, match="missing"):
            S3PathPairing.move_pairs(pairs)
        with pytest.raises(ValueError, match="same destination"):
            S3PathPairing.move_pairs(pairs, missing=set())
        assert S3PathPairing.move_pairs(pairs, missing={"bucket/d"}) == pairs
        # The missing sources can be given in any form that names them.
        assert S3PathPairing.move_pairs(pairs, missing={"s3://bucket/d"}) == pairs

    def test_move_pairs_version_names_an_object(self):
        # A version is never taken for a directory.
        pairs = [
            ("s3://bucket/d?versionId=null", "s3://bucket/out"),
            ("s3://bucket/d/x", "s3://bucket/x"),
            ("s3://bucket/a", "s3://bucket/out"),
        ]

        assert S3PathPairing.conflict_candidates(pairs) == []
        with pytest.raises(ValueError, match="same destination"):
            S3PathPairing.move_pairs(pairs)

    def test_move_pairs_version_of_missing_directory_key(self):
        # The "null" version of a key without a current object still names an
        # object, so it writes its destination although the key is missing.
        pairs = [
            ("src/d", "dst/out"),
            ("src/d?versionId=null", "dst/out"),
            ("src/a", "dst/out"),
            ("src/d/x", "dst/x"),
        ]

        assert S3PathPairing.conflict_candidates(pairs) == ["src/d"]
        with pytest.raises(ValueError, match="same destination"):
            S3PathPairing.move_pairs(pairs, missing={"src/d"})

    def test_delete_paths(self):
        assert S3PathPairing.delete_paths(
            ["s3://bucket/d", "s3://bucket/b?versionId=v1", "s3://bucket/c"]
        ) == (["s3://bucket/b?versionId=v1"], ["s3://bucket/d", "s3://bucket/c"])

    @pytest.mark.parametrize("path", ["s3://bucket", "s3://bucket/", "s3://bucket//"])
    def test_delete_paths_bucket(self, path):
        with pytest.raises(ValueError, match="Cannot delete the bucket"):
            S3PathPairing.delete_paths(path)
