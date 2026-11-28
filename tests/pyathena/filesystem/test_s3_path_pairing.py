# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import pytest

from pyathena.filesystem.s3_path import S3Path
from pyathena.filesystem.s3_path_pairing import S3PathPairing


def _pairing(pairs):
    # The pairing of a move of the paths as lists.
    return S3PathPairing([p1 for p1, _ in pairs], [p2 for _, p2 in pairs])


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
        assert S3PathPairing(path1, path2).expands is expected

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
        assert (
            S3PathPairing(path1, "s3://bucket/out", recursive, maxdepth).skips_directories
            is expected
        )

    @pytest.mark.parametrize(
        ("path1", "path2", "expected"),
        [
            ("s3://bucket/a", "s3://bucket/b", True),
            # A version ID query is not a glob pattern.
            ("s3://bucket/a?versionId=v1", "s3://bucket/b", True),
            # GH-1469: nor is the slash that ends a version ID a separator,
            # unless the query names no version without it.
            ("s3://bucket/a?versionId=v1/", "s3://bucket/b", True),
            ("s3://bucket/a?versionId=/", "s3://bucket/b", False),
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
        assert S3PathPairing(path1, path2).looks_up_destination is expected

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
            # A list of sources is copied into the destination by name.
            (
                ["s3://bucket/b?versionId=v1"],
                "s3://bucket/d",
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
        assert S3PathPairing(path1, path2).copy_pairs(sources, destination_is_dir) == expected

    def test_copy_pairs_names_path_sources_by_key(self):
        # GH-1403: an S3Path source, such as a listed key, is named after its
        # bucket and key as they are, while a source string with a version ID
        # is named after its key.
        listed = S3Path("bucket", "d/x?versionId=null")
        pairing = S3PathPairing("s3://bucket/d", "out", recursive=True)
        assert pairing.copy_pairs(["bucket/d", listed], False) == [
            ("bucket/d", "out"),
            (listed, "out/x?versionId=null"),
        ]

    @pytest.mark.parametrize(
        ("path1", "path2", "sources", "destination_is_dir", "expected"),
        [
            # The destination root parsed, with the suffix of the listed key.
            (
                "s3://bucket/d",
                "s3://bucket/out",
                ["bucket/d", S3Path("bucket", "d/x?versionId=null")],
                False,
                [
                    ("bucket/d", "s3://bucket/out"),
                    (
                        S3Path("bucket", "d/x?versionId=null"),
                        S3Path("bucket", "out/x?versionId=null"),
                    ),
                ],
            ),
            # A bucket root, given with a trailing slash.
            (
                "s3://bucket/d/*",
                "s3://other/",
                [S3Path("bucket", "d/x?versionId=null")],
                None,
                [(S3Path("bucket", "d/x?versionId=null"), S3Path("other", "x?versionId=null"))],
            ),
            # A list of sources, copied into the destination by name.
            (
                ["s3://bucket/d"],
                "s3://bucket/out",
                ["bucket/d", S3Path("bucket", "d/x?versionId=null")],
                None,
                [
                    ("bucket/d", "s3://bucket/out/d"),
                    (
                        S3Path("bucket", "d/x?versionId=null"),
                        S3Path("bucket", "out/x?versionId=null"),
                    ),
                ],
            ),
        ],
    )
    def test_s3_copy_pairs(self, path1, path2, sources, destination_is_dir, expected):
        # GH-1403: the destination of an S3Path source is an S3Path, never a
        # string that parsing would read as a version.
        pairing = S3PathPairing(path1, path2, recursive=True)
        assert pairing.s3_copy_pairs(sources, destination_is_dir) == expected

    def test_s3_copy_pairs_versioned_destination(self):
        pairing = S3PathPairing("s3://bucket/d", "s3://bucket/out?versionId=v1", recursive=True)
        with pytest.raises(ValueError, match="versioned"):
            pairing.s3_copy_pairs([S3Path("bucket", "d/x")], False)

    def test_copy_pairs_needs_destination_lookup(self):
        with pytest.raises(ValueError, match="destination_is_dir"):
            S3PathPairing("s3://bucket/a", "s3://bucket/b").copy_pairs(["bucket/a"])

    def test_copy_pairs_needs_sources(self):
        # Not passing the expansion is not the same as expanding to nothing.
        with pytest.raises(ValueError, match="sources"):
            S3PathPairing("s3://bucket/a/", "s3://bucket/b/").copy_pairs()

    @pytest.mark.parametrize(
        ("enabled_buckets", "expected"),
        [
            # The "null" version of a key moved onto the key stays in place.
            ((), []),
            (("enabled",), [("enabled/key?versionId=null", "enabled/key")]),
            (("other",), [("other/key?versionId=null", "other/key")]),
        ],
    )
    def test_move_pairs_null_version_bucket_state(self, enabled_buckets, expected):
        moved = ("enabled/d/a", "s3://enabled/z")
        pairs = [
            ("enabled/key?versionId=null", "enabled/key"),
            ("other/key?versionId=null", "other/key"),
            ("enabled/unchanged?versionId=null", "s3://enabled/unchanged?versionID=null"),
            # An ordinary move.
            moved,
        ]
        pairing = _pairing(pairs)

        assert pairing.conflict_candidates(pairs, versioning_enabled_buckets=enabled_buckets) == []
        assert pairing.move_pairs(pairs, versioning_enabled_buckets=enabled_buckets) == [
            *expected,
            moved,
        ]
        assert pairing.move_pairs(pairs) == [moved]

    def test_move_pairs_distinguishes_null_version_source(self):
        pairs = [
            ("bucket/key?versionId=null", "bucket/out"),
            ("bucket/other", "bucket/key"),
        ]
        pairing = _pairing(pairs)

        with pytest.raises(ValueError, match="another path"):
            pairing.move_pairs(pairs, missing=set())
        assert pairing.move_pairs(pairs, versioning_enabled_buckets={"bucket"}) == pairs

    @pytest.mark.parametrize("method", ["conflict_candidates", "move_pairs"])
    def test_move_bucket_names_are_a_collection(self, method):
        pairs = [("logs/key?versionId=null", "logs/key")]
        with pytest.raises(TypeError, match="collection of bucket names"):
            getattr(_pairing(pairs), method)(pairs, versioning_enabled_buckets="logs-archive")

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
            _pairing(pairs).move_pairs(pairs, missing=set())

    def test_move_pairs_directory_without_object(self):
        # A source without a version may be a directory, with or without
        # another source below it; one without an object at its key is not
        # copied, so its destination does not conflict.
        pairs = [
            ("s3://bucket/d", "s3://bucket/e"),
            ("s3://bucket/d/x", "s3://bucket/e"),
            ("s3://bucket/e/y", "s3://bucket/out"),
        ]

        assert _pairing(pairs).conflict_candidates(pairs) == ["bucket/d", "bucket/d/x"]
        with pytest.raises(ValueError, match="missing"):
            _pairing(pairs).move_pairs(pairs)
        with pytest.raises(ValueError, match="same destination"):
            _pairing(pairs).move_pairs(pairs, missing=set())
        assert _pairing(pairs).move_pairs(pairs, missing={"bucket/d"}) == pairs
        assert _pairing(pairs).move_pairs(pairs, missing={"bucket/d/x"}) == pairs
        # The missing sources can be given in any form that names them.
        assert _pairing(pairs).move_pairs(pairs, missing={"s3://bucket/d"}) == pairs

    def test_move_pairs_keeps_listed_keys_apart_from_versions(self):
        # GH-1403: the S3Path of a key that ends in a null version query is
        # compared as that key, not as the null version, which is the key
        # without the query.
        pairs = [
            (S3Path("bucket", "a/x"), S3Path("bucket", "b/x")),
            (S3Path("bucket", "a/x?versionId=null"), S3Path("bucket", "b/x?versionId=null")),
        ]
        assert _pairing(pairs).move_pairs(pairs) == pairs

    def test_move_pairs_path_conflict_candidates(self):
        # GH-1403: the candidates of S3Path sources are S3Path objects, and
        # missing ones may be given as such; a key that ends in what looks
        # like a version ID query is no version.
        directory = S3Path("bucket", "d?versionId=v1")
        listed = S3Path("bucket", "d?versionId=v1/x")
        pairs = [(directory, S3Path("bucket", "e")), (listed, S3Path("bucket", "e"))]
        assert _pairing(pairs).conflict_candidates(pairs) == [directory, listed]
        assert _pairing(pairs).move_pairs(pairs, missing={directory}) == pairs

    def test_move_pairs_missing_is_a_collection(self):
        pairs = [("s3://bucket/d", "s3://bucket/e"), ("s3://bucket/d/x", "s3://bucket/e")]
        with pytest.raises(TypeError, match="collection"):
            _pairing(pairs).move_pairs(pairs, missing="bucket/d")

    def test_conflict_candidates_one_per_pair(self):
        # A source given twice is looked up twice, as each pair is checked.
        pairs = [
            ("s3://bucket/d", "s3://bucket/out"),
            ("s3://bucket/d", "s3://bucket/out"),
            ("s3://bucket/d/x", "s3://bucket/x"),
        ]
        assert _pairing(pairs).conflict_candidates(pairs) == ["bucket/d", "bucket/d"]

    def test_move_pairs_version_names_an_object(self):
        # A version is never taken for a directory, even with another source
        # below its key, so it is not looked up.
        pairs = [
            ("s3://bucket/d?versionId=null", "s3://bucket/out"),
            ("s3://bucket/d/x", "s3://bucket/x"),
            ("s3://bucket/a", "s3://bucket/out"),
        ]

        assert _pairing(pairs).conflict_candidates(pairs) == ["bucket/a"]
        with pytest.raises(ValueError, match="same destination"):
            _pairing(pairs).move_pairs(pairs, missing=set())

    def test_move_pairs_version_of_missing_directory_key(self):
        # The "null" version of a key without a current object still names an
        # object, so it writes its destination although the key is missing.
        pairs = [
            ("src/d", "dst/out"),
            ("src/d?versionId=null", "dst/out"),
            ("src/a", "dst/out"),
            ("src/d/x", "dst/x"),
        ]

        assert _pairing(pairs).conflict_candidates(pairs) == ["src/d", "src/a"]
        with pytest.raises(ValueError, match="same destination"):
            _pairing(pairs).move_pairs(pairs, missing={"src/d"})

    def test_delete_paths(self):
        assert S3PathPairing.delete_paths(
            ["s3://bucket/d", "s3://bucket/b?versionId=v1", "s3://bucket/c"]
        ) == (["s3://bucket/b?versionId=v1"], ["s3://bucket/d", "s3://bucket/c"])

    @pytest.mark.parametrize("path", ["s3://bucket", "s3://bucket/", "s3://bucket//"])
    def test_delete_paths_bucket(self, path):
        with pytest.raises(ValueError, match="Cannot delete the bucket"):
            S3PathPairing.delete_paths(path)
