# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""The pairing of the paths that the S3 filesystem copies, moves and deletes."""

from __future__ import annotations

from collections import Counter
from collections.abc import Collection, Sequence
from glob import has_magic

from fsspec.implementations.local import trailing_sep
from fsspec.utils import other_paths

from pyathena.filesystem.s3_path import S3Path


class S3PathPairing:
    """The rules that pair the paths of ``copy()``, ``get()``, ``mv()`` and ``rm()``.

    The sources are paired with their destinations as fsspec's ``copy()``
    pairs them, except that a path with a version ID names that version of an
    object: it is not a glob pattern, and its destination is named after its
    key without the version. A move compares the paths by what they name
    (see :attr:`~pyathena.filesystem.s3_path.S3Path.target`).

    The rules are pure functions of the paths and of the lookups that they
    need, which the caller makes: the filesystems ask :meth:`expands`,
    :meth:`skips_directories`, :meth:`looks_up_destination` and
    :meth:`conflict_candidates` what to look up, expand and look up the paths
    with their own requests and cache, and pass the results in. A lookup that
    a rule needs and that is not passed raises ``ValueError``.

    Example:
        >>> sources = fs.expand_path("s3://bucket/dir", recursive=True)
        >>> S3PathPairing.copy_pairs("s3://bucket/dir", "s3://bucket/copy/", sources)
    """

    @staticmethod
    def expands(path1: str | list[str], path2: str | list[str]) -> bool:
        """Return whether a copy expands its sources.

        Args:
            path1: Source path, glob pattern, or list of them.
            path2: Destination path, or list of paths.

        Returns:
            False if both are lists, which are paired as given, without
            expansion or lookups.
        """
        return not (isinstance(path1, list) and isinstance(path2, list))

    @staticmethod
    def skips_directories(path1: str | list[str], recursive: bool, maxdepth: int | None) -> bool:
        """Return whether a copy leaves out the directories among its expanded sources.

        A directory is a source that ends with a slash or that the filesystem
        reports as a directory.

        Args:
            path1: Source path, glob pattern, or list of them.
            recursive: Whether the copy includes the contents of directories.
            maxdepth: Maximum depth of the expansion.

        Returns:
            True for a string source copied without ``recursive``, or with a
            ``maxdepth``.
        """
        return isinstance(path1, str) and (not recursive or maxdepth is not None)

    @staticmethod
    def looks_up_destination(path1: str | list[str], path2: str | list[str]) -> bool:
        """Return whether the pairing of a copy depends on its destination being a directory.

        Args:
            path1: Source path, glob pattern, or list of them.
            path2: Destination path, or list of paths.

        Returns:
            True for a string source that is neither a glob pattern nor ends
            with a slash, copied to a string destination that does not end
            with a slash. :meth:`copy_pairs` then needs
            ``destination_is_dir``.
        """
        return (
            isinstance(path1, str)
            and not S3PathPairing._is_glob(path1)
            and not trailing_sep(path1)
            and isinstance(path2, str)
            and not trailing_sep(path2)
        )

    @staticmethod
    def copy_pairs(
        path1: str | list[str],
        path2: str | list[str],
        sources: Sequence[str] | None = None,
        destination_is_dir: bool | None = None,
    ) -> list[tuple[str, str]]:
        """Pair the sources of a copy with their destinations as fsspec's ``copy()`` does.

        Args:
            path1: Source S3 path, glob pattern, or list of them, as given to
                the copy.
            path2: Destination path, or a list of paths: as many as the
                sources, or, when ``path1`` is a list, its destinations.
            sources: The expansion of ``path1``, without the directories that
                :meth:`skips_directories` leaves out; needed unless both
                ``path1`` and ``path2`` are lists, which are paired without
                it.
            destination_is_dir: Whether ``path2`` is a directory; needed when
                :meth:`looks_up_destination` is true and there are sources.

        Returns:
            The sources and their destinations, which keep the form of
            ``path2``. When both ``path1`` and ``path2`` are lists, they are
            paired as given, up to the end of the shorter list, as in
            fsspec's ``copy()``. Empty if there are no sources.

        Raises:
            ValueError: If ``sources`` or ``destination_is_dir`` is needed and
                None.
        """
        if not S3PathPairing.expands(path1, path2):
            return list(zip(path1, path2, strict=False))
        if sources is None:
            raise ValueError("sources is needed to pair the paths.")
        if not sources:
            return []
        looks_up = S3PathPairing.looks_up_destination(path1, path2)
        if destination_is_dir is None and looks_up:
            raise ValueError("destination_is_dir is needed to pair the paths.")
        source_is_str = isinstance(path1, str)
        glob = isinstance(path1, str) and S3PathPairing._is_glob(path1)
        exists = source_is_str and (
            (glob and len(sources) == 1)
            or (
                not glob
                and not trailing_sep(path1)
                and isinstance(path2, str)
                and trailing_sep(path2)
            )
            or (looks_up and bool(destination_is_dir))
        )
        names = [S3Path.split_version_id(p)[0] for p in sources]
        destinations = other_paths(names, path2, exists=exists, flatten=not source_is_str)
        return list(zip(sources, destinations, strict=True))

    @staticmethod
    def conflict_candidates(pairs: Sequence[tuple[str, str]]) -> list[str]:
        """Return the sources of a move whose conflicts depend on an object at their key.

        A source with another source below it may be a directory. If no object
        exists at its key, it is not copied, so it writes no destination and
        is left out of the conflict checks of :meth:`move_pairs`. A path with
        a version always names an object.

        Args:
            pairs: The sources and destinations of the move, as
                :meth:`copy_pairs` pairs them.

        Returns:
            The sources, in ``bucket/key`` form (their
            :attr:`~pyathena.filesystem.s3_path.S3Path.target`), that have
            another source below them and a destination that conflicts, in the
            order of the pairs; empty if nothing needs to be looked up.
        """
        return S3PathPairing._moves(pairs)[2]

    @staticmethod
    def move_pairs(
        pairs: Sequence[tuple[str, str]], missing: Collection[str] | None = None
    ) -> list[tuple[str, str]]:
        """Check the pairs of a move and leave out the sources that stay in place.

        Args:
            pairs: The sources and destinations of the move, as
                :meth:`copy_pairs` pairs them.
            missing: The :meth:`conflict_candidates` without an object at their
                key, in any form that names them; needed when there are
                candidates.

        Returns:
            The pairs, except those whose destination is the source itself or,
            for a ``null`` version, the key of the source.

        Raises:
            ValueError: If two sources have the same destination, or a
                destination is another source, including one left in place,
                except for a directory with no object at its key, which is not
                copied. Also if ``missing`` is needed and None.
        """
        named, sources, candidates = S3PathPairing._moves(pairs)
        if missing is None and candidates:
            raise ValueError("missing is needed to check the pairs.")
        # A directory without an object at its key writes no destination.
        skipped = set(candidates).intersection(
            str(S3Path.parse(path).target) for path in missing or ()
        )
        writers = Counter(
            dest for _, _, _, source, dest in named if source != dest and source not in skipped
        )
        for dest in writers:
            if writers[dest] > 1:
                raise ValueError("Cannot move several paths to the same destination.")
            if dest in sources:
                raise ValueError("Cannot move a path onto another path that is moved.")
        return [(p1, p2) for p1, p2, _, source, dest in named if source != dest]

    @staticmethod
    def delete_paths(path: str | list[str]) -> tuple[list[str], list[str]]:
        """Split the paths that ``rm()`` deletes into those deleted as given and those expanded.

        Args:
            path: S3 path or list of paths.

        Returns:
            The paths with a version ID, which are deleted as given, and the
            other paths, which the filesystem expands.

        Raises:
            ValueError: If a path is a bucket.
        """
        paths = [path] if isinstance(path, str) else list(path)
        versioned_paths, unversioned_paths = [], []
        for p in paths:
            s3_path = S3Path.parse(p)
            # expand_path strips the slashes of "bucket//" to the bucket.
            if s3_path.is_bucket:
                raise ValueError("Cannot delete the bucket.")
            if s3_path.version_id:
                versioned_paths.append(p)
            else:
                unversioned_paths.append(p)
        return versioned_paths, unversioned_paths

    @staticmethod
    def _is_glob(path: str) -> bool:
        """Return whether a path is a glob pattern; a version ID query is not one.

        Args:
            path: The path.

        Returns:
            True if the path has glob characters and no version ID query.
        """
        return has_magic(path) and not S3Path.has_version_id(path)

    @staticmethod
    def _moves(
        pairs: Sequence[tuple[str, str]],
    ) -> tuple[list[tuple[str, str, bool, str, str]], set[str], list[str]]:
        """Compare the paths of a move by what they name.

        Args:
            pairs: The sources and destinations of the move.

        Returns:
            Each pair with whether its source has a version and the targets
            of its source and destination; the targets of all sources,
            including those left in place; and the conflict candidates (see
            :meth:`conflict_candidates`).
        """
        named = []
        for p1, p2 in pairs:
            source_path = S3Path.parse(p1)
            named.append(
                (
                    p1,
                    p2,
                    bool(source_path.version_id),
                    str(source_path.target),
                    str(S3Path.parse(p2).target),
                )
            )
        # The sources left in place count too; a copy onto one overwrites it.
        sources = {source for _, _, _, source, _ in named}
        # A source with another source below it may be a directory.
        directories: set[str] = set()
        for source in sources:
            parent = source.rpartition("/")[0]
            while parent and parent not in directories:
                directories.add(parent)
                parent = parent.rpartition("/")[0]
        counts = Counter(dest for _, _, _, source, dest in named if source != dest)
        candidates = list(
            dict.fromkeys(
                source
                for _, _, versioned, source, dest in named
                if source != dest
                and (counts[dest] > 1 or dest in sources)
                and source in directories
                and not versioned
            )
        )
        return named, sources, candidates
