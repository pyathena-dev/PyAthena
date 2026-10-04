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
from dataclasses import dataclass
from glob import has_magic

from fsspec.implementations.local import trailing_sep
from fsspec.utils import other_paths

from pyathena.filesystem.s3_path import S3Path


@dataclass(frozen=True)
class S3PathPairing:
    """The pairing of the paths of one ``copy()``, ``get()`` or ``mv()``.

    The sources are paired with their destinations as fsspec's ``copy()``
    pairs them, except that a path with a version ID names that version of an
    object: it is not a glob pattern, and its destination is named after its
    key without the version. A move compares the paths by what they name
    (see :attr:`~pyathena.filesystem.s3_path.S3Path.target`), keeping ``null``
    versions distinct from their keys in the caller's versioning-enabled
    buckets.
    :meth:`delete_paths` splits the paths of an ``rm()``.

    The pairing is a frozen dataclass of the paths as given, and holds no
    filesystem. The caller makes the lookups that it asks for
    (:attr:`expands`, :attr:`skips_directories`, :attr:`looks_up_destination`
    and :meth:`conflict_candidates`) with its own requests and cache: it
    expands the sources, leaves out the directories when
    :attr:`skips_directories` says so, and passes the results in. A
    ``sources``, ``destination_is_dir`` or ``missing`` that a rule needs and
    that is not passed raises ``ValueError``.

    Attributes:
        path1: The source path, glob pattern, or list of them, as given to
            the copy.
        path2: The destination path, or list of paths.
        recursive: Whether the copy includes the contents of directories.
        maxdepth: The maximum depth of the expansion.

    Example:
        >>> pairing = S3PathPairing("s3://bucket/dir/", "s3://bucket/copy/", recursive=True)
        >>> sources = fs.expand_path(pairing.path1, recursive=True)
        >>> pairing.copy_pairs(sources)
    """

    path1: str | list[str]
    path2: str | list[str]
    recursive: bool = False
    maxdepth: int | None = None

    @property
    def expands(self) -> bool:
        """Whether the sources are expanded.

        False if both paths are lists, which are paired as given, without
        expansion or lookups.
        """
        return not (isinstance(self.path1, list) and isinstance(self.path2, list))

    @property
    def skips_directories(self) -> bool:
        """Whether the directories among the expanded sources are left out.

        True for a string source copied without ``recursive``, or with a
        ``maxdepth``. A directory is a source that ends with a slash or that
        the filesystem reports as a directory.
        """
        return isinstance(self.path1, str) and (not self.recursive or self.maxdepth is not None)

    @property
    def looks_up_destination(self) -> bool:
        """Whether the pairing depends on the destination being a directory.

        True for a string source that is neither a glob pattern nor ends with
        a slash, copied to a string destination that does not end with a
        slash. :meth:`copy_pairs` then needs ``destination_is_dir``.
        """
        return (
            isinstance(self.path1, str)
            and not self._is_glob(self.path1)
            and not trailing_sep(self.path1)
            and isinstance(self.path2, str)
            and not trailing_sep(self.path2)
        )

    def copy_pairs(
        self,
        sources: Sequence[str] | None = None,
        destination_is_dir: bool | None = None,
    ) -> list[tuple[str, str]]:
        """Pair the sources with their destinations as fsspec's ``copy()`` does.

        Args:
            sources: The expansion of ``path1``, without the directories that
                :attr:`skips_directories` leaves out; needed when
                :attr:`expands`.
            destination_is_dir: Whether ``path2`` is a directory; needed when
                :attr:`looks_up_destination` is true and there are sources.

        Returns:
            The sources and their destinations, which keep the form of
            ``path2``. When both paths are lists, they are paired as given, up
            to the end of the shorter list, as in fsspec's ``copy()``. Empty
            if there are no sources.

        Raises:
            ValueError: If ``sources`` or ``destination_is_dir`` is needed and
                None.
        """
        path1, path2 = self.path1, self.path2
        if not self.expands:
            return list(zip(path1, path2, strict=False))
        if sources is None:
            raise ValueError("sources is needed to pair the paths.")
        if not sources:
            return []
        if destination_is_dir is None and self.looks_up_destination:
            raise ValueError("destination_is_dir is needed to pair the paths.")
        source_is_str = isinstance(path1, str)
        glob = isinstance(path1, str) and self._is_glob(path1)
        # As in fsspec's copy(); destination_is_dir is None only when
        # looks_up_destination is false, where the other terms decide.
        dest_is_dir = isinstance(path2, str) and (trailing_sep(path2) or bool(destination_is_dir))
        exists = source_is_str and (
            (glob and len(sources) == 1) or (not glob and dest_is_dir and not trailing_sep(path1))
        )
        names = [S3Path.split_version_id(p)[0] for p in sources]
        destinations = other_paths(names, path2, exists=exists, flatten=not source_is_str)
        return list(zip(sources, destinations, strict=True))

    def conflict_candidates(
        self,
        pairs: Sequence[tuple[str, str]],
        *,
        versioning_enabled_buckets: Collection[str] = (),
    ) -> list[str]:
        """Return the sources of a move whose conflicts depend on an object at their key.

        A source with another source below it may be a directory. If no object
        exists at its key, it is not copied, so it writes no destination and
        is left out of the conflict checks of :meth:`move_pairs`. A path with
        a version always names an object.

        Args:
            pairs: The sources and destinations of the move, as
                :meth:`copy_pairs` of this pairing returns them.
            versioning_enabled_buckets: Buckets whose versioning is enabled.
                Their ``null`` versions are distinct from their unversioned
                keys. Use the same collection for :meth:`move_pairs`.

        Returns:
            The sources, in ``bucket/key`` form (their
            :attr:`~pyathena.filesystem.s3_path.S3Path.target`), that have
            another source below them and a destination that conflicts, one
            per pair in the order of the pairs; empty if nothing needs to be
            looked up.
        """
        return self._moves(pairs, versioning_enabled_buckets)[2]

    def move_pairs(
        self,
        pairs: Sequence[tuple[str, str]],
        missing: Collection[str] | None = None,
        *,
        versioning_enabled_buckets: Collection[str] = (),
    ) -> list[tuple[str, str]]:
        """Check the pairs of a move and leave out the sources that stay in place.

        Args:
            pairs: The sources and destinations of the move, as
                :meth:`copy_pairs` of this pairing returns them.
            missing: The :meth:`conflict_candidates` without an object at their
                key, in any form that names them; needed when there are
                candidates.
            versioning_enabled_buckets: Buckets whose versioning is enabled.
                Their ``null`` versions are distinct from their unversioned
                keys. Use the same collection for :meth:`conflict_candidates`.

        Returns:
            The pairs, except those whose destination is the source itself or,
            for a ``null`` version in a bucket without enabled versioning,
            the key of the source.

        Raises:
            ValueError: If two sources have the same destination, or a
                destination is another source, including one left in place,
                except for a directory with no object at its key, which is not
                copied. Also if ``missing`` is needed and None.
            TypeError: If ``missing`` is a string instead of a collection.
        """
        if isinstance(missing, str):
            raise TypeError("missing is a collection of paths, not a path.")
        named, sources, candidates = self._moves(pairs, versioning_enabled_buckets)
        if missing is None and candidates:
            raise ValueError("missing is needed to check the pairs.")
        # A directory without an object at its key writes no destination.
        skipped = set(candidates).intersection(
            self._target(S3Path.parse(path), versioning_enabled_buckets) for path in missing or ()
        )
        # A path with a version always names an object, so it writes its
        # destination even when the key has no current object.
        writers = Counter(
            dest
            for _, _, versioned, source, dest in named
            if source != dest and (versioned or source not in skipped)
        )
        for dest in writers:
            if writers[dest] > 1:
                raise ValueError("Cannot move several paths to the same destination.")
            if dest in sources:
                raise ValueError("Cannot move a path onto another path that is moved.")
        return [(p1, p2) for p1, p2, _, source, dest in named if source != dest]

    @classmethod
    def delete_paths(cls, path: str | list[str]) -> tuple[list[str], list[str]]:
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
    def _target(path: S3Path, versioning_enabled_buckets: Collection[str]) -> str:
        """Return a move target using the caller's bucket versioning state."""
        return str(path if path.bucket in versioning_enabled_buckets else path.target)

    @staticmethod
    def _moves(
        pairs: Sequence[tuple[str, str]],
        versioning_enabled_buckets: Collection[str] = (),
    ) -> tuple[list[tuple[str, str, bool, str, str]], set[str], list[str]]:
        """Compare the paths of a move by what they name.

        Args:
            pairs: The sources and destinations of the move.
            versioning_enabled_buckets: Buckets whose versioning is enabled.

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
                    S3PathPairing._target(source_path, versioning_enabled_buckets),
                    S3PathPairing._target(S3Path.parse(p2), versioning_enabled_buckets),
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
        candidates = [
            source
            for _, _, versioned, source, dest in named
            if source != dest
            and (counts[dest] > 1 or dest in sources)
            and source in directories
            and not versioned
        ]
        return named, sources, candidates
