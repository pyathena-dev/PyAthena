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
    key without the version. A source may also be an
    :class:`~pyathena.filesystem.s3_path.S3Path`, such as a key that a listing
    returns, which names its bucket and key as they are, also when the key
    ends in what looks like a version ID query, and which is never parsed
    again (see :meth:`s3_copy_pairs`). A move compares the paths by what they
    name (see :attr:`~pyathena.filesystem.s3_path.S3Path.target`), keeping
    ``null`` versions distinct from their keys in the caller's
    versioning-enabled buckets.
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
        sources: Sequence[str | S3Path] | None = None,
        destination_is_dir: bool | None = None,
    ) -> list[tuple[str | S3Path, str]]:
        """Pair the sources with their destinations as fsspec's ``copy()`` does.

        A source string with a version ID is named after its key without the
        version, and an ``S3Path`` source after its bucket and key (its
        ``name``), whose suffix the destination keeps as it is.

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
        names = [
            p.name if isinstance(p, S3Path) else S3Path.split_version_id(p)[0] for p in sources
        ]
        destinations = other_paths(names, path2, exists=exists, flatten=not source_is_str)
        return list(zip(sources, destinations, strict=True))

    def s3_copy_pairs(
        self,
        sources: Sequence[str | S3Path] | None = None,
        destination_is_dir: bool | None = None,
    ) -> list[tuple[str | S3Path, str | S3Path]]:
        """Pair the sources with their S3 destinations, as :meth:`copy_pairs` does.

        The destination of an ``S3Path`` source, when ``path2`` is a string,
        is an ``S3Path`` too: ``path2`` parsed, with the suffix that
        :meth:`copy_pairs` appends to it added to its key as it is, so that a
        key such as ``a/x?versionId=null`` is copied to the key
        ``b/x?versionId=null`` instead of being parsed as a version. The
        other destinations are strings, as :meth:`copy_pairs` returns them.

        Args:
            sources: As for :meth:`copy_pairs`.
            destination_is_dir: As for :meth:`copy_pairs`.

        Returns:
            The sources and their destinations.

        Raises:
            ValueError: If ``sources`` or ``destination_is_dir`` is needed and
                None, or if ``path2`` names a version, which cannot be
                written, and a destination is an ``S3Path``.
        """
        pairs = self.copy_pairs(sources, destination_is_dir)
        if not isinstance(self.path2, str) or not any(isinstance(p1, S3Path) for p1, _ in pairs):
            return list(pairs)
        # Every destination that copy_pairs() names after a source starts with
        # path2 without its trailing slashes, followed by the suffix.
        root_name = self.path2.rstrip("/")
        root = S3Path.parse(root_name)
        if root.version_id:
            raise ValueError("Cannot copy to a versioned file.")
        typed: list[tuple[str | S3Path, str | S3Path]] = []
        for p1, p2 in pairs:
            if isinstance(p1, S3Path):
                suffix = p2[len(root_name) :]
                key = f"{root.key}{suffix}" if root.key else suffix.removeprefix("/")
                typed.append((p1, S3Path(root.bucket, key)))
            else:
                typed.append((p1, p2))
        return typed

    def conflict_candidates(
        self,
        pairs: Sequence[tuple[str | S3Path, str | S3Path]],
        *,
        versioning_enabled_buckets: Collection[str] = (),
    ) -> list[str | S3Path]:
        """Return the sources of a move or a copy whose conflicts depend on an object at their key.

        A source without a version may be a directory, such as a prefix given
        in a list or one with other sources below it. If no object exists at
        its key, it is not copied, so it writes no destination and is left
        out of the conflict checks of :meth:`move_pairs` and
        :meth:`check_copy_pairs`. A path with a version always names an
        object.

        Args:
            pairs: The sources and destinations of the move or the copy, as
                :meth:`copy_pairs` or :meth:`s3_copy_pairs` of this pairing
                returns them. A string is parsed; an ``S3Path`` is used as
                it is.
            versioning_enabled_buckets: Buckets whose versioning is enabled.
                Their ``null`` versions are distinct from their unversioned
                keys. Use the same collection for :meth:`move_pairs` or
                :meth:`check_copy_pairs`.

        Returns:
            The sources without a version whose destination conflicts, one
            per pair in the order of the pairs, as their
            :attr:`~pyathena.filesystem.s3_path.S3Path.target`: in
            ``bucket/key`` form for a source string, and as an ``S3Path`` for
            an ``S3Path`` source. Empty if nothing needs to be looked up.

        Raises:
            TypeError: If ``versioning_enabled_buckets`` is a string.
        """
        return [
            pair.source if isinstance(pair.path1, S3Path) else str(pair.source)
            for pair in self._moves(pairs, versioning_enabled_buckets)[2]
        ]

    def move_pairs(
        self,
        pairs: Sequence[tuple[str | S3Path, str | S3Path]],
        missing: Collection[str | S3Path] | None = None,
        *,
        versioning_enabled_buckets: Collection[str] = (),
    ) -> list[tuple[str | S3Path, str | S3Path]]:
        """Check the pairs of a move and leave out the sources that stay in place.

        Args:
            pairs: The sources and destinations of the move, as
                :meth:`copy_pairs` or :meth:`s3_copy_pairs` of this pairing
                returns them.
            missing: The :meth:`conflict_candidates` without an object at their
                key, as strings in any form that names them or as ``S3Path``
                objects; needed when there are candidates.
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
                except for a source without a version in ``missing``, which
                writes no destination. Also if ``missing`` is needed and None.
            TypeError: If ``missing`` is a string instead of a collection.
                Also if ``versioning_enabled_buckets`` is a string.
        """
        named = self._check(pairs, missing, versioning_enabled_buckets, "move")
        return [(pair.path1, pair.path2) for pair in named if pair.source != pair.destination]

    def check_copy_pairs(
        self,
        pairs: Sequence[tuple[str | S3Path, str | S3Path]],
        missing: Collection[str | S3Path] | None = None,
        *,
        versioning_enabled_buckets: Collection[str] = (),
    ) -> None:
        """Check the pairs of a copy whose objects are copied in parallel.

        A copy has the conflicts of a move (see :meth:`move_pairs`), since a
        parallel copy has no order in which one pair would overwrite another
        or read a source after another pair has written it. A pair whose
        destination is its own source is no conflict: it rewrites the object
        in place, such as with ``MetadataDirective="REPLACE"``.

        Args:
            pairs: The sources and destinations of the copy, as
                :meth:`copy_pairs` or :meth:`s3_copy_pairs` of this pairing
                returns them.
            missing: The :meth:`conflict_candidates` without an object at their
                key, as strings in any form that names them or as ``S3Path``
                objects; needed when there are candidates.
            versioning_enabled_buckets: Buckets whose versioning is enabled.
                Use the same collection for :meth:`conflict_candidates`.

        Raises:
            ValueError: If two sources have the same destination, or a
                destination is another source, except for a source without a
                version in ``missing``, which writes no destination. Also if
                ``missing`` is needed and None.
            TypeError: If ``missing`` is a string instead of a collection.
                Also if ``versioning_enabled_buckets`` is a string.
        """
        self._check(pairs, missing, versioning_enabled_buckets, "copy")

    def _check(
        self,
        pairs: Sequence[tuple[str | S3Path, str | S3Path]],
        missing: Collection[str | S3Path] | None,
        versioning_enabled_buckets: Collection[str],
        operation: str,
    ) -> list[_NamedPair]:
        """Raise for the conflicts of a move or a copy.

        Args:
            pairs: The sources and destinations.
            missing: The conflict candidates without an object at their key.
            versioning_enabled_buckets: Buckets whose versioning is enabled.
            operation: ``"move"`` or ``"copy"``, for the error messages.

        Returns:
            The pairs as :meth:`_moves` names them.

        Raises:
            ValueError: If the pairs conflict, or ``missing`` is needed and
                None.
            TypeError: If ``missing`` or ``versioning_enabled_buckets`` is a
                string.
        """
        if isinstance(missing, str):
            raise TypeError("missing is a collection of paths, not a path.")
        named, sources, candidates = self._moves(pairs, versioning_enabled_buckets)
        if missing is None and candidates:
            raise ValueError("missing is needed to check the pairs.")
        # A source without an object at its key, such as a directory, writes
        # no destination.
        skipped = {
            self._target(S3Path.of(path), versioning_enabled_buckets) for path in missing or ()
        }
        # A path with a version always names an object, so it writes its
        # destination even when the key has no current object.
        writers = Counter(
            pair.destination
            for pair in named
            if pair.source != pair.destination and (pair.versioned or pair.source not in skipped)
        )
        for dest in writers:
            if writers[dest] > 1:
                raise ValueError(f"Cannot {operation} several paths to the same destination.")
            if dest in sources:
                done = "moved" if operation == "move" else "copied"
                raise ValueError(f"Cannot {operation} a path onto another path that is {done}.")
        return named

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
    def _target(path: S3Path, versioning_enabled_buckets: Collection[str]) -> S3Path:
        """Return a move target using the caller's bucket versioning state.

        Args:
            path: The path of the source or destination.
            versioning_enabled_buckets: Buckets whose versioning is enabled.

        Returns:
            The path, retaining a ``null`` version only when its bucket has
            versioning enabled.
        """
        return path if path.bucket in versioning_enabled_buckets else path.target

    @staticmethod
    def _moves(
        pairs: Sequence[tuple[str | S3Path, str | S3Path]],
        versioning_enabled_buckets: Collection[str] = (),
    ) -> tuple[list[_NamedPair], set[S3Path], list[_NamedPair]]:
        """Compare the paths of a move or a copy by what they name.

        Args:
            pairs: The sources and destinations of the move or the copy.
            versioning_enabled_buckets: Buckets whose versioning is enabled.

        Returns:
            Each pair with whether its source has a version and the targets
            of its source and destination; the targets of all sources,
            including those left in place; and the pairs of the conflict
            candidates (see :meth:`conflict_candidates`).

        Raises:
            TypeError: If ``versioning_enabled_buckets`` is a string.
        """
        if isinstance(versioning_enabled_buckets, str):
            raise TypeError(
                "versioning_enabled_buckets is a collection of bucket names, not a bucket name."
            )
        named = []
        for p1, p2 in pairs:
            source_path = S3Path.of(p1)
            named.append(
                _NamedPair(
                    p1,
                    p2,
                    bool(source_path.version_id),
                    S3PathPairing._target(source_path, versioning_enabled_buckets),
                    S3PathPairing._target(S3Path.of(p2), versioning_enabled_buckets),
                )
            )
        # The sources left in place count too; a copy onto one overwrites it.
        sources = {pair.source for pair in named}
        counts = Counter(pair.destination for pair in named if pair.source != pair.destination)
        candidates = [
            pair
            for pair in named
            if pair.source != pair.destination
            and (counts[pair.destination] > 1 or pair.destination in sources)
            and not pair.versioned
        ]
        return named, sources, candidates


@dataclass(frozen=True)
class _NamedPair:
    """A pair of a move or a copy with the paths that it names.

    Attributes:
        path1: The source, as given in the pairs.
        path2: The destination, as given in the pairs.
        versioned: Whether the source has a version.
        source: The target of the source (see ``S3PathPairing._target``).
        destination: The target of the destination.
    """

    path1: str | S3Path
    path2: str | S3Path
    versioned: bool
    source: S3Path
    destination: S3Path
