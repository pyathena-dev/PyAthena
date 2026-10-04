# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""The pairing of the paths that the S3 filesystem copies, moves and deletes."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from glob import has_magic
from typing import TYPE_CHECKING

from fsspec.implementations.local import trailing_sep
from fsspec.utils import other_paths

from pyathena.filesystem.s3_path import S3Path

if TYPE_CHECKING:
    from pyathena.filesystem.s3 import S3FileSystem


class S3PathPairing:
    """The paths that ``copy()``, ``get()``, ``mv()`` and ``rm()`` operate on.

    Expands the paths, glob patterns and directories as fsspec's ``copy()``
    and ``rm()`` do, and pairs the sources with their destinations, except
    that a path with a version ID names that version of an object: it is not
    a glob pattern, nothing is expanded below it, and its destination is named
    after its key without the version. The paths are compared by what they
    name (see :attr:`S3Path.target`).

    ``S3FileSystem`` builds one and exposes it as ``S3FileSystem.pairing``;
    ``AioS3FileSystem.pairing`` is the same object. The lookups go through the
    filesystem, with its cache.

    Example:
        >>> for source, destination in fs.pairing.copy_pairs(
        ...     "s3://bucket/dir/", "s3://bucket/copy/", recursive=True
        ... ):
        ...     print(source, "->", destination)
    """

    def __init__(self, fs: S3FileSystem) -> None:
        """Create the pairing of a filesystem.

        Args:
            fs: The filesystem that expands and looks up the paths.
        """
        self._fs = fs

    def copy_pairs(
        self,
        path1: str | list[str],
        path2: str | list[str],
        recursive: bool = False,
        maxdepth: int | None = None,
        isdir: Callable[[str], bool] | None = None,
    ) -> list[tuple[str, str]]:
        """Pair the sources of a copy with their destinations as fsspec's ``copy()`` does.

        Args:
            path1: Source S3 path, glob pattern, or list of them.
            path2: Destination path, or list of paths when ``path1`` is a
                list.
            recursive: Whether to include the contents of the directories.
            maxdepth: Maximum depth of the expansion.
            isdir: Whether a destination path is a directory, by default the
                filesystem's ``isdir``; ``get()`` passes the local
                filesystem's.

        Returns:
            The sources and their destinations. When both ``path1`` and
            ``path2`` are lists, they are paired as given. Empty if ``path1``
            is a string that matches only directories without ``recursive``.

        Raises:
            ValueError: If ``maxdepth`` is less than 1.
            FileNotFoundError: If the expansion of ``path1`` matches nothing:
                a glob pattern without matches, or a missing path with
                ``recursive``.
        """
        if isinstance(path1, list) and isinstance(path2, list):
            return list(zip(path1, path2, strict=False))
        fs = self._fs
        source_is_str = isinstance(path1, str)
        paths1 = fs.expand_path(path1, recursive=recursive, maxdepth=maxdepth)
        if source_is_str and (not recursive or maxdepth is not None):
            # Non-recursive glob does not copy directories.
            paths1 = [p for p in paths1 if not (trailing_sep(p) or fs.isdir(p))]
            if not paths1:
                return []
        glob = isinstance(path1, str) and has_magic(path1) and not S3Path.has_version_id(path1)
        # The destination is looked up only when it decides the mapping.
        exists = source_is_str and (
            (glob and len(paths1) == 1)
            or (
                not glob
                and not trailing_sep(path1)
                and isinstance(path2, str)
                and (trailing_sep(path2) or (isdir or fs.isdir)(path2))
            )
        )
        names = [S3Path.split_version_id(p)[0] for p in paths1]
        paths2 = other_paths(names, path2, exists=exists, flatten=not source_is_str)
        return list(zip(paths1, paths2, strict=True))

    def move_pairs(
        self,
        path1: str | list[str],
        path2: str | list[str],
        recursive: bool = False,
        maxdepth: int | None = None,
    ) -> list[tuple[str, str]]:
        """Pair the sources of a move with their destinations as :meth:`copy_pairs` does.

        Args:
            path1: Source S3 path, glob pattern, or list of paths.
            path2: Destination S3 path, or list of paths when ``path1`` is a
                list.
            recursive: Whether to include the contents of the directories.
            maxdepth: Maximum depth of the expansion.

        Returns:
            The sources and their destinations, except the sources whose
            destination is the source itself or, for a ``null`` version, the
            key of the source.

        Raises:
            ValueError: If two sources have the same destination, or a
                destination is another source, including one left in place,
                except for a directory with no object at its key, which is not
                copied. Also if ``maxdepth`` is less than 1.
            FileNotFoundError: If the expansion of ``path1`` matches nothing,
                as for :meth:`copy_pairs`.
        """
        # The paths are moved as given, and compared by what they name.
        named = [
            (p1, p2, str(S3Path.parse(p1).target), str(S3Path.parse(p2).target))
            for p1, p2 in self.copy_pairs(path1, path2, recursive=recursive, maxdepth=maxdepth)
        ]
        pairs = [(p1, p2) for p1, p2, source, dest in named if source != dest]
        moved = [(p1, source, dest) for p1, _, source, dest in named if source != dest]
        # The sources left in place count too; a copy onto one overwrites it.
        sources = {source for _, _, source, _ in named}
        counts = Counter(dest for _, _, dest in moved)
        # A source with another source below it may be a directory.
        directories: set[str] = set()
        for source in sources:
            parent = source.rpartition("/")[0]
            while parent and parent not in directories:
                directories.add(parent)
                parent = parent.rpartition("/")[0]
        # A directory without an object at its key is not copied, so it
        # writes no destination and is left out of the checks. A path with a
        # version always names an object.
        writers = [
            (source, dest)
            for p1, source, dest in moved
            if not (
                (counts[dest] > 1 or dest in sources)
                and source in directories
                and not S3Path.parse(p1).version_id
                and self._fs._head_object(source) is None
            )
        ]
        counts = Counter(dest for _, dest in writers)
        for _, dest in writers:
            if counts[dest] > 1:
                raise ValueError("Cannot move several paths to the same destination.")
            if dest in sources:
                raise ValueError("Cannot move a path onto another path that is moved.")
        return pairs

    def delete_paths(
        self, path: str | list[str], recursive: bool = False, maxdepth: int | None = None
    ) -> list[str]:
        """Expand the paths that ``rm()`` deletes.

        Args:
            path: S3 path or list of paths.
            recursive: Whether to include all objects below the paths.
            maxdepth: Maximum depth to expand when ``recursive`` is True.

        Returns:
            The paths with a version ID as given, followed by the expansion
            of the other paths by the filesystem's ``expand_path``.

        Raises:
            ValueError: If a path is a bucket, or ``maxdepth`` is less than 1.
            FileNotFoundError: If the expansion of the paths without a version
                ID matches nothing, as for :meth:`copy_pairs`.
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

        if unversioned_paths:
            # Versioned paths are deleted as given, without the lookup that
            # expand_path makes for them with recursive.
            unversioned_paths = self._fs.expand_path(
                unversioned_paths, recursive=recursive, maxdepth=maxdepth
            )
        return versioned_paths + unversioned_paths
