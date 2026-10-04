# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""The S3 path model of the S3 filesystem."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, replace
from re import Pattern
from typing import ClassVar

from pyathena.util import override


@dataclass(frozen=True)
class S3Path:
    """An S3 path: a bucket, optionally a key, and optionally a version ID.

    Paths are parsed from strings such as ``s3://bucket/key``,
    ``s3a://bucket/key`` or ``bucket/key``, optionally followed by a version
    ID query (``?versionId=``, ``?versionID=``, ``?versionid=`` or
    ``?version_id=``). Only a query at the end of the path is a version ID;
    any other ``?`` is part of the key. The root path, which names no bucket,
    is not an ``S3Path``.

    Attributes:
        bucket: The name of the bucket.
        key: The key, or None for a bucket path. A trailing slash is kept,
            and a key of only slashes (``bucket//``) names the bucket (see
            ``is_bucket``).
        version_id: The version ID, or None for a path without a version.

    Example:
        >>> path = S3Path.parse("s3://bucket/dir/key?versionId=v1")
        >>> path.bucket, path.key, path.version_id
        ('bucket', 'dir/key', 'v1')
        >>> path.name
        'bucket/dir/key'
        >>> str(path)  # The name, with a version ID query if there is one.
        'bucket/dir/key?versionId=v1'
        >>> path.uri
        's3://bucket/dir/key?versionId=v1'
    """

    # Version IDs do not contain "?", so only the last query can be one.
    VERSION_QUERY: ClassVar[Pattern[str]] = re.compile(
        r"\?version(Id|ID|id|_id)=(?P<version_id>[^?]+)\Z"
    )
    # Keys may contain any character, including newlines. A bare "/" is tried
    # before a key so that "bucket/?versionId=..." names the bucket.
    PATTERN: ClassVar[Pattern[str]] = re.compile(
        r"(^s3://|^s3a://|^)(?P<bucket>[a-zA-Z0-9.\-_]+)(/|/(?P<key>.+?))?"
        rf"(\Z|{VERSION_QUERY.pattern})",
        re.DOTALL,
    )

    bucket: str
    key: str | None = None
    version_id: str | None = None

    @classmethod
    def parse(cls, path: str) -> S3Path:
        """Parse a string into an S3 path.

        Args:
            path: The S3 path (e.g., "s3://bucket/key?versionId=...").

        Returns:
            The path. The key is None for a bucket path (``bucket`` or
            ``bucket/``), and keeps a trailing slash otherwise.

        Raises:
            ValueError: If the string is not a valid S3 path.
        """
        match = cls.PATTERN.search(path)
        if not match:
            raise ValueError(f"Invalid S3 path format {path}.")
        return cls(match.group("bucket"), match.group("key"), match.group("version_id"))

    @classmethod
    def split_version_id(cls, path: str) -> tuple[str, str | None]:
        """Split the version ID query from the end of a path string.

        Unlike :meth:`parse`, any string is accepted, such as the names that
        fsspec builds.

        Args:
            path: The path string.

        Returns:
            Tuple of the string without the version ID query and the version
            ID, or of the string itself and None if it ends with no version
            ID query.
        """
        match = cls.VERSION_QUERY.search(path)
        if not match:
            return path, None
        return path[: match.start()], match.group("version_id")

    @classmethod
    def has_version_id(cls, path: str | os.PathLike[str]) -> bool:
        """Return whether a path string ends with a version ID query.

        Unlike :meth:`parse`, any string is accepted, as with
        :meth:`split_version_id`.

        Args:
            path: The path.

        Returns:
            Whether the path ends with a version ID query.
        """
        return cls.split_version_id(os.fspath(path))[1] is not None

    @property
    def is_bucket(self) -> bool:
        """Whether the path names the bucket: it has no key, or a key of only slashes."""
        return not self.key or not self.key.strip("/")

    @property
    def is_directory_bucket(self) -> bool:
        """Whether the bucket is a directory bucket (S3 Express One Zone).

        The names of directory buckets end with ``--x-s3``.
        """
        return self.bucket.endswith("--x-s3")

    @property
    def name(self) -> str:
        """The path without a scheme or version, in ``bucket/key`` form, or the bucket."""
        return f"{self.bucket}/{self.key}" if self.key else self.bucket

    @property
    def uri(self) -> str:
        """The path as an ``s3://`` URI, with its version ID query, if any."""
        return f"s3://{self}"

    @property
    def target(self) -> S3Path:
        """The default move target, assuming bucket versioning is not enabled.

        A ``null`` version is taken to name the key itself: in a bucket without
        versioning, or with versioning suspended, a write to the key replaces
        its ``null`` version. With versioning enabled, a write adds a new
        version instead, so callers with that state must keep the ``null``
        version distinct from its key. Any other path is its own target.
        """
        return self.with_version_id(None) if self.version_id == "null" else self

    def with_version_id(self, version_id: str | None) -> S3Path:
        """Return the path with another version ID.

        Args:
            version_id: The version ID, or None for the path without a version.

        Returns:
            The path with the version ID.
        """
        return replace(self, version_id=version_id)

    @override
    def __str__(self) -> str:
        if self.version_id:
            return f"{self.name}?versionId={self.version_id}"
        return self.name
