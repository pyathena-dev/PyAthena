# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""The listing cache of the S3 filesystems."""

from __future__ import annotations

import contextlib
import hashlib
import time
import weakref
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar, cast

from pyathena.filesystem.s3_path import S3Path

if TYPE_CHECKING:
    from fsspec.dircache import DirCache

    from pyathena.filesystem.s3_object import S3Object


class _Lookups(dict[tuple[tuple[str, Any], ...], tuple[float, "S3Object"]]):
    """The lookups of a path made with lookup request parameters.

    Each set of parameters, as :meth:`S3ListingCache._lookup_id` identifies
    it, maps to the time at which its result was cached and the result. The
    dictionary can be referenced weakly, so that :class:`S3ListingCache` can
    tell which one is current without keeping it alive.
    """

    __slots__ = ("__weakref__",)


class S3ListingCache:
    """The lookups and listings that an S3 filesystem caches in its ``dircache``.

    The cache holds the fsspec ``DirCache`` that the filesystem exposes as its
    ``dircache``, and owns the keys under which it stores the entries, which
    are private to it:

    - the bucket listing (ListBuckets) under ``""``;
    - a listing (ListObjectsV2) under ``(name, delimiter)``, with the name of
      the directory, as :meth:`_parent_listing_key` names it;
    - the lookup (HeadObject or HeadBucket) of a path without a version under
      its name, and that of an explicit version under
      ``(name, "versionId", version_id)``;
    - the lookups made with lookup request parameters under
      ``(lookup key, "lookups")``, each with the time at which it was cached.

    The ``use_listings_cache``, ``listings_expiry_time`` and ``max_paths`` of
    the ``DirCache`` apply. The cache returns the stored entries without
    copying them, so callers must not change them, and copy those that they
    return.

    The cache also keeps, outside the ``DirCache``, a weak reference to the
    current dictionary of lookups with parameters of each lookup key, and
    uses a dictionary from the ``DirCache`` only if it is that one. An
    invalidation through this cache drops the reference, so a lookup that
    starts after it returns does not return a result with parameters cached
    before it, even if a lookup that was running stores the dictionary
    again. The cache adds no lock. A lookup whose request was answered
    before a write can still cache its own answer after the invalidation.

    Attributes:
        dircache: The ``DirCache`` that stores the entries, which the
            filesystem replaces when its ``dircache`` is assigned.
    """

    # The second element of the key, ``(lookup key, LOOKUPS_KEY)``, of the
    # lookup results of a path made with lookup request parameters.
    LOOKUPS_KEY: ClassVar[str] = "lookups"
    # The second element of the lookup key, ``(name, VERSION_KEY, version_id)``,
    # of an explicit version, which no key of an unversioned path, a name
    # string, can equal.
    VERSION_KEY: ClassVar[str] = "versionId"

    __slots__ = ("_lookups", "dircache")

    def __init__(self, dircache: DirCache) -> None:
        """Create a cache that stores its entries in a ``DirCache``.

        Args:
            dircache: The ``DirCache`` that stores the entries.
        """
        self.dircache = dircache
        # The current dictionary of lookups with parameters of each lookup
        # key. An entry normally goes away when the dictionary is no longer
        # referenced, such as after the DirCache drops it. If garbage
        # collection cleared the weak reference that the WeakValueDictionary
        # keeps to itself for its removal callbacks, and a finalizer
        # resurrected it, the keys of dead dictionaries stay until a lookup
        # replaces them, they are invalidated, or the cache is cleared.
        self._lookups: weakref.WeakValueDictionary[str | tuple[str, str, str], _Lookups] = (
            weakref.WeakValueDictionary()
        )

    @staticmethod
    def _parent_listing_key(path: S3Path) -> str | tuple[str, str]:
        """Return the key of the listing that holds a path.

        The directory of a key is named by cutting the name of the key at its
        last slash, without parsing it, so that it is named as its listing
        names it: ``bucket/a`` for the key ``a/x``, ``bucket/a/`` for
        ``a//x``, and ``bucket/a/dir?versionId=v1`` for
        ``a/dir?versionId=v1/x``.

        Args:
            path: The path of the bucket or the object.

        Returns:
            The key of the bucket listing for a bucket, and otherwise that of
            the listing of the directory that holds the key.
        """
        return (path.name.rpartition("/")[0], "/") if path.key else ""

    @classmethod
    def _lookup_key(cls, path: S3Path) -> str | tuple[str, str, str]:
        """Return the key of the lookup of a path.

        A path without a version is cached under its name, so that a key that
        ends in a query such as ``?versionId=v1`` is cached under its own
        name. An explicit version is cached under
        ``(name, "versionId", version_id)``, which no name equals.

        Args:
            path: The path of the bucket or the object.

        Returns:
            The key of the lookup.
        """
        if path.version_id:
            return path.name, cls.VERSION_KEY, path.version_id
        return path.name

    @staticmethod
    def _lookup_id(lookup_kwargs: Mapping[str, Any]) -> tuple[tuple[str, Any], ...]:
        """Identify a set of lookup parameters in the cache.

        Args:
            lookup_kwargs: The lookup parameters.

        Returns:
            The sorted parameters, with ``SSECustomerKey`` replaced by its
            SHA-256 digest so that the cache does not keep the key.
        """
        return tuple(
            sorted(
                (
                    k,
                    hashlib.sha256(v if isinstance(v, bytes) else str(v).encode()).hexdigest()
                    if k == "SSECustomerKey"
                    else v,
                )
                for k, v in lookup_kwargs.items()
            )
        )

    def get_lookup(self, path: S3Path, lookup_kwargs: Mapping[str, Any]) -> S3Object | None:
        """Get the cached HeadObject or HeadBucket result of a lookup.

        A lookup without lookup parameters uses the result of a lookup
        without them. A lookup with them uses only the result of a lookup
        with the same values, because the authorization of the request
        depends on them. Each of these results expires on its own after the
        ``listings_expiry_time`` of the ``dircache``.

        Args:
            path: The path of the bucket or the object, with its version, if
                any.
            lookup_kwargs: The lookup parameters of the lookup.

        Returns:
            The cached object, or None if there is none.
        """
        dircache = self.dircache
        key = self._lookup_key(path)
        if not lookup_kwargs:
            return cast("S3Object | None", dircache.get(key))
        lookups = self._current_lookups(dircache, key)
        cached = lookups.get(self._lookup_id(lookup_kwargs)) if lookups is not None else None
        if cached is None:
            return None
        cached_at, file = cached
        expiry_time = dircache.listings_expiry_time
        if expiry_time and time.time() - cached_at > expiry_time:
            # Caching the result of any parameters renews the dircache entry
            # of the path, so each result expires on its own.
            return None
        return file

    def put_lookup(self, path: S3Path, lookup_kwargs: Mapping[str, Any], file: S3Object) -> None:
        """Cache the HeadObject or HeadBucket result of a lookup.

        The ``"null"`` version is not cached: writes invalidate only the path
        without the version, and an overwrite replaces the ``"null"`` version
        of a bucket without versioning, so that version is looked up every
        time.

        Args:
            path: The path of the bucket or the object as it was looked up,
                with its version, if any.
            lookup_kwargs: The lookup parameters that the request was made
                with.
            file: The object to cache.
        """
        if path.version_id == "null":
            return
        dircache = self.dircache
        key = self._lookup_key(path)
        if not lookup_kwargs:
            dircache[key] = file
            return
        lookups = self._current_lookups(dircache, key)
        if lookups is None:
            # Only a new, empty dictionary becomes current, and only once, so
            # that one that an invalidation has replaced is never current
            # again, even if this or another lookup stores it again.
            lookups = _Lookups()
            self._lookups[key] = lookups
        # Updated in place: a copy written back could replace a result that
        # another thread has cached since for other parameters.
        lookups[self._lookup_id(lookup_kwargs)] = (time.time(), file)
        # Set again so that the expiry time of the entry is renewed.
        dircache[(key, self.LOOKUPS_KEY)] = lookups

    def _current_lookups(
        self, dircache: DirCache, key: str | tuple[str, str, str]
    ) -> _Lookups | None:
        """Get the current cached lookups with parameters of a lookup key.

        Args:
            dircache: The ``DirCache`` to read, read once by the caller so
                that a lookup stores into the cache that it read.
            key: The lookup key (see :meth:`_lookup_key`).

        Returns:
            The dictionary of lookups with parameters in the ``DirCache``,
            or None if there is none or it is not the current one, such as
            one that an invalidation removed and a lookup stored again.
        """
        lookups = dircache.get((key, self.LOOKUPS_KEY))
        if lookups is None or self._lookups.get(key) is not lookups:
            return None
        return cast("_Lookups", lookups)

    def evict_missing(self, path: S3Path) -> None:
        """Remove the cached entries of a bucket or an object found missing.

        The lookups of the path are removed, with and without lookup
        parameters. Unless the path has a version, the cached listing that
        would hold it, the listing of its parent directory or the bucket
        listing, is removed too if it still lists the path.

        Args:
            path: The path of the bucket or the object, with its version, if
                any.
        """
        self._evict_lookup(self._lookup_key(path))
        if path.version_id:
            return
        key = self._parent_listing_key(path)
        files = self.dircache.get(key)
        name = path.name
        if files and any(f.name == name for f in files):
            self._evict(key)

    def evict_parent_listing(self, path: S3Path) -> None:
        """Remove the cached listing that would hold a path.

        Args:
            path: The path of the bucket or the object.
        """
        self._evict(self._parent_listing_key(path))

    def get_buckets(self) -> list[S3Object] | None:
        """Get the cached bucket listing.

        Returns:
            The buckets, or None if the listing is not cached.
        """
        return cast("list[S3Object] | None", self.dircache.get(""))

    def put_buckets(self, buckets: list[S3Object]) -> None:
        """Cache the bucket listing.

        Args:
            buckets: The buckets that ListBuckets listed.
        """
        self.dircache[""] = buckets

    def evict_buckets(self) -> None:
        """Remove the cached bucket listing."""
        self._evict("")

    def evict_bucket(self, bucket: str) -> None:
        """Remove the bucket listing and every cached entry of a bucket or below it.

        A bucket that has been created or deleted holds none of the entries
        cached below it, which :meth:`invalidate_path` of the bucket keeps.

        Args:
            bucket: The name of the bucket.
        """
        self.evict_buckets()
        prefix = bucket + "/"
        for key in list(self.dircache):
            # The name is the first element of a key, nested in the key of a
            # version lookup with parameters.
            name = key
            while isinstance(name, tuple):
                name = name[0]
            if name == bucket or name.startswith(prefix):
                self._evict(key)

    def get_listing(self, name: str, delimiter: str) -> list[S3Object] | None:
        """Get the cached listing of a directory.

        Args:
            name: The name of the bucket or the directory, as a listing names
                it.
            delimiter: The delimiter of the listing; ``""`` for a recursive
                one.

        Returns:
            The listed directories and files, or None if the listing is not
            cached.
        """
        return cast("list[S3Object] | None", self.dircache.get((name, delimiter)))

    def put_listing(self, name: str, delimiter: str, files: list[S3Object]) -> None:
        """Cache the listing of a directory.

        An empty listing is not cached, and removes the cached one, so that a
        cached listing shows that the directory exists.

        Args:
            name: The name of the bucket or the directory, as a listing names
                it.
            delimiter: The delimiter of the listing; ``""`` for a recursive
                one.
            files: The complete listing of the directory.
        """
        if files:
            self.dircache[(name, delimiter)] = files
        else:
            self.evict_listing(name, delimiter)

    def evict_listing(self, name: str, delimiter: str) -> None:
        """Remove the cached listing of a directory.

        Args:
            name: The name of the bucket or the directory, as a listing names
                it.
            delimiter: The delimiter of the listing; ``""`` for a recursive
                one.
        """
        self._evict((name, delimiter))

    def get_entries(self, path: S3Path) -> list[S3Object] | S3Object | None:
        """Get the cached entry of a path, or the entries of a listing named as the path.

        Args:
            path: The path of the bucket or the object.

        Returns:
            The cached lookup of the path, the entries of a cached parent
            listing named as the path, or None if no cached entry describes
            the path. A listing of the path itself is not used, because it
            cannot tell whether an object of the same name exists. A path
            with a version uses only its own lookup, because listings
            describe the current versions.

        Raises:
            FileNotFoundError: If a cached listing of the parent directory of
                a key path does not contain the path.
        """
        cache = self.dircache.get(self._lookup_key(path))
        if cache is not None:
            return cast("list[S3Object] | S3Object", cache)
        if path.version_id:
            return None
        parent_cache = self.dircache.get(self._parent_listing_key(path))
        if parent_cache is not None:
            name = path.name
            files = [f for f in parent_cache if f["name"] == name]
            if files:
                return files
            if path.key:
                raise FileNotFoundError(str(path))
            # The bucket listing holds only the buckets that the caller owns,
            # so a bucket missing from it is looked up with HeadBucket.
        return None

    def clear(self) -> None:
        """Remove every cached entry."""
        self._lookups.clear()
        self.dircache.clear()

    def invalidate_path(self, name: str, trailing: int = 0) -> None:
        """Remove the cached entries of a path and of the directories above it.

        The bucket listing is removed only by the root, ``""``. A path with a
        version invalidates the lookup of the version, and also the object
        path without the version, because deleting or copying a version can
        change the current version of the object. It also invalidates the
        keys that end in its query, in each spelling that names the version,
        such as the keys ``a/x?versionId=v1`` and ``a/x?version_id=v1`` of
        ``bucket/a/x?versionId=v1``, which a listing names as the paths. Only
        the path itself is parsed for a version: the directories above it are
        named as they are listed, such as ``bucket/a/dir?versionId=v1`` for
        the key ``a/dir?versionId=v1/x``. A path that ends in slashes is
        invalidated both with and without them, since lookups strip them
        while a key such as ``dir//`` is listed in the directory
        ``bucket/dir/``.

        Args:
            name: The path without the protocol and its trailing slashes.
            trailing: The number of trailing slashes that the path was given
                with.
        """
        if not name:
            self.evict_buckets()
            return
        for spelling in (name, name + "/" * trailing) if trailing else (name,):
            base, version_id = S3Path.split_version_id(spelling)
            if not version_id:
                # Also the directory of a trailing slash.
                self._invalidate_name(spelling)
                continue
            # The lookup of the version, the keys that end in the query in
            # each spelling that names the version, and the object path
            # without the version.
            self._evict_lookup((base, self.VERSION_KEY, version_id))
            for query in S3Path.VERSION_QUERY_NAMES:
                self._invalidate_name(f"{base}?{query}={version_id}")
            self._invalidate_name(base)

    def invalidate(self, path: S3Path) -> None:
        """Remove the cached entries of a written path and of the directories above it.

        A path with a version removes the lookup of that version, and then
        the entries of its key.

        Args:
            path: The path of the bucket or the object.
        """
        if path.version_id:
            self._evict_lookup(self._lookup_key(path))
        self._invalidate_name(path.name)

    def _invalidate_name(self, name: str) -> None:
        """Remove the cached entries of a name and of the directories above it.

        The names of the directories are cut at their last slash without
        parsing them, so that a directory named with a version ID query or
        an empty segment is invalidated as it is listed.

        Args:
            name: The name of the bucket, the directory or the object.
        """
        while name:
            self.evict_listing(name, "/")
            self.evict_listing(name, "")
            self._evict_lookup(name)
            name = name.rpartition("/")[0]

    def _evict_lookup(self, key: str | tuple[str, str, str]) -> None:
        """Remove the cached lookups under a lookup key, with and without parameters.

        The dictionary of lookups with parameters stops being current, so
        that a lookup that read it before and stores it again does not make
        its results usable again.

        Args:
            key: The lookup key (see :meth:`_lookup_key`).
        """
        self._lookups.pop(key, None)
        self._evict(key)
        self._evict((key, self.LOOKUPS_KEY))

    def _evict(self, key: str | tuple[Any, ...]) -> None:
        """Remove a ``dircache`` entry if it exists.

        ``DirCache.pop()`` reads and then deletes the entry, so it raises
        KeyError when another thread removes the same entry in between,
        such as the request threads of ``rm()`` invalidating a shared parent
        at once. A single ``del`` raises KeyError only when the entry is
        already gone, which this ignores.

        Args:
            key: The key of the entry.
        """
        with contextlib.suppress(KeyError):
            del self.dircache[key]
