"""Model classes for S3 objects and S3 API responses used by the S3 filesystem."""

from __future__ import annotations

import copy
import dataclasses
import logging
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pyathena.filesystem.s3_path import S3Path
from pyathena.filesystem.util import override

_logger = logging.getLogger(__name__)

_API_FIELD_TO_S3_OBJECT_PROPERTY = {
    "ETag": "etag",
    "CacheControl": "cache_control",
    "ContentDisposition": "content_disposition",
    "ContentEncoding": "content_encoding",
    "ContentLanguage": "content_language",
    "ContentLength": "content_length",
    "ContentType": "content_type",
    "Expires": "expires",
    "WebsiteRedirectLocation": "website_redirect_location",
    "ServerSideEncryption": "server_side_encryption",
    "SSECustomerAlgorithm": "sse_customer_algorithm",
    "SSEKMSKeyId": "sse_kms_key_id",
    "BucketKeyEnabled": "bucket_key_enabled",
    "StorageClass": "storage_class",
    "ObjectLockMode": "object_lock_mode",
    "ObjectLockRetainUntilDate": "object_lock_retain_until_date",
    "ObjectLockLegalHoldStatus": "object_lock_legal_hold_status",
    "Metadata": "metadata",
    "LastModified": "last_modified",
}
# Fields read as None through attribute access when the object does not have them.
_S3_OBJECT_FIELDS = frozenset(
    [
        *_API_FIELD_TO_S3_OBJECT_PROPERTY.values(),
        "name",
        "type",
        "bucket",
        "key",
        "size",
        "version_id",
        "is_latest",
    ]
)
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
# The checksum fields of the S3 API responses, by their names in the models.
_CHECKSUM_FIELDS = {
    "checksum_crc32": "ChecksumCRC32",
    "checksum_crc32c": "ChecksumCRC32C",
    "checksum_crc64nvme": "ChecksumCRC64NVME",
    "checksum_sha1": "ChecksumSHA1",
    "checksum_sha256": "ChecksumSHA256",
    "checksum_sha512": "ChecksumSHA512",
    "checksum_md5": "ChecksumMD5",
    "checksum_xxhash64": "ChecksumXXHASH64",
    "checksum_xxhash3": "ChecksumXXHASH3",
    "checksum_xxhash128": "ChecksumXXHASH128",
}


class S3ObjectType:
    """Constants for S3 object types in filesystem operations.

    These constants are used to distinguish between directories and files
    when working with S3 paths through the S3FileSystem interface.
    """

    S3_OBJECT_TYPE_DIRECTORY: str = "directory"
    S3_OBJECT_TYPE_FILE: str = "file"


class S3StorageClass:
    """Constants for Amazon S3 storage classes.

    S3 storage classes determine the availability, durability, and cost
    characteristics of stored objects. Each class is optimized for different
    access patterns and use cases.

    Storage classes:
        - STANDARD: Default storage for frequently accessed data
        - REDUCED_REDUNDANCY: Lower cost, reduced durability (deprecated)
        - STANDARD_IA: Infrequently accessed data with rapid retrieval
        - ONEZONE_IA: Lower cost IA storage in single availability zone
        - INTELLIGENT_TIERING: Automatic tiering between frequent/infrequent
        - GLACIER: Archive storage for long-term backup
        - DEEP_ARCHIVE: Lowest cost archive storage
        - GLACIER_IR: Archive with faster retrieval than standard Glacier
        - OUTPOSTS: Storage on AWS Outposts
        - BUCKET: Pseudo storage class PyAthena assigns to bucket entries
        - DIRECTORY: Pseudo storage class PyAthena assigns to directory entries

    See Also:
        AWS S3 storage classes documentation:
        https://docs.aws.amazon.com/s3/latest/userguide/storage-class-intro.html
    """

    S3_STORAGE_CLASS_STANDARD: str = "STANDARD"
    S3_STORAGE_CLASS_REDUCED_REDUNDANCY: str = "REDUCED_REDUNDANCY"
    S3_STORAGE_CLASS_STANDARD_IA: str = "STANDARD_IA"
    S3_STORAGE_CLASS_ONEZONE_IA: str = "ONEZONE_IA"
    S3_STORAGE_CLASS_INTELLIGENT_TIERING: str = "INTELLIGENT_TIERING"
    S3_STORAGE_CLASS_GLACIER: str = "GLACIER"
    S3_STORAGE_CLASS_DEEP_ARCHIVE: str = "DEEP_ARCHIVE"
    S3_STORAGE_CLASS_OUTPOSTS: str = "OUTPOSTS"
    S3_STORAGE_CLASS_GLACIER_IR: str = "GLACIER_IR"

    S3_STORAGE_CLASS_BUCKET: str = "BUCKET"
    S3_STORAGE_CLASS_DIRECTORY: str = "DIRECTORY"


class S3Object(dict[str, Any]):
    """Represents an S3 object with metadata and filesystem-like properties.

    A dictionary of the fields of an object, a directory or a bucket, keyed
    by their property names (e.g., ``content_type``), as the filesystem
    returns them from ``info()`` and the listings. Only the constructor maps
    S3 API field names (e.g., ``ContentType``) to the property names;
    ``update()``, ``setdefault()`` and ``|=`` store keys as given, as on a
    dictionary.

    The fields can also be read and set as attributes. Reading an absent
    field returns None if it is a known field, such as ``content_type``, and
    raises AttributeError otherwise, while dictionary-style access raises
    KeyError for any absent key. Setting or deleting an attribute sets or
    deletes the field of that name.

    Example:
        >>> s3_obj = S3Object({"ContentType": "text/csv", "ContentLength": 1024})
        >>> print(s3_obj.content_type)  # "text/csv"
        >>> print(s3_obj["content_length"])  # 1024
        >>> s3_obj.storage_class = "STANDARD_IA"

    Note:
        This class is primarily used internally by S3FileSystem for
        representing S3 objects in filesystem operations.
    """

    __slots__ = ()

    def __init__(
        self,
        init: Mapping[str, Any],
        **kwargs,
    ) -> None:
        """Initialize the object from an S3 API response.

        Only the fields of ``init`` that have a property mapping are kept,
        under their property names (e.g., ``ContentType`` -> ``content_type``).
        ``storage_class`` defaults to ``STANDARD`` when ``init`` has no
        ``StorageClass``, and ``size`` is taken from ``Size`` or
        ``ContentLength``, or is 0, also for an empty ``init``. ``name`` is
        set to ``bucket/key``, or to the bucket when there is no key.

        Args:
            init: An S3 API response or listing entry, such as a HeadObject
                response or a ListObjectsV2 ``Contents`` entry.
            **kwargs: Additional fields stored as-is, such as ``type``,
                ``bucket``, ``key`` and ``version_id``. S3 API field names
                are stored under their property names.
        """
        super().__init__()
        for k, v in init.items():
            field = _API_FIELD_TO_S3_OBJECT_PROPERTY.get(k)
            if field is not None:
                self[field] = v
        if "StorageClass" not in init:
            # https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_ResponseSyntax
            # Amazon S3 returns this header for all objects except for
            # S3 Standard storage class objects.
            self["storage_class"] = S3StorageClass.S3_STORAGE_CLASS_STANDARD
        if "Size" in init:
            self["content_length"] = init["Size"]
            self["size"] = init["Size"]
        elif "ContentLength" in init:
            self["size"] = init["ContentLength"]
        else:
            self["content_length"] = 0
            self["size"] = 0
        for k, v in kwargs.items():
            self[_API_FIELD_TO_S3_OBJECT_PROPERTY.get(k, k)] = v
        key = self.get("key")
        self["name"] = self.get("bucket") if key is None else f"{self.get('bucket')}/{key}"

    def __getattr__(self, item: str) -> Any:
        """Return the field of a name, or None for an absent known field.

        Called only when normal attribute lookup fails, so the methods of the
        dictionary are found before the fields of the same names.

        Args:
            item: The attribute name.

        Returns:
            The field, or None if ``item`` is an absent known S3 object field.

        Raises:
            AttributeError: If ``item`` is neither a field of the object nor
                a known S3 object field.
        """
        try:
            return self[item]
        except KeyError:
            if item in _S3_OBJECT_FIELDS:
                return None
            raise AttributeError(
                f"{type(self).__name__!r} object has no attribute {item!r}"
            ) from None

    @override
    def __setattr__(self, attr: str, value: Any) -> None:
        self[attr] = value

    @override
    def __delattr__(self, attr: str) -> None:
        try:
            del self[attr]
        except KeyError:
            raise AttributeError(
                f"{type(self).__name__!r} object has no attribute {attr!r}"
            ) from None

    @override
    def copy(self) -> S3Object:  # type: ignore[override]
        """Return a copy of the object, with a copy of its ``metadata`` mapping.

        The other fields are shared with the object.

        Returns:
            A new S3Object with the same fields.
        """
        entry = type(self).__new__(type(self))
        entry.update(self)
        metadata = entry.get("metadata")
        if metadata is not None:
            entry["metadata"] = dict(metadata)
        return entry

    def to_dict(self) -> dict[str, Any]:
        """Convert S3Object to dictionary representation.

        Returns:
            Deep copy of the object's fields as a dictionary.
        """
        return copy.deepcopy(dict(self))

    def to_api_repr(self) -> dict[str, Any]:
        """Convert the object metadata to S3 API request parameters.

        Returns:
            Dictionary keyed by S3 API field names (e.g., ``ContentType``) of
            the fields that are set, excluding ``ETag``, ``ContentLength`` and
            ``LastModified``.
        """
        fields = {}
        for k, v in _API_FIELD_TO_S3_OBJECT_PROPERTY.items():
            if k in ["ETag", "ContentLength", "LastModified"]:
                # Excluded from API representation
                continue
            field = self.get(v)
            if field is not None:
                fields[k] = field
        return fields

    @classmethod
    def _from_listing(cls, listed: S3ObjectSummary | S3ObjectVersion, **fields: Any) -> S3Object:
        """Build the file entry of a listed object or version.

        The entry has the fields of the entry that the constructor builds
        from the listed ``ETag``, ``Size``, ``StorageClass`` and
        ``LastModified`` that are not None, with the type, bucket, key and
        ``fields`` as keyword arguments, in the same order.

        Args:
            listed: The listed object or version.
            **fields: Further fields, set after the key, such as
                ``version_id``; ``name`` replaces ``bucket/key``.

        Returns:
            The file entry.
        """
        entry = cls.__new__(cls)
        etag, last_modified, storage_class = listed.etag, listed.last_modified, listed.storage_class
        if etag is not None:
            entry["etag"] = etag
        if storage_class is not None:
            entry["storage_class"] = storage_class
        if last_modified is not None:
            entry["last_modified"] = last_modified
        if storage_class is None:
            # Placed as the constructor places the default.
            entry["storage_class"] = S3StorageClass.S3_STORAGE_CLASS_STANDARD
        size = listed.size
        entry["content_length"] = entry["size"] = 0 if size is None else size
        entry["type"] = S3ObjectType.S3_OBJECT_TYPE_FILE
        entry["bucket"] = bucket = listed.bucket
        entry["key"] = key = listed.key
        if fields:
            entry.update(fields)
        if "name" not in entry:
            entry["name"] = f"{bucket}/{key}"
        return entry

    @classmethod
    def from_summary(cls, summary: S3ObjectSummary) -> S3Object:
        """Build the file entry of an object listed by ListObjectsV2.

        Args:
            summary: The listed object.

        Returns:
            The file entry, named ``bucket/key``.
        """
        return cls._from_listing(summary)

    @classmethod
    def from_version(cls, version: S3ObjectVersion) -> S3Object:
        """Build the file entry of a version listed by ListObjectVersions.

        The entry is named ``bucket/key?versionId=<id>`` so that the version
        can be addressed, except for the ``null`` version, which a write to
        the key replaces and which is named ``bucket/key``.

        Args:
            version: The listed version.

        Returns:
            The file entry.
        """
        fields: dict[str, Any] = {"version_id": version.version_id, "is_latest": version.is_latest}
        if version.version_id != "null":
            fields["name"] = str(S3Path(version.bucket, version.key, version.version_id))
        return cls._from_listing(version, **fields)

    @classmethod
    def from_metadata(cls, metadata: S3Metadata, version_id: str | None = None) -> S3Object:
        """Build the file entry of an object looked up with HeadObject.

        The fields that the response does not have are left out, except
        ``Metadata``, which botocore always returns.

        Args:
            metadata: The metadata of the object, with the looked up path.
            version_id: The version ID of the entry, which may be pinned or
                omitted apart from the looked up path.

        Returns:
            The file entry, named ``bucket/key``.

        Raises:
            ValueError: If the metadata has no object path.
        """
        if metadata.path is None or not metadata.path.key:
            raise ValueError("The metadata has no object path.")
        fields = {
            "ETag": metadata.etag,
            "CacheControl": metadata.cache_control,
            "ContentDisposition": metadata.content_disposition,
            "ContentEncoding": metadata.content_encoding,
            "ContentLanguage": metadata.content_language,
            "ContentLength": metadata.content_length,
            "ContentType": metadata.content_type,
            "Expires": metadata.expires,
            "WebsiteRedirectLocation": metadata.website_redirect_location,
            "ServerSideEncryption": metadata.server_side_encryption,
            "SSECustomerAlgorithm": metadata.sse_customer_algorithm,
            "SSEKMSKeyId": metadata.sse_kms_key_id,
            "BucketKeyEnabled": metadata.bucket_key_enabled,
            "StorageClass": metadata.storage_class,
            "ObjectLockMode": metadata.object_lock_mode,
            "ObjectLockRetainUntilDate": metadata.object_lock_retain_until_date,
            "ObjectLockLegalHoldStatus": metadata.object_lock_legal_hold_status,
            "Metadata": metadata.user_metadata,
            "LastModified": metadata.last_modified,
        }
        return cls(
            init={k: v for k, v in fields.items() if v is not None},
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket=metadata.path.bucket,
            key=metadata.path.key,
            version_id=version_id,
        )

    @classmethod
    def from_bucket(cls, bucket: S3Bucket) -> S3Object:
        """Build the directory entry of a bucket.

        Args:
            bucket: The bucket.

        Returns:
            The directory entry, named after the bucket.
        """
        return cls(
            init={
                "ContentLength": 0,
                "ContentType": None,
                "StorageClass": S3StorageClass.S3_STORAGE_CLASS_BUCKET,
                "ETag": None,
                "LastModified": None,
            },
            type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
            bucket=bucket.name,
            key=None,
            version_id=None,
        )


@dataclass(frozen=True, eq=False, init=False)
class _S3Response:
    """The base of the models whose constructors read an S3 API response."""

    def _set_fields(self, **fields: Any) -> None:
        """Set the fields of the frozen model; called by the constructors only.

        Args:
            **fields: The values of the fields, by field name.
        """
        # The instance dictionary is set directly, as a frozen dataclass
        # rejects attribute assignment.
        vars(self).update(fields)

    @staticmethod
    def _checksums(result: Mapping[str, Any]) -> dict[str, str | None]:
        """Read the checksum values of a response.

        Args:
            result: The response, or the part of it that has the checksums,
                such as ``CopyObjectResult``.

        Returns:
            The ``checksum_*`` fields, None where the response has none.
        """
        return {name: result.get(key) for name, key in _CHECKSUM_FIELDS.items()}


@dataclass(frozen=True, eq=False, init=False, repr=False)
class S3Metadata(_S3Response, Mapping[str, str]):
    """Represents the metadata of an S3 object as returned by HeadObject.

    Behaves as a read-only mapping of the user-defined metadata
    (``x-amz-meta-*``, whose keys are arbitrary user-chosen strings), so it
    is a drop-in for implementations that return the user-defined metadata
    as a plain dictionary. Its truth value and equality are those of that
    mapping: it is false without user-defined metadata, and it compares
    equal to any mapping of the same user-defined metadata, such as ``{}``,
    whatever its system-defined metadata. The system-defined metadata
    (content type, encryption settings, storage class, etc.) is exposed as
    typed fields.

    Example:
        >>> metadata = fs.metadata("s3://bucket/key")
        >>> metadata["attr1"]  # user-defined metadata
        'value1'
        >>> metadata.content_type  # system-defined metadata
        'text/plain'

    See https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html

    Attributes:
        path: The path that was looked up, or None if it is not known.
        cache_control: The ``CacheControl`` header of the object.
        content_disposition: The ``ContentDisposition`` header of the object.
        content_encoding: The ``ContentEncoding`` header of the object.
        content_language: The ``ContentLanguage`` header of the object.
        content_length: The ``ContentLength`` of the object in bytes.
        content_type: The ``ContentType`` of the object.
        etag: The ``ETag`` (entity tag) of the object.
        expiration: The ``Expiration`` header of the object.
        expires: The ``Expires`` date of the object.
        last_modified: The ``LastModified`` time of the object.
        storage_class: The ``StorageClass`` of the object; ``STANDARD`` when
            S3 omits it.
        server_side_encryption: The ``ServerSideEncryption`` algorithm of
            the object.
        sse_customer_algorithm: The ``SSECustomerAlgorithm`` of the object.
        sse_kms_key_id: The ``SSEKMSKeyId`` of the KMS key for the object.
        bucket_key_enabled: Whether the object uses an S3 Bucket Key
            (``BucketKeyEnabled``).
        website_redirect_location: The ``WebsiteRedirectLocation`` of the
            object.
        version_id: The ``VersionId`` of the object.
        object_lock_mode: The ``ObjectLockMode`` of the object.
        object_lock_retain_until_date: The ``ObjectLockRetainUntilDate`` of
            the object.
        object_lock_legal_hold_status: The ``ObjectLockLegalHoldStatus`` of
            the object.
        checksum_*: The checksums of the object (``ChecksumCRC32`` and the
            like), which HeadObject returns with ``ChecksumMode=ENABLED``.
        checksum_type: The ``ChecksumType`` of the object: ``COMPOSITE`` or
            ``FULL_OBJECT``.
    """

    path: S3Path | None
    cache_control: str | None
    content_disposition: str | None
    content_encoding: str | None
    content_language: str | None
    content_length: int | None
    content_type: str | None
    etag: str | None
    expiration: str | None
    expires: datetime | None
    last_modified: datetime | None
    storage_class: str
    server_side_encryption: str | None
    sse_customer_algorithm: str | None
    sse_kms_key_id: str | None
    bucket_key_enabled: bool | None
    website_redirect_location: str | None
    version_id: str | None
    object_lock_mode: str | None
    object_lock_retain_until_date: datetime | None
    object_lock_legal_hold_status: str | None
    checksum_crc32: str | None
    checksum_crc32c: str | None
    checksum_crc64nvme: str | None
    checksum_sha1: str | None
    checksum_sha256: str | None
    checksum_sha512: str | None
    checksum_md5: str | None
    checksum_xxhash64: str | None
    checksum_xxhash3: str | None
    checksum_xxhash128: str | None
    checksum_type: str | None
    _user_metadata: dict[str, str]

    def __init__(self, response: Mapping[str, Any], path: S3Path | None = None) -> None:
        """Initialize the metadata from a HeadObject response.

        Args:
            response: The HeadObject response.
            path: The path that was looked up, if known.
        """
        self._set_fields(
            path=path,
            cache_control=response.get("CacheControl"),
            content_disposition=response.get("ContentDisposition"),
            content_encoding=response.get("ContentEncoding"),
            content_language=response.get("ContentLanguage"),
            content_length=response.get("ContentLength"),
            content_type=response.get("ContentType"),
            etag=response.get("ETag"),
            expiration=response.get("Expiration"),
            expires=response.get("Expires"),
            last_modified=response.get("LastModified"),
            # https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_ResponseSyntax
            # Amazon S3 returns this header for all objects except for
            # S3 Standard storage class objects.
            storage_class=response.get("StorageClass", S3StorageClass.S3_STORAGE_CLASS_STANDARD),
            server_side_encryption=response.get("ServerSideEncryption"),
            sse_customer_algorithm=response.get("SSECustomerAlgorithm"),
            sse_kms_key_id=response.get("SSEKMSKeyId"),
            bucket_key_enabled=response.get("BucketKeyEnabled"),
            website_redirect_location=response.get("WebsiteRedirectLocation"),
            version_id=response.get("VersionId"),
            object_lock_mode=response.get("ObjectLockMode"),
            object_lock_retain_until_date=response.get("ObjectLockRetainUntilDate"),
            object_lock_legal_hold_status=response.get("ObjectLockLegalHoldStatus"),
            **self._checksums(response),
            checksum_type=response.get("ChecksumType"),
            _user_metadata=response.get("Metadata", {}),
        )

    @override
    def __getitem__(self, key: str) -> str:
        return self._user_metadata[key]

    @override
    def __iter__(self) -> Iterator[str]:
        return iter(self._user_metadata)

    @override
    def __len__(self) -> int:
        return len(self._user_metadata)

    @override
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._user_metadata!r})"

    @property
    def user_metadata(self) -> dict[str, str]:
        """A copy of the user-defined metadata (``x-amz-meta-*``).

        The keys are arbitrary user-chosen strings, returned as stored in S3
        (S3 normalizes them to lowercase). The same key/value pairs are also
        accessible directly through the mapping interface of this class.
        """
        return dict(self._user_metadata)


@dataclass(frozen=True)
class S3ObjectSummary:
    """An object listed by ListObjectsV2 (a ``Contents`` entry).

    Attributes:
        bucket: The bucket of the object.
        key: The key of the object.
        size: The size of the object in bytes.
        etag: The entity tag of the object.
        last_modified: When the object was last modified.
        storage_class: The storage class of the object.
        checksum_algorithm: The algorithms of the checksums of the object
            (``ChecksumAlgorithm``), if any.
        checksum_type: The ``ChecksumType`` of the object: ``COMPOSITE`` or
            ``FULL_OBJECT``.
    """

    bucket: str
    key: str
    size: int | None = None
    etag: str | None = None
    last_modified: datetime | None = None
    storage_class: str | None = None
    checksum_algorithm: tuple[str, ...] = ()
    checksum_type: str | None = None

    @classmethod
    def from_response(cls, bucket: str, entry: Mapping[str, Any]) -> S3ObjectSummary:
        """Build the object from a ``Contents`` entry of ListObjectsV2.

        Args:
            bucket: The bucket that was listed.
            entry: The ``Contents`` entry.

        Returns:
            The listed object.
        """
        return cls(
            bucket=bucket,
            key=entry["Key"],
            size=entry.get("Size"),
            etag=entry.get("ETag"),
            last_modified=entry.get("LastModified"),
            storage_class=entry.get("StorageClass"),
            checksum_algorithm=tuple(entry.get("ChecksumAlgorithm", ())),
            checksum_type=entry.get("ChecksumType"),
        )

    @property
    def path(self) -> S3Path:
        """The path of the object."""
        return S3Path(self.bucket, self.key)


@dataclass(frozen=True, init=False)
class S3ObjectVersion(_S3Response):
    """Represents a version of an S3 object as returned by ListObjectVersions.

    Attributes:
        bucket: The name of the bucket that contains the version.
        key: The ``Key`` of the object.
        version_id: The ``VersionId`` of the version.
        is_latest: Whether the version is the latest version of the object
            (``IsLatest``).
        is_delete_marker: Whether the version is a delete marker.
        last_modified: The ``LastModified`` time of the version.
        etag: The ``ETag`` of the version; None for delete markers.
        size: The ``Size`` of the version in bytes; None for delete markers.
        storage_class: The ``StorageClass`` of the version; None for delete
            markers.
        checksum_algorithm: The algorithms of the checksums of the version
            (``ChecksumAlgorithm``); empty for delete markers.
        checksum_type: The ``ChecksumType`` of the version; None for delete
            markers.
        owner: The ``Owner`` of the version, or None if the response has
            none.
    """

    bucket: str
    key: str
    version_id: str | None
    is_latest: bool
    is_delete_marker: bool
    last_modified: datetime | None
    etag: str | None
    size: int | None
    storage_class: str | None
    checksum_algorithm: tuple[str, ...]
    checksum_type: str | None
    owner: S3Owner | None

    def __init__(self, bucket: str, is_delete_marker: bool, response: Mapping[str, Any]) -> None:
        """Initialize the version from a ListObjectVersions entry.

        Args:
            bucket: The name of the bucket that contains the version.
            is_delete_marker: Whether the entry comes from ``DeleteMarkers``
                rather than ``Versions``.
            response: A ``Versions`` or ``DeleteMarkers`` entry of the
                ListObjectVersions response.
        """
        owner = response.get("Owner")
        self._set_fields(
            bucket=bucket,
            key=response["Key"],
            version_id=response.get("VersionId"),
            is_latest=response.get("IsLatest", False),
            is_delete_marker=is_delete_marker,
            last_modified=response.get("LastModified"),
            etag=response.get("ETag"),
            size=response.get("Size"),
            storage_class=response.get("StorageClass"),
            checksum_algorithm=tuple(response.get("ChecksumAlgorithm", ())),
            checksum_type=response.get("ChecksumType"),
            owner=S3Owner(owner) if owner else None,
        )

    @property
    def name(self) -> str:
        """The path of the object in ``bucket/key`` form, without the version."""
        return f"{self.bucket}/{self.key}"

    @property
    def path(self) -> S3Path:
        """The path of the version, with its version ID, including ``null``."""
        return S3Path(self.bucket, self.key, self.version_id)


@dataclass(frozen=True)
class S3CommonPrefix:
    """A key prefix listed with a delimiter (a ``CommonPrefixes`` entry).

    Attributes:
        bucket: The bucket that was listed.
        prefix: The key prefix, ending with the delimiter.
    """

    bucket: str
    prefix: str

    @classmethod
    def from_response(cls, bucket: str, entry: Mapping[str, Any]) -> S3CommonPrefix:
        """Build the prefix from a ``CommonPrefixes`` entry.

        Args:
            bucket: The bucket that was listed.
            entry: The ``CommonPrefixes`` entry.

        Returns:
            The listed prefix.
        """
        return cls(bucket=bucket, prefix=entry["Prefix"])


@dataclass(frozen=True)
class S3Bucket:
    """A bucket, as listed by ListBuckets or found by HeadBucket.

    Attributes:
        name: The name of the bucket.
        creation_date: When the bucket was created, if returned.
        bucket_region: The region of the bucket, if returned.
    """

    name: str
    creation_date: datetime | None = None
    bucket_region: str | None = None

    @classmethod
    def from_response(cls, entry: Mapping[str, Any]) -> S3Bucket:
        """Build the bucket from a ``Buckets`` entry of ListBuckets.

        Args:
            entry: The ``Buckets`` entry.

        Returns:
            The listed bucket.
        """
        return cls(
            name=entry["Name"],
            creation_date=entry.get("CreationDate"),
            bucket_region=entry.get("BucketRegion"),
        )


@dataclass(frozen=True)
class S3ListObjectsPage:
    """One page of a ListObjectsV2 listing.

    Attributes:
        bucket: The bucket that was listed.
        objects: The listed objects (``Contents``).
        common_prefixes: The listed key prefixes (``CommonPrefixes``).
        key_count: The number of objects and prefixes of the page
            (``KeyCount``), if returned.
        is_truncated: Whether more pages follow.
        next_continuation_token: The token of the next page, if any.
    """

    bucket: str
    objects: tuple[S3ObjectSummary, ...] = ()
    common_prefixes: tuple[S3CommonPrefix, ...] = ()
    key_count: int | None = None
    is_truncated: bool = False
    next_continuation_token: str | None = None

    @classmethod
    def from_response(cls, bucket: str, response: Mapping[str, Any]) -> S3ListObjectsPage:
        """Build the page from a ListObjectsV2 response.

        Args:
            bucket: The bucket that was listed.
            response: The ListObjectsV2 response.

        Returns:
            The page.
        """
        return cls(
            bucket=bucket,
            objects=tuple(
                S3ObjectSummary.from_response(bucket, c) for c in response.get("Contents", [])
            ),
            common_prefixes=tuple(
                S3CommonPrefix.from_response(bucket, c) for c in response.get("CommonPrefixes", [])
            ),
            key_count=response.get("KeyCount"),
            is_truncated=response.get("IsTruncated", False),
            next_continuation_token=response.get("NextContinuationToken"),
        )


@dataclass(frozen=True)
class S3ListObjectVersionsPage:
    """One page of a ListObjectVersions listing.

    Attributes:
        bucket: The bucket that was listed.
        versions: The listed versions (``Versions``).
        delete_markers: The listed delete markers (``DeleteMarkers``).
        common_prefixes: The listed key prefixes (``CommonPrefixes``).
        is_truncated: Whether more pages follow.
        next_key_marker: The key marker of the next page, if any.
        next_version_id_marker: The version ID marker of the next page, if
            any.
    """

    bucket: str
    versions: tuple[S3ObjectVersion, ...] = ()
    delete_markers: tuple[S3ObjectVersion, ...] = ()
    common_prefixes: tuple[S3CommonPrefix, ...] = ()
    is_truncated: bool = False
    next_key_marker: str | None = None
    next_version_id_marker: str | None = None

    @classmethod
    def from_response(cls, bucket: str, response: Mapping[str, Any]) -> S3ListObjectVersionsPage:
        """Build the page from a ListObjectVersions response.

        Args:
            bucket: The bucket that was listed.
            response: The ListObjectVersions response.

        Returns:
            The page.
        """
        return cls(
            bucket=bucket,
            versions=tuple(
                S3ObjectVersion(bucket=bucket, is_delete_marker=False, response=v)
                for v in response.get("Versions", [])
            ),
            delete_markers=tuple(
                S3ObjectVersion(bucket=bucket, is_delete_marker=True, response=m)
                for m in response.get("DeleteMarkers", [])
            ),
            common_prefixes=tuple(
                S3CommonPrefix.from_response(bucket, c) for c in response.get("CommonPrefixes", [])
            ),
            is_truncated=response.get("IsTruncated", False),
            next_key_marker=response.get("NextKeyMarker"),
            next_version_id_marker=response.get("NextVersionIdMarker"),
        )


@dataclass(frozen=True)
class S3ListBucketsPage:
    """One page of a ListBuckets listing.

    Attributes:
        buckets: The listed buckets.
        continuation_token: The token of the next page, if any.
    """

    buckets: tuple[S3Bucket, ...] = ()
    continuation_token: str | None = None

    @classmethod
    def from_response(cls, response: Mapping[str, Any]) -> S3ListBucketsPage:
        """Build the page from a ListBuckets response.

        Args:
            response: The ListBuckets response.

        Returns:
            The page.
        """
        return cls(
            buckets=tuple(S3Bucket.from_response(b) for b in response.get("Buckets", [])),
            continuation_token=response.get("ContinuationToken"),
        )


@dataclass(frozen=True)
class S3ListMultipartUploadsPage:
    """One page of a ListMultipartUploads listing.

    Attributes:
        bucket: The bucket that was listed.
        uploads: The listed in-progress multipart uploads (``Uploads``),
            with the listed bucket as their bucket.
        is_truncated: Whether more pages follow.
        next_key_marker: The key marker of the next page, if any.
        next_upload_id_marker: The upload ID marker of the next page, if any.
    """

    bucket: str
    uploads: tuple[S3MultipartUpload, ...] = ()
    is_truncated: bool = False
    next_key_marker: str | None = None
    next_upload_id_marker: str | None = None

    @classmethod
    def from_response(cls, bucket: str, response: Mapping[str, Any]) -> S3ListMultipartUploadsPage:
        """Build the page from a ListMultipartUploads response.

        Args:
            bucket: The bucket that was listed.
            response: The ListMultipartUploads response.

        Returns:
            The page.
        """
        return cls(
            bucket=bucket,
            uploads=tuple(
                S3MultipartUpload({**u, "Bucket": bucket}) for u in response.get("Uploads", [])
            ),
            is_truncated=response.get("IsTruncated", False),
            next_key_marker=response.get("NextKeyMarker"),
            next_upload_id_marker=response.get("NextUploadIdMarker"),
        )


@dataclass(frozen=True)
class S3DeleteError:
    """An object that DeleteObjects could not delete (an ``Errors`` entry).

    ``str()`` gives ``"path (code: message)"``.

    Attributes:
        path: The path of the object, with the version ID of the request, if
            any.
        code: The error code.
        message: The error message.
    """

    path: S3Path
    code: str | None = None
    message: str | None = None

    @classmethod
    def from_response(cls, bucket: str, entry: Mapping[str, Any]) -> S3DeleteError:
        """Build the error from an ``Errors`` entry of DeleteObjects.

        Args:
            bucket: The bucket of the request.
            entry: The ``Errors`` entry.

        Returns:
            The error.
        """
        return cls(
            path=S3Path(bucket, entry["Key"], entry.get("VersionId")),
            code=entry.get("Code"),
            message=entry.get("Message"),
        )

    @override
    def __str__(self) -> str:
        return f"{self.path} ({self.code}: {self.message})"


@dataclass(frozen=True)
class S3DeleteResult:
    """The result of a DeleteObjects request.

    S3 answers a request with 200 also when it could not delete some of the
    objects, and lists them in ``errors``.

    Attributes:
        bucket: The bucket of the request.
        deleted: The deleted objects, with the version ID of the request, if
            any. Empty for a quiet batch.
        errors: The objects that S3 could not delete.
    """

    bucket: str
    deleted: tuple[S3Path, ...] = ()
    errors: tuple[S3DeleteError, ...] = ()

    @classmethod
    def from_response(cls, bucket: str, response: Mapping[str, Any]) -> S3DeleteResult:
        """Build the result from a DeleteObjects response.

        Args:
            bucket: The bucket of the request.
            response: The DeleteObjects response.

        Returns:
            The result.
        """
        return cls(
            bucket=bucket,
            deleted=tuple(
                S3Path(bucket, d["Key"], d.get("VersionId")) for d in response.get("Deleted", [])
            ),
            errors=tuple(
                S3DeleteError.from_response(bucket, e) for e in response.get("Errors", [])
            ),
        )


@dataclass(frozen=True, init=False)
class S3DeleteObject(_S3Response):
    """The result of a DeleteObject request.

    Attributes:
        version_id: The ``VersionId`` of the deleted version, or of the
            delete marker that the request created, if the bucket is
            versioned.
        delete_marker: Whether the deleted version, or the version that the
            request created, is a delete marker (``DeleteMarker``).
    """

    version_id: str | None
    delete_marker: bool

    def __init__(self, response: Mapping[str, Any]) -> None:
        """Initialize the result from a DeleteObject response.

        Args:
            response: The DeleteObject response.
        """
        self._set_fields(
            version_id=response.get("VersionId"),
            delete_marker=response.get("DeleteMarker", False),
        )


@dataclass(frozen=True, init=False)
class S3CopyObject(_S3Response):
    """The result of a CopyObject request.

    Attributes:
        version_id: The ``VersionId`` of the copy, if the destination bucket
            is versioned.
        copy_source_version_id: The ``CopySourceVersionId`` of the copied
            version of the source, if the source bucket is versioned.
        etag: The ``ETag`` of the copy.
        last_modified: The ``LastModified`` time of the copy.
        checksum_*: The checksums of the copy (``ChecksumCRC32`` and the
            like), if returned.
        checksum_type: The ``ChecksumType`` of the copy: ``COMPOSITE`` or
            ``FULL_OBJECT``.
    """

    version_id: str | None
    copy_source_version_id: str | None
    etag: str | None
    last_modified: datetime | None
    checksum_crc32: str | None
    checksum_crc32c: str | None
    checksum_crc64nvme: str | None
    checksum_sha1: str | None
    checksum_sha256: str | None
    checksum_sha512: str | None
    checksum_md5: str | None
    checksum_xxhash64: str | None
    checksum_xxhash3: str | None
    checksum_xxhash128: str | None
    checksum_type: str | None

    def __init__(self, response: Mapping[str, Any]) -> None:
        """Initialize the result from a CopyObject response.

        The ``ETag``, ``LastModified`` and checksums are read from its
        ``CopyObjectResult``.

        Args:
            response: The CopyObject response.
        """
        result = response.get("CopyObjectResult", {})
        self._set_fields(
            version_id=response.get("VersionId"),
            copy_source_version_id=response.get("CopySourceVersionId"),
            etag=result.get("ETag"),
            last_modified=result.get("LastModified"),
            **self._checksums(result),
            checksum_type=result.get("ChecksumType"),
        )


@dataclass(frozen=True, init=False)
class S3PutObject(_S3Response):
    """Represents the response from an S3 PUT object operation.

    Attributes:
        expiration: The ``Expiration`` header of the uploaded object.
        version_id: The ``VersionId`` of the uploaded object.
        etag: The ``ETag`` of the uploaded object.
        checksum_*: The checksums of the uploaded object (``ChecksumCRC32``
            and the like), if returned.
        checksum_type: The ``ChecksumType`` of the uploaded object.
        server_side_encryption: The ``ServerSideEncryption`` algorithm of
            the uploaded object.
        sse_customer_algorithm: The ``SSECustomerAlgorithm`` of the uploaded
            object.
        sse_customer_key_md5: The ``SSECustomerKeyMD5`` of the
            customer-provided key for the uploaded object.
        sse_kms_key_id: The ``SSEKMSKeyId`` of the KMS key for the uploaded
            object.
        sse_kms_encryption_context: The ``SSEKMSEncryptionContext`` of the
            uploaded object.
        bucket_key_enabled: Whether the uploaded object uses an S3 Bucket
            Key (``BucketKeyEnabled``).
        request_charged: The ``RequestCharged`` field of the response.

    Note:
        This class is used internally by S3FileSystem operations and
        typically not instantiated directly by users.
    """

    expiration: str | None
    version_id: str | None
    etag: str | None
    checksum_crc32: str | None
    checksum_crc32c: str | None
    checksum_crc64nvme: str | None
    checksum_sha1: str | None
    checksum_sha256: str | None
    checksum_sha512: str | None
    checksum_md5: str | None
    checksum_xxhash64: str | None
    checksum_xxhash3: str | None
    checksum_xxhash128: str | None
    checksum_type: str | None
    server_side_encryption: str | None
    sse_customer_algorithm: str | None
    sse_customer_key_md5: str | None
    sse_kms_key_id: str | None
    sse_kms_encryption_context: str | None
    bucket_key_enabled: bool | None
    request_charged: str | None

    def __init__(self, response: Mapping[str, Any]) -> None:
        """Initialize the result from a PutObject response.

        Args:
            response: The PutObject response.
        """
        self._set_fields(
            expiration=response.get("Expiration"),
            version_id=response.get("VersionId"),
            etag=response.get("ETag"),
            **self._checksums(response),
            checksum_type=response.get("ChecksumType"),
            server_side_encryption=response.get("ServerSideEncryption"),
            sse_customer_algorithm=response.get("SSECustomerAlgorithm"),
            sse_customer_key_md5=response.get("SSECustomerKeyMD5"),
            sse_kms_key_id=response.get("SSEKMSKeyId"),
            sse_kms_encryption_context=response.get("SSEKMSEncryptionContext"),
            bucket_key_enabled=response.get("BucketKeyEnabled"),
            request_charged=response.get("RequestCharged"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert the result to a dictionary.

        Returns:
            A deep copy of the fields, keyed by their names with a leading
            underscore (e.g., ``_etag``).
        """
        return {f"_{k}": v for k, v in dataclasses.asdict(self).items()}


@dataclass(frozen=True, init=False)
class S3Owner(_S3Response):
    """Represents the owner or initiator of an S3 object or multipart upload.

    Attributes:
        display_name: The ``DisplayName`` of the owner.
        id: The canonical user ``ID`` of the owner.
    """

    display_name: str | None
    id: str | None

    def __init__(self, response: Mapping[str, Any]) -> None:
        """Initialize the owner from an ``Owner`` or ``Initiator`` response field.

        Args:
            response: The ``Owner`` or ``Initiator`` field of an S3 API response.
        """
        self._set_fields(display_name=response.get("DisplayName"), id=response.get("ID"))


@dataclass(frozen=True, init=False)
class S3MultipartUpload(_S3Response):
    """Represents an S3 multipart upload operation.

    Attributes:
        abort_date: The ``AbortDate`` of the upload set by a lifecycle rule.
        abort_rule_id: The ``AbortRuleId`` of the lifecycle rule that applies
            to the upload.
        bucket: The ``Bucket`` of the upload.
        key: The ``Key`` of the object being uploaded.
        upload_id: The ``UploadId`` of the multipart upload.
        server_side_encryption: The ``ServerSideEncryption`` algorithm of
            the upload.
        sse_customer_algorithm: The ``SSECustomerAlgorithm`` of the upload.
        sse_customer_key_md5: The ``SSECustomerKeyMD5`` of the
            customer-provided key for the upload.
        sse_kms_key_id: The ``SSEKMSKeyId`` of the KMS key for the upload.
        sse_kms_encryption_context: The ``SSEKMSEncryptionContext`` of the
            upload.
        bucket_key_enabled: Whether the upload uses an S3 Bucket Key
            (``BucketKeyEnabled``).
        request_charged: The ``RequestCharged`` field of the response.
        checksum_algorithm: The ``ChecksumAlgorithm`` of the upload.
        checksum_type: The ``ChecksumType`` of the upload: ``COMPOSITE`` or
            ``FULL_OBJECT``.
        initiated: The ``Initiated`` time of the upload, returned by
            ListMultipartUploads.
        storage_class: The ``StorageClass`` of the upload, returned by
            ListMultipartUploads.
        owner: The ``Owner`` of the upload, returned by ListMultipartUploads.
        initiator: The ``Initiator`` of the upload, returned by
            ListMultipartUploads.

    Note:
        Used internally by S3FileSystem for large file upload operations,
        and returned by ``S3FileSystem.list_multipart_uploads``.
    """

    abort_date: datetime | None
    abort_rule_id: str | None
    bucket: str | None
    key: str | None
    upload_id: str | None
    server_side_encryption: str | None
    sse_customer_algorithm: str | None
    sse_customer_key_md5: str | None
    sse_kms_key_id: str | None
    sse_kms_encryption_context: str | None
    bucket_key_enabled: bool | None
    request_charged: str | None
    checksum_algorithm: str | None
    checksum_type: str | None
    initiated: datetime | None
    storage_class: str | None
    owner: S3Owner | None
    initiator: S3Owner | None

    def __init__(self, response: Mapping[str, Any]) -> None:
        """Initialize the upload from an S3 API response.

        Args:
            response: A CreateMultipartUpload response or an ``Uploads`` entry
                of a ListMultipartUploads response.
        """
        owner = response.get("Owner")
        initiator = response.get("Initiator")
        self._set_fields(
            abort_date=response.get("AbortDate"),
            abort_rule_id=response.get("AbortRuleId"),
            bucket=response.get("Bucket"),
            key=response.get("Key"),
            upload_id=response.get("UploadId"),
            server_side_encryption=response.get("ServerSideEncryption"),
            sse_customer_algorithm=response.get("SSECustomerAlgorithm"),
            sse_customer_key_md5=response.get("SSECustomerKeyMD5"),
            sse_kms_key_id=response.get("SSEKMSKeyId"),
            sse_kms_encryption_context=response.get("SSEKMSEncryptionContext"),
            bucket_key_enabled=response.get("BucketKeyEnabled"),
            request_charged=response.get("RequestCharged"),
            checksum_algorithm=response.get("ChecksumAlgorithm"),
            checksum_type=response.get("ChecksumType"),
            # The following fields are returned by the ListMultipartUploads API.
            initiated=response.get("Initiated"),
            storage_class=response.get("StorageClass"),
            owner=S3Owner(owner) if owner else None,
            initiator=S3Owner(initiator) if initiator else None,
        )


@dataclass(frozen=True, init=False)
class S3MultipartUploadPart(_S3Response):
    """Represents a single part in an S3 multipart upload operation.

    Attributes:
        part_number: The part number of the part.
        copy_source_version_id: The ``CopySourceVersionId`` of the source
            object of a copied part.
        last_modified: The ``LastModified`` time from ``CopyPartResult``;
            None for uploaded parts.
        etag: The ``ETag`` of the part.
        checksum_*: The checksums of the part (``ChecksumCRC32`` and the
            like), if returned.
        server_side_encryption: The ``ServerSideEncryption`` algorithm of
            the part.
        sse_customer_algorithm: The ``SSECustomerAlgorithm`` of the part.
        sse_customer_key_md5: The ``SSECustomerKeyMD5`` of the
            customer-provided key for the part.
        sse_kms_key_id: The ``SSEKMSKeyId`` of the KMS key for the part.
        bucket_key_enabled: Whether the part uses an S3 Bucket Key
            (``BucketKeyEnabled``).
        request_charged: The ``RequestCharged`` field of the response.

    Note:
        Parts must be at least 5MB except for the last part. Used internally
        by S3FileSystem for chunked upload operations.
    """

    part_number: int
    copy_source_version_id: str | None
    last_modified: datetime | None
    etag: str | None
    checksum_crc32: str | None
    checksum_crc32c: str | None
    checksum_crc64nvme: str | None
    checksum_sha1: str | None
    checksum_sha256: str | None
    checksum_sha512: str | None
    checksum_md5: str | None
    checksum_xxhash64: str | None
    checksum_xxhash3: str | None
    checksum_xxhash128: str | None
    server_side_encryption: str | None
    sse_customer_algorithm: str | None
    sse_customer_key_md5: str | None
    sse_kms_key_id: str | None
    bucket_key_enabled: bool | None
    request_charged: str | None

    def __init__(self, part_number: int, response: Mapping[str, Any]) -> None:
        """Initialize the part from an UploadPart or UploadPartCopy response.

        For an UploadPartCopy response, the ``ETag``, ``LastModified`` and
        checksums are read from its ``CopyPartResult``.

        Args:
            part_number: The part number of the part.
            response: The UploadPart or UploadPartCopy response.
        """
        copy_part_result = response.get("CopyPartResult")
        part_result = copy_part_result or response
        self._set_fields(
            part_number=part_number,
            copy_source_version_id=response.get("CopySourceVersionId"),
            last_modified=copy_part_result.get("LastModified") if copy_part_result else None,
            etag=part_result.get("ETag"),
            **self._checksums(part_result),
            server_side_encryption=response.get("ServerSideEncryption"),
            sse_customer_algorithm=response.get("SSECustomerAlgorithm"),
            sse_customer_key_md5=response.get("SSECustomerKeyMD5"),
            sse_kms_key_id=response.get("SSEKMSKeyId"),
            bucket_key_enabled=response.get("BucketKeyEnabled"),
            request_charged=response.get("RequestCharged"),
        )

    def to_api_repr(self) -> dict[str, Any]:
        """Convert the part to a part entry of a CompleteMultipartUpload request.

        Returns:
            Dictionary with the ``ETag``, checksum and ``PartNumber`` fields of
            the part, omitting fields whose value is None.
        """
        fields = {
            "ETag": self.etag,
            **{key: getattr(self, name) for name, key in _CHECKSUM_FIELDS.items()},
            "PartNumber": self.part_number,
        }
        return {key: value for key, value in fields.items() if value is not None}


@dataclass(frozen=True, init=False)
class S3CompleteMultipartUpload(_S3Response):
    """Represents the completion of an S3 multipart upload operation.

    Attributes:
        location: The ``Location`` URI of the completed object.
        bucket: The ``Bucket`` of the completed object.
        key: The ``Key`` of the completed object.
        expiration: The ``Expiration`` header of the completed object.
        version_id: The ``VersionId`` of the completed object.
        etag: The ``ETag`` of the completed object.
        checksum_*: The checksums of the completed object
            (``ChecksumCRC32`` and the like), if returned.
        checksum_type: The ``ChecksumType`` of the completed object:
            ``COMPOSITE`` or ``FULL_OBJECT``.
        server_side_encryption: The ``ServerSideEncryption`` algorithm of
            the completed object.
        sse_kms_key_id: The ``SSEKMSKeyId`` of the KMS key for the completed
            object.
        bucket_key_enabled: Whether the completed object uses an S3 Bucket
            Key (``BucketKeyEnabled``).
        request_charged: The ``RequestCharged`` field of the response.

    Note:
        This represents the successful completion of a multipart upload.
        Used internally by S3FileSystem operations.
    """

    location: str | None
    bucket: str | None
    key: str | None
    expiration: str | None
    version_id: str | None
    etag: str | None
    checksum_crc32: str | None
    checksum_crc32c: str | None
    checksum_crc64nvme: str | None
    checksum_sha1: str | None
    checksum_sha256: str | None
    checksum_sha512: str | None
    checksum_md5: str | None
    checksum_xxhash64: str | None
    checksum_xxhash3: str | None
    checksum_xxhash128: str | None
    checksum_type: str | None
    server_side_encryption: str | None
    sse_kms_key_id: str | None
    bucket_key_enabled: bool | None
    request_charged: str | None

    def __init__(self, response: Mapping[str, Any]) -> None:
        """Initialize the result from a CompleteMultipartUpload response.

        Args:
            response: The CompleteMultipartUpload response.
        """
        self._set_fields(
            location=response.get("Location"),
            bucket=response.get("Bucket"),
            key=response.get("Key"),
            expiration=response.get("Expiration"),
            version_id=response.get("VersionId"),
            etag=response.get("ETag"),
            **self._checksums(response),
            checksum_type=response.get("ChecksumType"),
            server_side_encryption=response.get("ServerSideEncryption"),
            sse_kms_key_id=response.get("SSEKMSKeyId"),
            bucket_key_enabled=response.get("BucketKeyEnabled"),
            request_charged=response.get("RequestCharged"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert the result to a dictionary.

        Returns:
            A deep copy of the fields, keyed by their names with a leading
            underscore (e.g., ``_etag``).
        """
        return {f"_{k}": v for k, v in dataclasses.asdict(self).items()}
