"""Model classes for S3 objects and S3 API responses used by the S3 filesystem."""

from __future__ import annotations

import copy
import logging
from collections.abc import Iterator, Mapping, MutableMapping
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pyathena.filesystem.s3_path import S3Path
from pyathena.util import override

if TYPE_CHECKING:
    from pyathena.filesystem.s3_core import S3Bucket, S3ObjectSummary

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


class S3Object(MutableMapping[str, Any]):
    """Represents an S3 object with metadata and filesystem-like properties.

    This class provides a dictionary-like interface to S3 object metadata,
    making it easier to work with S3 objects in filesystem operations.
    It handles the mapping between S3 API field names and more pythonic
    property names.

    The object supports both dictionary-style access and property-style
    access to metadata fields like content type, storage class, encryption
    settings, and object lock configurations. Dictionary-style access
    behaves like a dictionary, so a missing key raises KeyError. Property-style
    access returns None for a known field that the object does not have,
    and raises AttributeError for any other missing name.

    Example:
        >>> s3_obj = S3Object({"ContentType": "text/csv", "ContentLength": 1024})
        >>> print(s3_obj.content_type)  # "text/csv"
        >>> print(s3_obj["content_length"])  # 1024
        >>> s3_obj.storage_class = "STANDARD_IA"

    Note:
        This class is primarily used internally by S3FileSystem for
        representing S3 objects in filesystem operations.
    """

    def __init__(
        self,
        init: dict[str, Any],
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
        filtered = {}
        for k, v in init.items():
            if k not in _API_FIELD_TO_S3_OBJECT_PROPERTY:
                continue
            filtered[_API_FIELD_TO_S3_OBJECT_PROPERTY[k]] = v
        if "StorageClass" not in init:
            # https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_ResponseSyntax
            # Amazon S3 returns this header for all objects except for
            # S3 Standard storage class objects.
            filtered[_API_FIELD_TO_S3_OBJECT_PROPERTY["StorageClass"]] = (
                S3StorageClass.S3_STORAGE_CLASS_STANDARD
            )
        super().update(filtered)
        if "Size" in init:
            self.content_length = init["Size"]
            self.size = init["Size"]
        elif "ContentLength" in init:
            self.size = init["ContentLength"]
        else:
            self.content_length = 0
            self.size = 0
        super().update({_API_FIELD_TO_S3_OBJECT_PROPERTY.get(k, k): v for k, v in kwargs.items()})
        if self.get("key") is None:
            self.name = self.get("bucket")
        else:
            self.name = f"{self.get('bucket')}/{self.get('key')}"

    @override
    def get(self, key: str, default: Any = None) -> Any:
        return super().get(key, default)

    @override
    def __getitem__(self, item: str) -> Any:
        return self.__dict__[item]

    def __getattr__(self, item: str) -> Any:
        """Return None for a known field that the object does not have.

        Called only when normal attribute lookup fails, so fields that the
        object has are returned without reaching this method.

        Args:
            item: The attribute name.

        Returns:
            None, if ``item`` is a known S3 object field.

        Raises:
            AttributeError: If ``item`` is not a known S3 object field.
        """
        if item in _S3_OBJECT_FIELDS:
            return None
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {item!r}")

    @override
    def __setitem__(self, key: str, value: Any) -> None:
        self.__dict__[key] = value

    @override
    def __setattr__(self, attr: str, value: Any) -> None:
        self[attr] = value

    @override
    def __delitem__(self, key: str) -> None:
        del self.__dict__[key]

    @override
    def __iter__(self) -> Iterator[str]:
        return iter(self.__dict__.keys())

    @override
    def __len__(self) -> int:
        return len(self.__dict__)

    @override
    def __str__(self):
        return str(self.__dict__)

    def copy(self) -> S3Object:
        """Return a shallow copy of the object.

        Returns:
            A new S3Object with the same fields.
        """
        return copy.copy(self)

    def to_dict(self) -> dict[str, Any]:
        """Convert S3Object to dictionary representation.

        Returns:
            Deep copy of the object's attributes as a dictionary.
        """
        return copy.deepcopy(self.__dict__)

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
    def from_summary(cls, summary: S3ObjectSummary) -> S3Object:
        """Build the file entry of an object listed by ListObjectsV2.

        Args:
            summary: The listed object.

        Returns:
            The file entry, named ``bucket/key``.
        """
        return cls(
            init={
                k: v
                for k, v in {
                    "ETag": summary.etag,
                    "Size": summary.size,
                    "StorageClass": summary.storage_class,
                    "LastModified": summary.last_modified,
                }.items()
                if v is not None
            },
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket=summary.bucket,
            key=summary.key,
        )

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
        file = cls(
            init={
                k: v
                for k, v in {
                    "ETag": version.etag,
                    "Size": version.size,
                    "StorageClass": version.storage_class,
                    "LastModified": version.last_modified,
                }.items()
                if v is not None
            },
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket=version.bucket,
            key=version.key,
            version_id=version.version_id,
            is_latest=version.is_latest,
        )
        if version.version_id != "null":
            file.name = str(S3Path(version.bucket, version.key, version.version_id))
        return file

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


class S3Metadata(Mapping[str, str]):
    """Represents the metadata of an S3 object as returned by HeadObject.

    Behaves as a read-only mapping of the user-defined metadata
    (``x-amz-meta-*``, whose keys are arbitrary user-chosen strings), so it
    is a drop-in for implementations that return the user-defined metadata
    as a plain dictionary, and compares equal to such dictionaries. The
    system-defined metadata (content type, encryption settings, storage
    class, etc.) is exposed as typed properties.

    Example:
        >>> metadata = fs.metadata("s3://bucket/key")
        >>> metadata["attr1"]  # user-defined metadata
        'value1'
        >>> metadata.content_type  # system-defined metadata
        'text/plain'

    See https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html
    """

    def __init__(self, response: dict[str, Any], path: S3Path | None = None) -> None:
        """Initialize the metadata from a HeadObject response.

        Args:
            response: The HeadObject response.
            path: The path that was looked up, if known.
        """
        self._path = path
        self._cache_control: str | None = response.get("CacheControl")
        self._content_disposition: str | None = response.get("ContentDisposition")
        self._content_encoding: str | None = response.get("ContentEncoding")
        self._content_language: str | None = response.get("ContentLanguage")
        self._content_length: int | None = response.get("ContentLength")
        self._content_type: str | None = response.get("ContentType")
        self._etag: str | None = response.get("ETag")
        self._expiration: str | None = response.get("Expiration")
        self._expires: datetime | None = response.get("Expires")
        self._last_modified: datetime | None = response.get("LastModified")
        # https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_ResponseSyntax
        # Amazon S3 returns this header for all objects except for
        # S3 Standard storage class objects.
        self._storage_class: str = response.get(
            "StorageClass", S3StorageClass.S3_STORAGE_CLASS_STANDARD
        )
        self._server_side_encryption: str | None = response.get("ServerSideEncryption")
        self._sse_customer_algorithm: str | None = response.get("SSECustomerAlgorithm")
        self._sse_kms_key_id: str | None = response.get("SSEKMSKeyId")
        self._bucket_key_enabled: bool | None = response.get("BucketKeyEnabled")
        self._website_redirect_location: str | None = response.get("WebsiteRedirectLocation")
        self._version_id: str | None = response.get("VersionId")
        self._object_lock_mode: str | None = response.get("ObjectLockMode")
        self._object_lock_retain_until_date: datetime | None = response.get(
            "ObjectLockRetainUntilDate"
        )
        self._object_lock_legal_hold_status: str | None = response.get("ObjectLockLegalHoldStatus")
        self._user_metadata: dict[str, str] = response.get("Metadata", {})

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
    def cache_control(self) -> str | None:
        """The ``CacheControl`` header of the object."""
        return self._cache_control

    @property
    def content_disposition(self) -> str | None:
        """The ``ContentDisposition`` header of the object."""
        return self._content_disposition

    @property
    def content_encoding(self) -> str | None:
        """The ``ContentEncoding`` header of the object."""
        return self._content_encoding

    @property
    def content_language(self) -> str | None:
        """The ``ContentLanguage`` header of the object."""
        return self._content_language

    @property
    def content_length(self) -> int | None:
        """The ``ContentLength`` of the object in bytes."""
        return self._content_length

    @property
    def content_type(self) -> str | None:
        """The ``ContentType`` of the object."""
        return self._content_type

    @property
    def etag(self) -> str | None:
        """The ``ETag`` (entity tag) of the object."""
        return self._etag

    @property
    def expiration(self) -> str | None:
        """The ``Expiration`` header of the object."""
        return self._expiration

    @property
    def expires(self) -> datetime | None:
        """The ``Expires`` date of the object."""
        return self._expires

    @property
    def last_modified(self) -> datetime | None:
        """The ``LastModified`` time of the object."""
        return self._last_modified

    @property
    def storage_class(self) -> str:
        """The ``StorageClass`` of the object; ``STANDARD`` when S3 omits it."""
        return self._storage_class

    @property
    def server_side_encryption(self) -> str | None:
        """The ``ServerSideEncryption`` algorithm of the object."""
        return self._server_side_encryption

    @property
    def sse_customer_algorithm(self) -> str | None:
        """The ``SSECustomerAlgorithm`` of the object."""
        return self._sse_customer_algorithm

    @property
    def sse_kms_key_id(self) -> str | None:
        """The ``SSEKMSKeyId`` of the KMS key for the object."""
        return self._sse_kms_key_id

    @property
    def bucket_key_enabled(self) -> bool | None:
        """Whether the object uses an S3 Bucket Key (``BucketKeyEnabled``)."""
        return self._bucket_key_enabled

    @property
    def website_redirect_location(self) -> str | None:
        """The ``WebsiteRedirectLocation`` of the object."""
        return self._website_redirect_location

    @property
    def version_id(self) -> str | None:
        """The ``VersionId`` of the object."""
        return self._version_id

    @property
    def path(self) -> S3Path | None:
        """The path that was looked up, or None if it is not known."""
        return self._path

    @property
    def object_lock_mode(self) -> str | None:
        """The ``ObjectLockMode`` of the object."""
        return self._object_lock_mode

    @property
    def object_lock_retain_until_date(self) -> datetime | None:
        """The ``ObjectLockRetainUntilDate`` of the object."""
        return self._object_lock_retain_until_date

    @property
    def object_lock_legal_hold_status(self) -> str | None:
        """The ``ObjectLockLegalHoldStatus`` of the object."""
        return self._object_lock_legal_hold_status

    @property
    def user_metadata(self) -> dict[str, str]:
        """A copy of the user-defined metadata (``x-amz-meta-*``).

        The keys are arbitrary user-chosen strings, returned as stored in S3
        (S3 normalizes them to lowercase). The same key/value pairs are also
        accessible directly through the mapping interface of this class.
        """
        return dict(self._user_metadata)


class S3ObjectVersion:
    """Represents a version of an S3 object as returned by ListObjectVersions.

    Attributes:
        bucket: S3 bucket name.
        key: Object key.
        version_id: The version ID of the object.
        is_latest: Whether the version is the latest version of the object.
        is_delete_marker: Whether the version is a delete marker.
        last_modified: Date and time when the version was last modified.
        etag: Entity tag of the version. None for delete markers.
        size: Size in bytes of the version. None for delete markers.
        storage_class: Storage class of the version. None for delete markers.
        owner: Owner of the version.
    """

    def __init__(self, bucket: str, is_delete_marker: bool, response: dict[str, Any]) -> None:
        """Initialize the version from a ListObjectVersions entry.

        Args:
            bucket: The name of the bucket that contains the version.
            is_delete_marker: Whether the entry comes from ``DeleteMarkers``
                rather than ``Versions``.
            response: A ``Versions`` or ``DeleteMarkers`` entry of the
                ListObjectVersions response.
        """
        self._bucket = bucket
        self._is_delete_marker = is_delete_marker
        self._key: str = response["Key"]
        self._version_id: str | None = response.get("VersionId")
        self._is_latest: bool = response.get("IsLatest", False)
        self._last_modified: datetime | None = response.get("LastModified")
        self._etag: str | None = response.get("ETag")
        self._size: int | None = response.get("Size")
        self._storage_class: str | None = response.get("StorageClass")
        owner = response.get("Owner")
        self._owner: S3Owner | None = S3Owner(owner) if owner else None

    @property
    def bucket(self) -> str:
        """The name of the bucket that contains the version."""
        return self._bucket

    @property
    def key(self) -> str:
        """The ``Key`` of the object."""
        return self._key

    @property
    def name(self) -> str:
        """The path of the version in ``bucket/key`` form."""
        return f"{self._bucket}/{self._key}"

    @property
    def version_id(self) -> str | None:
        """The ``VersionId`` of the version."""
        return self._version_id

    @property
    def is_latest(self) -> bool:
        """Whether the version is the latest version of the object (``IsLatest``)."""
        return self._is_latest

    @property
    def is_delete_marker(self) -> bool:
        """Whether the version is a delete marker."""
        return self._is_delete_marker

    @property
    def last_modified(self) -> datetime | None:
        """The ``LastModified`` time of the version."""
        return self._last_modified

    @property
    def etag(self) -> str | None:
        """The ``ETag`` of the version; None for delete markers."""
        return self._etag

    @property
    def size(self) -> int | None:
        """The ``Size`` of the version in bytes; None for delete markers."""
        return self._size

    @property
    def storage_class(self) -> str | None:
        """The ``StorageClass`` of the version; None for delete markers."""
        return self._storage_class

    @property
    def owner(self) -> S3Owner | None:
        """The ``Owner`` of the version, or None if the response has none."""
        return self._owner


class S3PutObject:
    """Represents the response from an S3 PUT object operation.

    This class encapsulates the metadata returned when uploading an object
    to S3, including encryption details, versioning information, and
    integrity checksums.

    Attributes:
        expiration: Object expiration time if lifecycle policy applies.
        version_id: Version ID if bucket versioning is enabled.
        etag: Entity tag for the uploaded object.
        server_side_encryption: Server-side encryption method used.
        Various checksum properties: For data integrity verification.

    Note:
        This class is used internally by S3FileSystem operations and
        typically not instantiated directly by users.
    """

    def __init__(self, response: dict[str, Any]) -> None:
        """Initialize the result from a PutObject response.

        Args:
            response: The PutObject response.
        """
        self._expiration: str | None = response.get("Expiration")
        self._version_id: str | None = response.get("VersionId")
        self._etag: str | None = response.get("ETag")
        self._checksum_crc32: str | None = response.get("ChecksumCRC32")
        self._checksum_crc32c: str | None = response.get("ChecksumCRC32C")
        self._checksum_sha1: str | None = response.get("ChecksumSHA1")
        self._checksum_sha256: str | None = response.get("ChecksumSHA256")
        self._server_side_encryption = response.get("ServerSideEncryption")
        self._sse_customer_algorithm = response.get("SSECustomerAlgorithm")
        self._sse_customer_key_md5 = response.get("SSECustomerKeyMD5")
        self._sse_kms_key_id = response.get("SSEKMSKeyId")
        self._sse_kms_encryption_context = response.get("SSEKMSEncryptionContext")
        self._bucket_key_enabled = response.get("BucketKeyEnabled")
        self._request_charged = response.get("RequestCharged")

    @property
    def expiration(self) -> str | None:
        """The ``Expiration`` header of the uploaded object."""
        return self._expiration

    @property
    def version_id(self) -> str | None:
        """The ``VersionId`` of the uploaded object."""
        return self._version_id

    @property
    def etag(self) -> str | None:
        """The ``ETag`` of the uploaded object."""
        return self._etag

    @property
    def checksum_crc32(self) -> str | None:
        """The ``ChecksumCRC32`` of the uploaded object."""
        return self._checksum_crc32

    @property
    def checksum_crc32c(self) -> str | None:
        """The ``ChecksumCRC32C`` of the uploaded object."""
        return self._checksum_crc32c

    @property
    def checksum_sha1(self) -> str | None:
        """The ``ChecksumSHA1`` of the uploaded object."""
        return self._checksum_sha1

    @property
    def checksum_sha256(self) -> str | None:
        """The ``ChecksumSHA256`` of the uploaded object."""
        return self._checksum_sha256

    @property
    def server_side_encryption(self) -> str | None:
        """The ``ServerSideEncryption`` algorithm of the uploaded object."""
        return self._server_side_encryption

    @property
    def sse_customer_algorithm(self) -> str | None:
        """The ``SSECustomerAlgorithm`` of the uploaded object."""
        return self._sse_customer_algorithm

    @property
    def sse_customer_key_md5(self) -> str | None:
        """The ``SSECustomerKeyMD5`` of the customer-provided key for the uploaded object."""
        return self._sse_customer_key_md5

    @property
    def sse_kms_key_id(self) -> str | None:
        """The ``SSEKMSKeyId`` of the KMS key for the uploaded object."""
        return self._sse_kms_key_id

    @property
    def sse_kms_encryption_context(self) -> str | None:
        """The ``SSEKMSEncryptionContext`` of the uploaded object."""
        return self._sse_kms_encryption_context

    @property
    def bucket_key_enabled(self) -> bool | None:
        """Whether the uploaded object uses an S3 Bucket Key (``BucketKeyEnabled``)."""
        return self._bucket_key_enabled

    @property
    def request_charged(self) -> str | None:
        """The ``RequestCharged`` field of the response."""
        return self._request_charged

    def to_dict(self) -> dict[str, Any]:
        """Convert the response to a dictionary.

        Returns:
            Deep copy of the instance attributes, keyed by their attribute
            names (e.g., ``_etag``).
        """
        return copy.deepcopy(self.__dict__)


class S3Owner:
    """Represents the owner or initiator of an S3 object or multipart upload.

    Attributes:
        display_name: The display name of the owner.
        id: The canonical user ID of the owner.
    """

    def __init__(self, response: dict[str, Any]) -> None:
        """Initialize the owner from an ``Owner`` or ``Initiator`` response field.

        Args:
            response: The ``Owner`` or ``Initiator`` field of an S3 API response.
        """
        self._display_name: str | None = response.get("DisplayName")
        self._id: str | None = response.get("ID")

    @property
    def display_name(self) -> str | None:
        """The ``DisplayName`` of the owner."""
        return self._display_name

    @property
    def id(self) -> str | None:
        """The canonical user ``ID`` of the owner."""
        return self._id


class S3MultipartUpload:
    """Represents an S3 multipart upload operation.

    This class manages the metadata for multipart uploads, which allow
    uploading large files in chunks for better reliability and performance.
    It tracks upload identifiers, encryption settings, and lifecycle rules.

    Attributes:
        bucket: S3 bucket name for the upload.
        key: Object key being uploaded.
        upload_id: Unique identifier for the multipart upload.
        server_side_encryption: Encryption method applied to the upload.
        abort_date/abort_rule_id: Lifecycle rule information for upload cleanup.
        initiated/storage_class/owner/initiator: Fields returned by the
            ListMultipartUploads API for in-progress uploads.

    Note:
        Used internally by S3FileSystem for large file upload operations,
        and returned by ``S3FileSystem.list_multipart_uploads``.
    """

    def __init__(self, response: dict[str, Any]) -> None:
        """Initialize the upload from an S3 API response.

        Args:
            response: A CreateMultipartUpload response or an ``Uploads`` entry
                of a ListMultipartUploads response.
        """
        self._abort_date = response.get("AbortDate")
        self._abort_rule_id = response.get("AbortRuleId")
        self._bucket = response.get("Bucket")
        self._key = response.get("Key")
        self._upload_id = response.get("UploadId")
        self._server_side_encryption = response.get("ServerSideEncryption")
        self._sse_customer_algorithm = response.get("SSECustomerAlgorithm")
        self._sse_customer_key_md5 = response.get("SSECustomerKeyMD5")
        self._sse_kms_key_id = response.get("SSEKMSKeyId")
        self._sse_kms_encryption_context = response.get("SSEKMSEncryptionContext")
        self._bucket_key_enabled = response.get("BucketKeyEnabled")
        self._request_charged = response.get("RequestCharged")
        self._checksum_algorithm = response.get("ChecksumAlgorithm")
        self._checksum_type = response.get("ChecksumType")
        # The following fields are returned by the ListMultipartUploads API.
        self._initiated: datetime | None = response.get("Initiated")
        self._storage_class: str | None = response.get("StorageClass")
        owner = response.get("Owner")
        self._owner: S3Owner | None = S3Owner(owner) if owner else None
        initiator = response.get("Initiator")
        self._initiator: S3Owner | None = S3Owner(initiator) if initiator else None

    @property
    def abort_date(self) -> datetime | None:
        """The ``AbortDate`` of the upload set by a lifecycle rule."""
        return self._abort_date

    @property
    def abort_rule_id(self) -> str | None:
        """The ``AbortRuleId`` of the lifecycle rule that applies to the upload."""
        return self._abort_rule_id

    @property
    def bucket(self) -> str | None:
        """The ``Bucket`` of the upload."""
        return self._bucket

    @property
    def key(self) -> str | None:
        """The ``Key`` of the object being uploaded."""
        return self._key

    @property
    def upload_id(self) -> str | None:
        """The ``UploadId`` of the multipart upload."""
        return self._upload_id

    @property
    def server_side_encryption(self) -> str | None:
        """The ``ServerSideEncryption`` algorithm of the upload."""
        return self._server_side_encryption

    @property
    def sse_customer_algorithm(self) -> str | None:
        """The ``SSECustomerAlgorithm`` of the upload."""
        return self._sse_customer_algorithm

    @property
    def sse_customer_key_md5(self) -> str | None:
        """The ``SSECustomerKeyMD5`` of the customer-provided key for the upload."""
        return self._sse_customer_key_md5

    @property
    def sse_kms_key_id(self) -> str | None:
        """The ``SSEKMSKeyId`` of the KMS key for the upload."""
        return self._sse_kms_key_id

    @property
    def sse_kms_encryption_context(self) -> str | None:
        """The ``SSEKMSEncryptionContext`` of the upload."""
        return self._sse_kms_encryption_context

    @property
    def bucket_key_enabled(self) -> bool | None:
        """Whether the upload uses an S3 Bucket Key (``BucketKeyEnabled``)."""
        return self._bucket_key_enabled

    @property
    def request_charged(self) -> str | None:
        """The ``RequestCharged`` field of the response."""
        return self._request_charged

    @property
    def checksum_algorithm(self) -> str | None:
        """The ``ChecksumAlgorithm`` of the upload."""
        return self._checksum_algorithm

    @property
    def checksum_type(self) -> str | None:
        """The ``ChecksumType`` of the upload: COMPOSITE or FULL_OBJECT."""
        return self._checksum_type

    @property
    def initiated(self) -> datetime | None:
        """The ``Initiated`` time of the upload, returned by ListMultipartUploads."""
        return self._initiated

    @property
    def storage_class(self) -> str | None:
        """The ``StorageClass`` of the upload, returned by ListMultipartUploads."""
        return self._storage_class

    @property
    def owner(self) -> S3Owner | None:
        """The ``Owner`` of the upload, returned by ListMultipartUploads."""
        return self._owner

    @property
    def initiator(self) -> S3Owner | None:
        """The ``Initiator`` of the upload, returned by ListMultipartUploads."""
        return self._initiator


class S3MultipartUploadPart:
    """Represents a single part in an S3 multipart upload operation.

    Each part in a multipart upload has its own metadata including checksums,
    encryption details, and part identification. This class manages that
    metadata and provides methods to convert it to API-compatible formats.

    Attributes:
        part_number: The sequential part number (1-based).
        etag: Entity tag for this specific part.
        checksum_*: Various integrity checksums for the part data.
        server_side_encryption: Encryption settings for this part.

    Note:
        Parts must be at least 5MB except for the last part. Used internally
        by S3FileSystem for chunked upload operations.
    """

    def __init__(self, part_number: int, response: dict[str, Any]) -> None:
        """Initialize the part from an UploadPart or UploadPartCopy response.

        For an UploadPartCopy response, the ``ETag``, ``LastModified`` and
        checksums are read from its ``CopyPartResult``.

        Args:
            part_number: The part number of the part.
            response: The UploadPart or UploadPartCopy response.
        """
        self._part_number = part_number
        self._copy_source_version_id: str | None = response.get("CopySourceVersionId")
        copy_part_result = response.get("CopyPartResult")
        self._last_modified: datetime | None = (
            copy_part_result.get("LastModified") if copy_part_result else None
        )
        part_result = copy_part_result or response
        self._etag: str | None = part_result.get("ETag")
        self._checksum_crc32: str | None = part_result.get("ChecksumCRC32")
        self._checksum_crc32c: str | None = part_result.get("ChecksumCRC32C")
        self._checksum_crc64nvme: str | None = part_result.get("ChecksumCRC64NVME")
        self._checksum_sha1: str | None = part_result.get("ChecksumSHA1")
        self._checksum_sha256: str | None = part_result.get("ChecksumSHA256")
        self._checksum_sha512: str | None = part_result.get("ChecksumSHA512")
        self._checksum_md5: str | None = part_result.get("ChecksumMD5")
        self._checksum_xxhash64: str | None = part_result.get("ChecksumXXHASH64")
        self._checksum_xxhash3: str | None = part_result.get("ChecksumXXHASH3")
        self._checksum_xxhash128: str | None = part_result.get("ChecksumXXHASH128")
        self._server_side_encryption: str | None = response.get("ServerSideEncryption")
        self._sse_customer_algorithm: str | None = response.get("SSECustomerAlgorithm")
        self._sse_customer_key_md5: str | None = response.get("SSECustomerKeyMD5")
        self._sse_kms_key_id: str | None = response.get("SSEKMSKeyId")
        self._bucket_key_enabled: bool | None = response.get("BucketKeyEnabled")
        self._request_charged: str | None = response.get("RequestCharged")

    @property
    def part_number(self) -> int:
        """The part number of the part."""
        return self._part_number

    @property
    def copy_source_version_id(self) -> str | None:
        """The ``CopySourceVersionId`` of the source object of a copied part."""
        return self._copy_source_version_id

    @property
    def last_modified(self) -> datetime | None:
        """The ``LastModified`` time from ``CopyPartResult``; None for uploaded parts."""
        return self._last_modified

    @property
    def etag(self) -> str | None:
        """The ``ETag`` of the part."""
        return self._etag

    @property
    def checksum_crc32(self) -> str | None:
        """The ``ChecksumCRC32`` of the part."""
        return self._checksum_crc32

    @property
    def checksum_crc32c(self) -> str | None:
        """The ``ChecksumCRC32C`` of the part."""
        return self._checksum_crc32c

    @property
    def checksum_sha1(self) -> str | None:
        """The ``ChecksumSHA1`` of the part."""
        return self._checksum_sha1

    @property
    def checksum_sha256(self) -> str | None:
        """The ``ChecksumSHA256`` of the part."""
        return self._checksum_sha256

    @property
    def checksum_crc64nvme(self) -> str | None:
        """The ``ChecksumCRC64NVME`` of the part."""
        return self._checksum_crc64nvme

    @property
    def checksum_sha512(self) -> str | None:
        """The ``ChecksumSHA512`` of the part."""
        return self._checksum_sha512

    @property
    def checksum_md5(self) -> str | None:
        """The ``ChecksumMD5`` of the part."""
        return self._checksum_md5

    @property
    def checksum_xxhash64(self) -> str | None:
        """The ``ChecksumXXHASH64`` of the part."""
        return self._checksum_xxhash64

    @property
    def checksum_xxhash3(self) -> str | None:
        """The ``ChecksumXXHASH3`` of the part."""
        return self._checksum_xxhash3

    @property
    def checksum_xxhash128(self) -> str | None:
        """The ``ChecksumXXHASH128`` of the part."""
        return self._checksum_xxhash128

    @property
    def server_side_encryption(self) -> str | None:
        """The ``ServerSideEncryption`` algorithm of the part."""
        return self._server_side_encryption

    @property
    def sse_customer_algorithm(self) -> str | None:
        """The ``SSECustomerAlgorithm`` of the part."""
        return self._sse_customer_algorithm

    @property
    def sse_customer_key_md5(self) -> str | None:
        """The ``SSECustomerKeyMD5`` of the customer-provided key for the part."""
        return self._sse_customer_key_md5

    @property
    def sse_kms_key_id(self) -> str | None:
        """The ``SSEKMSKeyId`` of the KMS key for the part."""
        return self._sse_kms_key_id

    @property
    def bucket_key_enabled(self) -> bool | None:
        """Whether the part uses an S3 Bucket Key (``BucketKeyEnabled``)."""
        return self._bucket_key_enabled

    @property
    def request_charged(self) -> str | None:
        """The ``RequestCharged`` field of the response."""
        return self._request_charged

    def to_api_repr(self) -> dict[str, Any]:
        """Convert the part to a part entry of a CompleteMultipartUpload request.

        Returns:
            Dictionary with the ``ETag``, checksum and ``PartNumber`` fields of
            the part, omitting fields whose value is None.
        """
        fields = {
            "ETag": self.etag,
            "ChecksumCRC32": self.checksum_crc32,
            "ChecksumCRC32C": self.checksum_crc32c,
            "ChecksumCRC64NVME": self.checksum_crc64nvme,
            "ChecksumSHA1": self.checksum_sha1,
            "ChecksumSHA256": self.checksum_sha256,
            "ChecksumSHA512": self.checksum_sha512,
            "ChecksumMD5": self.checksum_md5,
            "ChecksumXXHASH64": self.checksum_xxhash64,
            "ChecksumXXHASH3": self.checksum_xxhash3,
            "ChecksumXXHASH128": self.checksum_xxhash128,
            "PartNumber": self.part_number,
        }
        return {key: value for key, value in fields.items() if value is not None}


class S3CompleteMultipartUpload:
    """Represents the completion of an S3 multipart upload operation.

    This class encapsulates the final response when a multipart upload is
    completed, including the final object location, versioning information,
    and consolidated metadata from all parts.

    Attributes:
        location: Final S3 URL of the completed object.
        bucket: S3 bucket containing the object.
        key: Final object key.
        version_id: Version ID if bucket versioning is enabled.
        etag: Final entity tag of the complete object.
        server_side_encryption: Encryption applied to the final object.

    Note:
        This represents the successful completion of a multipart upload.
        Used internally by S3FileSystem operations.
    """

    def __init__(self, response: dict[str, Any]) -> None:
        """Initialize the result from a CompleteMultipartUpload response.

        Args:
            response: The CompleteMultipartUpload response.
        """
        self._location: str | None = response.get("Location")
        self._bucket: str | None = response.get("Bucket")
        self._key: str | None = response.get("Key")
        self._expiration: str | None = response.get("Expiration")
        self._version_id: str | None = response.get("VersionId")
        self._etag: str | None = response.get("ETag")
        self._checksum_crc32: str | None = response.get("ChecksumCRC32")
        self._checksum_crc32c: str | None = response.get("ChecksumCRC32C")
        self._checksum_sha1: str | None = response.get("ChecksumSHA1")
        self._checksum_sha256: str | None = response.get("ChecksumSHA256")
        self._server_side_encryption = response.get("ServerSideEncryption")
        self._sse_kms_key_id = response.get("SSEKMSKeyId")
        self._bucket_key_enabled = response.get("BucketKeyEnabled")
        self._request_charged = response.get("RequestCharged")

    @property
    def location(self) -> str | None:
        """The ``Location`` URI of the completed object."""
        return self._location

    @property
    def bucket(self) -> str | None:
        """The ``Bucket`` of the completed object."""
        return self._bucket

    @property
    def key(self) -> str | None:
        """The ``Key`` of the completed object."""
        return self._key

    @property
    def expiration(self) -> str | None:
        """The ``Expiration`` header of the completed object."""
        return self._expiration

    @property
    def version_id(self) -> str | None:
        """The ``VersionId`` of the completed object."""
        return self._version_id

    @property
    def etag(self) -> str | None:
        """The ``ETag`` of the completed object."""
        return self._etag

    @property
    def checksum_crc32(self) -> str | None:
        """The ``ChecksumCRC32`` of the completed object."""
        return self._checksum_crc32

    @property
    def checksum_crc32c(self) -> str | None:
        """The ``ChecksumCRC32C`` of the completed object."""
        return self._checksum_crc32c

    @property
    def checksum_sha1(self) -> str | None:
        """The ``ChecksumSHA1`` of the completed object."""
        return self._checksum_sha1

    @property
    def checksum_sha256(self) -> str | None:
        """The ``ChecksumSHA256`` of the completed object."""
        return self._checksum_sha256

    @property
    def server_side_encryption(self) -> str | None:
        """The ``ServerSideEncryption`` algorithm of the completed object."""
        return self._server_side_encryption

    @property
    def sse_kms_key_id(self) -> str | None:
        """The ``SSEKMSKeyId`` of the KMS key for the completed object."""
        return self._sse_kms_key_id

    @property
    def bucket_key_enabled(self) -> bool | None:
        """Whether the completed object uses an S3 Bucket Key (``BucketKeyEnabled``)."""
        return self._bucket_key_enabled

    @property
    def request_charged(self) -> str | None:
        """The ``RequestCharged`` field of the response."""
        return self._request_charged

    def to_dict(self):
        """Convert the response to a dictionary.

        Returns:
            Deep copy of the instance attributes, keyed by their attribute
            names (e.g., ``_etag``).
        """
        return copy.deepcopy(self.__dict__)
