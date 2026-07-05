from datetime import datetime

from pyathena.filesystem.s3_object import (
    S3CompleteMultipartUpload,
    S3Metadata,
    S3MultipartUpload,
    S3MultipartUploadPart,
    S3Object,
    S3ObjectType,
    S3ObjectVersion,
    S3PutObject,
    S3StorageClass,
)


class TestS3Object:
    def test_init(self):
        actual = S3Object(
            init={
                "ContentLength": 0,
                "ContentType": None,
                "StorageClass": S3StorageClass.S3_STORAGE_CLASS_BUCKET,
                "ETag": None,
                "LastModified": None,
            },
            type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
            bucket="test-bucket",
            key=None,
            version_id=None,
        )
        assert actual.type == S3ObjectType.S3_OBJECT_TYPE_DIRECTORY
        assert actual.bucket == "test-bucket"
        assert actual.key is None
        assert actual.name == "test-bucket"
        assert actual.size == 0
        assert actual.content_type is None
        assert actual.storage_class == S3StorageClass.S3_STORAGE_CLASS_BUCKET
        assert actual.etag is None
        assert actual.version_id is None
        assert actual.last_modified is None

        actual = S3Object(
            init={
                "ContentLength": 100,
                "ContentType": "application/json",
                "StorageClass": S3StorageClass.S3_STORAGE_CLASS_STANDARD,
                "ETag": "test-etag",
                "LastModified": datetime(2024, 5, 2, 1, 2, 3),
            },
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="test-bucket",
            key="path/to/object",
            version_id="latest",
        )
        assert actual.type == S3ObjectType.S3_OBJECT_TYPE_FILE
        assert actual.bucket == "test-bucket"
        assert actual.key == "path/to/object"
        assert actual.name == "test-bucket/path/to/object"
        assert actual.size == 100
        assert actual.storage_class == S3StorageClass.S3_STORAGE_CLASS_STANDARD
        assert actual.etag == "test-etag"
        assert actual.version_id == "latest"
        assert actual.last_modified == datetime(2024, 5, 2, 1, 2, 3)

    def test_to_api_repr(self):
        actual = S3Object(
            init={
                "ContentLength": 0,
                "ContentType": None,
                "StorageClass": S3StorageClass.S3_STORAGE_CLASS_BUCKET,
                "ETag": None,
                "LastModified": None,
            },
            type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
            bucket="test-bucket",
            key=None,
            version_id=None,
        )
        assert actual.to_api_repr() == {"StorageClass": "BUCKET"}

        actual = S3Object(
            init={
                "ContentLength": 100,
                "ContentType": "application/json",
                "StorageClass": S3StorageClass.S3_STORAGE_CLASS_STANDARD,
                "ETag": "test-etag",
                "LastModified": datetime(2024, 5, 2, 1, 2, 3),
            },
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="test-bucket",
            key="path/to/object",
            version_id="latest",
        )
        assert actual.to_api_repr() == {
            "ContentType": "application/json",
            "StorageClass": "STANDARD",
        }


class TestS3Metadata:
    def test_init(self):
        actual = S3Metadata(
            {
                "CacheControl": "no-cache",
                "ContentDisposition": "attachment",
                "ContentEncoding": "gzip",
                "ContentLanguage": "en",
                "ContentLength": 1024,
                "ContentType": "text/plain",
                "ETag": '"test_etag"',
                "Expiration": "test_expiration",
                "Expires": datetime(2015, 1, 1, 0, 0, 0),
                "LastModified": datetime(2015, 1, 2, 0, 0, 0),
                "StorageClass": "STANDARD_IA",
                "ServerSideEncryption": "AES256",
                "SSECustomerAlgorithm": "test_sse_customer_algorithm",
                "SSEKMSKeyId": "test_sse_kms_key_id",
                "BucketKeyEnabled": True,
                "WebsiteRedirectLocation": "test_website_redirect_location",
                "VersionId": "test_version_id",
                "Metadata": {"attr1": "value1", "attr-2": "value2"},
            }
        )
        assert actual.cache_control == "no-cache"
        assert actual.content_disposition == "attachment"
        assert actual.content_encoding == "gzip"
        assert actual.content_language == "en"
        assert actual.content_length == 1024
        assert actual.content_type == "text/plain"
        assert actual.etag == '"test_etag"'
        assert actual.expiration == "test_expiration"
        assert actual.expires == datetime(2015, 1, 1, 0, 0, 0)
        assert actual.last_modified == datetime(2015, 1, 2, 0, 0, 0)
        assert actual.storage_class == "STANDARD_IA"
        assert actual.server_side_encryption == "AES256"
        assert actual.sse_customer_algorithm == "test_sse_customer_algorithm"
        assert actual.sse_kms_key_id == "test_sse_kms_key_id"
        assert actual.bucket_key_enabled is True
        assert actual.website_redirect_location == "test_website_redirect_location"
        assert actual.version_id == "test_version_id"
        assert actual.user_metadata == {"attr1": "value1", "attr-2": "value2"}

    def test_init_defaults(self):
        actual = S3Metadata({})
        assert actual.content_type is None
        # StorageClass is omitted from responses for S3 Standard objects.
        assert actual.storage_class == S3StorageClass.S3_STORAGE_CLASS_STANDARD
        assert actual.user_metadata == {}
        assert actual == {}

    def test_mapping_interface(self):
        # The mapping interface exposes the user-defined metadata, so the
        # object is a drop-in for a plain user-metadata dictionary.
        actual = S3Metadata(
            {"ContentType": "text/plain", "Metadata": {"attr1": "value1", "attr-2": "value2"}}
        )
        assert actual["attr1"] == "value1"
        assert actual.get("attr-2") == "value2"
        assert actual.get("missing") is None
        assert "attr1" in actual
        assert len(actual) == 2
        assert sorted(actual) == ["attr-2", "attr1"]
        assert dict(actual) == {"attr1": "value1", "attr-2": "value2"}
        assert actual == {"attr1": "value1", "attr-2": "value2"}
        assert actual != {"attr1": "value1"}
        # System-defined metadata is not part of the mapping.
        assert "content_type" not in actual


class TestS3ObjectVersion:
    def test_init(self):
        actual = S3ObjectVersion(
            bucket="test_bucket",
            is_delete_marker=False,
            response={
                "Key": "test_key",
                "VersionId": "test_version_id",
                "IsLatest": True,
                "LastModified": datetime(2015, 1, 1, 0, 0, 0),
                "ETag": '"test_etag"',
                "Size": 100,
                "StorageClass": "STANDARD",
                "Owner": {"DisplayName": "test_owner", "ID": "test_owner_id"},
            },
        )
        assert actual.bucket == "test_bucket"
        assert actual.key == "test_key"
        assert actual.name == "test_bucket/test_key"
        assert actual.version_id == "test_version_id"
        assert actual.is_latest is True
        assert actual.is_delete_marker is False
        assert actual.last_modified == datetime(2015, 1, 1, 0, 0, 0)
        assert actual.etag == '"test_etag"'
        assert actual.size == 100
        assert actual.storage_class == "STANDARD"
        assert actual.owner
        assert actual.owner.display_name == "test_owner"
        assert actual.owner.id == "test_owner_id"

    def test_init_delete_marker(self):
        actual = S3ObjectVersion(
            bucket="test_bucket",
            is_delete_marker=True,
            response={
                "Key": "test_key",
                "VersionId": "test_version_id",
                "IsLatest": True,
                "LastModified": datetime(2015, 1, 1, 0, 0, 0),
            },
        )
        assert actual.is_delete_marker is True
        assert actual.etag is None
        assert actual.size is None
        assert actual.storage_class is None
        assert actual.owner is None


class TestS3PutObject:
    def test_init(self):
        actual = S3PutObject(
            {
                "Expiration": "test_expiration",
                "ETag": "test_etag",
                "ChecksumCRC32": "test_checksum_crc32",
                "ChecksumCRC32C": "test_checksum_crc32c",
                "ChecksumSHA1": "test_checksum_sha1",
                "ChecksumSHA256": "test_checksum_sha256",
                "ServerSideEncryption": "AES256",
                "VersionId": "test_version_id",
                "SSECustomerAlgorithm": "test_sse_customer_algorithm",
                "SSECustomerKeyMD5": "test_sse_customer_key_md5",
                "SSEKMSKeyId": "test_sse_kms_key_id",
                "SSEKMSEncryptionContext": "test_sse_kms_encryption_context",
                "BucketKeyEnabled": True,
                "RequestCharged": "requester",
            }
        )
        assert actual.expiration == "test_expiration"
        assert actual.etag == "test_etag"
        assert actual.checksum_crc32 == "test_checksum_crc32"
        assert actual.checksum_crc32c == "test_checksum_crc32c"
        assert actual.checksum_sha1 == "test_checksum_sha1"
        assert actual.checksum_sha256 == "test_checksum_sha256"
        assert actual.server_side_encryption == "AES256"
        assert actual.version_id == "test_version_id"
        assert actual.sse_customer_algorithm == "test_sse_customer_algorithm"
        assert actual.sse_customer_key_md5 == "test_sse_customer_key_md5"
        assert actual.sse_kms_key_id == "test_sse_kms_key_id"
        assert actual.sse_kms_encryption_context == "test_sse_kms_encryption_context"
        assert actual.bucket_key_enabled is True
        assert actual.request_charged == "requester"


class TestS3MultipartUpload:
    def test_init(self):
        actual = S3MultipartUpload(
            {
                "AbortDate": datetime(2015, 1, 1, 0, 0, 0),
                "AbortRuleId": "test_abort_rule_id",
                "Bucket": "test_bucket",
                "Key": "test_key",
                "UploadId": "test_upload_id",
                "ServerSideEncryption": "AES256",
                "SSECustomerAlgorithm": "test_sse_customer_algorithm",
                "SSECustomerKeyMD5": "test_sse_customer_key_md5",
                "SSEKMSKeyId": "test_sse_kms_key_id",
                "SSEKMSEncryptionContext": "test_sse_kms_encryption_context",
                "BucketKeyEnabled": True,
                "RequestCharged": "requester",
                "ChecksumAlgorithm": "CRC32",
                "Initiated": datetime(2015, 1, 1, 0, 0, 0),
                "StorageClass": "STANDARD",
                "Owner": {"DisplayName": "test_owner", "ID": "test_owner_id"},
                "Initiator": {"DisplayName": "test_initiator", "ID": "test_initiator_id"},
            }
        )
        assert actual.abort_date == datetime(2015, 1, 1, 0, 0, 0)
        assert actual.abort_rule_id == "test_abort_rule_id"
        assert actual.bucket == "test_bucket"
        assert actual.key == "test_key"
        assert actual.upload_id == "test_upload_id"
        assert actual.server_side_encryption == "AES256"
        assert actual.sse_customer_algorithm == "test_sse_customer_algorithm"
        assert actual.sse_customer_key_md5 == "test_sse_customer_key_md5"
        assert actual.sse_kms_key_id == "test_sse_kms_key_id"
        assert actual.sse_kms_encryption_context == "test_sse_kms_encryption_context"
        assert actual.bucket_key_enabled is True
        assert actual.request_charged == "requester"
        assert actual.checksum_algorithm == "CRC32"
        assert actual.initiated == datetime(2015, 1, 1, 0, 0, 0)
        assert actual.storage_class == "STANDARD"
        assert actual.owner
        assert actual.owner.display_name == "test_owner"
        assert actual.owner.id == "test_owner_id"
        assert actual.initiator
        assert actual.initiator.display_name == "test_initiator"
        assert actual.initiator.id == "test_initiator_id"

    def test_init_without_list_fields(self):
        actual = S3MultipartUpload(
            {
                "Bucket": "test_bucket",
                "Key": "test_key",
                "UploadId": "test_upload_id",
            }
        )
        assert actual.initiated is None
        assert actual.storage_class is None
        assert actual.owner is None
        assert actual.initiator is None


class TestS3MultipartUploadPart:
    def test_init(self):
        # upload_part response
        actual = S3MultipartUploadPart(
            part_number=1,
            response={
                "ServerSideEncryption": "AES256",
                "ETag": "test_etag",
                "ChecksumCRC32": "test_checksum_crc32",
                "ChecksumCRC32C": "test_checksum_crc32c",
                "ChecksumSHA1": "test_checksum_sha1",
                "ChecksumSHA256": "test_checksum_sha256",
                "SSECustomerAlgorithm": "test_sse_customer_algorithm",
                "SSECustomerKeyMD5": "test_sse_customer_key_md5",
                "SSEKMSKeyId": "test_sse_kms_key_id",
                "BucketKeyEnabled": True,
                "RequestCharged": "requester",
            },
        )
        assert actual.part_number == 1
        assert actual.copy_source_version_id is None
        assert actual.server_side_encryption == "AES256"
        assert actual.etag == "test_etag"
        assert actual.checksum_crc32 == "test_checksum_crc32"
        assert actual.checksum_crc32c == "test_checksum_crc32c"
        assert actual.checksum_sha1 == "test_checksum_sha1"
        assert actual.checksum_sha256 == "test_checksum_sha256"
        assert actual.sse_customer_algorithm == "test_sse_customer_algorithm"
        assert actual.sse_customer_key_md5 == "test_sse_customer_key_md5"
        assert actual.sse_kms_key_id == "test_sse_kms_key_id"
        assert actual.bucket_key_enabled is True
        assert actual.request_charged == "requester"

        # upload_part_copy response
        actual = S3MultipartUploadPart(
            part_number=1,
            response={
                "CopySourceVersionId": "test_copy_source_version_id",
                "CopyPartResult": {
                    "ETag": "test_etag",
                    "LastModified": datetime(2015, 1, 1, 0, 0, 0),
                    "ChecksumCRC32": "test_checksum_crc32",
                    "ChecksumCRC32C": "test_checksum_crc32c",
                    "ChecksumSHA1": "test_checksum_sha1",
                    "ChecksumSHA256": "test_checksum_sha256",
                },
                "ServerSideEncryption": "AES256",
                "SSECustomerAlgorithm": "test_sse_customer_algorithm",
                "SSECustomerKeyMD5": "test_sse_customer_key_md5",
                "SSEKMSKeyId": "test_sse_kms_key_id",
                "BucketKeyEnabled": False,
            },
        )
        assert actual.part_number == 1
        assert actual.copy_source_version_id == "test_copy_source_version_id"
        assert actual.server_side_encryption == "AES256"
        assert actual.etag == "test_etag"
        assert actual.checksum_crc32 == "test_checksum_crc32"
        assert actual.checksum_crc32c == "test_checksum_crc32c"
        assert actual.checksum_sha1 == "test_checksum_sha1"
        assert actual.checksum_sha256 == "test_checksum_sha256"
        assert actual.sse_customer_algorithm == "test_sse_customer_algorithm"
        assert actual.sse_customer_key_md5 == "test_sse_customer_key_md5"
        assert actual.sse_kms_key_id == "test_sse_kms_key_id"
        assert actual.bucket_key_enabled is False
        assert actual.request_charged is None


class TestS3CompleteMultipartUpload:
    def test_init(self):
        actual = S3CompleteMultipartUpload(
            {
                "Location": "test_location",
                "Bucket": "test_bucket",
                "Key": "test_key",
                "Expiration": "test_expiration",
                "ETag": "test_etag",
                "ChecksumCRC32": "test_checksum_crc32",
                "ChecksumCRC32C": "test_checksum_crc32c",
                "ChecksumSHA1": "test_checksum_sha1",
                "ChecksumSHA256": "test_checksum_sha256",
                "ServerSideEncryption": "AES256",
                "VersionId": "test_version_id",
                "SSEKMSKeyId": "test_sse_kms_key_id",
                "BucketKeyEnabled": False,
                "RequestCharged": "requester",
            }
        )
        assert actual.location == "test_location"
        assert actual.bucket == "test_bucket"
        assert actual.key == "test_key"
        assert actual.expiration == "test_expiration"
        assert actual.version_id == "test_version_id"
        assert actual.etag == "test_etag"
        assert actual.checksum_crc32 == "test_checksum_crc32"
        assert actual.checksum_crc32c == "test_checksum_crc32c"
        assert actual.checksum_sha1 == "test_checksum_sha1"
        assert actual.checksum_sha256 == "test_checksum_sha256"
        assert actual.sse_kms_key_id == "test_sse_kms_key_id"
        assert actual.bucket_key_enabled is False
        assert actual.request_charged == "requester"
