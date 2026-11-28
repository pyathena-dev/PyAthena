# Copyright 2022 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import copy
import dataclasses
import pickle
from datetime import datetime
from types import MappingProxyType

import pytest

from pyathena.filesystem.s3_object import (
    S3CompleteMultipartUpload,
    S3CopyObject,
    S3DeleteError,
    S3DeleteObject,
    S3Metadata,
    S3MultipartUpload,
    S3MultipartUploadPart,
    S3Object,
    S3ObjectSummary,
    S3ObjectType,
    S3ObjectVersion,
    S3Owner,
    S3PutObject,
    S3StorageClass,
)
from pyathena.filesystem.s3_path import S3Path


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

    @staticmethod
    def _file_object():
        return S3Object(
            init={"ContentLength": 3, "ETag": '"etag"'},
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="test-bucket",
            key="path/to/object",
        )

    def test_mapping(self):
        actual = self._file_object()
        assert actual["etag"] == '"etag"'
        with pytest.raises(KeyError):
            actual["version_id"]
        assert "etag" in actual
        assert "version_id" not in actual
        assert actual.get("etag") == '"etag"'
        assert actual.get("version_id") is None
        assert actual.get("version_id", "default") == "default"
        assert actual.pop("version_id", "default") == "default"
        assert actual.setdefault("version_id", "v1") == "v1"
        assert actual["version_id"] == "v1"
        # GH-1382: a dictionary, whose fields may be named as its methods.
        assert isinstance(actual, dict)
        actual["keys"] = 1
        actual["get"] = 2
        assert dict(actual)["keys"] == 1
        assert actual.get("get") == 2
        # Only the constructor maps the S3 API names.
        actual.update({"ContentType": "text/csv"})
        assert actual["ContentType"] == "text/csv"
        assert actual.content_type is None

    def test_attribute(self):
        actual = self._file_object()
        assert actual.etag == '"etag"'
        # Known fields that the object does not have read as None.
        assert actual.version_id is None
        assert actual.is_latest is None
        assert actual.content_type is None
        assert "content_type" not in actual
        with pytest.raises(AttributeError):
            _ = actual.unknown
        assert not hasattr(actual, "__setstate__")
        # Attributes set and delete the fields, also of unknown names.
        actual.unknown = 1
        assert actual["unknown"] == 1
        assert actual.unknown == 1
        del actual.unknown
        assert "unknown" not in actual
        with pytest.raises(AttributeError):
            del actual.unknown

    @pytest.mark.parametrize(
        "func",
        [
            copy.copy,
            copy.deepcopy,
            lambda obj: pickle.loads(pickle.dumps(obj)),
            lambda obj: obj.copy(),
        ],
        ids=["copy", "deepcopy", "pickle", "copy_method"],
    )
    def test_copy(self, func):
        expected = self._file_object()
        actual = func(expected)
        assert isinstance(actual, S3Object)
        assert actual is not expected
        assert actual == expected
        assert actual.name == "test-bucket/path/to/object"
        actual["name"] = "renamed"
        assert expected.name == "test-bucket/path/to/object"

    def test_copy_method_copies_metadata(self):
        # GH-1382: the user metadata of a copy is its own.
        expected = S3Object(
            init={"Metadata": {"k": "v"}},
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="test-bucket",
            key="path/to/object",
        )
        actual = expected.copy()
        actual["metadata"]["k"] = "changed"
        assert expected["metadata"] == {"k": "v"}
        # Also a mapping that copy.copy() cannot copy.
        backing = {"k": "v"}
        expected["metadata"] = MappingProxyType(backing)
        actual = expected.copy()
        backing["k"] = "changed"
        assert actual["metadata"] == {"k": "v"}

    @pytest.mark.parametrize(
        "listed",
        [
            # In the order in which the entries were built from these fields.
            {
                "Key": "k",
                "ETag": '"e"',
                "Size": 3,
                "StorageClass": "GLACIER",
                "LastModified": datetime(2026, 1, 1),
            },
            # The default storage class follows the last modified time.
            {"Key": "k", "ETag": '"e"', "Size": 3, "LastModified": datetime(2026, 1, 1)},
            {"Key": "k", "Size": 0},
            {"Key": "k"},
        ],
        ids=["full", "default_storage_class", "empty", "sparse"],
    )
    def test_from_summary(self, listed):
        # GH-1382: built without the constructor, with the fields, and their
        # order, that the constructor gives the listed entry.
        summary = S3ObjectSummary.from_response("b", listed)
        expected = S3Object(
            init={k: v for k, v in listed.items() if k != "Key"},
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="b",
            key="k",
        )
        assert list(S3Object.from_summary(summary).items()) == list(expected.items())

    @pytest.mark.parametrize(
        ("version_id", "name"),
        [("v1", "b/k?versionId=v1"), ("null", "b/k")],
    )
    def test_from_version(self, version_id, name):
        listed = {"Key": "k", "VersionId": version_id, "IsLatest": True, "Size": 3}
        version = S3ObjectVersion(bucket="b", is_delete_marker=False, response=listed)
        expected = S3Object(
            init={"Size": 3},
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="b",
            key="k",
            version_id=version_id,
            is_latest=True,
        )
        expected["name"] = name
        assert list(S3Object.from_version(version).items()) == list(expected.items())


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

    def test_user_metadata_is_a_copy(self):
        # GH-1383: the frozen model does not change with the response.
        response = {"Metadata": {"attr1": "value1"}}
        actual = S3Metadata(response)
        response["Metadata"]["attr1"] = "changed"
        assert dict(actual) == {"attr1": "value1"}

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
        # GH-1383: the truth value and the equality are those of the
        # user-defined metadata, whatever the system-defined metadata.
        empty = S3Metadata({"ContentType": "text/plain", "ETag": '"e"'})
        assert not empty
        assert empty == {}
        assert empty == S3Metadata({"ContentType": "application/json"})
        assert actual == S3Metadata({"Metadata": {"attr1": "value1", "attr-2": "value2"}})


class TestS3ObjectSummary:
    def test_from_response(self):
        actual = S3ObjectSummary.from_response(
            "bucket",
            {
                "Key": "key",
                "Size": 1,
                "ETag": '"e"',
                "StorageClass": "STANDARD",
                "ChecksumAlgorithm": ["CRC32"],
                "ChecksumType": "FULL_OBJECT",
            },
        )
        assert actual == S3ObjectSummary(
            "bucket",
            "key",
            size=1,
            etag='"e"',
            storage_class="STANDARD",
            checksum_algorithm=("CRC32",),
            checksum_type="FULL_OBJECT",
        )
        assert S3ObjectSummary.from_response("bucket", {"Key": "key"}).checksum_algorithm == ()


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
                "ChecksumAlgorithm": ["CRC32"],
                "ChecksumType": "FULL_OBJECT",
                "Owner": {"DisplayName": "test_owner", "ID": "test_owner_id"},
            },
        )
        assert actual.checksum_algorithm == ("CRC32",)
        assert actual.checksum_type == "FULL_OBJECT"
        assert actual.bucket == "test_bucket"
        assert actual.key == "test_key"
        assert actual.name == "test_bucket/test_key"
        assert actual.version_id == "test_version_id"
        assert actual.path == S3Path("test_bucket", "test_key", "test_version_id")
        assert actual.is_latest is True
        assert actual.is_delete_marker is False
        assert actual.last_modified == datetime(2015, 1, 1, 0, 0, 0)
        assert actual.etag == '"test_etag"'
        assert actual.size == 100
        assert actual.storage_class == "STANDARD"
        assert actual.owner
        assert actual.owner.display_name == "test_owner"
        assert actual.owner.id == "test_owner_id"


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

    def test_to_dict(self):
        # GH-1383: keyed by the field names.
        actual = S3PutObject({"ETag": '"e"', "VersionId": "v"}).to_dict()
        assert actual["etag"] == '"e"'
        assert actual["version_id"] == "v"
        assert actual["checksum_sha256"] is None
        assert actual.keys() == {
            *_CHECKSUMS.values(),
            "checksum_type",
            "etag",
            "version_id",
            "expiration",
            "server_side_encryption",
            "sse_customer_algorithm",
            "sse_customer_key_md5",
            "sse_kms_key_id",
            "sse_kms_encryption_context",
            "bucket_key_enabled",
            "request_charged",
        }

    def test_equality(self):
        # GH-1383: the results compare and hash by their fields.
        assert S3PutObject({"ETag": '"a"'}) == S3PutObject({"ETag": '"a"'})
        assert S3PutObject({"ETag": '"a"'}) != S3PutObject({"ETag": '"b"'})
        assert hash(S3PutObject({"ETag": '"a"'})) == hash(S3PutObject({"ETag": '"a"'}))


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
                "ChecksumType": "FULL_OBJECT",
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
        assert actual.checksum_type == "FULL_OBJECT"
        assert actual.initiated == datetime(2015, 1, 1, 0, 0, 0)
        assert actual.storage_class == "STANDARD"
        assert actual.owner
        assert actual.owner.display_name == "test_owner"
        assert actual.owner.id == "test_owner_id"
        assert actual.initiator
        assert actual.initiator.display_name == "test_initiator"
        assert actual.initiator.id == "test_initiator_id"


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

    @pytest.mark.parametrize(
        ("field", "property_name"),
        [
            ("ChecksumCRC32", "checksum_crc32"),
            ("ChecksumCRC32C", "checksum_crc32c"),
            ("ChecksumCRC64NVME", "checksum_crc64nvme"),
            ("ChecksumSHA1", "checksum_sha1"),
            ("ChecksumSHA256", "checksum_sha256"),
            ("ChecksumSHA512", "checksum_sha512"),
            ("ChecksumMD5", "checksum_md5"),
            ("ChecksumXXHASH64", "checksum_xxhash64"),
            ("ChecksumXXHASH3", "checksum_xxhash3"),
            ("ChecksumXXHASH128", "checksum_xxhash128"),
        ],
    )
    @pytest.mark.parametrize("copy", [False, True])
    def test_to_api_repr_with_checksum(self, field, property_name, copy):
        result = {"ETag": '"part"', field: "checksum"}
        response = {"CopyPartResult": result} if copy else result
        part = S3MultipartUploadPart(2, response)
        assert getattr(part, property_name) == "checksum"
        assert part.to_api_repr() == {"ETag": '"part"', "PartNumber": 2, field: "checksum"}


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

    def test_to_dict(self):
        # GH-1383: keyed by the field names.
        actual = S3CompleteMultipartUpload({"ETag": '"e"', "VersionId": "v"}).to_dict()
        assert actual["etag"] == '"e"'
        assert actual["version_id"] == "v"
        assert actual["location"] is None
        assert actual.keys() == {
            *_CHECKSUMS.values(),
            "checksum_type",
            "location",
            "etag",
            "version_id",
            "bucket",
            "key",
            "expiration",
            "server_side_encryption",
            "sse_kms_key_id",
            "bucket_key_enabled",
            "request_charged",
        }


class TestS3CopyObject:
    def test_init(self):
        actual = S3CopyObject(
            {
                "VersionId": "test_version_id",
                "CopySourceVersionId": "test_copy_source_version_id",
                "CopyObjectResult": {
                    "ETag": '"test_etag"',
                    "LastModified": datetime(2015, 1, 1, 0, 0, 0),
                },
            }
        )
        assert actual.version_id == "test_version_id"
        assert actual.copy_source_version_id == "test_copy_source_version_id"
        assert actual.etag == '"test_etag"'
        assert actual.last_modified == datetime(2015, 1, 1, 0, 0, 0)

        # An unversioned copy.
        actual = S3CopyObject({"CopyObjectResult": {"ETag": '"test_etag"'}})
        assert actual.version_id is None
        assert actual.copy_source_version_id is None
        assert actual.etag == '"test_etag"'


class TestS3DeleteObject:
    @pytest.mark.parametrize(
        ("response", "version_id", "delete_marker"),
        [
            pytest.param({}, None, False, id="unversioned"),
            pytest.param({"VersionId": "v1"}, "v1", False, id="version"),
            pytest.param({"VersionId": "m1", "DeleteMarker": True}, "m1", True, id="marker"),
        ],
    )
    def test_init(self, response, version_id, delete_marker):
        actual = S3DeleteObject(response)
        assert actual.version_id == version_id
        assert actual.delete_marker is delete_marker


class TestS3DeleteError:
    def test_from_response(self):
        error = S3DeleteError.from_response(
            "bucket", {"Key": "a", "VersionId": "v1", "Code": "InternalError", "Message": "Error"}
        )
        assert error == S3DeleteError(S3Path("bucket", "a", "v1"), "InternalError", "Error")
        assert str(error) == "bucket/a?versionId=v1 (InternalError: Error)"
        assert str(S3DeleteError.from_response("bucket", {"Key": "a"})) == "bucket/a (None: None)"


# The checksum fields of the responses and their names in the models.
_CHECKSUMS = {
    "ChecksumCRC32": "checksum_crc32",
    "ChecksumCRC32C": "checksum_crc32c",
    "ChecksumCRC64NVME": "checksum_crc64nvme",
    "ChecksumSHA1": "checksum_sha1",
    "ChecksumSHA256": "checksum_sha256",
    "ChecksumSHA512": "checksum_sha512",
    "ChecksumMD5": "checksum_md5",
    "ChecksumXXHASH64": "checksum_xxhash64",
    "ChecksumXXHASH3": "checksum_xxhash3",
    "ChecksumXXHASH128": "checksum_xxhash128",
}


@pytest.mark.parametrize(
    ("model", "result_key"),
    [
        pytest.param(S3Metadata, None, id="head_object"),
        pytest.param(S3PutObject, None, id="put_object"),
        pytest.param(S3CopyObject, "CopyObjectResult", id="copy_object"),
        pytest.param(S3CompleteMultipartUpload, None, id="complete_multipart_upload"),
    ],
)
def test_checksums(model, result_key):
    # GH-1383: the checksums and the checksum type that botocore defines for
    # the response of each operation.
    result = {key: f"value of {key}" for key in _CHECKSUMS}
    result["ChecksumType"] = "FULL_OBJECT"
    actual = model({result_key: result} if result_key else result)
    assert {name: getattr(actual, name) for name in _CHECKSUMS.values()} == {
        name: f"value of {key}" for key, name in _CHECKSUMS.items()
    }
    assert actual.checksum_type == "FULL_OBJECT"


@pytest.mark.parametrize("model", [S3PutObject, S3MultipartUpload])
def test_repr_leaves_out_encryption_context(model):
    # GH-1383: the representation, which logs may show, leaves out the
    # encryption context; the field keeps its value.
    actual = model({"SSEKMSEncryptionContext": "secret-context"})
    assert "secret-context" not in repr(actual)
    assert actual.sse_kms_encryption_context == "secret-context"


@pytest.mark.parametrize(
    ("model", "args"),
    [
        pytest.param(S3Metadata, ({"ETag": '"e"', "Metadata": {"a": "b"}},), id="S3Metadata"),
        pytest.param(S3ObjectVersion, ("bucket", False, {"Key": "key"}), id="S3ObjectVersion"),
        pytest.param(S3PutObject, ({},), id="S3PutObject"),
        pytest.param(S3CopyObject, ({},), id="S3CopyObject"),
        pytest.param(S3DeleteObject, ({},), id="S3DeleteObject"),
        pytest.param(
            S3MultipartUpload, ({"Owner": {"DisplayName": "o", "ID": "i"}},), id="S3MultipartUpload"
        ),
        pytest.param(S3Owner, ({"DisplayName": "o", "ID": "i"},), id="S3Owner"),
        pytest.param(S3MultipartUploadPart, (1, {}), id="S3MultipartUploadPart"),
        pytest.param(S3CompleteMultipartUpload, ({},), id="S3CompleteMultipartUpload"),
    ],
)
def test_models_set_every_field(model, args):
    # GH-1383: the constructors read a response into every field, so that the
    # fields compare, hash and convert.
    actual = model(*args)
    assert {f.name for f in dataclasses.fields(actual)} == vars(actual).keys()
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(actual, dataclasses.fields(actual)[0].name, None)
    assert actual == model(*args)
    # Compared by their fields, which the equality of S3Metadata ignores.
    for restored in (copy.copy(actual), pickle.loads(pickle.dumps(actual))):
        assert type(restored) is model
        assert vars(restored) == vars(actual)
