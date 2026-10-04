# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from datetime import UTC, datetime

import boto3
import botocore.exceptions
import pytest
from botocore.stub import Stubber

from pyathena.filesystem.s3_core import (
    S3Bucket,
    S3CommonPrefix,
    S3Core,
    S3ListBucketsPage,
    S3ListObjectsPage,
    S3ListObjectVersionsPage,
    S3ObjectSummary,
)
from pyathena.filesystem.s3_path import S3Path
from pyathena.util import RetryConfig

MODIFIED = datetime(2026, 10, 4, tzinfo=UTC)


def _make_core(**kwargs):
    client = boto3.client(
        "s3", region_name="us-east-1", aws_access_key_id="dummy", aws_secret_access_key="dummy"
    )
    return S3Core(client, retry_config=RetryConfig(attempt=1), **kwargs), Stubber(client)


class TestS3Core:
    @pytest.mark.parametrize(
        ("method", "params", "expected"),
        [
            (
                "get_object",
                {"ServerSideEncryption": "AES256", "RequestPayer": "requester", "IfMatch": '"e"'},
                {"RequestPayer": "requester", "IfMatch": '"e"'},
            ),
            ("head_bucket", {"RequestPayer": "requester"}, {}),
            (
                "upload_part",
                {"ContentType": "text/csv", "SSECustomerAlgorithm": "AES256"},
                {"SSECustomerAlgorithm": "AES256"},
            ),
            # Not an S3 API operation.
            ("generate_presigned_url", {"RequestPayer": "requester"}, {}),
        ],
    )
    def test_operation_params(self, method, params, expected):
        core, _ = _make_core()
        assert core.operation_params(method, params) == expected

    def test_properties(self):
        core, _ = _make_core(request_kwargs={"RequestPayer": "requester"})
        assert core.retry_config.attempt == 1
        assert core.request_kwargs == {"RequestPayer": "requester"}
        # A copy, so the core keeps its parameters.
        core.request_kwargs.clear()
        assert core.request_kwargs == {"RequestPayer": "requester"}
        assert S3Core(core.client).retry_config.attempt == RetryConfig().attempt

    def test_request_kwargs_go_to_operations_that_accept_them(self):
        core, stubber = _make_core(request_kwargs={"RequestPayer": "requester"})
        stubber.add_response(
            "head_object",
            {"ContentLength": 1},
            {"Bucket": "bucket", "Key": "key", "RequestPayer": "requester"},
        )
        # HeadBucket does not accept RequestPayer.
        stubber.add_response("head_bucket", {}, {"Bucket": "bucket"})
        with stubber:
            core.head_object(S3Path("bucket", "key"))
            core.head_bucket("bucket")
        stubber.assert_no_pending_responses()

    def test_call_params_are_sent_as_given(self):
        # A per-call parameter is not filtered, so botocore rejects a
        # misspelled one instead of the core dropping it.
        core, _ = _make_core()
        with pytest.raises(botocore.exceptions.ParamValidationError):
            core.head_object(S3Path("bucket", "key"), RequestPayers="requester")

    @pytest.mark.parametrize(
        ("status", "code", "error"),
        [
            (404, "404", FileNotFoundError),
            (404, "NoSuchKey", FileNotFoundError),
            (403, "403", PermissionError),
            (500, "InternalError", OSError),
        ],
    )
    def test_call_translates_errors(self, status, code, error):
        core, stubber = _make_core()
        stubber.add_client_error("head_object", service_error_code=code, http_status_code=status)
        with stubber, pytest.raises(error):
            core.head_object(S3Path("bucket", "key"))

    def test_head_object(self):
        core, stubber = _make_core()
        stubber.add_response(
            "head_object",
            {
                "ContentLength": 4,
                "ETag": '"e"',
                "VersionId": "v1",
                "ObjectLockMode": "GOVERNANCE",
                "ObjectLockRetainUntilDate": MODIFIED,
                "ObjectLockLegalHoldStatus": "ON",
                "Metadata": {"a": "1"},
            },
            {"Bucket": "bucket", "Key": "key", "VersionId": "v1", "IfMatch": '"e"'},
        )
        with stubber:
            path = S3Path("bucket", "key", "v1")
            metadata = core.head_object(path, IfMatch='"e"')
        assert metadata.path == path
        assert (metadata.content_length, metadata.version_id, dict(metadata)) == (
            4,
            "v1",
            {"a": "1"},
        )
        assert (
            metadata.object_lock_mode,
            metadata.object_lock_retain_until_date,
            metadata.object_lock_legal_hold_status,
        ) == ("GOVERNANCE", MODIFIED, "ON")

    def test_head_object_requires_a_key(self):
        core, _ = _make_core()
        with pytest.raises(ValueError, match="no key"):
            core.head_object(S3Path("bucket"))

    def test_head_bucket(self):
        core, stubber = _make_core()
        stubber.add_response("head_bucket", {"BucketRegion": "us-west-2"}, {"Bucket": "bucket"})
        stubber.add_client_error("head_bucket", service_error_code="404", http_status_code=404)
        with stubber:
            assert core.head_bucket("bucket") == S3Bucket("bucket", bucket_region="us-west-2")
            with pytest.raises(FileNotFoundError):
                core.head_bucket("bucket")

    def test_list_objects(self):
        core, stubber = _make_core()
        stubber.add_response(
            "list_objects_v2",
            {
                "Contents": [{"Key": "dir/a", "Size": 1, "ETag": '"a"', "LastModified": MODIFIED}],
                "CommonPrefixes": [{"Prefix": "dir/sub/"}],
                "KeyCount": 2,
                "IsTruncated": True,
                "NextContinuationToken": "t1",
            },
            {"Bucket": "bucket", "Prefix": "dir/", "Delimiter": "/", "MaxKeys": 2},
        )
        stubber.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": "dir/b", "Size": 2}], "KeyCount": 1, "IsTruncated": False},
            {
                "Bucket": "bucket",
                "Prefix": "dir/",
                "Delimiter": "/",
                "MaxKeys": 2,
                "ContinuationToken": "t1",
            },
        )
        with stubber:
            pages = list(core.list_objects("bucket", prefix="dir/", delimiter="/", max_keys=2))
        stubber.assert_no_pending_responses()
        assert pages == [
            S3ListObjectsPage(
                bucket="bucket",
                objects=(
                    S3ObjectSummary(
                        bucket="bucket", key="dir/a", size=1, etag='"a"', last_modified=MODIFIED
                    ),
                ),
                common_prefixes=(S3CommonPrefix("bucket", "dir/sub/"),),
                key_count=2,
                is_truncated=True,
                next_continuation_token="t1",
            ),
            S3ListObjectsPage(
                bucket="bucket",
                objects=(S3ObjectSummary(bucket="bucket", key="dir/b", size=2),),
                key_count=1,
            ),
        ]
        assert pages[0].objects[0].path == S3Path("bucket", "dir/a")

    def test_list_objects_without_delimiter(self):
        # A recursive listing sends no Delimiter.
        core, stubber = _make_core()
        stubber.add_response("list_objects_v2", {}, {"Bucket": "bucket", "Prefix": ""})
        with stubber:
            assert core.list_objects_page("bucket") == S3ListObjectsPage(bucket="bucket")

    def test_list_object_versions(self):
        core, stubber = _make_core()
        stubber.add_response(
            "list_object_versions",
            {
                "Versions": [{"Key": "k", "VersionId": "v2", "IsLatest": True, "Size": 2}],
                "DeleteMarkers": [{"Key": "j", "VersionId": "d1", "IsLatest": True}],
                "CommonPrefixes": [{"Prefix": "p/"}],
                "IsTruncated": True,
                "NextKeyMarker": "k",
            },
            {"Bucket": "bucket", "Prefix": "", "Delimiter": "/", "EncodingType": "url"},
        )
        # A missing NextVersionIdMarker is sent as "".
        stubber.add_response(
            "list_object_versions",
            {
                "Versions": [{"Key": "k", "VersionId": "v1", "IsLatest": False, "Size": 1}],
                "IsTruncated": False,
            },
            {
                "Bucket": "bucket",
                "Prefix": "",
                "Delimiter": "/",
                "EncodingType": "url",
                "KeyMarker": "k",
                "VersionIdMarker": "",
            },
        )
        with stubber:
            pages = list(core.list_object_versions("bucket", delimiter="/", EncodingType="url"))
        stubber.assert_no_pending_responses()
        assert [type(p) for p in pages] == [S3ListObjectVersionsPage] * 2
        assert [(v.key, v.version_id, v.is_delete_marker) for v in pages[0].versions] == [
            ("k", "v2", False)
        ]
        assert [(m.key, m.is_delete_marker) for m in pages[0].delete_markers] == [("j", True)]
        assert pages[0].common_prefixes == (S3CommonPrefix("bucket", "p/"),)
        assert [v.version_id for v in pages[1].versions] == ["v1"]

    def test_list_object_versions_stops_without_key_marker(self):
        core, stubber = _make_core()
        stubber.add_response(
            "list_object_versions", {"IsTruncated": True}, {"Bucket": "bucket", "Prefix": "k"}
        )
        with stubber:
            assert len(list(core.list_object_versions("bucket", prefix="k"))) == 1

    def test_list_objects_page_zero_max_keys(self):
        core, stubber = _make_core()
        stubber.add_response(
            "list_objects_v2", {"KeyCount": 0}, {"Bucket": "bucket", "Prefix": "", "MaxKeys": 0}
        )
        with stubber:
            assert core.list_objects_page("bucket", max_keys=0).key_count == 0

    def test_list_object_versions_page_key_marker_only(self):
        # A key marker alone is a valid request, without a VersionIdMarker.
        core, stubber = _make_core()
        stubber.add_response(
            "list_object_versions", {}, {"Bucket": "bucket", "Prefix": "", "KeyMarker": "k"}
        )
        with stubber:
            core.list_object_versions_page("bucket", key_marker="k")
        stubber.assert_no_pending_responses()

    def test_list_object_versions_from_markers(self):
        # The iterator starts at the given markers and then follows the pages.
        core, stubber = _make_core()
        stubber.add_response(
            "list_object_versions",
            {"IsTruncated": True, "NextKeyMarker": "m", "NextVersionIdMarker": "w"},
            {"Bucket": "bucket", "Prefix": "", "KeyMarker": "k", "VersionIdMarker": "v"},
        )
        stubber.add_response(
            "list_object_versions",
            {"IsTruncated": False},
            {"Bucket": "bucket", "Prefix": "", "KeyMarker": "m", "VersionIdMarker": "w"},
        )
        with stubber:
            pages = list(core.list_object_versions("bucket", key_marker="k", version_id_marker="v"))
        stubber.assert_no_pending_responses()
        assert len(pages) == 2

    def test_list_buckets_from_token(self):
        core, stubber = _make_core()
        stubber.add_response(
            "list_buckets", {"Buckets": [], "ContinuationToken": "t1"}, {"ContinuationToken": "t0"}
        )
        stubber.add_response("list_buckets", {"Buckets": []}, {"ContinuationToken": "t1"})
        with stubber:
            assert len(list(core.list_buckets(continuation_token="t0"))) == 2
        stubber.assert_no_pending_responses()

    def test_list_buckets(self):
        core, stubber = _make_core()
        stubber.add_response(
            "list_buckets",
            {
                "Buckets": [{"Name": "a", "CreationDate": MODIFIED, "BucketRegion": "us-east-1"}],
                "ContinuationToken": "t1",
            },
            {},
        )
        stubber.add_response(
            "list_buckets", {"Buckets": [{"Name": "b"}]}, {"ContinuationToken": "t1"}
        )
        with stubber:
            pages = list(core.list_buckets())
        stubber.assert_no_pending_responses()
        assert pages == [
            S3ListBucketsPage(
                buckets=(S3Bucket("a", creation_date=MODIFIED, bucket_region="us-east-1"),),
                continuation_token="t1",
            ),
            S3ListBucketsPage(buckets=(S3Bucket("b"),)),
        ]


class TestS3ObjectSummary:
    def test_from_response(self):
        entry = {
            "Key": "dir/a",
            "Size": 1,
            "ETag": '"a"',
            "LastModified": MODIFIED,
            "StorageClass": "STANDARD_IA",
        }
        summary = S3ObjectSummary.from_response("bucket", entry)
        assert summary == S3ObjectSummary("bucket", "dir/a", 1, '"a"', MODIFIED, "STANDARD_IA")
        assert summary.path == S3Path("bucket", "dir/a")
        # Optional fields that the entry does not have are None.
        assert S3ObjectSummary.from_response("bucket", {"Key": "k"}) == S3ObjectSummary(
            "bucket", "k"
        )

    def test_frozen(self):
        with pytest.raises(AttributeError):
            S3ObjectSummary("bucket", "k").key = "other"  # type: ignore[misc]
