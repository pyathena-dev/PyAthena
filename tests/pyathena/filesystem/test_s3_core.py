# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import io
from datetime import UTC, datetime
from itertools import pairwise

import boto3
import botocore.exceptions
import pytest
from botocore.response import StreamingBody
from botocore.stub import Stubber

from pyathena.filesystem.s3_core import (
    S3Bucket,
    S3CommonPrefix,
    S3Core,
    S3DeleteBatch,
    S3DeleteError,
    S3DeleteResult,
    S3ListBucketsPage,
    S3ListObjectsPage,
    S3ListObjectVersionsPage,
    S3MultipartCopyPlan,
    S3ObjectSummary,
)
from pyathena.filesystem.s3_object import S3MultipartUpload, S3MultipartUploadPart
from pyathena.filesystem.s3_path import S3Path
from pyathena.util import RetryConfig
from tests.pyathena.util import (
    MULTIPART_COPY_BLOCK_SIZE,
    MULTIPART_COPY_KWARGS,
    MULTIPART_COPY_SIZE,
)

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

    @pytest.mark.parametrize(
        ("version_id", "range_", "expected"),
        [
            (None, None, {}),
            ("null", None, {"VersionId": "null"}),
            ("v1", (0, 100), {"VersionId": "v1", "Range": "bytes=0-99"}),
            (None, (100, None), {"Range": "bytes=100-"}),
            (None, (-8, None), {"Range": "bytes=-8"}),
        ],
    )
    def test_get_object(self, version_id, range_, expected):
        core, stubber = _make_core(request_kwargs={"ServerSideEncryption": "AES256"})
        # ServerSideEncryption, which GetObject does not accept, is not sent.
        stubber.add_response(
            "get_object",
            {"Body": StreamingBody(io.BytesIO(b"data"), 4)},
            {"Bucket": "bucket", "Key": "key", "IfMatch": '"e"', **expected},
        )
        with stubber:
            data = core.get_object(S3Path("bucket", "key", version_id), range_, IfMatch='"e"')
        stubber.assert_no_pending_responses()
        assert data == b"data"

    @pytest.mark.parametrize(
        ("range_", "expected"),
        [
            # The range of the call takes precedence over a parameter.
            ((0, 3), "bytes=0-2"),
            # Without one, the parameter is sent.
            (None, "bytes=1-2"),
        ],
    )
    def test_get_object_range_parameter(self, range_, expected):
        core, stubber = _make_core()
        stubber.add_response(
            "get_object",
            {"Body": StreamingBody(io.BytesIO(b"ab"), 2)},
            {"Bucket": "bucket", "Key": "key", "Range": expected},
        )
        with stubber:
            core.get_object(S3Path("bucket", "key"), range_, Range="bytes=1-2")
        stubber.assert_no_pending_responses()

    @pytest.mark.parametrize(
        ("path", "range_", "match"),
        [
            (S3Path("bucket"), None, "no key"),
            # S3 would ignore these ranges and return the whole object.
            (S3Path("bucket", "key"), (5, 5), "Invalid range"),
            (S3Path("bucket", "key"), (6, 5), "Invalid range"),
            (S3Path("bucket", "key"), (-8, 5), "Invalid range"),
        ],
    )
    def test_get_object_invalid(self, path, range_, match):
        core, stubber = _make_core()
        # No request is stubbed, so one that is sent fails the test.
        with stubber, pytest.raises(ValueError, match=match):
            core.get_object(path, range_)

    def test_get_object_closes_body(self):
        class FailingStream(io.BytesIO):
            def read(self, size=-1):
                raise botocore.exceptions.ReadTimeoutError(endpoint_url="https://s3")

        core, stubber = _make_core()
        raws = [io.BytesIO(b"data"), FailingStream(b"data")]
        stubber.add_response("get_object", {"Body": StreamingBody(raws[0], 4)})
        stubber.add_response("get_object", {"Body": StreamingBody(raws[1], 4)})
        with stubber:
            assert core.get_object(S3Path("bucket", "key")) == b"data"
            # The failure is neither translated nor retried.
            with pytest.raises(botocore.exceptions.ReadTimeoutError):
                core.get_object(S3Path("bucket", "key"))
        stubber.assert_no_pending_responses()
        assert [raw.closed for raw in raws] == [True, True]

    @pytest.mark.parametrize(
        ("status", "code", "error"),
        [(404, "NoSuchKey", FileNotFoundError), (416, "InvalidRange", OSError)],
    )
    def test_get_object_translates_errors(self, status, code, error):
        core, stubber = _make_core()
        stubber.add_client_error("get_object", service_error_code=code, http_status_code=status)
        with stubber, pytest.raises(error) as e:
            core.get_object(S3Path("bucket", "key"), (10, None))
        assert isinstance(e.value.__cause__, botocore.exceptions.ClientError)
        assert e.value.__cause__.response["Error"]["Code"] == code

    @pytest.mark.parametrize(
        ("body", "params", "expected"),
        [
            (b"data", {}, {"Body": b"data"}),
            # An empty body sends no body of its own.
            (None, {}, {}),
            (b"", {}, {}),
            (None, {"Body": b"x"}, {"Body": b"x"}),
            # The body of the call takes precedence over a parameter.
            (b"data", {"Body": b"x"}, {"Body": b"data"}),
            # So do the bucket and the key of the path.
            (None, {"Bucket": "other", "Key": "other"}, {}),
        ],
    )
    def test_put_object(self, body, params, expected):
        core, stubber = _make_core(request_kwargs={"ServerSideEncryption": "AES256"})
        stubber.add_response(
            "put_object",
            {"ETag": '"e"', "VersionId": "v1"},
            {"Bucket": "bucket", "Key": "key", "ServerSideEncryption": "AES256", **expected},
        )
        with stubber:
            result = core.put_object(S3Path("bucket", "key"), body, **params)
        stubber.assert_no_pending_responses()
        assert (result.etag, result.version_id) == ('"e"', "v1")

    @pytest.mark.parametrize(
        ("path", "match"),
        [
            (S3Path("bucket"), "no key"),
            (S3Path("bucket", "key", "v1"), "Cannot write to a version"),
            (S3Path("bucket", "key", "null"), "Cannot write to a version"),
        ],
    )
    def test_put_object_rejects_paths(self, path, match):
        core, stubber = _make_core()
        with stubber, pytest.raises(ValueError, match=match):
            core.put_object(path, b"data")

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

    @pytest.mark.parametrize(
        ("method", "params"),
        [
            ("list_objects", {"ContinuationToken": "t0"}),
            ("list_object_versions", {"KeyMarker": "k"}),
            ("list_object_versions", {"VersionIdMarker": "v"}),
            ("list_buckets", {"ContinuationToken": "t0"}),
        ],
    )
    def test_iterators_reject_api_cursors(self, method, params):
        # The iterator advances the cursor, so it is passed as an argument;
        # in params it would be sent twice from the second page on.
        core, stubber = _make_core()
        args = () if method == "list_buckets" else ("bucket",)
        with stubber, pytest.raises(TypeError, match="Pass the first page"):
            next(getattr(core, method)(*args, **params))
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

    def test_delete_object(self):
        core, stubber = _make_core()
        stubber.add_response("delete_object", {}, {"Bucket": "bucket", "Key": "key"})
        stubber.add_response(
            "delete_object",
            {},
            {
                "Bucket": "bucket",
                "Key": "key",
                "VersionId": "v1",
                "BypassGovernanceRetention": True,
            },
        )
        with stubber:
            core.delete_object(S3Path("bucket", "key"))
            core.delete_object(S3Path("bucket", "key", "v1"), BypassGovernanceRetention=True)
        stubber.assert_no_pending_responses()

    def test_delete_object_requires_a_key(self):
        core, _ = _make_core()
        with pytest.raises(ValueError, match="has no key"):
            core.delete_object(S3Path("bucket"))

    def test_delete_objects(self):
        core, stubber = _make_core()
        stubber.add_response(
            "delete_objects",
            {
                "Deleted": [{"Key": "a", "DeleteMarker": True, "DeleteMarkerVersionId": "m1"}],
                "Errors": [
                    {"Key": "b", "VersionId": "v1", "Code": "AccessDenied", "Message": "Denied"}
                ],
            },
            {
                "Bucket": "bucket",
                "Delete": {
                    "Objects": [{"Key": "a"}, {"Key": "b", "VersionId": "v1"}],
                    "Quiet": False,
                },
                "ExpectedBucketOwner": "111122223333",
            },
        )
        batch = S3DeleteBatch(
            "bucket", (S3Path("bucket", "a"), S3Path("bucket", "b", "v1")), quiet=False
        )
        with stubber:
            result = core.delete_objects(batch, ExpectedBucketOwner="111122223333")
        # A 200 response with errors is a result, not an exception.
        assert result == S3DeleteResult(
            "bucket",
            deleted=(S3Path("bucket", "a"),),
            errors=(S3DeleteError(S3Path("bucket", "b", "v1"), "AccessDenied", "Denied"),),
        )

    def test_delete_objects_quiet(self):
        core, stubber = _make_core()
        stubber.add_response(
            "delete_objects",
            {},
            {"Bucket": "bucket", "Delete": {"Objects": [{"Key": "a"}], "Quiet": True}},
        )
        with stubber:
            result = core.delete_objects(S3DeleteBatch("bucket", (S3Path("bucket", "a"),)))
        assert result == S3DeleteResult("bucket")

    def test_delete_objects_translates_errors(self):
        core, stubber = _make_core()
        stubber.add_client_error(
            "delete_objects", service_error_code="AccessDenied", http_status_code=403
        )
        with stubber, pytest.raises(PermissionError):
            core.delete_objects(S3DeleteBatch("bucket", (S3Path("bucket", "a"),)))

    def test_create_multipart_upload(self):
        core, stubber = _make_core()
        stubber.add_response(
            "create_multipart_upload",
            {"Bucket": "bucket", "Key": "key", "UploadId": "u"},
            {"Bucket": "bucket", "Key": "key", "ContentType": "text/csv"},
        )
        with stubber:
            # The key of the path takes precedence over a parameter.
            upload = core.create_multipart_upload(
                S3Path("bucket", "key"), ContentType="text/csv", Key="other"
            )
        assert upload.upload_id == "u"

    def test_upload_part(self):
        core, stubber = _make_core()
        stubber.add_response(
            "upload_part",
            {"ETag": '"e1"'},
            {
                "Bucket": "bucket",
                "Key": "key",
                "UploadId": "u",
                "PartNumber": 1,
                "Body": b"data",
                "SSECustomerAlgorithm": "AES256",
            },
        )
        with stubber:
            part = core.upload_part(
                S3MultipartUpload({"Bucket": "bucket", "Key": "key", "UploadId": "u"}),
                1,
                b"data",
                SSECustomerAlgorithm="AES256",
                PartNumber=99,
            )
        assert (part.part_number, part.etag) == (1, '"e1"')

    @pytest.mark.parametrize(
        "bucket", ["myap-abc123-s3alias", "arn:aws:s3:us-east-1:123456789012:accesspoint/myap"]
    )
    def test_multipart_upload_preserves_access_point_identity(self, bucket):
        core, stubber = _make_core()
        identity = {"Bucket": bucket, "Key": "key", "UploadId": "u"}
        checksum = {"ChecksumAlgorithm": "SHA256", "ChecksumType": "COMPOSITE"}
        first = {"ETag": '"first"', "ChecksumSHA256": "sha1"}
        copied = {"ETag": '"copy"', "ChecksumSHA256": "sha2"}
        stubber.add_response(
            "create_multipart_upload",
            {"Bucket": "underlying-bucket", "Key": "key", "UploadId": "u", **checksum},
            {"Bucket": bucket, "Key": "key", **checksum},
        )
        stubber.add_response(
            "upload_part",
            first,
            {**identity, "PartNumber": 1, "Body": b"data", "ChecksumAlgorithm": "SHA256"},
        )
        stubber.add_response(
            "upload_part_copy",
            {"CopyPartResult": copied},
            {**identity, "PartNumber": 2, "CopySource": {"Bucket": "source", "Key": "object"}},
        )
        stubber.add_response(
            "complete_multipart_upload",
            {"ETag": '"done"'},
            {
                **identity,
                "ChecksumType": "COMPOSITE",
                "MultipartUpload": {
                    "Parts": [{**first, "PartNumber": 1}, {**copied, "PartNumber": 2}]
                },
            },
        )
        stubber.add_client_error(
            "abort_multipart_upload",
            service_error_code="NoSuchUpload",
            http_status_code=404,
            expected_params=identity,
        )
        with stubber:
            upload = core.create_multipart_upload(S3Path(bucket, "key"), **checksum)
            assert (upload.bucket, upload.key, upload.upload_id) == (bucket, "key", "u")
            parts = [
                core.upload_part(upload, 1, b"data"),
                core.upload_part_copy(upload, 2, S3Path("source", "object")),
            ]
            core.complete_multipart_upload(upload, parts)
            with pytest.raises(FileNotFoundError):
                core.abort_multipart_upload(upload)
        stubber.assert_no_pending_responses()

    def test_upload_part_copy(self):
        core, stubber = _make_core()
        stubber.add_response(
            "upload_part_copy",
            {"CopyPartResult": {"ETag": '"p2"'}},
            {
                "Bucket": "bucket",
                "Key": "dst",
                "CopySource": {"Bucket": "src-bucket", "Key": "src", "VersionId": "v1"},
                "UploadId": "u",
                "PartNumber": 2,
                "CopySourceRange": "bytes=10-19",
                "CopySourceIfMatch": '"src"',
            },
        )
        # Without a range, the whole source is copied.
        stubber.add_response(
            "upload_part_copy",
            {"CopyPartResult": {"ETag": '"p1"'}},
            {
                "Bucket": "bucket",
                "Key": "dst",
                "CopySource": {"Bucket": "bucket", "Key": "dst"},
                "UploadId": "u",
                "PartNumber": 1,
            },
        )
        with stubber:
            part = core.upload_part_copy(
                S3MultipartUpload({"Bucket": "bucket", "Key": "dst", "UploadId": "u"}),
                2,
                S3Path("src-bucket", "src", "v1"),
                range_=(10, 20),
                CopySourceIfMatch='"src"',
            )
            whole = core.upload_part_copy(
                S3MultipartUpload({"Bucket": "bucket", "Key": "dst", "UploadId": "u"}),
                1,
                S3Path("bucket", "dst"),
            )
        stubber.assert_no_pending_responses()
        assert (part.part_number, part.etag) == (2, '"p2"')
        assert (whole.part_number, whole.etag) == (1, '"p1"')

    def test_complete_multipart_upload(self):
        core, stubber = _make_core()
        stubber.add_response(
            "complete_multipart_upload",
            {"ETag": '"dst"', "VersionId": "v-dst"},
            {
                "Bucket": "bucket",
                "Key": "key",
                "UploadId": "u",
                "MultipartUpload": {
                    "Parts": [{"ETag": '"e1"', "PartNumber": 1}, {"ETag": '"e2"', "PartNumber": 2}]
                },
                "RequestPayer": "requester",
            },
        )
        parts = [S3MultipartUploadPart(n, {"ETag": f'"e{n}"'}) for n in (1, 2)]
        with stubber:
            completed = core.complete_multipart_upload(
                S3MultipartUpload({"Bucket": "bucket", "Key": "key", "UploadId": "u"}),
                parts,
                RequestPayer="requester",
            )
        assert (completed.etag, completed.version_id) == ('"dst"', "v-dst")

    def test_abort_multipart_upload(self):
        core, stubber = _make_core()
        stubber.add_response(
            "abort_multipart_upload",
            {},
            {"Bucket": "bucket", "Key": "key", "UploadId": "u", "RequestPayer": "requester"},
        )
        # Unlike the callers that log a failed abort, the core raises it.
        stubber.add_client_error(
            "abort_multipart_upload", service_error_code="NoSuchUpload", http_status_code=404
        )
        with stubber:
            core.abort_multipart_upload(
                S3MultipartUpload({"Bucket": "bucket", "Key": "key", "UploadId": "u"}),
                RequestPayer="requester",
            )
            with pytest.raises(FileNotFoundError):
                core.abort_multipart_upload(
                    S3MultipartUpload({"Bucket": "bucket", "Key": "key", "UploadId": "u"})
                )

    @pytest.mark.parametrize(
        ("method", "args"),
        [
            ("create_multipart_upload", (S3Path("bucket"),)),
            (
                "upload_part_copy",
                (
                    S3MultipartUpload({"Bucket": "bucket", "Key": "dst", "UploadId": "u"}),
                    1,
                    S3Path("bucket"),
                ),
            ),
        ],
    )
    def test_multipart_upload_requires_keys(self, method, args):
        core, _ = _make_core()
        with pytest.raises(ValueError, match="has no key"):
            getattr(core, method)(*args)

    @pytest.mark.parametrize(
        ("method", "args"),
        [
            ("upload_part", (1, b"data")),
            ("upload_part_copy", (1, S3Path("source", "key"))),
            ("complete_multipart_upload", ([],)),
            ("abort_multipart_upload", ()),
        ],
    )
    @pytest.mark.parametrize(
        ("missing", "message"),
        [("Bucket", "no bucket"), ("Key", "no key"), ("UploadId", "no upload ID")],
    )
    def test_multipart_upload_requires_identity(self, method, args, missing, message):
        core, _ = _make_core()
        response = {"Bucket": "bucket", "Key": "key", "UploadId": "u"}
        del response[missing]
        with pytest.raises(ValueError, match=message):
            getattr(core, method)(S3MultipartUpload(response), *args)

    def test_upload_part_uses_creation_algorithm_and_identity(self):
        core, stubber = _make_core()
        upload = S3MultipartUpload(
            {"Bucket": "bucket", "Key": "key", "UploadId": "u", "ChecksumAlgorithm": "SHA256"}
        )
        stubber.add_response(
            "upload_part",
            {"ETag": '"part"', "ChecksumSHA256": "sha"},
            {
                "Bucket": "bucket",
                "Key": "key",
                "UploadId": "u",
                "PartNumber": 1,
                "Body": b"data",
                "ChecksumAlgorithm": "SHA256",
            },
        )
        with stubber:
            part = core.upload_part(
                upload,
                1,
                b"data",
                Bucket="other",
                Key="other",
                UploadId="other",
                ChecksumAlgorithm="CRC32",
            )
        stubber.assert_no_pending_responses()
        assert part.to_api_repr()["ChecksumSHA256"] == "sha"

    def test_create_multipart_upload_rejects_versions(self):
        # A write replaces the object at the key, not the named version.
        core, _ = _make_core()
        with pytest.raises(ValueError, match="Cannot write to a version"):
            core.create_multipart_upload(S3Path("bucket", "key", "v1"))

    @pytest.mark.parametrize(
        ("size", "block_size", "ranges"),
        [
            # A single range.
            (5 * 2**20, 5 * 2**20, [(0, 5 * 2**20)]),
            # The size is an exact multiple of the block size.
            (10 * 2**30, 5 * 2**30, [(0, 5 * 2**30), (5 * 2**30, 10 * 2**30)]),
            # A last range of the minimum part size is kept.
            (
                5 * 2**30 + 5 * 2**20,
                5 * 2**30,
                [(0, 5 * 2**30), (5 * 2**30, 5 * 2**30 + 5 * 2**20)],
            ),
            # GH-951: a last range shorter than the minimum part size is
            # merged into the previous one,
            (15 * 2**20 - 1, 5 * 2**20, [(0, 5 * 2**20), (5 * 2**20, 15 * 2**20 - 1)]),
            # which is split in half if it exceeds the maximum part size.
            (
                5 * 2**30 + 2**20,
                5 * 2**30,
                [(0, 5 * 2**29 + 2**19), (5 * 2**29 + 2**19, 5 * 2**30 + 2**20)],
            ),
        ],
    )
    def test_part_ranges(self, size, block_size, ranges):
        core, _ = _make_core()
        assert core.part_ranges(size, block_size) == ranges

    @pytest.mark.parametrize(
        ("size", "num_ranges"),
        [
            # The block size splits the object into the maximum number of parts.
            (10_000 * 5 * 2**20, 10_000),
            # GH-953: a larger object is split by a larger size instead of
            # into more parts than the maximum,
            (10_000 * 5 * 2**20 + 1, 9_999),
            (50 * 2**30, 10_000),
            # including the maximum object size.
            (5 * 2**40, 10_000),
        ],
    )
    def test_part_ranges_max_parts(self, size, num_ranges):
        core, _ = _make_core()
        ranges = core.part_ranges(size, 5 * 2**20)

        assert len(ranges) == num_ranges
        assert ranges[0][0] == 0
        assert ranges[-1][1] == size
        assert all(end == start for (_, end), (start, _) in pairwise(ranges))
        assert all(
            core.MULTIPART_UPLOAD_MIN_PART_SIZE
            <= end - start
            <= core.MULTIPART_UPLOAD_MAX_PART_SIZE
            for start, end in ranges
        )

    def test_copy_object(self):
        core, stubber = _make_core()
        stubber.add_response(
            "copy_object",
            {},
            {
                "CopySource": {"Bucket": "src-bucket", "Key": "src", "VersionId": "v1"},
                "Bucket": "bucket",
                "Key": "dst",
                "MetadataDirective": "REPLACE",
            },
        )
        stubber.add_response(
            "copy_object",
            {},
            {"CopySource": {"Bucket": "bucket", "Key": "src"}, "Bucket": "bucket", "Key": "dst"},
        )
        with stubber:
            core.copy_object(
                S3Path("src-bucket", "src", "v1"),
                S3Path("bucket", "dst"),
                MetadataDirective="REPLACE",
            )
            core.copy_object(S3Path("bucket", "src"), S3Path("bucket", "dst"))
        stubber.assert_no_pending_responses()

    @pytest.mark.parametrize(
        ("method", "args", "match"),
        [
            ("copy_object", (S3Path("bucket"), S3Path("bucket", "dst")), "has no key"),
            ("copy_object", (S3Path("bucket", "src"), S3Path("bucket")), "has no key"),
            (
                "copy_object",
                (S3Path("bucket", "src"), S3Path("bucket", "dst", "v1")),
                "Cannot write to a version",
            ),
            ("plan_multipart_copy", (S3Path("bucket"), S3Path("bucket", "dst")), "has no key"),
            ("plan_multipart_copy", (S3Path("bucket", "src"), S3Path("bucket")), "has no key"),
            (
                "plan_multipart_copy",
                (S3Path("bucket", "src"), S3Path("bucket", "dst", "v1")),
                "Cannot write to a version",
            ),
            ("list_object_annotations", (S3Path("bucket"),), "has no key"),
            (
                "copy_object_annotation",
                ("a", S3Path("bucket"), S3Path("bucket", "dst"), None, None),
                "has no key",
            ),
            (
                "copy_object_annotation",
                ("a", S3Path("bucket", "src"), S3Path("bucket"), None, None),
                "has no key",
            ),
        ],
    )
    def test_copy_rejects_paths(self, method, args, match):
        core, stubber = _make_core()
        with stubber, pytest.raises(ValueError, match=match):
            getattr(core, method)(*args)

    @staticmethod
    def _stub_head(stubber, response, version_id=None, **params):
        expected = {"Bucket": "bucket", "Key": "src", **params}
        if version_id:
            expected.update({"VersionId": version_id})
        stubber.add_response("head_object", response, expected)

    def test_plan_multipart_copy(self):
        # The source is read as CopyObject would read it: the version that
        # HeadObject reports is pinned, its metadata and tags replace those of
        # the parameters, and its annotations are listed on every page. The
        # source's expected owner reaches the reads under their own names.
        core, stubber = _make_core()
        size = MULTIPART_COPY_SIZE
        source_params = {"RequestPayer": "requester", "ExpectedBucketOwner": "222222222222"}
        self._stub_head(
            stubber,
            {
                "ContentLength": size,
                "ContentType": "text/csv",
                "Metadata": {"owner": "etl"},
                "VersionId": "v-src",
            },
            **source_params,
        )
        source = {"Bucket": "bucket", "Key": "src", "VersionId": "v-src", **source_params}
        stubber.add_response("get_object_tagging", {"TagSet": [{"Key": "t", "Value": "1"}]}, source)
        stubber.add_response(
            "list_object_annotations",
            {
                "Annotations": [{"AnnotationName": "a1", "LastModified": MODIFIED, "Size": 1}],
                "NextContinuationToken": "next",
            },
            source,
        )
        stubber.add_response(
            "list_object_annotations",
            {"Annotations": [{"AnnotationName": "a2", "LastModified": MODIFIED, "Size": 1}]},
            {**source, "ContinuationToken": "next"},
        )
        with stubber:
            plan = core.plan_multipart_copy(
                S3Path("bucket", "src"),
                S3Path("bucket", "dst"),
                MULTIPART_COPY_BLOCK_SIZE,
                **MULTIPART_COPY_KWARGS,
            )
        stubber.assert_no_pending_responses()

        destination_params = {"RequestPayer": "requester", "ExpectedBucketOwner": "111111111111"}
        assert plan == S3MultipartCopyPlan(
            source=S3Path("bucket", "src", "v-src"),
            destination=S3Path("bucket", "dst"),
            size=size,
            ranges=((0, MULTIPART_COPY_BLOCK_SIZE), (MULTIPART_COPY_BLOCK_SIZE, size)),
            create_params={
                **destination_params,
                "ContentType": "text/csv",
                "Metadata": {"owner": "etl"},
                "Tagging": "t=1",
                "StorageClass": "STANDARD_IA",
            },
            part_params={
                **destination_params,
                "ExpectedSourceBucketOwner": "222222222222",
                "CopySourceIfMatch": '"src"',
            },
            complete_params=destination_params,
            abort_params=destination_params,
            annotations=("a1", "a2"),
        )

    @pytest.mark.parametrize(
        ("version_id", "head_version_id", "expected"),
        [
            # The version that HeadObject reports is pinned,
            (None, "v1", "v1"),
            # except a "null" version, which a write can replace,
            (None, "null", None),
            # and a version given with the path is kept.
            ("null", "null", "null"),
            ("v0", "v0", "v0"),
        ],
    )
    @pytest.mark.parametrize("size", [0, S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE + 1])
    def test_plan_multipart_copy_source_version(self, version_id, head_version_id, expected, size):
        core, stubber = _make_core()
        self._stub_head(stubber, {"ContentLength": size, "VersionId": head_version_id}, version_id)
        with stubber:
            plan = core.plan_multipart_copy(
                S3Path("bucket", "src", version_id),
                S3Path("bucket", "dst"),
                MetadataDirective="REPLACE",
                TaggingDirective="REPLACE",
                AnnotationDirective="EXCLUDE",
            )
        stubber.assert_no_pending_responses()
        assert plan.source == S3Path("bucket", "src", expected)
        assert plan.fits_single_request is (size == 0)

    @pytest.mark.parametrize("size", [0, S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE])
    def test_plan_multipart_copy_fits_single_request(self, size):
        # GH-973: a source that fits in a single CopyObject request, such as
        # one whose cached size was stale, is not read any further.
        core, stubber = _make_core()
        self._stub_head(stubber, {"ContentLength": size}, RequestPayer="requester")
        with stubber:
            plan = core.plan_multipart_copy(
                S3Path("bucket", "src"), S3Path("bucket", "dst"), RequestPayer="requester"
            )
        stubber.assert_no_pending_responses()
        assert plan == S3MultipartCopyPlan(
            source=S3Path("bucket", "src"),
            destination=S3Path("bucket", "dst"),
            size=size,
            fits_single_request=True,
        )

    def test_plan_multipart_copy_without_size(self):
        core, stubber = _make_core()
        self._stub_head(stubber, {})
        with stubber, pytest.raises(ValueError, match="no size"):
            core.plan_multipart_copy(S3Path("bucket", "src"), S3Path("bucket", "dst"))

    def test_plan_multipart_copy_unknown_parameter(self):
        # A parameter that CopyObject does not accept is passed on to
        # CreateMultipartUpload, so that botocore still rejects it.
        core, stubber = _make_core()
        self._stub_head(stubber, {"ContentLength": core.MULTIPART_UPLOAD_MAX_PART_SIZE + 1})
        with stubber:
            plan = core.plan_multipart_copy(
                S3Path("bucket", "src"),
                S3Path("bucket", "dst"),
                ContentTyp="text/csv",
                MetadataDirective="REPLACE",
                TaggingDirective="REPLACE",
                AnnotationDirective="EXCLUDE",
            )
        assert plan.create_params == {"ContentTyp": "text/csv"}
        assert plan.part_params == {}
        with pytest.raises(botocore.exceptions.ParamValidationError, match="ContentTyp"):
            core.client.create_multipart_upload(Bucket="bucket", Key="dst", **plan.create_params)

    def test_list_object_annotations(self):
        core, stubber = _make_core()
        stubber.add_response(
            "list_object_annotations",
            {
                "Annotations": [{"AnnotationName": "a1", "LastModified": MODIFIED, "Size": 1}],
                "NextContinuationToken": "next",
            },
            {"Bucket": "bucket", "Key": "key", "VersionId": "v1", "RequestPayer": "requester"},
        )
        stubber.add_response(
            "list_object_annotations",
            {},
            {
                "Bucket": "bucket",
                "Key": "key",
                "VersionId": "v1",
                "RequestPayer": "requester",
                "ContinuationToken": "next",
            },
        )
        with stubber:
            # The key of the path takes precedence over a parameter.
            names = core.list_object_annotations(
                S3Path("bucket", "key", "v1"), RequestPayer="requester", Key="other"
            )
        stubber.assert_no_pending_responses()
        assert names == ["a1"]

    def test_copy_object_annotation(self):
        # The source is read with the source's parameters of the copy, and
        # the annotation is written to the version and the ETag that the copy
        # wrote, with the parameters that PutObjectAnnotation accepts.
        core, stubber = _make_core()
        stubber.add_response(
            "get_object_annotation",
            {"AnnotationPayload": StreamingBody(io.BytesIO(b"payload"), 7)},
            {
                "Bucket": "bucket",
                "Key": "src",
                "VersionId": "v-src",
                "AnnotationName": "a1",
                "RequestPayer": "requester",
                "ExpectedBucketOwner": "222222222222",
            },
        )
        stubber.add_response(
            "put_object_annotation",
            {},
            {
                "Bucket": "bucket",
                "Key": "dst",
                "AnnotationName": "a1",
                "AnnotationPayload": b"payload",
                "VersionId": "v-dst",
                "ObjectIfMatch": '"dst"',
                "RequestPayer": "requester",
                "ExpectedBucketOwner": "111111111111",
            },
        )
        with stubber:
            core.copy_object_annotation(
                "a1",
                S3Path("bucket", "src", "v-src"),
                S3Path("bucket", "dst"),
                "v-dst",
                '"dst"',
                RequestPayer="requester",
                ExpectedBucketOwner="111111111111",
                ExpectedSourceBucketOwner="222222222222",
                ContentType="text/csv",
                # A field of the request takes precedence.
                ObjectIfMatch='"other"',
            )
        stubber.assert_no_pending_responses()

    @pytest.mark.parametrize(
        "field",
        [
            "ChecksumCRC32",
            "ChecksumCRC32C",
            "ChecksumCRC64NVME",
            "ChecksumSHA1",
            "ChecksumSHA256",
            "ChecksumSHA512",
            "ChecksumMD5",
            "ChecksumXXHASH64",
            "ChecksumXXHASH3",
            "ChecksumXXHASH128",
        ],
    )
    @pytest.mark.parametrize("copy", [False, True])
    def test_complete_multipart_upload_preserves_part_checksums(self, field, copy):
        core, stubber = _make_core()
        identity = {"Bucket": "bucket", "Key": "key", "UploadId": "u"}
        algorithm = field.removeprefix("Checksum")
        stubber.add_response(
            "create_multipart_upload",
            {**identity, "ChecksumAlgorithm": algorithm},
            {"Bucket": "bucket", "Key": "key", "ChecksumAlgorithm": algorithm},
        )
        expected_parts = []
        for number in (1, 2):
            result = {"ETag": f'"e{number}"', field: f"checksum{number}"}
            request = {"Bucket": "bucket", "Key": "key", "UploadId": "u", "PartNumber": number}
            if copy:
                request["CopySource"] = {"Bucket": "bucket", "Key": "source"}
                stubber.add_response("upload_part_copy", {"CopyPartResult": result}, request)
            else:
                request["Body"] = b"data"
                request["ChecksumAlgorithm"] = algorithm
                request[field] = f"checksum{number}"
                stubber.add_response("upload_part", result, request)
            expected_parts.append({**result, "PartNumber": number})
        stubber.add_response(
            "complete_multipart_upload",
            {"ETag": '"done"'},
            {
                "Bucket": "bucket",
                "Key": "key",
                "UploadId": "u",
                "MultipartUpload": {"Parts": expected_parts},
                "RequestPayer": "requester",
            },
        )
        with stubber:
            upload = core.create_multipart_upload(
                S3Path("bucket", "key"), ChecksumAlgorithm=algorithm
            )
            if copy:
                parts = [
                    core.upload_part_copy(upload, n, S3Path("bucket", "source")) for n in (1, 2)
                ]
            else:
                parts = [
                    core.upload_part(upload, n, b"data", **{field: f"checksum{n}"}) for n in (1, 2)
                ]
            completed = core.complete_multipart_upload(
                upload,
                parts,
                RequestPayer="requester",
                MultipartUpload={"Parts": []},
            )
        stubber.assert_no_pending_responses()
        assert completed.etag == '"done"'

    @pytest.mark.parametrize(
        ("algorithm", "checksum_type"),
        [(None, None), ("SHA256", "COMPOSITE"), ("CRC32", "FULL_OBJECT")],
    )
    def test_complete_multipart_upload_uses_creation_algorithm(self, algorithm, checksum_type):
        core, stubber = _make_core()
        upload = S3MultipartUpload(
            {
                "Bucket": "bucket",
                "Key": "key",
                "UploadId": "u",
                "ChecksumAlgorithm": algorithm,
                "ChecksumType": checksum_type,
            }
        )
        part = S3MultipartUploadPart(
            1,
            {
                "ETag": '"part"',
                "ChecksumCRC32": "sdk-crc",
                "ChecksumSHA256": "upload-sha",
            },
        )
        expected = {"ETag": '"part"', "PartNumber": 1}
        if algorithm:
            expected[f"Checksum{algorithm}"] = "upload-sha" if algorithm == "SHA256" else "sdk-crc"
        stubber.add_response(
            "complete_multipart_upload",
            {"ETag": '"done"'},
            {
                "Bucket": "bucket",
                "Key": "key",
                "UploadId": "u",
                "MultipartUpload": {"Parts": [expected]},
                **({"ChecksumType": checksum_type} if checksum_type else {}),
            },
        )
        with stubber:
            core.complete_multipart_upload(upload, [part])
        stubber.assert_no_pending_responses()


class TestS3DeleteBatch:
    def test_from_paths(self):
        paths = [S3Path("b1", f"k{i}") for i in range(S3DeleteBatch.MAX_KEYS + 1)]
        paths.insert(1, S3Path("b2", "a", "v1"))

        batches = S3DeleteBatch.from_paths(paths, quiet=False)
        # Grouped by bucket in the order of the paths, MAX_KEYS keys each.
        assert [(b.bucket, len(b.objects), b.quiet) for b in batches] == [
            ("b1", S3DeleteBatch.MAX_KEYS, False),
            ("b1", 1, False),
            ("b2", 1, False),
        ]
        assert batches[0].objects[:2] == (S3Path("b1", "k0"), S3Path("b1", "k1"))
        assert batches[1].objects == (S3Path("b1", f"k{S3DeleteBatch.MAX_KEYS}"),)
        assert batches[2].objects == (S3Path("b2", "a", "v1"),)
        assert S3DeleteBatch.from_paths([]) == []

    @pytest.mark.parametrize(
        ("objects", "match"),
        [
            ((), "1 to 1000 objects, not 0"),
            (
                tuple(S3Path("bucket", f"k{i}") for i in range(S3DeleteBatch.MAX_KEYS + 1)),
                "1 to 1000 objects, not 1001",
            ),
            ((S3Path("bucket"),), "Not an object of the bucket bucket: s3://bucket"),
            ((S3Path("bucket", ""),), "Not an object of the bucket bucket: s3://bucket"),
            ((S3Path("other", "a"),), "Not an object of the bucket bucket: s3://other/a"),
        ],
    )
    def test_invalid(self, objects, match):
        with pytest.raises(ValueError, match=match):
            S3DeleteBatch("bucket", objects)

    def test_from_paths_rejects_bucket_paths(self):
        with pytest.raises(ValueError, match="Not an object of the bucket bucket: s3://bucket"):
            S3DeleteBatch.from_paths([S3Path("bucket", "a"), S3Path("bucket")])


class TestS3DeleteError:
    def test_from_response(self):
        error = S3DeleteError.from_response(
            "bucket", {"Key": "a", "VersionId": "v1", "Code": "InternalError", "Message": "Error"}
        )
        assert error == S3DeleteError(S3Path("bucket", "a", "v1"), "InternalError", "Error")
        assert str(error) == "bucket/a?versionId=v1 (InternalError: Error)"
        assert str(S3DeleteError.from_response("bucket", {"Key": "a"})) == "bucket/a (None: None)"


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
