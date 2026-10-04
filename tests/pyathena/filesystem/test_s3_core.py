# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from datetime import UTC, datetime
from itertools import pairwise

import boto3
import botocore.exceptions
import pytest
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
    S3ObjectSummary,
)
from pyathena.filesystem.s3_object import S3MultipartUploadPart
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
                S3Path("bucket", "key"),
                "u",
                1,
                b"data",
                SSECustomerAlgorithm="AES256",
                PartNumber=99,
            )
        assert (part.part_number, part.etag) == (1, '"e1"')

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
                S3Path("bucket", "dst"),
                "u",
                2,
                S3Path("src-bucket", "src", "v1"),
                range_=(10, 20),
                CopySourceIfMatch='"src"',
            )
            whole = core.upload_part_copy(S3Path("bucket", "dst"), "u", 1, S3Path("bucket", "dst"))
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
                S3Path("bucket", "key"), "u", parts, RequestPayer="requester"
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
            core.abort_multipart_upload(S3Path("bucket", "key"), "u", RequestPayer="requester")
            with pytest.raises(FileNotFoundError):
                core.abort_multipart_upload(S3Path("bucket", "key"), "u")

    @pytest.mark.parametrize(
        ("method", "args"),
        [
            ("create_multipart_upload", (S3Path("bucket"),)),
            ("upload_part", (S3Path("bucket"), "u", 1, b"")),
            ("upload_part_copy", (S3Path("bucket"), "u", 1, S3Path("bucket", "src"))),
            ("upload_part_copy", (S3Path("bucket", "dst"), "u", 1, S3Path("bucket"))),
            ("complete_multipart_upload", (S3Path("bucket"), "u", [])),
            ("abort_multipart_upload", (S3Path("bucket"), "u")),
        ],
    )
    def test_multipart_upload_requires_keys(self, method, args):
        core, _ = _make_core()
        with pytest.raises(ValueError, match="has no key"):
            getattr(core, method)(*args)

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
