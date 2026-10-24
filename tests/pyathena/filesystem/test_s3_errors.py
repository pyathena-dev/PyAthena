# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import errno

import botocore.exceptions
import pytest

from pyathena.filesystem.s3_errors import S3ClientError


def _client_error(code, message="error message", status_code=None, condition=None, headers=None):
    error_response = {"Error": {"Code": code, "Message": message}}
    if condition is not None:
        error_response["Error"]["Condition"] = condition
    if status_code is not None:
        error_response["ResponseMetadata"] = {"HTTPStatusCode": status_code}
    if headers is not None:
        # botocore lowercases the names of the response headers.
        error_response.setdefault("ResponseMetadata", {})["HTTPHeaders"] = headers
    return botocore.exceptions.ClientError(error_response, "TestOperation")


class TestS3ClientError:
    @pytest.mark.parametrize(
        ("code", "expected"),
        [
            ("AccessDenied", PermissionError),
            ("403", PermissionError),
            ("InvalidAccessKeyId", PermissionError),
            ("ExpiredToken", PermissionError),
            ("SignatureDoesNotMatch", PermissionError),
            ("NoSuchKey", FileNotFoundError),
            ("NoSuchBucket", FileNotFoundError),
            ("NoSuchUpload", FileNotFoundError),
            ("NoSuchVersion", FileNotFoundError),
            ("404", FileNotFoundError),
            ("BucketAlreadyExists", FileExistsError),
            ("BucketAlreadyOwnedByYou", FileExistsError),
            ("RequestTimeout", TimeoutError),
        ],
    )
    def test_os_error_by_error_code(self, code, expected):
        error = S3ClientError(_client_error(code))
        assert (error.code, error.message) == (code, "error message")
        actual = error.os_error
        assert type(actual) is expected
        assert "error message" in str(actual)

    @pytest.mark.parametrize(
        ("code", "expected_errno"),
        [
            ("BucketNotEmpty", errno.ENOTEMPTY),
            ("SlowDown", errno.EBUSY),
            ("ServiceUnavailable", errno.EBUSY),
            ("OperationAborted", errno.EBUSY),
            ("InternalError", errno.EIO),
            # An unknown code without a status code.
            ("UnknownCode", errno.EIO),
        ],
    )
    def test_os_error_by_error_code_errno(self, code, expected_errno):
        error = S3ClientError(_client_error(code))
        assert error.http_status_code is None
        actual = error.os_error
        # OSError specializes itself into a subclass for some errno values
        # (e.g., EPERM -> PermissionError), so check the errno, not the exact type.
        assert isinstance(actual, OSError)
        assert actual.errno == expected_errno
        assert "error message" in str(actual)

    @pytest.mark.parametrize(
        ("status_code", "expected_errno"),
        [
            (400, errno.EINVAL),
            (405, errno.EPERM),
            (409, errno.EBUSY),
            (412, errno.EINVAL),
            (416, errno.EINVAL),
            (500, errno.EIO),
            (501, errno.ENOSYS),
            (503, errno.EBUSY),
        ],
    )
    def test_os_error_by_http_status_code(self, status_code, expected_errno):
        error = S3ClientError(_client_error("UnknownCode", status_code=status_code))
        assert error.http_status_code == status_code
        actual = error.os_error
        assert isinstance(actual, OSError)
        assert actual.errno == expected_errno

    @pytest.mark.parametrize(
        ("condition", "expected"),
        [
            # GH-972: a conditional write (IfNoneMatch="*") found an existing
            # object.
            ("If-None-Match", FileExistsError),
            # A read conditional on the ETag (IfMatch) found another object.
            ("If-Match", OSError),
            (None, OSError),
        ],
    )
    def test_os_error_precondition_failed(self, condition, expected):
        actual = S3ClientError(
            _client_error("PreconditionFailed", status_code=412, condition=condition)
        ).os_error
        assert type(actual) is expected
        if expected is OSError:
            assert actual.errno == errno.EINVAL
        assert "error message" in str(actual)

    @pytest.mark.parametrize(
        "code",
        [
            # HEAD responses have no body, so the code is the status code.
            "405",
            "MethodNotAllowed",
        ],
    )
    def test_os_error_delete_marker(self, code):
        # GH-1338: HeadObject/GetObject of a version that is a delete marker
        # is answered with 405 and the x-amz-delete-marker header, and the
        # version does not exist as an object. Other 405 errors map to EPERM.
        actual = S3ClientError(
            _client_error(code, status_code=405, headers={"x-amz-delete-marker": "true"})
        ).os_error
        assert type(actual) is FileNotFoundError
        assert "error message" in str(actual)
