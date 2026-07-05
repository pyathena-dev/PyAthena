import errno

import botocore.exceptions
import pytest

from pyathena.filesystem.s3_errors import S3ClientError


def _client_error(code, message="error message", status_code=None):
    error_response = {"Error": {"Code": code, "Message": message}}
    if status_code is not None:
        error_response["ResponseMetadata"] = {"HTTPStatusCode": status_code}
    return botocore.exceptions.ClientError(error_response, "TestOperation")


class TestS3ClientError:
    def test_properties(self):
        error = S3ClientError(_client_error("NoSuchKey", status_code=404))
        assert error.code == "NoSuchKey"
        assert error.message == "error message"
        assert error.http_status_code == 404

        error = S3ClientError(_client_error("NoSuchKey"))
        assert error.http_status_code is None

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
        actual = S3ClientError(_client_error(code)).os_error
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
        ],
    )
    def test_os_error_by_error_code_errno(self, code, expected_errno):
        actual = S3ClientError(_client_error(code)).os_error
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
        actual = S3ClientError(_client_error("UnknownCode", status_code=status_code)).os_error
        assert isinstance(actual, OSError)
        assert actual.errno == expected_errno

    def test_os_error_unknown_code(self):
        actual = S3ClientError(_client_error("UnknownCode")).os_error
        assert type(actual) is OSError
        assert actual.errno == errno.EIO
        assert "error message" in str(actual)
