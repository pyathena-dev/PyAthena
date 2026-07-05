import errno

import botocore.exceptions
import pytest

from pyathena.filesystem.s3_errors import translate_client_error


def _client_error(code, message="error message", status_code=None):
    error_response = {"Error": {"Code": code, "Message": message}}
    if status_code is not None:
        error_response["ResponseMetadata"] = {"HTTPStatusCode": status_code}
    return botocore.exceptions.ClientError(error_response, "TestOperation")


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
def test_translate_client_error_to_exception(code, expected):
    actual = translate_client_error(_client_error(code))
    assert type(actual) is expected
    assert "error message" in str(actual)


@pytest.mark.parametrize(
    ("code", "expected_errno"),
    [
        ("BucketNotEmpty", errno.ENOTEMPTY),
        ("SlowDown", errno.EBUSY),
        ("ServiceUnavailable", errno.EBUSY),
        ("OperationAborted", errno.EBUSY),
        ("MethodNotAllowed", errno.EPERM),
        ("InternalError", errno.EIO),
    ],
)
def test_translate_client_error_to_os_error(code, expected_errno):
    actual = translate_client_error(_client_error(code))
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
def test_translate_client_error_by_http_status_code(status_code, expected_errno):
    actual = translate_client_error(_client_error("UnknownCode", status_code=status_code))
    assert isinstance(actual, OSError)
    assert actual.errno == expected_errno


def test_translate_client_error_unknown_code():
    actual = translate_client_error(_client_error("UnknownCode"))
    assert type(actual) is OSError
    assert actual.errno == errno.EIO
    assert "error message" in str(actual)
