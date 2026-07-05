"""Translation of S3 error responses into standard Python exceptions.

Maps botocore ``ClientError`` responses to the matching ``OSError`` subclasses
so that filesystem operations raise natural Python exceptions
(e.g. ``403`` -> ``PermissionError``, ``404`` -> ``FileNotFoundError``).

The error codes are taken from the official list of Amazon S3 error codes:
https://docs.aws.amazon.com/AmazonS3/latest/API/API_Error.html
(see also https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html).
The mapping to Python exceptions is PyAthena's own.
"""

from __future__ import annotations

import errno
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import botocore.exceptions

# S3 error codes (https://docs.aws.amazon.com/AmazonS3/latest/API/API_Error.html)
# that map to a specific Python built-in exception.
# The "403" / "404" entries are not S3 error codes: HEAD requests
# (HeadObject/HeadBucket) have no response body, so botocore reports the
# HTTP status code as the error code instead.
# https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
_ERROR_CODE_TO_EXCEPTION: dict[str, type[Exception]] = {
    "AccessDenied": PermissionError,
    "AccountProblem": PermissionError,
    "AllAccessDisabled": PermissionError,
    "BucketAlreadyExists": FileExistsError,
    "BucketAlreadyOwnedByYou": FileExistsError,
    "ExpiredToken": PermissionError,
    "InvalidAccessKeyId": PermissionError,
    "InvalidObjectState": PermissionError,
    "InvalidPayer": PermissionError,
    "InvalidSecurity": PermissionError,
    "NoSuchBucket": FileNotFoundError,
    "NoSuchBucketPolicy": FileNotFoundError,
    "NoSuchKey": FileNotFoundError,
    "NoSuchLifecycleConfiguration": FileNotFoundError,
    "NoSuchUpload": FileNotFoundError,
    "NoSuchVersion": FileNotFoundError,
    "NotSignedUp": PermissionError,
    "RequestTimeout": TimeoutError,
    "RequestTimeTooSkewed": PermissionError,
    "SignatureDoesNotMatch": PermissionError,
    "403": PermissionError,
    "404": FileNotFoundError,
}

# S3 error codes (https://docs.aws.amazon.com/AmazonS3/latest/API/API_Error.html)
# that map to an OSError with a specific errno.
_ERROR_CODE_TO_ERRNO: dict[str, int] = {
    "BucketNotEmpty": errno.ENOTEMPTY,
    "InternalError": errno.EIO,
    "OperationAborted": errno.EBUSY,
    "RestoreAlreadyInProgress": errno.EBUSY,
    "ServiceUnavailable": errno.EBUSY,
    "SlowDown": errno.EBUSY,
}

# Fallbacks by HTTP status code for error codes not listed above.
_HTTP_STATUS_CODE_TO_ERRNO: dict[int, int] = {
    400: errno.EINVAL,
    405: errno.EPERM,
    409: errno.EBUSY,
    412: errno.EINVAL,
    416: errno.EINVAL,
    500: errno.EIO,
    501: errno.ENOSYS,
    503: errno.EBUSY,
}


def translate_client_error(error: botocore.exceptions.ClientError) -> Exception:
    """Convert a botocore ClientError into a natural Python exception.

    Args:
        error: The client error raised by an S3 API call.

    Returns:
        An instantiated exception ready to be raised. The error is mapped by
        its S3 error code first, then by its HTTP status code. If neither is
        recognized, an ``OSError`` with the original error message is
        returned. The original exception should be preserved by raising the
        translated exception with ``raise ... from error``.

    Note:
        ``OSError`` specializes itself into a subclass for some errno values
        (e.g., ``EPERM`` -> ``PermissionError``), so the returned exception
        may be a subclass of ``OSError`` even for the errno-based mappings.
    """
    code = error.response.get("Error", {}).get("Code", "")
    message = error.response.get("Error", {}).get("Message", str(error))

    exception = _ERROR_CODE_TO_EXCEPTION.get(code)
    if exception:
        return exception(message)

    errno_ = _ERROR_CODE_TO_ERRNO.get(code)
    if errno_ is None:
        status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        errno_ = _HTTP_STATUS_CODE_TO_ERRNO.get(status_code, errno.EIO)
    return OSError(errno_, message)
