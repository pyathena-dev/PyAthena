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
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    import botocore.exceptions


class S3ClientError:
    """Represents an S3 error response with its translation to an OSError.

    Wraps a botocore ``ClientError`` and exposes the error response fields
    as properties, along with :attr:`os_error`, the equivalent standard
    Python exception. The error is mapped by its S3 error code first, then
    by its HTTP status code; if neither is recognized, a generic ``OSError``
    with the original error message is used.

    Example:
        >>> try:
        ...     client.head_object(Bucket="bucket", Key="key")
        ... except botocore.exceptions.ClientError as e:
        ...     raise S3ClientError(e).os_error from e
    """

    # S3 error codes (https://docs.aws.amazon.com/AmazonS3/latest/API/API_Error.html)
    # that map to a specific OSError subclass.
    # The "403" / "404" entries are not S3 error codes: HEAD requests
    # (HeadObject/HeadBucket) have no response body, so botocore reports the
    # HTTP status code as the error code instead.
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
    _ERROR_CODE_TO_EXCEPTION: ClassVar[dict[str, type[OSError]]] = {
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
    _ERROR_CODE_TO_ERRNO: ClassVar[dict[str, int]] = {
        "BucketNotEmpty": errno.ENOTEMPTY,
        "InternalError": errno.EIO,
        "OperationAborted": errno.EBUSY,
        "RestoreAlreadyInProgress": errno.EBUSY,
        "ServiceUnavailable": errno.EBUSY,
        "SlowDown": errno.EBUSY,
    }

    # Fallbacks by HTTP status code for error codes not listed above.
    _HTTP_STATUS_CODE_TO_ERRNO: ClassVar[dict[int, int]] = {
        400: errno.EINVAL,
        405: errno.EPERM,
        409: errno.EBUSY,
        412: errno.EINVAL,
        416: errno.EINVAL,
        500: errno.EIO,
        501: errno.ENOSYS,
        503: errno.EBUSY,
    }

    def __init__(self, error: botocore.exceptions.ClientError) -> None:
        error_info = error.response.get("Error", {})
        self._code: str = str(error_info.get("Code", ""))
        self._message: str = str(error_info.get("Message", error))
        status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        self._http_status_code: int | None = int(status_code) if status_code is not None else None
        self._os_error: OSError = self._translate()

    def _translate(self) -> OSError:
        exception = self._ERROR_CODE_TO_EXCEPTION.get(self._code)
        if exception:
            return exception(self._message)
        errno_ = self._ERROR_CODE_TO_ERRNO.get(self._code)
        if errno_ is None:
            if self._http_status_code is not None:
                errno_ = self._HTTP_STATUS_CODE_TO_ERRNO.get(self._http_status_code, errno.EIO)
            else:
                errno_ = errno.EIO
        return OSError(errno_, self._message)

    @property
    def code(self) -> str:
        """The S3 error code that uniquely identifies the error condition."""
        return self._code

    @property
    def message(self) -> str:
        """The error message returned by the server."""
        return self._message

    @property
    def http_status_code(self) -> int | None:
        """The HTTP status code of the error response."""
        return self._http_status_code

    @property
    def os_error(self) -> OSError:
        """The standard Python exception equivalent to this error response.

        Resolved once at construction time. The exception is instantiated
        and ready to be raised; raise it with ``raise ... from error`` to
        preserve the original exception.

        Note:
            ``OSError`` specializes itself into a subclass for some errno
            values (e.g., ``EPERM`` -> ``PermissionError``), so the returned
            exception may be a subclass of ``OSError`` even for the
            errno-based mappings.
        """
        return self._os_error
