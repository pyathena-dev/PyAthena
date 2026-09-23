from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterable
from re import Pattern
from typing import Any

import tenacity
from tenacity import (
    after_log,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)

from pyathena import DataError

_logger = logging.getLogger(__name__)

PATTERN_OUTPUT_LOCATION: Pattern[str] = re.compile(
    r"^s3://(?P<bucket>[a-zA-Z0-9.\-_]+)/(?P<key>.+)$"
)
THROTTLING_ERROR_CODES: tuple[str, ...] = ("ThrottlingException", "TooManyRequestsException")
# Athena wraps Glue errors without marking them retryable in its model.
# Match the service error envelope, not arbitrary words in its message.
PATTERN_METADATA_SERVICE_ERROR: Pattern[str] = re.compile(
    r"\(Service: AmazonDataCatalog; Status Code: \d+; "
    r"Error Code: ([A-Za-z][A-Za-z0-9]+);[^()]*\)\s*$"
)


def parse_output_location(output_location: str) -> tuple[str, str]:
    """Parse an S3 output location URL into bucket and key components.

    Args:
        output_location: S3 URL in format 's3://bucket-name/path/to/object'

    Returns:
        Tuple of (bucket_name, object_key)

    Raises:
        DataError: If the output_location format is invalid.

    Example:
        >>> bucket, key = parse_output_location("s3://my-bucket/results/query.csv")
        >>> print(bucket)  # "my-bucket"
        >>> print(key)    # "results/query.csv"
    """
    match = PATTERN_OUTPUT_LOCATION.search(output_location)
    if match:
        return match.group("bucket"), match.group("key")
    raise DataError("Unknown `output_location` format.")


def strtobool(val):
    """Convert a string representation of truth to True or False.

    This function replaces the deprecated distutils.util.strtobool method.
    It converts string representations of boolean values to actual boolean values.

    Args:
        val: String representation of a boolean value.

    Returns:
        1 for True values, 0 for False values.

    Raises:
        ValueError: If the input string is not a recognized boolean representation.

    Example:
        >>> strtobool("yes")  # 1
        >>> strtobool("false")  # 0
        >>> strtobool("invalid")  # ValueError

    Note:
        True values: y, yes, t, true, on, 1 (case-insensitive)
        False values: n, no, f, false, off, 0 (case-insensitive)

    References:
        - https://peps.python.org/pep-0632/
        - https://github.com/pypa/distutils/blob/main/distutils/util.py#L340-L353
    """
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return 1
    if val in ("n", "no", "f", "false", "off", "0"):
        return 0
    raise ValueError(f"invalid truth value {val!r}")


class RetryConfig:
    """Configuration for automatic retry behavior on failed API calls.

    This class configures how PyAthena handles transient failures when
    communicating with AWS services. It uses exponential backoff with
    customizable parameters to retry failed operations.

    Attributes:
        exceptions: Tuple of AWS exception names to retry on.
        attempt: Maximum number of attempts, including the first call.
        multiplier: Base multiplier for exponential backoff in seconds. Each
            wait also adds uniform random jitter of up to one multiplier.
        max_delay: Maximum exponential delay between retries in seconds,
            before jitter is added.
        exponential_base: Base for exponential backoff calculation.

    Example:
        >>> from pyathena.util import RetryConfig
        >>>
        >>> # Default retry configuration
        >>> retry_config = RetryConfig()
        >>>
        >>> # Custom retry configuration
        >>> custom_retry = RetryConfig(
        ...     exceptions=["ThrottlingException", "ServiceUnavailableException"],
        ...     attempt=10,
        ...     max_delay=60
        ... )
        >>>
        >>> # Use with connection
        >>> conn = pyathena.connect(
        ...     s3_staging_dir="s3://bucket/path/",
        ...     retry_config=custom_retry
        ... )

    Note:
        Exception names may be supplied as a single string or an iterable.
        They are captured as a tuple at construction; later changes to the
        original iterable do not change this configuration.
        Retries are applied to AWS API calls, not to SQL query execution.
        Query failures typically require manual intervention or query fixes.
        With the default settings, the exponential waits between attempts sum
        to 127 seconds plus jitter, which outlasts the metadata API throttling
        episodes observed under concurrent reflection. SDK retries configured
        on the boto3 client are a separate layer applied within each attempt.
        Recognized Glue error codes wrapped in Athena MetadataException are
        matched against exceptions in the same way as direct AWS error codes.
    """

    def __init__(
        self,
        exceptions: Iterable[str] = THROTTLING_ERROR_CODES,
        attempt: int = 8,
        multiplier: int = 1,
        max_delay: int = 100,
        exponential_base: int = 2,
    ) -> None:
        self.exceptions = (exceptions,) if isinstance(exceptions, str) else tuple(exceptions)
        self.attempt = attempt
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.exponential_base = exponential_base


def _without_retries(config: RetryConfig, codes: Iterable[str]) -> RetryConfig:
    """Copy a retry policy without retrying ``codes``."""
    excluded = set(codes)
    return RetryConfig(
        exceptions=[c for c in config.exceptions if c not in excluded],
        attempt=config.attempt,
        multiplier=config.multiplier,
        max_delay=config.max_delay,
        exponential_base=config.exponential_base,
    )


def _get_error_code(ex: BaseException, unwrap_metadata: bool = False) -> str | None:
    response = getattr(ex, "response", None)
    error = response.get("Error") if isinstance(response, dict) else None
    if not isinstance(error, dict):
        return None
    code = error.get("Code")
    if unwrap_metadata and code == "MetadataException":
        message = error.get("Message", "")
        if isinstance(message, str):
            match = PATTERN_METADATA_SERVICE_ERROR.search(message)
            if match:
                return match.group(1)
    return code if isinstance(code, str) else None


def is_retryable_error(ex: BaseException, config: RetryConfig) -> bool:
    """Return whether an exception matches the retry policy's AWS error codes.

    Args:
        ex: The exception raised by an AWS API call.
        config: RetryConfig whose ``exceptions`` list the retryable error codes.

    Returns:
        True if the direct error code, or a recognized Glue error code wrapped in
        an Athena ``MetadataException``, is listed in ``config.exceptions``.
    """
    code = _get_error_code(ex)
    if code in config.exceptions:
        return True
    return (
        code == "MetadataException"
        and _get_error_code(ex, unwrap_metadata=True) in config.exceptions
    )


def retry_api_call(
    func: Callable[..., Any],
    config: RetryConfig,
    logger: logging.Logger | None = None,
    *args,
    **kwargs,
) -> Any:
    """Execute a function with automatic retry logic for AWS API calls.

    This function wraps AWS API calls with retry behavior based on the provided
    configuration. It uses exponential backoff with uniform jitter and only
    retries on specific AWS exceptions that indicate transient failures.

    Args:
        func: The AWS API function to call.
        config: RetryConfig instance specifying retry behavior.
        logger: Optional logger for retry attempt logging.
        *args: Positional arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The result of the successful function call.

    Raises:
        The original exception if all retry attempts are exhausted.

    Example:
        >>> from pyathena.util import RetryConfig, retry_api_call
        >>> config = RetryConfig(attempt=3, max_delay=30)
        >>> result = retry_api_call(
        ...     client.describe_table,
        ...     config=config,
        ...     logger=logger,
        ...     TableName="my_table"
        ... )

    Note:
        Only retries on AWS exceptions listed in the RetryConfig.exceptions.
        This includes recognized Glue error codes wrapped in MetadataException.
        Other errors are propagated without retrying.
    """

    retry = tenacity.Retrying(
        retry=retry_if_exception(lambda ex: is_retryable_error(ex, config)),
        stop=stop_after_attempt(config.attempt),
        # Uniform jitter of up to one multiplier keeps concurrent clients from
        # retrying in lockstep after a shared throttling response.
        wait=wait_exponential(
            multiplier=config.multiplier,
            max=config.max_delay,
            exp_base=config.exponential_base,
        )
        + wait_random(0, config.multiplier),
        after=after_log(logger, logger.getEffectiveLevel()) if logger else None,  # type: ignore[arg-type]
        reraise=True,
    )
    return retry(func, *args, **kwargs)
