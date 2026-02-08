# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import re
from typing import Any, Callable, Iterable, Optional, Pattern, Tuple, cast

import tenacity
from tenacity import after_log, retry_if_exception, stop_after_attempt, wait_exponential

from pyathena import DataError

_logger = logging.getLogger(__name__)  # type: ignore

PATTERN_OUTPUT_LOCATION: Pattern[str] = re.compile(
    r"^s3://(?P<bucket>[a-zA-Z0-9.\-_]+)/(?P<key>.+)$"
)


def parse_output_location(output_location: str) -> Tuple[str, str]:
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
        exceptions: List of AWS exception names to retry on.
        attempt: Maximum number of retry attempts.
        multiplier: Base multiplier for exponential backoff.
        max_delay: Maximum delay between retries in seconds.
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
        Retries are applied to AWS API calls, not to SQL query execution.
        Query failures typically require manual intervention or query fixes.
    """

    def __init__(
        self,
        exceptions: Iterable[str] = (
            "ThrottlingException",
            "TooManyRequestsException",
        ),
        attempt: int = 5,
        multiplier: int = 1,
        max_delay: int = 100,
        exponential_base: int = 2,
    ) -> None:
        self.exceptions = exceptions
        self.attempt = attempt
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.exponential_base = exponential_base


def retry_api_call(
    func: Callable[..., Any],
    config: RetryConfig,
    logger: Optional[logging.Logger] = None,
    *args,
    **kwargs,
) -> Any:
    """Execute a function with automatic retry logic for AWS API calls.

    This function wraps AWS API calls with retry behavior based on the provided
    configuration. It uses exponential backoff and only retries on specific
    AWS exceptions that indicate transient failures.

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
        Does not retry on client errors or non-AWS exceptions.
    """

    def _extract_code(ex: BaseException) -> Optional[str]:
        resp = cast(Optional[dict[str, Any]], getattr(ex, "response", None))
        err = cast(Optional[dict[str, Any]], (resp or {}).get("Error"))
        return cast(Optional[str], (err or {}).get("Code"))

    def _is_retryable(ex: BaseException) -> bool:
        code = _extract_code(ex)
        return code is not None and code in config.exceptions

    retry = tenacity.Retrying(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(config.attempt),
        wait=wait_exponential(
            multiplier=config.multiplier,
            max=config.max_delay,
            exp_base=config.exponential_base,
        ),
        after=after_log(logger, logger.level) if logger else None,  # type: ignore
        reraise=True,
    )
    return retry(func, *args, **kwargs)
