"""Helpers for S3 output locations, truth-value parsing, and retrying AWS API calls."""

from __future__ import annotations

import logging
import re
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future, wait
from contextlib import contextmanager
from contextvars import ContextVar
from re import Pattern
from typing import TYPE_CHECKING, Any, TypeVar

import tenacity
from tenacity import (
    RetryCallState,
    after_log,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)

from pyathena import DataError

_T = TypeVar("_T")

# Replace with ``from typing import override`` when Python 3.11 support ends.
if TYPE_CHECKING:
    from typing_extensions import override
else:

    def override(func):
        """Mark a method as overriding a base class method (PEP 698).

        Type checkers use ``typing_extensions.override`` and report a marked
        method whose base class method does not exist. At runtime this
        decorator does nothing.

        Args:
            func: The overriding method.

        Returns:
            The method unchanged.
        """
        return func


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


def strtobool(val: str) -> bool:
    """Convert a string representation of truth to True or False.

    This function accepts the same strings as the deprecated
    distutils.util.strtobool, but returns bool instead of int.

    Args:
        val: String representation of a boolean value.

    Returns:
        True for true values, False for false values.

    Raises:
        ValueError: If the input string is not a recognized boolean representation.

    Example:
        >>> strtobool("yes")
        True
        >>> strtobool("false")
        False
        >>> strtobool("invalid")  # ValueError

    Note:
        True values: y, yes, t, true, on, 1 (case-insensitive)
        False values: n, no, f, false, off, 0 (case-insensitive)

    .. versionchanged:: 4.0
       Returns True or False instead of 1 or 0. Call ``int(strtobool(value))``
       if an integer result is required.

    References:
        - https://peps.python.org/pep-0632/
        - https://github.com/pypa/distutils/blob/e32c71f38d6faf95c8241f4bfe4b6abf06a2ded9/distutils/util.py#L340-L353
    """
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return True
    if val in ("n", "no", "f", "false", "off", "0"):
        return False
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
        """Initialize the retry configuration.

        Args:
            exceptions: AWS error code, or iterable of error codes, to retry on.
                Stored as a tuple.
            attempt: Maximum number of attempts, including the first call.
            multiplier: Base multiplier for exponential backoff in seconds, and
                the upper bound of the random jitter added to each wait.
            max_delay: Maximum exponential delay between retries in seconds,
                before jitter is added.
            exponential_base: Base for exponential backoff calculation.
        """
        self.exceptions = (exceptions,) if isinstance(exceptions, str) else tuple(exceptions)
        self.attempt = attempt
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.exponential_base = exponential_base


def _without_retries(config: RetryConfig, codes: Iterable[str]) -> RetryConfig:
    """Copy a retry policy without retrying ``codes``.

    Args:
        config: The retry policy to copy.
        codes: The error codes to leave out of ``config.exceptions``.

    Returns:
        A new policy; ``config`` is unchanged.
    """
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


def _is_throttling_error(ex: BaseException) -> bool:
    """Whether an AWS error is throttling.

    That includes Glue's own throttling, which Athena reports inside a
    ``MetadataException``.

    Args:
        ex: The exception an AWS API call raised.

    Returns:
        True if its error code is in ``THROTTLING_ERROR_CODES``.
    """
    return _get_error_code(ex, unwrap_metadata=True) in THROTTLING_ERROR_CODES


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


# The events of the enclosing stop scopes of retry_api_call(); see stop_retries_on().
_RETRY_STOP_EVENTS: ContextVar[tuple[threading.Event, ...]] = ContextVar(
    "pyathena_retry_stop_events", default=()
)
# The longest that a backoff wait sleeps before it checks again whether the
# retries are stopped, unless it can wait for the single event of a scope.
_STOP_POLL_INTERVAL = 0.1


@contextmanager
def stop_retries_on(event: threading.Event) -> Iterator[None]:
    """Stop the retries of AWS API calls in this context once an event is set.

    A call of :func:`retry_api_call` in the block makes no further attempt
    once the event is set, ends its backoff wait, and raises the error of its
    last attempt. That includes the calls in threads that run a copy of this
    context, as ``asyncio.to_thread()`` runs its function. The scopes nest: a
    call stops when the event of any enclosing scope is set. The stop is
    cooperative: a request that has been sent, and the retries of botocore
    itself, still run to their end.

    Args:
        event: The event that stops the retries.

    Yields:
        None.
    """
    token = _RETRY_STOP_EVENTS.set((*_RETRY_STOP_EVENTS.get(), event))
    try:
        yield
    finally:
        _RETRY_STOP_EVENTS.reset(token)


@contextmanager
def unstoppable_retries() -> Iterator[None]:
    """Run the retries of AWS API calls in this block to their end.

    The events of the enclosing :func:`stop_retries_on` scopes do not stop
    the calls of :func:`retry_api_call` in the block, which retry by their
    policy. For cleanup that has to run to its end even when the work that it
    cleans up was stopped, such as the close of an object whose construction
    was cancelled, or a request that stops a query. A scope that
    :func:`stop_retries_on` opens in the block stops the retries again.

    Yields:
        None.
    """
    token = _RETRY_STOP_EVENTS.set(())
    try:
        yield
    finally:
        _RETRY_STOP_EVENTS.reset(token)


def wait_with_retry_stop(future: Future[_T], *, stop: threading.Event) -> _T:
    """Wait for work whose retries an event stops, relaying the stop of this context.

    The work of the future runs in a :func:`stop_retries_on` scope of
    ``stop``, typically in another thread that another caller started. Once an
    event of the enclosing stop scopes of this context is set while the work
    runs, ``stop`` is set, and the wait continues until the work ends. Without
    an enclosing scope, the function only waits.

    Args:
        future: The future of the work.
        stop: The event of the stop scope of the work.

    Returns:
        The result of the work.

    Raises:
        BaseException: The exception of the work, such as the error of the last
            attempt of a call whose retries were stopped.
    """
    events = _RETRY_STOP_EVENTS.get()
    while events and not future.done():
        if any(event.is_set() for event in events):
            stop.set()
            break
        wait((future,), _STOP_POLL_INTERVAL)
    return future.result()


def retry_api_call(
    func: Callable[..., Any],
    config: RetryConfig,
    logger: logging.Logger | None = None,
    *args,
    stop_on: Callable[[BaseException], bool] | None = None,
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
        stop_on: Optional predicate; an exception it accepts is raised at once
            instead of being retried.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The result of the successful function call.

    Raises:
        The original exception if all retry attempts are exhausted, or the
        error of the last attempt if the retries are stopped.

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
    events = _RETRY_STOP_EVENTS.get()

    def stopped() -> bool:
        """Whether an event of the enclosing scopes is set."""
        return any(event.is_set() for event in events)

    # The error of the attempt before a backoff wait, raised if the wait is stopped.
    failures: list[BaseException] = []
    # A single event can be waited for; anything else is polled.
    wait_for_event = len(events) == 1

    def record_failure(retry_state: RetryCallState) -> None:
        """Keep the error of the failed attempt, which a stopped wait raises.

        Args:
            retry_state: The state of the call before the backoff wait.
        """
        outcome = retry_state.outcome
        if outcome is not None and (error := outcome.exception()) is not None:
            failures[:] = [error]

    def sleep(seconds: float) -> None:
        """Wait before the next attempt, ending the wait once the retries are stopped.

        Args:
            seconds: The backoff time.

        Raises:
            BaseException: The error of the last attempt, instead of
                making the next one, if the retries are stopped.
        """
        deadline = time.monotonic() + seconds
        while not stopped():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            if wait_for_event:
                events[0].wait(remaining)
            else:
                time.sleep(min(remaining, _STOP_POLL_INTERVAL))
        raise failures[0]

    # Without a stop, tenacity's own sleep is kept.
    stop_kwargs: dict[str, Any] = {"before_sleep": record_failure, "sleep": sleep} if events else {}

    retry = tenacity.Retrying(
        retry=retry_if_exception(
            lambda ex: (
                is_retryable_error(ex, config) and not (stop_on and stop_on(ex)) and not stopped()
            )
        ),
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
        **stop_kwargs,
    )
    try:
        return retry(func, *args, **kwargs)
    finally:
        # The traceback of a raised error keeps this frame, which would keep
        # the error in a reference cycle through the list.
        failures.clear()
