# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""The retry policy of the S3 requests of the filesystem and the scopes that stop them."""

from __future__ import annotations

import copy
import logging
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import CancelledError
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Protocol, TypeVar

import tenacity
from tenacity import (
    RetryCallState,
    after_log,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)

#: The error codes that a retry policy retries by default.
THROTTLING_ERROR_CODES: tuple[str, ...] = ("ThrottlingException", "TooManyRequestsException")


class RetryPolicy(Protocol):
    """The attributes of a retry policy that the filesystem reads.

    These are ``exceptions``, ``attempt``, ``multiplier``, ``max_delay``, and
    ``exponential_base``, with the meaning that :class:`S3RetryConfig`
    describes. :class:`S3RetryConfig` and :class:`pyathena.util.RetryConfig`
    have them.
    """

    exceptions: tuple[str, ...]
    attempt: int
    multiplier: int
    max_delay: int
    exponential_base: int


_PolicyT = TypeVar("_PolicyT", bound=RetryPolicy)


class S3RetryConfig:
    """The retry policy of the S3 requests of a filesystem.

    The requests are retried with exponential backoff and uniform jitter when
    their error code is in ``exceptions``. A policy with the default
    ``exceptions`` also retries the transient S3 error codes (see
    :meth:`~pyathena.filesystem.s3_core.S3Core.resolve_retry_config`).

    Attributes:
        exceptions: The error codes to retry.
        attempt: Maximum number of attempts, including the first call.
        multiplier: Base multiplier for exponential backoff in seconds. Each
            wait also adds uniform random jitter of up to one multiplier.
        max_delay: Maximum exponential delay between retries in seconds,
            before jitter is added.
        exponential_base: Base for exponential backoff calculation.
    """

    def __init__(
        self,
        exceptions: Iterable[str] = THROTTLING_ERROR_CODES,
        attempt: int = 8,
        multiplier: int = 1,
        max_delay: int = 100,
        exponential_base: int = 2,
    ) -> None:
        """Initialize the retry policy.

        Args:
            exceptions: Error code, or iterable of error codes, to retry.
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


def with_exceptions(config: _PolicyT, exceptions: Iterable[str]) -> _PolicyT:
    """Copy a retry policy with other error codes to retry.

    Args:
        config: The retry policy to copy.
        exceptions: The error codes that the copy retries.

    Returns:
        A shallow copy of the policy, of the same type; ``config`` is unchanged.
    """
    copied = copy.copy(config)
    copied.exceptions = tuple(exceptions)
    return copied


def is_retryable_error(ex: BaseException, config: RetryPolicy) -> bool:
    """Return whether an exception has an error code that a retry policy retries.

    Args:
        ex: The exception raised by an AWS API call.
        config: The retry policy.

    Returns:
        True if the error code of the response of the exception is in
        ``config.exceptions``.
    """
    response = getattr(ex, "response", None)
    error = response.get("Error") if isinstance(response, dict) else None
    code = error.get("Code") if isinstance(error, dict) else None
    return isinstance(code, str) and code in config.exceptions


# The events of the enclosing stop scopes of retry_api_call(); see stop_retries_on().
_RETRY_STOP_EVENTS: ContextVar[tuple[threading.Event, ...]] = ContextVar(
    "pyathena_filesystem_retry_stop_events", default=()
)
# The longest that a backoff wait sleeps before it checks again whether the
# retries are stopped, unless it can wait for the single event of a scope.
_STOP_POLL_INTERVAL = 0.1


@contextmanager
def stop_retries_on(event: threading.Event) -> Iterator[None]:
    """Stop the retries of the S3 requests in this context once an event is set.

    A call of :func:`retry_api_call` in the block makes no further attempt
    once the event is set, ends its backoff wait, and raises the error of its
    last attempt. That includes the calls in threads that run a copy of this
    context, as ``asyncio.to_thread()`` runs its function. The scopes nest: a
    call stops when the event of any enclosing scope is set. The stop is
    cooperative: a request that has been sent, and the retries of botocore
    itself, still run to their end.

    :func:`pyathena.util.stop_retries_on` also opens a scope of this function.

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
    """Run the retries of the S3 requests in this block to their end.

    The events of the enclosing :func:`stop_retries_on` scopes do not stop
    the calls of :func:`retry_api_call` in the block, which retry by their
    policy. For cleanup that has to run to its end even when the work that it
    cleans up was stopped, such as the abort of a multipart upload. A scope
    that :func:`stop_retries_on` opens in the block stops the retries again.

    :func:`pyathena.util.unstoppable_retries` also opens a scope of this
    function.

    Yields:
        None.
    """
    token = _RETRY_STOP_EVENTS.set(())
    try:
        yield
    finally:
        _RETRY_STOP_EVENTS.reset(token)


def retries_stopped() -> bool:
    """Return whether an event of the enclosing stop scopes is set.

    Returns:
        Whether an event of the enclosing :func:`stop_retries_on` scopes,
        apart from those that :func:`unstoppable_retries` clears, is set.
    """
    return any(event.is_set() for event in _RETRY_STOP_EVENTS.get())


def raise_if_stopped(operation: str) -> None:
    """Raise if an event of the enclosing stop scopes is set.

    A writer calls this before a request that adds to or commits a write,
    such as a part or the completion of a multipart upload, so that a write
    whose owner has stopped, such as a batch operation after the failure of
    another object, sends nothing more. The retries that
    :func:`stop_retries_on` stops do not cover the first attempt of a new
    request. The scopes that :func:`unstoppable_retries` clears do not stop
    the writer either.

    Args:
        operation: What the caller was about to do, for the error message.

    Raises:
        concurrent.futures.CancelledError: If an event of the enclosing
            :func:`stop_retries_on` scopes is set.
    """
    if retries_stopped():
        raise CancelledError(f"Stopped before it could {operation}.")


def retry_api_call(
    func: Callable[..., Any],
    config: RetryPolicy,
    logger: logging.Logger | None = None,
    *args,
    **kwargs,
) -> Any:
    """Call an AWS API with the retries of a policy, which the stop scopes stop.

    The call is retried with exponential backoff and uniform jitter while it
    fails with an error code in ``config.exceptions``, up to
    ``config.attempt`` attempts. Once an event of the enclosing
    :func:`stop_retries_on` scopes is set, no further attempt is made and the
    error of the last attempt is raised.

    Args:
        func: The AWS API function to call.
        config: The retry policy.
        logger: Optional logger for retry attempt logging.
        *args: Positional arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The result of the successful function call.

    Raises:
        BaseException: The error of the last attempt, once the attempts are
            exhausted or the retries are stopped, or an error that the policy
            does not retry.
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
        retry=retry_if_exception(lambda ex: is_retryable_error(ex, config) and not stopped()),
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
