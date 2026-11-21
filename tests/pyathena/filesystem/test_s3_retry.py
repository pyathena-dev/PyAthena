# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import contextvars
import threading
import time
from concurrent.futures import CancelledError, ThreadPoolExecutor

import pytest
import tenacity.nap
from botocore.exceptions import ClientError

from pyathena.filesystem.s3_retry import (
    S3RetryConfig,
    is_retryable_error,
    raise_if_stopped,
    retries_stopped,
    retry_api_call,
    stop_retries_on,
    unstoppable_retries,
    with_exceptions,
)
from tests.pyathena.util import (
    EVENT_TIMEOUT,
    StoppingOperation,
    ThrottledOperation,
    throttling_error,
)


class _Policy:
    """A retry policy of another type, such as ``pyathena.util.RetryConfig``."""

    def __init__(self, exceptions):
        self.exceptions = exceptions
        self.attempt = 3
        self.multiplier = 0
        self.max_delay = 0
        self.exponential_base = 2


def _error(code, message="message"):
    return ClientError({"Error": {"Code": code, "Message": message}}, "GetObject")


class TestS3RetryConfig:
    def test_defaults(self):
        config = S3RetryConfig()
        assert config.exceptions == ("ThrottlingException", "TooManyRequestsException")
        assert (config.attempt, config.multiplier, config.max_delay, config.exponential_base) == (
            8,
            1,
            100,
            2,
        )

    @pytest.mark.parametrize("policy_type", [tuple, list, iter, str])
    def test_captures_exceptions(self, policy_type):
        codes = ["SlowDown", "InternalError"]
        exceptions = codes[0] if policy_type is str else policy_type(codes)
        config = S3RetryConfig(exceptions=exceptions)
        codes.append("RequestTimeout")
        assert config.exceptions == (
            ("SlowDown",) if policy_type is str else ("SlowDown", "InternalError")
        )


@pytest.mark.parametrize(
    "config",
    [S3RetryConfig(exceptions=("SlowDown",), attempt=3), _Policy(("SlowDown",))],
    ids=["S3RetryConfig", "other"],
)
def test_with_exceptions(config):
    copied = with_exceptions(config, ["SlowDown", "InternalError"])
    assert type(copied) is type(config)
    assert copied.exceptions == ("SlowDown", "InternalError")
    assert copied.attempt == 3
    assert config.exceptions == ("SlowDown",)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (_error("SlowDown"), True),
        (_error("NoSuchKey"), False),
        # Unlike pyathena.util.is_retryable_error(), Glue errors that Athena
        # wraps are not unwrapped.
        (
            _error(
                "MetadataException",
                "Rate exceeded (Service: AmazonDataCatalog; Status Code: 400; "
                "Error Code: SlowDown; Request ID: example; Proxy: null)",
            ),
            False,
        ),
        (ValueError("no response"), False),
    ],
    ids=["listed", "unlisted", "metadata-exception", "no-response"],
)
def test_is_retryable_error(error, expected):
    assert is_retryable_error(error, _Policy(("SlowDown",))) is expected


def test_retry_api_call_retries_listed_codes():
    operation = ThrottledOperation(failures=2)
    assert retry_api_call(operation, S3RetryConfig(multiplier=0)) == "success"
    assert operation.calls == 3
    # An error that the policy does not list is raised at once.
    calls = 0

    def missing():
        nonlocal calls
        calls += 1
        raise _error("NoSuchKey")

    with pytest.raises(ClientError):
        retry_api_call(missing, S3RetryConfig(multiplier=0))
    assert calls == 1


@pytest.mark.parametrize(
    ("multiplier", "max_delay", "expected_bases"),
    [
        (1, 100, [1, 2, 4, 8, 16, 32]),
        (2, 10, [2, 4, 8, 10, 10, 10]),
        (0, 0, [0, 0, 0, 0, 0, 0]),
    ],
)
def test_retry_api_call_waits_with_jitter(monkeypatch, multiplier, max_delay, expected_bases):
    sleeps = []
    monkeypatch.setattr(tenacity.nap.time, "sleep", sleeps.append)
    error = throttling_error("GetObject")
    calls = 0

    def call():
        nonlocal calls
        calls += 1
        raise error

    config = S3RetryConfig(attempt=7, multiplier=multiplier, max_delay=max_delay)
    with pytest.raises(ClientError):
        retry_api_call(call, config)
    assert calls == 7
    assert len(sleeps) == len(expected_bases)
    jitters = [waited - base for waited, base in zip(sleeps, expected_bases, strict=True)]
    assert all(0 <= jitter <= multiplier for jitter in jitters)
    if multiplier:
        assert max(jitters) - min(jitters) > multiplier / 100


@pytest.mark.parametrize("stop_in", ["request", "backoff"])
def test_retry_api_call_stopped_by_event(stop_in):
    # Without the stop, the backoff wait would take at least EVENT_TIMEOUT seconds.
    event = threading.Event()
    call = StoppingOperation(event.set, stop_in)
    started = time.monotonic()
    with stop_retries_on(event), pytest.raises(ClientError) as caught:
        retry_api_call(call, S3RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT))
    call.join()
    assert time.monotonic() - started < EVENT_TIMEOUT
    assert caught.value is call.error
    assert call.calls == 1


def test_retry_api_call_stopped_before_call():
    # A call in a stopped scope makes its first attempt, but no retry.
    event = threading.Event()
    event.set()
    call = StoppingOperation(event.set, "request")
    with stop_retries_on(event), pytest.raises(ClientError):
        retry_api_call(call, S3RetryConfig(multiplier=0))
    assert call.calls == 1


def test_retry_api_call_stopped_by_enclosing_scope():
    outer = threading.Event()
    call = StoppingOperation(outer.set, "backoff")
    with (
        stop_retries_on(outer),
        stop_retries_on(threading.Event()),
        pytest.raises(ClientError),
    ):
        retry_api_call(call, S3RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT))
    call.join()
    assert call.calls == 1


def test_retry_api_call_stopped_in_copied_context():
    # A thread that runs a copy of the context, as the request pool of a
    # filesystem does, sees the scopes of its submitter.
    event = threading.Event()
    event.set()
    operation = ThrottledOperation(failures=2)
    with stop_retries_on(event), ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            contextvars.copy_context().run,
            retry_api_call,
            operation,
            S3RetryConfig(multiplier=0),
        )
        with pytest.raises(ClientError):
            future.result(EVENT_TIMEOUT)
    assert operation.calls == 1


def test_retry_api_call_unstoppable():
    event = threading.Event()
    event.set()
    operation = ThrottledOperation(failures=2)
    with stop_retries_on(event), unstoppable_retries():
        assert retry_api_call(operation, S3RetryConfig(multiplier=0)) == "success"
    assert operation.calls == 3
    # A scope opened in the unstoppable block stops the retries again.
    operation = ThrottledOperation(failures=2)
    with (
        stop_retries_on(event),
        unstoppable_retries(),
        stop_retries_on(event),
        pytest.raises(ClientError),
    ):
        retry_api_call(operation, S3RetryConfig(multiplier=0))
    assert operation.calls == 1


def test_retry_api_call_scope_ends_with_block():
    event = threading.Event()
    with stop_retries_on(event):
        event.set()
    operation = ThrottledOperation(failures=2)
    assert retry_api_call(operation, S3RetryConfig(multiplier=0)) == "success"
    assert operation.calls == 3


def test_raise_if_stopped():
    event = threading.Event()
    with stop_retries_on(event):
        assert not retries_stopped()
        raise_if_stopped("upload part 1")
        event.set()
        assert retries_stopped()
        with pytest.raises(CancelledError, match=r"Stopped before it could upload part 1\."):
            raise_if_stopped("upload part 1")
        with unstoppable_retries():
            assert not retries_stopped()
            raise_if_stopped("abort the upload")
