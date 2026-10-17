import threading
import time

import pytest
import tenacity.nap
from botocore.exceptions import ClientError

from pyathena import DataError
from pyathena.util import (
    RetryConfig,
    _is_throttling_error,
    _without_retries,
    is_retryable_error,
    parse_output_location,
    retry_api_call,
    stop_retries_on,
    strtobool,
    unstoppable_retries,
)
from tests.pyathena.util import (
    EVENT_TIMEOUT,
    StoppingOperation,
    ThrottledOperation,
    throttling_error,
)


def test_parse_output_location():
    # valid
    actual = parse_output_location("s3://bucket/path/to")
    assert actual[0] == "bucket"
    assert actual[1] == "path/to"

    # invalid
    with pytest.raises(DataError):
        parse_output_location("http://foobar")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("y", True),
        ("Y", True),
        ("yes", True),
        ("YES", True),
        ("t", True),
        ("T", True),
        ("true", True),
        ("True", True),
        ("TRUE", True),
        ("on", True),
        ("On", True),
        ("ON", True),
        ("1", True),
        ("n", False),
        ("N", False),
        ("no", False),
        ("No", False),
        ("NO", False),
        ("f", False),
        ("F", False),
        ("false", False),
        ("False", False),
        ("FALSE", False),
        ("off", False),
        ("Off", False),
        ("OFF", False),
        ("0", False),
    ],
)
def test_strtobool(value, expected):
    assert strtobool(value) is expected


@pytest.mark.parametrize("value", ["", "invalid", "2", " true "])
def test_strtobool_invalid(value):
    with pytest.raises(ValueError, match="invalid truth value"):
        strtobool(value)


class _NoResponseError(Exception):
    def __init__(self) -> None:
        super().__init__("error")
        self.response = None


@pytest.mark.parametrize(
    ("code", "message", "exceptions", "expected_calls"),
    [
        ("ThrottlingException", "Rate exceeded", ("ThrottlingException",), 2),
        (
            "MetadataException",
            "Rate exceeded (Service: AmazonDataCatalog; Status Code: 400; "
            "Error Code: ThrottlingException; Request ID: example; Proxy: null)",
            ("ThrottlingException",),
            2,
        ),
        (
            "MetadataException",
            "Not authorized (Service: AmazonDataCatalog; Status Code: 400; "
            "Error Code: AccessDeniedException; Request ID: example; Proxy: null)",
            ("ThrottlingException",),
            1,
        ),
        ("MetadataException", "Table ThrottlingException not found", ("ThrottlingException",), 1),
        ("MetadataException", "Rate exceeded", ("ThrottlingException",), 1),
        (
            "MetadataException",
            "Rate exceeded (Service: AmazonDataCatalog; Status Code: 400; "
            "Error Code: ThrottlingException; Request ID: example; Proxy: null)",
            (),
            1,
        ),
        ("MetadataException", "Custom error", ("MetadataException",), 2),
        (
            "MetadataException",
            "Too many requests (Service: AmazonDataCatalog; Status Code: 400; "
            "Error Code: TooManyRequestsException; Request ID: example; Proxy: null)",
            ("TooManyRequestsException",),
            2,
        ),
        (
            "MetadataException",
            "Table '(Service: AmazonDataCatalog; Status Code: 400; "
            "Error Code: ThrottlingException; Request ID: fake; Proxy: null)' not found "
            "(Service: AmazonDataCatalog; Status Code: 400; "
            "Error Code: EntityNotFoundException; Request ID: actual; Proxy: null)",
            ("ThrottlingException",),
            1,
        ),
    ],
)
def test_retry_metadata_errors(code, message, exceptions, expected_calls):
    error = ClientError({"Error": {"Code": code, "Message": message}}, "GetTableMetadata")
    calls = 0

    def call():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise error
        return "success"

    config = RetryConfig(exceptions=exceptions, attempt=2, multiplier=0, max_delay=0)
    if expected_calls == 1:
        with pytest.raises(ClientError) as caught:
            retry_api_call(call, config)
        assert caught.value is error
    else:
        assert retry_api_call(call, config) == "success"
    assert calls == expected_calls


@pytest.mark.parametrize("wrapped", [False, True])
@pytest.mark.parametrize("policy_type", [tuple, list, iter, str])
def test_retry_api_call_with_reusable_exception_policy(wrapped, policy_type):
    error = ClientError(
        {
            "Error": {
                "Code": "MetadataException" if wrapped else "ThrottlingException",
                "Message": "Rate exceeded (Service: AmazonDataCatalog; Status Code: 400; "
                "Error Code: ThrottlingException; Request ID: example; Proxy: null)",
            }
        },
        "GetTableMetadata",
    )
    exceptions = (
        "ThrottlingException" if policy_type is str else policy_type(("ThrottlingException",))
    )
    config = RetryConfig(exceptions=exceptions, attempt=3, multiplier=0, max_delay=0)
    for _ in range(2):
        calls = 0

        def call():
            nonlocal calls
            calls += 1
            if calls < 3:
                raise error
            return "success"

        assert retry_api_call(call, config) == "success"
        assert calls == 3


class TestRetryConfig:
    def test_default_attempts(self):
        assert RetryConfig().attempt == 8

    @pytest.mark.parametrize("policy_type", [tuple, list, iter, str])
    def test_captures_exception_names(self, policy_type):
        names = ["ThrottlingException", "TooManyRequestsException"]
        exceptions = names[0] if policy_type is str else policy_type(names)
        config = RetryConfig(exceptions=exceptions)
        names.append("InternalServerException")
        assert config.exceptions == (
            ("ThrottlingException",) if policy_type is str else tuple(names[:2])
        )


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
    error = throttling_error("GetTableMetadata")
    calls = 0

    def call():
        nonlocal calls
        calls += 1
        raise error

    config = RetryConfig(attempt=7, multiplier=multiplier, max_delay=max_delay)
    with pytest.raises(ClientError):
        retry_api_call(call, config)
    assert calls == 7
    assert len(sleeps) == len(expected_bases)
    jitters = [waited - base for waited, base in zip(sleeps, expected_bases, strict=True)]
    assert all(0 <= jitter <= multiplier for jitter in jitters)
    if multiplier:
        # Random, so the six waits do not add the same jitter. Rounded, so that
        # the float residuals of a constant jitter do not differ.
        assert len({round(jitter, 6) for jitter in jitters}) > 1


@pytest.mark.parametrize(
    ("code", "message", "expected"),
    [
        ("ThrottlingException", "Rate exceeded", True),
        (
            "MetadataException",
            "Rate exceeded (Service: AmazonDataCatalog; Status Code: 400; "
            "Error Code: ThrottlingException; Request ID: example; Proxy: null)",
            True,
        ),
        (
            "MetadataException",
            "Not authorized (Service: AmazonDataCatalog; Status Code: 400; "
            "Error Code: AccessDeniedException; Request ID: example; Proxy: null)",
            False,
        ),
        ("MetadataException", "Table not found", False),
    ],
)
def test_is_retryable_error(code, message, expected):
    error = ClientError({"Error": {"Code": code, "Message": message}}, "GetTableMetadata")
    assert is_retryable_error(error, RetryConfig()) is expected


@pytest.mark.parametrize("error", [ValueError("no response"), _NoResponseError()])
def test_is_retryable_error_without_response(error):
    assert is_retryable_error(error, RetryConfig()) is False


@pytest.mark.parametrize(
    ("code", "message", "expected"),
    [
        ("ThrottlingException", "Rate exceeded", True),
        ("TooManyRequestsException", "Too many requests", True),
        # Glue's own throttling, reported by Athena inside a MetadataException.
        (
            "MetadataException",
            "Rate exceeded (Service: AmazonDataCatalog; Status Code: 400; "
            "Error Code: ThrottlingException; Request ID: example; Proxy: null)",
            True,
        ),
        (
            "MetadataException",
            "Not authorized (Service: AmazonDataCatalog; Status Code: 400; "
            "Error Code: AccessDeniedException; Request ID: example; Proxy: null)",
            False,
        ),
        ("MetadataException", "Table ThrottlingException not found", False),
        ("InternalServerException", "Internal error", False),
    ],
)
def test_is_throttling_error(code, message, expected):
    error = ClientError({"Error": {"Code": code, "Message": message}}, "GetTableMetadata")
    assert _is_throttling_error(error) is expected
    assert _is_throttling_error(ValueError("no response")) is False


def test_without_retries():
    config = RetryConfig(
        exceptions=("ThrottlingException", "MetadataException", "InternalServerException"),
        attempt=4,
        multiplier=2,
        max_delay=30,
        exponential_base=3,
    )

    derived = _without_retries(config, ["ThrottlingException", "MetadataException"])

    assert derived.exceptions == ("InternalServerException",)
    assert (derived.attempt, derived.multiplier, derived.max_delay) == (4, 2, 30)
    assert derived.exponential_base == 3
    # The original policy is left as it was.
    assert config.exceptions == (
        "ThrottlingException",
        "MetadataException",
        "InternalServerException",
    )


@pytest.mark.parametrize(
    ("code", "expected_calls"),
    [
        # Stopped at once, although the policy retries it.
        ("ThrottlingException", 1),
        # Other retryable codes keep the policy's attempts.
        ("InternalServerException", 3),
    ],
)
def test_retry_api_call_stops_on_predicate(code, expected_calls):
    error = ClientError({"Error": {"Code": code, "Message": ""}}, "GetTableMetadata")
    calls = 0

    def call():
        nonlocal calls
        calls += 1
        raise error

    config = RetryConfig(
        exceptions=("ThrottlingException", "InternalServerException"),
        attempt=3,
        multiplier=0,
        max_delay=0,
    )

    with pytest.raises(ClientError) as caught:
        retry_api_call(call, config, stop_on=_is_throttling_error)

    assert caught.value is error
    assert calls == expected_calls


@pytest.mark.parametrize("stop_in", ["request", "backoff"])
def test_retry_api_call_stopped_by_event(stop_in):
    """GH-1202: the event of a stop scope ends the retries and their backoff wait.

    Without the stop, the backoff wait would take at least EVENT_TIMEOUT seconds.
    """
    event = threading.Event()
    call = StoppingOperation(event.set, stop_in)
    started = time.monotonic()
    with stop_retries_on(event), pytest.raises(ClientError) as caught:
        retry_api_call(call, RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT))
    call.join()
    assert time.monotonic() - started < EVENT_TIMEOUT
    assert caught.value is call.error
    assert call.calls == 1


def test_retry_api_call_stopped_before_call():
    """A call in a stopped scope makes its first attempt, but no retry."""
    event = threading.Event()
    event.set()
    call = StoppingOperation(event.set, "request")
    with stop_retries_on(event), pytest.raises(ClientError):
        retry_api_call(call, RetryConfig(multiplier=0))
    assert call.calls == 1


def test_retry_api_call_stopped_by_enclosing_scope():
    """The event of any enclosing scope stops the retries."""
    outer = threading.Event()
    call = StoppingOperation(outer.set, "backoff")
    with (
        stop_retries_on(outer),
        stop_retries_on(threading.Event()),
        pytest.raises(ClientError),
    ):
        retry_api_call(call, RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT))
    call.join()
    assert call.calls == 1


def test_retry_api_call_unstoppable():
    """Retries in an unstoppable block ignore the events of the enclosing scopes."""
    event = threading.Event()
    event.set()
    operation = ThrottledOperation(failures=2)
    with stop_retries_on(event), unstoppable_retries():
        assert retry_api_call(operation, RetryConfig(multiplier=0)) == "success"
    assert operation.calls == 3
    # A scope opened in the unstoppable block stops the retries again.
    operation = ThrottledOperation(failures=2)
    with (
        stop_retries_on(event),
        unstoppable_retries(),
        stop_retries_on(event),
        pytest.raises(ClientError),
    ):
        retry_api_call(operation, RetryConfig(multiplier=0))
    assert operation.calls == 1


def test_retry_api_call_scope_ends_with_block():
    """The event of a scope does not stop the retries after its block."""
    event = threading.Event()
    with stop_retries_on(event):
        event.set()
    operation = ThrottledOperation(failures=2)
    assert retry_api_call(operation, RetryConfig(multiplier=0)) == "success"
    assert operation.calls == 3
