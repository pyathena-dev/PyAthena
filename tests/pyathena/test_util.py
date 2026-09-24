from typing import Any

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
    strtobool,
)


def test_parse_output_location():
    # valid
    actual = parse_output_location("s3://bucket/path/to")
    assert actual[0] == "bucket"
    assert actual[1] == "path/to"

    # invalid
    with pytest.raises(DataError):
        parse_output_location("http://foobar")


def test_strtobool():
    yes = ("y", "Y", "yes", "True", "t", "true", "True", "On", "on", "1")
    no = ("n", "no", "f", "false", "off", "0", "Off", "No", "N")

    for y in yes:
        assert strtobool(y)

    for n in no:
        assert not strtobool(n)


class _WithCodeError(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(f"error:{code}")
        self.response = {"Error": {"Code": code}}


class _NoResponseError(Exception):
    def __init__(self) -> None:
        super().__init__("error")
        self.response = None


def _test_retry(ex: Exception) -> None:
    calls = {"n": 0}

    def fn() -> Any:
        calls["n"] += 1
        raise ex

    cfg = RetryConfig(attempt=1, max_delay=1)

    with pytest.raises(type(ex)):
        retry_api_call(fn, config=cfg)

    assert calls["n"] == 1


def test_retry_api_call():
    _test_retry(_WithCodeError(500))


def test_retry_api_call_with_none_error():
    _test_retry(_NoResponseError())


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


def _throttling_error() -> ClientError:
    return ClientError(
        {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}},
        "GetTableMetadata",
    )


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
    error = _throttling_error()
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
    for waited, base in zip(sleeps, expected_bases, strict=True):
        assert base <= waited <= base + multiplier


def test_retry_api_call_jitter_varies(monkeypatch):
    sleeps = []
    monkeypatch.setattr(tenacity.nap.time, "sleep", sleeps.append)
    error = _throttling_error()

    def call():
        raise error

    config = RetryConfig(attempt=20, multiplier=1, max_delay=1)
    with pytest.raises(ClientError):
        retry_api_call(call, config)
    assert all(1 <= waited <= 2 for waited in sleeps)
    assert len(set(sleeps)) > 1


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
    assert is_retryable_error(ValueError("no response"), RetryConfig()) is False


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
