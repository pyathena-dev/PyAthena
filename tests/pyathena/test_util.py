from typing import Any

import pytest

from pyathena import DataError
from pyathena.util import RetryConfig, parse_output_location, retry_api_call, strtobool


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
