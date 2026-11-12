# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import contextlib
import inspect
import os
import re
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from inspect import signature
from typing import Any
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qsl

import botocore.session
import pytest
from botocore.awsrequest import AWSResponse
from botocore.config import Config

import pyathena
from pyathena.aio.arrow.cursor import AioArrowCursor
from pyathena.aio.connection import AioConnection
from pyathena.aio.cursor import AioCursor, AioDictCursor
from pyathena.aio.pandas.cursor import AioPandasCursor
from pyathena.aio.polars.cursor import AioPolarsCursor
from pyathena.aio.s3fs.cursor import AioS3FSCursor
from pyathena.aio.spark.cursor import AioSparkCursor
from pyathena.arrow.async_cursor import AsyncArrowCursor
from pyathena.arrow.cursor import ArrowCursor
from pyathena.async_cursor import AsyncCursor, AsyncDictCursor
from pyathena.connection import Connection
from pyathena.converter import DefaultTypeConverter
from pyathena.cursor import Cursor, DictCursor
from pyathena.error import OperationalError, ProgrammingError
from pyathena.filesystem import s3_filesystem
from pyathena.pandas.async_cursor import AsyncPandasCursor
from pyathena.pandas.cursor import PandasCursor
from pyathena.polars.async_cursor import AsyncPolarsCursor
from pyathena.polars.cursor import PolarsCursor
from pyathena.result_set import WithResultSet
from pyathena.s3fs.async_cursor import AsyncS3FSCursor
from pyathena.s3fs.cursor import S3FSCursor
from pyathena.spark.async_cursor import AsyncSparkCursor
from pyathena.spark.common import SparkBaseCursor
from pyathena.spark.cursor import SparkCursor
from pyathena.util import RetryConfig
from tests.pyathena.util import EVENT_TIMEOUT, answered_requests

# Cursors whose arraysize is the GetQueryResults page size, capped at 1000.
PAGED_CURSORS = [Cursor, DictCursor, AsyncCursor, AsyncDictCursor, AioCursor, AioDictCursor]
# Cursors whose arraysize only sets the fetchmany() batch, with no upper limit.
UNCAPPED_CURSORS = [
    ArrowCursor,
    PandasCursor,
    PolarsCursor,
    S3FSCursor,
    AsyncArrowCursor,
    AsyncPandasCursor,
    AsyncPolarsCursor,
    AsyncS3FSCursor,
    AioArrowCursor,
    AioPandasCursor,
    AioPolarsCursor,
    AioS3FSCursor,
]
CURSOR_CLASSES = PAGED_CURSORS + UNCAPPED_CURSORS + [SparkCursor, AsyncSparkCursor, AioSparkCursor]
# Cursors that run on_start_query_execution.
START_CALLBACK_CURSORS = [
    Cursor,
    DictCursor,
    ArrowCursor,
    PandasCursor,
    PolarsCursor,
    S3FSCursor,
    AioCursor,
    AioDictCursor,
    AioArrowCursor,
    AioPandasCursor,
    AioPolarsCursor,
    AioS3FSCursor,
]
# The native asyncio cursors, whose execute() is a coroutine function.
AIO_CURSOR_CLASSES = [
    AioCursor,
    AioDictCursor,
    AioArrowCursor,
    AioPandasCursor,
    AioPolarsCursor,
    AioS3FSCursor,
    AioSparkCursor,
]
# Cursors that pass execute() keyword arguments to a DataFrame reader.
READER_CURSOR_CLASSES = [PandasCursor, PolarsCursor, AsyncPandasCursor, AsyncPolarsCursor]


@pytest.mark.parametrize(
    ("setting", "kwargs", "expected"),
    [
        ("s3_staging_dir", {}, "s3://environment/path/"),
        ("s3_staging_dir", {"s3_staging_dir": None}, "s3://environment/path/"),
        ("s3_staging_dir", {"s3_staging_dir": ""}, None),
        ("s3_staging_dir", {"s3_staging_dir": "s3://explicit/path/"}, "s3://explicit/path/"),
        ("work_group", {}, "environment-workgroup"),
        ("work_group", {"work_group": None}, "environment-workgroup"),
        ("work_group", {"work_group": ""}, None),
        ("work_group", {"work_group": "explicit-workgroup"}, "explicit-workgroup"),
    ],
)
def test_environment_fallback(setting, kwargs, expected, monkeypatch):
    monkeypatch.setenv("AWS_ATHENA_S3_STAGING_DIR", "s3://environment/path/")
    monkeypatch.setenv("AWS_ATHENA_WORK_GROUP", "environment-workgroup")
    conn = Connection(
        region_name="us-east-1",
        aws_access_key_id="access_key",
        aws_secret_access_key="secret_key",
        **kwargs,
    )
    try:
        assert getattr(conn, setting) == expected
    finally:
        conn.close()


def test_empty_settings_disable_both_environment_fallbacks(monkeypatch):
    monkeypatch.setenv("AWS_ATHENA_S3_STAGING_DIR", "s3://environment/path/")
    monkeypatch.setenv("AWS_ATHENA_WORK_GROUP", "environment-workgroup")
    with pytest.raises(ProgrammingError, match="Required argument"):
        Connection(region_name="us-east-1", s3_staging_dir="", work_group="")


@pytest.mark.parametrize(
    "factory",
    [pyathena.connect, Connection, pyathena.aio_connect, AioConnection.create, AioConnection],
)
def test_connect_keyword_only(factory):
    # aio_connect() and AioConnection.create() raise when they are called.
    with pytest.raises(TypeError):
        factory("s3://bucket/path/")


class RecordingCursor(Cursor):
    def __init__(self, custom_option: Any = None, **kwargs: Any) -> None:
        self.kwargs = {**kwargs, "custom_option": custom_option}
        super().__init__(**kwargs)


class _RawBody:
    """The raw body of a botocore HTTP response."""

    def __init__(self, body: bytes) -> None:
        """Initialize the body.

        Args:
            body: The bytes of the response body.
        """
        self._body = body

    def stream(self, **_: Any) -> Iterator[bytes]:
        """Yield the body.

        Args:
            **_: The streaming options, which are ignored.

        Yields:
            The bytes of the body.
        """
        yield self._body


def _sts_credentials(access_key_id: str, expiration: datetime) -> tuple[str, datetime]:
    """Describe the credentials of an STS response.

    Args:
        access_key_id: The access key ID.
        expiration: The expiration time.

    Returns:
        The access key ID and the expiration time.
    """
    return access_key_id, expiration


class _STSResponder:
    """Answer the STS requests of a botocore session with the given credentials, in order."""

    def __init__(self, credentials: list[tuple[str, datetime]]) -> None:
        """Register the responder on a new botocore session.

        Args:
            credentials: The access key ID and expiration of each response.
        """
        self._credentials = iter(credentials)
        #: The parameters of each request, by name.
        self.requests: list[dict[str, str]] = []
        #: The access key ID that signed each request.
        self.signed_with: list[str] = []
        self.botocore_session = botocore.session.Session()
        self.botocore_session.register("before-send.sts", self._send)

    def _send(self, request: Any, **_: Any) -> AWSResponse:
        """Record the request and answer it with the next credentials.

        Args:
            request: The prepared STS request.
            **_: The other event arguments.

        Returns:
            The STS response.
        """
        body = request.body.decode() if isinstance(request.body, bytes) else request.body
        params = dict(parse_qsl(body))
        self.requests.append(params)
        authorization = request.headers["Authorization"].decode()
        self.signed_with.append(re.search(r"Credential=([^/]+)/", authorization).group(1))
        access_key_id, expiration = next(self._credentials)
        action = params["Action"]
        xml = (
            f'<{action}Response xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
            f"<{action}Result><Credentials>"
            f"<AccessKeyId>{access_key_id}</AccessKeyId>"
            "<SecretAccessKey>secret_key</SecretAccessKey>"
            "<SessionToken>session_token</SessionToken>"
            f"<Expiration>{expiration.strftime('%Y-%m-%dT%H:%M:%SZ')}</Expiration>"
            f"</Credentials></{action}Result></{action}Response>"
        )
        return AWSResponse(request.url, 200, {}, _RawBody(xml.encode()))


def _connection(**kwargs: Any) -> Connection[Any]:
    return Connection(
        region_name="us-east-1",
        s3_staging_dir="s3://bucket/path/",
        aws_access_key_id="access_key",
        aws_secret_access_key="secret_key",
        **kwargs,
    )


@pytest.fixture
def isolated_aws_config(monkeypatch, tmp_path):
    """Hide the developer's AWS environment variables and config files.

    Args:
        monkeypatch: The pytest monkeypatch fixture.
        tmp_path: The pytest temporary directory, holding no config files.
    """
    for key in list(os.environ):
        if key.startswith("AWS_"):
            monkeypatch.delenv(key)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "credentials"))


@pytest.fixture
def offline_connection():
    """Yield a connection whose cursors fail the test if they call AWS.

    A Spark cursor reports a started session without calling AWS.
    """
    conn = _connection()
    with (
        patch.object(
            conn.client, "_make_api_call", side_effect=AssertionError("unexpected AWS request")
        ) as request,
        patch.object(SparkBaseCursor, "_start_session", return_value="session"),
    ):
        yield conn
    request.assert_not_called()
    conn.close()


def _open_cursor(conn: Connection[Any], cursor_class: type[Any], **kwargs: Any) -> Any:
    if issubclass(cursor_class, SparkBaseCursor):
        # The session was not started, so closing the cursor must not terminate it.
        kwargs["terminate_session_on_close"] = False
    return conn.cursor(cursor_class, **kwargs)


@contextlib.asynccontextmanager
async def _opened_cursor(conn: Connection[Any], cursor_class: type[Any]) -> Any:
    """Open a cursor and close it, awaiting the close of an aio cursor.

    Args:
        conn: The connection.
        cursor_class: The cursor class.

    Yields:
        The cursor.
    """
    cursor = _open_cursor(conn, cursor_class)
    try:
        yield cursor
    finally:
        closed = cursor.close()
        if inspect.isawaitable(closed):
            await closed


@pytest.mark.parametrize("cursor_class", CURSOR_CLASSES)
def test_cursor_constructor_keyword_only(cursor_class):
    # Binds the arguments as a call does before the constructor runs. A call
    # through Connection.cursor() passes every setting by keyword, so a positional
    # setting would fail there as a duplicate keyword argument too.
    with pytest.raises(TypeError):
        signature(cursor_class).bind("positional-setting")


@pytest.mark.parametrize("cursor_class", CURSOR_CLASSES)
async def test_cursor_execute_keyword_only(offline_connection, cursor_class):
    async with _opened_cursor(offline_connection, cursor_class) as cursor:
        # An aio execute() raises when it is called, before it is awaited.
        with pytest.raises(TypeError):
            cursor.execute("SELECT %(value)s", {"value": 1}, "group")


@pytest.mark.parametrize("cursor_class", CURSOR_CLASSES)
def test_cursor_unknown_constructor_keyword(offline_connection, cursor_class):
    with pytest.raises(TypeError, match="unexpected keyword argument 'work_gruop'"):
        offline_connection.cursor(cursor_class, work_gruop="typo")
    offline_connection.cursor_kwargs = {"work_gruop": "typo"}
    with pytest.raises(TypeError, match="unexpected keyword argument 'work_gruop'"):
        offline_connection.cursor(cursor_class)


@pytest.mark.parametrize(
    "given_by", ["connection", "cursor_kwargs", "cursor_kwargs_over_connection"]
)
@pytest.mark.parametrize("cursor_class", CURSOR_CLASSES)
async def test_cursor_accepts_connection_callbacks(offline_connection, cursor_class, given_by):
    """The connection's callbacks, or the ones its cursor_kwargs give over them, run."""
    started, polled, overridden = [], [], []
    callbacks = {"on_start_query_execution": started.append, "on_poll": polled.append}
    if given_by == "connection":
        for name, callback in callbacks.items():
            setattr(offline_connection, name, callback)
    else:
        if given_by == "cursor_kwargs_over_connection":
            for name in callbacks:
                setattr(offline_connection, name, overridden.append)
        offline_connection.cursor_kwargs = callbacks

    with answered_requests(offline_connection.client):
        async with _opened_cursor(offline_connection, cursor_class) as cursor:
            result = None
            with contextlib.suppress(OperationalError):
                result = cursor.execute("SELECT 1")
                if inspect.isawaitable(result):
                    await result
            if isinstance(result, tuple):
                # The Async cursors run the execution in a future.
                result[1].result(timeout=EVENT_TIMEOUT)

    assert [execution.state for execution in polled] == ["FAILED"]
    # The Async and Spark cursors do not run on_start_query_execution.
    assert started == (["query-id"] if cursor_class in START_CALLBACK_CURSORS else [])
    assert overridden == []


# aio/test_connection.py tests the native asyncio cursors.
@pytest.mark.parametrize(
    "cursor_class",
    [c for c in CURSOR_CLASSES if c not in READER_CURSOR_CLASSES + AIO_CURSOR_CLASSES],
)
def test_cursor_execute_unknown_keyword(offline_connection, cursor_class):
    with _open_cursor(offline_connection, cursor_class) as cursor:
        previous = MagicMock(is_closed=False)
        if isinstance(cursor, SparkBaseCursor):
            cursor._calculation_id = "previous"
        elif isinstance(cursor, WithResultSet):
            cursor._query_id = "previous"
            cursor._result_set = previous
        state = vars(cursor).copy()

        with pytest.raises(
            TypeError,
            match=rf"{cursor_class.__name__}\.execute\(\) got an unexpected keyword "
            "argument 'work_gruop'",
        ):
            cursor.execute("SELECT 1", work_gruop="typo")

        assert vars(cursor) == state
        previous.close.assert_not_called()


@pytest.mark.parametrize(
    ("cursor_class", "kwargs"),
    [
        (cursor_class, {"block_size": 64, "connect_timeout": 1, "request_timeout": 2})
        for cursor_class in (ArrowCursor, AsyncArrowCursor)
    ]
    + [
        (cursor_class, {"block_size": 64, "csv_reader": None, "filesystem_class": None})
        for cursor_class in (S3FSCursor, AsyncS3FSCursor)
    ]
    + [
        (cursor_class, {"chunksize": 10, "parse_dates": []})
        for cursor_class in (PandasCursor, AsyncPandasCursor)
    ]
    + [
        (cursor_class, {"chunksize": 10, "separator": "|"})
        for cursor_class in (PolarsCursor, AsyncPolarsCursor)
    ],
)
def test_cursor_execute_supported_keywords(offline_connection, cursor_class, kwargs):
    cursor = _open_cursor(offline_connection, cursor_class)
    with (
        cursor,
        patch.object(cursor, "_execute", side_effect=RuntimeError("query started")),
        pytest.raises(RuntimeError, match="query started"),
    ):
        cursor.execute("SELECT 1", **kwargs)


class TestConnection:
    @pytest.mark.parametrize(
        ("key", "configured", "explicit"),
        [
            ("converter", DefaultTypeConverter(), DefaultTypeConverter()),
            ("kill_on_interrupt", False, True),
            ("retry_config", RetryConfig(attempt=1), RetryConfig(attempt=2)),
            ("schema_name", "configured", "explicit"),
            ("custom_option", True, False),
        ],
    )
    def test_cursor_arguments_override_cursor_kwargs(self, key, configured, explicit):
        cursor_kwargs = {key: configured}
        conn = _connection(cursor_kwargs=cursor_kwargs)

        cursor = conn.cursor(RecordingCursor, **{key: explicit})

        assert cursor.kwargs[key] is explicit
        # The connection's defaults are not changed by one cursor's arguments.
        assert conn.cursor_kwargs == {key: configured}
        assert conn.cursor(RecordingCursor).kwargs[key] is configured

    def test_cursor_kwargs_override_connection_defaults(self):
        conn = _connection(
            schema_name="connection",
            kill_on_interrupt=True,
            cursor_kwargs={"schema_name": "configured", "kill_on_interrupt": False},
        )

        cursor = conn.cursor(RecordingCursor)

        assert cursor.kwargs["schema_name"] == "configured"
        assert cursor.kwargs["kill_on_interrupt"] is False

    @pytest.mark.parametrize("cursor_class", PAGED_CURSORS + UNCAPPED_CURSORS)
    def test_cursor_arraysize(self, cursor_class):
        conn = _connection(cursor_kwargs={"arraysize": 25})

        assert conn.cursor(cursor_class).arraysize == 25
        assert conn.cursor(cursor_class, arraysize=50).arraysize == 50

    def test_cursor_arraysize_default_fetch_size(self):
        class SmallPageCursor(Cursor):
            DEFAULT_FETCH_SIZE = 500

        assert _connection().cursor(SmallPageCursor).arraysize == 500

    @pytest.mark.parametrize("cursor_class", PAGED_CURSORS)
    def test_cursor_arraysize_over_page_size(self, cursor_class):
        with pytest.raises(ProgrammingError):
            _connection().cursor(cursor_class, arraysize=1001)

    @pytest.mark.parametrize("cursor_class", UNCAPPED_CURSORS)
    def test_cursor_arraysize_uncapped(self, cursor_class):
        assert _connection().cursor(cursor_class, arraysize=1001).arraysize == 1001

    @pytest.mark.parametrize("cursor_class", PAGED_CURSORS + UNCAPPED_CURSORS)
    def test_cursor_arraysize_not_positive(self, cursor_class):
        with pytest.raises(ProgrammingError):
            _connection().cursor(cursor_class, arraysize=0)

    def test_s3_client_built_once_across_threads(self):
        conn = _connection()
        created = []
        session_client = conn.session.client

        second_build = threading.Event()

        def contended_client(*args, **kwargs):
            created.append(args)
            # Without the lock, another thread would build a client meanwhile.
            if len(created) == 1:
                second_build.wait(1)
            else:
                second_build.set()
            return session_client(*args, **kwargs)

        conn.session.client = contended_client
        barrier = threading.Barrier(8)

        def get_client(_):
            barrier.wait()
            return conn.s3_client

        with ThreadPoolExecutor(max_workers=8) as executor:
            clients = list(executor.map(get_client, range(8)))

        assert created == [("s3",)]
        assert all(client is clients[0] for client in clients)
        assert clients[0].meta.service_model.service_name == "s3"

    def test_s3_client_leaves_out_athena_endpoint(self, isolated_aws_config):
        # GH-576: Athena's endpoint_url (e.g. its VPC endpoint) was sent to S3.
        conn = _connection(
            endpoint_url="https://athena.us-east-1.amazonaws.com",
            # Athena's API version, which S3 does not have.
            api_version="2017-05-18",
        )

        assert conn.client.meta.endpoint_url == "https://athena.us-east-1.amazonaws.com"
        assert conn.s3_client.meta.endpoint_url == "https://s3.amazonaws.com"
        assert conn.s3_client.meta.service_model.api_version == "2006-03-01"

    def test_s3_client_uses_s3_endpoint_setting(self, isolated_aws_config, monkeypatch):
        monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://localhost:4566")
        conn = _connection(endpoint_url="https://athena.us-east-1.amazonaws.com")

        assert conn.s3_client.meta.endpoint_url == "http://localhost:4566"
        assert conn.client.meta.endpoint_url == "https://athena.us-east-1.amazonaws.com"

    def test_s3_filesystem_uses_connection_s3_client(self):
        # A policy with the default codes gets a copy with the S3 codes
        # (GH-1375); one with other codes is used as is.
        conn = _connection(retry_config=RetryConfig(exceptions=["SlowDown"], attempt=3))

        fs = s3_filesystem(conn, skip_instance_cache=True)

        assert fs._client is conn.s3_client
        assert fs.core.retry_config is conn.retry_config

    def test_s3_config_defaults_to_config(self):
        conn = _connection(config=Config(max_pool_connections=20))

        assert conn.s3_config is conn.config
        assert conn.s3_client.meta.config.max_pool_connections == 20

    def test_s3_config_merged_over_config(self):
        conn = _connection(
            config=Config(connect_timeout=3, max_pool_connections=20),
            s3_config=Config(max_pool_connections=50, user_agent_extra="s3-agent"),
        )

        s3_config = conn.s3_client.meta.config
        assert s3_config.max_pool_connections == 50
        assert s3_config.connect_timeout == 3
        assert pyathena.user_agent_extra in s3_config.user_agent_extra
        assert "s3-agent" in s3_config.user_agent_extra
        assert conn.client.meta.config.max_pool_connections == 20
        assert "s3-agent" not in conn.client.meta.config.user_agent_extra

    @pytest.mark.parametrize(
        ("kwargs", "prompts"),
        [
            ({"role_arn": "arn:aws:iam::123456789012:role/role"}, False),
            ({"serial_number": "arn:aws:iam::123456789012:mfa/device"}, True),
            (
                {
                    "role_arn": "arn:aws:iam::123456789012:role/role",
                    "serial_number": "arn:aws:iam::123456789012:mfa/device",
                },
                True,
            ),
        ],
        ids=["role_arn", "serial_number", "role_arn_with_mfa"],
    )
    def test_sts_credentials_refreshed(self, isolated_aws_config, monkeypatch, kwargs, prompts):
        # GH-1343: the STS credentials were stored as static keys, so every client
        # of a long-lived connection failed with ExpiredToken after duration_seconds.
        now = datetime.now(UTC)
        sts = _STSResponder(
            [
                # Expire within botocore's mandatory refresh window.
                _sts_credentials("ASIAEXPIRING", now + timedelta(minutes=1)),
                _sts_credentials("ASIAREFRESHED", now + timedelta(hours=1)),
            ]
        )
        codes = iter(["111111", "222222"])
        monkeypatch.setattr("builtins.input", lambda _: next(codes))
        signed_with = []

        def send(request, **_):
            authorization = request.headers["Authorization"].decode()
            signed_with.append(re.search(r"Credential=([^/]+)/", authorization).group(1))
            # Athena answers in JSON, S3 in XML.
            body = b"{}" if "athena" in request.url else b"<ListAllMyBucketsResult/>"
            return AWSResponse(request.url, 200, {}, _RawBody(body))

        with contextlib.closing(
            _connection(botocore_session=sts.botocore_session, **kwargs)
        ) as conn:
            for client in (conn.client, conn.s3_client):
                client.meta.events.register("before-send", send)
            conn.client.list_work_groups()
            conn.s3_client.list_buckets()

        assert signed_with == ["ASIAREFRESHED", "ASIAREFRESHED"]
        # The STS requests are signed with the connection's credential arguments.
        assert sts.signed_with == ["access_key", "access_key"]
        action = "AssumeRole" if "role_arn" in kwargs else "GetSessionToken"
        assert [r["Action"] for r in sts.requests] == [action, action]
        if prompts:
            # Each request prompts for a new MFA code.
            assert [r["TokenCode"] for r in sts.requests] == ["111111", "222222"]
            assert all(r["SerialNumber"] == kwargs["serial_number"] for r in sts.requests)
        else:
            assert all("TokenCode" not in r for r in sts.requests)

    @pytest.mark.parametrize(
        ("kwargs", "requests"),
        [
            ({"role_arn": "arn:aws:iam::123456789012:role/role"}, 2),
            # Each request prompts for an MFA code, so it waits for the mandatory refresh.
            ({"serial_number": "arn:aws:iam::123456789012:mfa/device"}, 1),
        ],
        ids=["role_arn", "serial_number"],
    )
    def test_sts_credentials_advisory_refresh(
        self, isolated_aws_config, monkeypatch, kwargs, requests
    ):
        now = datetime.now(UTC)
        sts = _STSResponder(
            [
                # Within the advisory refresh window of a duration of an hour (15
                # minutes), but not within the mandatory one (10 minutes).
                _sts_credentials("ASIAADVISORY", now + timedelta(minutes=12)),
                _sts_credentials("ASIAREFRESHED", now + timedelta(hours=1)),
            ]
        )
        monkeypatch.setattr("builtins.input", lambda _: "123456")
        with contextlib.closing(
            _connection(botocore_session=sts.botocore_session, **kwargs)
        ) as conn:
            conn.session.get_credentials().get_frozen_credentials()

        assert len(sts.requests) == requests

    def test_close_closes_built_clients(self):
        conn = _connection()
        with (
            patch.object(conn.session, "client") as build,
            patch.object(conn.client, "close") as athena_close,
        ):
            conn.close()

        athena_close.assert_called_once_with()
        # Clients that were not used are not built to be closed.
        build.assert_not_called()

        with (
            patch.object(conn.client, "close") as athena_close,
            patch.object(conn._glue.client, "close") as glue_close,
            patch.object(conn.s3_client, "close") as s3_close,
        ):
            conn.close()

        athena_close.assert_called_once_with()
        glue_close.assert_called_once_with()
        s3_close.assert_called_once_with()

    def test_close_closes_sts_client(self, isolated_aws_config):
        # GH-1343: the STS client that refreshes the credentials of role_arn is
        # closed with the connection.
        sts = _STSResponder([_sts_credentials("ASIAROLE", datetime.now(UTC) + timedelta(hours=1))])
        conn = _connection(
            role_arn="arn:aws:iam::123456789012:role/role", botocore_session=sts.botocore_session
        )
        with patch.object(conn._sts_client, "close") as sts_close:
            conn.close()

        sts_close.assert_called_once_with()

    def test_internal_cursor_leaves_out_default_cursor_options(self):
        cursor_kwargs = {
            "unload": True,
            "chunksize": 10,
            "arraysize": 5000,
            "schema_name": "configured",
        }
        conn = _connection(
            cursor_class=PandasCursor,
            cursor_kwargs=cursor_kwargs,
            converter=PandasCursor.get_default_converter(True),
        )

        cursor = conn._internal_cursor(Cursor)

        assert type(cursor) is Cursor
        assert cursor._schema_name == "configured"
        assert cursor.arraysize == Cursor.DEFAULT_FETCH_SIZE
        assert type(cursor._converter) is DefaultTypeConverter
        assert conn.cursor_kwargs == cursor_kwargs
        # Creating the cursor directly still passes the options it does not accept.
        with pytest.raises(TypeError, match="unexpected keyword argument 'unload'"):
            conn.cursor(Cursor)

    def test_internal_cursor_leaves_out_custom_cursor_options(self):
        class CustomCursor(Cursor):
            def __init__(self, **kwargs: Any) -> None:
                self.trace_id = kwargs.pop("trace_id")
                super().__init__(**kwargs)

        conn = _connection(cursor_class=CustomCursor, cursor_kwargs={"trace_id": "x"})

        assert conn.cursor().trace_id == "x"
        assert type(conn._internal_cursor(Cursor)) is Cursor

    def test_cursor_kwargs_named_cursor(self):
        class CustomCursor(Cursor):
            def __init__(self, *, cursor: Any = None, **kwargs: Any) -> None:
                self.custom_cursor = cursor
                super().__init__(**kwargs)

        conn = _connection(cursor_class=CustomCursor, cursor_kwargs={"cursor": "x"})

        assert conn.cursor().custom_cursor == "x"
