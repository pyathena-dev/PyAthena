# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import os
import threading
from concurrent.futures import ThreadPoolExecutor
from inspect import Parameter, signature
from typing import Any
from unittest.mock import patch

import pytest
from botocore.config import Config

import pyathena
from pyathena.arrow.async_cursor import AsyncArrowCursor
from pyathena.arrow.cursor import ArrowCursor
from pyathena.async_cursor import AsyncCursor, AsyncDictCursor
from pyathena.common import BaseCursor
from pyathena.connection import Connection
from pyathena.converter import DefaultTypeConverter
from pyathena.cursor import Cursor, DictCursor
from pyathena.error import ProgrammingError
from pyathena.filesystem.s3 import S3FileSystem
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

# Cursors whose arraysize is the GetQueryResults page size, capped at 1000.
PAGED_CURSORS = [Cursor, DictCursor, AsyncCursor, AsyncDictCursor]
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
]
CURSOR_CLASSES = PAGED_CURSORS + UNCAPPED_CURSORS + [SparkCursor, AsyncSparkCursor]


@pytest.mark.parametrize("factory", [pyathena.connect, Connection])
def test_connect_keyword_only(factory):
    assert all(
        parameter.kind in (Parameter.KEYWORD_ONLY, Parameter.VAR_KEYWORD)
        for parameter in signature(factory).parameters.values()
    )
    with patch("pyathena.connection.Session") as session:
        with pytest.raises(TypeError, match="positional"):
            factory("s3://bucket/path/", "us-east-1")
        session.assert_not_called()


@pytest.mark.parametrize(
    "cursor_class", [*CURSOR_CLASSES, BaseCursor, SparkBaseCursor, WithResultSet]
)
def test_cursor_constructor_keyword_only(cursor_class):
    constructor = cursor_class.__init__
    assert [
        parameter.name
        for parameter in signature(constructor).parameters.values()
        if parameter.kind in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
    ] == ["self"]
    with pytest.raises(TypeError, match="positional"):
        constructor(object(), "positional-setting")


@pytest.mark.parametrize("cursor_class", CURSOR_CLASSES)
def test_cursor_execute_keyword_only(cursor_class):
    execute_signature = signature(cursor_class.execute)
    assert [
        parameter.name
        for parameter in execute_signature.parameters.values()
        if parameter.kind in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
    ] == ["self", "operation", "parameters"]
    parameters = {"value": 1}
    cursor = object.__new__(cursor_class)
    bound = execute_signature.bind(cursor, "SELECT %(value)s", parameters, work_group="group")
    assert bound.arguments["parameters"] is parameters
    assert bound.arguments["work_group"] == "group"
    execute_signature.bind(cursor, operation="SELECT 1")
    with pytest.raises(TypeError, match="positional"):
        cursor.execute("SELECT %(value)s", parameters, "group")


class RecordingCursor(Cursor):
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        super().__init__(**kwargs)


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


class TestConnection:
    @pytest.mark.parametrize(
        ("key", "configured", "explicit"),
        [
            ("converter", DefaultTypeConverter(), DefaultTypeConverter()),
            ("kill_on_interrupt", False, True),
            ("retry_config", RetryConfig(attempt=1), RetryConfig(attempt=2)),
            ("schema_name", "configured", "explicit"),
            ("unload", True, False),
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

        def contended_client(*args, **kwargs):
            created.append((args, conn._s3_client_lock.locked()))
            return session_client(*args, **kwargs)

        conn._session.client = contended_client
        barrier = threading.Barrier(8)

        def get_client(_):
            barrier.wait()
            return conn.s3_client

        with ThreadPoolExecutor(max_workers=8) as executor:
            clients = list(executor.map(get_client, range(8)))

        assert created == [(("s3",), True)]
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
        conn = _connection()

        fs = S3FileSystem(connection=conn, skip_instance_cache=True)

        assert fs._client is conn.s3_client

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

    def test_close_closes_built_clients(self):
        conn = _connection()
        with patch.object(conn.client, "close") as athena_close:
            conn.close()

        athena_close.assert_called_once_with()
        # Clients that were not used are not built to be closed.
        assert conn._glue._client is None
        assert conn._s3_client is None

        with (
            patch.object(conn.client, "close") as athena_close,
            patch.object(conn._glue.client, "close") as glue_close,
            patch.object(conn.s3_client, "close") as s3_close,
        ):
            conn.close()

        athena_close.assert_called_once_with()
        glue_close.assert_called_once_with()
        s3_close.assert_called_once_with()
