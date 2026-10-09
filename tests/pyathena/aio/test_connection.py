# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from inspect import Parameter, signature
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import pyathena
from pyathena.aio.arrow.cursor import AioArrowCursor
from pyathena.aio.connection import AioConnection
from pyathena.aio.cursor import AioCursor, AioDictCursor
from pyathena.aio.pandas.cursor import AioPandasCursor
from pyathena.aio.polars.cursor import AioPolarsCursor
from pyathena.aio.s3fs.cursor import AioS3FSCursor
from pyathena.aio.spark.cursor import AioSparkCursor
from pyathena.converter import DefaultTypeConverter
from pyathena.error import ProgrammingError
from pyathena.spark.common import SparkBaseCursor

# Cursors whose arraysize is the GetQueryResults page size, capped at 1000.
PAGED_CURSORS = [AioCursor, AioDictCursor]
# Cursors whose arraysize only sets the fetchmany() batch, with no upper limit.
UNCAPPED_CURSORS = [AioArrowCursor, AioPandasCursor, AioPolarsCursor, AioS3FSCursor]
CURSOR_CLASSES = PAGED_CURSORS + UNCAPPED_CURSORS + [AioSparkCursor]
# Cursors that pass execute() keyword arguments to a DataFrame reader.
READER_CURSOR_CLASSES = [AioPandasCursor, AioPolarsCursor]


@pytest.mark.parametrize("factory", [pyathena.aio_connect, AioConnection.create])
async def test_connect_keyword_only(factory):
    assert all(
        parameter.kind in (Parameter.KEYWORD_ONLY, Parameter.VAR_KEYWORD)
        for parameter in signature(factory).parameters.values()
    )
    with patch("pyathena.connection.Session") as session:
        with pytest.raises(TypeError, match="positional"):
            await factory("s3://bucket/path/", "us-east-1")
        session.assert_not_called()


def test_connection_constructor_keyword_only():
    with patch("pyathena.connection.Session") as session:
        with pytest.raises(TypeError, match="positional"):
            AioConnection("s3://bucket/path/")
        session.assert_not_called()


@pytest.mark.parametrize("cursor_class", CURSOR_CLASSES)
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


class RecordingAioCursor(AioCursor):
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        super().__init__(**kwargs)


async def _connection(**kwargs: Any) -> AioConnection:
    return await AioConnection.create(
        region_name="us-east-1",
        s3_staging_dir="s3://bucket/path/",
        aws_access_key_id="access_key",
        aws_secret_access_key="secret_key",
        **kwargs,
    )


@pytest.fixture
async def offline_connection():
    """Yield a connection whose cursors fail the test if they call AWS.

    A Spark cursor reports a started session without calling AWS.
    """
    conn = await _connection()
    with (
        patch.object(
            conn.client, "_make_api_call", side_effect=AssertionError("unexpected AWS request")
        ) as request,
        patch.object(SparkBaseCursor, "_start_session", return_value="session"),
    ):
        yield conn
    request.assert_not_called()
    conn.close()


def _open_cursor(conn: AioConnection, cursor_class: type[Any], **kwargs: Any) -> Any:
    if issubclass(cursor_class, SparkBaseCursor):
        # The session was not started, so closing the cursor must not terminate it.
        kwargs["terminate_session_on_close"] = False
    return conn.cursor(cursor_class, **kwargs)


@pytest.mark.parametrize("cursor_class", CURSOR_CLASSES)
async def test_cursor_unknown_constructor_keyword(offline_connection, cursor_class):
    with pytest.raises(TypeError, match="unexpected keyword argument 'work_gruop'"):
        offline_connection.cursor(cursor_class, work_gruop="typo")
    offline_connection.cursor_kwargs = {"work_gruop": "typo"}
    with pytest.raises(TypeError, match="unexpected keyword argument 'work_gruop'"):
        offline_connection.cursor(cursor_class)


@pytest.mark.parametrize("cursor_class", CURSOR_CLASSES)
async def test_cursor_accepts_connection_callbacks(offline_connection, cursor_class):
    on_start_query_execution, on_poll = MagicMock(), MagicMock()
    offline_connection.on_start_query_execution = on_start_query_execution
    offline_connection.on_poll = on_poll

    async with _open_cursor(offline_connection, cursor_class) as cursor:
        assert cursor._on_start_query_execution is on_start_query_execution
        assert cursor._on_poll is on_poll


@pytest.mark.parametrize(
    "cursor_class", [c for c in CURSOR_CLASSES if c not in READER_CURSOR_CLASSES]
)
async def test_cursor_execute_unknown_keyword(offline_connection, cursor_class):
    async with _open_cursor(offline_connection, cursor_class) as cursor:
        previous = MagicMock(is_closed=False)
        if isinstance(cursor, SparkBaseCursor):
            cursor._calculation_id = "previous"
        else:
            cursor._query_id = "previous"
            cursor._result_set = previous
        state = vars(cursor).copy()

        with pytest.raises(
            TypeError,
            match=rf"{cursor_class.__name__}\.execute\(\) got an unexpected keyword "
            "argument 'work_gruop'",
        ):
            await cursor.execute("SELECT 1", work_gruop="typo")

        assert vars(cursor) == state
        previous.close.assert_not_called()


@pytest.mark.parametrize(
    ("cursor_class", "kwargs"),
    [
        (AioArrowCursor, {"block_size": 64, "connect_timeout": 1, "request_timeout": 2}),
        (AioS3FSCursor, {"block_size": 64, "csv_reader": None}),
        (AioPandasCursor, {"chunksize": 10, "parse_dates": []}),
        (AioPolarsCursor, {"chunksize": 10, "separator": "|"}),
    ],
)
async def test_cursor_execute_supported_keywords(offline_connection, cursor_class, kwargs):
    async with _open_cursor(offline_connection, cursor_class) as cursor:
        with (
            patch.object(cursor, "_execute", side_effect=RuntimeError("query started")),
            pytest.raises(RuntimeError, match="query started"),
        ):
            await cursor.execute("SELECT 1", **kwargs)


class TestAioConnection:
    async def test_cursor_arguments_override_cursor_kwargs(self):
        conn = await _connection(
            cursor_class=RecordingAioCursor,
            cursor_kwargs={"kill_on_interrupt": False, "schema_name": "configured"},
        )

        cursor = conn.cursor(kill_on_interrupt=True)

        assert cursor.kwargs["kill_on_interrupt"] is True
        assert cursor.kwargs["schema_name"] == "configured"
        assert conn.cursor_kwargs == {"kill_on_interrupt": False, "schema_name": "configured"}

    @pytest.mark.parametrize("cursor_class", PAGED_CURSORS + UNCAPPED_CURSORS)
    async def test_cursor_arraysize(self, cursor_class):
        conn = await _connection(cursor_kwargs={"arraysize": 25})

        assert conn.cursor(cursor_class).arraysize == 25
        assert conn.cursor(cursor_class, arraysize=50).arraysize == 50

    @pytest.mark.parametrize("cursor_class", PAGED_CURSORS)
    async def test_cursor_arraysize_over_page_size(self, cursor_class):
        conn = await _connection()

        with pytest.raises(ProgrammingError):
            conn.cursor(cursor_class, arraysize=1001)

    @pytest.mark.parametrize("cursor_class", UNCAPPED_CURSORS)
    async def test_cursor_arraysize_uncapped(self, cursor_class):
        conn = await _connection()

        assert conn.cursor(cursor_class, arraysize=1001).arraysize == 1001

    @pytest.mark.parametrize("cursor_class", PAGED_CURSORS + UNCAPPED_CURSORS)
    async def test_cursor_arraysize_not_positive(self, cursor_class):
        conn = await _connection()

        with pytest.raises(ProgrammingError):
            conn.cursor(cursor_class, arraysize=0)

    async def test_internal_cursor_leaves_out_default_cursor_options(self):
        cursor_kwargs = {"unload": True, "chunksize": 10, "schema_name": "configured"}
        conn = await _connection(
            cursor_class=AioPandasCursor,
            cursor_kwargs=cursor_kwargs,
            converter=AioPandasCursor.get_default_converter(True),
        )

        cursor = conn._internal_cursor(AioCursor)

        assert type(cursor) is AioCursor
        assert cursor._schema_name == "configured"
        assert type(cursor._converter) is DefaultTypeConverter
        assert conn.cursor_kwargs == cursor_kwargs
        # Creating the cursor directly still passes the options it does not accept.
        with pytest.raises(TypeError, match="unexpected keyword argument 'unload'"):
            conn.cursor(AioCursor)
