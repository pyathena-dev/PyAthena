# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from inspect import Parameter, signature
from typing import Any
from unittest.mock import patch

import pytest

import pyathena
from pyathena.aio.arrow.cursor import AioArrowCursor
from pyathena.aio.connection import AioConnection
from pyathena.aio.cursor import AioCursor, AioDictCursor
from pyathena.aio.pandas.cursor import AioPandasCursor
from pyathena.aio.polars.cursor import AioPolarsCursor
from pyathena.aio.s3fs.cursor import AioS3FSCursor
from pyathena.aio.spark.cursor import AioSparkCursor
from pyathena.error import ProgrammingError

# Cursors whose arraysize is the GetQueryResults page size, capped at 1000.
PAGED_CURSORS = [AioCursor, AioDictCursor]
# Cursors whose arraysize only sets the fetchmany() batch, with no upper limit.
UNCAPPED_CURSORS = [AioArrowCursor, AioPandasCursor, AioPolarsCursor, AioS3FSCursor]
CURSOR_CLASSES = PAGED_CURSORS + UNCAPPED_CURSORS + [AioSparkCursor]


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
