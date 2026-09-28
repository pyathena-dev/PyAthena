# Copyright 2022 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import pytest

from tests import ENV


async def _aio_connect(schema_name="default", **kwargs):
    from pyathena import aio_connect

    if "work_group" not in kwargs:
        kwargs["work_group"] = ENV.default_work_group
    return await aio_connect(schema_name=schema_name, **kwargs)


@pytest.fixture
async def aio_cursor(request):
    """Yield an ``AioCursor`` whose default schema is the fixture schema.

    Args:
        request: The fixture request; its optional ``param`` holds connection options.

    Yields:
        The cursor.
    """
    from pyathena.aio.cursor import AioCursor

    if not hasattr(request, "param"):
        request.param = {}
    conn = await _aio_connect(
        schema_name=ENV.fixture_schema, cursor_class=AioCursor, **request.param
    )
    try:
        async with conn.cursor() as cursor:
            yield cursor
    finally:
        conn.close()


@pytest.fixture
async def aio_dict_cursor(request):
    """Yield an ``AioDictCursor`` whose default schema is the fixture schema.

    Args:
        request: The fixture request; its optional ``param`` holds connection options.

    Yields:
        The cursor.
    """
    from pyathena.aio.cursor import AioDictCursor

    if not hasattr(request, "param"):
        request.param = {}
    conn = await _aio_connect(
        schema_name=ENV.fixture_schema, cursor_class=AioDictCursor, **request.param
    )
    try:
        async with conn.cursor() as cursor:
            yield cursor
    finally:
        conn.close()


@pytest.fixture
async def aio_pandas_cursor(request):
    """Yield an ``AioPandasCursor`` whose default schema is the fixture schema.

    Args:
        request: The fixture request; its optional ``param`` holds connection options.

    Yields:
        The cursor.
    """
    from pyathena.aio.pandas.cursor import AioPandasCursor

    if not hasattr(request, "param"):
        request.param = {}
    conn = await _aio_connect(
        schema_name=ENV.fixture_schema, cursor_class=AioPandasCursor, **request.param
    )
    try:
        async with conn.cursor() as cursor:
            yield cursor
    finally:
        conn.close()


@pytest.fixture
async def aio_arrow_cursor(request):
    """Yield an ``AioArrowCursor`` whose default schema is the fixture schema.

    Args:
        request: The fixture request; its optional ``param`` holds connection options.

    Yields:
        The cursor.
    """
    from pyathena.aio.arrow.cursor import AioArrowCursor

    if not hasattr(request, "param"):
        request.param = {}
    conn = await _aio_connect(
        schema_name=ENV.fixture_schema, cursor_class=AioArrowCursor, **request.param
    )
    try:
        async with conn.cursor() as cursor:
            yield cursor
    finally:
        conn.close()


@pytest.fixture
async def aio_polars_cursor(request):
    """Yield an ``AioPolarsCursor`` whose default schema is the fixture schema.

    Args:
        request: The fixture request; its optional ``param`` holds connection options.

    Yields:
        The cursor.
    """
    from pyathena.aio.polars.cursor import AioPolarsCursor

    if not hasattr(request, "param"):
        request.param = {}
    conn = await _aio_connect(
        schema_name=ENV.fixture_schema, cursor_class=AioPolarsCursor, **request.param
    )
    try:
        async with conn.cursor() as cursor:
            yield cursor
    finally:
        conn.close()


@pytest.fixture
async def aio_s3fs_cursor(request):
    """Yield an ``AioS3FSCursor`` whose default schema is the fixture schema.

    Args:
        request: The fixture request; its optional ``param`` holds connection options.

    Yields:
        The cursor.
    """
    from pyathena.aio.s3fs.cursor import AioS3FSCursor

    if not hasattr(request, "param"):
        request.param = {}
    conn = await _aio_connect(
        schema_name=ENV.fixture_schema, cursor_class=AioS3FSCursor, **request.param
    )
    try:
        async with conn.cursor() as cursor:
            yield cursor
    finally:
        conn.close()


@pytest.fixture
async def aio_spark_cursor(request):
    """Yield an ``AioSparkCursor`` whose default schema is the fixture schema.

    Args:
        request: The fixture request; its optional ``param`` holds connection options.

    Yields:
        The cursor.
    """
    import asyncio

    from pyathena.aio.spark.cursor import AioSparkCursor

    if not hasattr(request, "param"):
        request.param = {}
    conn = await _aio_connect(
        schema_name=ENV.fixture_schema,
        cursor_class=AioSparkCursor,
        work_group=ENV.spark_work_group,
        **request.param,
    )
    cursor = await asyncio.to_thread(conn.cursor)
    try:
        yield cursor
    finally:
        await cursor.close()
