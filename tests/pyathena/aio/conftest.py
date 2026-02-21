# -*- coding: utf-8 -*-
import pytest

from tests import ENV


async def _aio_connect(schema_name="default", **kwargs):
    from pyathena import aio_connect

    if "work_group" not in kwargs:
        kwargs["work_group"] = ENV.default_work_group
    return await aio_connect(schema_name=schema_name, **kwargs)


@pytest.fixture
async def aio_cursor(request):
    from pyathena.aio.cursor import AioCursor

    if not hasattr(request, "param"):
        setattr(request, "param", {})  # noqa: B010
    conn = await _aio_connect(schema_name=ENV.schema, cursor_class=AioCursor, **request.param)
    try:
        async with conn.cursor() as cursor:
            yield cursor
    finally:
        conn.close()


@pytest.fixture
async def aio_dict_cursor(request):
    from pyathena.aio.cursor import AioDictCursor

    if not hasattr(request, "param"):
        setattr(request, "param", {})  # noqa: B010
    conn = await _aio_connect(schema_name=ENV.schema, cursor_class=AioDictCursor, **request.param)
    try:
        async with conn.cursor() as cursor:
            yield cursor
    finally:
        conn.close()


@pytest.fixture
async def aio_pandas_cursor(request):
    from pyathena.aio.pandas.cursor import AioPandasCursor

    if not hasattr(request, "param"):
        setattr(request, "param", {})  # noqa: B010
    conn = await _aio_connect(schema_name=ENV.schema, cursor_class=AioPandasCursor, **request.param)
    try:
        async with conn.cursor() as cursor:
            yield cursor
    finally:
        conn.close()


@pytest.fixture
async def aio_arrow_cursor(request):
    from pyathena.aio.arrow.cursor import AioArrowCursor

    if not hasattr(request, "param"):
        setattr(request, "param", {})  # noqa: B010
    conn = await _aio_connect(schema_name=ENV.schema, cursor_class=AioArrowCursor, **request.param)
    try:
        async with conn.cursor() as cursor:
            yield cursor
    finally:
        conn.close()


@pytest.fixture
async def aio_polars_cursor(request):
    from pyathena.aio.polars.cursor import AioPolarsCursor

    if not hasattr(request, "param"):
        setattr(request, "param", {})  # noqa: B010
    conn = await _aio_connect(schema_name=ENV.schema, cursor_class=AioPolarsCursor, **request.param)
    try:
        async with conn.cursor() as cursor:
            yield cursor
    finally:
        conn.close()


@pytest.fixture
async def aio_s3fs_cursor(request):
    from pyathena.aio.s3fs.cursor import AioS3FSCursor

    if not hasattr(request, "param"):
        setattr(request, "param", {})  # noqa: B010
    conn = await _aio_connect(schema_name=ENV.schema, cursor_class=AioS3FSCursor, **request.param)
    try:
        async with conn.cursor() as cursor:
            yield cursor
    finally:
        conn.close()


@pytest.fixture
async def aio_spark_cursor(request):
    import asyncio

    from pyathena.aio.spark.cursor import AioSparkCursor

    if not hasattr(request, "param"):
        setattr(request, "param", {})  # noqa: B010
    conn = await _aio_connect(
        schema_name=ENV.schema,
        cursor_class=AioSparkCursor,
        work_group=ENV.spark_work_group,
        **request.param,
    )
    cursor = await asyncio.to_thread(conn.cursor)
    try:
        yield cursor
    finally:
        await cursor.close()
