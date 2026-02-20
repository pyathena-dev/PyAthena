# -*- coding: utf-8 -*-
import pytest

from tests import ENV


async def _aio_connect(schema_name="default", **kwargs):
    from pyathena import aconnect

    if "work_group" not in kwargs:
        kwargs["work_group"] = ENV.default_work_group
    return await aconnect(schema_name=schema_name, **kwargs)


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
