# -*- coding: utf-8 -*-
import contextlib
from io import BytesIO
from pathlib import Path

import boto3
import pytest
import sqlalchemy

from tests import ASYNC_SQLALCHEMY_CONNECTION_STRING, ENV, SQLALCHEMY_CONNECTION_STRING
from tests.pyathena.util import read_query


def pytest_sessionstart(session):
    _upload_rows()
    with contextlib.closing(connect()) as conn, conn.cursor() as cursor:
        _create_database(cursor)
        _create_table(cursor)


def pytest_sessionfinish(session):
    with contextlib.closing(connect()) as conn, conn.cursor() as cursor:
        _drop_database(cursor)
    _delete_rows()


def _upload_rows():
    client = boto3.client("s3")
    rows = Path(__file__).parents[1].resolve() / "resources" / "rows"
    for row in rows.iterdir():
        key = f"{ENV.s3_staging_key}{ENV.schema}/{row.stem}/{row.name}"
        client.upload_file(str(row), ENV.s3_staging_bucket, key)
    client.upload_fileobj(
        BytesIO(b"0123456789"),
        ENV.s3_staging_bucket,
        ENV.s3_filesystem_test_file_key,
    )


def _delete_rows():
    client = boto3.client("s3")
    rows = Path(__file__).parents[1].resolve() / "resources" / "rows"
    for row in rows.iterdir():
        key = f"{ENV.s3_staging_key}{ENV.schema}/{row.stem}/{row.name}"
        client.delete_object(Bucket=ENV.s3_staging_bucket, Key=key)
    client.delete_object(Bucket=ENV.s3_staging_bucket, Key=ENV.s3_filesystem_test_file_key)


def _create_database(cursor):
    for q in read_query("create_database.sql.jinja2", schema=ENV.schema):
        cursor.execute(q)


def _drop_database(cursor):
    for q in read_query("drop_database.sql.jinja2", schema=ENV.schema):
        cursor.execute(q)


def _create_table(cursor):
    for q in read_query(
        "create_table.sql.jinja2", s3_staging_dir=ENV.s3_staging_dir, schema=ENV.schema
    ):
        cursor.execute(q)


def connect(schema_name="default", **kwargs):
    from pyathena import connect

    if "work_group" not in kwargs:
        kwargs["work_group"] = ENV.default_work_group
    return connect(schema_name=schema_name, **kwargs)


def create_engine(**kwargs):
    conn_str = SQLALCHEMY_CONNECTION_STRING
    for arg in [
        "bucket_count",
        "cluster",
        "compression",
        "duration_seconds",
        "file_format",
        "kill_on_interrupt",
        "partition",
        "poll_interval",
        "result_reuse_enable",
        "result_reuse_minutes",
        "row_format",
        "serdeproperties",
        "tblproperties",
        "verify",
    ]:
        if arg in kwargs:
            conn_str += f"&{arg}={{{arg}}}"
    return sqlalchemy.engine.create_engine(
        conn_str.format(
            region_name=ENV.region_name,
            schema_name=ENV.schema,
            s3_staging_dir=ENV.s3_staging_dir,
            location=ENV.s3_staging_dir,
            **kwargs,
        )
    )


def create_async_engine(**kwargs):
    conn_str = ASYNC_SQLALCHEMY_CONNECTION_STRING
    return sqlalchemy.ext.asyncio.create_async_engine(
        conn_str.format(
            region_name=ENV.region_name,
            schema_name=ENV.schema,
            s3_staging_dir=ENV.s3_staging_dir,
            location=ENV.s3_staging_dir,
            **kwargs,
        )
    )


def _cursor(cursor_class, request):
    if not hasattr(request, "param"):
        setattr(request, "param", {})  # noqa: B010
    with (
        contextlib.closing(
            connect(schema_name=ENV.schema, cursor_class=cursor_class, **request.param)
        ) as conn,
        conn.cursor() as cursor,
    ):
        yield cursor


@pytest.fixture
def cursor(request):
    from pyathena.cursor import Cursor

    yield from _cursor(Cursor, request)


@pytest.fixture
def dict_cursor(request):
    from pyathena.cursor import DictCursor

    yield from _cursor(DictCursor, request)


@pytest.fixture
def async_cursor(request):
    from pyathena.async_cursor import AsyncCursor

    yield from _cursor(AsyncCursor, request)


@pytest.fixture
def async_dict_cursor(request):
    from pyathena.async_cursor import AsyncDictCursor

    yield from _cursor(AsyncDictCursor, request)


@pytest.fixture
def pandas_cursor(request):
    from pyathena.pandas.cursor import PandasCursor

    yield from _cursor(PandasCursor, request)


@pytest.fixture
def async_pandas_cursor(request):
    from pyathena.pandas.async_cursor import AsyncPandasCursor

    yield from _cursor(AsyncPandasCursor, request)


@pytest.fixture
def arrow_cursor(request):
    from pyathena.arrow.cursor import ArrowCursor

    yield from _cursor(ArrowCursor, request)


@pytest.fixture
def async_arrow_cursor(request):
    from pyathena.arrow.async_cursor import AsyncArrowCursor

    yield from _cursor(AsyncArrowCursor, request)


@pytest.fixture
def s3fs_cursor(request):
    from pyathena.s3fs.cursor import S3FSCursor

    yield from _cursor(S3FSCursor, request)


@pytest.fixture
def async_s3fs_cursor(request):
    from pyathena.s3fs.async_cursor import AsyncS3FSCursor

    yield from _cursor(AsyncS3FSCursor, request)


@pytest.fixture
def polars_cursor(request):
    from pyathena.polars.cursor import PolarsCursor

    yield from _cursor(PolarsCursor, request)


@pytest.fixture
def async_polars_cursor(request):
    from pyathena.polars.async_cursor import AsyncPolarsCursor

    yield from _cursor(AsyncPolarsCursor, request)


@pytest.fixture
def spark_cursor(request):
    from pyathena.spark.cursor import SparkCursor

    if not hasattr(request, "param"):
        setattr(request, "param", {})  # noqa: B010
    request.param.update({"work_group": ENV.spark_work_group})
    yield from _cursor(SparkCursor, request)


@pytest.fixture
def async_spark_cursor(request):
    from pyathena.spark.async_cursor import AsyncSparkCursor

    if not hasattr(request, "param"):
        setattr(request, "param", {})  # noqa: B010
    request.param.update({"work_group": ENV.spark_work_group})
    yield from _cursor(AsyncSparkCursor, request)


@pytest.fixture
def engine(request):
    if not hasattr(request, "param"):
        setattr(request, "param", {})  # noqa: B010
    engine_ = create_engine(**request.param)
    try:
        with contextlib.closing(engine_.connect()) as conn:
            yield engine_, conn
    finally:
        engine_.dispose()


@pytest.fixture
async def async_engine(request):
    if not hasattr(request, "param"):
        setattr(request, "param", {})  # noqa: B010
    engine_ = create_async_engine(**request.param)
    try:
        async with engine_.connect() as conn:
            yield engine_, conn
    finally:
        await engine_.dispose()


@pytest.fixture
def formatter():
    from pyathena.formatter import DefaultParameterFormatter

    return DefaultParameterFormatter()
