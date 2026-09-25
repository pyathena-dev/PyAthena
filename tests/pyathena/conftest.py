import contextlib
import functools
import uuid
from io import BytesIO
from pathlib import Path

import boto3
import pytest
import sqlalchemy
from sqlalchemy.ext.asyncio import create_async_engine as _create_async_engine

from tests import ASYNC_SQLALCHEMY_CONNECTION_STRING, ENV, SQLALCHEMY_CONNECTION_STRING
from tests.pyathena.util import read_query


def pytest_sessionstart(session):
    # pytest skips pytest_sessionfinish after a failed pytest_sessionstart, so
    # a failure after the namespace is created deletes it here.
    is_test_process = _is_test_process(session.config)
    if is_test_process:
        _create_s3tables_namespace()
    try:
        _upload_rows()
        with contextlib.closing(connect()) as conn, conn.cursor() as cursor:
            _create_database(cursor)
            _create_table(cursor)
    except BaseException:
        if is_test_process:
            _delete_s3tables_namespace()
        raise


def pytest_sessionfinish(session):
    # Each cleanup step runs even if an earlier one fails.
    try:
        with contextlib.closing(connect()) as conn, conn.cursor() as cursor:
            _drop_database(cursor)
    finally:
        try:
            _delete_rows()
        finally:
            if _is_test_process(session.config):
                _delete_s3tables_namespace()


def _is_test_process(config):
    """Whether this process runs tests, rather than only controlling xdist workers.

    Args:
        config: The pytest config.

    Returns:
        False for the pytest-xdist controller, True for a worker or a run without
        workers.
    """
    return hasattr(config, "workerinput") or not getattr(config.option, "numprocesses", None)


@functools.cache
def _s3tables():
    """Return an S3 Tables client and the ARN of ``ENV.s3tables_catalog``'s table bucket.

    The ARN uses the client's region, so the two always agree.

    Returns:
        The client and the table bucket's ARN.

    Raises:
        ValueError: If ``AWS_ATHENA_S3_TABLES_CATALOG`` is not
            ``s3tablescatalog/<table-bucket>``.
    """
    prefix, _, bucket = ENV.s3tables_catalog.partition("/")
    if prefix != "s3tablescatalog" or not bucket:
        raise ValueError(
            "AWS_ATHENA_S3_TABLES_CATALOG must be s3tablescatalog/<table-bucket>, "
            f"not {ENV.s3tables_catalog!r}."
        )
    client = boto3.client("s3tables")
    account = boto3.client("sts").get_caller_identity()["Account"]
    region = client.meta.region_name
    return client, f"arn:aws:s3tables:{region}:{account}:bucket/{bucket}"


def _create_s3tables_namespace():
    """Create this process's S3 Tables namespace when S3 Tables are configured."""
    if not ENV.s3tables_catalog:
        return
    client, arn = _s3tables()
    client.create_namespace(tableBucketARN=arn, namespace=[ENV.s3tables_namespace])


def _delete_s3tables_namespace():
    """Delete this process's S3 Tables namespace and any table left in it."""
    if not ENV.s3tables_catalog:
        return
    client, arn = _s3tables()
    tables = [
        table["name"]
        for page in client.get_paginator("list_tables").paginate(
            tableBucketARN=arn, namespace=ENV.s3tables_namespace
        )
        for table in page["tables"]
    ]
    for table in tables:
        client.delete_table(tableBucketARN=arn, namespace=ENV.s3tables_namespace, name=table)
    client.delete_namespace(tableBucketARN=arn, namespace=ENV.s3tables_namespace)


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
    driver = kwargs.pop("driver", "rest")
    conn_str = SQLALCHEMY_CONNECTION_STRING.replace("+rest", f"+{driver}")
    for arg in [
        "bucket_count",
        "catalog_name",
        "cluster",
        "compression",
        "duration_seconds",
        "file_format",
        "glue_metadata_fallback",
        "kill_on_interrupt",
        "partition",
        "poll_interval",
        "result_reuse_enable",
        "result_reuse_minutes",
        "row_format",
        "serdeproperties",
        "tblproperties",
        "unload",
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
    driver = kwargs.pop("driver", "aiorest")
    conn_str = ASYNC_SQLALCHEMY_CONNECTION_STRING.replace("+aiorest", f"+{driver}")
    if "unload" in kwargs:
        conn_str += "&unload={unload}"
    return _create_async_engine(
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
        request.param = {}
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
def executemany_table(cursor):
    """Isolate mutable DML data for each test and remove it on teardown.

    Groups 1, 2, and 99 match two, one, and zero rows respectively.
    Use a separate cursor so setup and teardown do not change the cursor under test.
    """
    table_name = f"executemany_{uuid.uuid4().hex}"
    table = f"{ENV.schema}.{table_name}"
    with cursor.connection.cursor() as table_cursor:
        try:
            table_cursor.execute(
                f"""
                CREATE TABLE {table} (id INT, group_id INT, value INT)
                LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table_name}/'
                TBLPROPERTIES ('table_type'='ICEBERG')
                """
            )
            table_cursor.execute(f"INSERT INTO {table} VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)")
            yield table
        finally:
            table_cursor.execute(f"DROP TABLE IF EXISTS {table}")


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
        request.param = {}
    request.param.update({"work_group": ENV.spark_work_group})
    yield from _cursor(SparkCursor, request)


@pytest.fixture
def async_spark_cursor(request):
    from pyathena.spark.async_cursor import AsyncSparkCursor

    if not hasattr(request, "param"):
        request.param = {}
    request.param.update({"work_group": ENV.spark_work_group})
    yield from _cursor(AsyncSparkCursor, request)


@pytest.fixture
def engine(request):
    if not hasattr(request, "param"):
        request.param = {}
    engine_ = create_engine(**request.param)
    try:
        with contextlib.closing(engine_.connect()) as conn:
            yield engine_, conn
    finally:
        engine_.dispose()


@pytest.fixture
async def async_engine(request):
    if not hasattr(request, "param"):
        request.param = {}
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
