import contextlib
import functools
import sys
import uuid

import boto3
import pytest
import sqlalchemy
from botocore.exceptions import ClientError
from sqlalchemy.ext.asyncio import create_async_engine as _create_async_engine

from tests import ASYNC_SQLALCHEMY_CONNECTION_STRING, ENV, SQLALCHEMY_CONNECTION_STRING
from tests.pyathena.tables import TABLES, VIEWS, spark_group_by_csv
from tests.pyathena.util import read_query

# The key of ENV.fixture_schema in a pytest-xdist worker's workerinput.
_FIXTURE_SCHEMA_KEY = "pyathena_fixture_schema"


class _XDistHooks:
    """pytest-xdist hooks, registered only when the plugin is present."""

    def pytest_configure_node(self, node):
        """Pass the fixture schema's name to a pytest-xdist worker.

        Args:
            node: The controller's handle of the worker.
        """
        node.workerinput[_FIXTURE_SCHEMA_KEY] = ENV.fixture_schema


def pytest_configure(config):
    """Share one fixture schema between the pytest-xdist controller and its workers.

    A worker takes the name the controller passed; pytest-xdist sets
    ``workerinput`` before it configures the worker.

    Args:
        config: The pytest config.
    """
    workerinput = getattr(config, "workerinput", None)
    if workerinput is not None:
        ENV.fixture_schema = workerinput[_FIXTURE_SCHEMA_KEY]
    elif config.pluginmanager.hasplugin("xdist"):
        config.pluginmanager.register(_XDistHooks())


# The removals of what pytest_sessionstart created, in creation order.
_cleanups = []


def pytest_sessionstart(session):
    """Create the fixture schema and this process's own schema, as its role requires.

    The pytest-xdist controller creates the fixture schema before it starts the
    workers, and each worker creates its own schema. A run without workers
    creates both. Each removal is recorded before its step, and
    ``pytest_sessionfinish`` runs them. pytest skips ``pytest_sessionfinish``
    after a failed session start, including a failure to start the workers, so
    a config cleanup runs whatever is still recorded then.

    Args:
        session: The pytest session.
    """
    config = session.config
    config.add_cleanup(_run_cleanups)
    if _owns_fixture_schema(config):
        _cleanups.append(_drop_fixture_schema)
        _create_fixture_schema()
    if _is_test_process(config):
        _cleanups.append(_delete_s3tables_namespace)
        _create_s3tables_namespace()
        _cleanups.append(functools.partial(_drop_database, ENV.schema))
        with contextlib.closing(connect()) as conn, conn.cursor() as cursor:
            _create_database(cursor, ENV.schema)


def pytest_sessionfinish(session):
    """Check the fixture schema in the process that owns it, then remove what was created.

    The removal runs here, not in a config cleanup, because a pytest-xdist worker
    reports that it finished after this hook and the controller may then stop
    it.

    Args:
        session: The pytest session.
    """
    try:
        if _owns_fixture_schema(session.config):
            _check_fixture_schema(session)
    finally:
        _run_cleanups()


def _run_cleanups():
    """Run and forget the recorded removals, newest first, each even if one fails."""
    steps = list(reversed(_cleanups))
    _cleanups.clear()
    _run_all(steps)


def _is_test_process(config):
    """Whether this process runs tests, rather than only controlling xdist workers.

    Args:
        config: The pytest config.

    Returns:
        False for the pytest-xdist controller, True for a worker or a run without
        workers.
    """
    return hasattr(config, "workerinput") or not getattr(config.option, "numprocesses", None)


def _owns_fixture_schema(config):
    """Whether this process creates and drops the fixture schema.

    Args:
        config: The pytest config.

    Returns:
        True for the pytest-xdist controller or a run without workers, False for
        a worker, which uses the controller's fixture schema.
    """
    return not hasattr(config, "workerinput")


def _run_all(steps):
    """Run every step in order, even after one fails, then raise the last failure.

    Args:
        steps: Callables that take no arguments.

    Raises:
        BaseException: The last exception a step raised, with the earlier ones
            as its context.
    """
    with contextlib.ExitStack() as stack:
        for step in reversed(list(steps)):
            stack.callback(step)


def _create_fixture_schema():
    """Upload the data files and create the fixture schema with its tables and views."""
    _upload_data()
    with contextlib.closing(connect()) as conn, conn.cursor() as cursor:
        _create_database(cursor, ENV.fixture_schema)
        _create_tables(cursor)


def _drop_fixture_schema():
    """Drop the fixture schema and delete the uploaded data files."""
    _run_all([functools.partial(_drop_database, ENV.fixture_schema), _delete_data])


def _check_fixture_schema(session):
    """Fail the run if the fixture schema holds other tables than those it was created with.

    A missing fixture schema counts as holding no tables. Another failure to
    list the tables is reported but does not fail the run.

    Args:
        session: The pytest session, whose exit status is set to failed.
    """
    expected = {t.name for t in TABLES} | {v.name for v in VIEWS}
    try:
        actual = {
            table["Name"]
            for page in boto3.client("glue")
            .get_paginator("get_tables")
            .paginate(DatabaseName=ENV.fixture_schema)
            for table in page["TableList"]
        }
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            sys.stderr.write(
                f"\nCould not list the tables of fixture schema {ENV.fixture_schema}: {e!r}\n"
            )
            return
        actual = set()
    except Exception as e:
        sys.stderr.write(
            f"\nCould not list the tables of fixture schema {ENV.fixture_schema}: {e!r}\n"
        )
        return
    if actual != expected:
        sys.stderr.write(
            f"\nFixture schema {ENV.fixture_schema} changed during the run; "
            f"unexpected: {sorted(actual - expected)}, missing: {sorted(expected - actual)}. "
            "Tests must create their objects in ENV.schema.\n"
        )
        if session.exitstatus == pytest.ExitCode.OK:
            session.exitstatus = pytest.ExitCode.TESTS_FAILED


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
    """Delete this process's S3 Tables namespace and any table left in it, if it exists."""
    if not ENV.s3tables_catalog:
        return
    client, arn = _s3tables()
    try:
        client.get_namespace(tableBucketARN=arn, namespace=ENV.s3tables_namespace)
    except client.exceptions.NotFoundException:
        return
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


@functools.cache
def _data_objects():
    """Return the S3 objects of the fixture schema: the table data files and test files.

    Returns:
        A dict from S3 key to object content.
    """
    prefix = f"{ENV.s3_staging_key}{ENV.fixture_schema}"
    objects = {
        ENV.s3_filesystem_test_file_key: b"0123456789",
        f"{prefix}/spark_group_by/spark_group_by.csv": spark_group_by_csv(),
    }
    for table in TABLES:
        if data_file := table.data_file():
            name, content = data_file
            objects[f"{prefix}/{table.name}/{name}"] = content
    return objects


def _upload_data():
    """Upload the objects from ``_data_objects``."""
    client = boto3.client("s3")
    for key, content in _data_objects().items():
        client.put_object(Bucket=ENV.s3_staging_bucket, Key=key, Body=content)


def _delete_data():
    """Delete the objects from ``_data_objects``."""
    client = boto3.client("s3")
    for key in _data_objects():
        client.delete_object(Bucket=ENV.s3_staging_bucket, Key=key)


def _create_database(cursor, schema):
    """Create a database.

    Args:
        cursor: The cursor to run the statement with.
        schema: The database name.
    """
    for q in read_query("create_database.sql.jinja2", schema=schema):
        cursor.execute(q)


def _drop_database(schema):
    """Drop a database and its tables.

    Args:
        schema: The database name.
    """
    with contextlib.closing(connect()) as conn, conn.cursor() as cursor:
        for q in read_query("drop_database.sql.jinja2", schema=schema):
            cursor.execute(q)


def _create_tables(cursor):
    """Create the tables and views from ``tests.pyathena.tables`` in the fixture schema.

    Args:
        cursor: The cursor to run the statements with.
    """
    for table in TABLES:
        location = f"{ENV.s3_staging_dir}{ENV.fixture_schema}/{table.name}/"
        cursor.execute(table.create_statement(ENV.fixture_schema, location))
    for view in VIEWS:
        cursor.execute(view.create_statement(ENV.fixture_schema))


def connect(schema_name="default", **kwargs):
    from pyathena import connect

    if "work_group" not in kwargs:
        kwargs["work_group"] = ENV.default_work_group
    return connect(schema_name=schema_name, **kwargs)


def create_engine(**kwargs):
    """Create a SQLAlchemy engine whose default schema is the fixture schema.

    Args:
        **kwargs: ``driver`` and the connection options to add to the URL.

    Returns:
        The engine.
    """
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
            schema_name=ENV.fixture_schema,
            s3_staging_dir=ENV.s3_staging_dir,
            location=ENV.s3_staging_dir,
            **kwargs,
        )
    )


def create_async_engine(**kwargs):
    """Create an async SQLAlchemy engine whose default schema is the fixture schema.

    Args:
        **kwargs: ``driver`` and the connection options to add to the URL.

    Returns:
        The engine.
    """
    driver = kwargs.pop("driver", "aiorest")
    conn_str = ASYNC_SQLALCHEMY_CONNECTION_STRING.replace("+aiorest", f"+{driver}")
    if "unload" in kwargs:
        conn_str += "&unload={unload}"
    return _create_async_engine(
        conn_str.format(
            region_name=ENV.region_name,
            schema_name=ENV.fixture_schema,
            s3_staging_dir=ENV.s3_staging_dir,
            location=ENV.s3_staging_dir,
            **kwargs,
        )
    )


def _cursor(cursor_class, request):
    """Yield a cursor whose default schema is the fixture schema.

    Args:
        cursor_class: The cursor class.
        request: The fixture request; its optional ``param`` holds connection options.

    Yields:
        The cursor.
    """
    if not hasattr(request, "param"):
        request.param = {}
    with (
        contextlib.closing(
            connect(schema_name=ENV.fixture_schema, cursor_class=cursor_class, **request.param)
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
def empty_table():
    """Create an empty ``(a INT, b STRING)`` text table for one test and drop it on teardown.

    The table has its own connection, so a test with any cursor type, including
    the aio cursors, can write to it.

    Yields:
        The table name qualified with ``ENV.schema``.
    """
    table_name = f"empty_{uuid.uuid4().hex}"
    table = f"{ENV.schema}.{table_name}"
    with contextlib.closing(connect(schema_name=ENV.schema)) as conn, conn.cursor() as cursor:
        try:
            cursor.execute(
                f"""
                CREATE EXTERNAL TABLE {table} (a INT, b STRING)
                ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'
                STORED AS TEXTFILE
                LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table_name}/'
                """
            )
            yield table
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")


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
