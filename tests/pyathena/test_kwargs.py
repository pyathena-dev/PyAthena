# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from inspect import iscoroutinefunction
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pyathena.aio.arrow.cursor import AioArrowCursor
from pyathena.aio.connection import AioConnection
from pyathena.aio.cursor import AioCursor, AioDictCursor
from pyathena.aio.pandas.cursor import AioPandasCursor
from pyathena.aio.polars.cursor import AioPolarsCursor
from pyathena.aio.s3fs.cursor import AioS3FSCursor
from pyathena.aio.spark.cursor import AioSparkCursor
from pyathena.aio.sqlalchemy.base import AsyncAdapt_pyathena_connection
from pyathena.arrow.async_cursor import AsyncArrowCursor
from pyathena.arrow.cursor import ArrowCursor
from pyathena.async_cursor import AsyncCursor, AsyncDictCursor
from pyathena.connection import Connection
from pyathena.cursor import Cursor, DictCursor
from pyathena.error import ProgrammingError
from pyathena.formatter import DefaultParameterFormatter
from pyathena.pandas.async_cursor import AsyncPandasCursor
from pyathena.pandas.cursor import PandasCursor
from pyathena.polars.async_cursor import AsyncPolarsCursor
from pyathena.polars.cursor import PolarsCursor
from pyathena.s3fs.async_cursor import AsyncS3FSCursor
from pyathena.s3fs.cursor import S3FSCursor
from pyathena.spark.async_cursor import AsyncSparkCursor
from pyathena.spark.common import SparkBaseCursor
from pyathena.spark.cursor import SparkCursor
from pyathena.sqlalchemy.base import AthenaDialect
from pyathena.util import RetryConfig

CURSORS = [
    Cursor,
    DictCursor,
    AsyncCursor,
    AsyncDictCursor,
    AioCursor,
    AioDictCursor,
    PandasCursor,
    AsyncPandasCursor,
    AioPandasCursor,
    ArrowCursor,
    AsyncArrowCursor,
    AioArrowCursor,
    PolarsCursor,
    AsyncPolarsCursor,
    AioPolarsCursor,
    S3FSCursor,
    AsyncS3FSCursor,
    AioS3FSCursor,
    SparkCursor,
    AsyncSparkCursor,
    AioSparkCursor,
]


@pytest.fixture
def offline_connection(monkeypatch):
    conn = Connection(
        region_name="us-east-1",
        s3_staging_dir="s3://bucket/path/",
        aws_access_key_id="access_key",
        aws_secret_access_key="secret_key",
    )
    request = MagicMock(side_effect=AssertionError("An offline test made an AWS request"))
    monkeypatch.setattr(conn.client, "_make_api_call", request)
    monkeypatch.setattr(SparkBaseCursor, "_start_session", MagicMock(return_value="session"))
    yield conn
    request.assert_not_called()
    conn.close()


def _constructor_kwargs(conn, cursor_class):
    kwargs = {
        "connection": conn,
        "converter": cursor_class.get_default_converter(),
        "formatter": DefaultParameterFormatter(),
        "retry_config": RetryConfig(),
        "s3_staging_dir": conn.s3_staging_dir,
        "schema_name": conn.schema_name,
        "catalog_name": conn.catalog_name,
        "work_group": conn.work_group,
        "poll_interval": 0,
        "encryption_option": None,
        "kms_key": None,
        "kill_on_interrupt": True,
        "result_reuse_enable": False,
        "result_reuse_minutes": 60,
    }
    if issubclass(cursor_class, SparkBaseCursor):
        kwargs["terminate_session_on_close"] = False
    return kwargs


async def _close(cursor):
    if iscoroutinefunction(cursor.close):
        await cursor.close()
    else:
        cursor.close()


async def _execute(cursor, operation="SELECT 1", **kwargs):
    if iscoroutinefunction(cursor.execute):
        return await cursor.execute(operation, **kwargs)
    return cursor.execute(operation, **kwargs)


@pytest.mark.parametrize("cursor_class", CURSORS)
@pytest.mark.parametrize("source", ["constructor", "cursor", "defaults"])
def test_unknown_constructor_keyword(offline_connection, cursor_class, source):
    if source == "constructor":
        factory = cursor_class
        kwargs = _constructor_kwargs(offline_connection, cursor_class)
    else:
        factory = offline_connection.cursor
        kwargs = {"cursor": cursor_class}
        if source == "defaults":
            offline_connection.cursor_kwargs = {"work_gruop": "typo"}
    if source != "defaults":
        kwargs["work_gruop"] = "typo"
    with pytest.raises(TypeError, match="unexpected keyword argument 'work_gruop'"):
        factory(**kwargs)


@pytest.mark.parametrize("cursor_class", CURSORS)
async def test_shared_constructor_callbacks(offline_connection, cursor_class):
    on_start, on_poll = MagicMock(), MagicMock()
    offline_connection.on_start_query_execution = on_start
    offline_connection.on_poll = on_poll
    kwargs = (
        {"terminate_session_on_close": False} if issubclass(cursor_class, SparkBaseCursor) else {}
    )
    cursor = offline_connection.cursor(cursor_class, **kwargs)
    try:
        assert cursor._on_start_query_execution is on_start
        assert cursor._on_poll is on_poll
    finally:
        await _close(cursor)


@pytest.mark.parametrize("cursor_class", CURSORS)
async def test_unknown_execute_keyword_preserves_state(offline_connection, cursor_class):
    cursor = cursor_class(**_constructor_kwargs(offline_connection, cursor_class))
    cursor._query_id = "previous"
    cursor._result_set = MagicMock(is_closed=False)
    if isinstance(cursor, SparkBaseCursor):
        cursor._calculation_id = "previous-calculation"
        cursor._calculation_execution = MagicMock()
    before = vars(cursor).copy()
    try:
        with pytest.raises(TypeError, match="unexpected keyword argument 'work_gruop'"):
            await _execute(cursor, work_gruop="typo")
        assert vars(cursor) == before
        cursor._result_set.close.assert_not_called()
    finally:
        await _close(cursor)


@pytest.mark.parametrize(
    "cursor_class",
    [
        PandasCursor,
        AsyncPandasCursor,
        AioPandasCursor,
        PolarsCursor,
        AsyncPolarsCursor,
        AioPolarsCursor,
    ],
)
@pytest.mark.parametrize(
    "failure", ["empty", "missing_staging", "invalid_options", "invalid_operation"]
)
@pytest.mark.parametrize("unknown_keyword", [False, True])
async def test_preparation_failure_resets_state_except_unknown_keyword(
    offline_connection, cursor_class, failure, unknown_keyword
):
    cursor = offline_connection.cursor(cursor_class, unload=True)
    previous_result = MagicMock(is_closed=False)
    cursor._query_id = "previous"
    cursor._result_set = previous_result
    operation, options = "SELECT 1", {}
    if failure == "empty":
        operation = " "
    elif failure == "missing_staging":
        cursor._s3_staging_dir = None
    elif failure == "invalid_options":
        options["options"] = object()
    else:
        operation = object()
    if unknown_keyword:
        options["work_gruop"] = "typo"
    before = vars(cursor).copy()
    error = (
        TypeError
        if unknown_keyword
        else (
            AttributeError
            if failure in ("invalid_options", "invalid_operation")
            else ProgrammingError
        )
    )
    message = (
        "unexpected keyword argument 'work_gruop'"
        if unknown_keyword
        else (
            "merge"
            if failure == "invalid_options"
            else (
                "strip"
                if failure == "invalid_operation"
                else "Query is none|s3_staging_dir is required"
            )
        )
    )
    try:
        with pytest.raises(error, match=message):
            await _execute(cursor, operation=operation, **options)
        if unknown_keyword or cursor_class.__name__.startswith("Async"):
            assert vars(cursor) == before
            previous_result.close.assert_not_called()
        else:
            assert cursor.query_id is None
            assert cursor.result_set is None
            previous_result.close.assert_called_once()
    finally:
        await _close(cursor)


@pytest.mark.parametrize(
    ("cursor_class", "settings", "options"),
    [(cls, {}, {"parse_dates": []}) for cls in (PandasCursor, AsyncPandasCursor, AioPandasCursor)]
    + [
        (cls, {"unload": True}, {"use_threads": False})
        for cls in (PandasCursor, AsyncPandasCursor, AioPandasCursor)
    ]
    + [(cls, {}, {"separator": "|"}) for cls in (PolarsCursor, AsyncPolarsCursor, AioPolarsCursor)]
    + [
        (cls, {"unload": True}, {"parallel": "none"})
        for cls in (PolarsCursor, AsyncPolarsCursor, AioPolarsCursor)
    ]
    + [
        (cls, {}, {"block_size": 64, "connect_timeout": None, "request_timeout": 2})
        for cls in (ArrowCursor, AsyncArrowCursor, AioArrowCursor)
    ]
    + [(cls, {}, {"block_size": 64}) for cls in (S3FSCursor, AsyncS3FSCursor, AioS3FSCursor)],
)
async def test_supported_execute_keywords(offline_connection, cursor_class, settings, options):
    cursor = offline_connection.cursor(cursor_class, **settings)
    try:
        with (
            patch.object(cursor, "_execute", side_effect=RuntimeError("execution reached")),
            pytest.raises(RuntimeError, match="execution reached"),
        ):
            await _execute(cursor, **options)
    finally:
        await _close(cursor)


@pytest.mark.parametrize(
    "cursor_class",
    [
        PandasCursor,
        AsyncPandasCursor,
        AioPandasCursor,
        PolarsCursor,
        AsyncPolarsCursor,
        AioPolarsCursor,
    ],
)
@pytest.mark.parametrize("unload", [False, True])
async def test_keyword_rejected_for_other_reader_mode(offline_connection, cursor_class, unload):
    cursor = offline_connection.cursor(cursor_class, unload=unload)
    if "Pandas" in cursor_class.__name__:
        key = "sep" if unload else "use_threads"
    else:
        key = "separator" if unload else "parallel"
    try:
        with pytest.raises(TypeError, match=f"unexpected keyword argument '{key}'"):
            await _execute(cursor, **{key: "|"})
    finally:
        await _close(cursor)


@pytest.mark.parametrize(
    "cursor_class",
    [
        PandasCursor,
        AsyncPandasCursor,
        AioPandasCursor,
        PolarsCursor,
        AsyncPolarsCursor,
        AioPolarsCursor,
    ],
)
async def test_reader_input_cannot_replace_query_results(offline_connection, cursor_class):
    cursor = offline_connection.cursor(cursor_class)
    key = "filepath_or_buffer" if "Pandas" in cursor_class.__name__ else "source"
    try:
        with pytest.raises(TypeError, match=f"unexpected keyword argument '{key}'"):
            await _execute(cursor, **{key: "other.csv"})
    finally:
        await _close(cursor)


@pytest.mark.parametrize("cursor_class", [PolarsCursor, AsyncPolarsCursor, AioPolarsCursor])
@pytest.mark.parametrize("chunksize", [None, 10])
async def test_polars_selects_effective_chunk_reader(offline_connection, cursor_class, chunksize):
    cursor = offline_connection.cursor(cursor_class, chunksize=10)
    try:
        with patch.object(cursor, "_execute", side_effect=RuntimeError("execution reached")):
            error = TypeError if chunksize is None else RuntimeError
            message = "include_file_paths" if chunksize is None else "execution reached"
            with pytest.raises(error, match=message):
                await _execute(cursor, chunksize=chunksize, include_file_paths="path")
    finally:
        await _close(cursor)


@pytest.mark.parametrize(
    ("cursor_class", "options"),
    [(cls, {"nrows": 10}) for cls in (PandasCursor, AsyncPandasCursor, AioPandasCursor)]
    + [
        (cls, {"separator": "\t", "null_values": "NULL"})
        for cls in (PolarsCursor, AsyncPolarsCursor, AioPolarsCursor)
    ],
)
async def test_unload_non_select_uses_csv_keywords(offline_connection, cursor_class, options):
    cursor = offline_connection.cursor(cursor_class, unload=True)
    try:
        with (
            patch.object(cursor, "_execute", side_effect=RuntimeError("execution reached")),
            pytest.raises(RuntimeError, match="execution reached"),
        ):
            await _execute(cursor, operation="DESCRIBE t", **options)
    finally:
        await _close(cursor)


@pytest.mark.parametrize("cursor_class", [PolarsCursor, AsyncPolarsCursor, AioPolarsCursor])
async def test_polars_chunked_csv_allows_managed_reader_options(offline_connection, cursor_class):
    cursor = offline_connection.cursor(cursor_class, chunksize=10)
    try:
        with (
            patch.object(cursor, "_execute", side_effect=RuntimeError("execution reached")),
            pytest.raises(RuntimeError, match="execution reached"),
        ):
            await _execute(cursor, columns=["a"], n_threads=1, use_pyarrow=False, batch_size=1024)
    finally:
        await _close(cursor)


@pytest.mark.parametrize("cursor_class", [PolarsCursor, AsyncPolarsCursor, AioPolarsCursor])
@pytest.mark.parametrize("unload", [False, True])
async def test_polars_accepts_reader_aliases(offline_connection, cursor_class, unload):
    cursor = offline_connection.cursor(cursor_class, unload=unload)
    try:
        with (
            patch.object(cursor, "_execute", side_effect=RuntimeError("execution reached")),
            pytest.raises(RuntimeError, match="execution reached"),
        ):
            await _execute(cursor, row_count_name="row_number", row_count_offset=2)
    finally:
        await _close(cursor)


@pytest.mark.parametrize(
    "cursor_class",
    [PandasCursor, ArrowCursor, PolarsCursor, AioPandasCursor, AioArrowCursor, AioPolarsCursor],
)
async def test_internal_cursor_isolates_backend_defaults(cursor_class):
    is_async = cursor_class.__name__.startswith("Aio")
    cls = AioConnection if is_async else Connection
    defaults = {"unload": True, "schema_name": "configured", "arraysize": 25}
    conn = cls(
        region_name="us-east-1",
        s3_staging_dir="s3://bucket/path/",
        aws_access_key_id="access_key",
        aws_secret_access_key="secret_key",
        cursor_class=cursor_class,
        cursor_kwargs=defaults,
        converter=cursor_class.get_default_converter(True),
    )
    adapted = AsyncAdapt_pyathena_connection(MagicMock(), conn) if is_async else conn
    raw_connection = SimpleNamespace(driver_connection=adapted)
    cursor = None
    try:
        internal = AthenaDialect._internal_cursor(raw_connection)
        cursor = internal._cursor if is_async else internal
        assert type(cursor) is (AioCursor if is_async else Cursor)
        assert cursor._schema_name == "configured"
        assert cursor.arraysize == 25
        assert type(cursor._converter) is type(Cursor.get_default_converter())
        assert conn.cursor_kwargs == defaults
        # Public creation still rejects settings unsupported by the requested class.
        with pytest.raises(TypeError, match="unexpected keyword argument 'unload'"):
            conn.cursor(AioCursor if is_async else Cursor)
    finally:
        if cursor is not None:
            await _close(cursor)
        conn.close()


def test_internal_cursor_does_not_hide_unknown_defaults(offline_connection):
    offline_connection.cursor_class = PandasCursor
    offline_connection.cursor_kwargs = {"unload": True, "work_gruop": "typo"}
    with pytest.raises(TypeError, match="unexpected keyword argument 'work_gruop'"):
        offline_connection._internal_cursor(Cursor)
