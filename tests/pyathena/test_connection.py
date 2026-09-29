# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from typing import Any

import pytest

from pyathena.arrow.async_cursor import AsyncArrowCursor
from pyathena.arrow.cursor import ArrowCursor
from pyathena.async_cursor import AsyncCursor, AsyncDictCursor
from pyathena.connection import Connection
from pyathena.converter import DefaultTypeConverter
from pyathena.cursor import Cursor, DictCursor
from pyathena.error import ProgrammingError
from pyathena.pandas.async_cursor import AsyncPandasCursor
from pyathena.pandas.cursor import PandasCursor
from pyathena.polars.async_cursor import AsyncPolarsCursor
from pyathena.polars.cursor import PolarsCursor
from pyathena.s3fs.async_cursor import AsyncS3FSCursor
from pyathena.s3fs.cursor import S3FSCursor
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
