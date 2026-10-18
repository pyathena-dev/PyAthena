import time
from datetime import datetime
from random import randint
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pyathena.error import NotSupportedError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.pandas.async_cursor import AsyncPandasCursor
from pyathena.result_set import AthenaResultSet
from pyathena.util import RetryConfig
from tests import ENV
from tests.pyathena.conftest import connect


class TestAsyncPandasCursor:
    def test_binary_null_vs_empty(self, async_pandas_cursor):
        query = """SELECT * FROM (VALUES
                    (1, CAST(NULL AS VARBINARY), 'null', CAST(NULL AS VARCHAR)),
                    (2, X'', 'empty', ''),
                    (3, X'00ff275c25', 'comma, quote" and' || chr(10) || 'newline', 'NULL')
                ) AS t(id, value, label, text_value) ORDER BY id"""
        _, future = async_pandas_cursor.execute(query, chunksize=2)
        result = future.result()
        rows = result.fetchall()
        assert [row[:3] for row in rows] == [
            (1, None, "null"),
            (2, b"", "empty"),
            (3, b"\x00\xff'\\%", 'comma, quote" and\nnewline'),
        ]

    @pytest.mark.parametrize(
        ("async_pandas_cursor", "chunksize"),
        [
            ({}, None),
            ({}, 1_000),
            ({}, 1_000_000),
        ],
        indirect=["async_pandas_cursor"],
    )
    def test_get_chunk(self, async_pandas_cursor, chunksize):
        query_id, future = async_pandas_cursor.execute(
            "SELECT * FROM many_rows LIMIT 15", chunksize=chunksize
        )
        df = future.result().as_pandas()
        if chunksize:
            assert len(df.get_chunk(10)) == 10
            assert len(df.get_chunk(10)) == 5
            pytest.raises(StopIteration, lambda: df.get_chunk(10))
        else:
            assert len(df) == 15

    @pytest.mark.parametrize(
        ("async_pandas_cursor", "parquet_engine", "chunksize"),
        [
            ({"cursor_kwargs": {"unload": False}}, "auto", None),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000),
            ({"cursor_kwargs": {"unload": True}}, "auto", None),
        ],
        indirect=["async_pandas_cursor"],
    )
    def test_arraysize(self, async_pandas_cursor, parquet_engine, chunksize):
        async_pandas_cursor.arraysize = 5
        query_id, future = async_pandas_cursor.execute(
            "SELECT * FROM many_rows LIMIT 20",
            engine=parquet_engine,
            chunksize=chunksize,
        )
        result_set = future.result()
        assert len(result_set.fetchmany()) == 5

    def test_arraysize_default(self, async_pandas_cursor):
        assert async_pandas_cursor.arraysize == AthenaResultSet.DEFAULT_FETCH_SIZE

    def test_invalid_arraysize(self, async_pandas_cursor):
        async_pandas_cursor.arraysize = 10000
        assert async_pandas_cursor.arraysize == 10000
        with pytest.raises(ProgrammingError):
            async_pandas_cursor.arraysize = -1

    @pytest.mark.parametrize(
        ("async_pandas_cursor", "parquet_engine", "chunksize"),
        [
            ({"cursor_kwargs": {"unload": False}}, "auto", None),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000),
            ({"cursor_kwargs": {"unload": True}}, "auto", None),
        ],
        indirect=["async_pandas_cursor"],
    )
    def test_description(self, async_pandas_cursor, parquet_engine, chunksize):
        query_id, future = async_pandas_cursor.execute(
            "SELECT 1 AS foobar FROM one_row",
            engine=parquet_engine,
            chunksize=chunksize,
        )
        result_set = future.result()
        assert result_set.fetchall() == [(1,)]
        if async_pandas_cursor._unload:
            assert result_set.description == [("foobar", "integer", None, None, 10, 0, "NULLABLE")]
        else:
            assert result_set.description == [("foobar", "integer", None, None, 10, 0, "UNKNOWN")]

        future = async_pandas_cursor.description(query_id)
        description = future.result()
        assert result_set.description == description

    @pytest.mark.parametrize(
        ("async_pandas_cursor", "parquet_engine", "chunksize"),
        [
            ({"cursor_kwargs": {"unload": False}}, "auto", None),
            ({"cursor_kwargs": {"unload": True}}, "auto", None),
        ],
        indirect=["async_pandas_cursor"],
    )
    def test_query_execution(self, async_pandas_cursor, parquet_engine, chunksize):
        query = "SELECT * FROM one_row"
        query_id, future = async_pandas_cursor.execute(
            query, engine=parquet_engine, chunksize=chunksize
        )
        result_set = future.result()

        future = async_pandas_cursor.query_execution(query_id)
        query_execution = future.result()

        assert query_execution.database == ENV.schema
        assert query_execution.catalog
        assert query_execution.query_id
        if async_pandas_cursor._unload:
            assert query_execution.query.startswith("UNLOAD")
            assert query in query_execution.query
        else:
            assert query_execution.query == query
        assert query_execution.statement_type == AthenaQueryExecution.STATEMENT_TYPE_DML
        assert query_execution.work_group == ENV.default_work_group
        assert query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED
        assert query_execution.state_change_reason is None
        assert query_execution.submission_date_time
        assert isinstance(query_execution.submission_date_time, datetime)
        assert query_execution.completion_date_time
        assert isinstance(query_execution.completion_date_time, datetime)
        assert query_execution.data_scanned_in_bytes
        assert query_execution.engine_execution_time_in_millis
        assert query_execution.query_queue_time_in_millis
        assert query_execution.total_execution_time_in_millis
        # assert query_execution.query_planning_time_in_millis  # TODO flaky test
        # assert query_execution.service_processing_time_in_millis  # TODO flaky test
        assert query_execution.output_location
        assert query_execution.encryption_option is None
        assert query_execution.kms_key is None
        assert query_execution.selected_engine_version
        assert query_execution.effective_engine_version

        assert result_set.database == query_execution.database
        assert result_set.catalog == query_execution.catalog
        assert result_set.query_id == query_execution.query_id
        assert result_set.query == query_execution.query
        assert result_set.statement_type == query_execution.statement_type
        assert result_set.work_group == query_execution.work_group
        assert result_set.state == query_execution.state
        assert result_set.state_change_reason == query_execution.state_change_reason
        assert result_set.submission_date_time == query_execution.submission_date_time
        assert result_set.completion_date_time == query_execution.completion_date_time
        assert result_set.data_scanned_in_bytes == query_execution.data_scanned_in_bytes
        assert (
            result_set.engine_execution_time_in_millis
            == query_execution.engine_execution_time_in_millis
        )
        assert result_set.query_queue_time_in_millis == query_execution.query_queue_time_in_millis
        assert (
            result_set.total_execution_time_in_millis
            == query_execution.total_execution_time_in_millis
        )
        assert (
            result_set.query_planning_time_in_millis
            == query_execution.query_planning_time_in_millis
        )
        assert (
            result_set.service_processing_time_in_millis
            == query_execution.service_processing_time_in_millis
        )
        assert result_set.output_location == query_execution.output_location
        assert result_set.data_manifest_location == query_execution.data_manifest_location
        assert result_set.encryption_option == query_execution.encryption_option
        assert result_set.kms_key == query_execution.kms_key
        assert result_set.selected_engine_version == query_execution.selected_engine_version
        assert result_set.effective_engine_version == query_execution.effective_engine_version

    @pytest.mark.parametrize(
        ("async_pandas_cursor", "parquet_engine", "chunksize"),
        [
            ({"cursor_kwargs": {"unload": False}}, "auto", None),
            ({"cursor_kwargs": {"unload": True}}, "auto", None),
        ],
        indirect=["async_pandas_cursor"],
    )
    def test_poll(self, async_pandas_cursor, parquet_engine, chunksize):
        query_id, _ = async_pandas_cursor.execute(
            "SELECT * FROM one_row",
            engine=parquet_engine,
            chunksize=chunksize,
        )
        future = async_pandas_cursor.poll(query_id)
        query_execution = future.result()
        assert query_execution.state in [
            AthenaQueryExecution.STATE_QUEUED,
            AthenaQueryExecution.STATE_RUNNING,
            AthenaQueryExecution.STATE_SUCCEEDED,
            AthenaQueryExecution.STATE_FAILED,
            AthenaQueryExecution.STATE_CANCELLED,
        ]

    @pytest.mark.parametrize(
        ("async_pandas_cursor", "parquet_engine", "chunksize"),
        [
            ({"cursor_kwargs": {"unload": False}}, "auto", None),
            ({"cursor_kwargs": {"unload": True}}, "pyarrow", None),
        ],
        indirect=["async_pandas_cursor"],
    )
    def test_bad_query(self, async_pandas_cursor, parquet_engine, chunksize):
        query_id, future = async_pandas_cursor.execute(
            "SELECT does_not_exist FROM this_really_does_not_exist",
            engine=parquet_engine,
            chunksize=chunksize,
        )
        result_set = future.result()
        assert result_set.state == AthenaQueryExecution.STATE_FAILED
        assert result_set.state_change_reason is not None
        assert result_set.error_category is not None
        assert result_set.error_type is not None

    @pytest.mark.parametrize(
        ("async_pandas_cursor", "parquet_engine", "chunksize"),
        [
            ({"cursor_kwargs": {"unload": False}}, "auto", None),
            ({"cursor_kwargs": {"unload": False}}, "auto", 1_000),
            ({"cursor_kwargs": {"unload": True}}, "auto", None),
        ],
        indirect=["async_pandas_cursor"],
    )
    def test_as_pandas(self, async_pandas_cursor, parquet_engine, chunksize):
        query_id, future = async_pandas_cursor.execute(
            "SELECT * FROM one_row",
            engine=parquet_engine,
            chunksize=chunksize,
        )
        df = future.result().as_pandas()
        if chunksize:
            df = pd.concat((d for d in df), ignore_index=True)
        assert df.shape[0] == 1
        assert df.shape[1] == 1
        assert [(row["number_of_rows"],) for _, row in df.iterrows()] == [(1,)]

    def test_cancel(self, async_pandas_cursor):
        query_id, future = async_pandas_cursor.execute(
            """
            SELECT a.a * rand(), b.a * rand()
            FROM many_rows a
            CROSS JOIN many_rows b
            """
        )
        time.sleep(randint(5, 10))
        async_pandas_cursor.cancel(query_id)
        result_set = future.result()
        assert result_set.state == AthenaQueryExecution.STATE_CANCELLED
        assert result_set.description is None
        assert result_set.fetchone() is None
        assert result_set.fetchmany() == []
        assert result_set.fetchall() == []

    def test_no_ops(self):
        conn = connect()
        cursor = conn.cursor(AsyncPandasCursor)
        pytest.raises(NotSupportedError, lambda: cursor.executemany("SELECT * FROM one_row", []))
        cursor.close()
        conn.close()

    @pytest.mark.parametrize(
        "execute_kwargs",
        [
            {},
            {
                "engine": "python",
                "na_values": None,
                "block_size": 2048,
                "cache_type": "none",
                "s3_max_workers": 3,
                "auto_optimize_chunksize": False,
            },
        ],
    )
    def test_read_options(self, execute_kwargs):
        """The cursor's read options reach the result set, and execute() overrides them.

        No AWS calls; the query and its result set are mocked.
        """
        cursor_kwargs = {
            "engine": "c",
            "block_size": 1024,
            "cache_type": "bytes",
            "auto_optimize_chunksize": True,
            "s3_max_workers": 2,
        }
        query_execution = MagicMock(state=AthenaQueryExecution.STATE_SUCCEEDED)
        with (
            AsyncPandasCursor(
                connection=MagicMock(),
                converter=MagicMock(),
                formatter=MagicMock(),
                retry_config=RetryConfig(),
                max_workers=1,
                **cursor_kwargs,
            ) as cursor,
            patch.object(AsyncPandasCursor, "_execute", return_value="query_id"),
            patch.object(AsyncPandasCursor, "_poll", return_value=query_execution),
            patch("pyathena.pandas.async_cursor.AthenaPandasResultSet") as result_set_class,
        ):
            _, future = cursor.execute("SELECT 1", **execute_kwargs)
            future.result()
        kwargs = result_set_class.call_args.kwargs
        expected = {**cursor_kwargs, **execute_kwargs}
        expected["max_workers"] = expected.pop("s3_max_workers")
        assert {key: kwargs[key] for key in expected} == expected
        assert cursor._executor._max_workers == 1

    def test_closed(self, async_pandas_cursor):
        """GH-1207: a closed cursor rejects execute()."""
        async_pandas_cursor.close()

        with pytest.raises(ProgrammingError):
            async_pandas_cursor.execute("SELECT * FROM one_row")
