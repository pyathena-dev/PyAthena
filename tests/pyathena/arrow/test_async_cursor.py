import contextlib
import random
import string
import time
from datetime import datetime
from random import randint

import pytest

from pyathena.arrow.async_cursor import AsyncArrowCursor
from pyathena.error import NotSupportedError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet
from tests import ENV
from tests.pyathena.conftest import connect


class TestAsyncArrowCursor:
    @pytest.mark.parametrize(
        "async_arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["async_arrow_cursor"],
    )
    def test_fetchone(self, async_arrow_cursor):
        query_id, future = async_arrow_cursor.execute("SELECT * FROM one_row")
        result_set = future.result()
        assert result_set.rownumber == 0
        assert result_set.fetchone() == (1,)
        assert result_set.rownumber == 1
        assert result_set.fetchone() is None

    @pytest.mark.parametrize(
        "async_arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["async_arrow_cursor"],
    )
    def test_fetchmany(self, async_arrow_cursor):
        query_id, future = async_arrow_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        result_set = future.result()
        assert len(result_set.fetchmany(10)) == 10
        assert len(result_set.fetchmany(10)) == 5

    @pytest.mark.parametrize(
        "async_arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["async_arrow_cursor"],
    )
    def test_fetchall(self, async_arrow_cursor):
        query_id, future = async_arrow_cursor.execute("SELECT * FROM one_row")
        result_set = future.result()
        assert result_set.fetchall() == [(1,)]
        query_id, future = async_arrow_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        result_set = future.result()
        if async_arrow_cursor._unload:
            assert sorted(result_set.fetchall()) == [(i,) for i in range(10000)]
        else:
            assert result_set.fetchall() == [(i,) for i in range(10000)]

    @pytest.mark.parametrize(
        "async_arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["async_arrow_cursor"],
    )
    def test_iterator(self, async_arrow_cursor):
        query_id, future = async_arrow_cursor.execute("SELECT * FROM one_row")
        result_set = future.result()
        assert list(result_set) == [(1,)]
        pytest.raises(StopIteration, result_set.__next__)

    @pytest.mark.parametrize(
        "async_arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["async_arrow_cursor"],
    )
    def test_arraysize(self, async_arrow_cursor):
        async_arrow_cursor.arraysize = 5
        query_id, future = async_arrow_cursor.execute("SELECT * FROM many_rows LIMIT 20")
        result_set = future.result()
        assert len(result_set.fetchmany()) == 5

    def test_arraysize_default(self, async_arrow_cursor):
        assert async_arrow_cursor.arraysize == AthenaResultSet.DEFAULT_FETCH_SIZE

    def test_invalid_arraysize(self, async_arrow_cursor):
        async_arrow_cursor.arraysize = 10000
        assert async_arrow_cursor.arraysize == 10000
        with pytest.raises(ProgrammingError):
            async_arrow_cursor.arraysize = -1

    @pytest.mark.parametrize(
        "async_arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["async_arrow_cursor"],
    )
    def test_description(self, async_arrow_cursor):
        query_id, future = async_arrow_cursor.execute(
            "SELECT CAST(1 AS INT) AS foobar FROM one_row"
        )
        result_set = future.result()
        assert result_set.fetchall() == [(1,)]
        if async_arrow_cursor._unload:
            assert result_set.description == [("foobar", "integer", None, None, 10, 0, "NULLABLE")]
        else:
            assert result_set.description == [("foobar", "integer", None, None, 10, 0, "UNKNOWN")]

        future = async_arrow_cursor.description(query_id)
        description = future.result()
        assert result_set.description == description

    @pytest.mark.parametrize(
        "async_arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["async_arrow_cursor"],
    )
    def test_query_execution(self, async_arrow_cursor):
        query = "SELECT * FROM one_row"
        query_id, future = async_arrow_cursor.execute(query)
        result_set = future.result()

        future = async_arrow_cursor.query_execution(query_id)
        query_execution = future.result()

        assert query_execution.database == ENV.schema
        assert query_execution.catalog
        assert query_execution.query_id
        if async_arrow_cursor._unload:
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
        "async_arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["async_arrow_cursor"],
    )
    def test_poll(self, async_arrow_cursor):
        query_id, _ = async_arrow_cursor.execute("SELECT * FROM one_row")
        future = async_arrow_cursor.poll(query_id)
        query_execution = future.result()
        assert query_execution.state in [
            AthenaQueryExecution.STATE_QUEUED,
            AthenaQueryExecution.STATE_RUNNING,
            AthenaQueryExecution.STATE_SUCCEEDED,
            AthenaQueryExecution.STATE_FAILED,
            AthenaQueryExecution.STATE_CANCELLED,
        ]

    @pytest.mark.parametrize(
        "async_arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["async_arrow_cursor"],
    )
    def test_bad_query(self, async_arrow_cursor):
        query_id, future = async_arrow_cursor.execute(
            "SELECT does_not_exist FROM this_really_does_not_exist"
        )
        result_set = future.result()
        assert result_set.state == AthenaQueryExecution.STATE_FAILED
        assert result_set.state_change_reason is not None
        assert result_set.error_category is not None
        assert result_set.error_type is not None

    @pytest.mark.parametrize(
        "async_arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["async_arrow_cursor"],
    )
    def test_as_arrow(self, async_arrow_cursor):
        query_id, future = async_arrow_cursor.execute("SELECT * FROM one_row")
        table = future.result().as_arrow()
        assert table.shape[0] == 1
        assert table.shape[1] == 1
        assert list(zip(*table.to_pydict().values(), strict=False)) == [(1,)]

    @pytest.mark.parametrize(
        "async_arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["async_arrow_cursor"],
    )
    def test_many_as_arrow(self, async_arrow_cursor):
        query_id, future = async_arrow_cursor.execute("SELECT * FROM many_rows")
        table = future.result().as_arrow()
        assert table.shape[0] == 10000
        assert table.shape[1] == 1
        assert list(zip(*table.to_pydict().values(), strict=False)) == [(i,) for i in range(10000)]

    def test_cancel(self, async_arrow_cursor):
        query_id, future = async_arrow_cursor.execute(
            """
            SELECT a.a * rand(), b.a * rand()
            FROM many_rows a
            CROSS JOIN many_rows b
            """
        )
        time.sleep(randint(5, 10))
        async_arrow_cursor.cancel(query_id)
        result_set = future.result()
        assert result_set.state == AthenaQueryExecution.STATE_CANCELLED
        assert result_set.description is None
        assert result_set.fetchone() is None
        assert result_set.fetchmany() == []
        assert result_set.fetchall() == []

    def test_open_close(self):
        with contextlib.closing(connect()) as conn, conn.cursor(AsyncArrowCursor):
            pass

    def test_no_ops(self):
        conn = connect()
        cursor = conn.cursor(AsyncArrowCursor)
        pytest.raises(NotSupportedError, lambda: cursor.executemany("SELECT * FROM one_row", []))
        cursor.close()
        conn.close()

    @pytest.mark.parametrize(
        "async_arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=["async_arrow_cursor"],
    )
    def test_empty_result(self, async_arrow_cursor):
        table = "test_pandas_cursor_empty_result_" + "".join(
            random.choices(string.ascii_lowercase + string.digits, k=10)
        )
        query_id, future = async_arrow_cursor.execute(
            f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS
            {ENV.schema}.{table} (number_of_rows INT)
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            LINES TERMINATED BY '\n' STORED AS TEXTFILE
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table}/'
            """
        )
        table = future.result().as_arrow()
        assert table.shape[0] == 0
        assert table.shape[1] == 0

    @pytest.mark.parametrize(
        "async_arrow_cursor",
        [
            {
                "cursor_kwargs": {"unload": True},
            },
        ],
        indirect=["async_arrow_cursor"],
    )
    def test_empty_result_unload(self, async_arrow_cursor):
        query_id, future = async_arrow_cursor.execute(
            """
            SELECT * FROM one_row LIMIT 0
            """
        )
        table = future.result().as_arrow()
        assert table.shape[0] == 0
        assert table.shape[1] == 0
