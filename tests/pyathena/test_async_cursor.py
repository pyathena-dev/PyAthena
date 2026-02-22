import contextlib
import time
from datetime import datetime
from random import randint

import pytest

from pyathena.async_cursor import AsyncCursor
from pyathena.error import NotSupportedError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet
from tests import ENV
from tests.pyathena.conftest import connect


class TestAsyncCursor:
    def test_fetchone(self, async_cursor):
        query_id, future = async_cursor.execute("SELECT * FROM one_row")
        result_set = future.result()
        assert result_set.rownumber == 0
        assert result_set.fetchone() == (1,)
        assert result_set.rownumber == 1
        assert result_set.fetchone() is None
        assert result_set.database == ENV.schema
        assert result_set.catalog
        assert result_set.query_id
        assert result_set.query
        assert result_set.statement_type == AthenaQueryExecution.STATEMENT_TYPE_DML
        assert result_set.work_group == ENV.default_work_group
        assert result_set.state == AthenaQueryExecution.STATE_SUCCEEDED
        assert result_set.state_change_reason is None
        assert result_set.submission_date_time
        assert isinstance(result_set.submission_date_time, datetime)
        assert result_set.completion_date_time
        assert isinstance(result_set.completion_date_time, datetime)
        assert result_set.data_scanned_in_bytes
        assert result_set.engine_execution_time_in_millis
        assert result_set.query_queue_time_in_millis
        assert result_set.total_execution_time_in_millis
        # assert result_set.query_planning_time_in_millis  # TODO flaky test
        # assert result_set.service_processing_time_in_millis  # TODO flaky test
        assert result_set.output_location
        assert result_set.data_manifest_location is None
        assert result_set.selected_engine_version
        assert result_set.effective_engine_version

    def test_fetchmany(self, async_cursor):
        query_id, future = async_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        result_set = future.result()
        actual1 = result_set.fetchmany(10)
        assert len(actual1) == 10
        assert actual1 == [(i,) for i in range(10)]
        actual2 = result_set.fetchmany(10)
        assert len(actual2) == 5
        assert actual2 == [(i,) for i in range(10, 15)]

    def test_fetchall(self, async_cursor):
        query_id, future = async_cursor.execute("SELECT * FROM one_row")
        result_set = future.result()
        assert result_set.fetchall() == [(1,)]
        query_id, future = async_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        result_set = future.result()
        assert result_set.fetchall() == [(i,) for i in range(10000)]

    def test_iterator(self, async_cursor):
        query_id, future = async_cursor.execute("SELECT * FROM one_row")
        result_set = future.result()
        assert list(result_set) == [(1,)]
        pytest.raises(StopIteration, result_set.__next__)

    def test_arraysize(self, async_cursor):
        async_cursor.arraysize = 5
        query_id, future = async_cursor.execute("SELECT * FROM many_rows LIMIT 20")
        result_set = future.result()
        assert len(result_set.fetchmany()) == 5

    def test_arraysize_default(self, async_cursor):
        assert async_cursor.arraysize == AthenaResultSet.DEFAULT_FETCH_SIZE

    def test_invalid_arraysize(self, async_cursor):
        with pytest.raises(ProgrammingError):
            async_cursor.arraysize = 10000
        with pytest.raises(ProgrammingError):
            async_cursor.arraysize = -1

    def test_description(self, async_cursor):
        query_id, future = async_cursor.execute("SELECT 1 AS foobar FROM one_row")
        result_set = future.result()
        assert result_set.fetchall() == [(1,)]
        assert result_set.description == [("foobar", "integer", None, None, 10, 0, "UNKNOWN")]

        future = async_cursor.description(query_id)
        description = future.result()
        assert result_set.description == description

    def test_query_execution(self, async_cursor):
        query = "SELECT * FROM one_row"
        query_id, future = async_cursor.execute(query)
        result_set = future.result()

        future = async_cursor.query_execution(query_id)
        query_execution = future.result()

        assert query_execution.database == ENV.schema
        assert query_execution.catalog
        assert query_execution.query_id
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

    def test_poll(self, async_cursor):
        query_id, _ = async_cursor.execute("SELECT * FROM one_row")
        future = async_cursor.poll(query_id)
        query_execution = future.result()
        assert query_execution.state in [
            AthenaQueryExecution.STATE_QUEUED,
            AthenaQueryExecution.STATE_RUNNING,
            AthenaQueryExecution.STATE_SUCCEEDED,
            AthenaQueryExecution.STATE_FAILED,
            AthenaQueryExecution.STATE_CANCELLED,
        ]

    def test_bad_query(self, async_cursor):
        query_id, future = async_cursor.execute(
            "SELECT does_not_exist FROM this_really_does_not_exist"
        )
        result_set = future.result()
        assert result_set.state == AthenaQueryExecution.STATE_FAILED
        assert result_set.state_change_reason

    def test_cancel(self, async_cursor):
        query_id, future = async_cursor.execute(
            """
            SELECT a.a * rand(), b.a * rand()
            FROM many_rows a
            CROSS JOIN many_rows b
            """
        )
        time.sleep(randint(5, 10))
        async_cursor.cancel(query_id)
        result_set = future.result()
        assert result_set.state == AthenaQueryExecution.STATE_CANCELLED
        assert result_set.description is None
        assert result_set.fetchone() is None
        assert result_set.fetchmany() == []
        assert result_set.fetchall() == []

    def test_open_close(self):
        with contextlib.closing(connect()) as conn, conn.cursor(AsyncCursor):
            pass

    def test_no_ops(self):
        conn = connect()
        cursor = conn.cursor(AsyncCursor)
        pytest.raises(NotSupportedError, lambda: cursor.executemany("SELECT * FROM one_row", []))
        cursor.close()
        conn.close()


class TestAsyncDictCursor:
    def test_fetchone(self, async_dict_cursor):
        query_id, future = async_dict_cursor.execute("SELECT * FROM one_row")
        result_set = future.result()
        assert result_set.fetchone() == {"number_of_rows": 1}

    def test_fetchmany(self, async_dict_cursor):
        query_id, future = async_dict_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        result_set = future.result()
        actual1 = result_set.fetchmany(10)
        assert len(actual1) == 10
        assert actual1 == [{"a": i} for i in range(10)]
        actual2 = result_set.fetchmany(10)
        assert len(actual2) == 5
        assert actual2 == [{"a": i} for i in range(10, 15)]

    def test_fetchall(self, async_dict_cursor):
        query_id, future = async_dict_cursor.execute("SELECT * FROM one_row")
        result_set = future.result()
        assert result_set.fetchall() == [{"number_of_rows": 1}]
        query_id, future = async_dict_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        result_set = future.result()
        assert result_set.fetchall() == [{"a": i} for i in range(10000)]
