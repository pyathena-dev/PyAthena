import contextlib
import time
from collections import OrderedDict
from datetime import datetime
from random import randint

import pytest

from pyathena.async_cursor import AsyncCursor, AsyncDictCursor
from pyathena.error import NotSupportedError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaDictResultSet, AthenaResultSet
from tests import ENV
from tests.pyathena.conftest import connect
from tests.pyathena.util import (
    EVENT_TIMEOUT,
    answered_requests,
    assert_close_stops_waiting_futures,
    assert_closes_result_sets_after_close,
    assert_description_reads_only_metadata,
    csv_result_responses,
    placeholder_connection,
)


class TestAsyncCursor:
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

    def test_on_poll(self):
        """on_poll fires during async polling (issue #723 example)."""
        states = []

        with contextlib.closing(
            connect(on_poll=lambda execution: states.append(execution.state))
        ) as conn:
            cursor = conn.cursor(AsyncCursor)
            _, future = cursor.execute("SELECT 1")
            future.result()

            assert len(states) >= 1
            assert states[-1] == AthenaQueryExecution.STATE_SUCCEEDED

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

    def test_closed(self, async_cursor):
        """GH-1207: a closed cursor rejects operations."""
        async_cursor.close()

        with pytest.raises(ProgrammingError):
            async_cursor.execute("SELECT * FROM one_row")
        with pytest.raises(ProgrammingError):
            async_cursor.executemany("SELECT * FROM one_row", [])
        with pytest.raises(ProgrammingError):
            async_cursor.description("query_id")
        with pytest.raises(ProgrammingError):
            async_cursor.query_execution("query_id")
        with pytest.raises(ProgrammingError):
            async_cursor.poll("query_id")
        with pytest.raises(ProgrammingError):
            async_cursor.cancel("query_id")

    def test_close_closes_result_sets_after_close(self):
        """GH-1208: closing the cursor closes the result sets created after it (no AWS)."""
        assert_closes_result_sets_after_close(AsyncCursor)

    def test_close_stops_waiting_futures(self):
        """GH-1281: closing the cursor stops the futures that wait for a query (no AWS)."""
        assert_close_stops_waiting_futures(AsyncCursor)

    def test_execute_result_set_class_with_earlier_parameters(self):
        """GH-1281: execute() builds a result set class with the earlier parameters (no AWS).

        The class takes none of the keywords that ``describe()`` passes.
        """

        class CustomResultSet(AthenaResultSet):
            def __init__(
                self,
                connection,
                converter,
                query_execution,
                arraysize,
                retry_config,
                result_set_type_hints=None,
            ):
                super().__init__(
                    connection,
                    converter,
                    query_execution,
                    arraysize,
                    retry_config,
                    result_set_type_hints=result_set_type_hints,
                )

        cursor = placeholder_connection().cursor(AsyncCursor)
        cursor._result_set_class = CustomResultSet
        try:
            with answered_requests(cursor.connection.client, csv_result_responses()):
                _, future = cursor.execute("SELECT 1")
                result_set = future.result(EVENT_TIMEOUT)
        finally:
            cursor.close(wait=True)
        assert isinstance(result_set, CustomResultSet)
        assert result_set.description == [("a", "integer", None, None, 3, 0, "UNKNOWN")]

    def test_description_reads_only_metadata(self):
        """GH-1281: description() reads only the metadata of a CSV result (no AWS)."""
        assert_description_reads_only_metadata(AsyncCursor)


class TestAsyncDictCursor:
    def test_dict_type(self, async_dict_cursor):
        with async_dict_cursor.connection.cursor(dict_type=OrderedDict) as ordered_cursor:
            _, future = ordered_cursor.execute("SELECT * FROM one_row")
            assert type(future.result().fetchone()) is OrderedDict
        # dict_type of another cursor does not change the row type of this one.
        _, future = async_dict_cursor.execute("SELECT * FROM one_row")
        assert type(future.result().fetchone()) is dict

    def test_dict_type_custom_result_set(self, async_dict_cursor):
        class CustomResultSet(AthenaDictResultSet):
            pass

        class CustomDictCursor(AsyncDictCursor):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self._result_set_class = CustomResultSet

        with async_dict_cursor.connection.cursor(CustomDictCursor, dict_type=OrderedDict) as cursor:
            _, future = cursor.execute("SELECT * FROM one_row")
            result_set = future.result()
            assert isinstance(result_set, CustomResultSet)
            assert type(result_set.fetchone()) is OrderedDict
