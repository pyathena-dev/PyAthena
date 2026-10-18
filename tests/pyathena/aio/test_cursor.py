import asyncio
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from botocore.exceptions import ClientError

from pyathena import ExecuteOptions
from pyathena.aio.connection import AioConnection
from pyathena.aio.cursor import AioCursor, AioDictCursor
from pyathena.aio.result_set import AthenaAioDictResultSet
from pyathena.error import DatabaseError, OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.util import RetryConfig
from tests import ENV
from tests.pyathena.util import (
    EVENT_TIMEOUT,
    FAILED_EXECUTION_RESPONSES,
    QueryExecutionHistory,
    ThrottledOperation,
    answered_requests,
    cache_search_cursor,
    placeholder_connection,
    succeeded_query_execution_data,
    throttle_metadata_api,
    wait_until,
)


def _offline_cursor(kill_on_interrupt, final_state):
    """An AioCursor whose first status request blocks until the task is cancelled.

    Later status requests report ``RUNNING`` once, then ``final_state``.

    Args:
        kill_on_interrupt: Whether the cursor cancels the query on cancellation.
        final_state: The state of the query after cancellation.

    Returns:
        The cursor, the mock of its cancellation request, and an event set when the
        first status request starts.
    """
    polling = asyncio.Event()
    states = iter([AthenaQueryExecution.STATE_RUNNING, final_state])

    async def get_query_execution(query_id):
        if not polling.is_set():
            polling.set()
            await asyncio.Event().wait()
        return MagicMock(state=next(states))

    cursor = AioCursor.__new__(AioCursor)  # bypass __init__ to avoid AWS calls
    cursor._closed = False
    cursor._rowcount = -1
    cursor._result_set = None
    cursor._poll_interval = 0
    cursor._kill_on_interrupt = kill_on_interrupt
    cursor._on_poll = None
    cursor._on_start_query_execution = None
    cursor._execute = AsyncMock(return_value="query_id")
    cursor._get_query_execution = get_query_execution
    # A successful query builds a result set from these.
    cursor._connection = MagicMock()
    cursor._converter = MagicMock()
    cursor._arraysize = 1
    cursor._retry_config = RetryConfig()
    cursor._result_set_class = MagicMock(create=AsyncMock())
    cancel = cursor._cancel = AsyncMock()
    return cursor, cancel, polling


def _starting_cursor(kill_on_interrupt=True, response=None):
    """An AioCursor whose StartQueryExecution request blocks in a thread until released.

    Status requests report ``RUNNING`` once, then ``CANCELLED``.

    Args:
        kill_on_interrupt: Whether the cursor cancels the query on cancellation.
        response: The exception the start request raises once released; the
            default returns ``query_id``.

    Returns:
        The cursor, the mock of its cancellation request, an event set when the
        start request starts, and an event that releases it.
    """
    started = threading.Event()
    release = threading.Event()

    def start_query_execution(**kwargs):
        started.set()
        assert release.wait(EVENT_TIMEOUT)
        if response:
            raise response
        return {"QueryExecutionId": "query_id"}

    cursor, cancel, _ = _offline_cursor(kill_on_interrupt, AthenaQueryExecution.STATE_CANCELLED)
    del cursor._execute  # use the real _execute()
    cursor._query_id = None
    cursor._prepare_query = MagicMock(return_value=("SELECT 1", None))
    cursor._build_start_query_execution_request = MagicMock(return_value={})
    cursor._find_previous_query_id = AsyncMock(return_value=None)
    cursor._connection.client.start_query_execution.side_effect = start_query_execution
    cursor._retry_config = RetryConfig(attempt=2, multiplier=0)
    cursor._get_query_execution = AsyncMock(
        side_effect=[
            MagicMock(state=AthenaQueryExecution.STATE_RUNNING),
            MagicMock(state=AthenaQueryExecution.STATE_CANCELLED),
        ]
    )
    return cursor, cancel, started, release


class TestAioCursor:
    async def test_fetchone(self, aio_cursor):
        assert await aio_cursor.execute("SELECT * FROM one_row") is aio_cursor
        assert aio_cursor.rowcount == -1
        assert aio_cursor.rownumber == 0
        assert await aio_cursor.fetchone() == (1,)
        assert aio_cursor.rownumber == 1
        assert await aio_cursor.fetchone() is None
        assert aio_cursor.database == ENV.schema
        assert aio_cursor.catalog
        assert aio_cursor.query_id
        assert aio_cursor.query
        assert aio_cursor.statement_type == AthenaQueryExecution.STATEMENT_TYPE_DML
        assert aio_cursor.work_group == ENV.default_work_group
        assert aio_cursor.state == AthenaQueryExecution.STATE_SUCCEEDED
        assert aio_cursor.state_change_reason is None
        assert aio_cursor.submission_date_time
        assert isinstance(aio_cursor.submission_date_time, datetime)
        assert aio_cursor.completion_date_time
        assert isinstance(aio_cursor.completion_date_time, datetime)
        assert aio_cursor.data_scanned_in_bytes
        assert aio_cursor.engine_execution_time_in_millis
        assert aio_cursor.query_queue_time_in_millis
        assert aio_cursor.total_execution_time_in_millis
        assert aio_cursor.output_location
        assert aio_cursor.data_manifest_location is None
        assert aio_cursor.encryption_option is None
        assert aio_cursor.kms_key is None
        assert aio_cursor.selected_engine_version
        assert aio_cursor.effective_engine_version

    async def test_fetchmany(self, aio_cursor):
        await aio_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        actual1 = await aio_cursor.fetchmany(10)
        assert len(actual1) == 10
        assert actual1 == [(i,) for i in range(10)]
        actual2 = await aio_cursor.fetchmany(10)
        assert len(actual2) == 5
        assert actual2 == [(i,) for i in range(10, 15)]

    async def test_fetchall(self, aio_cursor):
        await aio_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        assert await aio_cursor.fetchall() == [(i,) for i in range(10000)]

    async def test_execute_with_callback(self, aio_cursor):
        callback_results = []
        await aio_cursor.execute("SELECT 1", on_start_query_execution=callback_results.append)
        assert callback_results == [aio_cursor.query_id]
        assert await aio_cursor.fetchone() == (1,)

    async def test_execute_with_options(self, aio_cursor):
        callback_results = []
        options = ExecuteOptions(on_start_query_execution=callback_results.append)
        await aio_cursor.execute("SELECT 1", options=options)
        assert callback_results == [aio_cursor.query_id]
        assert await aio_cursor.fetchone() == (1,)

    async def test_execute_internal_options_passthrough(self):
        """The options of execute() reach its Athena requests (no AWS)."""
        cursor = placeholder_connection(AioConnection).cursor()
        options = ExecuteOptions(
            work_group="test_work_group",
            s3_staging_dir="s3://test-bucket/path/",
            cache_size=10,
            cache_expiration_time=100,
            result_reuse_enable=True,
            result_reuse_minutes=5,
            paramstyle="qmark",
        )
        # The same query an hour ago, too old for cache_expiration_time to reuse.
        previous = {
            **succeeded_query_execution_data(
                "previous", "SELECT 1", datetime.now(UTC) - timedelta(hours=1), schema="default"
            ),
            "QueryExecutionContext": {"Database": "default", "Catalog": "awsdatacatalog"},
        }
        responses = {
            **FAILED_EXECUTION_RESPONSES,
            "ListQueryExecutions": {"QueryExecutionIds": ["previous"]},
            "BatchGetQueryExecution": {"QueryExecutions": [previous]},
        }
        with (
            answered_requests(cursor.connection.client, responses) as requests,
            pytest.raises(OperationalError),
        ):
            await cursor.execute("SELECT 1", options=options)

        parameters = dict(requests)
        assert parameters["ListQueryExecutions"] == {
            "WorkGroup": "test_work_group",
            "MaxResults": 10,
        }
        start = parameters["StartQueryExecution"]
        assert start["WorkGroup"] == "test_work_group"
        assert start["ResultConfiguration"] == {"OutputLocation": "s3://test-bucket/path/"}
        assert start["ResultReuseConfiguration"] == {
            "ResultReuseByAgeConfiguration": {"Enabled": True, "MaxAgeInMinutes": 5}
        }

    async def test_execute_qmark_parameters_skip_cache(self):
        """A qmark query with parameters never searches the cache (no AWS, #941)."""
        cursor = placeholder_connection(AioConnection).cursor()
        with (
            answered_requests(cursor.connection.client) as requests,
            pytest.raises(OperationalError),
        ):
            await cursor.execute("SELECT ?", ["'1'"], paramstyle="qmark", cache_size=10)

        assert [operation for operation, _ in requests] == [
            "StartQueryExecution",
            "GetQueryExecution",
        ]
        assert requests[0][1]["ExecutionParameters"] == ["'1'"]

    @pytest.mark.parametrize(
        "final_state",
        [AthenaQueryExecution.STATE_CANCELLED, AthenaQueryExecution.STATE_SUCCEEDED],
    )
    async def test_execute_kill_on_interrupt(self, final_state):
        """Task cancellation cancels the query, waits for it, and is re-raised (no AWS)."""
        polled = []
        cursor, cancel, polling = _offline_cursor(kill_on_interrupt=True, final_state=final_state)
        cursor._on_poll = polled.append
        task = asyncio.create_task(cursor.execute("SELECT 1"))
        await polling.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert task.cancelled()
        cancel.assert_awaited_once_with("query_id")
        # The cancellation is re-raised only after the query reaches a terminal state.
        assert [execution.state for execution in polled] == [
            AthenaQueryExecution.STATE_RUNNING,
            final_state,
        ]
        assert cursor.query_id == "query_id"
        assert cursor.result_set is None

    @pytest.mark.parametrize("failing", ["cancel", "wait"])
    async def test_execute_kill_on_interrupt_failure(self, failing):
        """A failure to cancel or wait becomes the cause of the cancellation (no AWS)."""
        error = OperationalError("failed")
        cursor, cancel, _ = _offline_cursor(
            kill_on_interrupt=True, final_state=AthenaQueryExecution.STATE_SUCCEEDED
        )
        # Raise the cancellation from the first status request directly, so that the
        # test receives the re-raised exception itself rather than one made by a task.
        cursor._get_query_execution = AsyncMock(side_effect=[asyncio.CancelledError(), error])
        if failing == "cancel":
            cancel.side_effect = error
        with pytest.raises(asyncio.CancelledError) as exc_info:
            await cursor.execute("SELECT 1")

        assert exc_info.value.__cause__ is error
        cancel.assert_awaited_once_with("query_id")

    async def test_execute_without_kill_on_interrupt(self):
        """Without kill_on_interrupt, cancellation propagates at once (no AWS)."""
        cursor, cancel, polling = _offline_cursor(
            kill_on_interrupt=False, final_state=AthenaQueryExecution.STATE_SUCCEEDED
        )
        task = asyncio.create_task(cursor.execute("SELECT 1"))
        await polling.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        cancel.assert_not_awaited()
        assert cursor.query_id == "query_id"

    async def test_execute_cancelled_while_starting(self):
        """Cancellation during the start request stops the query it starts (no AWS)."""
        polled = []
        cursor, cancel, started, release = _starting_cursor()
        cursor._on_poll = polled.append
        task = asyncio.create_task(cursor.execute("SELECT 1"))
        assert await asyncio.to_thread(started.wait, EVENT_TIMEOUT)
        task.cancel()
        # Let the task handle the cancellation before the start request finishes.
        for _ in range(5):
            await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert task.cancelled()
        cursor._connection.client.start_query_execution.assert_called_once()
        cancel.assert_awaited_once_with("query_id")
        assert [execution.state for execution in polled] == [
            AthenaQueryExecution.STATE_RUNNING,
            AthenaQueryExecution.STATE_CANCELLED,
        ]
        assert cursor.query_id == "query_id"
        assert cursor.result_set is None

    async def test_execute_cancelled_while_start_is_retried(self):
        """GH-1202: a cancellation does not stop the retries of the shielded start (no AWS).

        The query that the retried request starts is then cancelled.
        """
        cursor, cancel, _, _ = _starting_cursor()
        operation = ThrottledOperation(block=True, failures=1, operation_name="StartQueryExecution")

        def start_query_execution(**kwargs):
            operation(**kwargs)
            return {"QueryExecutionId": "query_id"}

        cursor._connection.client.start_query_execution.side_effect = start_query_execution
        task = asyncio.create_task(cursor.execute("SELECT 1"))
        try:
            assert await asyncio.to_thread(operation.started.wait, EVENT_TIMEOUT)
            task.cancel()
            # Let the task handle the cancellation before the first attempt fails.
            for _ in range(5):
                await asyncio.sleep(0)
            assert not task.done()
        finally:
            operation.release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert task.cancelled()
        assert operation.calls == 2
        cancel.assert_awaited_once_with("query_id")
        assert cursor.query_id == "query_id"

    async def test_execute_cancelled_again_while_start_is_retried(self):
        """GH-1202: a second cancellation stops the retries of the start request (no AWS)."""
        executor = ThreadPoolExecutor()
        asyncio.get_running_loop().set_default_executor(executor)
        cursor, cancel, _, _ = _starting_cursor()
        operation = ThrottledOperation(block=True, failures=1, operation_name="StartQueryExecution")

        def start_query_execution(**kwargs):
            operation(**kwargs)
            return {"QueryExecutionId": "query_id"}

        cursor._connection.client.start_query_execution.side_effect = start_query_execution
        task = asyncio.create_task(cursor.execute("SELECT 1"))
        try:
            assert await asyncio.to_thread(operation.started.wait, EVENT_TIMEOUT)
            task.cancel()
            # Let the task handle the cancellation and wait for the start task.
            for _ in range(5):
                await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            operation.release.set()
        # The thread of the start request ends once its retries stop; a retry
        # would follow the first attempt at once.
        executor.shutdown(wait=True)
        assert operation.calls == 1
        cancel.assert_not_awaited()

    async def test_cancel_cancelled_while_retrying(self):
        """GH-1202: a cancellation does not stop the retries of StopQueryExecution (no AWS)."""
        cursor, _, _ = _offline_cursor(
            kill_on_interrupt=True, final_state=AthenaQueryExecution.STATE_CANCELLED
        )
        del cursor._cancel  # use the real _cancel()
        cursor._retry_config = RetryConfig(attempt=3, multiplier=0)
        operation = ThrottledOperation(block=True, failures=2, operation_name="StopQueryExecution")
        cursor._connection.client.stop_query_execution.side_effect = operation
        task = asyncio.create_task(cursor._cancel("query_id"))
        try:
            assert await asyncio.to_thread(operation.started.wait, EVENT_TIMEOUT)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            operation.release.set()
        await wait_until(lambda: operation.calls == 3)

    async def test_execute_cancelled_before_request_is_sent(self):
        """Cancellation before the start task begins sends no request (no AWS)."""
        cursor, cancel, started, release = _starting_cursor()
        release.set()
        task = asyncio.create_task(cursor.execute("SELECT 1"))
        # The task runs until it awaits the start task, which has not begun yet.
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert task.cancelled()
        assert not started.is_set()
        cursor._connection.client.start_query_execution.assert_not_called()
        cancel.assert_not_awaited()
        assert cursor.query_id is None

    @pytest.mark.parametrize("failing", ["start", "cancel"])
    async def test_execute_cancelled_while_starting_failure(self, failing):
        """A failure to start or cancel becomes the cause of the cancellation (no AWS)."""
        error = OperationalError("failed")
        cursor, cancel, started, release = _starting_cursor(
            response=ClientError(
                {"Error": {"Code": "InvalidRequestException", "Message": "failed"}},
                "StartQueryExecution",
            )
            if failing == "start"
            else None
        )
        if failing == "cancel":
            cancel.side_effect = error
        raised = []

        async def execute():
            try:
                await cursor.execute("SELECT 1")
            except asyncio.CancelledError as e:
                raised.append(e)
                raise

        task = asyncio.create_task(execute())
        assert await asyncio.to_thread(started.wait, EVENT_TIMEOUT)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert task.cancelled()
        if failing == "start":
            assert isinstance(raised[0].__cause__, DatabaseError)
            cancel.assert_not_awaited()
            assert cursor.query_id is None
        else:
            assert raised[0].__cause__ is error
            cancel.assert_awaited_once_with("query_id")
            assert cursor.query_id == "query_id"

    async def test_execute_cancelled_while_starting_without_kill_on_interrupt(self):
        """Without kill_on_interrupt, cancellation during the start propagates at once (no AWS)."""
        cursor, cancel, started, release = _starting_cursor(kill_on_interrupt=False)
        task = asyncio.create_task(cursor.execute("SELECT 1"))
        assert await asyncio.to_thread(started.wait, EVENT_TIMEOUT)
        try:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()

        assert task.cancelled()
        cancel.assert_not_awaited()
        assert cursor.query_id is None

    async def test_cache_search_stops_at_expired_execution(self):
        query = "SELECT * FROM one_row"
        now = datetime.now(UTC)
        page_size = AioCursor.LIST_QUERY_EXECUTIONS_MAX_RESULTS
        executions = [
            succeeded_query_execution_data("other", "SELECT 1", now),
            succeeded_query_execution_data("expired", query, now - timedelta(hours=2)),
            *(
                succeeded_query_execution_data(f"older_{i}", "SELECT 1", now - timedelta(hours=3))
                for i in range(page_size - 2)
            ),
            # A search that went on to the second page would return this match.
            succeeded_query_execution_data("next_page", query, now),
        ]
        history = QueryExecutionHistory(executions)
        cursor = cache_search_cursor(AioCursor, history)

        # Without cache_size, cache_expiration_time alone bounds the search.
        assert await cursor._find_previous_query_id(query, None, cache_expiration_time=3600) is None
        assert len(history.list_requests) == 1
        # A failed search would also return None, but the same search reuses the
        # execution once it is within cache_expiration_time.
        assert (
            await cursor._find_previous_query_id(query, None, cache_expiration_time=3 * 3600)
            == "expired"
        )

    @pytest.mark.parametrize(
        ("position", "expected"),
        [(59, "match"), (60, None)],
        ids=["last_searched", "after_cache_size"],
    )
    async def test_cache_search_reads_pages_up_to_cache_size(self, position, expected):
        """With cache_size=60, the search reads the 60 latest executions over two pages."""
        query = "SELECT * FROM one_row"
        now = datetime.now(UTC)
        executions = [
            succeeded_query_execution_data(f"other_{i}", "SELECT 1", now) for i in range(70)
        ]
        executions[position] = succeeded_query_execution_data("match", query, now)
        history = QueryExecutionHistory(executions)
        cursor = cache_search_cursor(AioCursor, history)

        assert await cursor._find_previous_query_id(query, "wg", cache_size=60) == expected
        assert [request["WorkGroup"] for request in history.list_requests] == ["wg", "wg"]

    async def test_description(self, aio_cursor):
        await aio_cursor.execute("SELECT 1 AS foobar FROM one_row")
        assert await aio_cursor.fetchall() == [(1,)]
        assert aio_cursor.description == [("foobar", "integer", None, None, 10, 0, "UNKNOWN")]

    async def test_bad_query(self, aio_cursor):
        with pytest.raises(DatabaseError):
            await aio_cursor.execute("SELECT does_not_exist FROM this_really_does_not_exist")

    # The sum of the update counts does not depend on the statement; the synchronous
    # test_iceberg_table checks the update count of INSERT, UPDATE, and DELETE.
    @pytest.mark.parametrize(
        ("operation", "expected"),
        [
            pytest.param(
                "UPDATE {table} SET value=value+1 WHERE group_id=%(group_id)d",
                [(1, 11), (2, 21), (3, 31)],
                id="update",
            ),
        ],
    )
    async def test_executemany_rowcount(self, aio_cursor, executemany_table, operation, expected):
        operation = operation.format(table=executemany_table)
        await aio_cursor.execute(f"SELECT id, value FROM {executemany_table} ORDER BY id")
        previous = aio_cursor.result_set
        result = await aio_cursor.executemany(
            operation,
            [{"group_id": 1}, {"group_id": 2}, {"group_id": 99}],
            work_group=ENV.default_work_group,
        )
        assert result is None
        assert aio_cursor.rowcount == 3
        assert aio_cursor.description is None
        assert aio_cursor.result_set is None
        assert aio_cursor.query_id is None
        assert previous.is_closed

        await aio_cursor.executemany(operation, [])
        assert aio_cursor.rowcount == 0
        await aio_cursor.executemany(operation, [{"group_id": 99}, {"group_id": 100}])
        assert aio_cursor.rowcount == 0
        aio_cursor.close()
        assert aio_cursor.rowcount == -1

        async with aio_cursor.connection.cursor() as check_cursor:
            await check_cursor.execute(f"SELECT id, value FROM {executemany_table} ORDER BY id")
            assert await check_cursor.fetchall() == expected

    # A failure of the first statement reaches the same code; this case also shows
    # that the statements before the failure are not rolled back.
    @pytest.mark.parametrize("failure_index", [1])
    async def test_executemany_failure(self, aio_cursor, executemany_table, failure_index):
        operation = (
            f"UPDATE {executemany_table} SET value=value+1 "
            "WHERE group_id=CAST(%(group_id)s AS INTEGER)"
        )
        parameters = [{"group_id": "1"}] * failure_index + [
            {"group_id": "invalid"},
            {"group_id": "2"},
        ]
        with pytest.raises(OperationalError):
            await aio_cursor.executemany(operation, parameters)
        assert aio_cursor.rowcount == -1
        assert aio_cursor.description is None
        assert aio_cursor.result_set is None
        assert aio_cursor.query_id

        await aio_cursor.execute(f"SELECT id, value FROM {executemany_table} ORDER BY id")
        assert await aio_cursor.fetchall() == [
            (1, 10 + failure_index),
            (2, 20 + failure_index),
            (3, 30),
        ]
        await aio_cursor.execute(operation, {"group_id": "2"})
        assert aio_cursor.rowcount == 1

    @pytest.mark.parametrize("aio_cursor", [{"kill_on_interrupt": False}], indirect=True)
    async def test_executemany_cancellation(self, aio_cursor, executemany_table):
        query_ids = []

        def on_start(query_id):
            query_ids.append(query_id)
            if len(query_ids) == 2:
                task.cancel()

        task = asyncio.create_task(
            aio_cursor.executemany(
                f"UPDATE {executemany_table} SET value=value+1 WHERE group_id=%(group_id)d",
                [{"group_id": 1}, {"group_id": 2}, {"group_id": 99}],
                on_start_query_execution=on_start,
            )
        )
        try:
            with pytest.raises(asyncio.CancelledError):
                await task
            assert len(query_ids) == 2
            assert aio_cursor.rowcount == -1
            assert aio_cursor.result_set is None
            assert aio_cursor.query_id == query_ids[-1]
            await aio_cursor.cancel()
        finally:
            # Wait for the submitted query to stop before the fixture drops its table.
            if query_ids:
                await aio_cursor._cancel(query_ids[-1])
                await aio_cursor._poll(query_ids[-1])

    async def test_aio_connect(self):
        from pyathena import aio_connect

        conn = await aio_connect(work_group=ENV.default_work_group)
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
            assert await cursor.fetchone() == (1,)
        conn.close()

    async def test_arraysize(self, aio_cursor):
        aio_cursor.arraysize = 5
        await aio_cursor.execute("SELECT * FROM many_rows LIMIT 20")
        actual = await aio_cursor.fetchmany()
        assert len(actual) == 5

    async def test_glue_request_runs_off_the_event_loop(self, monkeypatch):
        """The Glue client and request of a throttled lookup do not block the loop (no AWS)."""
        conn = placeholder_connection(AioConnection)
        throttle_metadata_api(conn.client, monkeypatch)
        threads = []

        def get_table(**kwargs):
            threads.append(("request", threading.get_ident()))
            return {"Table": {"Name": kwargs["Name"], "StorageDescriptor": {"Columns": []}}}

        glue = MagicMock()
        glue.get_table.side_effect = get_table

        def build_client(service_name, **kwargs):
            assert service_name == "glue"
            threads.append(("client", threading.get_ident()))
            return glue

        monkeypatch.setattr(conn.session, "client", build_client)

        assert (await conn.cursor().get_table_metadata("one_row")).name == "one_row"
        assert sorted(step for step, _ in threads) == ["client", "request"]
        assert threading.get_ident() not in [thread for _, thread in threads]

    async def test_throttled_metadata_reads_glue(self, aio_cursor, monkeypatch):
        def view(metadata):
            return (metadata.name, metadata.table_type, metadata.parameters)

        async def read():
            return (
                view(await aio_cursor.get_table_metadata("one_row")),
                sorted(view(m) for m in await aio_cursor.list_table_metadata()),
                ENV.schema in [d.name for d in await aio_cursor.list_databases("AwsDataCatalog")],
            )

        expected = await read()
        table, listing, has_schema = expected
        assert table[0] == "one_row"
        assert "one_row" in [name for name, _, _ in listing]
        assert has_schema
        calls = throttle_metadata_api(aio_cursor.connection.client, monkeypatch)

        # Glue reports each table as Athena does, and nothing is retried first.
        assert await read() == expected
        assert sorted(calls) == ["get_table_metadata", "list_databases", "list_table_metadata"]

    async def test_closed(self, aio_cursor):
        """GH-1207: a closed cursor rejects operations."""
        await aio_cursor.execute("SELECT * FROM one_row")
        aio_cursor.close()

        with pytest.raises(ProgrammingError):
            await aio_cursor.execute("SELECT * FROM one_row")
        with pytest.raises(ProgrammingError):
            await aio_cursor.executemany("SELECT %(x)d FROM one_row", [{"x": 1}])
        with pytest.raises(ProgrammingError):
            await aio_cursor.fetchone()
        with pytest.raises(ProgrammingError):
            await aio_cursor.fetchmany()
        with pytest.raises(ProgrammingError):
            await aio_cursor.fetchall()
        with pytest.raises(ProgrammingError):
            await anext(aio_cursor)
        with pytest.raises(ProgrammingError):
            await aio_cursor.cancel()
        with pytest.raises(ProgrammingError):
            await aio_cursor.list_databases(None)
        with pytest.raises(ProgrammingError):
            await aio_cursor.get_table_metadata("one_row")
        with pytest.raises(ProgrammingError):
            await aio_cursor.list_table_metadata()


class TestAioDictCursor:
    async def test_fetchone(self, aio_dict_cursor):
        await aio_dict_cursor.execute("SELECT * FROM one_row")
        assert await aio_dict_cursor.fetchone() == {"number_of_rows": 1}

    async def test_dict_type(self, aio_dict_cursor):
        class CustomResultSet(AthenaAioDictResultSet):
            pass

        class CustomDictCursor(AioDictCursor):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self._result_set_class = CustomResultSet

        async with aio_dict_cursor.connection.cursor(
            CustomDictCursor, dict_type=OrderedDict
        ) as ordered_cursor:
            await ordered_cursor.execute("SELECT * FROM one_row")
            assert isinstance(ordered_cursor.result_set, CustomResultSet)
            assert type(await ordered_cursor.fetchone()) is OrderedDict
        # dict_type of another cursor does not change the row type of this one.
        await aio_dict_cursor.execute("SELECT * FROM one_row")
        assert type(await aio_dict_cursor.fetchone()) is dict
