import asyncio
import re
import threading
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pyathena import BINARY, Binary, ExecuteOptions
from pyathena.aio.cursor import AioCursor
from pyathena.error import DatabaseError, OperationalError, ProgrammingError
from pyathena.glue import GlueMetadataClient
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet
from pyathena.util import RetryConfig
from tests import ENV
from tests.pyathena.aio.conftest import _aio_connect
from tests.pyathena.util import throttle_metadata_api


def _offline_cursor(kill_on_interrupt, final_state):
    """An AioCursor whose first status request blocks until the task is cancelled.

    Args:
        kill_on_interrupt: Whether the cursor cancels the query on cancellation.
        final_state: The state of the query after cancellation.

    Returns:
        The cursor, the mock of its cancellation request, and an event set when the
        first status request starts.
    """
    polling = asyncio.Event()

    async def get_query_execution(query_id):
        if not polling.is_set():
            polling.set()
            await asyncio.Event().wait()
        return MagicMock(state=final_state)

    cursor = AioCursor.__new__(AioCursor)  # bypass __init__ to avoid AWS calls
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


class TestAioCursor:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (b"", b""),
            (b"\x00\xff'\\%", b"\x00\xff'\\%"),
            (bytes(range(256)), bytes(range(256))),
            (bytearray(b"\x00\xff"), b"\x00\xff"),
            (memoryview(b"\x00\xff"), b"\x00\xff"),
            (Binary(bytearray(b"abc")), b"abc"),
            (None, None),
        ],
        ids=["empty", "special", "all_bytes", "bytearray", "memoryview", "dbapi_binary", "null"],
    )
    async def test_binary_parameter(self, aio_cursor, value, expected):
        await aio_cursor.execute("SELECT CAST(%(value)s AS VARBINARY)", {"value": value})
        assert await aio_cursor.fetchone() == (expected,)
        assert aio_cursor.description[0][1] == BINARY

    async def test_fetchone(self, aio_cursor):
        await aio_cursor.execute("SELECT * FROM one_row")
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
        await aio_cursor.execute("SELECT * FROM one_row")
        assert await aio_cursor.fetchall() == [(1,)]
        await aio_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        assert await aio_cursor.fetchall() == [(i,) for i in range(10000)]

    async def test_async_iterator(self, aio_cursor):
        await aio_cursor.execute("SELECT * FROM one_row")
        rows = [row async for row in aio_cursor]
        assert rows == [(1,)]

    async def test_execute_returns_self(self, aio_cursor):
        result = await aio_cursor.execute("SELECT * FROM one_row")
        assert result is aio_cursor

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

    async def test_execute_internal_legacy_kwargs_passthrough(self):
        """The pre-3.35 _execute() keywords are forwarded to the request (no AWS).

        Mirrors the synchronous cursor test (regression test for #734).
        """
        cursor = AioCursor.__new__(AioCursor)  # bypass __init__ to avoid AWS calls
        cursor._connection = MagicMock()
        cursor._connection.client.start_query_execution.return_value = {
            "QueryExecutionId": "test_query_id"
        }
        cursor._retry_config = RetryConfig()

        with (
            patch.object(
                AioCursor, "_build_start_query_execution_request", return_value={}
            ) as request_mock,
            patch.object(
                AioCursor, "_find_previous_query_id", new_callable=AsyncMock, return_value=None
            ) as cache_mock,
        ):
            query_id = await cursor._execute(
                "SELECT 1",
                parameters=None,
                work_group="test_work_group",
                s3_staging_dir="s3://test-bucket/path/",
                cache_size=10,
                cache_expiration_time=100,
                result_reuse_enable=True,
                result_reuse_minutes=5,
                paramstyle="qmark",
            )

        assert query_id == "test_query_id"
        request_mock.assert_called_once_with(
            query="SELECT 1",
            work_group="test_work_group",
            s3_staging_dir="s3://test-bucket/path/",
            result_reuse_enable=True,
            result_reuse_minutes=5,
            execution_parameters=None,
        )
        cache_mock.assert_awaited_once_with(
            "SELECT 1",
            "test_work_group",
            cache_size=10,
            cache_expiration_time=100,
        )

    @pytest.mark.parametrize(
        "final_state",
        [AthenaQueryExecution.STATE_CANCELLED, AthenaQueryExecution.STATE_SUCCEEDED],
    )
    async def test_execute_kill_on_interrupt(self, final_state):
        """Task cancellation cancels the query, waits for it, and is re-raised (no AWS)."""
        cursor, cancel, polling = _offline_cursor(kill_on_interrupt=True, final_state=final_state)
        task = asyncio.create_task(cursor.execute("SELECT 1"))
        await polling.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert task.cancelled()
        cancel.assert_awaited_once_with("query_id")
        assert cursor.query_id == "query_id"
        assert cursor.result_set is None

    async def test_execute_kill_on_interrupt_timeout(self):
        """A timeout cancels the query and raises TimeoutError (no AWS)."""
        cursor, cancel, _ = _offline_cursor(
            kill_on_interrupt=True, final_state=AthenaQueryExecution.STATE_CANCELLED
        )
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(cursor.execute("SELECT 1"), timeout=0.01)

        cancel.assert_awaited_once_with("query_id")

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

    async def test_cache_size_different_schema(self):
        """A cached result is only reused when it ran against the same schema (#739).

        Mirrors the synchronous cursor test: identical SQL can resolve to different
        tables depending on the database it runs against, so a prior execution from
        another schema must not be a cache hit.
        """
        query = "SELECT * FROM one_row"

        def execution(schema):
            return AthenaQueryExecution(
                {
                    "QueryExecution": {
                        "QueryExecutionId": f"query_id_{schema}",
                        "Query": query,
                        "StatementType": AthenaQueryExecution.STATEMENT_TYPE_DML,
                        "QueryExecutionContext": {"Database": schema},
                        "Status": {
                            "State": AthenaQueryExecution.STATE_SUCCEEDED,
                            "CompletionDateTime": datetime.now(timezone.utc),
                        },
                    }
                }
            )

        cursor = AioCursor.__new__(AioCursor)  # bypass __init__ to avoid AWS calls
        cursor._catalog_name = None

        with patch.object(
            AioCursor,
            "_list_query_executions",
            new_callable=AsyncMock,
            return_value=(None, [execution("other_schema")]),
        ):
            cursor._schema_name = "this_schema"
            assert await cursor._find_previous_query_id(query, None, cache_size=100) is None
            cursor._schema_name = "other_schema"
            assert (
                await cursor._find_previous_query_id(query, None, cache_size=100)
                == "query_id_other_schema"
            )

    async def test_cache_size_different_catalog(self):
        query = "SELECT * FROM one_row"
        schema = "this_schema"

        def execution(catalog):
            return AthenaQueryExecution(
                {
                    "QueryExecution": {
                        "QueryExecutionId": f"query_id_{catalog}",
                        "Query": query,
                        "StatementType": AthenaQueryExecution.STATEMENT_TYPE_DML,
                        "QueryExecutionContext": {"Database": schema, "Catalog": catalog},
                        "Status": {
                            "State": AthenaQueryExecution.STATE_SUCCEEDED,
                            "CompletionDateTime": datetime.now(timezone.utc),
                        },
                    }
                }
            )

        cursor = AioCursor.__new__(AioCursor)
        cursor._schema_name = schema

        with patch.object(
            AioCursor,
            "_list_query_executions",
            new_callable=AsyncMock,
            return_value=(None, [execution("awsdatacatalog")]),
        ):
            # A different catalog must not be a cache hit.
            cursor._catalog_name = "other_catalog"
            assert await cursor._find_previous_query_id(query, None, cache_size=100) is None
            # The same catalog, differing only in case, must still be a cache hit.
            cursor._catalog_name = "AwsDataCatalog"
            assert (
                await cursor._find_previous_query_id(query, None, cache_size=100)
                == "query_id_awsdatacatalog"
            )

    async def test_no_result_set_raises(self, aio_cursor):
        with pytest.raises(ProgrammingError):
            await aio_cursor.fetchone()
        with pytest.raises(ProgrammingError):
            await aio_cursor.fetchmany()
        with pytest.raises(ProgrammingError):
            await aio_cursor.fetchall()

    async def test_description(self, aio_cursor):
        await aio_cursor.execute("SELECT 1 AS foobar FROM one_row")
        assert await aio_cursor.fetchall() == [(1,)]
        assert aio_cursor.description == [("foobar", "integer", None, None, 10, 0, "UNKNOWN")]

    async def test_description_initial(self, aio_cursor):
        assert aio_cursor.description is None

    async def test_bad_query(self, aio_cursor):
        with pytest.raises(DatabaseError):
            await aio_cursor.execute("SELECT does_not_exist FROM this_really_does_not_exist")

    async def test_query_id(self, aio_cursor):
        assert aio_cursor.query_id is None
        await aio_cursor.execute("SELECT * FROM one_row")
        expected_pattern = (
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
        )
        assert re.match(expected_pattern, aio_cursor.query_id)

    async def test_query_execution_initial(self, aio_cursor):
        assert not aio_cursor.has_result_set
        assert aio_cursor.rownumber is None
        assert aio_cursor.rowcount == -1
        assert aio_cursor.database is None
        assert aio_cursor.catalog is None
        assert aio_cursor.query_id is None
        assert aio_cursor.query is None
        assert aio_cursor.statement_type is None
        assert aio_cursor.work_group is None
        assert aio_cursor.state is None
        assert aio_cursor.state_change_reason is None
        assert aio_cursor.submission_date_time is None
        assert aio_cursor.completion_date_time is None
        assert aio_cursor.data_scanned_in_bytes is None
        assert aio_cursor.engine_execution_time_in_millis is None
        assert aio_cursor.query_queue_time_in_millis is None
        assert aio_cursor.total_execution_time_in_millis is None
        assert aio_cursor.query_planning_time_in_millis is None
        assert aio_cursor.service_processing_time_in_millis is None
        assert aio_cursor.output_location is None
        assert aio_cursor.data_manifest_location is None
        assert aio_cursor.encryption_option is None
        assert aio_cursor.kms_key is None
        assert aio_cursor.selected_engine_version is None
        assert aio_cursor.effective_engine_version is None

    async def test_cancel_initial(self, aio_cursor):
        with pytest.raises(ProgrammingError):
            await aio_cursor.cancel()

    async def test_executemany(self, aio_cursor):
        rows = [(1, "foo"), (2, "bar"), (3, "jim o'rourke")]
        await aio_cursor.executemany(
            "INSERT INTO execute_many_aio (a, b) VALUES (%(a)d, %(b)s)",
            [{"a": a, "b": b} for a, b in rows],
        )
        assert aio_cursor.rowcount == len(rows)
        await aio_cursor.execute("SELECT * FROM execute_many_aio")
        assert sorted(await aio_cursor.fetchall()) == list(rows)

    @pytest.mark.parametrize(
        ("operation", "expected"),
        [
            pytest.param(
                "INSERT INTO {table} SELECT id+10, group_id+10, value "
                "FROM {table} WHERE group_id=%(group_id)d",
                [(1, 10), (2, 20), (3, 30), (11, 10), (12, 20), (13, 30)],
                id="insert",
            ),
            pytest.param(
                "UPDATE {table} SET value=value+1 WHERE group_id=%(group_id)d",
                [(1, 11), (2, 21), (3, 31)],
                id="update",
            ),
            pytest.param("DELETE FROM {table} WHERE group_id=%(group_id)d", [], id="delete"),
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

        await aio_cursor.execute(f"SELECT id, value FROM {executemany_table} ORDER BY id")
        assert aio_cursor.rowcount == -1
        assert await aio_cursor.fetchall() == expected

    @pytest.mark.parametrize("failure_index", [0, 1])
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

    async def test_executemany_parameter_iteration_failure(self, aio_cursor, executemany_table):
        previous = None
        query_id = None

        def parameters():
            nonlocal previous, query_id
            yield {"group_id": 1}
            previous = aio_cursor.result_set
            query_id = aio_cursor.query_id
            raise ValueError("invalid parameters")

        with pytest.raises(ValueError, match="invalid parameters"):
            await aio_cursor.executemany(
                f"UPDATE {executemany_table} SET value=value+1 WHERE group_id=%(group_id)d",
                parameters(),
            )
        assert aio_cursor.rowcount == -1
        assert aio_cursor.result_set is None
        assert query_id is not None
        assert aio_cursor.query_id == query_id
        assert previous is not None
        assert previous.is_closed
        await aio_cursor.execute(f"SELECT id, value FROM {executemany_table} ORDER BY id")
        assert await aio_cursor.fetchall() == [(1, 11), (2, 21), (3, 30)]

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

    async def test_executemany_fetch(self, aio_cursor):
        await aio_cursor.executemany("SELECT %(x)d FROM one_row", [])
        assert aio_cursor.rowcount == 0
        await aio_cursor.executemany("SELECT %(x)d FROM one_row", [{"x": i} for i in range(1, 2)])
        assert aio_cursor.rowcount == -1
        with pytest.raises(ProgrammingError):
            await aio_cursor.fetchall()
        with pytest.raises(ProgrammingError):
            await aio_cursor.fetchmany()
        with pytest.raises(ProgrammingError):
            await aio_cursor.fetchone()

    async def test_context_manager(self):
        conn = await _aio_connect(schema_name=ENV.schema)
        try:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT * FROM one_row")
                assert await cursor.fetchone() == (1,)
        finally:
            conn.close()

    async def test_open_close(self):
        conn = await _aio_connect()
        conn.close()

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

    async def test_arraysize_default(self, aio_cursor):
        assert aio_cursor.arraysize == AthenaResultSet.DEFAULT_FETCH_SIZE

    async def test_invalid_arraysize(self, aio_cursor):
        with pytest.raises(ProgrammingError):
            aio_cursor.arraysize = 10000
        with pytest.raises(ProgrammingError):
            aio_cursor.arraysize = -1

    async def test_glue_request_runs_off_the_event_loop(self, aio_cursor, monkeypatch):
        throttle_metadata_api(aio_cursor.connection.client, monkeypatch)
        loop_thread = threading.get_ident()
        threads = []
        # Record where the real client is built and the real request is sent.
        client = GlueMetadataClient.client
        get_table = GlueMetadataClient.get_table

        def recorded_client(self):
            threads.append(("client", threading.get_ident()))
            return client.fget(self)

        def recorded_get_table(self, *args, **kwargs):
            threads.append(("request", threading.get_ident()))
            return get_table(self, *args, **kwargs)

        monkeypatch.setattr(GlueMetadataClient, "client", property(recorded_client))
        monkeypatch.setattr(GlueMetadataClient, "get_table", recorded_get_table)

        assert (await aio_cursor.get_table_metadata("one_row")).name == "one_row"
        # Building the client and sending the request would block the loop.
        assert [step for step, _ in threads] == ["request", "client"]
        assert all(thread != loop_thread for _, thread in threads)

    async def test_list_databases(self, aio_cursor):
        databases = await aio_cursor.list_databases(catalog_name="AwsDataCatalog")
        assert len(databases) > 0
        database_names = [db.name for db in databases]
        assert "default" in database_names

    async def test_get_table_metadata(self, aio_cursor):
        metadata = await aio_cursor.get_table_metadata(table_name="one_row")
        assert metadata.name == "one_row"
        assert metadata.table_type

    async def test_list_table_metadata(self, aio_cursor):
        metadata_list = await aio_cursor.list_table_metadata()
        assert len(metadata_list) > 0
        table_names = [m.name for m in metadata_list]
        assert "one_row" in table_names

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
        calls = throttle_metadata_api(aio_cursor.connection.client, monkeypatch)

        # The Glue request runs in a worker thread, as the Athena calls do.
        assert await read() == expected
        assert sorted(calls) == ["get_table_metadata", "list_databases", "list_table_metadata"]


class TestAioDictCursor:
    async def test_fetchone(self, aio_dict_cursor):
        await aio_dict_cursor.execute("SELECT * FROM one_row")
        assert await aio_dict_cursor.fetchone() == {"number_of_rows": 1}

    async def test_fetchmany(self, aio_dict_cursor):
        await aio_dict_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        actual1 = await aio_dict_cursor.fetchmany(10)
        assert len(actual1) == 10
        assert actual1 == [{"a": i} for i in range(10)]
        actual2 = await aio_dict_cursor.fetchmany(10)
        assert len(actual2) == 5
        assert actual2 == [{"a": i} for i in range(10, 15)]

    async def test_fetchall(self, aio_dict_cursor):
        await aio_dict_cursor.execute("SELECT * FROM one_row")
        assert await aio_dict_cursor.fetchall() == [{"number_of_rows": 1}]
        await aio_dict_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        assert await aio_dict_cursor.fetchall() == [{"a": i} for i in range(10000)]
