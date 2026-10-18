# Copyright 2022 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import asyncio
import io
import threading
import time
from concurrent.futures import Future, wait
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.implementations.local import LocalFileSystem
from jinja2 import Environment, FileSystemLoader
from sqlalchemy import cast, create_engine, engine_from_config, literal, types

from pyathena.connection import Connection
from pyathena.error import ProgrammingError
from pyathena.filesystem.s3 import S3FileSystem
from pyathena.filesystem.s3_async import AioS3FileSystem
from pyathena.filesystem.s3_core import S3Core
from pyathena.formatter import DefaultParameterFormatter
from pyathena.glue import GlueMetadataClient
from pyathena.model import AthenaCalculationExecutionStatus, AthenaQueryExecution
from pyathena.spark.common import SparkBaseCursor
from pyathena.util import RetryConfig, override, retry_api_call

_queries = Environment(
    loader=FileSystemLoader(Path(__file__).parents[1].resolve() / "resources" / "queries")
)


# Seconds a test waits for an event from a helper thread before failing.
EVENT_TIMEOUT = 10


def get_query_results_responses(*pages):
    """Build stubbed ``GetQueryResults`` responses of an integer column ``a``.

    Args:
        *pages: The values of the column, one sequence per page, or an exception,
            including a ``BaseException`` such as ``KeyboardInterrupt``, that the
            request for the page raises.

    Returns:
        The responses, the first with the result set metadata and each but the last
        with a ``NextToken``.
    """
    responses = []
    for i, page in enumerate(pages):
        if isinstance(page, BaseException):
            responses.append(page)
            continue
        response = {"ResultSet": {"Rows": [{"Data": [{"VarCharValue": v}]} for v in page]}}
        if i == 0:
            response["ResultSet"]["ResultSetMetadata"] = {
                "ColumnInfo": [{"Name": "a", "Type": "integer"}]
            }
        if i + 1 < len(pages):
            response["NextToken"] = f"token{i + 1}"
        responses.append(response)
    return responses


def assert_fetches_fail(result_set):
    """Assert that each fetch of a result set raises for an earlier failed fetch.

    Args:
        result_set: The synchronous result set.
    """
    for fetch in (result_set.fetchone, result_set.fetchmany, result_set.fetchall):
        with pytest.raises(ProgrammingError, match=r"cannot be fetched after a failed fetch\."):
            fetch()


def throttling_error(operation_name="GetQueryResults"):
    """Build the ``ThrottlingException`` error of an AWS API operation.

    Args:
        operation_name: The name of the operation.

    Returns:
        The error.
    """
    return ClientError(
        {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}}, operation_name
    )


class ThrottledOperation:
    """A stubbed AWS API operation that throttles, signalling its first attempt.

    The first attempt waits for ``release`` if ``block`` is set, so that a test
    can stop the retries during the request.
    """

    def __init__(self, block=False, failures=None, operation_name="GetQueryResults"):
        """Initialize the operation.

        Args:
            block: Whether the first attempt waits for ``release``.
            failures: The number of attempts that throttle before one succeeds;
                all of them if None.
            operation_name: The name of the operation in its error.
        """
        self.block = block
        self.failures = failures
        self.operation_name = operation_name
        self.started = threading.Event()
        self.release = threading.Event()
        self.calls = 0

    def __call__(self, *args, **kwargs):
        self.calls += 1
        if self.calls == 1:
            self.started.set()
            if self.block:
                assert self.release.wait(EVENT_TIMEOUT)
        if self.failures is None or self.calls <= self.failures:
            raise throttling_error(self.operation_name)
        return "success"


def retried(operation, config):
    """Return a stub of an ``S3Core`` request method that retries an operation.

    The stub sends the operation with :func:`~pyathena.util.retry_api_call`
    in the thread that calls it, as ``S3Core.call()`` does, so that the stop
    scopes of that thread reach its retries.

    Args:
        operation: The stubbed AWS API operation, such as a
            :class:`ThrottledOperation`.
        config: The retry policy.

    Returns:
        The stub, which accepts and ignores any arguments.
    """

    def request(*args, **kwargs):
        return retry_api_call(operation, config)

    return request


class StoppingOperation:
    """A stubbed AWS API operation that always throttles and stops its retries.

    The retries are stopped during the first request, or from another thread
    during the backoff wait after it.
    """

    def __init__(self, stop, stop_in, operation_name="GetQueryResults"):
        """Initialize the operation.

        Args:
            stop: Stops the retries; called once.
            stop_in: "request" to stop during the first request, "backoff" to
                stop from another thread 0.1 seconds into the backoff wait.
            operation_name: The name of the operation in its error.
        """
        self.stop = stop
        self.stop_in = stop_in
        self.error = throttling_error(operation_name)
        self.calls = 0
        self._stopper = threading.Timer(0.1, stop)

    def __call__(self, *args, **kwargs):
        if self.calls == 0:
            if self.stop_in == "request":
                self.stop()
            else:
                self._stopper.start()
        self.calls += 1
        raise self.error

    def join(self):
        """Wait for the thread that stops the retries during the backoff wait, if any."""
        if self._stopper.ident is not None:
            self._stopper.join()


async def cancel_when_started(task, started, release):
    """Cancel a task once the blocking call that it waits for has started.

    The call is released after the cancellation has reached the task.

    Args:
        task: The task.
        started: The event that the call sets when it starts.
        release: The event that the call waits for before it returns.
    """
    # Polled, since the default executor may have no free thread.
    await wait_until(started.is_set)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release.set()


async def wait_until(predicate, timeout=EVENT_TIMEOUT):
    """Wait, without blocking the event loop, until a condition holds.

    Args:
        predicate: The condition.
        timeout: The longest wait, in seconds.
    """
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline
        await asyncio.sleep(0.01)


async def await_bounded(awaitable, timeout=EVENT_TIMEOUT):
    """Await an awaitable, failing if it is not done within the timeout.

    Unlike ``asyncio.wait_for``, it does not wait for the cancellation of an
    awaitable that the timeout interrupts, which a regression could also
    hold, so that the test fails instead of hanging.

    Args:
        awaitable: The coroutine or future to await.
        timeout: The longest wait, in seconds.

    Returns:
        The result of the awaitable.
    """
    task = asyncio.ensure_future(awaitable)
    done, _ = await asyncio.wait({task}, timeout=timeout)
    assert done, f"not done within {timeout} s"
    return task.result()


def run_bounded(main, timeout=EVENT_TIMEOUT):
    """Run a coroutine with ``asyncio.run()``, failing if it does not end within the timeout.

    The event loop runs in a daemon thread, so that a shutdown of the loop
    that waits for a task that a regression blocks fails the test instead of
    hanging it, and does not keep the interpreter from exiting.

    Args:
        main: The coroutine function to run.
        timeout: The longest run, in seconds.

    Returns:
        The result of the coroutine.

    Raises:
        BaseException: The error that the run raised.
    """
    outcome = {}

    def run():
        try:
            outcome["result"] = asyncio.run(main())
        except BaseException as e:
            outcome["error"] = e

    runner = threading.Thread(target=run, daemon=True)
    runner.start()
    runner.join(timeout)
    assert not runner.is_alive(), f"asyncio.run() did not end within {timeout} s"
    if "error" in outcome:
        raise outcome["error"]
    return outcome.get("result")


def shutdown_bounded(pool, timeout=EVENT_TIMEOUT):
    """Shut down an executor and wait for its calls, failing if they run past the timeout.

    The wait runs in a daemon thread, so that a call that a regression blocks
    fails the test instead of hanging it, and does not keep the interpreter
    from exiting through this wait.

    Args:
        pool: The executor, such as an ``S3AioThreadPool``.
        timeout: The longest wait, in seconds.

    Raises:
        BaseException: The error that the shutdown raised.
    """
    errors = []

    def shutdown():
        try:
            pool.shutdown(wait=True)
        except BaseException as e:
            errors.append(e)

    waiter = threading.Thread(target=shutdown, daemon=True)
    waiter.start()
    waiter.join(timeout)
    assert not waiter.is_alive(), f"the calls of the executor did not end within {timeout} s"
    if errors:
        raise errors[0]


def overlapping_calls(calls, timeout=5, grace=0.5):
    """Return a callable that records the peak number of its overlapping calls.

    Until ``calls`` calls have overlapped, each call waits for that, or until
    the timeout. The calls of that first overlap then wait up to ``grace``
    seconds for another overlapping call, which only a caller that runs more
    than ``calls`` at once makes, so that the peak also shows an exceeded
    bound. Later calls do not wait.

    Args:
        calls: The number of calls to wait for.
        timeout: The longest wait of a call for the overlap, in seconds.
        grace: The longest wait of a call of the first overlap for one more,
            in seconds.

    Returns:
        The callable, which accepts and ignores any arguments, and a
        dictionary whose ``peak`` item is the peak number.
    """
    condition = threading.Condition()
    state = {"active": 0, "peak": 0, "released": False}

    def call(*args, **kwargs):
        with condition:
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
            condition.notify_all()
            # The peak, unlike the active calls, stays reached when the last
            # call returns before the others wake up.
            if not state["released"] and condition.wait_for(
                lambda: state["peak"] >= calls, timeout=timeout
            ):
                condition.wait_for(lambda: state["peak"] > calls, timeout=grace)
                state["released"] = True
        with condition:
            state["active"] -= 1

    return call, state


def read_query(name, **kwargs):
    template = _queries.get_template(name)
    return [q.strip() for q in template.render(**kwargs).split(";") if q and q.strip()]


def cached_file_systems(connection):
    """Return the filesystems in the fsspec instance cache that hold the connection."""
    return [
        fs
        for cls in (S3FileSystem, AioS3FileSystem)
        for fs in cls._cache.values()
        if fs.storage_options.get("connection") is connection
    ]


METADATA_OPERATIONS = ("get_table_metadata", "list_table_metadata", "list_databases")


def throttle_metadata_api(
    client,
    monkeypatch,
    code="ThrottlingException",
    message="Rate exceeded",
    operations=METADATA_OPERATIONS,
):
    """Make the Athena client's metadata requests fail with ``code``; return the call list."""
    calls = []

    def failing(operation):
        def fail(**kwargs):
            calls.append(operation)
            raise ClientError({"Error": {"Code": code, "Message": message}}, operation)

        return fail

    for operation in operations:
        monkeypatch.setattr(client, operation, failing(operation))
    return calls


def succeeded_query_execution_data(query_id, query, completion_date_time, schema="this_schema"):
    """Build a succeeded DML query execution as ``BatchGetQueryExecution`` returns it.

    Args:
        query_id: The query execution ID.
        query: The query string.
        completion_date_time: The completion time.
        schema: The database the query ran against.

    Returns:
        The ``QueryExecution`` dictionary.
    """
    return {
        "QueryExecutionId": query_id,
        "Query": query,
        "StatementType": AthenaQueryExecution.STATEMENT_TYPE_DML,
        "QueryExecutionContext": {"Database": schema},
        "Status": {
            "State": AthenaQueryExecution.STATE_SUCCEEDED,
            "CompletionDateTime": completion_date_time,
        },
    }


def succeeded_query_execution(query_id, query, completion_date_time, schema="this_schema"):
    """Build a succeeded DML query execution, as the result cache search lists it.

    Args:
        query_id: The query execution ID.
        query: The query string.
        completion_date_time: The completion time.
        schema: The database the query ran against.

    Returns:
        The query execution.
    """
    return AthenaQueryExecution(
        {
            "QueryExecution": succeeded_query_execution_data(
                query_id, query, completion_date_time, schema
            )
        }
    )


def cache_search_cursor(cursor_class, client):
    """Build a cursor whose result cache search sends its requests to a client stand-in.

    Args:
        cursor_class: The cursor class, such as ``Cursor`` or ``AioCursor``.
        client: The Athena client stand-in, such as ``QueryExecutionHistory``.

    Returns:
        The cursor, for the schema ``this_schema``.
    """
    cursor = cursor_class.__new__(cursor_class)  # bypass __init__ to avoid AWS calls
    cursor._connection = MagicMock(_client=client)
    cursor._retry_config = RetryConfig(attempt=1)
    cursor._work_group = None
    cursor._schema_name = "this_schema"
    cursor._catalog_name = None
    return cursor


# Athena's answers to a query or calculation that fails at its first status request,
# after a search that finds no earlier executions.
FAILED_EXECUTION_RESPONSES = {
    "ListQueryExecutions": {"QueryExecutionIds": []},
    "StartQueryExecution": {"QueryExecutionId": "query-id"},
    "GetQueryExecution": {
        "QueryExecution": {
            "QueryExecutionId": "query-id",
            "Query": "SELECT 1",
            "StatementType": "DML",
            "Status": {"State": "FAILED", "StateChangeReason": "failed"},
        }
    },
    "StartCalculationExecution": {"CalculationExecutionId": "calculation-id", "State": "CREATING"},
    "GetCalculationExecutionStatus": {
        "Status": {"State": "FAILED"},
        "Statistics": {"DpuExecutionInMillis": 0},
    },
    "GetCalculationExecution": {
        "CalculationExecutionId": "calculation-id",
        "SessionId": "session",
        "Status": {"State": "FAILED", "StateChangeReason": "failed"},
        "Statistics": {"DpuExecutionInMillis": 0},
        "Result": {},
    },
}


# Athena's answers to a calculation that completes at its first status request.
COMPLETED_CALCULATION_RESPONSES = {
    **FAILED_EXECUTION_RESPONSES,
    "GetCalculationExecutionStatus": {
        "Status": {"State": "COMPLETED"},
        "Statistics": {"DpuExecutionInMillis": 0},
    },
    "GetCalculationExecution": {
        "CalculationExecutionId": "calculation-id",
        "SessionId": "session",
        "Status": {"State": "COMPLETED"},
        "Statistics": {"DpuExecutionInMillis": 0},
        "Result": {},
    },
}


def placeholder_connection(connection_class=Connection):
    """Build a connection with placeholder settings and credentials.

    Building it sends no request to AWS.

    Args:
        connection_class: The connection class, such as ``AioConnection``.

    Returns:
        The connection.
    """
    return connection_class(
        region_name="us-east-1",
        s3_staging_dir="s3://bucket/path/",
        aws_access_key_id="access_key",
        aws_secret_access_key="secret_key",
    )


def assert_closes_result_sets_after_close(cursor_class):
    """Check that an AsyncCursor closes only the result sets created after it closes.

    The cursor sends no request to AWS: its queries start without one, and
    its result sets are created, one at a time, when the test releases them.

    Args:
        cursor_class: The AsyncCursor class.
    """
    cursor = placeholder_connection().cursor(cursor_class, max_workers=1)
    release = threading.Event()
    result_sets = []

    def collect(*args, **kwargs):
        assert release.wait(EVENT_TIMEOUT)
        result_sets.append(MagicMock())
        return result_sets[-1]

    with (
        patch.object(cursor, "_execute", return_value="query_id"),
        patch.object(cursor, "_collect_result_set", side_effect=collect),
    ):
        try:
            _, before = cursor.execute("SELECT 1")
            release.set()
            taken = before.result(EVENT_TIMEOUT)
            release.clear()
            _, after = cursor.execute("SELECT 1")
            cursor.close()
            release.set()
            assert after.result(EVENT_TIMEOUT) is result_sets[1]
        finally:
            release.set()
            cursor.close(wait=True)
    result_sets[1].close.assert_called_once_with()
    taken.close.assert_not_called()


def assert_close_stops_waiting_futures(cursor_class):
    """Check that closing an AsyncCursor stops the futures that wait for a query.

    The cursor sends no request to AWS. First, its query stays running, and it
    waits a minute between status requests, so that the futures of
    ``execute()``, ``description()``, and ``poll()`` end within
    ``EVENT_TIMEOUT`` only because the close ends their wait; a ``cancel()``
    queued behind them still runs. Then the cursor closes during the status
    request that reports the query finished, and each future still raises
    instead of building a result set from the placeholder execution.

    Args:
        cursor_class: The AsyncCursor class.
    """
    cursor = placeholder_connection().cursor(cursor_class, poll_interval=60, max_workers=3)
    # The three futures and the test; a second status request would time out here.
    polled = threading.Barrier(4)

    def running(query_id):
        polled.wait(EVENT_TIMEOUT)
        return SimpleNamespace(state=AthenaQueryExecution.STATE_RUNNING)

    with (
        patch.object(cursor, "_execute", return_value="query_id"),
        patch.object(cursor, "_get_query_execution", side_effect=running) as status,
        patch.object(cursor, "_cancel") as cancel,
    ):
        try:
            futures = [
                cursor.execute("SELECT 1")[1],
                cursor.description("query_id"),
                cursor.poll("query_id"),
            ]
            polled.wait(EVENT_TIMEOUT)
            cancelled = cursor.cancel("query_id")
            cursor.close()
            for future in futures:
                with pytest.raises(ProgrammingError, match=r"Cursor is closed\."):
                    future.result(EVENT_TIMEOUT)
            cancelled.result(EVENT_TIMEOUT)
        finally:
            polled.abort()
            cursor.close(wait=True)
    assert status.call_count == 3
    cancel.assert_called_once_with("query_id")

    for submit in (
        lambda cursor: cursor.execute("SELECT 1")[1],
        lambda cursor: cursor.description("query_id"),
        lambda cursor: cursor.poll("query_id"),
    ):
        cursor = placeholder_connection().cursor(cursor_class)

        def finished(query_id, cursor=cursor):
            cursor.close()
            return SimpleNamespace(state=AthenaQueryExecution.STATE_SUCCEEDED)

        with (
            patch.object(cursor, "_execute", return_value="query_id"),
            patch.object(cursor, "_get_query_execution", side_effect=finished),
        ):
            future = submit(cursor)
            with pytest.raises(ProgrammingError, match=r"Cursor is closed\."):
                future.result(EVENT_TIMEOUT)


def assert_description_reads_only_metadata(cursor_class):
    """Check that ``description()`` of an AsyncCursor reads only the result metadata.

    For a CSV result, the future requests one row of ``GetQueryResults`` and
    reads no result file: the connection sends no other request, and the
    placeholder credentials would fail one to S3.

    Args:
        cursor_class: The AsyncCursor class.
    """
    cursor = placeholder_connection().cursor(cursor_class)
    responses = {
        **FAILED_EXECUTION_RESPONSES,
        "GetQueryExecution": {
            "QueryExecution": {
                "QueryExecutionId": "query-id",
                "Query": "SELECT 1",
                "StatementType": "DML",
                "Status": {"State": "SUCCEEDED"},
                "ResultConfiguration": {"OutputLocation": "s3://bucket/path/query-id.csv"},
            }
        },
        "GetQueryResults": {
            "ResultSet": {
                "Rows": [{"Data": [{"VarCharValue": "a"}]}, {"Data": [{"VarCharValue": "1"}]}],
                "ResultSetMetadata": {
                    "ColumnInfo": [
                        {
                            "Name": "a",
                            "Type": "integer",
                            "Precision": 10,
                            "Scale": 0,
                            "Nullable": "UNKNOWN",
                        }
                    ]
                },
            }
        },
    }
    try:
        with answered_requests(cursor.connection.client, responses) as requests:
            description = cursor.description("query-id").result(EVENT_TIMEOUT)
    finally:
        cursor.close(wait=True)
    assert description == [("a", "integer", None, None, 10, 0, "UNKNOWN")]
    assert [operation for operation, _ in requests] == ["GetQueryExecution", "GetQueryResults"]
    assert requests[1][1]["MaxResults"] == 1


def offline_spark_cursor(cursor_class):
    """Build a Spark cursor on a connection that sends no request to AWS on its own.

    The cursor reports a started session, ``session``, which closing it leaves
    alone.

    Args:
        cursor_class: The Spark cursor class.

    Returns:
        The cursor.
    """
    conn = placeholder_connection()
    with patch.object(SparkBaseCursor, "_start_session", return_value="session"):
        return conn.cursor(cursor_class, terminate_session_on_close=False)


@contextmanager
def answered_requests(client, responses=FAILED_EXECUTION_RESPONSES):
    """Answer each request of a boto3 client once, without AWS, and record them.

    A repeated request raises ``AssertionError``, so that a cursor that keeps
    polling stops instead of waiting for ever, and the block then raises it too,
    also when the cursor caught it.

    Args:
        client: The boto3 client.
        responses: The response to each operation name.

    Yields:
        The operation name and parameters of each request, in order.
    """
    requests = []
    repeated = []

    def answer(operation, parameters):
        if any(operation == requested for requested, _ in requests):
            repeated.append(operation)
            raise AssertionError(f"repeated {operation} request")
        requests.append((operation, parameters))
        return responses[operation]

    with patch.object(client, "_make_api_call", side_effect=answer):
        yield requests
    assert not repeated, f"repeated requests: {repeated}"


class QueryExecutionHistory:
    """An Athena client stand-in that lists the query executions of a work group.

    ``ListQueryExecutions`` returns up to ``MaxResults`` IDs per page in the given
    order, latest first as Athena lists them, and ``BatchGetQueryExecution``
    returns their executions.

    Attributes:
        executions: The ``QueryExecution`` dictionaries, latest first.
        list_requests: The parameters of each ``ListQueryExecutions`` request.
    """

    def __init__(self, executions):
        """Initialize the history.

        Args:
            executions: The ``QueryExecution`` dictionaries, latest first.
        """
        self.executions = executions
        self.list_requests = []

    def list_query_executions(self, **request):
        """List a page of query execution IDs.

        Args:
            **request: The request parameters: ``MaxResults``, the ``NextToken``
                of a later page, and others such as ``WorkGroup``.

        Returns:
            The response, with a ``NextToken`` unless the page is the last.
        """
        self.list_requests.append(request)
        # ListQueryExecutions rejects a MaxResults above 50.
        if request["MaxResults"] > 50:
            raise ClientError(
                {"Error": {"Code": "InvalidRequestException", "Message": "MaxResults"}},
                "ListQueryExecutions",
            )
        start = int(request.get("NextToken", 0))
        end = start + request["MaxResults"]
        response = {
            "QueryExecutionIds": [e["QueryExecutionId"] for e in self.executions[start:end]]
        }
        if end < len(self.executions):
            response["NextToken"] = str(end)
        return response

    def batch_get_query_execution(self, **request):
        """Get the executions of query execution IDs.

        Args:
            **request: The request parameters, with ``QueryExecutionIds``.

        Returns:
            The response.
        """
        executions = {e["QueryExecutionId"]: e for e in self.executions}
        return {"QueryExecutions": [executions[i] for i in request["QueryExecutionIds"]]}


def unreachable_glue(connection):
    """A Glue client for the connection that sends requests to a closed proxy port."""
    return GlueMetadataClient(
        connection.session,
        connection.region_name,
        Config(
            proxies={"https": "http://127.0.0.1:9"},
            connect_timeout=1,
            retries={"mode": "standard", "max_attempts": 1},
        ),
        {},
    )


# TIME values of several precisions, with and without a time zone, TIMESTAMP WITH TIME
# ZONE values with UTC offsets and a zone name, NULL JSON and TIME values, and the row
# that every cursor should fetch for them.
CONVERTED_VALUES_QUERY = """
SELECT
  1 AS col
  ,CAST('12:34:56' AS TIME(0)) AS col_time_0
  ,CAST('12:34:56.123456789' AS TIME(9)) AS col_time_9
  ,CAST('12:34:56.789 +09:00' AS TIME WITH TIME ZONE) AS col_time_tz
  ,CAST('12:34:56 -05:30' AS TIME(0) WITH TIME ZONE) AS col_time_tz_0
  ,CAST(NULL AS TIME WITH TIME ZONE) AS col_time_tz_null
  ,CAST(NULL AS JSON) AS col_json_null
  ,TIMESTAMP '2024-02-29 23:59:58.123 +05:30' AS col_timestamp_tz
  ,TIMESTAMP '2024-02-29 23:59:58.123 -08:00' AS col_timestamp_tz_negative
  ,TIMESTAMP '2024-02-29 23:59:58.123 America/New_York' AS col_timestamp_tz_name
  ,CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS col_timestamp_tz_null
  ,CAST(NULL AS TIME) AS col_time_null
"""
CONVERTED_VALUES_ROW = (
    1,
    datetime(2000, 1, 1, 12, 34, 56).time(),
    datetime(2000, 1, 1, 12, 34, 56, 123456).time(),
    datetime(2000, 1, 1, 12, 34, 56, 789000, tzinfo=timezone(timedelta(hours=9))).timetz(),
    datetime(2000, 1, 1, 12, 34, 56, tzinfo=timezone(-timedelta(hours=5, minutes=30))).timetz(),
    None,
    None,
    datetime(2024, 2, 29, 23, 59, 58, 123000, tzinfo=timezone(timedelta(hours=5, minutes=30))),
    datetime(2024, 2, 29, 23, 59, 58, 123000, tzinfo=timezone(-timedelta(hours=8))),
    datetime(2024, 2, 29, 23, 59, 58, 123000, tzinfo=ZoneInfo("America/New_York")),
    None,
    None,
)

# A TIMESTAMP WITH TIME ZONE value in the hour that a DST transition repeats. Athena
# writes 05:30 and 06:30 UTC both as 2020-11-01 01:30:00.000 America/New_York.
AMBIGUOUS_TIME_ZONE_QUERY = """
SELECT at_timezone(TIMESTAMP '2020-11-01 06:30:00 UTC', 'America/New_York') AS col_timestamp_tz
"""

# Values that the cursors reading CSV result files must read the same way from the
# GetQueryResults rows of managed query result storage.
RESULT_FILE_VALUES_QUERY = """
SELECT * FROM (VALUES
  (1, 'a,"b"' || chr(10) || 'c', '', DATE '2020-01-02', TIMESTAMP '2020-01-02 03:04:05.123',
   CAST('2020-01-02 03:04:05.123456' AS TIMESTAMP(6)), CAST('12.30' AS DECIMAL(10, 2)), X'0102',
   ARRAY[1, 2], MAP(ARRAY['k'], ARRAY[1]), CAST(ROW(1, 'x') AS ROW(a INTEGER, b VARCHAR)),
   json_parse('{"a": [1, 2]}'), BIGINT '9007199254740993',
   CAST('2020-01-02 03:04:05.123456789012' AS TIMESTAMP(12))),
  (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
) AS t(col_int, col_varchar, col_empty, col_date, col_timestamp, col_timestamp_6, col_decimal,
       col_varbinary, col_array, col_map, col_row, col_json, col_bigint, col_timestamp_12)
ORDER BY col_int
"""


# A Spark job whose executor tasks sleep, so that StopCalculationExecution can cancel it.
CANCELABLE_SPARK_JOB = """
import time
from pyspark.sql.functions import udf

slow = udf(lambda x: (time.sleep(90), x)[1], "long")
spark.range(0, 2, 1, 2).select(slow("id")).collect()
"""


def wait_for_spark_job(client, calculation_id, timeout=120, job_start_delay=10):
    """Wait until a calculation runs, then give its Spark job time to reach the executors.

    Args:
        client: The Athena client.
        calculation_id: The calculation execution ID.
        timeout: Seconds to wait for the RUNNING state.
        job_start_delay: Seconds to wait after the RUNNING state.

    Raises:
        AssertionError: If the calculation does not reach the RUNNING state in time.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = client.get_calculation_execution_status(CalculationExecutionId=calculation_id)
        state = response["Status"]["State"]
        if state == AthenaCalculationExecutionStatus.STATE_RUNNING:
            time.sleep(job_start_delay)
            return
        assert state not in (
            AthenaCalculationExecutionStatus.STATE_COMPLETED,
            AthenaCalculationExecutionStatus.STATE_FAILED,
            AthenaCalculationExecutionStatus.STATE_CANCELED,
        ), f"Calculation {calculation_id} ended as {state} before it was canceled."
        time.sleep(1)
    raise AssertionError(f"Calculation {calculation_id} did not start in {timeout} seconds.")


def decorated(impl):
    """Wrap a SQLAlchemy type in a TypeDecorator.

    Args:
        impl: The type the decorator delegates to.

    Returns:
        A TypeDecorator instance whose implementation is ``impl``.
    """
    return type("Decorated", (types.TypeDecorator,), {"impl": impl, "cache_ok": True})()


def recording_engine(rowcounts=None, query="", config=None, rows=None, **kwargs):
    """Create an engine whose DB API connection records statements offline.

    Args:
        rowcounts: Row counts reported by successive cursor calls. By default,
            a call reports the number of rows it inserted.
        query: The query string of the connection URL, without ``?``.
        config: String engine options to pass through ``engine_from_config``
            instead of calling ``create_engine`` directly.
        rows: Result rows that each ``execute()`` returns, as tuples of the
            values Athena returns. By default, a statement returns no rows.
        **kwargs: Additional keyword arguments for ``create_engine``.

    Returns:
        A tuple of the engine and the list of recorded
        ``(method, operation, parameters)`` calls.
    """
    calls = []
    counts = iter(rowcounts or ())

    class RecordingCursor:
        description = None
        rowcount = -1

        def execute(self, operation, parameters=None, **_):
            calls.append(("execute", operation, parameters))
            inserted = sum(1 for key in parameters if key.startswith("id"))
            self.rowcount = next(counts, inserted)
            if rows is not None:
                self.description = [
                    (f"col_{i}", "varchar", None, None, None, None, True)
                    for i in range(len(rows[0]))
                ]
                self._rows = iter(rows)

        def executemany(self, operation, seq_of_parameters, **_):
            calls.append(("executemany", operation, seq_of_parameters))
            self.rowcount = next(counts, len(seq_of_parameters))

        def fetchone(self):
            return next(self._rows, None)

        def close(self):
            pass

    connection = SimpleNamespace(
        cursor=RecordingCursor, close=lambda: None, commit=lambda: None, rollback=lambda: None
    )
    url = f"awsathena+rest://athena.us-west-2.amazonaws.com/default?{query}"
    if config is not None:
        configuration = {"sqlalchemy.url": url}
        configuration.update({f"sqlalchemy.{key}": value for key, value in config.items()})
        engine = engine_from_config(configuration, creator=lambda: connection, **kwargs)
    else:
        engine = create_engine(url, creator=lambda: connection, **kwargs)
    return engine, calls


def executed_sql(statement, parameters=None):
    """Execute a statement through ``recording_engine`` and format what the cursor receives.

    Bind processors and SQLAlchemy's bind name escaping apply as in a real
    execution, and ``DefaultParameterFormatter`` renders the parameters as
    ``Cursor.execute()`` would.

    Args:
        statement: SQLAlchemy statement to execute.
        parameters: Execution parameters for the statement.

    Returns:
        The formatted SQL of the single recorded call.
    """
    engine, calls = recording_engine()
    with engine.connect() as conn:
        conn.execute(statement, parameters)
    ((_, operation, formatted_parameters),) = calls
    return DefaultParameterFormatter().format(operation, formatted_parameters)


def fetched_scalar(statement, value):
    """Execute a statement through ``recording_engine`` and return its processed result.

    Args:
        statement: SQLAlchemy statement that selects one column.
        value: The value that the cursor returns for that column, as Athena returns it.

    Returns:
        The value after SQLAlchemy's result processing.
    """
    engine, _ = recording_engine(rows=[(value,)])
    with engine.connect() as conn:
        return conn.execute(statement).scalar_one()


BINARY_VALUES = (
    None,
    b"",
    b"\x00\xff'\\%",
    bytes(range(256)),
    bytearray(b"\x00\xff"),
    memoryview(b"\x00\xff"),
)


def binary_columns():
    """Cast each binary value to each binary type, as a bound and as a literal-execute parameter.

    Returns:
        A tuple of the column expressions to select and the row that Athena returns for them.
    """
    cases = [
        (type_, value, literal_execute)
        for type_ in (types.LargeBinary, types.BINARY, types.VARBINARY)
        for value in BINARY_VALUES
        for literal_execute in (False, True)
    ]
    columns = [
        cast(literal(value, type_=type_, literal_execute=literal_execute), type_)
        for type_, value, literal_execute in cases
    ]
    expected = tuple(None if value is None else bytes(value) for _, value, _ in cases)
    return columns, expected


def wait_for_spark_session_state(client, session_id, state, timeout=120):
    """Wait until a Spark session reaches a state.

    Args:
        client: The Athena client.
        session_id: The session ID.
        state: The session state to wait for.
        timeout: Seconds to wait.

    Raises:
        AssertionError: If the session does not reach the state in time.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if client.get_session_status(SessionId=session_id)["Status"]["State"] == state:
            return
        time.sleep(1)
    raise AssertionError(f"Session {session_id} did not become {state} in {timeout} seconds.")


class ImmediateExecutor:
    """An executor that runs each call when it is submitted."""

    def __init__(self, max_workers=None):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def shutdown(self, wait=True, cancel_futures=False):
        pass

    def submit(self, fn, *args, **kwargs):
        future = self._new_future()
        try:
            future.set_result(fn(*args, **kwargs))
        except Exception as e:
            future.set_exception(e)
        return future

    def _new_future(self):
        return Future()


def interrupt_start_waits(started, release, interrupts=1):
    """Patch the wait for a start request on a helper thread to raise KeyboardInterrupt.

    The first ``interrupts`` waits raise ``KeyboardInterrupt`` once the request
    has started; later waits release the request and wait for it.

    Args:
        started: Set when the start request starts.
        release: Releases the start request.
        interrupts: How many waits raise.

    Returns:
        The patcher, and the list of raised interrupts.
    """
    raised = []

    def interrupting_wait(futures, timeout=None):
        if len(raised) < interrupts:
            assert started.wait(EVENT_TIMEOUT)
            raised.append(KeyboardInterrupt())
            raise raised[-1]
        release.set()
        return wait(futures, timeout)

    return patch("pyathena.common.wait", side_effect=interrupting_wait), raised


# A source object larger than a single CopyObject request allows, copied
# from bucket/src to bucket/dst by a multipart copy of two parts with the
# maximum block size (GH-973).
MULTIPART_COPY_BLOCK_SIZE = S3Core.MULTIPART_UPLOAD_MAX_PART_SIZE
MULTIPART_COPY_SIZE = MULTIPART_COPY_BLOCK_SIZE + S3Core.MULTIPART_UPLOAD_MIN_PART_SIZE
MULTIPART_COPY_EXPIRES = datetime(2030, 1, 1, tzinfo=UTC)
MULTIPART_COPY_KWARGS = {
    # Ignored with the default COPY directives, as CopyObject ignores them.
    "ContentType": "text/plain",
    "Tagging": "ignored=1",
    # Not metadata: sent to CreateMultipartUpload.
    "StorageClass": "STANDARD_IA",
    "RequestPayer": "requester",
    "ExpectedBucketOwner": "111111111111",
    "ExpectedSourceBucketOwner": "222222222222",
    # A source condition: sent to the part copies only.
    "CopySourceIfMatch": '"src"',
}


def _annotation(name):
    return {"AnnotationName": name, "LastModified": MULTIPART_COPY_EXPIRES, "Size": 1}


def stub_multipart_copy(
    stubber, fail_list=False, fail_part=False, fail_abort=False, fail_annotation=False
):
    """Queue the requests of a multipart copy with MULTIPART_COPY_KWARGS.

    The source, in a versioned bucket, has user-defined metadata, a tag and
    two annotations listed on two pages, which are copied with the default
    COPY directives from the version that HeadObject reports.

    Args:
        stubber: The Stubber of the S3 client.
        fail_list: Deny the listing of the annotations; nothing is written
            then.
        fail_part: Fail the second part copy; the upload is then aborted.
        fail_abort: Deny that abort, with fail_part.
        fail_annotation: Fail the write of the first annotation; no
            other annotation is copied then.
    """
    source = {
        "Bucket": "bucket",
        "Key": "src",
        "RequestPayer": "requester",
        "ExpectedBucketOwner": "222222222222",
    }
    destination = {
        "Bucket": "bucket",
        "Key": "dst",
        "RequestPayer": "requester",
        "ExpectedBucketOwner": "111111111111",
    }
    stubber.add_response(
        "head_object",
        {
            "ContentLength": MULTIPART_COPY_SIZE,
            "ContentType": "text/csv",
            "CacheControl": "max-age=60",
            "Expires": MULTIPART_COPY_EXPIRES,
            "StorageClass": "GLACIER_IR",
            "Metadata": {"owner": "etl"},
            "VersionId": "v-src",
        },
        source,
    )
    # The version that HeadObject reports is read from then on.
    source = {**source, "VersionId": "v-src"}
    stubber.add_response("get_object_tagging", {"TagSet": [{"Key": "t 1", "Value": "v1"}]}, source)
    # The annotations are listed before anything is written.
    if fail_list:
        stubber.add_client_error(
            "list_object_annotations", "AccessDenied", 403, expected_params=source
        )
        return
    stubber.add_response(
        "list_object_annotations",
        {"Annotations": [_annotation("a1")], "NextContinuationToken": "next"},
        source,
    )
    stubber.add_response(
        "list_object_annotations",
        {"Annotations": [_annotation("a2")]},
        {**source, "ContinuationToken": "next"},
    )
    stubber.add_response(
        "create_multipart_upload",
        {"Bucket": "bucket", "Key": "dst", "UploadId": "u"},
        {
            **destination,
            "CacheControl": "max-age=60",
            "ContentType": "text/csv",
            "Expires": MULTIPART_COPY_EXPIRES,
            "Metadata": {"owner": "etl"},
            "Tagging": "t+1=v1",
            "StorageClass": "STANDARD_IA",
        },
    )
    ranges = {
        1: (0, MULTIPART_COPY_BLOCK_SIZE - 1),
        2: (MULTIPART_COPY_BLOCK_SIZE, MULTIPART_COPY_SIZE - 1),
    }
    for part_number in (1, 2):
        part = {
            **destination,
            "CopySource": {"Bucket": "bucket", "Key": "src", "VersionId": "v-src"},
            "UploadId": "u",
            "PartNumber": part_number,
            "CopySourceRange": "bytes={}-{}".format(*ranges[part_number]),
            "CopySourceIfMatch": '"src"',
            "ExpectedSourceBucketOwner": "222222222222",
        }
        if fail_part and part_number == 2:
            stubber.add_client_error(
                "upload_part_copy", "InternalError", "part failed", 500, expected_params=part
            )
            abort = {**destination, "UploadId": "u"}
            if fail_abort:
                stubber.add_client_error(
                    "abort_multipart_upload",
                    "AccessDenied",
                    "abort denied",
                    403,
                    expected_params=abort,
                )
            else:
                stubber.add_response("abort_multipart_upload", {}, abort)
            return
        stubber.add_response(
            "upload_part_copy", {"CopyPartResult": {"ETag": f'"p{part_number}"'}}, part
        )
    stubber.add_response(
        "complete_multipart_upload",
        {"ETag": '"dst"', "VersionId": "v-dst"},
        {
            **destination,
            "UploadId": "u",
            "MultipartUpload": {
                "Parts": [{"ETag": '"p1"', "PartNumber": 1}, {"ETag": '"p2"', "PartNumber": 2}]
            },
        },
    )
    for name in ("a1", "a2"):
        payload = f"payload of {name}".encode()
        stubber.add_response(
            "get_object_annotation",
            {"AnnotationPayload": StreamingBody(io.BytesIO(payload), len(payload))},
            {**source, "AnnotationName": name},
        )
        put = {
            **destination,
            "AnnotationName": name,
            "AnnotationPayload": payload,
            "VersionId": "v-dst",
            "ObjectIfMatch": '"dst"',
        }
        if fail_annotation:
            stubber.add_client_error(
                "put_object_annotation", "AccessDenied", 403, expected_params=put
            )
            return
        stubber.add_response("put_object_annotation", {}, put)


def query_results_response(columns):
    """Build a ``GetQueryResults`` response with the metadata of the given columns.

    Args:
        columns: The (name, Athena type) pairs of the result columns.

    Returns:
        The response, without rows, as for a result that is read from its result file.
    """
    return {
        "ResultSet": {
            "ResultSetMetadata": {
                "ColumnInfo": [
                    {"Name": name, "Type": type_, "Precision": 3, "Scale": 0, "Nullable": "UNKNOWN"}
                    for name, type_ in columns
                ]
            },
            "Rows": [],
        }
    }


class LocalBucketFileSystem(DirFileSystem):
    """The local files under a directory, addressed by S3 paths, with the files it opened.

    ``s3://bucket/key`` is the file ``bucket/key`` under the directory.
    """

    def __init__(self, root):
        """Initialize the filesystem.

        Args:
            root: The directory that holds the files.
        """
        super().__init__(path=str(root), fs=LocalFileSystem(), skip_instance_cache=True)
        # The keyword arguments of each open() and the file that it returned.
        self.opened = []

    @override
    def _join(self, path):
        if isinstance(path, str):
            path = path.removeprefix("s3://")
        elif isinstance(path, list):
            path = [p.removeprefix("s3://") for p in path]
        return super()._join(path)

    @override
    def open(self, path, *args, **kwargs):
        file = super().open(path, *args, **kwargs)
        self.opened.append((kwargs, file))
        return file
