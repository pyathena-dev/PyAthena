# Copyright 2022 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import time
from concurrent.futures import wait
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from botocore.config import Config
from botocore.exceptions import ClientError
from dateutil.tz import gettz
from jinja2 import Environment, FileSystemLoader
from sqlalchemy import types

from pyathena.filesystem.s3 import S3FileSystem
from pyathena.filesystem.s3_async import AioS3FileSystem
from pyathena.glue import GlueMetadataClient
from pyathena.model import AthenaCalculationExecutionStatus, AthenaQueryExecution

_queries = Environment(
    loader=FileSystemLoader(Path(__file__).parents[1].resolve() / "resources" / "queries")
)


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
            "QueryExecution": {
                "QueryExecutionId": query_id,
                "Query": query,
                "StatementType": AthenaQueryExecution.STATEMENT_TYPE_DML,
                "QueryExecutionContext": {"Database": schema},
                "Status": {
                    "State": AthenaQueryExecution.STATE_SUCCEEDED,
                    "CompletionDateTime": completion_date_time,
                },
            }
        }
    )


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
# ZONE values with UTC offsets and a zone name, a NULL JSON value, and the row that
# every cursor should fetch for them.
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
    datetime(2024, 2, 29, 23, 59, 58, 123000, tzinfo=gettz("America/New_York")),
    None,
)


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


# Seconds a test waits for an event from a helper thread before failing.
EVENT_TIMEOUT = 10


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
