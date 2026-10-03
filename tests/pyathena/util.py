# Copyright 2022 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import time
from concurrent.futures import wait
from pathlib import Path
from unittest.mock import patch

from botocore.config import Config
from botocore.exceptions import ClientError
from jinja2 import Environment, FileSystemLoader
from sqlalchemy import types

from pyathena.glue import GlueMetadataClient
from pyathena.model import AthenaCalculationExecutionStatus, AthenaQueryExecution

_queries = Environment(
    loader=FileSystemLoader(Path(__file__).parents[1].resolve() / "resources" / "queries")
)


def read_query(name, **kwargs):
    template = _queries.get_template(name)
    return [q.strip() for q in template.render(**kwargs).split(";") if q and q.strip()]


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
