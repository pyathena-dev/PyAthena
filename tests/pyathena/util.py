# Copyright 2022 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import time
from pathlib import Path

from botocore.config import Config
from botocore.exceptions import ClientError
from jinja2 import Environment, FileSystemLoader
from sqlalchemy import types

from pyathena.glue import GlueMetadataClient
from pyathena.model import AthenaCalculationExecutionStatus
from pyathena.sqlalchemy.types import TINYINT, AthenaArray, AthenaStruct
from tests.pyathena.expected import base_type, type_arguments, type_parameters
from tests.pyathena.tables import Column

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


# The SQLAlchemy type class of a reflected column, by base type.
_SQLALCHEMY_TYPES = {
    "boolean": types.BOOLEAN,
    "tinyint": TINYINT,
    "smallint": types.SMALLINT,
    "int": types.INTEGER,
    "bigint": types.BIGINT,
    "float": types.FLOAT,
    "double": types.DOUBLE,
    "string": types.String,
    "varchar": types.VARCHAR,
    "timestamp": types.TIMESTAMP,
    "date": types.DATE,
    "binary": types.BINARY,
    "array": AthenaArray,
    "map": types.String,
    "struct": AthenaStruct,
    "decimal": types.DECIMAL,
}


def assert_sqlalchemy_type(sqlalchemy_type, column: Column) -> None:
    """Assert that a reflected SQLAlchemy column type matches a table column's definition.

    Args:
        sqlalchemy_type: The reflected column type.
        column: The column in ``tests.pyathena.tables``.

    Raises:
        AssertionError: If the type class or its parameters differ.
    """
    name = base_type(column.athena_type)
    assert isinstance(sqlalchemy_type, _SQLALCHEMY_TYPES[name]), (column.name, sqlalchemy_type)
    if name == "varchar":
        assert sqlalchemy_type.length == type_parameters(column.athena_type)[0], column.name
    elif name == "decimal":
        precision, scale = type_parameters(column.athena_type)
        assert (sqlalchemy_type.precision, sqlalchemy_type.scale) == (precision, scale)
    elif name == "array":
        (element,) = type_arguments(column.athena_type)
        assert isinstance(sqlalchemy_type.item_type, _SQLALCHEMY_TYPES[base_type(element)])
