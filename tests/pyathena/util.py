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
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from jinja2 import Environment, FileSystemLoader
from sqlalchemy import types

from pyathena.error import ProgrammingError
from pyathena.filesystem.s3 import S3FileSystem
from pyathena.filesystem.s3_async import AioS3FileSystem
from pyathena.filesystem.s3_core import S3Core
from pyathena.glue import GlueMetadataClient
from pyathena.model import AthenaCalculationExecutionStatus, AthenaQueryExecution
from pyathena.util import retry_api_call

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


def overlapping_calls(calls, timeout=5):
    """Return a callable that records the peak number of its overlapping calls.

    Each call waits until ``calls`` calls overlap, or until the timeout.

    Args:
        calls: The number of calls to wait for.
        timeout: The longest wait of a call, in seconds.

    Returns:
        The callable, which accepts and ignores any arguments, and a
        dictionary whose ``peak`` item is the peak number.
    """
    condition = threading.Condition()
    state = {"active": 0, "peak": 0}

    def call(*args, **kwargs):
        with condition:
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
            condition.notify_all()
            condition.wait_for(lambda: state["active"] >= calls, timeout=timeout)
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
