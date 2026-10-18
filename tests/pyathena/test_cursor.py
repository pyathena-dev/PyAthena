import contextlib
import random
import re
import string
import threading
import time
import uuid
from collections import OrderedDict
from concurrent import futures
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from random import randint
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from pyathena import (
    BINARY,
    BOOLEAN,
    DATE,
    DATETIME,
    JSON,
    NUMBER,
    STRING,
    TIME,
    Binary,
    ExecuteOptions,
)
from pyathena.async_cursor import AsyncCursor
from pyathena.cursor import Cursor, DictCursor
from pyathena.error import (
    DatabaseError,
    DataError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaDictResultSet
from pyathena.util import RetryConfig, stop_retries_on
from tests import ENV
from tests.pyathena import temporal
from tests.pyathena.conftest import connect
from tests.pyathena.tables import TABLES, VIEWS
from tests.pyathena.util import (
    AMBIGUOUS_TIME_ZONE_QUERY,
    CONVERTED_VALUES_QUERY,
    CONVERTED_VALUES_ROW,
    EVENT_TIMEOUT,
    QueryExecutionHistory,
    ThrottledOperation,
    cache_search_cursor,
    interrupt_start_waits,
    succeeded_query_execution,
    succeeded_query_execution_data,
    throttle_metadata_api,
    unreachable_glue,
)


def _offline_cursor(kill_on_interrupt, cursor_class=Cursor):
    """A cursor whose query requests go to a mocked Athena client.

    Args:
        kill_on_interrupt: Whether the cursor cancels the query on interrupt.
        cursor_class: The cursor class.

    Returns:
        The cursor and the mock of its cancellation request.
    """
    cursor = cursor_class.__new__(cursor_class)  # bypass __init__ to avoid AWS calls
    cursor._rowcount = -1
    cursor._result_set = None
    cursor._query_id = None
    cursor._poll_interval = 0
    cursor._kill_on_interrupt = kill_on_interrupt
    cursor._on_poll = None
    cursor._on_start_query_execution = None
    cursor._prepare_query = MagicMock(return_value=("SELECT 1", None))
    cursor._build_start_query_execution_request = MagicMock(return_value={})
    cursor._find_previous_query_id = MagicMock(return_value=None)
    cursor._connection = MagicMock()
    cursor._connection.client.start_query_execution.return_value = {"QueryExecutionId": "query_id"}
    cursor._retry_config = RetryConfig(attempt=2, multiplier=0)
    # A successful query builds a result set from these.
    cursor._converter = MagicMock()
    cursor._arraysize = 1
    cursor._result_set_class = MagicMock()
    cancel = cursor._cancel = MagicMock()
    return cursor, cancel


def _types(value):
    """The type of a value and of each value it contains, for comparing them.

    Args:
        value: A fetched value, such as a row of lists and dictionaries.

    Returns:
        The types, in the shape of the value.
    """
    if isinstance(value, dict):
        return {k: _types(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_types(v) for v in value]
    return type(value)


def _block_start(cursor, response=None):
    """Make the cursor's StartQueryExecution request block until released.

    Args:
        cursor: The cursor from ``_offline_cursor``.
        response: The exception to raise once released; the default returns
            ``query_id``.

    Returns:
        An event set when the request starts, and an event that releases it.
    """
    started = threading.Event()
    release = threading.Event()

    def start_query_execution(**kwargs):
        started.set()
        assert release.wait(EVENT_TIMEOUT)
        if response:
            raise response
        return {"QueryExecutionId": "query_id"}

    cursor._connection.client.start_query_execution.side_effect = start_query_execution
    return started, release


class TestCursor:
    def test_builds_no_s3_client(self, cursor):
        # GH-1011: the result set built an S3 client it never used.
        session = cursor.connection.session
        with patch.object(session, "client", side_effect=session.client) as client:
            cursor.execute("SELECT * FROM one_row")
            assert cursor.fetchall() == [(1,)]
        client.assert_not_called()

    def test_fetchone(self, cursor):
        cursor.execute("SELECT * FROM one_row")
        assert cursor.rowcount == -1
        assert cursor.rownumber == 0
        assert cursor.fetchone() == (1,)
        assert cursor.rownumber == 1
        assert cursor.fetchone() is None
        assert cursor.database == ENV.schema
        assert cursor.catalog
        assert cursor.query_id
        assert cursor.query
        assert cursor.statement_type == AthenaQueryExecution.STATEMENT_TYPE_DML
        assert cursor.work_group == ENV.default_work_group
        assert cursor.state == AthenaQueryExecution.STATE_SUCCEEDED
        assert cursor.state_change_reason is None
        assert cursor.submission_date_time
        assert isinstance(cursor.submission_date_time, datetime)
        assert cursor.completion_date_time
        assert isinstance(cursor.completion_date_time, datetime)
        assert cursor.data_scanned_in_bytes
        assert cursor.engine_execution_time_in_millis
        assert cursor.query_queue_time_in_millis
        assert cursor.total_execution_time_in_millis
        # assert cursor.query_planning_time_in_millis  # TODO flaky test
        # assert cursor.service_processing_time_in_millis  # TODO flaky test
        assert cursor.output_location
        assert cursor.data_manifest_location is None
        assert cursor.encryption_option is None
        assert cursor.kms_key is None
        assert cursor.selected_engine_version
        assert cursor.effective_engine_version

    def test_fetchmany(self, cursor):
        cursor.execute("SELECT * FROM many_rows LIMIT 15")
        actual1 = cursor.fetchmany(10)
        assert len(actual1) == 10
        assert actual1 == [(i,) for i in range(10)]
        actual2 = cursor.fetchmany(10)
        assert len(actual2) == 5
        assert actual2 == [(i,) for i in range(10, 15)]

    def test_fetchall(self, cursor):
        cursor.execute("SELECT a FROM many_rows ORDER BY a")
        assert cursor.fetchall() == [(i,) for i in range(10000)]

    # Cache hits are asserted in ENV.work_group: the default work group runs most test
    # queries, which can push earlier executions out of the cache_size window.
    @pytest.mark.parametrize("cursor", [{"work_group": ENV.work_group}], indirect=["cursor"])
    def test_cache_size(self, cursor):
        # To test caching, we need to make sure the query is unique, otherwise
        # we might accidentally pick up the cache results from another CI run.
        query = f"SELECT * FROM one_row -- {datetime.now(UTC)!s}"

        cursor.execute(query)
        first_query_id = cursor.query_id

        cursor.execute(query)
        second_query_id = cursor.query_id

        cursor.execute(query, cache_size=100)
        third_query_id = cursor.query_id

        # Make sure default behavior is no caching, i.e. same query has
        # run twice results in different query IDs
        assert first_query_id != second_query_id
        # When using caching, the same query ID should be returned.
        assert third_query_id in [first_query_id, second_query_id]

    @pytest.mark.parametrize("cursor", [{"work_group": ENV.work_group}], indirect=["cursor"])
    def test_cache_size_with_work_group(self, cursor):
        now = datetime.now(UTC)
        cursor.execute("SELECT %(now)s as date", {"now": now})
        first_query_id = cursor.query_id

        cursor.execute("SELECT %(now)s as date", {"now": now})
        second_query_id = cursor.query_id

        cursor.execute("SELECT %(now)s as date", {"now": now}, cache_size=100)
        third_query_id = cursor.query_id

        assert first_query_id != second_query_id
        assert third_query_id in [first_query_id, second_query_id]

    @pytest.mark.parametrize("cursor", [{"work_group": ENV.work_group}], indirect=["cursor"])
    def test_cache_size_with_qmark_parameters(self, cursor):
        query = f"SELECT ? AS v -- {datetime.now(UTC)!s}"

        cursor.execute(query, ["'1'"], paramstyle="qmark")
        first_query_id = cursor.query_id

        # Different parameters must not reuse the earlier execution (#941).
        cursor.execute(query, ["'2'"], paramstyle="qmark", cache_size=100)
        assert cursor.query_id != first_query_id
        assert cursor.fetchall() == [("2",)]

        # Athena does not return the parameters of earlier executions,
        # so even the same parameters run again.
        cursor.execute(query, ["'1'"], paramstyle="qmark", cache_size=100)
        assert cursor.query_id != first_query_id
        assert cursor.fetchall() == [("1",)]

    def test_cache_expiration_time(self, cursor):
        query = f"SELECT * FROM one_row -- {datetime.now(UTC)!s}"

        cursor.execute(query)
        query_id_1 = cursor.query_id

        cursor.execute(query)
        query_id_2 = cursor.query_id

        cursor.execute(query, cache_expiration_time=3600)  # 1 hours
        query_id_3 = cursor.query_id

        assert query_id_1 != query_id_2
        assert query_id_3 in [query_id_1, query_id_2]

    @pytest.mark.parametrize("cursor", [{"work_group": ENV.work_group}], indirect=["cursor"])
    def test_cache_expiration_time_with_cache_size(self, cursor):
        # Cache miss
        query = f"SELECT * FROM one_row -- {datetime.now(UTC)!s}"

        cursor.execute(query)
        query_id_1 = cursor.query_id

        cursor.execute(query)
        query_id_2 = cursor.query_id

        time.sleep(2)

        cursor.execute(query, cache_size=100, cache_expiration_time=1)  # 1 seconds
        query_id_3 = cursor.query_id

        assert query_id_1 != query_id_2
        assert query_id_3 not in [query_id_1, query_id_2]

        # Cache miss
        query = f"SELECT * FROM one_row -- {datetime.now(UTC)!s}"

        cursor.execute(query)
        query_id_4 = cursor.query_id

        cursor.execute(query)
        query_id_5 = cursor.query_id

        for _ in range(5):
            cursor.execute("SELECT %(now)s as date", {"now": datetime.now(UTC)})

        cursor.execute(query, cache_size=1, cache_expiration_time=3600)  # 1 hours
        query_id_6 = cursor.query_id

        assert query_id_4 != query_id_5
        assert query_id_6 not in [query_id_4, query_id_5]

        # Cache hit
        query = f"SELECT * FROM one_row -- {datetime.now(UTC)!s}"

        cursor.execute(query)
        query_id_7 = cursor.query_id

        cursor.execute(query)
        query_id_8 = cursor.query_id

        time.sleep(2)
        for _ in range(5):
            cursor.execute("SELECT %(now)s as date", {"now": datetime.now(UTC)})

        cursor.execute(query, cache_size=1000, cache_expiration_time=3600)  # 1 hours
        query_id_9 = cursor.query_id

        assert query_id_7 != query_id_8
        assert query_id_9 in [query_id_7, query_id_8]

    def test_cache_size_different_schema(self):
        """A cached result is only reused when it ran against the same schema (#739).

        Identical SQL can resolve to different tables depending on the database it
        runs against, so a prior execution from another schema must not be a cache hit.
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
                            "CompletionDateTime": datetime.now(UTC),
                        },
                    }
                }
            )

        cursor = Cursor.__new__(Cursor)  # bypass __init__ to avoid AWS calls
        cursor._catalog_name = None

        with patch.object(
            Cursor, "_list_query_executions", return_value=(None, [execution("other_schema")])
        ):
            cursor._schema_name = "this_schema"
            assert cursor._find_previous_query_id(query, None, cache_size=100) is None
            cursor._schema_name = "other_schema"
            assert (
                cursor._find_previous_query_id(query, None, cache_size=100)
                == "query_id_other_schema"
            )

    def test_cancel_in_stopped_scope(self):
        """GH-1202: a stop scope does not stop the retries of StopQueryExecution (no AWS)."""
        cursor = Cursor.__new__(Cursor)  # bypass __init__ to avoid AWS calls
        cursor._connection = MagicMock()
        cursor._retry_config = RetryConfig(attempt=3, multiplier=0)
        operation = ThrottledOperation(failures=2, operation_name="StopQueryExecution")
        cursor._connection.client.stop_query_execution.side_effect = operation
        stop = threading.Event()
        stop.set()
        with stop_retries_on(stop):
            cursor._cancel("query_id")
        assert operation.calls == 3

    def test_cache_size_different_catalog(self):
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
                            "CompletionDateTime": datetime.now(UTC),
                        },
                    }
                }
            )

        cursor = Cursor.__new__(Cursor)
        cursor._schema_name = schema

        with patch.object(
            Cursor, "_list_query_executions", return_value=(None, [execution("awsdatacatalog")])
        ):
            # A different catalog must not be a cache hit.
            cursor._catalog_name = "other_catalog"
            assert cursor._find_previous_query_id(query, None, cache_size=100) is None
            # The same catalog, differing only in case, must still be a cache hit
            cursor._catalog_name = "AwsDataCatalog"
            assert (
                cursor._find_previous_query_id(query, None, cache_size=100)
                == "query_id_awsdatacatalog"
            )

    def test_cache_search_stops_at_expired_execution(self):
        query = "SELECT * FROM one_row"
        now = datetime.now(UTC)
        page_size = Cursor.LIST_QUERY_EXECUTIONS_MAX_RESULTS
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
        cursor = cache_search_cursor(Cursor, history)

        # Without cache_size, cache_expiration_time alone bounds the search.
        assert cursor._find_previous_query_id(query, None, cache_expiration_time=3600) is None
        assert len(history.list_requests) == 1
        # A failed search would also return None, but the same search reuses the
        # execution once it is within cache_expiration_time.
        assert (
            cursor._find_previous_query_id(query, None, cache_expiration_time=3 * 3600) == "expired"
        )

    @pytest.mark.parametrize(
        ("position", "expected"),
        [(59, "match"), (60, None)],
        ids=["last_searched", "after_cache_size"],
    )
    def test_cache_search_reads_pages_up_to_cache_size(self, position, expected):
        """With cache_size=60, the search reads the 60 latest executions over two pages."""
        query = "SELECT * FROM one_row"
        now = datetime.now(UTC)
        executions = [
            succeeded_query_execution_data(f"other_{i}", "SELECT 1", now) for i in range(70)
        ]
        executions[position] = succeeded_query_execution_data("match", query, now)
        history = QueryExecutionHistory(executions)
        cursor = cache_search_cursor(Cursor, history)

        assert cursor._find_previous_query_id(query, "wg", cache_size=60) == expected
        assert [request["WorkGroup"] for request in history.list_requests] == ["wg", "wg"]

    def test_cache_search_prefers_latest_execution(self):
        query = "SELECT * FROM one_row"
        now = datetime.now(UTC)
        cursor = Cursor.__new__(Cursor)
        cursor._schema_name = "this_schema"
        cursor._catalog_name = None
        page = [
            succeeded_query_execution("older", query, now - timedelta(minutes=1)),
            succeeded_query_execution("latest", query, now),
        ]

        with patch.object(Cursor, "_list_query_executions", return_value=(None, page)):
            assert cursor._find_previous_query_id(query, None, cache_size=10) == "latest"

    @pytest.mark.parametrize(
        "cursor",
        [{"work_group": ENV.work_group, "result_reuse_enable": True, "result_reuse_minutes": 5}],
        indirect=["cursor"],
    )
    def test_cursor_query_result_reuse(self, cursor):
        now = datetime.now(UTC)
        cursor.execute("SELECT %(now)s as date", {"now": now})
        assert not cursor.reused_previous_result
        assert cursor.result_reuse_enabled
        assert cursor.result_reuse_minutes == 5
        cursor.execute("SELECT %(now)s as date", {"now": now})
        assert cursor.reused_previous_result
        assert cursor.result_reuse_enabled
        assert cursor.result_reuse_minutes == 5

    @pytest.mark.parametrize("cursor", [{"work_group": ENV.work_group}], indirect=["cursor"])
    def test_execute_query_result_reuse(self, cursor):
        now = datetime.now(UTC)
        cursor.execute(
            "SELECT %(now)s as date", {"now": now}, result_reuse_enable=True, result_reuse_minutes=5
        )
        assert not cursor.reused_previous_result
        assert cursor.result_reuse_enabled
        assert cursor.result_reuse_minutes == 5
        cursor.execute(
            "SELECT %(now)s as date", {"now": now}, result_reuse_enable=True, result_reuse_minutes=5
        )
        assert cursor.reused_previous_result
        assert cursor.result_reuse_enabled
        assert cursor.result_reuse_minutes == 5

    def test_arraysize(self, cursor):
        cursor.arraysize = 5
        cursor.execute("SELECT * FROM many_rows LIMIT 20")
        assert len(cursor.fetchmany()) == 5

    def test_arraysize_default(self, cursor):
        assert cursor.arraysize == Cursor.DEFAULT_FETCH_SIZE

    def test_invalid_arraysize(self, cursor):
        with pytest.raises(ProgrammingError):
            cursor.arraysize = 10000
        with pytest.raises(ProgrammingError):
            cursor.arraysize = -1

    def test_description(self, cursor):
        cursor.execute("SELECT 1 AS foobar FROM one_row")
        assert cursor.description == [("foobar", "integer", None, None, 10, 0, "UNKNOWN")]

    def test_description_initial(self, cursor):
        assert cursor.description is None

    def test_description_failed(self, cursor):
        with contextlib.suppress(DatabaseError):
            cursor.execute("blah_blah")
        assert cursor.description is None

    def test_description_with_ctas(self, cursor):
        table_name = (
            f"test_description_with_ctas_{''.join(random.choices(string.ascii_lowercase, k=10))}"
        )
        location = f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/"
        cursor.execute(
            f"""
            CREATE TABLE {ENV.schema}.{table_name}
            WITH (
                format='PARQUET',
                external_location='{location}'
            ) AS SELECT a FROM many_rows LIMIT 1
            """
        )
        assert cursor.description == [("rows", "bigint", None, None, 19, 0, "UNKNOWN")]
        # CTAS returns affected row count via rowcount, not via fetchone()
        assert cursor.rowcount == 1
        assert cursor.fetchone() is None

    def test_bad_query(self, cursor):
        def run():
            cursor.execute("SELECT does_not_exist FROM this_really_does_not_exist")
            cursor.fetchone()

        pytest.raises(DatabaseError, run)

    def test_fetch_no_data(self, cursor):
        pytest.raises(ProgrammingError, cursor.fetchone)
        pytest.raises(ProgrammingError, cursor.fetchmany)
        pytest.raises(ProgrammingError, cursor.fetchall)

    def test_query_with_parameter(self, cursor):
        cursor.execute(
            """
            SELECT * FROM many_rows
            WHERE a < %(param)d
            """,
            {"param": 10},
        )
        assert cursor.fetchall() == [(i,) for i in range(10)]

        cursor.execute(
            """
            SELECT col_string FROM one_row_complex
            WHERE col_string = %(param)s
            """,
            {"param": "a string"},
        )
        assert cursor.fetchall() == [("a string",)]

    def test_query_with_parameter_qmark(self, cursor):
        cursor.execute(
            """
            SELECT * FROM many_rows
            WHERE a < ?
            """,
            ["10"],
            paramstyle="qmark",
        )
        assert cursor.fetchall() == [(i,) for i in range(10)]

        cursor.execute(
            """
            SELECT col_string FROM one_row_complex
            WHERE col_string = ?
            """,
            ["'a string'"],
            paramstyle="qmark",
        )
        assert cursor.fetchall() == [("a string",)]

    def test_null_param(self, cursor):
        cursor.execute("SELECT %(param)s FROM one_row", {"param": None})
        assert cursor.fetchall() == [(None,)]

    def test_binary_parameter(self, cursor):
        # Empty, special, all bytes, bytearray, memoryview, DB API Binary, and NULL.
        values = [
            (b"", b""),
            (b"\x00\xff'\\%", b"\x00\xff'\\%"),
            (bytes(range(256)), bytes(range(256))),
            (bytearray(b"\x00\xff"), b"\x00\xff"),
            (memoryview(b"\x00\xff"), b"\x00\xff"),
            (Binary(bytearray(b"abc")), b"abc"),
            (None, None),
        ]
        cursor.execute(
            "SELECT " + ", ".join(f"CAST(%(v{i})s AS VARBINARY)" for i in range(len(values))),
            {f"v{i}": value for i, (value, _) in enumerate(values)},
        )
        assert cursor.fetchone() == tuple(expected for _, expected in values)
        assert [d[1] for d in cursor.description] == [BINARY] * len(values)

    def test_datetime_parameter_microseconds(self, cursor):
        value = datetime(2017, 1, 1, 12, 0, 0, 789012)
        cursor.execute("SELECT %(value)s", {"value": value})
        assert cursor.fetchone() == (value,)

    def test_no_params(self, cursor):
        pytest.raises(DatabaseError, lambda: cursor.execute("SELECT %(param)s FROM one_row"))
        pytest.raises(KeyError, lambda: cursor.execute("SELECT %(param)s FROM one_row", {"a": 1}))

    def test_contain_special_character_query(self, cursor):
        cursor.execute(
            """
            SELECT col_string FROM one_row_complex
            WHERE col_string LIKE '%str%'
            """
        )
        assert cursor.fetchall() == [("a string",)]
        cursor.execute(
            """
            SELECT col_string FROM one_row_complex
            WHERE col_string LIKE '%%str%%'
            """
        )
        assert cursor.fetchall() == [("a string",)]
        cursor.execute(
            """
            SELECT col_string, '%' FROM one_row_complex
            WHERE col_string LIKE '%str%'
            """
        )
        assert cursor.fetchall() == [("a string", "%")]
        cursor.execute(
            """
            SELECT col_string, '%%' FROM one_row_complex
            WHERE col_string LIKE '%%str%%'
            """
        )
        assert cursor.fetchall() == [("a string", "%%")]

    def test_contain_special_character_query_with_parameter(self, cursor):
        # Python 3.15 raises ValueError instead of TypeError for a % without a mapping key.
        pytest.raises(
            (TypeError, ValueError),
            lambda: cursor.execute(
                """
                SELECT col_string, %(param)s FROM one_row_complex
                WHERE col_string LIKE '%str%'
                """,
                {"param": "a string"},
            ),
        )
        cursor.execute(
            """
            SELECT col_string, %(param)s FROM one_row_complex
            WHERE col_string LIKE '%%str%%'
            """,
            {"param": "a string"},
        )
        assert cursor.fetchall() == [("a string", "a string")]
        pytest.raises(
            ValueError,
            lambda: cursor.execute(
                """
                SELECT col_string, '%' FROM one_row_complex
                WHERE col_string LIKE %(param)s
                """,
                {"param": "%str%"},
            ),
        )
        cursor.execute(
            """
            SELECT col_string, '%%' FROM one_row_complex
            WHERE col_string LIKE %(param)s
            """,
            {"param": "%str%"},
        )
        assert cursor.fetchall() == [("a string", "%")]

    def test_escape(self, cursor):
        bad_str = """`~!@#$%^&*()_+-={}[]|\\;:'",./<>?\n\r\t """
        cursor.execute("SELECT %(a)d, %(b)s FROM one_row", {"a": 1, "b": bad_str})
        assert cursor.fetchall() == [
            (
                1,
                bad_str,
            )
        ]

    def test_none_empty_query(self, cursor):
        pytest.raises(ProgrammingError, lambda: cursor.execute(None))
        pytest.raises(ProgrammingError, lambda: cursor.execute(""))

    def test_invalid_params(self, cursor):
        pytest.raises(
            TypeError,
            lambda: cursor.execute("SELECT * FROM one_row", {"foo": {"bar": 1}}),
        )

    def test_open_close(self):
        with contextlib.closing(connect()):
            pass
        with contextlib.closing(connect()) as conn, conn.cursor():
            pass

    def test_unicode(self, cursor):
        unicode_str = "王兢"
        cursor.execute("SELECT %(param)s FROM one_row", {"param": unicode_str})
        assert cursor.fetchall() == [(unicode_str,)]

    def test_decimal(self, cursor):
        cursor.execute("SELECT %(decimal)s", {"decimal": Decimal("0.00000000001")})
        assert cursor.fetchall() == [(Decimal("0.00000000001"),)]

    def test_null(self, cursor):
        cursor.execute("SELECT null FROM many_rows")
        assert cursor.fetchall() == [(None,)] * 10000
        cursor.execute("SELECT IF(a % 11 = 0, null, a) FROM many_rows")
        assert cursor.fetchall() == [(None if a % 11 == 0 else a,) for a in range(10000)]

    def test_query_id(self, cursor):
        assert cursor.query_id is None
        cursor.execute("SELECT * from one_row")
        # query_id is UUID v4
        expected_pattern = (
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
        )
        assert re.match(expected_pattern, cursor.query_id)

    def test_output_location(self, cursor):
        assert cursor.output_location is None
        cursor.execute("SELECT * from one_row")
        assert cursor.output_location == f"{ENV.s3_staging_dir}{cursor.query_id}.csv"

    def test_query_execution_initial(self, cursor):
        assert not cursor.has_result_set
        assert cursor.rownumber is None
        assert cursor.rowcount == -1
        assert cursor.database is None
        assert cursor.catalog is None
        assert cursor.query_id is None
        assert cursor.query is None
        assert cursor.statement_type is None
        assert cursor.work_group is None
        assert cursor.state is None
        assert cursor.state_change_reason is None
        assert cursor.submission_date_time is None
        assert cursor.completion_date_time is None
        assert cursor.data_scanned_in_bytes is None
        assert cursor.engine_execution_time_in_millis is None
        assert cursor.query_queue_time_in_millis is None
        assert cursor.total_execution_time_in_millis is None
        assert cursor.query_planning_time_in_millis is None
        assert cursor.service_processing_time_in_millis is None
        assert cursor.output_location is None
        assert cursor.data_manifest_location is None
        assert cursor.encryption_option is None
        assert cursor.kms_key is None
        assert cursor.selected_engine_version is None
        assert cursor.effective_engine_version is None

    def test_temporal_types(self, cursor):
        """Temporal values convert as the contract of #1212 defines."""
        row = cursor.execute(temporal.query()).fetchone()
        assert row == temporal.fetched_row()
        assert temporal.zones(row) == temporal.zones(temporal.fetched_row())

    def test_complex(self, cursor):
        cursor.execute(
            """
            SELECT
              col_boolean
              ,col_tinyint
              ,col_smallint
              ,col_int
              ,col_bigint
              ,col_float
              ,col_double
              ,col_string
              ,col_varchar
              ,col_timestamp
              ,CAST(col_timestamp AS timestamp with time zone) AS col_timestamp_tz
              ,CAST(col_timestamp AS time) AS col_time
              ,col_date
              ,col_binary
              ,col_array
              ,CAST(col_array AS json) AS col_array_json
              ,col_map
              ,CAST(col_map AS json) AS col_map_json
              ,col_struct
              ,col_decimal
            FROM one_row_complex
            """
        )
        assert cursor.description == [
            ("col_boolean", "boolean", None, None, 0, 0, "UNKNOWN"),
            ("col_tinyint", "tinyint", None, None, 3, 0, "UNKNOWN"),
            ("col_smallint", "smallint", None, None, 5, 0, "UNKNOWN"),
            ("col_int", "integer", None, None, 10, 0, "UNKNOWN"),
            ("col_bigint", "bigint", None, None, 19, 0, "UNKNOWN"),
            ("col_float", "float", None, None, 17, 0, "UNKNOWN"),
            ("col_double", "double", None, None, 17, 0, "UNKNOWN"),
            ("col_string", "varchar", None, None, 2147483647, 0, "UNKNOWN"),
            ("col_varchar", "varchar", None, None, 10, 0, "UNKNOWN"),
            ("col_timestamp", "timestamp", None, None, 3, 0, "UNKNOWN"),
            ("col_timestamp_tz", "timestamp with time zone", None, None, 3, 0, "UNKNOWN"),
            ("col_time", "time", None, None, 3, 0, "UNKNOWN"),
            ("col_date", "date", None, None, 0, 0, "UNKNOWN"),
            ("col_binary", "varbinary", None, None, 1073741824, 0, "UNKNOWN"),
            ("col_array", "array", None, None, 0, 0, "UNKNOWN"),
            ("col_array_json", "json", None, None, 0, 0, "UNKNOWN"),
            ("col_map", "map", None, None, 0, 0, "UNKNOWN"),
            ("col_map_json", "json", None, None, 0, 0, "UNKNOWN"),
            ("col_struct", "row", None, None, 0, 0, "UNKNOWN"),
            ("col_decimal", "decimal", None, None, 10, 1, "UNKNOWN"),
        ]
        rows = cursor.fetchall()
        expected = [
            (
                True,
                127,
                32767,
                2147483647,
                9223372036854775807,
                0.5,
                0.25,
                "a string",
                "varchar",
                datetime(2017, 1, 1, 0, 0, 0),
                datetime(2017, 1, 1, 0, 0, 0, tzinfo=UTC),
                datetime(2017, 1, 1, 0, 0, 0).time(),
                date(2017, 1, 2),
                b"123",
                [1, 2],
                [1, 2],
                {"1": "2", "3": "4"},
                {"1": 2, "3": 4},
                {"a": "1", "b": "2"},
                Decimal("0.1"),
            )
        ]
        assert rows == expected
        # catch unicode/str
        assert list(map(type, rows[0])) == list(map(type, expected[0]))
        # compare dbapi type object
        assert [d[1] for d in cursor.description] == [
            BOOLEAN,
            NUMBER,
            NUMBER,
            NUMBER,
            NUMBER,
            NUMBER,
            NUMBER,
            STRING,
            STRING,
            DATETIME,
            DATETIME,
            TIME,
            DATE,
            BINARY,
            STRING,
            JSON,
            STRING,
            JSON,
            STRING,
            NUMBER,
        ]

    def test_complex_with_type_hints(self, cursor):
        # Without the hints, the numbers in the structs and the map are strings.
        cursor.execute(
            """
            SELECT
              col_array
              ,col_map
              ,col_struct
              ,CAST(
                ROW(ROW('2024-01-01', 123), 4.736, 0.583)
                AS ROW(header ROW(stamp VARCHAR, seq INTEGER), x DOUBLE, y DOUBLE)
              ) AS positions
              ,CAST(
                ARRAY[
                  ROW(ROW(1, 2), ROW(CAST(0.5 AS DOUBLE))),
                  ROW(ROW(3, 4), ROW(CAST(1.5 AS DOUBLE)))
                ]
                AS ARRAY(ROW(pos ROW(x INTEGER, y INTEGER), vel ROW(x DOUBLE)))
              ) AS data
            FROM one_row_complex
            """,
            result_set_type_hints={
                "col_array": "array(integer)",
                "col_map": "map(integer, integer)",
                "col_struct": "row(a integer, b integer)",
                "positions": "row(header row(stamp varchar, seq integer), x double, y double)",
                "data": "array(row(pos row(x integer, y integer), vel row(x double)))",
            },
        )
        expected = (
            [1, 2],
            {"1": 2, "3": 4},
            {"a": 1, "b": 2},
            {"header": {"stamp": "2024-01-01", "seq": 123}, "x": 4.736, "y": 0.583},
            [
                {"pos": {"x": 1, "y": 2}, "vel": {"x": 0.5}},
                {"pos": {"x": 3, "y": 4}, "vel": {"x": 1.5}},
            ],
        )
        row = cursor.fetchone()
        assert row == expected
        # == does not tell 123 from 123.0.
        assert _types(row) == _types(expected)

    def test_json_elements_with_type_hints(self, cursor):
        # JSON leaves the dates in the JSON elements strings; only the hints make them dates.
        cursor.execute(
            """
            SELECT
              ARRAY[json_parse('"2024-01-01"'), NULL] AS col_dates
              ,ARRAY[json_parse('{"d": "2024-01-01"}')] AS col_maps
              ,ARRAY[json_parse('{"d": "2024-01-01"}')] AS col_rows
            """,
            result_set_type_hints={
                "col_dates": "array(date)",
                "col_maps": "array(map(varchar, date))",
                "col_rows": "array(row(d date))",
            },
        )
        assert cursor.fetchone() == (
            [date(2024, 1, 1), None],
            [{"d": date(2024, 1, 1)}],
            [{"d": date(2024, 1, 1)}],
        )

    def test_cancel(self, cursor):
        def cancel(c):
            time.sleep(randint(5, 10))
            c.cancel()

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(cancel, cursor)

            pytest.raises(
                DatabaseError,
                lambda: cursor.execute(
                    """
                    SELECT a.a * rand(), b.a * rand()
                    FROM many_rows a
                    CROSS JOIN many_rows b
                    """
                ),
            )

    def test_cancel_initial(self, cursor):
        pytest.raises(ProgrammingError, cursor.cancel)

    def test_multiple_connection(self):
        def execute_other_thread():
            with (
                contextlib.closing(connect(schema_name=ENV.schema)) as conn,
                conn.cursor() as cursor,
            ):
                cursor.execute("SELECT * FROM one_row")
                return cursor.fetchall()

        with ThreadPoolExecutor(max_workers=2) as executor:
            fs = [executor.submit(execute_other_thread) for _ in range(2)]
            for f in futures.as_completed(fs):
                assert f.result() == [(1,)]

    def test_no_ops(self):
        conn = connect()
        cursor = conn.cursor()
        assert cursor.rowcount == -1
        cursor.setinputsizes([])
        cursor.setoutputsize(1, "blah")
        conn.commit()
        pytest.raises(NotSupportedError, lambda: conn.rollback())
        cursor.close()
        conn.close()

    def test_show_partition(self, cursor):
        table_name = f"partition_{uuid.uuid4().hex}"
        table = f"{ENV.schema}.{table_name}"
        location = f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/"
        try:
            cursor.execute(
                f"""
                CREATE EXTERNAL TABLE {table} (a STRING)
                PARTITIONED BY (b INT)
                LOCATION '{location}'
                """
            )
            for i in range(10):
                cursor.execute(
                    f"""
                    ALTER TABLE {table} ADD PARTITION (b=%(b)d)
                    LOCATION %(location)s
                    """,
                    {"b": i, "location": location},
                )
            cursor.execute(f"SHOW PARTITIONS {table}")
            assert sorted(cursor.fetchall()) == [(f"b={i}",) for i in range(10)]
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")

    @pytest.mark.parametrize("cursor", [{"work_group": ENV.work_group}], indirect=["cursor"])
    def test_workgroup(self, cursor):
        cursor.execute("SELECT * FROM one_row")
        assert cursor.work_group == ENV.work_group

    @pytest.mark.parametrize("cursor", [{"work_group": ENV.work_group}], indirect=["cursor"])
    def test_no_s3_staging_dir(self, cursor):
        cursor._s3_staging_dir = None
        cursor.execute("SELECT * FROM one_row")
        assert cursor.output_location

    def test_executemany(self, cursor, empty_table):
        rows = [(1, "foo"), (2, "bar"), (3, "jim o'rourke")]
        cursor.executemany(
            f"INSERT INTO {empty_table} (a, b) VALUES (%(a)d, %(b)s)",
            [{"a": a, "b": b} for a, b in rows],
        )
        assert cursor.rowcount == len(rows)
        cursor.execute(f"SELECT * FROM {empty_table}")
        assert sorted(cursor.fetchall()) == list(rows)

    # The sum of the update counts does not depend on the statement; test_iceberg_table
    # checks the update count of INSERT, UPDATE, and DELETE.
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
    def test_executemany_rowcount(self, cursor, executemany_table, operation, expected):
        operation = operation.format(table=executemany_table)
        cursor.execute(f"SELECT id, value FROM {executemany_table} ORDER BY id")
        previous = cursor.result_set
        result = cursor.executemany(
            operation,
            [{"group_id": 1}, {"group_id": 2}, {"group_id": 99}],
            work_group=ENV.default_work_group,
        )
        assert result is None
        assert cursor.rowcount == 3
        assert cursor.description is None
        assert cursor.result_set is None
        assert cursor.query_id is None
        assert previous.is_closed

        cursor.executemany(operation, [])
        assert cursor.rowcount == 0
        cursor.executemany(operation, [{"group_id": 99}, {"group_id": 100}])
        assert cursor.rowcount == 0
        cursor.close()
        assert cursor.rowcount == -1

        cursor.execute(f"SELECT id, value FROM {executemany_table} ORDER BY id")
        assert cursor.rowcount == -1
        assert cursor.fetchall() == expected

    @pytest.mark.parametrize("failure_index", [0, 1])
    def test_executemany_failure(self, cursor, executemany_table, failure_index):
        operation = (
            f"UPDATE {executemany_table} SET value=value+1 "
            "WHERE group_id=CAST(%(group_id)s AS INTEGER)"
        )
        parameters = [{"group_id": "1"}] * failure_index + [
            {"group_id": "invalid"},
            {"group_id": "2"},
        ]
        with pytest.raises(OperationalError):
            cursor.executemany(operation, parameters)
        assert cursor.rowcount == -1
        assert cursor.description is None
        assert cursor.result_set is None
        assert cursor.query_id

        cursor.execute(f"SELECT id, value FROM {executemany_table} ORDER BY id")
        assert cursor.fetchall() == [(1, 10 + failure_index), (2, 20 + failure_index), (3, 30)]
        cursor.execute(operation, {"group_id": "2"})
        assert cursor.rowcount == 1

    def test_executemany_parameter_iteration_failure(self, cursor, executemany_table):
        previous = None
        query_id = None

        def parameters():
            nonlocal previous, query_id
            yield {"group_id": 1}
            previous = cursor.result_set
            query_id = cursor.query_id
            raise ValueError("invalid parameters")

        with pytest.raises(ValueError, match="invalid parameters"):
            cursor.executemany(
                f"UPDATE {executemany_table} SET value=value+1 WHERE group_id=%(group_id)d",
                parameters(),
            )
        assert cursor.rowcount == -1
        assert cursor.result_set is None
        assert query_id is not None
        assert cursor.query_id == query_id
        assert previous is not None
        assert previous.is_closed
        cursor.execute(f"SELECT id, value FROM {executemany_table} ORDER BY id")
        assert cursor.fetchall() == [(1, 11), (2, 21), (3, 30)]

    def test_executemany_fetch(self, cursor):
        cursor.executemany("SELECT %(x)d FROM one_row", [])
        assert cursor.rowcount == 0
        cursor.executemany("SELECT %(x)d FROM one_row", [{"x": i} for i in range(1, 2)])
        assert cursor.rowcount == -1
        # Operations that have result sets are not allowed with executemany.
        pytest.raises(ProgrammingError, cursor.fetchall)
        pytest.raises(ProgrammingError, cursor.fetchmany)
        pytest.raises(ProgrammingError, cursor.fetchone)

    def test_iceberg_table(self, cursor):
        iceberg_table = "test_iceberg_table_cursor"
        cursor.execute(
            f"""
            CREATE TABLE {ENV.schema}.{iceberg_table} (
              id INT,
              col1 STRING
            )
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{iceberg_table}/'
            tblproperties('table_type'='ICEBERG')
            """
        )
        cursor.execute(
            f"""
            INSERT INTO {ENV.schema}.{iceberg_table} (id, col1)
            VALUES (1, 'test1'), (2, 'test2')
            """
        )
        assert cursor.description is None
        assert cursor.rowcount == 2
        cursor.execute(
            f"""
            SELECT COUNT(*) FROM {ENV.schema}.{iceberg_table}
            """
        )
        assert cursor.fetchall() == [(2,)]
        assert cursor.rowcount == -1

        cursor.execute(
            f"""
            UPDATE {ENV.schema}.{iceberg_table}
            SET col1 = 'test1_update'
            WHERE id = 1
            """
        )
        assert cursor.description is None
        assert cursor.rowcount == 1
        cursor.execute(
            f"""
            SELECT col1
            FROM {ENV.schema}.{iceberg_table}
            WHERE id = 1
            """
        )
        assert cursor.fetchall() == [("test1_update",)]

        cursor.execute(
            f"""
            CREATE TABLE {ENV.schema}.{iceberg_table}_merge
            WITH (
              table_type = 'ICEBERG',
              location = '{ENV.s3_staging_dir}{ENV.schema}/{iceberg_table}_merge/',
              is_external = false
            )
            AS SELECT 1 AS id, 'foobar' AS col1
            """
        )
        assert cursor.rowcount == 1
        cursor.execute(
            f"""
            MERGE INTO {ENV.schema}.{iceberg_table} AS t1
            USING {ENV.schema}.{iceberg_table}_merge AS t2
              ON t1.id = t2.id
            WHEN MATCHED
              THEN UPDATE SET col1 = t2.col1
            """
        )
        assert cursor.description is None
        assert cursor.rowcount == 1
        cursor.execute(
            f"""
            SELECT col1
            FROM {ENV.schema}.{iceberg_table}
            WHERE id = 1
            """
        )
        assert cursor.fetchall() == [("foobar",)]

        cursor.execute(
            f"""
            VACUUM {ENV.schema}.{iceberg_table}
            """
        )

        cursor.execute(
            f"""
            DELETE FROM {ENV.schema}.{iceberg_table}
            WHERE id = 2
            """
        )
        assert cursor.description is None
        assert cursor.rowcount == 1
        cursor.execute(
            f"""
            SELECT COUNT(*) FROM {ENV.schema}.{iceberg_table}
            """
        )
        assert cursor.fetchall() == [(1,)]

    def test_execute_with_callback(self, cursor):
        """Test execute with on_start_query_execution callback."""
        callback_results = []

        def on_start(query_id):
            # Callback should be called immediately after start_query_execution
            assert query_id is not None
            assert len(query_id) > 0
            callback_results.append(query_id)

        # Execute with callback
        result = cursor.execute("SELECT 1", on_start_query_execution=on_start)

        # Verify callback was called
        assert len(callback_results) == 1
        callback_query_id = callback_results[0]

        # Verify the query ID is consistent
        assert callback_query_id == cursor.query_id
        assert result is cursor

        # Verify the query completed successfully
        row = cursor.fetchone()
        assert row == (1,)

    def test_execute_with_options(self, cursor):
        """Test execute with an ExecuteOptions instance instead of individual kwargs."""
        callback_results = []
        options = ExecuteOptions(
            on_start_query_execution=callback_results.append,
            result_reuse_enable=False,
        )
        result = cursor.execute("SELECT 1", options=options)
        assert callback_results == [cursor.query_id]
        assert result is cursor
        assert cursor.fetchone() == (1,)

    def test_execute_kwargs_override_options(self, cursor):
        """Test that individual execute() kwargs take precedence over options fields."""
        from_options = []
        from_kwargs = []
        options = ExecuteOptions(on_start_query_execution=from_options.append)
        cursor.execute(
            "SELECT 1",
            options=options,
            on_start_query_execution=from_kwargs.append,
        )
        assert from_options == []
        assert from_kwargs == [cursor.query_id]
        assert cursor.fetchone() == (1,)

    def test_execute_internal_options_passthrough(self):
        """The private _execute() forwards its options to the request and the cache (no AWS)."""
        cursor = Cursor.__new__(Cursor)  # bypass __init__ to avoid AWS calls
        cursor._connection = MagicMock()
        cursor._connection.client.start_query_execution.return_value = {
            "QueryExecutionId": "test_query_id"
        }
        cursor._retry_config = RetryConfig()
        cursor._kill_on_interrupt = True

        with (
            patch.object(
                Cursor, "_build_start_query_execution_request", return_value={}
            ) as request_mock,
            patch.object(Cursor, "_find_previous_query_id", return_value=None) as cache_mock,
        ):
            query_id = cursor._execute(
                "SELECT 1",
                parameters=None,
                options=ExecuteOptions(
                    work_group="test_work_group",
                    s3_staging_dir="s3://test-bucket/path/",
                    cache_size=10,
                    cache_expiration_time=100,
                    result_reuse_enable=True,
                    result_reuse_minutes=5,
                    paramstyle="qmark",
                ),
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
        cache_mock.assert_called_once_with(
            "SELECT 1",
            "test_work_group",
            cache_size=10,
            cache_expiration_time=100,
        )

    def test_connection_level_callback(self):
        """Test connection-level default callback."""
        callback_results = []

        def connection_callback(query_id):
            callback_results.append(("connection", query_id))

        # Create connection with callback
        with contextlib.closing(connect(on_start_query_execution=connection_callback)) as conn:
            cursor = conn.cursor()

            # Execute without callback - should use connection default
            cursor.execute("SELECT 3")

            # Verify connection callback was called
            assert len(callback_results) == 1
            assert callback_results[0][0] == "connection"
            assert callback_results[0][1] == cursor.query_id

            row = cursor.fetchone()
            assert row == (3,)

    def test_multiple_callbacks_invoked(self):
        """Test that both connection and execute callbacks are invoked when both are set."""
        connection_callbacks = []
        execute_callbacks = []

        def connection_callback(query_id):
            connection_callbacks.append(query_id)

        def execute_callback(query_id):
            execute_callbacks.append(query_id)

        # Create connection with callback
        with contextlib.closing(connect(on_start_query_execution=connection_callback)) as conn:
            cursor = conn.cursor()

            # Execute with specific callback - both should be called
            cursor.execute("SELECT 4", on_start_query_execution=execute_callback)

            # Verify both callbacks were called
            assert len(connection_callbacks) == 1
            assert len(execute_callbacks) == 1
            assert connection_callbacks[0] == execute_callbacks[0]  # Same query_id
            assert connection_callbacks[0] == cursor.query_id

    @pytest.mark.parametrize(
        "final_state",
        [AthenaQueryExecution.STATE_CANCELLED, AthenaQueryExecution.STATE_SUCCEEDED],
    )
    def test_execute_kill_on_interrupt(self, final_state):
        """An interrupt cancels the query, waits for it, and is re-raised (no AWS)."""
        polled = []
        cursor, cancel = _offline_cursor(kill_on_interrupt=True)
        cursor._on_poll = polled.append
        cursor._get_query_execution = MagicMock(
            side_effect=[
                KeyboardInterrupt(),
                MagicMock(state=AthenaQueryExecution.STATE_RUNNING),
                MagicMock(state=final_state),
            ]
        )
        with pytest.raises(KeyboardInterrupt):
            cursor.execute("SELECT 1")

        cancel.assert_called_once_with("query_id")
        # The interrupt is re-raised only after the query reaches a terminal state.
        assert [execution.state for execution in polled] == [
            AthenaQueryExecution.STATE_RUNNING,
            final_state,
        ]
        assert cursor.query_id == "query_id"
        assert cursor.result_set is None

    @pytest.mark.parametrize("failing", ["cancel", "wait"])
    def test_execute_kill_on_interrupt_failure(self, failing):
        """A failure to cancel or wait becomes the cause of the interrupt (no AWS)."""
        error = OperationalError("failed")
        cursor, cancel = _offline_cursor(kill_on_interrupt=True)
        cursor._get_query_execution = MagicMock(side_effect=[KeyboardInterrupt(), error])
        if failing == "cancel":
            cancel.side_effect = error
        with pytest.raises(KeyboardInterrupt) as exc_info:
            cursor.execute("SELECT 1")

        assert exc_info.value.__cause__ is error
        cancel.assert_called_once_with("query_id")

    def test_execute_without_kill_on_interrupt(self):
        """Without kill_on_interrupt, an interrupt propagates without cancellation (no AWS)."""
        cursor, cancel = _offline_cursor(kill_on_interrupt=False)
        cursor._get_query_execution = MagicMock(side_effect=[KeyboardInterrupt()])
        with pytest.raises(KeyboardInterrupt):
            cursor.execute("SELECT 1")

        cancel.assert_not_called()
        assert cursor.query_id == "query_id"

    @pytest.mark.parametrize(
        "final_state",
        [AthenaQueryExecution.STATE_CANCELLED, AthenaQueryExecution.STATE_SUCCEEDED],
    )
    def test_execute_interrupted_while_starting(self, final_state):
        """An interrupt during the start request stops the query it starts (no AWS)."""
        polled = []
        cursor, cancel = _offline_cursor(kill_on_interrupt=True)
        cursor._on_poll = polled.append
        cursor._get_query_execution = MagicMock(
            side_effect=[
                MagicMock(state=AthenaQueryExecution.STATE_RUNNING),
                MagicMock(state=final_state),
            ]
        )
        started, release = _block_start(cursor)
        waits, raised = interrupt_start_waits(started, release)

        with waits, pytest.raises(KeyboardInterrupt) as exc_info:
            cursor.execute("SELECT 1")

        assert exc_info.value is raised[0]
        assert exc_info.value.__cause__ is None
        cursor._connection.client.start_query_execution.assert_called_once()
        cancel.assert_called_once_with("query_id")
        # The interrupt is re-raised only after the query reaches a terminal state.
        assert [execution.state for execution in polled] == [
            AthenaQueryExecution.STATE_RUNNING,
            final_state,
        ]
        assert cursor.query_id == "query_id"
        assert cursor.result_set is None

    @pytest.mark.parametrize("failing", ["start", "cancel"])
    def test_execute_interrupted_while_starting_failure(self, failing):
        """A failure to start or cancel becomes the cause of the interrupt (no AWS)."""
        error = OperationalError("failed")
        cursor, cancel = _offline_cursor(kill_on_interrupt=True)
        started, release = _block_start(
            cursor,
            response=ClientError(
                {"Error": {"Code": "InvalidRequestException", "Message": "failed"}},
                "StartQueryExecution",
            )
            if failing == "start"
            else None,
        )
        if failing == "cancel":
            cancel.side_effect = error
        waits, raised = interrupt_start_waits(started, release)

        with waits, pytest.raises(KeyboardInterrupt) as exc_info:
            cursor.execute("SELECT 1")

        assert exc_info.value is raised[0]
        if failing == "start":
            assert isinstance(exc_info.value.__cause__, DatabaseError)
            cancel.assert_not_called()
            assert cursor.query_id is None
        else:
            assert exc_info.value.__cause__ is error
            cancel.assert_called_once_with("query_id")
            assert cursor.query_id == "query_id"

    def test_execute_interrupted_while_starting_without_kill_on_interrupt(self):
        """Without kill_on_interrupt, the request runs on the caller's thread (no AWS)."""
        cursor, cancel = _offline_cursor(kill_on_interrupt=False)
        threads = []

        def start_query_execution(**kwargs):
            threads.append(threading.current_thread())
            raise KeyboardInterrupt

        cursor._connection.client.start_query_execution.side_effect = start_query_execution

        with pytest.raises(KeyboardInterrupt):
            cursor.execute("SELECT 1")

        assert threads == [threading.current_thread()]
        cancel.assert_not_called()
        assert cursor.query_id is None

    def test_async_cursor_execute_interrupted_while_starting(self):
        """AsyncCursor starts queries on the caller's thread, so it stops them too (no AWS)."""
        cursor, cancel = _offline_cursor(kill_on_interrupt=True, cursor_class=AsyncCursor)
        cursor._get_query_execution = MagicMock(
            return_value=MagicMock(state=AthenaQueryExecution.STATE_CANCELLED)
        )
        started, release = _block_start(cursor)
        waits, raised = interrupt_start_waits(started, release)

        with waits, pytest.raises(KeyboardInterrupt) as exc_info:
            cursor.execute("SELECT 1")

        assert exc_info.value is raised[0]
        cancel.assert_called_once_with("query_id")

    def test_on_poll_connection_level(self):
        """Connection-level on_poll fires during query execution."""
        states = []

        with contextlib.closing(
            connect(on_poll=lambda execution: states.append(execution.state))
        ) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")

            assert len(states) >= 1
            assert states[-1] == AthenaQueryExecution.STATE_SUCCEEDED
            assert cursor.fetchone() == (1,)

    def test_on_poll_cursor_level(self):
        """on_poll passed via cursor() fires during query execution."""
        states = []

        with contextlib.closing(connect()) as conn:
            cursor = conn.cursor(on_poll=lambda execution: states.append(execution.state))
            cursor.execute("SELECT 1")

            assert len(states) >= 1
            assert states[-1] == AthenaQueryExecution.STATE_SUCCEEDED
            assert cursor.fetchone() == (1,)

    def test_null_vs_empty_string(self, cursor):
        """
        Default Cursor should properly distinguish NULL from empty string.
        See docs/null_handling.rst for details.
        """
        query = """
        SELECT * FROM (
            VALUES
                (1, '', 'empty_string'),
                (2, CAST(NULL AS VARCHAR), 'null_value'),
                (3, 'hello', 'normal_string'),
                (4, 'N/A', 'na_string'),
                (5, 'NULL', 'null_string_literal')
        ) AS t(id, value, description)
        ORDER BY id
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        # Row 1: Empty string is preserved as empty string, not None
        assert rows[0][1] == ""
        assert rows[0][1] is not None

        # Row 2: NULL is properly returned as None
        assert rows[1][1] is None

        # Row 3: Normal string
        assert rows[2][1] == "hello"

        # Row 4: "N/A" string should be preserved as-is
        assert rows[3][1] == "N/A"

        # Row 5: "NULL" string literal should be preserved as-is
        assert rows[4][1] == "NULL"

    @pytest.mark.parametrize(
        "cursor",
        [
            pytest.param({}, id="default"),
            pytest.param(
                {"work_group": ENV.managed_work_group, "s3_staging_dir": ""},
                id="managed",
                marks=pytest.mark.skipif(
                    not ENV.managed_work_group,
                    reason="AWS_ATHENA_MANAGED_WORKGROUP not set",
                ),
            ),
        ],
        indirect=["cursor"],
    )
    def test_fetch_all_rows(self, cursor):
        cursor.execute(CONVERTED_VALUES_QUERY)
        assert cursor.fetchall() == [CONVERTED_VALUES_ROW]

    @pytest.mark.parametrize(
        "cursor",
        [
            pytest.param({}, id="default"),
            pytest.param(
                {"work_group": ENV.managed_work_group, "s3_staging_dir": ""},
                id="managed",
                marks=pytest.mark.skipif(
                    not ENV.managed_work_group,
                    reason="AWS_ATHENA_MANAGED_WORKGROUP not set",
                ),
            ),
        ],
        indirect=["cursor"],
    )
    def test_ambiguous_time_zone_value(self, cursor):
        """A local time that its zone repeats raises DataError instead of choosing one.

        The first page of rows is converted by execute().
        """
        with pytest.raises(DataError, match="is ambiguous"):
            cursor.execute(AMBIGUOUS_TIME_ZONE_QUERY)

    @pytest.mark.parametrize(
        "cursor",
        [
            pytest.param({}, id="default"),
            pytest.param(
                {"work_group": ENV.managed_work_group, "s3_staging_dir": ""},
                id="managed",
                marks=pytest.mark.skipif(
                    not ENV.managed_work_group,
                    reason="AWS_ATHENA_MANAGED_WORKGROUP not set",
                ),
            ),
        ],
        indirect=["cursor"],
    )
    def test_fetch_complex_values(self, cursor):
        """Native arrays keep empty, space-only, and comma items; array(json) keeps JSON."""
        query = """
            SELECT
              ARRAY['x', '', 'y'] AS col_empty
              ,ARRAY['x', ' ', 'a,b'] AS col_space_comma
              ,ARRAY[json_parse('1234567890'), json_parse('"a,b"')] AS col_json
              ,ARRAY[DATE '2023-01-01', DATE '2023-12-31'] AS col_date
              ,ARRAY[CAST('2023-01-01 12:00:00.123' AS TIMESTAMP(3))] AS col_timestamp
            """
        cursor.execute(query)
        # Without a hint, the dates and timestamps in an array stay strings.
        expected = (
            ["x", "", "y"],
            ["x", " ", "a,b"],
            [1234567890, "a,b"],
            ["2023-01-01", "2023-12-31"],
            ["2023-01-01 12:00:00.123"],
        )
        assert cursor.fetchall() == [expected]
        cursor.execute(
            query,
            result_set_type_hints={
                "col_empty": "array(varchar)",
                "col_space_comma": "array(varchar)",
                "col_json": "array(json)",
            },
        )
        assert cursor.fetchall() == [expected]

    @staticmethod
    def _metadata_view(metadata):
        return (
            metadata.name,
            metadata.table_type,
            metadata.create_time,
            [(c.name, c.type, c.comment) for c in metadata.columns],
            [(c.name, c.type, c.comment) for c in metadata.partition_keys],
            metadata.parameters,
        )

    def test_throttled_metadata_reads_glue(self, cursor, monkeypatch):
        tables = ("one_row", "parquet_with_compression", "partition_table", "view_one_row")

        def read():
            return (
                [self._metadata_view(cursor.get_table_metadata(t)) for t in tables],
                sorted(self._metadata_view(m) for m in cursor.list_table_metadata()),
                sorted(m.name for m in cursor.list_table_metadata(expression="one_row")),
                ENV.schema in [d.name for d in cursor.list_databases("AwsDataCatalog")],
            )

        expected = read()
        # The listing filtered by the expression is not empty.
        assert "one_row" in expected[2]
        calls = throttle_metadata_api(cursor.connection.client, monkeypatch)

        # Glue reports each table as Athena does, and nothing is retried first.
        assert read() == expected
        assert sorted(calls) == sorted(
            ["get_table_metadata"] * len(tables) + ["list_table_metadata"] * 2 + ["list_databases"]
        )

    @staticmethod
    def _unreachable_glue(connection, monkeypatch):
        glue = unreachable_glue(connection)
        monkeypatch.setattr(connection, "_glue", glue)
        return glue

    @pytest.mark.parametrize(
        "cursor", [{"retry_config": RetryConfig(attempt=1)}], indirect=["cursor"]
    )
    def test_glue_answer_that_the_table_is_missing(self, cursor, monkeypatch):
        # Glue's answer about absence is raised as Athena's would be.
        calls = throttle_metadata_api(cursor.connection.client, monkeypatch)

        with pytest.raises(OperationalError) as caught:
            cursor.get_table_metadata("no_such_table_786")

        assert caught.value.__cause__.response["Error"]["Code"] == "EntityNotFoundException"
        assert calls == ["get_table_metadata"]

    @pytest.mark.parametrize(
        "cursor", [{"retry_config": RetryConfig(attempt=1)}], indirect=["cursor"]
    )
    def test_missing_database_in_glue_listing_asks_athena(self, cursor, monkeypatch):
        # Only a table lookup takes Glue's answer about absence as final; a
        # listing asks Athena again, whose error for a missing catalog or
        # database callers may already handle.
        calls = throttle_metadata_api(cursor.connection.client, monkeypatch)

        with pytest.raises(OperationalError) as caught:
            cursor.list_table_metadata(schema_name="no_such_database_786")

        assert caught.value.__cause__.response["Error"]["Code"] == "ThrottlingException"
        assert calls == ["list_table_metadata"] * 2
        assert cursor.connection._glue.reachable

    @pytest.mark.parametrize(
        "cursor", [{"retry_config": RetryConfig(attempt=1)}], indirect=["cursor"]
    )
    def test_unreachable_glue_is_not_tried_again(self, cursor, monkeypatch):
        calls = throttle_metadata_api(cursor.connection.client, monkeypatch)
        glue = self._unreachable_glue(cursor.connection, monkeypatch)

        # The Athena request runs again with its policy after Glue fails.
        with pytest.raises(OperationalError) as caught:
            cursor.get_table_metadata("one_row")
        assert caught.value.__cause__.response["Error"]["Code"] == "ThrottlingException"
        assert calls == ["get_table_metadata"] * 2
        assert not glue.reachable

        # A later request does not wait for Glue again.
        with pytest.raises(OperationalError):
            cursor.get_table_metadata("one_row")
        assert calls == ["get_table_metadata"] * 3

    # A policy that retries every MetadataException.
    _METADATA_EXCEPTION_POLICY = RetryConfig(
        exceptions=("ThrottlingException", "MetadataException"), attempt=2, multiplier=0
    )

    @pytest.mark.parametrize(
        "cursor", [{"retry_config": _METADATA_EXCEPTION_POLICY}], indirect=["cursor"]
    )
    def test_wrapped_glue_throttling_skips_metadata_exception_retries(self, cursor, monkeypatch):
        calls = throttle_metadata_api(
            cursor.connection.client,
            monkeypatch,
            code="MetadataException",
            message=(
                "Rate exceeded (Service: AmazonDataCatalog; Status Code: 400; "
                "Error Code: ThrottlingException; Request ID: example; Proxy: null)"
            ),
        )

        assert cursor.get_table_metadata("one_row").name == "one_row"
        assert calls == ["get_table_metadata"]

    @pytest.mark.parametrize(
        "cursor", [{"retry_config": _METADATA_EXCEPTION_POLICY}], indirect=["cursor"]
    )
    def test_other_metadata_exception_keeps_its_retries(self, cursor, monkeypatch):
        calls = throttle_metadata_api(
            cursor.connection.client,
            monkeypatch,
            code="MetadataException",
            message="Unrecognized connector error",
        )

        with pytest.raises(OperationalError):
            cursor.get_table_metadata("one_row")
        assert calls == ["get_table_metadata"] * 2

    # Without the Glue fallback, a throttled page is retried, so Athena serves every page.
    @pytest.mark.parametrize("cursor", [{"glue_metadata_fallback": False}], indirect=["cursor"])
    def test_listing_reads_every_page(self, cursor, monkeypatch):
        client = cursor.connection.client
        list_table_metadata = client.list_table_metadata
        tokens = []

        def record_token(**kwargs):
            tokens.append(kwargs.get("NextToken"))
            return list_table_metadata(**kwargs)

        monkeypatch.setattr(client, "list_table_metadata", record_token)
        # Other tests add and drop their own tables, so only the session's are compared.
        session_tables = sorted(t.name for t in (*TABLES, *VIEWS))

        names = [m.name for m in cursor.list_table_metadata(max_results=4)]

        assert sorted(n for n in names if n in session_tables) == session_tables
        # At four per page, the session's tables and views span three pages or more.
        assert len(set(tokens)) >= 3

    @pytest.mark.parametrize(
        "cursor", [{"retry_config": RetryConfig(attempt=1)}], indirect=["cursor"]
    )
    def test_listing_resumes_after_a_failed_glue_request(self, cursor, monkeypatch):
        # A page throttled mid-listing is read again, not the pages before it.
        # The pages are stubbed so that real throttling cannot change the requests.
        pages = {
            None: {"TableMetadataList": [{"Name": "t0"}, {"Name": "t1"}], "NextToken": "1"},
            "1": {"TableMetadataList": [{"Name": "t2"}, {"Name": "t3"}], "NextToken": "2"},
            "2": {"TableMetadataList": [{"Name": "t4"}]},
        }
        client = cursor.connection.client
        requests = []

        def throttle_second_request_once(**kwargs):
            token = kwargs.get("NextToken")
            requests.append(token)
            if len(requests) == 2:
                raise ClientError(
                    {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}},
                    "ListTableMetadata",
                )
            return pages[token]

        monkeypatch.setattr(client, "list_table_metadata", throttle_second_request_once)
        glue = self._unreachable_glue(cursor.connection, monkeypatch)

        assert [m.name for m in cursor.list_table_metadata()] == ["t0", "t1", "t2", "t3", "t4"]
        assert requests == [None, "1", "1", "2"]
        assert not glue.reachable

    @pytest.mark.parametrize(
        ("cursor", "catalog_name"),
        [
            ({"retry_config": RetryConfig(attempt=1), "glue_metadata_fallback": False}, None),
            ({"retry_config": RetryConfig(attempt=1)}, "federated_catalog"),
        ],
        indirect=["cursor"],
        ids=["disabled", "outside-glue"],
    )
    def test_throttled_metadata_without_glue(self, cursor, monkeypatch, catalog_name):
        calls = throttle_metadata_api(cursor.connection.client, monkeypatch)
        # Still reachable afterwards means no request was sent to it.
        glue = self._unreachable_glue(cursor.connection, monkeypatch)

        with pytest.raises(OperationalError):
            cursor.get_table_metadata("one_row", catalog_name=catalog_name)

        assert calls == ["get_table_metadata"]
        assert glue.reachable


class TestDictCursor:
    def test_dict_type(self, dict_cursor):
        with dict_cursor.connection.cursor(dict_type=OrderedDict) as ordered_cursor:
            ordered_cursor.execute("SELECT * FROM one_row")
            assert type(ordered_cursor.fetchone()) is OrderedDict
        # dict_type of another cursor does not change the row type of this one.
        dict_cursor.execute("SELECT * FROM one_row")
        assert type(dict_cursor.fetchone()) is dict

    def test_dict_type_custom_result_set(self, dict_cursor):
        class CustomResultSet(AthenaDictResultSet):
            pass

        class CustomDictCursor(DictCursor):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self._result_set_class = CustomResultSet

        with dict_cursor.connection.cursor(CustomDictCursor, dict_type=OrderedDict) as cursor:
            cursor.execute("SELECT * FROM one_row")
            assert isinstance(cursor.result_set, CustomResultSet)
            assert type(cursor.fetchone()) is OrderedDict

    def test_null_vs_empty_string(self, dict_cursor):
        """
        DictCursor should properly distinguish NULL from empty string.
        See docs/null_handling.rst for details.
        """
        query = """
        SELECT * FROM (
            VALUES
                (1, '', 'empty_string'),
                (2, CAST(NULL AS VARCHAR), 'null_value'),
                (3, 'hello', 'normal_string'),
                (4, 'N/A', 'na_string'),
                (5, 'NULL', 'null_string_literal')
        ) AS t(id, value, description)
        ORDER BY id
        """
        dict_cursor.execute(query)
        rows = dict_cursor.fetchall()

        # Row 1: Empty string is preserved as empty string, not None
        assert rows[0]["value"] == ""
        assert rows[0]["value"] is not None

        # Row 2: NULL is properly returned as None
        assert rows[1]["value"] is None

        # Row 3: Normal string
        assert rows[2]["value"] == "hello"

        # Row 4: "N/A" string should be preserved as-is
        assert rows[3]["value"] == "N/A"

        # Row 5: "NULL" string literal should be preserved as-is
        assert rows[4]["value"] == "NULL"
