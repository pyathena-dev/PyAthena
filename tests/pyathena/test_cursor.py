import contextlib
import json
import logging
import random
import re
import string
import threading
import time
from concurrent import futures
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import date, datetime, timezone
from decimal import Decimal
from random import randint

import pytest

from pyathena import BINARY, BOOLEAN, DATE, DATETIME, JSON, NUMBER, STRING, TIME
from pyathena.converter import _to_array, _to_map, _to_struct
from pyathena.cursor import Cursor
from pyathena.error import DatabaseError, NotSupportedError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from tests import ENV
from tests.pyathena.conftest import connect

_logger = logging.getLogger(__name__)


class TestCursor:
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
        cursor.execute("SELECT * FROM one_row")
        assert cursor.fetchall() == [(1,)]
        cursor.execute("SELECT a FROM many_rows ORDER BY a")
        assert cursor.fetchall() == [(i,) for i in range(10000)]

    def test_iterator(self, cursor):
        cursor.execute("SELECT * FROM one_row")
        assert list(cursor) == [(1,)]
        pytest.raises(StopIteration, cursor.__next__)

    def test_cache_size(self, cursor):
        # To test caching, we need to make sure the query is unique, otherwise
        # we might accidentally pick up the cache results from another CI run.
        query = f"SELECT * FROM one_row -- {datetime.now(timezone.utc)!s}"

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
        now = datetime.now(timezone.utc)
        cursor.execute("SELECT %(now)s as date", {"now": now})
        first_query_id = cursor.query_id

        cursor.execute("SELECT %(now)s as date", {"now": now})
        second_query_id = cursor.query_id

        cursor.execute("SELECT %(now)s as date", {"now": now}, cache_size=100)
        third_query_id = cursor.query_id

        assert first_query_id != second_query_id
        assert third_query_id in [first_query_id, second_query_id]

    def test_cache_expiration_time(self, cursor):
        query = f"SELECT * FROM one_row -- {datetime.now(timezone.utc)!s}"

        cursor.execute(query)
        query_id_1 = cursor.query_id

        cursor.execute(query)
        query_id_2 = cursor.query_id

        cursor.execute(query, cache_expiration_time=3600)  # 1 hours
        query_id_3 = cursor.query_id

        assert query_id_1 != query_id_2
        assert query_id_3 in [query_id_1, query_id_2]

    def test_cache_expiration_time_with_cache_size(self, cursor):
        # Cache miss
        query = f"SELECT * FROM one_row -- {datetime.now(timezone.utc)!s}"

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
        query = f"SELECT * FROM one_row -- {datetime.now(timezone.utc)!s}"

        cursor.execute(query)
        query_id_4 = cursor.query_id

        cursor.execute(query)
        query_id_5 = cursor.query_id

        for _ in range(5):
            cursor.execute("SELECT %(now)s as date", {"now": datetime.now(timezone.utc)})

        cursor.execute(query, cache_size=1, cache_expiration_time=3600)  # 1 hours
        query_id_6 = cursor.query_id

        assert query_id_4 != query_id_5
        assert query_id_6 not in [query_id_4, query_id_5]

        # Cache hit
        query = f"SELECT * FROM one_row -- {datetime.now(timezone.utc)!s}"

        cursor.execute(query)
        query_id_7 = cursor.query_id

        cursor.execute(query)
        query_id_8 = cursor.query_id

        time.sleep(2)
        for _ in range(5):
            cursor.execute("SELECT %(now)s as date", {"now": datetime.now(timezone.utc)})

        cursor.execute(query, cache_size=1000, cache_expiration_time=3600)  # 1 hours
        query_id_9 = cursor.query_id

        assert query_id_7 != query_id_8
        assert query_id_9 in [query_id_7, query_id_8]

    @pytest.mark.parametrize(
        "cursor",
        [{"work_group": ENV.work_group, "result_reuse_enable": True, "result_reuse_minutes": 5}],
        indirect=["cursor"],
    )
    def test_cursor_query_result_reuse(self, cursor):
        now = datetime.now(timezone.utc)
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
        now = datetime.now(timezone.utc)
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
        pytest.raises(
            TypeError,
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
                datetime(2017, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
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
        # 1. Basic complex columns from one_row_complex
        cursor.execute(
            """
            SELECT col_array, col_map, col_struct
            FROM one_row_complex
            """,
            result_set_type_hints={
                "col_array": "array(integer)",
                "col_map": "map(integer, integer)",
                "col_struct": "row(a integer, b integer)",
            },
        )
        row = cursor.fetchone()
        assert row[0] == [1, 2]
        assert row[1] == {"1": 2, "3": 4}
        assert row[2] == {"a": 1, "b": 2}
        # Without type hints col_struct values are strings; with hints they are ints
        assert isinstance(row[2]["a"], int)
        assert isinstance(row[2]["b"], int)
        # Map values should also be ints
        assert isinstance(row[1]["1"], int)

        # 2. Nested struct
        cursor.execute(
            """
            SELECT CAST(
              ROW(ROW('2024-01-01', 123), 4.736, 0.583)
              AS ROW(header ROW(stamp VARCHAR, seq INTEGER), x DOUBLE, y DOUBLE)
            ) AS positions
            """,
            result_set_type_hints={
                "positions": "row(header row(stamp varchar, seq integer), x double, y double)",
            },
        )
        row = cursor.fetchone()
        positions = row[0]
        assert positions["header"]["stamp"] == "2024-01-01"
        assert positions["header"]["seq"] == 123
        assert isinstance(positions["header"]["seq"], int)
        assert positions["x"] == 4.736
        assert isinstance(positions["x"], float)
        assert positions["y"] == 0.583
        assert isinstance(positions["y"], float)

        # 3. Array of nested structs
        cursor.execute(
            """
            SELECT CAST(
              ARRAY[
                ROW(ROW(1, 2), ROW(CAST(0.5 AS DOUBLE))),
                ROW(ROW(3, 4), ROW(CAST(1.5 AS DOUBLE)))
              ]
              AS ARRAY(ROW(pos ROW(x INTEGER, y INTEGER), vel ROW(x DOUBLE)))
            ) AS data
            """,
            result_set_type_hints={
                "data": "array(row(pos row(x integer, y integer), vel row(x double)))",
            },
        )
        row = cursor.fetchone()
        data = row[0]
        assert len(data) == 2
        assert data[0]["pos"]["x"] == 1
        assert isinstance(data[0]["pos"]["x"], int)
        assert data[0]["pos"]["y"] == 2
        assert isinstance(data[0]["pos"]["y"], int)
        assert data[0]["vel"]["x"] == 0.5
        assert isinstance(data[0]["vel"]["x"], float)
        assert data[1]["pos"]["x"] == 3
        assert data[1]["vel"]["x"] == 1.5

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
        location = f"{ENV.s3_staging_dir}{ENV.schema}/partition_table/"
        for i in range(10):
            cursor.execute(
                """
                ALTER TABLE partition_table ADD PARTITION (b=%(b)d)
                LOCATION %(location)s
                """,
                {"b": i, "location": location},
            )
        cursor.execute("SHOW PARTITIONS partition_table")
        assert sorted(cursor.fetchall()) == [(f"b={i}",) for i in range(10)]

    @pytest.mark.parametrize("cursor", [{"work_group": ENV.work_group}], indirect=["cursor"])
    def test_workgroup(self, cursor):
        cursor.execute("SELECT * FROM one_row")
        assert cursor.work_group == ENV.work_group

    @pytest.mark.parametrize("cursor", [{"work_group": ENV.work_group}], indirect=["cursor"])
    def test_no_s3_staging_dir(self, cursor):
        cursor._s3_staging_dir = None
        cursor.execute("SELECT * FROM one_row")
        assert cursor.output_location

    def test_executemany(self, cursor):
        rows = [(1, "foo"), (2, "bar"), (3, "jim o'rourke")]
        cursor.executemany(
            "INSERT INTO execute_many (a, b) VALUES (%(a)d, %(b)s)",
            [{"a": a, "b": b} for a, b in rows],
        )
        # rowcount is not supported for executemany
        assert cursor.rowcount == -1
        cursor.execute("SELECT * FROM execute_many")
        assert sorted(cursor.fetchall()) == list(rows)

    def test_executemany_fetch(self, cursor):
        cursor.executemany("SELECT %(x)d FROM one_row", [{"x": i} for i in range(1, 2)])
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

    def test_callback_thread_safety(self, cursor):
        """Test that callback can be used for query ID access from another thread."""
        query_started = threading.Event()
        stored_query_id = []

        def on_start(query_id):
            stored_query_id.append(query_id)
            query_started.set()

        def verify_query_id():
            # Wait for query to start
            query_started.wait(timeout=10)

            # Verify query_id is accessible
            if stored_query_id:
                assert stored_query_id[0] == cursor.query_id

        # Use ThreadPoolExecutor for proper thread management
        with ThreadPoolExecutor(max_workers=1) as executor:
            # Start verification task
            verify_future = executor.submit(verify_query_id)

            # Execute query with callback
            cursor.execute("SELECT 5", on_start_query_execution=on_start)

            # Wait for verification to complete
            verify_future.result(timeout=5)

        # Verify events occurred
        assert query_started.is_set()
        assert len(stored_query_id) == 1

        # Verify query completed successfully
        row = cursor.fetchone()
        assert row == (5,)

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
        cursor.execute("SELECT 1 AS col")
        assert cursor.fetchall() == [(1,)]


class TestDictCursor:
    def test_fetchone(self, dict_cursor):
        dict_cursor.execute("SELECT * FROM one_row")
        assert dict_cursor.fetchone() == {"number_of_rows": 1}

    def test_fetchmany(self, dict_cursor):
        dict_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        actual1 = dict_cursor.fetchmany(10)
        assert len(actual1) == 10
        assert actual1 == [{"a": i} for i in range(10)]
        actual2 = dict_cursor.fetchmany(10)
        assert len(actual2) == 5
        assert actual2 == [{"a": i} for i in range(10, 15)]

    def test_fetchall(self, dict_cursor):
        dict_cursor.execute("SELECT * FROM one_row")
        assert dict_cursor.fetchall() == [{"number_of_rows": 1}]
        dict_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        assert dict_cursor.fetchall() == [{"a": i} for i in range(10000)]

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


class TestComplexDataTypes:
    """Test complex data types (STRUCT, ARRAY, MAP) with actual Athena queries."""

    @pytest.mark.parametrize(
        ("query", "description"),
        [
            ("SELECT ROW('John', 30) AS simple_struct", "simple_struct"),
            (
                "SELECT CAST(ROW('Alice', 25) AS ROW(name VARCHAR, age INTEGER)) AS named_struct",
                "named_struct",
            ),
            ("SELECT ROW('Hello, world', 'x=y+1') AS special_chars_struct", "special_chars_struct"),
            ("SELECT ROW('He said \"hello\"', 'It''s working') AS quotes_struct", "quotes_struct"),
            ("SELECT ROW('Alice', NULL, 'active') AS null_struct", "null_struct"),
            (
                "SELECT ROW(ROW('John', 30), ROW('Engineer', 'Tech')) AS nested_struct",
                "nested_struct",
            ),
            ("SELECT CAST(ROW('Alice', 25, 'Hello, world') AS JSON) AS json_struct", "json_struct"),
        ],
    )
    def test_struct_types(self, cursor, query, description):
        """Test various STRUCT type scenarios to understand Athena's behavior."""
        _logger.info("=== STRUCT Type Test: %s ===", description)
        cursor.execute(query)
        result = cursor.fetchone()
        struct_value = result[0]
        _logger.info("%s: %r (type: %s)", description, struct_value, type(struct_value).__name__)

        # Validate struct value and converter behavior
        assert struct_value is not None, f"STRUCT value should not be None for {description}"

        # Test struct conversion behavior
        if isinstance(struct_value, str):
            converted = _to_struct(struct_value)
            _logger.info("%s: Converted %r -> %r", description, struct_value, converted)
            # For string structs, conversion should succeed or return None for complex cases
            if converted is not None:
                assert isinstance(converted, dict), (
                    f"Converted struct should be dict for {description}"
                )
        elif isinstance(struct_value, dict):
            # Already converted by the cursor converter
            _logger.info("%s: Already converted to dict: %r", description, struct_value)
        else:
            # Log unexpected types for debugging but don't fail
            _logger.warning(
                "%s: Unexpected type %s: %r",
                description,
                type(struct_value).__name__,
                struct_value,
            )

    @pytest.mark.parametrize(
        ("query", "description"),
        [
            ("SELECT ARRAY[1, 2, 3, 4, 5] AS simple_array", "simple_array"),
            ("SELECT ARRAY['apple', 'banana', 'cherry'] AS string_array", "string_array"),
            (
                "SELECT ARRAY['Hello, world', 'x=y+1', 'It''s working'] AS special_array",
                "special_array",
            ),
            ("SELECT ARRAY[ROW('Alice', 25), ROW('Bob', 30)] AS struct_array", "struct_array"),
            ("SELECT ARRAY[ARRAY[1, 2], ARRAY[3, 4]] AS nested_array", "nested_array"),
            (
                "SELECT CAST(MAP(ARRAY['data'], ARRAY[ARRAY['Alice', 'Bob']]) AS JSON) "
                "AS array_json",
                "array_json",
            ),
        ],
    )
    def test_array_types(self, cursor, query, description):
        """Test various ARRAY type scenarios."""
        _logger.info("=== ARRAY Type Test: %s ===", description)
        cursor.execute(query)
        result = cursor.fetchone()
        array_value = result[0]
        _logger.info("%s: %r (type: %s)", description, array_value, type(array_value).__name__)

        # Validate array value
        assert array_value is not None, f"ARRAY value should not be None for {description}"
        _logger.info("%s: Array value type %s", description, type(array_value).__name__)

    @pytest.mark.parametrize(
        ("query", "description"),
        [
            (
                "SELECT MAP(ARRAY[1, 2, 3], ARRAY['one', 'two', 'three']) AS simple_map",
                "simple_map",
            ),
            (
                "SELECT MAP(ARRAY['name', 'age', 'city'], ARRAY['John', '30', 'Tokyo']) "
                "AS string_map",
                "string_map",
            ),
            (
                "SELECT MAP(ARRAY['msg', 'formula'], ARRAY['Hello, world', 'x=y+1']) "
                "AS special_map",
                "special_map",
            ),
            (
                "SELECT CAST(MAP(ARRAY['person1', 'person2'], "
                "ARRAY[ROW('Alice', 25), ROW('Bob', 30)]) AS JSON) AS struct_value_map",
                "struct_value_map",
            ),
            (
                "SELECT CAST(MAP(ARRAY['name', 'age'], ARRAY['Alice', '25']) AS JSON) AS json_map",
                "json_map",
            ),
        ],
    )
    def test_map_types(self, cursor, query, description):
        """Test various MAP type scenarios."""
        _logger.info("=== MAP Type Test: %s ===", description)
        cursor.execute(query)
        result = cursor.fetchone()
        map_value = result[0]
        _logger.info("%s: %r (type: %s)", description, map_value, type(map_value).__name__)

        # Validate map value and converter behavior
        assert map_value is not None, f"MAP value should not be None for {description}"

        # Test map conversion behavior
        if isinstance(map_value, str):
            # For complex MAP structures, string is expected (JSON or native format)
            if "ROW(" in map_value or "ARRAY[" in map_value:
                # Complex structure, expect string format
                _logger.info("%s: Complex MAP kept as string: %r", description, map_value)
            else:
                # Simple MAP, try conversion
                converted = _to_map(map_value)
                _logger.info("%s: Converted %r -> %r", description, map_value, converted)
                if converted is not None:
                    assert isinstance(converted, dict), (
                        f"Converted map should be dict for {description}"
                    )
        elif isinstance(map_value, dict):
            # Already converted by the cursor converter
            _logger.info("%s: Already converted to dict: %r", description, map_value)
        else:
            # Log unexpected types for debugging but don't fail
            _logger.warning(
                "%s: Unexpected type %s: %r",
                description,
                type(map_value).__name__,
                map_value,
            )

    @pytest.mark.parametrize(
        ("query", "description"),
        [
            (
                "SELECT CAST(ROW(ARRAY[1, 2, 3], MAP(ARRAY['a', 'b'], ARRAY[1, 2])) AS JSON) "
                "AS struct_with_collections",
                "struct_with_collections",
            ),
            (
                "SELECT CAST(ARRAY[MAP(ARRAY['name'], ARRAY['Alice']), "
                "MAP(ARRAY['name'], ARRAY['Bob'])] AS JSON) AS array_of_maps",
                "array_of_maps",
            ),
            (
                "SELECT CAST(MAP(ARRAY['numbers', 'letters'], "
                "ARRAY[ARRAY['1', '2', '3'], ARRAY['a', 'b', 'c']]) AS JSON) AS map_with_arrays",
                "map_with_arrays",
            ),
        ],
    )
    def test_complex_combinations(self, cursor, query, description):
        """Test complex combinations of data types."""
        _logger.info("=== Complex Combination Test: %s ===", description)
        cursor.execute(query)
        result = cursor.fetchone()
        complex_value = result[0]
        _logger.info("%s: %r (type: %s)", description, complex_value, type(complex_value).__name__)

        # For JSON cast results, expect string values that can be parsed as JSON
        if isinstance(complex_value, str):
            try:
                # Test that the JSON string can be parsed
                parsed = json.loads(complex_value)
                _logger.info("  Parsed JSON: %r", parsed)
                assert parsed is not None, f"Parsed JSON should not be None for {description}"
            except json.JSONDecodeError as e:
                raise AssertionError(f"JSON parsing failed for {description}: {e}") from e
        else:
            # If it's not a string, it should still be a valid value (not None)
            assert complex_value is not None, f"Complex value should not be None for {description}"
        _logger.info("%s: Complex value type %s", description, type(complex_value).__name__)

    @pytest.mark.parametrize(
        ("query", "description"),
        [
            ("SELECT ARRAY[1, 2, 3, 4, 5] AS simple_array", "simple_array"),
            ("SELECT ARRAY['apple', 'banana', 'cherry'] AS string_array", "string_array"),
            ("SELECT ARRAY[true, false, null] AS boolean_array", "boolean_array"),
            ("SELECT ARRAY[1.5, 2.7, 3.14] AS float_array", "float_array"),
            ("SELECT ARRAY[] AS empty_array", "empty_array"),
            ("SELECT ARRAY[1, null, 3, null, 5] AS null_elements_array", "null_elements_array"),
            (
                "SELECT ARRAY[CAST(1 AS VARCHAR), 'mixed', 'true', 'null', '2.5'] AS mixed_array",
                "mixed_array",
            ),
            (
                "SELECT ARRAY[CAST(1.23 AS DECIMAL(10,2)), CAST(4.56 AS DECIMAL(10,2))] "
                "AS decimal_array",
                "decimal_array",
            ),
            (
                "SELECT ARRAY[DATE '2023-01-01', DATE '2023-12-31'] AS date_array",
                "date_array",
            ),
            (
                "SELECT ARRAY[TIMESTAMP '2023-01-01 12:00:00'] AS timestamp_array",
                "timestamp_array",
            ),
        ],
    )
    def test_array_types_basic(self, cursor, query, description):
        """Test basic ARRAY type scenarios."""
        _logger.info("=== ARRAY Type Test: %s ===", description)
        cursor.execute(query)
        result = cursor.fetchone()
        array_value = result[0]
        _logger.info("%s: %r (type: %s)", description, array_value, type(array_value).__name__)

        # Validate array value
        assert array_value is not None or description == "empty_array", (
            f"ARRAY value should not be None for {description}"
        )
        if description == "empty_array":
            assert array_value == [], f"Empty array should be [] for {description}"
        else:
            assert isinstance(array_value, list), f"ARRAY value should be list for {description}"
        _logger.info("%s: Array value type %s", description, type(array_value).__name__)

    @pytest.mark.parametrize(
        ("query", "description"),
        [
            (
                "SELECT ARRAY[ROW(1, 'Alice'), ROW(2, 'Bob'), ROW(3, 'Charlie')] AS struct_array",
                "struct_array",
            ),
            (
                "SELECT ARRAY[ROW('name', 'John', 25), ROW('name', 'Jane', 30)] "
                "AS unnamed_struct_array",
                "unnamed_struct_array",
            ),
            (
                "SELECT ARRAY[CAST(ROW('John', 25) AS ROW(name VARCHAR, age INT)), "
                "CAST(ROW('Jane', 30) AS ROW(name VARCHAR, age INT))] AS named_struct_array",
                "named_struct_array",
            ),
        ],
    )
    def test_array_types_with_structs(self, cursor, query, description):
        """Test ARRAY types containing STRUCT elements."""
        _logger.info("=== ARRAY with STRUCT Test: %s ===", description)
        cursor.execute(query)
        result = cursor.fetchone()
        array_value = result[0]
        _logger.info("%s: %r (type: %s)", description, array_value, type(array_value).__name__)

        # Validate array value
        assert array_value is not None, f"ARRAY value should not be None for {description}"
        assert isinstance(array_value, list), f"ARRAY value should be list for {description}"
        assert len(array_value) > 0, f"ARRAY should not be empty for {description}"

        # Check first element is a dict (converted struct)
        first_element = array_value[0]
        assert isinstance(first_element, dict), (
            f"First array element should be dict (struct) for {description}"
        )
        _logger.info("%s: First element: %r", description, first_element)

    @pytest.mark.parametrize(
        ("query", "description"),
        [
            (
                "SELECT CAST(ARRAY[1, 2, 3] AS JSON) AS arr_json",
                "arr_json",
            ),
            (
                "SELECT CAST(ARRAY['a', 'b', 'c'] AS JSON) AS str_arr_json",
                "str_arr_json",
            ),
            (
                "SELECT CAST(ARRAY[ROW(1, 'Alice'), ROW(2, 'Bob')] AS JSON) AS struct_arr_json",
                "struct_arr_json",
            ),
        ],
    )
    def test_array_types_json_cast(self, cursor, query, description):
        """Test ARRAY types with JSON casting."""
        _logger.info("=== ARRAY JSON Cast Test: %s ===", description)
        cursor.execute(query)
        result = cursor.fetchone()
        array_value = result[0]
        _logger.info("%s: %r (type: %s)", description, array_value, type(array_value).__name__)

        # Validate array value
        assert array_value is not None, f"ARRAY value should not be None for {description}"
        assert isinstance(array_value, list), (
            f"JSON cast ARRAY value should be list for {description}"
        )
        _logger.info("%s: JSON cast array type %s", description, type(array_value).__name__)

    @pytest.mark.parametrize(
        ("query", "description"),
        [
            ("SELECT CARDINALITY(ARRAY[1, 2, 3, 4, 5]) AS array_size", "array_size"),
            ("SELECT ARRAY[10, 20, 30, 40][2] AS array_element", "array_element"),
            ("SELECT ARRAY[1, 2] || ARRAY[3, 4] AS array_concat", "array_concat"),
            ("SELECT CONTAINS(ARRAY[1, 2, 3], 2) AS array_contains", "array_contains"),
        ],
    )
    def test_array_operations(self, cursor, query, description):
        """Test ARRAY operations and functions."""
        _logger.info("=== ARRAY Operation Test: %s ===", description)
        cursor.execute(query)
        result = cursor.fetchone()
        operation_result = result[0]
        _logger.info(
            "%s: %r (type: %s)",
            description,
            operation_result,
            type(operation_result).__name__,
        )

        # Validate operation result
        assert operation_result is not None, (
            f"ARRAY operation result should not be None for {description}"
        )

        # Type-specific validations
        if description == "array_size":
            assert operation_result == 5, f"Array size should be 5 for {description}"
        elif description == "array_element":
            assert operation_result == 20, f"Array element [2] should be 20 for {description}"
        elif description == "array_concat":
            assert operation_result == [1, 2, 3, 4], (
                f"Array concat should be [1,2,3,4] for {description}"
            )
        elif description == "array_contains":
            assert operation_result is True, f"Array contains should be True for {description}"

    def test_array_converter_behavior(self, cursor):
        """Test ARRAY converter behavior with different formats."""
        _logger.info("=== ARRAY Converter Behavior Test ===")

        # Test simple array conversion
        cursor.execute("SELECT ARRAY[1, 2, 3] AS simple")
        result = cursor.fetchone()
        simple_array = result[0]
        _logger.info("Simple array: %r", simple_array)
        assert simple_array == [1, 2, 3]

        # Test array with struct conversion
        cursor.execute(
            "SELECT ARRAY[CAST(ROW(1, 2) AS ROW(a INT, b INT)), "
            "CAST(ROW(3, 4) AS ROW(a INT, b INT))] AS struct_array"
        )
        result = cursor.fetchone()
        struct_array = result[0]
        _logger.info("Struct array: %r", struct_array)
        assert isinstance(struct_array, list)
        assert len(struct_array) == 2
        assert isinstance(struct_array[0], dict)

        # Test converter function directly
        test_cases = [
            ("[1, 2, 3]", [1, 2, 3]),
            ('["a", "b", "c"]', ["a", "b", "c"]),
            ("[{a=1, b=2}]", [{"a": "1", "b": "2"}]),
            ("[]", []),
            (None, None),
            ("invalid", None),
        ]

        for test_input, expected in test_cases:
            result = _to_array(test_input)
            _logger.info("Converter test: %r -> %r", test_input, result)
            assert result == expected, (
                f"Converter failed for {test_input}: expected {expected}, got {result}"
            )
