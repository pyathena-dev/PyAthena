# -*- coding: utf-8 -*-
import re
from datetime import datetime

import pytest

from pyathena.error import DatabaseError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet
from tests import ENV
from tests.pyathena.aio.conftest import _aio_connect


class TestAioCursor:
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
        rows = []
        async for row in aio_cursor:
            rows.append(row)
        assert rows == [(1,)]

    async def test_execute_returns_self(self, aio_cursor):
        result = await aio_cursor.execute("SELECT * FROM one_row")
        assert result is aio_cursor

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
        assert aio_cursor.rowcount == -1
        await aio_cursor.execute("SELECT * FROM execute_many_aio")
        assert sorted(await aio_cursor.fetchall()) == list(rows)

    async def test_executemany_fetch(self, aio_cursor):
        await aio_cursor.executemany("SELECT %(x)d FROM one_row", [{"x": i} for i in range(1, 2)])
        with pytest.raises(ProgrammingError):
            await aio_cursor.fetchall()
        with pytest.raises(ProgrammingError):
            await aio_cursor.fetchmany()
        with pytest.raises(ProgrammingError):
            await aio_cursor.fetchone()

    async def test_execute_with_callback(self, aio_cursor):
        callback_results = []

        def on_start(query_id):
            assert query_id is not None
            assert len(query_id) > 0
            callback_results.append(query_id)

        result = await aio_cursor.execute("SELECT 1", on_start_query_execution=on_start)

        assert len(callback_results) == 1
        assert callback_results[0] == aio_cursor.query_id
        assert result is aio_cursor
        assert await aio_cursor.fetchone() == (1,)

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

    async def test_aconnect(self):
        from pyathena import aconnect

        conn = await aconnect(work_group=ENV.default_work_group)
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
