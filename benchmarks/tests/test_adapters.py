# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import asyncio
import weakref
from concurrent.futures import Future
from io import BytesIO
from types import SimpleNamespace

import boto3
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from awswrangler.athena import _read
from botocore.client import BaseClient
from botocore.stub import Stubber
from pyathena_bench.adapters import (
    RESULT_SETS,
    Adapter,
    consume,
    consume_aio,
    measure_query,
    run_adapter,
)
from pyathena_bench.cases import Case, matrix
from pyathena_bench.config import Settings


class Rows:
    def __init__(self):
        self.remaining = 5
        self.closed = False

    def fetchmany(self, size):
        batch = [(i,) for i in range(min(size, self.remaining))]
        self.remaining -= len(batch)
        return batch

    def close(self):
        self.closed = True


class Cursor:
    def __init__(self, api):
        self.api = api
        self.result_set = Rows()
        self.query_id = "test-query"

    def execute(self, sql, **kwargs):
        assert kwargs["cache_size"] == 0
        assert kwargs["result_reuse_enable"] is False
        if self.api == "thread":
            future = Future()
            future.set_result(self.result_set)
            return self.query_id, future
        return self


class AioCursor(Cursor):
    async def execute(self, sql, **kwargs):
        return super().execute(sql, **kwargs)

    async def fetchmany(self, size):
        await asyncio.sleep(0)
        return self.result_set.fetchmany(size)


class ChunkSource:
    def iter_chunks(self):
        previous = None
        for _ in range(3):
            if previous:
                assert previous() is None, "The consumer retained its previous chunk"
            frame = pd.DataFrame({"a": [1, 2]})
            previous = weakref.ref(frame)
            yield frame
            del frame


def test_native_chunks_are_drained_without_retention():
    assert consume(ChunkSource(), Case("pandas", output="native", chunksize=2)) == 6


def test_rows_and_arrow_consume_actual_row_counts():
    assert consume(Rows(), Case("cursor", arraysize=2)) == 5
    assert (
        consume(
            SimpleNamespace(as_arrow=lambda: SimpleNamespace(num_rows=7)),
            Case("arrow", output="native"),
        )
        == 7
    )
    assert consume(pd.DataFrame({"a": [1, 2]}), Case("wrangler", output="native")) == 2


class TestAdapter:
    @pytest.mark.parametrize("transport", ["csv", "ctas", "unload"])
    def test_wrangle_options_pass_actual_library_validation(self, monkeypatch, transport):
        calls = []

        def resolve(**kwargs):
            calls.append(kwargs)
            return pd.DataFrame({"a": [1]})

        monkeypatch.setattr(_read, "_resolve_query_without_cache", resolve)
        adapter = Adapter(
            Settings(),
            Case("wrangler", transport=transport, output="native"),
            None,
            "scratch",
            "s3://out/",
            "b_run_trial",
        )
        assert len(adapter.wrangler("SELECT a")) == 1
        assert calls[0]["ctas_approach"] == (transport == "ctas")
        assert calls[0]["unload_approach"] == (transport == "unload")
        assert bool(calls[0]["result_reuse_configuration"]) == (transport == "csv")

    def test_all_cursor_factories_accept_the_selected_options(self, monkeypatch):
        def unexpected_request(*args, **kwargs):
            pytest.fail("Cursor construction made an AWS request")

        monkeypatch.setattr(BaseClient, "_make_api_call", unexpected_request)
        session = boto3.Session(
            aws_access_key_id="testing", aws_secret_access_key="testing", region_name="us-west-2"
        )
        for case in matrix(Settings(), "single", "flat"):
            if case.family == "wrangler" or case.unsupported:
                continue
            adapter = Adapter(Settings(), case, session, "scratch", "s3://out/", "temp")
            with adapter.connection() as connection:
                cursor = adapter.cursor(connection)
                assert cursor.arraysize == case.arraysize
                cursor.close()

    @pytest.mark.parametrize("api", ["sync", "thread", "aio"])
    async def test_real_api_cursors_consume_stubbed_athena_results(self, api):
        session = boto3.Session(
            aws_access_key_id="testing", aws_secret_access_key="testing", region_name="us-west-2"
        )
        adapter = Adapter(Settings(), Case("cursor", api), session, "scratch", "s3://out/", "temp")
        with adapter.connection() as connection:
            cursor = adapter.cursor(connection)
            with Stubber(connection.client) as stubber:
                stubber.add_response("start_query_execution", {"QueryExecutionId": "q"})
                stubber.add_response(
                    "get_query_execution",
                    {
                        "QueryExecution": {
                            "QueryExecutionId": "q",
                            "Query": "SELECT a",
                            "Status": {"State": "SUCCEEDED"},
                            "ResultConfiguration": {"OutputLocation": "s3://out/q.csv"},
                        }
                    },
                )
                stubber.add_response(
                    "get_query_results",
                    {
                        "ResultSet": {
                            "ResultSetMetadata": {"ColumnInfo": [{"Name": "a", "Type": "bigint"}]},
                            "Rows": [
                                {"Data": [{"VarCharValue": value}]}
                                for value in ("a", "1", "2", "3")
                            ],
                        }
                    },
                )
                result = await measure_query(adapter, cursor, "SELECT a", 3)
                assert result["status"] == "ok", result
                assert result["rows"] == 3
                stubber.assert_no_pending_responses()
            cursor.close()

    @pytest.mark.parametrize("api", ["sync", "thread", "aio"])
    async def test_each_api_consumes_all_rows_and_reports_boundaries(self, api):
        adapter = Adapter(
            Settings(), Case("cursor", api, arraysize=2), None, "scratch", "s3://out/", "temp"
        )
        cursor = AioCursor(api) if api == "aio" else Cursor(api)
        result = await measure_query(adapter, cursor, "SELECT a", 5)
        assert result["status"] == "ok"
        assert result["rows"] == 5
        assert result["total_seconds"] == pytest.approx(
            result["execute_seconds"] + result["consume_seconds"]
        )

    async def test_wrong_row_count_is_not_success(self):
        adapter = Adapter(Settings(), Case("cursor"), None, "scratch", "s3://out/", "temp")
        result = await measure_query(adapter, Cursor("sync"), "SELECT a", 6)
        assert result["status"] == "row_count_mismatch"

    async def test_aio_native_consumption_is_offloaded(self, monkeypatch):
        calls = []

        async def offload(function, *args):
            calls.append(function)
            return function(*args)

        monkeypatch.setattr(asyncio, "to_thread", offload)
        result = await consume_aio(
            SimpleNamespace(result_set=ChunkSource()), Case("polars", output="native")
        )
        assert result == 6
        assert calls == [consume]

    async def test_concurrent_aio_uses_distinct_cursors(self, monkeypatch):
        active = 0
        peak = 0
        created = []

        class FakeCursor:
            def close(self):
                pass

        def make_cursor(self, connection):
            cursor = FakeCursor()
            created.append(cursor)
            return cursor

        async def measure(adapter, cursor, sql, expected):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.02)
            active -= 1
            return {"status": "ok", "rows": expected}

        monkeypatch.setattr(Adapter, "connection", lambda self: SimpleNamespace(close=lambda: None))
        monkeypatch.setattr(Adapter, "cursor", make_cursor)
        monkeypatch.setattr("pyathena_bench.adapters.measure_query", measure)
        adapter = Adapter(
            Settings(),
            Case("cursor", "aio", concurrency=3, suite="concurrent"),
            None,
            "scratch",
            "s3://out/",
            "temp",
        )
        result = await run_adapter(adapter, "SELECT a", 5)
        assert len(set(created)) == peak == 3
        assert result["status"] == "ok"
        assert len(result["queries"]) == 3
        assert result["loop_lag_seconds"]

    @pytest.mark.parametrize("case", matrix(Settings(), "init", "flat"), ids=lambda c: c.id)
    async def test_init_uses_real_resultset_constructors_and_validates_rows(
        self, monkeypatch, case
    ):
        def no_network(*args, **kwargs):
            pytest.fail("Constructor test made an AWS request")

        def prefetch(result):
            result._metadata = (
                {
                    "Name": "id",
                    "Type": "bigint",
                    "Precision": 19,
                    "Scale": 0,
                    "Nullable": "UNKNOWN",
                },
            )
            result._rows.append({"id": 1} if case.family == "dict" else (1,))

        monkeypatch.setattr(BaseClient, "_make_api_call", no_network)
        monkeypatch.setattr(RESULT_SETS["cursor"], "_pre_fetch", prefetch)
        csv_bytes = b'"id"\n"1"\n'
        monkeypatch.setattr(RESULT_SETS["s3fs"], "_get_content_length", lambda self: len(csv_bytes))
        monkeypatch.setattr(
            RESULT_SETS["s3fs"],
            "_create_s3_file_system",
            lambda self: SimpleNamespace(_open=lambda *args, **kwargs: BytesIO(csv_bytes)),
        )
        monkeypatch.setattr(
            RESULT_SETS["pandas"], "_as_pandas", lambda self: pd.DataFrame({"id": [1]})
        )
        monkeypatch.setattr(
            RESULT_SETS["polars"], "_as_polars", lambda self: pl.DataFrame({"id": [1]})
        )
        monkeypatch.setattr(RESULT_SETS["arrow"], "_as_arrow", lambda self: pa.table({"id": [1]}))
        session = boto3.Session(
            aws_access_key_id="testing", aws_secret_access_key="testing", region_name="us-west-2"
        )
        adapter = Adapter(
            Settings(executor_workers=3), case, session, "scratch", "s3://out/", "temp"
        )
        fixture = {
            "response": {
                "QueryExecution": {
                    "QueryExecutionId": "q",
                    "Query": "SELECT id",
                    "Status": {"State": "SUCCEEDED"},
                    "ResultConfiguration": {"OutputLocation": "s3://out/q.csv"},
                }
            },
            "unload_location": "s3://out/parquet/" if case.transport == "unload" else None,
        }
        result = await run_adapter(adapter, "SELECT id", 1, fixture)
        assert result["status"] == "ok", result
        assert result["queries"][0]["rows"] == 1
        assert 0 <= result["queries"][0]["init_seconds"] <= result["queries"][0]["total_seconds"]

    async def test_thread_pandas_passes_reader_workers_to_real_cursor(self, monkeypatch):
        captured = []
        session = boto3.Session(
            aws_access_key_id="testing", aws_secret_access_key="testing", region_name="us-west-2"
        )
        adapter = Adapter(
            Settings(executor_workers=3),
            Case("pandas", "thread"),
            session,
            "scratch",
            "s3://out/",
            "temp",
        )
        with adapter.connection() as connection:
            cursor = adapter.cursor(connection)
            monkeypatch.setattr(cursor, "_execute", lambda *args, **kwargs: "q")
            monkeypatch.setattr(cursor, "_poll", lambda *args: None)

            def resultset(**kwargs):
                captured.append(kwargs["max_workers"])
                return Rows()

            monkeypatch.setattr("pyathena.pandas.async_cursor.AthenaPandasResultSet", resultset)
            result = await measure_query(adapter, cursor, "SELECT id", 5)
            cursor.close(wait=True)
        assert result["status"] == "ok", result
        assert captured == [3]
