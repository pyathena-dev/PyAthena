# Copyright 2017 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Adapters measure public cursor paths without changing library behavior."""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

import awswrangler as wr
import pandas as pd
import polars as pl
import pyarrow as pa
from botocore.config import Config

from pyathena import connect
from pyathena.aio.arrow.cursor import AioArrowCursor
from pyathena.aio.cursor import AioCursor, AioDictCursor
from pyathena.aio.pandas.cursor import AioPandasCursor
from pyathena.aio.polars.cursor import AioPolarsCursor
from pyathena.aio.s3fs.cursor import AioS3FSCursor
from pyathena.arrow.async_cursor import AsyncArrowCursor
from pyathena.arrow.cursor import ArrowCursor
from pyathena.arrow.result_set import AthenaArrowResultSet
from pyathena.async_cursor import AsyncCursor, AsyncDictCursor
from pyathena.cursor import Cursor, DictCursor
from pyathena.model import AthenaQueryExecution
from pyathena.pandas.async_cursor import AsyncPandasCursor
from pyathena.pandas.cursor import PandasCursor
from pyathena.pandas.result_set import AthenaPandasResultSet
from pyathena.polars.async_cursor import AsyncPolarsCursor
from pyathena.polars.cursor import PolarsCursor
from pyathena.polars.result_set import AthenaPolarsResultSet
from pyathena.result_set import AthenaDictResultSet, AthenaResultSet
from pyathena.s3fs.async_cursor import AsyncS3FSCursor
from pyathena.s3fs.cursor import S3FSCursor
from pyathena.s3fs.result_set import AthenaS3FSResultSet
from pyathena_bench.cases import Case
from pyathena_bench.config import Settings

BACKEND_VERSIONS = {
    "pandas": pd.__version__,
    "polars": pl.__version__,
    "pyarrow": pa.__version__,
    "awswrangler": wr.__version__,
}

CURSORS: dict[str, dict[str, Any]] = {
    "sync": dict(
        zip(
            ("cursor", "dict", "s3fs", "pandas", "arrow", "polars"),
            (Cursor, DictCursor, S3FSCursor, PandasCursor, ArrowCursor, PolarsCursor),
            strict=True,
        )
    ),
    "thread": dict(
        zip(
            ("cursor", "dict", "s3fs", "pandas", "arrow", "polars"),
            (
                AsyncCursor,
                AsyncDictCursor,
                AsyncS3FSCursor,
                AsyncPandasCursor,
                AsyncArrowCursor,
                AsyncPolarsCursor,
            ),
            strict=True,
        )
    ),
    "aio": dict(
        zip(
            ("cursor", "dict", "s3fs", "pandas", "arrow", "polars"),
            (
                AioCursor,
                AioDictCursor,
                AioS3FSCursor,
                AioPandasCursor,
                AioArrowCursor,
                AioPolarsCursor,
            ),
            strict=True,
        )
    ),
}
RESULT_SETS: dict[str, Any] = {
    "cursor": AthenaResultSet,
    "dict": AthenaDictResultSet,
    "s3fs": AthenaS3FSResultSet,
    "pandas": AthenaPandasResultSet,
    "arrow": AthenaArrowResultSet,
    "polars": AthenaPolarsResultSet,
}


def consume(result: Any, case: Case) -> int:
    """Drain a result while retaining at most one application batch."""
    if case.output == "rows":
        count = 0
        while True:
            batch = result.fetchmany(case.arraysize)
            if not batch:
                return count
            count += len(batch)
            del batch
    if case.family == "arrow":
        table = result.as_arrow()
        return int(table.num_rows)
    if case.family in {"pandas", "polars"}:
        count = 0
        iterator = result.iter_chunks()
        while True:
            try:
                chunk = next(iterator)
            except StopIteration:
                return count
            count += len(chunk)
            del chunk
    if isinstance(result, pd.DataFrame):
        return len(result)
    count = 0
    iterator = iter(result)
    while True:
        try:
            chunk = next(iterator)
        except StopIteration:
            return count
        count += len(chunk)
        del chunk


async def consume_aio(cursor: Any, case: Case) -> int:
    if case.output == "native":
        # These native accessors are synchronous even on Aio cursors.
        return await asyncio.to_thread(consume, cursor.result_set, case)
    count = 0
    while True:
        batch = await cursor.fetchmany(case.arraysize)
        if not batch:
            return count
        count += len(batch)
        del batch


@dataclass
class Adapter:
    settings: Settings
    case: Case
    session: Any
    database: str
    output: str
    temp_table: str

    def connection(self) -> Any:
        return connect(
            session=self.session,
            region_name=self.settings.region,
            schema_name=self.database,
            work_group=self.settings.workgroup,
            s3_staging_dir=self.output,
            poll_interval=self.settings.poll_interval,
            result_reuse_enable=False,
            config=Config(
                max_pool_connections=max(
                    10, 2 * self.settings.executor_workers, self.case.concurrency
                ),
                connect_timeout=10,
                read_timeout=60,
            ),
        )

    def cursor(self, connection: Any) -> Any:
        case = self.case
        kwargs: dict[str, Any] = {"arraysize": case.arraysize}
        if case.api == "thread" or case.family in {"pandas", "polars"}:
            kwargs["max_workers"] = self.settings.executor_workers
        if case.family in {"pandas", "polars", "arrow"}:
            kwargs["unload"] = case.transport == "unload"
        if case.family in {"pandas", "polars"}:
            kwargs["chunksize"] = case.chunksize
        if case.family == "pandas" and case.transport == "unload":
            kwargs["engine"] = "pyarrow"
        cursor = connection.cursor(CURSORS[case.api][case.family], **kwargs)
        # Some cursor bases reset arraysize during construction.
        cursor.arraysize = case.arraysize
        return cursor

    def wrangler(self, sql: str) -> Any:
        options: dict[str, Any] = {
            "boto3_session": self.session,
            "workgroup": self.settings.workgroup,
            "s3_output": self.output,
            "ctas_approach": self.case.transport == "ctas",
            "unload_approach": self.case.transport == "unload",
            "chunksize": self.case.chunksize,
            "ctas_parameters": {
                "database": self.database,
                "temp_table_name": self.temp_table,
                "compression": "snappy",
            },
            "unload_parameters": {"compression": "snappy"},
            "athena_cache_settings": {"max_cache_seconds": 0},
            "athena_query_wait_polling_delay": self.settings.poll_interval,
            "keep_files": True,
            "use_threads": self.settings.executor_workers,
        }
        # Wrangler rejects this option for CTAS/UNLOAD, which cannot reuse results.
        if self.case.transport == "csv":
            options["result_reuse_configuration"] = {
                "ResultReuseByAgeConfiguration": {"Enabled": False}
            }
        return wr.athena.read_sql_query(sql, database=self.database, **options)

    def construct(self, connection: Any, fixture: dict[str, Any]) -> Any:
        case = self.case
        kwargs: dict[str, Any] = {
            "connection": connection,
            "converter": CURSORS["sync"][case.family].get_default_converter(
                case.transport == "unload"
            ),
            "query_execution": AthenaQueryExecution(fixture["response"]),
            "arraysize": case.arraysize,
            "retry_config": connection.retry_config,
        }
        if case.family in {"pandas", "arrow", "polars"}:
            kwargs.update(
                unload=case.transport == "unload", unload_location=fixture["unload_location"]
            )
        if case.family == "pandas":
            kwargs["engine"] = "pyarrow" if case.transport == "unload" else "auto"
        if case.family in {"pandas", "polars"}:
            kwargs["max_workers"] = self.settings.executor_workers
        return RESULT_SETS[case.family](**kwargs)


async def measure_query(adapter: Adapter, cursor: Any, sql: str, expected: int) -> dict[str, Any]:
    start = time.perf_counter()
    case = adapter.case
    result = None
    try:
        if case.family == "wrangler":
            result = adapter.wrangler(sql)
            ready = time.perf_counter()
            count = consume(result, case)
            query_id = None  # The botocore observer captures IDs even for iterators.
        elif case.api == "thread":
            # AsyncPandasCursor forwards result-reader options through execute().
            options = (
                {"max_workers": adapter.settings.executor_workers}
                if case.family == "pandas"
                else {}
            )
            query_id, future = await asyncio.to_thread(
                cursor.execute, sql, cache_size=0, result_reuse_enable=False, **options
            )
            result = await asyncio.wrap_future(future)
            ready = time.perf_counter()
            count = await asyncio.to_thread(consume, result, case)
        elif case.api == "aio":
            await cursor.execute(sql, cache_size=0, result_reuse_enable=False)
            ready = time.perf_counter()
            query_id = cursor.query_id
            count = await consume_aio(cursor, case)
        else:
            cursor.execute(sql, cache_size=0, result_reuse_enable=False)
            ready = time.perf_counter()
            query_id = cursor.query_id
            count = consume(cursor.result_set, case)
        end = time.perf_counter()
        return {
            "status": "ok" if count == expected else "row_count_mismatch",
            "rows": count,
            "expected_rows": expected,
            "query_id": query_id,
            "execute_seconds": ready - start,
            "consume_seconds": end - ready,
            "total_seconds": end - start,
            "ready_at": ready,
        }
    except Exception as exc:
        return {
            "status": "error",
            "error": f"{type(exc).__name__}: {exc}",
            "total_seconds": time.perf_counter() - start,
        }
    finally:
        if result is not None and hasattr(result, "close"):
            result.close()


async def heartbeat(interval: float, stop: asyncio.Event, samples: list[float]) -> None:
    while not stop.is_set():
        start = time.perf_counter()
        await asyncio.sleep(interval)
        samples.append(max(0.0, time.perf_counter() - start - interval))


async def run_adapter(
    adapter: Adapter, sql: str, expected: int, fixture: dict[str, Any] | None = None
) -> dict[str, Any]:
    loop = asyncio.get_running_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=adapter.settings.executor_workers))
    setup_start = time.perf_counter()
    case = adapter.case
    connection = None if case.family == "wrangler" else adapter.connection()
    cursors: list[Any] = []
    if case.suite != "init" and connection is not None:
        # AsyncCursor owns one shared pool; Aio cursors carry per-query state.
        cursors = [
            adapter.cursor(connection) for _ in range(case.concurrency if case.api == "aio" else 1)
        ]
    setup_seconds = time.perf_counter() - setup_start
    samples: list[float] = []
    stop = asyncio.Event()
    monitor = asyncio.create_task(heartbeat(adapter.settings.heartbeat_interval, stop, samples))
    await asyncio.sleep(0)
    start = time.perf_counter()
    try:
        if case.suite == "init":
            if fixture is None:
                raise ValueError("Missing completed query fixture")
            result = (
                await asyncio.to_thread(adapter.construct, connection, fixture)
                if case.api == "to_thread"
                else adapter.construct(connection, fixture)
            )
            ready = time.perf_counter()
            # Validation is separate from the constructor measurement.
            count = await asyncio.to_thread(consume, result, case)
            result.close()
            queries = [
                {
                    "status": "ok" if count == expected else "row_count_mismatch",
                    "rows": count,
                    "expected_rows": expected,
                    "init_seconds": ready - start,
                    "total_seconds": time.perf_counter() - start,
                    "query_id": fixture["response"]["QueryExecution"]["QueryExecutionId"],
                }
            ]
        else:
            queries = await asyncio.gather(
                *(
                    measure_query(
                        adapter,
                        cursors[i] if case.api == "aio" else (cursors[0] if cursors else None),
                        sql,
                        expected,
                    )
                    for i in range(case.concurrency)
                )
            )
        elapsed = time.perf_counter() - start
        return {
            "status": "ok" if all(q["status"] == "ok" for q in queries) else "error",
            "setup_seconds": setup_seconds,
            "batch_seconds": elapsed,
            "queries": queries,
            "successful_queries_per_second": sum(q["status"] == "ok" for q in queries) / elapsed,
            "loop_lag_seconds": samples
            if case.api in {"thread", "aio", "direct", "to_thread"}
            else None,
        }
    finally:
        stop.set()
        await monitor
        for cursor in cursors:
            if case.api == "thread":
                cursor.close(wait=True)
            else:
                cursor.close()
        if connection is not None:
            connection.close()
