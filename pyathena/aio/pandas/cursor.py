# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import logging
from multiprocessing import cpu_count
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

from pyathena.aio.common import WithAsyncFetch
from pyathena.common import CursorIterator
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.pandas.converter import (
    DefaultPandasTypeConverter,
    DefaultPandasUnloadTypeConverter,
)
from pyathena.pandas.result_set import AthenaPandasResultSet, PandasDataFrameIterator

if TYPE_CHECKING:
    from pandas import DataFrame

_logger = logging.getLogger(__name__)  # type: ignore


class AioPandasCursor(WithAsyncFetch):
    """Native asyncio cursor that returns results as pandas DataFrames.

    Uses ``asyncio.to_thread()`` for both result set creation and fetch
    operations, keeping the event loop free. This is especially important
    when ``chunksize`` is set, as fetch calls trigger lazy S3 reads.

    Example:
        >>> async with await pyathena.aconnect(...) as conn:
        ...     cursor = conn.cursor(AioPandasCursor)
        ...     await cursor.execute("SELECT * FROM my_table")
        ...     df = cursor.as_pandas()
    """

    def __init__(
        self,
        s3_staging_dir: Optional[str] = None,
        schema_name: Optional[str] = None,
        catalog_name: Optional[str] = None,
        work_group: Optional[str] = None,
        poll_interval: float = 1,
        encryption_option: Optional[str] = None,
        kms_key: Optional[str] = None,
        kill_on_interrupt: bool = True,
        unload: bool = False,
        engine: str = "auto",
        chunksize: Optional[int] = None,
        block_size: Optional[int] = None,
        cache_type: Optional[str] = None,
        max_workers: int = (cpu_count() or 1) * 5,
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
        auto_optimize_chunksize: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(
            s3_staging_dir=s3_staging_dir,
            schema_name=schema_name,
            catalog_name=catalog_name,
            work_group=work_group,
            poll_interval=poll_interval,
            encryption_option=encryption_option,
            kms_key=kms_key,
            kill_on_interrupt=kill_on_interrupt,
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            **kwargs,
        )
        self._unload = unload
        self._engine = engine
        self._chunksize = chunksize
        self._block_size = block_size
        self._cache_type = cache_type
        self._max_workers = max_workers
        self._auto_optimize_chunksize = auto_optimize_chunksize
        self._result_set: Optional[AthenaPandasResultSet] = None

    @staticmethod
    def get_default_converter(
        unload: bool = False,
    ) -> Union[DefaultPandasTypeConverter, Any]:
        if unload:
            return DefaultPandasUnloadTypeConverter()
        return DefaultPandasTypeConverter()

    async def execute(  # type: ignore[override]
        self,
        operation: str,
        parameters: Optional[Union[Dict[str, Any], List[str]]] = None,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        cache_size: Optional[int] = 0,
        cache_expiration_time: Optional[int] = 0,
        result_reuse_enable: Optional[bool] = None,
        result_reuse_minutes: Optional[int] = None,
        paramstyle: Optional[str] = None,
        keep_default_na: bool = False,
        na_values: Optional[Iterable[str]] = ("",),
        quoting: int = 1,
        **kwargs,
    ) -> "AioPandasCursor":
        """Execute a SQL query asynchronously and return results as pandas DataFrames.

        Args:
            operation: SQL query string to execute.
            parameters: Query parameters for parameterized queries.
            work_group: Athena workgroup to use for this query.
            s3_staging_dir: S3 location for query results.
            cache_size: Number of queries to check for result caching.
            cache_expiration_time: Cache expiration time in seconds.
            result_reuse_enable: Enable Athena result reuse for this query.
            result_reuse_minutes: Minutes to reuse cached results.
            paramstyle: Parameter style ('qmark' or 'pyformat').
            keep_default_na: Whether to keep default pandas NA values.
            na_values: Additional values to treat as NA.
            quoting: CSV quoting behavior (pandas csv.QUOTE_* constants).
            **kwargs: Additional pandas read_csv/read_parquet parameters.

        Returns:
            Self reference for method chaining.
        """
        self._reset_state()
        operation, unload_location = self._prepare_unload(operation, s3_staging_dir)
        self.query_id = await self._execute(
            operation,
            parameters=parameters,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
            cache_expiration_time=cache_expiration_time,
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            paramstyle=paramstyle,
        )

        query_execution = await self._poll(self.query_id)
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self.result_set = await asyncio.to_thread(
                AthenaPandasResultSet,
                connection=self._connection,
                converter=self._converter,
                query_execution=query_execution,
                arraysize=self.arraysize,
                retry_config=self._retry_config,
                keep_default_na=keep_default_na,
                na_values=na_values,
                quoting=quoting,
                unload=self._unload,
                unload_location=unload_location,
                engine=kwargs.pop("engine", self._engine),
                chunksize=kwargs.pop("chunksize", self._chunksize),
                block_size=kwargs.pop("block_size", self._block_size),
                cache_type=kwargs.pop("cache_type", self._cache_type),
                max_workers=kwargs.pop("max_workers", self._max_workers),
                auto_optimize_chunksize=self._auto_optimize_chunksize,
                **kwargs,
            )
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self

    async def fetchone(  # type: ignore[override]
        self,
    ) -> Optional[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch the next row of the result set.

        Wraps the synchronous fetch in ``asyncio.to_thread`` to avoid
        blocking the event loop when ``chunksize`` triggers lazy S3 reads.

        Returns:
            A tuple representing the next row, or None if no more rows.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPandasResultSet, self.result_set)
        return await asyncio.to_thread(result_set.fetchone)

    async def fetchmany(  # type: ignore[override]
        self, size: Optional[int] = None
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch multiple rows from the result set.

        Wraps the synchronous fetch in ``asyncio.to_thread`` to avoid
        blocking the event loop when ``chunksize`` triggers lazy S3 reads.

        Args:
            size: Maximum number of rows to fetch. Defaults to arraysize.

        Returns:
            List of tuples representing the fetched rows.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPandasResultSet, self.result_set)
        return await asyncio.to_thread(result_set.fetchmany, size)

    async def fetchall(  # type: ignore[override]
        self,
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch all remaining rows from the result set.

        Wraps the synchronous fetch in ``asyncio.to_thread`` to avoid
        blocking the event loop when ``chunksize`` triggers lazy S3 reads.

        Returns:
            List of tuples representing all remaining rows.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPandasResultSet, self.result_set)
        return await asyncio.to_thread(result_set.fetchall)

    async def __anext__(self):
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    def as_pandas(self) -> Union["DataFrame", PandasDataFrameIterator]:
        """Return DataFrame or PandasDataFrameIterator based on chunksize setting.

        Returns:
            DataFrame when chunksize is None, PandasDataFrameIterator when chunksize is set.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPandasResultSet, self.result_set)
        return result_set.as_pandas()
