from __future__ import annotations

import logging
from collections.abc import Iterable
from concurrent.futures import Future
from multiprocessing import cpu_count
from typing import Any, cast

from pyathena import ProgrammingError
from pyathena.async_cursor import AsyncCursor
from pyathena.common import CursorIterator
from pyathena.model import AthenaQueryExecution
from pyathena.pandas.converter import (
    DefaultPandasTypeConverter,
    DefaultPandasUnloadTypeConverter,
)
from pyathena.pandas.result_set import AthenaPandasResultSet

_logger = logging.getLogger(__name__)


class AsyncPandasCursor(AsyncCursor):
    """Asynchronous cursor that returns results as pandas DataFrames.

    This cursor extends AsyncCursor to provide asynchronous query execution
    with results returned as pandas DataFrames. It's designed for data analysis
    workflows where pandas integration is required and non-blocking query
    execution is beneficial.

    Features:
        - Asynchronous query execution with concurrent futures
        - Direct pandas DataFrame results for data analysis
        - Configurable CSV and Parquet engines for optimal performance
        - Support for chunked processing of large datasets
        - UNLOAD operations for improved performance with large results
        - Memory optimization through configurable chunking

    Attributes:
        arraysize: Number of rows to fetch per batch.
        engine: Parsing engine ('auto', 'c', 'python', 'pyarrow').
        chunksize: Number of rows per chunk for large datasets.

    Example:
        >>> from pyathena.pandas.async_cursor import AsyncPandasCursor
        >>>
        >>> cursor = connection.cursor(AsyncPandasCursor, chunksize=10000)
        >>> query_id, future = cursor.execute("SELECT * FROM large_table")
        >>>
        >>> # Get result when ready
        >>> result_set = future.result()
        >>> df = result_set.as_pandas()
        >>>
        >>> # Or iterate through chunks for large datasets
        >>> for chunk_df in result_set:
        ...     process_chunk(chunk_df)

    Note:
        Requires pandas to be installed. For large datasets, consider
        using chunksize or UNLOAD operations for better memory efficiency.
    """

    def __init__(
        self,
        s3_staging_dir: str | None = None,
        schema_name: str | None = None,
        catalog_name: str | None = None,
        work_group: str | None = None,
        poll_interval: float = 1,
        encryption_option: str | None = None,
        kms_key: str | None = None,
        kill_on_interrupt: bool = True,
        max_workers: int = (cpu_count() or 1) * 5,
        arraysize: int = CursorIterator.DEFAULT_FETCH_SIZE,
        unload: bool = False,
        engine: str = "auto",
        chunksize: int | None = None,
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
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
            max_workers=max_workers,
            arraysize=arraysize,
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            **kwargs,
        )
        self._unload = unload
        self._engine = engine
        self._chunksize = chunksize

    @staticmethod
    def get_default_converter(
        unload: bool = False,
    ) -> DefaultPandasTypeConverter | Any:
        if unload:
            return DefaultPandasUnloadTypeConverter()
        return DefaultPandasTypeConverter()

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value <= 0:
            raise ProgrammingError("arraysize must be a positive integer value.")
        self._arraysize = value

    def _collect_result_set(
        self,
        query_id: str,
        keep_default_na: bool = False,
        na_values: Iterable[str] | None = ("",),
        quoting: int = 1,
        unload_location: str | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> AthenaPandasResultSet:
        if kwargs is None:
            kwargs = {}
        query_execution = cast(AthenaQueryExecution, self._poll(query_id))
        return AthenaPandasResultSet(
            connection=self._connection,
            converter=self._converter,
            query_execution=query_execution,
            arraysize=self._arraysize,
            retry_config=self._retry_config,
            keep_default_na=keep_default_na,
            na_values=na_values,
            quoting=quoting,
            unload=self._unload,
            unload_location=unload_location,
            engine=kwargs.pop("engine", self._engine),
            chunksize=kwargs.pop("chunksize", self._chunksize),
            **kwargs,
        )

    def execute(
        self,
        operation: str,
        parameters: dict[str, Any] | list[str] | None = None,
        work_group: str | None = None,
        s3_staging_dir: str | None = None,
        cache_size: int | None = 0,
        cache_expiration_time: int | None = 0,
        result_reuse_enable: bool | None = None,
        result_reuse_minutes: int | None = None,
        paramstyle: str | None = None,
        keep_default_na: bool = False,
        na_values: Iterable[str] | None = ("",),
        quoting: int = 1,
        **kwargs,
    ) -> tuple[str, Future[AthenaPandasResultSet | Any]]:
        operation, unload_location = self._prepare_unload(operation, s3_staging_dir)
        query_id = self._execute(
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
        return (
            query_id,
            self._executor.submit(
                self._collect_result_set,
                query_id,
                keep_default_na,
                na_values,
                quoting,
                unload_location,
                kwargs,
            ),
        )
