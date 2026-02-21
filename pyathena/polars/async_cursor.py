# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from concurrent.futures import Future
from multiprocessing import cpu_count
from typing import Any, Dict, List, Optional, Tuple, Union, cast

from pyathena import ProgrammingError
from pyathena.async_cursor import AsyncCursor
from pyathena.common import CursorIterator
from pyathena.model import AthenaQueryExecution
from pyathena.polars.converter import (
    DefaultPolarsTypeConverter,
    DefaultPolarsUnloadTypeConverter,
)
from pyathena.polars.result_set import AthenaPolarsResultSet

_logger = logging.getLogger(__name__)


class AsyncPolarsCursor(AsyncCursor):
    """Asynchronous cursor that returns results as Polars DataFrames.

    This cursor extends AsyncCursor to provide asynchronous query execution
    with results returned as Polars DataFrames using Polars' native reading
    capabilities. It does not require PyArrow for basic functionality, but can
    optionally provide Arrow Table access when PyArrow is installed.

    Features:
        - Asynchronous query execution with concurrent futures
        - Native Polars CSV and Parquet reading (no PyArrow required)
        - Memory-efficient columnar data processing
        - Support for UNLOAD operations with Parquet output
        - Optional Arrow interoperability when PyArrow is installed

    Attributes:
        arraysize: Number of rows to fetch per batch (configurable).

    Example:
        >>> from pyathena.polars.async_cursor import AsyncPolarsCursor
        >>>
        >>> cursor = connection.cursor(AsyncPolarsCursor, unload=True)
        >>> query_id, future = cursor.execute("SELECT * FROM large_table")
        >>>
        >>> # Get result when ready
        >>> result_set = future.result()
        >>> df = result_set.as_polars()
        >>>
        >>> # Optional: Convert to Arrow Table if pyarrow is installed
        >>> table = result_set.as_arrow()

    Note:
        Requires polars to be installed. PyArrow is optional and only needed
        for as_arrow() functionality. UNLOAD operations generate Parquet files
        in S3 for optimal performance.
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
        max_workers: int = (cpu_count() or 1) * 5,
        arraysize: int = CursorIterator.DEFAULT_FETCH_SIZE,
        unload: bool = False,
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
        block_size: Optional[int] = None,
        cache_type: Optional[str] = None,
        chunksize: Optional[int] = None,
        **kwargs,
    ) -> None:
        """Initialize an AsyncPolarsCursor.

        Args:
            s3_staging_dir: S3 location for query results.
            schema_name: Default schema name.
            catalog_name: Default catalog name.
            work_group: Athena workgroup name.
            poll_interval: Query status polling interval in seconds.
            encryption_option: S3 encryption option (SSE_S3, SSE_KMS, CSE_KMS).
            kms_key: KMS key ARN for encryption.
            kill_on_interrupt: Cancel running query on keyboard interrupt.
            max_workers: Maximum number of workers for concurrent execution.
            arraysize: Number of rows to fetch per batch.
            unload: Enable UNLOAD for high-performance Parquet output.
            result_reuse_enable: Enable Athena query result reuse.
            result_reuse_minutes: Minutes to reuse cached results.
            block_size: S3 read block size.
            cache_type: S3 caching strategy.
            chunksize: Number of rows per chunk for memory-efficient processing.
                      If specified, data is loaded lazily in chunks for all data
                      access methods including fetchone(), fetchmany(), and iter_chunks().
            **kwargs: Additional connection parameters.

        Example:
            >>> cursor = connection.cursor(AsyncPolarsCursor, unload=True)
            >>> # With chunked processing
            >>> cursor = connection.cursor(AsyncPolarsCursor, chunksize=50000)
        """
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
        self._block_size = block_size
        self._cache_type = cache_type
        self._chunksize = chunksize

    @staticmethod
    def get_default_converter(
        unload: bool = False,
    ) -> Union[DefaultPolarsTypeConverter, DefaultPolarsUnloadTypeConverter, Any]:
        """Get the default type converter for Polars results.

        Args:
            unload: If True, returns converter for UNLOAD (Parquet) results.

        Returns:
            Type converter appropriate for the result format.
        """
        if unload:
            return DefaultPolarsUnloadTypeConverter()
        return DefaultPolarsTypeConverter()

    @property
    def arraysize(self) -> int:
        """Get the number of rows to fetch per batch."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        """Set the number of rows to fetch per batch.

        Args:
            value: Number of rows to fetch. Must be positive.

        Raises:
            ProgrammingError: If value is not positive.
        """
        if value <= 0:
            raise ProgrammingError("arraysize must be a positive integer value.")
        self._arraysize = value

    def _collect_result_set(
        self,
        query_id: str,
        unload_location: Optional[str] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> AthenaPolarsResultSet:
        if kwargs is None:
            kwargs = {}
        query_execution = cast(AthenaQueryExecution, self._poll(query_id))
        return AthenaPolarsResultSet(
            connection=self._connection,
            converter=self._converter,
            query_execution=query_execution,
            arraysize=self._arraysize,
            retry_config=self._retry_config,
            unload=self._unload,
            unload_location=unload_location,
            block_size=self._block_size,
            cache_type=self._cache_type,
            max_workers=self._max_workers,
            chunksize=self._chunksize,
            **kwargs,
        )

    def execute(
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
        **kwargs,
    ) -> Tuple[str, "Future[Union[AthenaPolarsResultSet, Any]]"]:
        """Execute a SQL query asynchronously and return results as Polars DataFrames.

        Executes the SQL query on Amazon Athena asynchronously and returns a
        future that resolves to a result set for Polars DataFrame output.

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
            **kwargs: Additional execution parameters passed to Polars read functions.

        Returns:
            Tuple of (query_id, future) where future resolves to AthenaPolarsResultSet.

        Example:
            >>> query_id, future = cursor.execute("SELECT * FROM sales")
            >>> result_set = future.result()
            >>> df = result_set.as_polars()  # Returns Polars DataFrame
        """
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
                unload_location,
                kwargs,
            ),
        )
