"""Native asyncio cursor that returns Athena query results as Polars DataFrames."""

from __future__ import annotations

import functools
import logging
import operator
from collections.abc import AsyncGenerator, Callable
from multiprocessing import cpu_count
from typing import TYPE_CHECKING, Any

from pyathena.aio.common import WithAsyncFetch
from pyathena.aio.util import create_in_thread
from pyathena.common import CursorIterator
from pyathena.error import OperationalError
from pyathena.model import AthenaQueryExecution
from pyathena.options import ExecuteOptions
from pyathena.polars.converter import (
    DefaultPolarsTypeConverter,
    DefaultPolarsUnloadTypeConverter,
)
from pyathena.polars.result_set import AthenaPolarsResultSet
from pyathena.util import override

if TYPE_CHECKING:
    import polars as pl
    from pyarrow import Table

_logger = logging.getLogger(__name__)


class AioPolarsCursor(WithAsyncFetch):
    """Native asyncio cursor that returns results as Polars DataFrames.

    Runs result set creation (with
    :func:`~pyathena.aio.util.create_in_thread`) and fetch operations in
    threads, keeping the event loop free. This is especially important
    when ``chunksize`` is set, as fetch calls trigger lazy S3 reads.
    :meth:`as_polars`, :meth:`as_arrow`, and :meth:`iter_chunks` also read in
    threads.

    Example:
        >>> async with await pyathena.aio_connect(...) as conn:
        ...     cursor = conn.cursor(AioPolarsCursor)
        ...     await cursor.execute("SELECT * FROM my_table")
        ...     df = await cursor.as_polars()
    """

    def __init__(
        self,
        *,
        s3_staging_dir: str | None = None,
        schema_name: str | None = None,
        catalog_name: str | None = None,
        work_group: str | None = None,
        poll_interval: float = 1,
        encryption_option: str | None = None,
        kms_key: str | None = None,
        kill_on_interrupt: bool = True,
        unload: bool = False,
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
        block_size: int | None = None,
        cache_type: str | None = None,
        s3_max_workers: int = (cpu_count() or 1) * 5,
        chunksize: int | None = None,
        **kwargs,
    ) -> None:
        """Initialize an AioPolarsCursor.

        Args:
            s3_staging_dir: S3 location for query results.
            schema_name: Default schema name.
            catalog_name: Default catalog name.
            work_group: Athena workgroup name.
            poll_interval: Query status polling interval in seconds.
            encryption_option: S3 encryption option for query results.
            kms_key: KMS key for encrypting query results.
            kill_on_interrupt: Cancel the query when the task is cancelled while
                ``execute()`` starts or waits for the query.
            unload: Whether to wrap queries in ``UNLOAD`` and read the Parquet output.
            result_reuse_enable: Whether to enable Athena query result reuse.
            result_reuse_minutes: Maximum age of a reused query result in minutes.
            block_size: Default block size of the S3 filesystem that reads the results.
            cache_type: The default fsspec cache type of the S3 filesystem that reads the results,
                such as ``"none"``, ``"bytes"``, ``"readahead"``, ``"blockcache"``, or
                ``"background"``; see ``S3FileSystem``. Without ``cache_type``, PyAthena's
                filesystem reads a Parquet file of a chunked UNLOAD result that is larger than
                32 MiB without a cache.
            s3_max_workers: Maximum number of workers of the S3 filesystem.
            chunksize: Number of rows per chunk. If set, result files in S3 are read
                lazily in chunks of this size.
            **kwargs: Other cursor arguments, such as ``connection`` and ``arraysize``,
                passed to the parent ``__init__``.
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
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            **kwargs,
        )
        self._unload = unload
        self._block_size = block_size
        self._cache_type = cache_type
        self._s3_max_workers = s3_max_workers
        self._chunksize = chunksize
        self._result_set: AthenaPolarsResultSet | None = None

    @staticmethod
    @override
    def get_default_converter(
        unload: bool = False,
    ) -> DefaultPolarsTypeConverter | DefaultPolarsUnloadTypeConverter | Any:
        if unload:
            return DefaultPolarsUnloadTypeConverter()
        return DefaultPolarsTypeConverter()

    @override
    async def execute(
        self,
        operation: str,
        parameters: dict[str, Any] | list[str] | None = None,
        *,
        work_group: str | None = None,
        s3_staging_dir: str | None = None,
        cache_size: int | None = None,
        cache_expiration_time: int | None = None,
        result_reuse_enable: bool | None = None,
        result_reuse_minutes: int | None = None,
        paramstyle: str | None = None,
        on_start_query_execution: Callable[[str], None] | None = None,
        result_set_type_hints: dict[str | int, str] | None = None,
        options: ExecuteOptions | None = None,
        **kwargs,
    ) -> AioPolarsCursor:
        """Execute a SQL query asynchronously and return results as Polars DataFrames.

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
            on_start_query_execution: Callback invoked with the query ID before ``execute()``
                waits for the query: after the ``StartQueryExecution`` call, or after a
                reusable query ID is found through ``cache_size``.
            result_set_type_hints: Athena type signatures for complex-type columns,
                keyed by column name (case-insensitive) or zero-based column index.
            options: Shared execution options as an
                :class:`~pyathena.options.ExecuteOptions` instance. Individual
                keyword arguments take precedence over ``options`` fields.
            **kwargs: Additional execution parameters passed to Polars read functions.
                ``block_size``, ``cache_type``, ``s3_max_workers``, and ``chunksize``
                override the cursor's values for this query.
                Read function arguments replace the ones the result set chooses, such as
                ``separator``, ``has_header``, ``schema_overrides``, and ``storage_options``
                (see :class:`~pyathena.polars.result_set.AthenaPolarsResultSet`).

        Returns:
            Self reference for method chaining.

        Raises:
            ProgrammingError: If the cursor is closed.
        """
        self._ensure_open()
        await self._async_reset_state()
        options = ExecuteOptions.resolve(
            options,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
            cache_expiration_time=cache_expiration_time,
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            paramstyle=paramstyle,
            on_start_query_execution=on_start_query_execution,
            result_set_type_hints=result_set_type_hints,
        )
        operation, unload_location = self._prepare_unload(operation, options.s3_staging_dir)
        self.query_id = await self._execute(
            operation,
            parameters=parameters,
            options=options,
        )

        # Call user callbacks immediately after start_query_execution
        self._call_on_start_query_execution(self.query_id, options)

        query_execution = await self._poll(self.query_id)
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self.result_set = await create_in_thread(
                functools.partial(
                    AthenaPolarsResultSet,
                    connection=self._connection,
                    converter=self._converter,
                    query_execution=query_execution,
                    arraysize=self.arraysize,
                    retry_config=self._retry_config,
                    unload=self._unload,
                    unload_location=unload_location,
                    block_size=kwargs.pop("block_size", self._block_size),
                    cache_type=kwargs.pop("cache_type", self._cache_type),
                    max_workers=kwargs.pop("s3_max_workers", self._s3_max_workers),
                    chunksize=kwargs.pop("chunksize", self._chunksize),
                    result_set_type_hints=options.result_set_type_hints,
                    **kwargs,
                )
            )
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self

    async def as_polars(self) -> pl.DataFrame:
        """Return the query results as a Polars DataFrame, reading them in a thread.

        When the result is read in chunks, the DataFrame joins the chunks that
        have not yet been read, and a later call returns an empty DataFrame;
        :meth:`iter_chunks` reads them one at a time. Otherwise, it is the same
        DataFrame on every call. A cancellation detaches and closes the result
        set, as one of a fetch does.

        Returns:
            The DataFrame of the query results.

        Raises:
            ProgrammingError: If the cursor is closed or has no result set.
        """
        return await self._fetch_in_thread(operator.methodcaller("as_polars"))

    async def as_arrow(self) -> Table:
        """Return the query results as an Apache Arrow Table, converting them in a thread.

        The Table holds the data that :meth:`as_polars` returns. A cancellation
        detaches and closes the result set, as one of a fetch does.

        Returns:
            The Table of the query results.

        Raises:
            ImportError: If pyarrow is not installed.
            ProgrammingError: If the cursor is closed or has no result set.
        """
        return await self._fetch_in_thread(operator.methodcaller("as_arrow"))

    def iter_chunks(self) -> AsyncGenerator[pl.DataFrame, None]:
        """Iterate over the Polars DataFrame chunks of the query results, reading each in a thread.

        The chunks are those of :meth:`AthenaPolarsResultSet.iter_chunks()
        <pyathena.polars.result_set.AthenaPolarsResultSet.iter_chunks>`, read from the result
        set that the cursor has when this method is called. A cancellation of a read
        detaches and closes the result set, as one of a fetch does. Leaving the
        loop early keeps the chunks that have not been read, which the fetch
        methods and a later ``iter_chunks()`` read.

        Returns:
            An async iterator of the DataFrame chunks. It raises
            ``ProgrammingError`` once the result set is closed, such as by another
            query of the cursor.

        Raises:
            ProgrammingError: If the cursor is closed or has no result set.

        Example:
            >>> async for chunk in cursor.iter_chunks():
            ...     process_chunk(chunk)
        """
        return self._iter_chunks_in_thread(operator.methodcaller("iter_chunks"))
