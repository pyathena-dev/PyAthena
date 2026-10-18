"""Native asyncio cursor that returns Athena query results as pandas DataFrames."""

from __future__ import annotations

import functools
import logging
import operator
from collections.abc import AsyncGenerator, Callable, Iterable
from multiprocessing import cpu_count
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)

from pyathena.aio.common import WithAsyncFetch
from pyathena.aio.util import create_in_thread
from pyathena.common import CursorIterator
from pyathena.error import OperationalError
from pyathena.model import AthenaQueryExecution
from pyathena.options import ExecuteOptions
from pyathena.pandas.converter import (
    DefaultPandasTypeConverter,
    DefaultPandasUnloadTypeConverter,
)
from pyathena.pandas.result_set import AthenaPandasResultSet, PandasDataFrameIterator
from pyathena.result_set import AthenaResultSet
from pyathena.util import override

if TYPE_CHECKING:
    from pandas import DataFrame

_logger = logging.getLogger(__name__)


class AioPandasCursor(WithAsyncFetch):
    """Native asyncio cursor that returns results as pandas DataFrames.

    Runs result set creation (with
    :func:`~pyathena.aio.util.create_in_thread`) and fetch operations in
    threads, keeping the event loop free. This is especially important
    when ``chunksize`` is set, as fetch calls trigger lazy S3 reads.
    :meth:`as_pandas` and :meth:`iter_chunks` also read in threads.

    Example:
        >>> async with await pyathena.aio_connect(...) as conn:
        ...     cursor = conn.cursor(AioPandasCursor)
        ...     await cursor.execute("SELECT * FROM my_table")
        ...     df = await cursor.as_pandas()
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
        engine: str = "auto",
        chunksize: int | None = None,
        block_size: int | None = None,
        cache_type: str | None = None,
        s3_max_workers: int = (cpu_count() or 1) * 5,
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
        auto_optimize_chunksize: bool = False,
        **kwargs,
    ) -> None:
        """Initialize an AioPandasCursor.

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
            engine: Parsing engine (``auto``, ``c``, ``python``, or ``pyarrow``).
            chunksize: Number of rows per DataFrame chunk when reading CSV results. If set,
                it takes precedence over ``auto_optimize_chunksize``.
            block_size: Default block size of the S3 filesystem that reads the results.
            cache_type: The default fsspec cache type of the S3 filesystem that reads the results,
                such as ``"none"``, ``"bytes"``, ``"readahead"``, ``"blockcache"``, or
                ``"background"``; see ``S3FileSystem``. Without ``block_size`` or ``cache_type``, a
                CSV result file is read in 32 MiB blocks with fsspec's background cache, which reads
                the next block while pandas parses the current one.
                Without ``cache_type``, PyAthena's filesystem reads a Parquet file of a chunked
                UNLOAD result that is larger than 32 MiB without a cache.
            s3_max_workers: Maximum number of workers of the S3 filesystem.
            result_reuse_enable: Whether to enable Athena query result reuse.
            result_reuse_minutes: Maximum age of a reused query result in minutes.
            auto_optimize_chunksize: Whether to choose a chunk size from the size of the
                CSV result file when ``chunksize`` is None.
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
        self._engine = engine
        self._chunksize = chunksize
        self._block_size = block_size
        self._cache_type = cache_type
        self._s3_max_workers = s3_max_workers
        self._auto_optimize_chunksize = auto_optimize_chunksize
        self._result_set: AthenaPandasResultSet | None = None

    @staticmethod
    @override
    def get_default_converter(
        unload: bool = False,
    ) -> DefaultPandasTypeConverter | Any:
        if unload:
            return DefaultPandasUnloadTypeConverter()
        return DefaultPandasTypeConverter()

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
        keep_default_na: bool = False,
        na_values: Iterable[str] | None = ("",),
        quoting: int = 1,
        on_start_query_execution: Callable[[str], None] | None = None,
        result_set_type_hints: dict[str | int, str] | None = None,
        options: ExecuteOptions | None = None,
        **kwargs,
    ) -> AioPandasCursor:
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
            on_start_query_execution: Callback invoked with the query ID before ``execute()``
                waits for the query: after the ``StartQueryExecution`` call, or after a
                reusable query ID is found through ``cache_size``.
            result_set_type_hints: Athena type signatures for complex-type columns,
                keyed by column name (case-insensitive) or zero-based column index.
            options: Shared execution options as an
                :class:`~pyathena.options.ExecuteOptions` instance. Individual
                keyword arguments take precedence over ``options`` fields.
            **kwargs: Additional pandas read_csv/read_parquet parameters.
                ``engine``, ``chunksize``, ``block_size``, ``cache_type``, ``s3_max_workers``,
                and ``auto_optimize_chunksize`` override the cursor's values for this query.
                ``storage_options`` and, for UNLOAD results, ``filesystem`` replace
                PyAthena's S3 filesystem (see
                :class:`~pyathena.pandas.result_set.AthenaPandasResultSet`).

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
                    max_workers=kwargs.pop("s3_max_workers", self._s3_max_workers),
                    auto_optimize_chunksize=kwargs.pop(
                        "auto_optimize_chunksize", self._auto_optimize_chunksize
                    ),
                    result_set_type_hints=options.result_set_type_hints,
                    **kwargs,
                )
            )
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self

    async def as_pandas(self) -> DataFrame:
        """Return the query results as one DataFrame, reading them in a thread.

        Unlike :meth:`PandasCursor.as_pandas()
        <pyathena.pandas.cursor.PandasCursor.as_pandas>`, this method always
        returns a DataFrame. When the result is read in chunks, it joins the
        chunks that have not yet been read, as ``PandasDataFrameIterator.as_pandas()``
        does, and a later call returns an empty DataFrame; :meth:`iter_chunks`
        reads them one at a time. Otherwise, it returns the same DataFrame on
        every call. A cancellation detaches and closes the result set, as one of
        a fetch does.

        Returns:
            The DataFrame of the query results.

        Raises:
            ProgrammingError: If the cursor is closed or has no result set.
        """

        def read(result_set: AthenaResultSet) -> DataFrame:
            """Read the results into one DataFrame."""
            df = cast(AthenaPandasResultSet, result_set).as_pandas()
            return df.as_pandas() if isinstance(df, PandasDataFrameIterator) else df

        return await self._fetch_in_thread(read)

    def iter_chunks(self) -> AsyncGenerator[DataFrame, None]:
        """Iterate over the DataFrame chunks of the query results, reading each in a thread.

        The chunks are those of :meth:`AthenaPandasResultSet.iter_chunks()
        <pyathena.pandas.result_set.AthenaPandasResultSet.iter_chunks>`, read from the result
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
