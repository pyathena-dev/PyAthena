from __future__ import annotations

import logging
from collections.abc import Callable, Generator, Iterable
from multiprocessing import cpu_count
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)

from pyathena.common import CursorIterator
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.pandas.converter import (
    DefaultPandasTypeConverter,
    DefaultPandasUnloadTypeConverter,
)
from pyathena.pandas.result_set import AthenaPandasResultSet, PandasDataFrameIterator
from pyathena.result_set import WithFetch

if TYPE_CHECKING:
    from pandas import DataFrame

_logger = logging.getLogger(__name__)


class PandasCursor(WithFetch):
    """Cursor for handling pandas DataFrame results from Athena queries.

    This cursor returns query results as pandas DataFrames with memory-efficient
    processing through chunking support and automatic chunksize optimization
    for large result sets. It's ideal for data analysis and data science workflows.

    The cursor supports both regular CSV-based results and high-performance
    UNLOAD operations that return results in Parquet format, which is significantly
    faster for large datasets and preserves data types more accurately.

    Attributes:
        description: Sequence of column descriptions for the last query.
        rowcount: Number of rows affected by the last query (-1 for SELECT queries).
        arraysize: Default number of rows to fetch with fetchmany().
        chunksize: Number of rows per chunk when iterating through results.

    Example:
        >>> from pyathena.pandas.cursor import PandasCursor
        >>> cursor = connection.cursor(PandasCursor)
        >>> cursor.execute("SELECT * FROM sales_data WHERE year = 2023")
        >>> df = cursor.fetchall()  # Returns pandas DataFrame
        >>> print(df.describe())

        # Memory-efficient iteration for large datasets
        >>> cursor.execute("SELECT * FROM huge_table")
        >>> for chunk_df in cursor:
        ...     process_chunk(chunk_df)  # Process data in chunks

        # High-performance UNLOAD for large datasets
        >>> cursor = connection.cursor(PandasCursor, unload=True)
        >>> cursor.execute("SELECT * FROM big_table")
        >>> df = cursor.fetchall()  # Faster Parquet-based result
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
        unload: bool = False,
        engine: str = "auto",
        chunksize: int | None = None,
        block_size: int | None = None,
        cache_type: str | None = None,
        max_workers: int = (cpu_count() or 1) * 5,
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
        auto_optimize_chunksize: bool = False,
        on_start_query_execution: Callable[[str], None] | None = None,
        **kwargs,
    ) -> None:
        """Initialize PandasCursor with configuration options.

        Args:
            s3_staging_dir: S3 directory for query result staging.
            schema_name: Default schema name for queries.
            catalog_name: Default catalog name for queries.
            work_group: Athena workgroup name.
            poll_interval: Query polling interval in seconds.
            encryption_option: S3 encryption option.
            kms_key: KMS key for encryption.
            kill_on_interrupt: Cancel query on interrupt signal.
            unload: Use UNLOAD statement for faster result retrieval.
            engine: CSV parsing engine ('auto', 'c', 'python', 'pyarrow').
            chunksize: Number of rows per chunk for memory-efficient processing.
                      If specified, takes precedence over auto_optimize_chunksize.
            block_size: S3 read block size.
            cache_type: S3 caching strategy.
            max_workers: Maximum worker threads for parallel processing.
            result_reuse_enable: Enable query result reuse.
            result_reuse_minutes: Result reuse duration in minutes.
            auto_optimize_chunksize: Enable automatic chunksize determination for
                                   large files. Only effective when chunksize is None.
                                   Default: False (no automatic chunking).
            on_start_query_execution: Callback for query start events.
            **kwargs: Additional arguments passed to pandas.read_csv.
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
        self._max_workers = max_workers
        self._auto_optimize_chunksize = auto_optimize_chunksize
        self._on_start_query_execution = on_start_query_execution

    @staticmethod
    def get_default_converter(
        unload: bool = False,
    ) -> DefaultPandasTypeConverter | Any:
        if unload:
            return DefaultPandasUnloadTypeConverter()
        return DefaultPandasTypeConverter()

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
        on_start_query_execution: Callable[[str], None] | None = None,
        result_set_type_hints: dict[str | int, str] | None = None,
        **kwargs,
    ) -> PandasCursor:
        """Execute a SQL query and return results as pandas DataFrames.

        Executes the SQL query on Amazon Athena and configures the result set
        for pandas DataFrame output. Supports both regular CSV-based results
        and high-performance UNLOAD operations with Parquet format.

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
            on_start_query_execution: Callback called when query starts.
            result_set_type_hints: Optional dictionary mapping column names to
                Athena DDL type signatures for precise type conversion within
                complex types.
            **kwargs: Additional pandas read_csv/read_parquet parameters.

        Returns:
            Self reference for method chaining.

        Example:
            >>> cursor.execute("SELECT * FROM sales WHERE year = %(year)s",
            ...                {"year": 2023})
            >>> df = cursor.fetchall()  # Returns pandas DataFrame
        """
        self._reset_state()
        operation, unload_location = self._prepare_unload(operation, s3_staging_dir)
        self.query_id = self._execute(
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

        # Call user callbacks immediately after start_query_execution
        # Both connection-level and execute-level callbacks are invoked if set
        if self._on_start_query_execution:
            self._on_start_query_execution(self.query_id)
        if on_start_query_execution:
            on_start_query_execution(self.query_id)
        query_execution = cast(AthenaQueryExecution, self._poll(self.query_id))
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self.result_set = AthenaPandasResultSet(
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
                result_set_type_hints=result_set_type_hints,
                **kwargs,
            )
        else:
            raise OperationalError(query_execution.state_change_reason)

        return self

    def as_pandas(self) -> DataFrame | PandasDataFrameIterator:
        """Return DataFrame or PandasDataFrameIterator based on chunksize setting.

        Returns:
            DataFrame when chunksize is None, PandasDataFrameIterator when chunksize is set.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPandasResultSet, self.result_set)
        return result_set.as_pandas()

    def iter_chunks(self) -> Generator[DataFrame, None, None]:
        """Iterate over DataFrame chunks for memory-efficient processing.

        This method provides an iterator interface for processing large result sets
        in chunks, preventing memory exhaustion when working with datasets that are
        too large to fit in memory as a single DataFrame.

        Chunking behavior:
        - If chunksize is explicitly set, uses that value
        - If auto_optimize_chunksize=True and chunksize=None, automatically determines
          optimal chunksize based on file size
        - If auto_optimize_chunksize=False and chunksize=None, yields entire DataFrame

        Yields:
            DataFrame: Individual chunks of the result set when chunking is enabled,
                      or the entire DataFrame as a single chunk when chunking is disabled.

        Examples:
            # Explicit chunksize
            cursor = connection.cursor(PandasCursor, chunksize=50000)
            cursor.execute("SELECT * FROM large_table")
            for chunk in cursor.iter_chunks():
                process_chunk(chunk)

            # Auto-optimization enabled
            cursor = connection.cursor(PandasCursor, auto_optimize_chunksize=True)
            cursor.execute("SELECT * FROM large_table")
            for chunk in cursor.iter_chunks():
                process_chunk(chunk)  # Chunks determined automatically for large files

            # No chunking (default behavior)
            cursor = connection.cursor(PandasCursor)
            cursor.execute("SELECT * FROM large_table")
            for chunk in cursor.iter_chunks():
                process_chunk(chunk)  # Single DataFrame regardless of size
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPandasResultSet, self.result_set)

        import gc

        for chunk_count, chunk in enumerate(result_set.iter_chunks(), 1):
            yield chunk

            # Suggest garbage collection every 10 chunks for large datasets
            if chunk_count % 10 == 0:
                gc.collect()
