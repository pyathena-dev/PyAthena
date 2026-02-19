# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from multiprocessing import cpu_count
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

from pyathena.common import BaseCursor, CursorIterator
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaCompression, AthenaFileFormat, AthenaQueryExecution
from pyathena.polars.converter import (
    DefaultPolarsTypeConverter,
    DefaultPolarsUnloadTypeConverter,
)
from pyathena.polars.result_set import AthenaPolarsResultSet
from pyathena.result_set import WithResultSet

if TYPE_CHECKING:
    import polars as pl
    from pyarrow import Table

_logger = logging.getLogger(__name__)


class PolarsCursor(BaseCursor, CursorIterator, WithResultSet):
    """Cursor for handling Polars DataFrame results from Athena queries.

    This cursor returns query results as Polars DataFrames using Polars' native
    reading capabilities. It does not require PyArrow for basic functionality,
    but can optionally provide Arrow Table access when PyArrow is installed.

    The cursor supports both regular CSV-based results and high-performance
    UNLOAD operations that return results in Parquet format for improved
    performance with large datasets.

    Attributes:
        description: Sequence of column descriptions for the last query.
        rowcount: Number of rows affected by the last query (-1 for SELECT queries).
        arraysize: Default number of rows to fetch with fetchmany().

    Example:
        >>> from pyathena.polars.cursor import PolarsCursor
        >>> cursor = connection.cursor(PolarsCursor)
        >>> cursor.execute("SELECT * FROM large_table")
        >>> df = cursor.as_polars()  # Returns polars.DataFrame

        # Optional: Get Arrow Table (requires pyarrow)
        >>> table = cursor.as_arrow()

        # High-performance UNLOAD for large datasets
        >>> cursor = connection.cursor(PolarsCursor, unload=True)
        >>> cursor.execute("SELECT * FROM huge_table")
        >>> df = cursor.as_polars()  # Faster Parquet-based result

    Note:
        Requires polars to be installed. PyArrow is optional and only
        needed for as_arrow() functionality.
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
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
        on_start_query_execution: Optional[Callable[[str], None]] = None,
        block_size: Optional[int] = None,
        cache_type: Optional[str] = None,
        max_workers: int = (cpu_count() or 1) * 5,
        chunksize: Optional[int] = None,
        **kwargs,
    ) -> None:
        """Initialize a PolarsCursor.

        Args:
            s3_staging_dir: S3 location for query results.
            schema_name: Default schema name.
            catalog_name: Default catalog name.
            work_group: Athena workgroup name.
            poll_interval: Query status polling interval in seconds.
            encryption_option: S3 encryption option (SSE_S3, SSE_KMS, CSE_KMS).
            kms_key: KMS key ARN for encryption.
            kill_on_interrupt: Cancel running query on keyboard interrupt.
            unload: Enable UNLOAD for high-performance Parquet output.
            result_reuse_enable: Enable Athena query result reuse.
            result_reuse_minutes: Minutes to reuse cached results.
            on_start_query_execution: Callback invoked when query starts.
            block_size: S3 read block size.
            cache_type: S3 caching strategy.
            max_workers: Maximum worker threads for parallel S3 operations.
            chunksize: Number of rows per chunk for memory-efficient processing.
                      If specified, data is loaded lazily in chunks for all data
                      access methods including fetchone(), fetchmany(), and iter_chunks().
            **kwargs: Additional connection parameters.

        Example:
            >>> cursor = connection.cursor(PolarsCursor, unload=True)
            >>> # With chunked processing
            >>> cursor = connection.cursor(PolarsCursor, chunksize=50000)
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
        self._on_start_query_execution = on_start_query_execution
        self._block_size = block_size
        self._cache_type = cache_type
        self._max_workers = max_workers
        self._chunksize = chunksize
        self._query_id: Optional[str] = None
        self._result_set: Optional[AthenaPolarsResultSet] = None

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

    @property  # type: ignore
    def result_set(self) -> Optional[AthenaPolarsResultSet]:
        """Get the current result set."""
        return self._result_set

    @result_set.setter
    def result_set(self, val) -> None:
        """Set the current result set."""
        self._result_set = val

    @property
    def query_id(self) -> Optional[str]:
        """Get the current query execution ID."""
        return self._query_id

    @query_id.setter
    def query_id(self, val) -> None:
        """Set the current query execution ID."""
        self._query_id = val

    @property
    def rownumber(self) -> Optional[int]:
        """Get the current row number in the result set."""
        return self.result_set.rownumber if self.result_set else None

    @property
    def rowcount(self) -> int:
        """Get the number of rows affected by the last operation."""
        return self.result_set.rowcount if self.result_set else -1

    def close(self) -> None:
        """Close the cursor and release resources."""
        if self.result_set and not self.result_set.is_closed:
            self.result_set.close()

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
        on_start_query_execution: Optional[Callable[[str], None]] = None,
        **kwargs,
    ) -> "PolarsCursor":
        """Execute a SQL query and return results as Polars DataFrames.

        Executes the SQL query on Amazon Athena and configures the result set
        for Polars DataFrame output using Polars' native reading capabilities.

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
            on_start_query_execution: Callback called when query starts.
            **kwargs: Additional execution parameters passed to Polars read functions.

        Returns:
            Self reference for method chaining.

        Example:
            >>> cursor.execute("SELECT * FROM sales WHERE year = 2023")
            >>> df = cursor.as_polars()  # Returns Polars DataFrame
        """
        self._reset_state()
        if self._unload:
            s3_staging_dir = s3_staging_dir if s3_staging_dir else self._s3_staging_dir
            if not s3_staging_dir:
                raise ProgrammingError("If the unload option is used, s3_staging_dir is required.")
            operation, unload_location = self._formatter.wrap_unload(
                operation,
                s3_staging_dir=s3_staging_dir,
                format_=AthenaFileFormat.FILE_FORMAT_PARQUET,
                compression=AthenaCompression.COMPRESSION_SNAPPY,
            )
        else:
            unload_location = None
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
            self.result_set = AthenaPolarsResultSet(
                connection=self._connection,
                converter=self._converter,
                query_execution=query_execution,
                arraysize=self.arraysize,
                retry_config=self._retry_config,
                unload=self._unload,
                unload_location=unload_location,
                block_size=self._block_size,
                cache_type=self._cache_type,
                max_workers=self._max_workers,
                chunksize=self._chunksize,
                **kwargs,
            )
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self

    def executemany(
        self,
        operation: str,
        seq_of_parameters: List[Optional[Union[Dict[str, Any], List[str]]]],
        **kwargs,
    ) -> None:
        """Execute a SQL query multiple times with different parameters.

        Args:
            operation: SQL query string to execute.
            seq_of_parameters: Sequence of parameter sets.
            **kwargs: Additional execution parameters.
        """
        for parameters in seq_of_parameters:
            self.execute(operation, parameters, **kwargs)
        # Operations that have result sets are not allowed with executemany.
        self._reset_state()

    def cancel(self) -> None:
        """Cancel the currently running query.

        Raises:
            ProgrammingError: If no query is currently running.
        """
        if not self.query_id:
            raise ProgrammingError("QueryExecutionId is none or empty.")
        self._cancel(self.query_id)

    def fetchone(
        self,
    ) -> Optional[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch the next row of the query result.

        Returns:
            A single row as a tuple, or None if no more rows are available.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPolarsResultSet, self.result_set)
        return result_set.fetchone()

    def fetchmany(
        self, size: Optional[int] = None
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch the next set of rows of the query result.

        Args:
            size: Number of rows to fetch. Defaults to arraysize.

        Returns:
            A list of rows as tuples.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPolarsResultSet, self.result_set)
        return result_set.fetchmany(size)

    def fetchall(
        self,
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch all remaining rows of the query result.

        Returns:
            A list of all remaining rows as tuples.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPolarsResultSet, self.result_set)
        return result_set.fetchall()

    def as_polars(self) -> "pl.DataFrame":
        """Return query results as a Polars DataFrame.

        Returns the query results as a Polars DataFrame. This is the primary
        method for accessing results with PolarsCursor.

        Returns:
            Polars DataFrame containing all query results.

        Raises:
            ProgrammingError: If no query has been executed or no results are available.

        Example:
            >>> cursor = connection.cursor(PolarsCursor)
            >>> cursor.execute("SELECT * FROM my_table")
            >>> df = cursor.as_polars()
            >>> print(f"DataFrame has {df.height} rows and {df.width} columns")
            >>> filtered = df.filter(pl.col("value") > 100)
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPolarsResultSet, self.result_set)
        return result_set.as_polars()

    def as_arrow(self) -> "Table":
        """Return query results as an Apache Arrow Table.

        Converts the Polars DataFrame to an Apache Arrow Table for
        interoperability with other Arrow-compatible tools and libraries.

        Returns:
            Apache Arrow Table containing all query results.

        Raises:
            ProgrammingError: If no query has been executed or no results are available.
            ImportError: If pyarrow is not installed.

        Example:
            >>> cursor = connection.cursor(PolarsCursor)
            >>> cursor.execute("SELECT * FROM my_table")
            >>> table = cursor.as_arrow()
            >>> print(f"Table has {table.num_rows} rows and {table.num_columns} columns")
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPolarsResultSet, self.result_set)
        return result_set.as_arrow()

    def iter_chunks(self) -> Iterator["pl.DataFrame"]:
        """Iterate over result chunks as Polars DataFrames.

        This method provides an iterator interface for processing result sets.
        When chunksize is specified, it yields DataFrames in chunks using lazy
        evaluation for memory-efficient processing. When chunksize is not specified,
        it yields the entire result as a single DataFrame, providing a consistent
        interface regardless of chunking configuration.

        Yields:
            Polars DataFrame for each chunk of rows, or the entire DataFrame
            if chunksize was not specified.

        Raises:
            ProgrammingError: If no result set is available.

        Example:
            >>> # With chunking for large datasets
            >>> cursor = connection.cursor(PolarsCursor, chunksize=50000)
            >>> cursor.execute("SELECT * FROM large_table")
            >>> for chunk in cursor.iter_chunks():
            ...     process_chunk(chunk)  # Each chunk is a Polars DataFrame
            >>>
            >>> # Without chunking - yields entire result as single chunk
            >>> cursor = connection.cursor(PolarsCursor)
            >>> cursor.execute("SELECT * FROM small_table")
            >>> for df in cursor.iter_chunks():
            ...     process(df)  # Single DataFrame with all data
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPolarsResultSet, self.result_set)
        yield from result_set.iter_chunks()
