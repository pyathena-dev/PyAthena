# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union, cast

from pyathena.arrow.converter import (
    DefaultArrowTypeConverter,
    DefaultArrowUnloadTypeConverter,
)
from pyathena.arrow.result_set import AthenaArrowResultSet
from pyathena.common import CursorIterator
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import WithFetch

if TYPE_CHECKING:
    import polars as pl
    from pyarrow import Table

_logger = logging.getLogger(__name__)  # type: ignore


class ArrowCursor(WithFetch):
    """Cursor for handling Apache Arrow Table results from Athena queries.

    This cursor returns query results as Apache Arrow Tables, which provide
    efficient columnar data processing and memory usage. Arrow Tables are
    especially useful for analytical workloads and data science applications.

    The cursor supports both regular CSV-based results and high-performance
    UNLOAD operations that return results in Parquet format for improved
    performance with large datasets.

    Attributes:
        description: Sequence of column descriptions for the last query.
        rowcount: Number of rows affected by the last query (-1 for SELECT queries).
        arraysize: Default number of rows to fetch with fetchmany().

    Example:
        >>> from pyathena.arrow.cursor import ArrowCursor
        >>> cursor = connection.cursor(ArrowCursor)
        >>> cursor.execute("SELECT * FROM large_table")
        >>> table = cursor.fetchall()  # Returns pyarrow.Table
        >>> df = table.to_pandas()  # Convert to pandas if needed

        # High-performance UNLOAD for large datasets
        >>> cursor = connection.cursor(ArrowCursor, unload=True)
        >>> cursor.execute("SELECT * FROM huge_table")
        >>> table = cursor.fetchall()  # Faster Parquet-based result
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
        connect_timeout: Optional[float] = None,
        request_timeout: Optional[float] = None,
        **kwargs,
    ) -> None:
        """Initialize an ArrowCursor.

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
            connect_timeout: Socket connection timeout in seconds for S3 operations.
                Defaults to AWS SDK default (typically 1 second) if not specified.
            request_timeout: Request timeout in seconds for S3 operations.
                Defaults to AWS SDK default (typically 3 seconds) if not specified.
                Increase this value if you experience timeout errors when using
                role assumption with STS or have high latency to S3.
            **kwargs: Additional connection parameters.

        Example:
            >>> # Use higher timeouts for role assumption scenarios
            >>> cursor = connection.cursor(
            ...     ArrowCursor,
            ...     connect_timeout=10,
            ...     request_timeout=30
            ... )
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
        self._connect_timeout = connect_timeout
        self._request_timeout = request_timeout

    @staticmethod
    def get_default_converter(
        unload: bool = False,
    ) -> Union[DefaultArrowTypeConverter, DefaultArrowUnloadTypeConverter, Any]:
        if unload:
            return DefaultArrowUnloadTypeConverter()
        return DefaultArrowTypeConverter()

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
    ) -> ArrowCursor:
        """Execute a SQL query and return results as Apache Arrow Tables.

        Executes the SQL query on Amazon Athena and configures the result set
        for Apache Arrow Table output. Arrow format provides high-performance
        columnar data processing with efficient memory usage.

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
            **kwargs: Additional execution parameters.

        Returns:
            Self reference for method chaining.

        Example:
            >>> cursor.execute("SELECT * FROM sales WHERE year = 2023")
            >>> table = cursor.as_arrow()  # Returns Apache Arrow Table
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
            self.result_set = AthenaArrowResultSet(
                connection=self._connection,
                converter=self._converter,
                query_execution=query_execution,
                arraysize=self.arraysize,
                retry_config=self._retry_config,
                unload=self._unload,
                unload_location=unload_location,
                connect_timeout=self._connect_timeout,
                request_timeout=self._request_timeout,
                **kwargs,
            )
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self

    def as_arrow(self) -> "Table":
        """Return query results as an Apache Arrow Table.

        Converts the entire result set into an Apache Arrow Table for efficient
        columnar data processing. Arrow Tables provide excellent performance for
        analytical workloads and interoperability with other data processing frameworks.

        Returns:
            Apache Arrow Table containing all query results.

        Raises:
            ProgrammingError: If no query has been executed or no results are available.

        Example:
            >>> cursor = connection.cursor(ArrowCursor)
            >>> cursor.execute("SELECT * FROM my_table")
            >>> table = cursor.as_arrow()
            >>> print(f"Table has {table.num_rows} rows and {table.num_columns} columns")
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaArrowResultSet, self.result_set)
        return result_set.as_arrow()

    def as_polars(self) -> "pl.DataFrame":
        """Return query results as a Polars DataFrame.

        Converts the Apache Arrow Table to a Polars DataFrame for
        interoperability with the Polars data processing library.

        Returns:
            Polars DataFrame containing all query results.

        Raises:
            ProgrammingError: If no query has been executed or no results are available.
            ImportError: If polars is not installed.

        Example:
            >>> cursor = connection.cursor(ArrowCursor)
            >>> cursor.execute("SELECT * FROM my_table")
            >>> df = cursor.as_polars()
            >>> print(f"DataFrame has {df.height} rows and {df.width} columns")
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaArrowResultSet, self.result_set)
        return result_set.as_polars()
