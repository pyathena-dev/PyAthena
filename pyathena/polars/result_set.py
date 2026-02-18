# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from collections import abc
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

from pyathena import OperationalError
from pyathena.converter import Converter
from pyathena.error import ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.polars.util import to_column_info
from pyathena.result_set import AthenaResultSet
from pyathena.util import RetryConfig

if TYPE_CHECKING:
    import polars as pl
    from pyarrow import Table

    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)


def _identity(x: Any) -> Any:
    """Identity function for use as default converter."""
    return x


class PolarsDataFrameIterator(abc.Iterator):  # type: ignore
    """Iterator for chunked DataFrame results from Athena queries.

    This class wraps either a Polars DataFrame iterator (for chunked reading) or
    a single DataFrame, providing a unified iterator interface. It applies
    optional type conversion to each DataFrame chunk as it's yielded.

    The iterator is used by AthenaPolarsResultSet to provide chunked access
    to large query results, enabling memory-efficient processing of datasets
    that would be too large to load entirely into memory.

    Example:
        >>> # Iterate over DataFrame chunks
        >>> for df_chunk in iterator:
        ...     process(df_chunk)
        >>>
        >>> # Iterate over individual rows
        >>> for idx, row in iterator.iterrows():
        ...     print(row)

    Note:
        This class is primarily for internal use by AthenaPolarsResultSet.
        Most users should access results through PolarsCursor methods.
    """

    def __init__(
        self,
        reader: Union[Iterator["pl.DataFrame"], "pl.DataFrame"],
        converters: Dict[str, Callable[[Optional[str]], Optional[Any]]],
        column_names: List[str],
    ) -> None:
        """Initialize the iterator.

        Args:
            reader: Either a DataFrame iterator (for chunked) or a single DataFrame.
            converters: Dictionary mapping column names to converter functions.
            column_names: List of column names in order.
        """
        import polars as pl

        if isinstance(reader, pl.DataFrame):
            self._reader: Iterator["pl.DataFrame"] = iter([reader])
        else:
            self._reader = reader
        self._converters = converters
        self._column_names = column_names

    def __next__(self) -> "pl.DataFrame":
        """Get the next DataFrame chunk.

        Returns:
            The next Polars DataFrame chunk.

        Raises:
            StopIteration: When no more chunks are available.
        """
        try:
            return next(self._reader)
        except StopIteration:
            self.close()
            raise

    def __iter__(self) -> "PolarsDataFrameIterator":
        """Return self as iterator."""
        return self

    def __enter__(self) -> "PolarsDataFrameIterator":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Context manager exit."""
        self.close()

    def close(self) -> None:
        """Close the iterator and release resources."""
        from types import GeneratorType

        if isinstance(self._reader, GeneratorType):
            self._reader.close()

    def iterrows(self) -> Iterator[Tuple[int, Dict[str, Any]]]:
        """Iterate over rows as (index, row_dict) tuples.

        Yields:
            Tuple of (row_index, row_dict) for each row across all chunks.
        """
        row_num = 0
        for df in self:
            for row_dict in df.iter_rows(named=True):
                # Apply converters (use module-level _identity to avoid creating lambdas)
                processed_row = {
                    col: self._converters.get(col, _identity)(row_dict.get(col))
                    for col in self._column_names
                }
                yield (row_num, processed_row)
                row_num += 1

    def as_polars(self) -> "pl.DataFrame":
        """Collect all chunks into a single DataFrame.

        Returns:
            Single Polars DataFrame containing all data.
        """
        import polars as pl

        dfs = cast(List["pl.DataFrame"], list(self))
        if not dfs:
            return pl.DataFrame()
        if len(dfs) == 1:
            return dfs[0]
        return pl.concat(dfs)


class AthenaPolarsResultSet(AthenaResultSet):
    """Result set that provides Polars DataFrame results with optional Arrow interoperability.

    This result set handles CSV and Parquet result files from S3, converting them to
    Polars DataFrames using Polars' native reading capabilities. It does not require
    PyArrow for basic functionality, but can optionally provide Arrow Table access
    when PyArrow is installed.

    Features:
        - Native Polars CSV and Parquet reading (no PyArrow required)
        - Efficient columnar data processing with Polars
        - Optional Arrow interoperability when PyArrow is available
        - Support for both CSV and Parquet result formats
        - Chunked iteration for memory-efficient processing of large datasets
        - Optimized memory usage through columnar format

    Example:
        >>> # Used automatically by PolarsCursor
        >>> cursor = connection.cursor(PolarsCursor)
        >>> cursor.execute("SELECT * FROM large_table")
        >>>
        >>> # Get Polars DataFrame
        >>> df = cursor.as_polars()
        >>>
        >>> # Work with Polars
        >>> print(f"DataFrame has {df.height} rows and {df.width} columns")
        >>> filtered = df.filter(pl.col("value") > 100)
        >>>
        >>> # Optional: Get Arrow Table (requires pyarrow)
        >>> table = cursor.as_arrow()
        >>>
        >>> # Memory-efficient chunked iteration
        >>> cursor = connection.cursor(PolarsCursor, chunksize=50000)
        >>> cursor.execute("SELECT * FROM huge_table")
        >>> for chunk in cursor.iter_chunks():
        ...     process_chunk(chunk)

    Note:
        This class is used internally by PolarsCursor and typically not
        instantiated directly by users. Requires polars to be installed.
        PyArrow is optional and only needed for as_arrow() functionality.
    """

    def __init__(
        self,
        connection: "Connection[Any]",
        converter: Converter,
        query_execution: AthenaQueryExecution,
        arraysize: int,
        retry_config: RetryConfig,
        unload: bool = False,
        unload_location: Optional[str] = None,
        block_size: Optional[int] = None,
        cache_type: Optional[str] = None,
        max_workers: int = (cpu_count() or 1) * 5,
        chunksize: Optional[int] = None,
        **kwargs,
    ) -> None:
        """Initialize the Polars result set.

        Args:
            connection: The Athena connection object.
            converter: Type converter for Athena data types.
            query_execution: Query execution metadata.
            arraysize: Number of rows to fetch per batch.
            retry_config: Configuration for retry behavior.
            unload: Whether this is an UNLOAD query result.
            unload_location: S3 location for UNLOAD results.
            block_size: Block size for S3 file reading.
            cache_type: Cache type for S3 file system.
            max_workers: Maximum number of worker threads.
            chunksize: Number of rows per chunk for memory-efficient processing.
                      If specified, data is loaded lazily in chunks for all data
                      access methods including fetchone(), fetchmany(), and iter_chunks().
            **kwargs: Additional arguments passed to Polars read functions.
        """
        super().__init__(
            connection=connection,
            converter=converter,
            query_execution=query_execution,
            arraysize=1,  # Fetch one row to retrieve metadata
            retry_config=retry_config,
        )
        self._rows.clear()  # Clear pre_fetch data
        self._arraysize = arraysize
        self._unload = unload
        self._unload_location = unload_location
        self._block_size = block_size
        self._cache_type = cache_type
        self._max_workers = max_workers
        self._chunksize = chunksize
        self._kwargs = kwargs

        # Build DataFrame iterator (handles both chunked and non-chunked cases)
        # Note: _create_dataframe_iterator() calls _as_polars() which may update
        # _metadata for unload queries, so we must cache column names AFTER this.
        if self.state == AthenaQueryExecution.STATE_SUCCEEDED and self.output_location:
            self._df_iter = self._create_dataframe_iterator()
        elif self.state == AthenaQueryExecution.STATE_SUCCEEDED:
            df = self._as_polars_from_api()
            self._df_iter = PolarsDataFrameIterator(df, self.converters, self._get_column_names())
        else:
            import polars as pl

            self._df_iter = PolarsDataFrameIterator(
                pl.DataFrame(), self.converters, self._get_column_names()
            )

        # Cache column names for efficient access in fetchone()
        # Must be after _create_dataframe_iterator() which updates _metadata for unload
        self._column_names_cache: List[str] = self._get_column_names()
        self._iterrows = self._df_iter.iterrows()

    @property
    def _csv_storage_options(self) -> Dict[str, Any]:
        """Get storage options for Polars CSV reading via fsspec.

        Polars read_csv uses fsspec for cloud storage access, which works
        with PyAthena's registered S3FileSystem.

        Returns:
            Dictionary with fsspec-compatible options for S3 access.
        """
        return {
            "connection": self.connection,
            "default_block_size": self._block_size,
            "default_cache_type": self._cache_type,
            "max_workers": self._max_workers,
        }

    @property
    def _parquet_storage_options(self) -> Dict[str, Any]:
        """Get storage options for Polars Parquet reading via native object_store.

        Polars read_parquet uses Rust's native object_store crate, which requires
        AWS credentials to be passed directly rather than through fsspec.

        Returns:
            Dictionary with AWS credentials and region for S3 access.
        """
        credentials = self.connection.session.get_credentials()
        options: Dict[str, Any] = {}
        if credentials:
            frozen_credentials = credentials.get_frozen_credentials()
            options["aws_access_key_id"] = frozen_credentials.access_key
            options["aws_secret_access_key"] = frozen_credentials.secret_key
            if frozen_credentials.token:
                options["aws_session_token"] = frozen_credentials.token
        if self.connection.region_name:
            options["aws_region"] = self.connection.region_name
        return options

    @property
    def dtypes(self) -> Dict[str, Any]:
        """Get Polars-compatible data types for result columns."""
        description = self.description if self.description else []
        return {
            d[0]: dtype
            for d in description
            if (dtype := self._converter.get_dtype(d[1], d[4], d[5])) is not None
        }

    @property
    def converters(self) -> Dict[str, Callable[[Optional[str]], Optional[Any]]]:
        """Get converter functions for each column.

        Returns:
            Dictionary mapping column names to their converter functions.
        """
        description = self.description if self.description else []
        return {d[0]: self._converter.get(d[1]) for d in description}

    def _get_column_names(self) -> List[str]:
        """Get column names from description.

        Returns:
            List of column names.
        """
        description = self.description if self.description else []
        return [d[0] for d in description]

    def _create_dataframe_iterator(self) -> PolarsDataFrameIterator:
        """Create a DataFrame iterator for the result set.

        Returns:
            PolarsDataFrameIterator that handles both chunked and non-chunked cases.
        """
        if self._chunksize is not None:
            # Chunked mode: create lazy iterator
            reader: Union[Iterator["pl.DataFrame"], "pl.DataFrame"] = (
                self._iter_parquet_chunks() if self.is_unload else self._iter_csv_chunks()
            )
        else:
            # Non-chunked mode: load entire DataFrame
            reader = self._as_polars()

        return PolarsDataFrameIterator(reader, self.converters, self._get_column_names())

    def fetchone(
        self,
    ) -> Optional[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch the next row of the query result.

        Returns:
            A single row as a tuple, or None if no more rows are available.
        """
        try:
            row = next(self._iterrows)
        except StopIteration:
            return None
        else:
            self._rownumber = row[0] + 1
            return tuple([row[1][col] for col in self._column_names_cache])

    def fetchmany(
        self, size: Optional[int] = None
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch the next set of rows of the query result.

        Args:
            size: Number of rows to fetch. Defaults to arraysize.

        Returns:
            A list of rows as tuples.
        """
        if not size or size <= 0:
            size = self._arraysize
        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def fetchall(
        self,
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch all remaining rows of the query result.

        Returns:
            A list of all remaining rows as tuples.
        """
        rows = []
        while True:
            row = self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def _is_csv_readable(self) -> bool:
        """Check if CSV output is available and can be read.

        Returns:
            True if CSV data is available to read, False otherwise.

        Raises:
            ProgrammingError: If output location is not set.
        """
        if not self.output_location:
            raise ProgrammingError("OutputLocation is none or empty.")
        if not self.output_location.endswith((".csv", ".txt")):
            return False
        if self.substatement_type and self.substatement_type.upper() in (
            "UPDATE",
            "DELETE",
            "MERGE",
            "VACUUM_TABLE",
        ):
            return False
        length = self._get_content_length()
        return length != 0

    def _prepare_parquet_location(self) -> bool:
        """Prepare unload location for Parquet reading.

        Returns:
            True if Parquet data is available to read, False otherwise.
        """
        manifests = self._read_data_manifest()
        if not manifests:
            return False
        if not self._unload_location:
            self._unload_location = "/".join(manifests[0].split("/")[:-1]) + "/"
        return True

    def _read_csv(self) -> "pl.DataFrame":
        """Read query results from CSV file in S3.

        Returns:
            Polars DataFrame containing the CSV data.

        Raises:
            ProgrammingError: If output location is not set.
            OperationalError: If reading the CSV file fails.
        """
        import polars as pl

        if not self._is_csv_readable():
            return pl.DataFrame()

        if self.output_location is None:
            raise ProgrammingError("output_location is not available.")

        separator, has_header, new_columns = self._get_csv_params()

        try:
            df = pl.read_csv(
                self.output_location,
                separator=separator,
                has_header=has_header,
                schema_overrides=self.dtypes,
                storage_options=self._csv_storage_options,
                **self._kwargs,
            )
            if new_columns:
                df.columns = new_columns
            return df
        except Exception as e:
            _logger.exception(f"Failed to read {self.output_location}.")
            raise OperationalError(*e.args) from e

    def _read_parquet(self) -> "pl.DataFrame":
        """Read query results from Parquet files in S3.

        Returns:
            Polars DataFrame containing the Parquet data.

        Raises:
            OperationalError: If reading the Parquet files fails.
        """
        import polars as pl

        if not self._prepare_parquet_location():
            return pl.DataFrame()

        if self._unload_location is None:
            raise ProgrammingError("unload_location is not available.")

        try:
            return pl.read_parquet(
                self._unload_location,
                storage_options=self._parquet_storage_options,
                **self._kwargs,
            )
        except Exception as e:
            _logger.exception(f"Failed to read {self._unload_location}.")
            raise OperationalError(*e.args) from e

    def _read_parquet_schema(self) -> Tuple[Dict[str, Any], ...]:
        """Read schema from Parquet files for metadata."""
        import polars as pl

        if not self._unload_location:
            raise ProgrammingError("UnloadLocation is none or empty.")

        try:
            # Use scan_parquet to get schema without reading all data
            lazy_df = pl.scan_parquet(
                self._unload_location,
                storage_options=self._parquet_storage_options,
            )
            schema = lazy_df.collect_schema()
            return to_column_info(schema)
        except Exception as e:
            _logger.exception(f"Failed to read schema from {self._unload_location}.")
            raise OperationalError(*e.args) from e

    def _as_polars(self) -> "pl.DataFrame":
        """Load query results as a Polars DataFrame.

        Reads from Parquet for UNLOAD queries, otherwise from CSV.

        Returns:
            Polars DataFrame containing the query results.
        """
        if self.is_unload:
            df = self._read_parquet()
            if df.is_empty():
                self._metadata = ()
            else:
                self._metadata = self._read_parquet_schema()
        else:
            df = self._read_csv()
        return df

    def _as_polars_from_api(self) -> "pl.DataFrame":
        """Build a Polars DataFrame from GetQueryResults API.

        Used as a fallback when ``output_location`` is not available
        (e.g. managed query result storage).
        """
        import polars as pl

        rows = self._fetch_all_rows()
        if not rows:
            return pl.DataFrame()
        description = self.description if self.description else []
        columns = [d[0] for d in description]
        return pl.DataFrame(self._rows_to_columnar(rows, columns))

    def as_polars(self) -> "pl.DataFrame":
        """Return query results as a Polars DataFrame.

        Returns the query results as a Polars DataFrame. This is the primary
        method for accessing results with PolarsCursor.

        Note:
            When chunksize is set, calling this method will collect all chunks
            into a single DataFrame, loading all data into memory. Use
            iter_chunks() for memory-efficient processing of large datasets.

        Returns:
            Polars DataFrame containing all query results.

        Example:
            >>> cursor = connection.cursor(PolarsCursor)
            >>> cursor.execute("SELECT * FROM my_table")
            >>> df = cursor.as_polars()
            >>> print(f"DataFrame has {df.height} rows")
            >>> filtered = df.filter(pl.col("value") > 100)
        """
        return self._df_iter.as_polars()

    def as_arrow(self) -> "Table":
        """Return query results as an Apache Arrow Table.

        Converts the Polars DataFrame to an Apache Arrow Table for
        interoperability with other Arrow-compatible tools and libraries.

        Returns:
            Apache Arrow Table containing all query results.

        Raises:
            ImportError: If pyarrow is not installed.

        Example:
            >>> cursor = connection.cursor(PolarsCursor)
            >>> cursor.execute("SELECT * FROM my_table")
            >>> table = cursor.as_arrow()
            >>> # Use with other Arrow-compatible libraries
        """
        try:
            return self._df_iter.as_polars().to_arrow()
        except ImportError as e:
            raise ImportError(
                "pyarrow is required for as_arrow(). Install it with: pip install pyarrow"
            ) from e

    def _get_csv_params(self) -> Tuple[str, bool, Optional[List[str]]]:
        """Get CSV parsing parameters based on file type.

        Returns:
            Tuple of (separator, has_header, new_columns).
        """
        if self.output_location and self.output_location.endswith(".txt"):
            separator = "\t"
            has_header = False
            new_columns: Optional[List[str]] = self._get_column_names()
        else:
            separator = ","
            has_header = True
            new_columns = None
        return separator, has_header, new_columns

    def _iter_csv_chunks(self) -> Iterator["pl.DataFrame"]:
        """Iterate over CSV data in chunks using lazy evaluation.

        Yields:
            Polars DataFrame for each chunk.

        Raises:
            ProgrammingError: If output location is not set.
            OperationalError: If reading the CSV file fails.
        """
        import polars as pl

        if not self._is_csv_readable():
            return

        if self.output_location is None:
            raise ProgrammingError("output_location is not available.")

        separator, has_header, new_columns = self._get_csv_params()

        try:
            # scan_csv uses Rust's native object_store (like scan_parquet),
            # not fsspec, so we use the same storage options as Parquet
            lazy_df = pl.scan_csv(
                self.output_location,
                separator=separator,
                has_header=has_header,
                schema_overrides=self.dtypes,
                storage_options=self._parquet_storage_options,
                **self._kwargs,
            )
            for batch in lazy_df.collect_batches(chunk_size=self._chunksize):
                if new_columns:
                    batch.columns = new_columns
                yield batch
        except Exception as e:
            _logger.exception(f"Failed to read {self.output_location}.")
            raise OperationalError(*e.args) from e

    def _iter_parquet_chunks(self) -> Iterator["pl.DataFrame"]:
        """Iterate over Parquet data in chunks using lazy evaluation.

        Yields:
            Polars DataFrame for each chunk.

        Raises:
            OperationalError: If reading the Parquet files fails.
        """
        import polars as pl

        if not self._prepare_parquet_location():
            return

        if self._unload_location is None:
            raise ProgrammingError("unload_location is not available.")

        try:
            lazy_df = pl.scan_parquet(
                self._unload_location,
                storage_options=self._parquet_storage_options,
                **self._kwargs,
            )
            for batch in lazy_df.collect_batches(chunk_size=self._chunksize):
                yield batch
        except Exception as e:
            _logger.exception(f"Failed to read {self._unload_location}.")
            raise OperationalError(*e.args) from e

    def iter_chunks(self) -> PolarsDataFrameIterator:
        """Iterate over result chunks as Polars DataFrames.

        This method provides an iterator interface for processing large result sets.
        When chunksize is specified, it yields DataFrames in chunks using lazy
        evaluation for memory-efficient processing. When chunksize is not specified,
        it yields the entire result as a single DataFrame.

        Returns:
            PolarsDataFrameIterator that yields Polars DataFrames for each chunk
            of rows, or the entire DataFrame if chunksize was not specified.

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
        return self._df_iter

    def close(self) -> None:
        """Close the result set and release resources."""
        import polars as pl

        super().close()
        self._df_iter = PolarsDataFrameIterator(pl.DataFrame(), {}, [])
        self._iterrows = iter([])
