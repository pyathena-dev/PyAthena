# Copyright 2017 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Result set that reads Athena query results into Polars DataFrames."""

from __future__ import annotations

import logging
from collections import abc
from collections.abc import Callable, Iterator
from multiprocessing import cpu_count
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)

from pyathena import OperationalError
from pyathena.converter import Converter, _text_value_converter
from pyathena.error import ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.polars.util import to_column_info
from pyathena.result_set import AthenaResultSet
from pyathena.util import RetryConfig, override

if TYPE_CHECKING:
    import polars as pl
    from pyarrow import Table

    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)


def _identity(x: Any) -> Any:
    """Identity function for use as default converter."""
    return x


class PolarsDataFrameIterator(abc.Iterator):  # type: ignore[type-arg]
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
        reader: Iterator[pl.DataFrame] | pl.DataFrame,
        converters: dict[str, Callable[[str | None], Any | None]],
        column_names: list[str],
    ) -> None:
        """Initialize the iterator.

        Args:
            reader: Either a DataFrame iterator (for chunked) or a single DataFrame.
            converters: Dictionary mapping column names to converter functions.
            column_names: List of column names in order.
        """
        import polars as pl

        if isinstance(reader, pl.DataFrame):
            self._reader: Iterator[pl.DataFrame] = iter([reader])
        else:
            self._reader = reader
        self._converters = converters
        self._column_names = column_names

    @override
    def __next__(self) -> pl.DataFrame:
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

    @override
    def __iter__(self) -> PolarsDataFrameIterator:
        """Return self as iterator."""
        return self

    def __enter__(self) -> PolarsDataFrameIterator:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Context manager exit."""
        self.close()

    def close(self) -> None:
        """Close the iterator and release resources."""
        from types import GeneratorType

        reader = self._reader
        self._reader = iter(())
        if isinstance(reader, GeneratorType):
            reader.close()

    def iterrows(self) -> Iterator[tuple[int, dict[str, Any]]]:
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

    def as_polars(self) -> pl.DataFrame:
        """Collect all chunks into a single DataFrame.

        Returns:
            Single Polars DataFrame containing all data.
        """
        import polars as pl

        dfs = cast(list["pl.DataFrame"], list(self))
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
        connection: Connection[Any],
        converter: Converter,
        query_execution: AthenaQueryExecution,
        arraysize: int,
        retry_config: RetryConfig,
        unload: bool = False,
        unload_location: str | None = None,
        block_size: int | None = None,
        cache_type: str | None = None,
        max_workers: int = (cpu_count() or 1) * 5,
        chunksize: int | None = None,
        result_set_type_hints: dict[str | int, str] | None = None,
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
            result_set_type_hints: Athena type signatures for complex-type columns,
                keyed by column name (case-insensitive) or zero-based column index.
            **kwargs: Additional arguments passed to Polars read functions.
                They replace the arguments the result set chooses, such as ``separator``,
                ``has_header``, ``schema_overrides``, and ``storage_options``. A given
                ``storage_options`` replaces PyAthena's S3 settings as a whole: non-chunked
                CSV results are read through fsspec, and chunked CSV and UNLOAD results
                through Polars' native object store.
        """
        super().__init__(
            connection=connection,
            converter=converter,
            query_execution=query_execution,
            arraysize=1,  # Fetch one row to retrieve metadata
            retry_config=retry_config,
            result_set_type_hints=result_set_type_hints,
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

        import polars as pl

        # The whole result when it was not read in chunks.
        # Note: _as_polars() and _create_dataframe_iterator() update _metadata for unload
        # queries, so the converters and column names must be read AFTER them.
        self._df: pl.DataFrame | None = None
        # Converters for the rows of self._df.
        self._df_converters: dict[str, Callable[[str | None], Any | None]] = {}
        if self.state == AthenaQueryExecution.STATE_SUCCEEDED and self.output_location:
            if self._chunksize is None:
                self._df = self._as_polars()
                self._df_converters = self.converters
            else:
                self._df_iter = self._create_dataframe_iterator()
        elif self.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self._df = self._as_polars_from_api()
            # GetQueryResults values are already converted, except json and time with
            # time zone values kept as text.
            self._df_converters = self._text_value_converters(self.converters)
        else:
            self._df = pl.DataFrame()
        if self._df is not None:
            # A clone keeps assignments to the DataFrame from as_polars()
            # out of the rows that the fetch methods return.
            self._df_iter = PolarsDataFrameIterator(
                self._df.clone(), self._df_converters, self._get_column_names()
            )

        # Cache column names for efficient access in fetchone()
        # Must be after _as_polars() and _create_dataframe_iterator(), which update
        # _metadata for unload
        self._column_names_cache: list[str] = self._get_column_names()
        self._iterrows = self._df_iter.iterrows()

    def _storage_options(self, default: Callable[[], dict[str, Any]]) -> Any:
        """Get the storage options for a Polars read function.

        Args:
            default: Returns the storage options that the result set chooses. It is called
                only when ``execute()`` was not given ``storage_options``, so replaced
                options do not fetch credentials.

        Returns:
            The ``storage_options`` given to ``execute()``, or else the default.
        """
        if "storage_options" in self._kwargs:
            return self._kwargs["storage_options"]
        return default()

    def _read_kwargs(
        self, storage_options: Callable[[], dict[str, Any]], **defaults: Any
    ) -> dict[str, Any]:
        """Combine the arguments of a Polars read function with the ones given to ``execute()``.

        Args:
            storage_options: Returns the storage options that the result set chooses;
                see ``_storage_options()``.
            **defaults: The other arguments that the result set chooses, such as
                ``separator``.

        Returns:
            The arguments for the read function. A value given to ``execute()`` replaces
            the one the result set chose, including the whole ``storage_options``.
        """
        return {
            **defaults,
            **self._kwargs,
            "storage_options": self._storage_options(storage_options),
        }

    @property
    def _csv_storage_options(self) -> dict[str, Any]:
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
            # Not cached by fsspec so that the connection and the dircache are
            # released with the result set.
            "skip_instance_cache": True,
        }

    @property
    def _parquet_storage_options(self) -> dict[str, Any]:
        """Get storage options for Polars Parquet reading via native object_store.

        Polars read_parquet uses Rust's native object_store crate, which requires
        AWS credentials to be passed directly rather than through fsspec.

        Returns:
            Dictionary with AWS credentials and region for S3 access.
        """
        credentials = self.connection.session.get_credentials()
        options: dict[str, Any] = {}
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
    def dtypes(self) -> dict[str, Any]:
        """Get Polars-compatible data types for result columns."""
        description = self.description if self.description else []
        return {
            d[0]: dtype
            for d in description
            if (dtype := self._converter.get_dtype(d[1], d[4], d[5])) is not None
        }

    @property
    def converters(self) -> dict[str, Callable[[str | None], Any | None]]:
        """Get converter functions for each column.

        Returns:
            Dictionary mapping column names to their converter functions.
        """
        description = self.description if self.description else []
        return {d[0]: self._converter.get(d[1]) for d in description}

    def _get_column_names(self) -> list[str]:
        """Get column names from description.

        Returns:
            List of column names.
        """
        description = self.description if self.description else []
        return [d[0] for d in description]

    def _create_dataframe_iterator(self) -> PolarsDataFrameIterator:
        """Create a DataFrame iterator that reads the result file in chunks.

        For unload queries, it replaces the metadata with the schema of the Parquet
        files, as ``_as_polars()`` does, before the chunks are read.

        Returns:
            PolarsDataFrameIterator that reads each chunk lazily.
        """
        reader: Iterator[pl.DataFrame]
        if not self.is_unload:
            reader = self._iter_csv_chunks()
        elif self._prepare_parquet_location():
            self._metadata = self._read_parquet_schema()
            reader = self._iter_parquet_chunks()
        else:
            self._metadata = ()
            reader = iter(())
        return PolarsDataFrameIterator(reader, self.converters, self._get_column_names())

    @override
    def fetchone(
        self,
    ) -> tuple[Any | None, ...] | dict[Any, Any | None] | None:
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

    def _read_csv(self) -> pl.DataFrame:
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
                **self._read_kwargs(
                    lambda: self._csv_storage_options,
                    separator=separator,
                    has_header=has_header,
                    schema_overrides=self.dtypes,
                ),
            )
            if new_columns:
                df.columns = new_columns
            return df
        except Exception as e:
            _logger.exception(f"Failed to read {self.output_location}.")
            raise OperationalError(*e.args) from e

    def _read_parquet(self) -> pl.DataFrame:
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
                **self._read_kwargs(lambda: self._parquet_storage_options),
            )
        except Exception as e:
            _logger.exception(f"Failed to read {self._unload_location}.")
            raise OperationalError(*e.args) from e

    def _read_parquet_schema(self) -> tuple[dict[str, Any], ...]:
        """Read schema from Parquet files for metadata."""
        import polars as pl

        if not self._unload_location:
            raise ProgrammingError("UnloadLocation is none or empty.")

        try:
            # Use scan_parquet to get schema without reading all data
            lazy_df = pl.scan_parquet(
                self._unload_location,
                storage_options=self._storage_options(lambda: self._parquet_storage_options),
            )
            schema = lazy_df.collect_schema()
            return to_column_info(schema)
        except Exception as e:
            _logger.exception(f"Failed to read schema from {self._unload_location}.")
            raise OperationalError(*e.args) from e

    def _as_polars(self) -> pl.DataFrame:
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

    def _as_polars_from_api(self, converter: Converter | None = None) -> pl.DataFrame:
        """Build a Polars DataFrame from GetQueryResults API.

        Used as a fallback when ``output_location`` is not available
        (e.g. managed query result storage).

        Args:
            converter: Type converter for result values. Defaults to
                ``DefaultTypeConverter`` with json and time with time zone values kept as
                text, as in the CSV result file. A Polars ``Time`` has no time zone.
        """
        import polars as pl

        rows = self._fetch_all_rows(converter or _text_value_converter())
        if not rows:
            return pl.DataFrame()
        description = self.description if self.description else []
        columns = [d[0] for d in description]
        return pl.DataFrame(self._rows_to_columnar(rows, columns))

    def as_polars(self) -> pl.DataFrame:
        """Return query results as a Polars DataFrame.

        Returns the query results as a Polars DataFrame. This is the primary
        method for accessing results with PolarsCursor.

        Note:
            When chunksize is set and the result file is read in chunks, calling this
            method will collect the chunks that the fetch methods and iter_chunks()
            have not yet read into a single DataFrame, loading them all into memory,
            and a later call returns an empty DataFrame. Use iter_chunks() for
            memory-efficient processing of large datasets.

        Returns:
            Polars DataFrame containing all query results. When the result is not
            read in chunks, it is the same DataFrame on every call.

        Example:
            >>> cursor = connection.cursor(PolarsCursor)
            >>> cursor.execute("SELECT * FROM my_table")
            >>> df = cursor.as_polars()
            >>> print(f"DataFrame has {df.height} rows")
            >>> filtered = df.filter(pl.col("value") > 100)
        """
        if self._df is not None:
            return self._df
        return self._df_iter.as_polars()

    def as_arrow(self) -> Table:
        """Return query results as an Apache Arrow Table.

        Converts the Polars DataFrame to an Apache Arrow Table for
        interoperability with other Arrow-compatible tools and libraries.

        Returns:
            Apache Arrow Table containing all query results. When the result file is
            read in chunks, it contains the chunks that have not yet been read, as
            with as_polars().

        Raises:
            ImportError: If pyarrow is not installed.

        Example:
            >>> cursor = connection.cursor(PolarsCursor)
            >>> cursor.execute("SELECT * FROM my_table")
            >>> table = cursor.as_arrow()
            >>> # Use with other Arrow-compatible libraries
        """
        try:
            return self.as_polars().to_arrow()
        except ImportError as e:
            raise ImportError(
                "pyarrow is required for as_arrow(). Install it with: pip install pyarrow"
            ) from e

    def _get_csv_params(self) -> tuple[str, bool, list[str] | None]:
        """Get CSV parsing parameters based on file type.

        Returns:
            Tuple of (separator, has_header, new_columns).
        """
        if self.output_location and self.output_location.endswith(".txt"):
            separator = "\t"
            has_header = False
            new_columns: list[str] | None = self._get_column_names()
        else:
            separator = ","
            has_header = True
            new_columns = None
        return separator, has_header, new_columns

    def _iter_csv_chunks(self) -> Iterator[pl.DataFrame]:
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
                **self._read_kwargs(
                    lambda: self._parquet_storage_options,
                    separator=separator,
                    has_header=has_header,
                    schema_overrides=self.dtypes,
                ),
            )
            for batch in lazy_df.collect_batches(chunk_size=self._chunksize):
                if new_columns:
                    batch.columns = new_columns
                yield batch
        except Exception as e:
            _logger.exception(f"Failed to read {self.output_location}.")
            raise OperationalError(*e.args) from e

    def _iter_parquet_chunks(self) -> Iterator[pl.DataFrame]:
        """Iterate over Parquet data in chunks using lazy evaluation.

        ``_prepare_parquet_location()`` must have found the unload location first.

        Yields:
            Polars DataFrame for each chunk.

        Raises:
            ProgrammingError: If the unload location is not set.
            OperationalError: If reading the Parquet files fails.
        """
        import polars as pl

        if self._unload_location is None:
            raise ProgrammingError("unload_location is not available.")

        try:
            lazy_df = pl.scan_parquet(
                self._unload_location,
                **self._read_kwargs(lambda: self._parquet_storage_options),
            )
            yield from lazy_df.collect_batches(chunk_size=self._chunksize)
        except Exception as e:
            _logger.exception(f"Failed to read {self._unload_location}.")
            raise OperationalError(*e.args) from e

    def iter_chunks(self) -> PolarsDataFrameIterator:
        """Iterate over result chunks as Polars DataFrames.

        This method provides an iterator interface for processing large result sets.
        When chunksize is specified, it yields DataFrames in chunks using lazy
        evaluation for memory-efficient processing. These chunks come from the same
        iterator as the fetch methods, so a chunk that one of them reads is not
        available to the other. When chunksize is not specified, or the result has
        no result file to read in chunks, each call returns a new iterator that
        yields the entire result as a single DataFrame, and the fetch methods keep
        their position.

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
        if self._df is not None:
            return PolarsDataFrameIterator(self._df, self._df_converters, self._get_column_names())
        return self._df_iter

    @override
    def close(self) -> None:
        """Close the result set and release resources."""
        import polars as pl

        super().close()
        self._df_iter.close()
        self._df = pl.DataFrame()
        self._df_iter = PolarsDataFrameIterator(self._df, {}, [])
        self._iterrows = iter([])
