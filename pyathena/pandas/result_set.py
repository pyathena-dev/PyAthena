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
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

from pyathena import OperationalError
from pyathena.converter import Converter
from pyathena.error import ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet
from pyathena.util import RetryConfig, parse_output_location

if TYPE_CHECKING:
    from pandas import DataFrame
    from pandas.io.parsers import TextFileReader

    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


def _no_trunc_date(df: "DataFrame") -> "DataFrame":
    return df


class PandasDataFrameIterator(abc.Iterator):  # type: ignore
    """Iterator for chunked DataFrame results from Athena queries.

    This class wraps either a pandas TextFileReader (for chunked reading) or
    a single DataFrame, providing a unified iterator interface. It applies
    optional date truncation to each DataFrame chunk as it's yielded.

    The iterator is used by AthenaPandasResultSet to provide chunked access
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
        This class is primarily for internal use by AthenaPandasResultSet.
        Most users should access results through PandasCursor methods.
    """

    def __init__(
        self,
        reader: Union["TextFileReader", "DataFrame"],
        trunc_date: Callable[["DataFrame"], "DataFrame"],
    ) -> None:
        """Initialize the iterator.

        Args:
            reader: Either a TextFileReader (for chunked) or a single DataFrame.
            trunc_date: Function to apply date truncation to each chunk.
        """
        from pandas import DataFrame

        if isinstance(reader, DataFrame):
            self._reader = iter([reader])
        else:
            self._reader = reader
        self._trunc_date = trunc_date

    def __next__(self) -> "DataFrame":
        """Get the next DataFrame chunk.

        Returns:
            The next pandas DataFrame chunk with date truncation applied.

        Raises:
            StopIteration: When no more chunks are available.
        """
        try:
            df = next(self._reader)
            return self._trunc_date(df)
        except StopIteration:
            self.close()
            raise

    def __iter__(self) -> "PandasDataFrameIterator":
        """Return self as iterator."""
        return self

    def __enter__(self) -> "PandasDataFrameIterator":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Context manager exit."""
        self.close()

    def close(self) -> None:
        """Close the iterator and release resources."""
        from pandas.io.parsers import TextFileReader

        if isinstance(self._reader, TextFileReader):
            self._reader.close()

    def iterrows(self) -> Iterator[Tuple[int, Dict[str, Any]]]:
        """Iterate over rows as (index, row_dict) tuples.

        Row indices are continuous across all chunks, starting from 0.

        Yields:
            Tuple of (row_index, row_dict) for each row across all chunks.
        """
        row_num = 0
        for df in self:
            # Use itertuples for memory efficiency instead of to_dict("records")
            # which loads all rows into memory at once
            columns = df.columns.tolist()
            for row in df.itertuples(index=False):
                yield (row_num, dict(zip(columns, row, strict=True)))
                row_num += 1

    def get_chunk(self, size: Optional[int] = None) -> "DataFrame":
        """Get a chunk of specified size.

        Args:
            size: Number of rows to retrieve. If None, returns entire chunk.

        Returns:
            DataFrame chunk.
        """
        from pandas.io.parsers import TextFileReader

        if isinstance(self._reader, TextFileReader):
            return self._reader.get_chunk(size)
        return next(self._reader)

    def as_pandas(self) -> "DataFrame":
        """Collect all chunks into a single DataFrame.

        Returns:
            Single pandas DataFrame containing all data.
        """
        import pandas as pd

        dfs: List["DataFrame"] = list(self)
        if not dfs:
            return pd.DataFrame()
        if len(dfs) == 1:
            return dfs[0]
        return pd.concat(dfs, ignore_index=True)


class AthenaPandasResultSet(AthenaResultSet):
    """Result set that provides pandas DataFrame results with memory optimization.

    This result set handles CSV and Parquet result files from S3, converting them to
    pandas DataFrames with configurable chunking for memory-efficient processing.
    It automatically optimizes chunk sizes based on file size and provides iterative
    processing capabilities for large datasets.

    Features:
        - Automatic chunk size optimization based on file size
        - Support for both CSV and Parquet result formats
        - Memory-efficient iterative processing
        - Automatic date/time parsing for pandas compatibility
        - PyArrow integration for Parquet files

    Attributes:
        LARGE_FILE_THRESHOLD_BYTES: File size threshold for chunking (50MB).
        AUTO_CHUNK_SIZE_LARGE: Default chunk size for large files (100,000 rows).
        AUTO_CHUNK_SIZE_MEDIUM: Default chunk size for medium files (50,000 rows).

    Example:
        >>> # Used automatically by PandasCursor
        >>> cursor = connection.cursor(PandasCursor)
        >>> cursor.execute("SELECT * FROM large_table")
        >>>
        >>> # Get full DataFrame
        >>> df = cursor.fetchall()
        >>>
        >>> # Or iterate through chunks for memory efficiency
        >>> for chunk_df in cursor:
        ...     process_chunk(chunk_df)

    Note:
        This class is used internally by PandasCursor and typically not
        instantiated directly by users.
    """

    # File size thresholds and chunking configuration - Public for user customization
    PYARROW_MIN_FILE_SIZE_BYTES: int = 100
    LARGE_FILE_THRESHOLD_BYTES: int = 50 * 1024 * 1024  # 50MB
    ESTIMATED_BYTES_PER_ROW: int = 100
    AUTO_CHUNK_THRESHOLD_LARGE: int = 2_000_000
    AUTO_CHUNK_THRESHOLD_MEDIUM: int = 1_000_000
    AUTO_CHUNK_SIZE_LARGE: int = 100_000
    AUTO_CHUNK_SIZE_MEDIUM: int = 50_000

    _PARSE_DATES: List[str] = [
        "date",
        "time",
        "time with time zone",
        "timestamp",
        "timestamp with time zone",
    ]

    def __init__(
        self,
        connection: "Connection[Any]",
        converter: Converter,
        query_execution: AthenaQueryExecution,
        arraysize: int,
        retry_config: RetryConfig,
        keep_default_na: bool = False,
        na_values: Optional[Iterable[str]] = ("",),
        quoting: int = 1,
        unload: bool = False,
        unload_location: Optional[str] = None,
        engine: str = "auto",
        chunksize: Optional[int] = None,
        block_size: Optional[int] = None,
        cache_type: Optional[str] = None,
        max_workers: int = (cpu_count() or 1) * 5,
        auto_optimize_chunksize: bool = False,
        **kwargs,
    ) -> None:
        """Initialize AthenaPandasResultSet with pandas-specific configurations.

        Args:
            connection: Database connection instance.
            converter: Data type converter for Athena types to pandas types.
            query_execution: Query execution metadata from Athena.
            arraysize: Number of rows to fetch in each batch (not used for pandas processing).
            retry_config: Retry configuration for S3 operations.
            keep_default_na: pandas option for handling NA values.
            na_values: Additional values to recognize as NA.
            quoting: CSV quoting behavior.
            unload: Whether result uses UNLOAD statement (Parquet format).
            unload_location: S3 location for UNLOAD results.
            engine: Parsing engine ('auto', 'c', 'python', 'pyarrow').
            chunksize: Number of rows per chunk. If specified, takes precedence
                      over auto_optimize_chunksize.
            block_size: S3 read block size.
            cache_type: S3 caching strategy.
            max_workers: Maximum worker threads for parallel operations.
            auto_optimize_chunksize: Enable automatic chunksize determination
                                   for large files when chunksize is None.
            **kwargs: Additional arguments passed to pandas.read_csv/read_parquet.
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
        self._keep_default_na = keep_default_na
        self._na_values = na_values
        self._quoting = quoting
        self._unload = unload
        self._unload_location = unload_location
        self._engine = engine
        self._chunksize = chunksize
        self._block_size = block_size
        self._cache_type = cache_type
        self._max_workers = max_workers
        self._auto_optimize_chunksize = auto_optimize_chunksize
        self._data_manifest: List[str] = []
        self._kwargs = kwargs
        self._fs = self.__s3_file_system()

        # Cache time column names for efficient _trunc_date processing
        description = self.description if self.description else []
        self._time_columns: List[str] = [
            d[0] for d in description if d[1] in ("time", "time with time zone")
        ]

        if self.state == AthenaQueryExecution.STATE_SUCCEEDED and self.output_location:
            df = self._as_pandas()
            trunc_date = _no_trunc_date if self.is_unload else self._trunc_date
            self._df_iter = PandasDataFrameIterator(df, trunc_date)
        elif self.state == AthenaQueryExecution.STATE_SUCCEEDED:
            df = self._as_pandas_from_api()
            self._df_iter = PandasDataFrameIterator(df, self._trunc_date)
        else:
            import pandas as pd

            self._df_iter = PandasDataFrameIterator(pd.DataFrame(), _no_trunc_date)
        self._iterrows = self._df_iter.iterrows()

    def _get_parquet_engine(self) -> str:
        """Get the parquet engine to use, handling auto-detection.

        Returns:
            Name of the parquet engine to use ('pyarrow').

        Raises:
            ImportError: If pyarrow is not available.
        """
        if self._engine == "auto":
            return self._get_available_engine(["pyarrow"])
        return self._engine

    def _get_csv_engine(
        self, file_size_bytes: Optional[int] = None, chunksize: Optional[int] = None
    ) -> str:
        """Determine the appropriate CSV engine based on configuration and compatibility.

        Args:
            file_size_bytes: Size of the CSV file in bytes.
            chunksize: Chunksize parameter (overrides self._chunksize if provided).

        Returns:
            CSV engine name ('pyarrow', 'c', or 'python').
        """
        effective_chunksize = chunksize if chunksize is not None else self._chunksize

        if self._engine == "pyarrow":
            return self._get_pyarrow_engine(file_size_bytes, effective_chunksize)

        if self._engine in ("c", "python"):
            return self._engine

        # Auto-selection for "auto" or unknown engine values
        return self._get_optimal_csv_engine(file_size_bytes)

    def _get_pyarrow_engine(self, file_size_bytes: Optional[int], chunksize: Optional[int]) -> str:
        """Get PyArrow engine if compatible, otherwise return optimal engine."""
        # Check parameter compatibility
        if chunksize is not None or self._quoting != 1 or self.converters:
            return self._get_optimal_csv_engine(file_size_bytes)

        # Check file size compatibility
        if file_size_bytes is not None and file_size_bytes < self.PYARROW_MIN_FILE_SIZE_BYTES:
            return self._get_optimal_csv_engine(file_size_bytes)

        # Check availability
        try:
            return self._get_available_engine(["pyarrow"])
        except ImportError:
            return self._get_optimal_csv_engine(file_size_bytes)

    def _get_available_engine(self, engine_candidates: List[str]) -> str:
        """Get the first available engine from a list of candidates.

        Args:
            engine_candidates: List of engine names to try in order.

        Returns:
            First available engine name.

        Raises:
            ImportError: If no engines are available.
        """
        import importlib

        error_msgs = ""
        for engine in engine_candidates:
            try:
                module = importlib.import_module(engine)
                return module.__name__
            except ImportError as e:
                error_msgs += f"\n - {str(e)}"

        available_engines = ", ".join(f"'{e}'" for e in engine_candidates)
        raise ImportError(
            f"Unable to find a usable engine; tried using: {available_engines}."
            f"Trying to import the above resulted in these errors:"
            f"{error_msgs}"
        )

    def _get_optimal_csv_engine(self, file_size_bytes: Optional[int] = None) -> str:
        """Get the optimal CSV engine based on file size.

        Args:
            file_size_bytes: Size of the CSV file in bytes.

        Returns:
            'python' for large files (>50MB) to avoid C parser limits, otherwise 'c'.
        """
        if file_size_bytes and file_size_bytes > self.LARGE_FILE_THRESHOLD_BYTES:
            return "python"
        return "c"

    def _auto_determine_chunksize(self, file_size_bytes: int) -> Optional[int]:
        """Determine appropriate chunksize for large files based on file size.

        This method provides a simple file-size-based chunksize determination.
        Users can customize the thresholds and chunk sizes by modifying the class
        attributes (e.g., LARGE_FILE_THRESHOLD_BYTES, AUTO_CHUNK_SIZE_LARGE).

        Args:
            file_size_bytes: Size of the result file in bytes.

        Returns:
            Suggested chunksize or None if chunking is not needed.
        """
        if file_size_bytes <= self.LARGE_FILE_THRESHOLD_BYTES:
            return None

        # Simple file size-based estimation
        estimated_rows = file_size_bytes // self.ESTIMATED_BYTES_PER_ROW

        if estimated_rows > self.AUTO_CHUNK_THRESHOLD_LARGE:
            return self.AUTO_CHUNK_SIZE_LARGE
        if estimated_rows > self.AUTO_CHUNK_THRESHOLD_MEDIUM:
            return self.AUTO_CHUNK_SIZE_MEDIUM
        return None

    def __s3_file_system(self):
        from pyathena.filesystem.s3 import S3FileSystem

        return S3FileSystem(
            connection=self.connection,
            default_block_size=self._block_size,
            default_cache_type=self._cache_type,
            max_workers=self._max_workers,
        )

    @property
    def dtypes(self) -> Dict[str, Type[Any]]:
        """Get pandas-compatible data types for result columns.

        Returns:
            Dictionary mapping column names to their corresponding Python types
            based on the converter's type mapping.
        """
        description = self.description if self.description else []
        return {
            d[0]: dtype
            for d in description
            if (dtype := self._converter.get_dtype(d[1], d[4], d[5])) is not None
        }

    @property
    def converters(
        self,
    ) -> Dict[Optional[Any], Callable[[Optional[str]], Optional[Any]]]:
        description = self.description if self.description else []
        return {
            d[0]: self._converter.get(d[1]) for d in description if d[1] in self._converter.mappings
        }

    @property
    def parse_dates(self) -> List[Optional[Any]]:
        description = self.description if self.description else []
        return [d[0] for d in description if d[1] in self._PARSE_DATES]

    def _trunc_date(self, df: "DataFrame") -> "DataFrame":
        if self._time_columns:
            truncated = df.loc[:, self._time_columns].apply(lambda r: r.dt.time)
            for time_col in self._time_columns:
                df.isetitem(df.columns.get_loc(time_col), truncated[time_col])
        return df

    def fetchone(
        self,
    ) -> Optional[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        try:
            row = next(self._iterrows)
        except StopIteration:
            return None
        else:
            self._rownumber = row[0] + 1
            description = self.description if self.description else []
            return tuple([row[1][d[0]] for d in description])

    def fetchmany(
        self, size: Optional[int] = None
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
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
        rows = []
        while True:
            row = self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def _read_csv(self) -> Union["TextFileReader", "DataFrame"]:
        import pandas as pd

        if not self.output_location:
            raise ProgrammingError("OutputLocation is none or empty.")
        if not self.output_location.endswith((".csv", ".txt")):
            return pd.DataFrame()
        if self.substatement_type and self.substatement_type.upper() in (
            "UPDATE",
            "DELETE",
            "MERGE",
            "VACUUM_TABLE",
        ):
            return pd.DataFrame()
        length = self._get_content_length()
        if length == 0:
            return pd.DataFrame()

        if self.output_location.endswith(".txt"):
            sep = "\t"
            header = None
            description = self.description if self.description else []
            names = [d[0] for d in description]
        elif self.output_location.endswith(".csv"):
            sep = ","
            header = 0
            names = None
        else:
            return pd.DataFrame()

        # Chunksize determination with user preference priority
        effective_chunksize = self._chunksize

        # Only auto-optimize if user hasn't specified chunksize AND auto_optimize is enabled
        if effective_chunksize is None and self._auto_optimize_chunksize:
            effective_chunksize = self._auto_determine_chunksize(length)
            if effective_chunksize:
                _logger.debug(
                    f"Auto-determined chunksize: {effective_chunksize} "
                    f"for file size: {length} bytes"
                )

        csv_engine = self._get_csv_engine(length, effective_chunksize)
        read_csv_kwargs = {
            "sep": sep,
            "header": header,
            "names": names,
            "dtype": self.dtypes,
            "converters": self.converters,
            "parse_dates": self.parse_dates,
            "skip_blank_lines": False,
            "keep_default_na": self._keep_default_na,
            "na_values": self._na_values,
            "quoting": self._quoting,
            "storage_options": {
                "connection": self.connection,
                "default_block_size": self._block_size,
                "default_cache_type": self._cache_type,
                "max_workers": self._max_workers,
            },
            "chunksize": effective_chunksize,
            "engine": csv_engine,
        }

        # Engine-specific compatibility adjustments
        if csv_engine == "pyarrow":
            # PyArrow doesn't support these pandas-specific options
            read_csv_kwargs.pop("quoting", None)
            read_csv_kwargs.pop("converters", None)

        read_csv_kwargs.update(self._kwargs)

        try:
            result = pd.read_csv(self.output_location, **read_csv_kwargs)

            # Log performance information for large files
            if length > self.LARGE_FILE_THRESHOLD_BYTES:
                mode = "chunked" if effective_chunksize else "full"
                _logger.info(
                    f"Reading {length} bytes from S3 in {mode} mode using {csv_engine} engine"
                    + (f" with chunksize={effective_chunksize}" if effective_chunksize else "")
                )

            return result

        except Exception as e:
            _logger.exception(f"Failed to read {self.output_location}.")
            error_msg = str(e).lower()
            if any(
                phrase in error_msg
                for phrase in ["signed integer", "maximum", "overflow", "int32", "c parser"]
            ):
                # Enhanced error message with specific recommendations
                file_mb = (length or 0) // (1024 * 1024)
                detailed_msg = (
                    f"Large dataset processing error ({file_mb}MB file): {e}. "
                    "This is likely due to pandas C parser limitations. "
                    "Recommended solutions:\n"
                    "1. Set chunksize: cursor = connection.cursor(PandasCursor, chunksize=50000)\n"
                    "2. Enable auto-optimization: "
                    "cursor = connection.cursor(PandasCursor, auto_optimize_chunksize=True)\n"
                    "3. Use PyArrow engine: "
                    "cursor = connection.cursor(PandasCursor, engine='pyarrow')\n"
                    "4. Use Python engine: "
                    "cursor = connection.cursor(PandasCursor, engine='python')"
                )
                raise OperationalError(detailed_msg) from e
            raise OperationalError(*e.args) from e

    def _read_parquet(self, engine) -> "DataFrame":
        import pandas as pd

        self._data_manifest = self._read_data_manifest()
        if not self._data_manifest:
            return pd.DataFrame()
        if not self._unload_location:
            self._unload_location = "/".join(self._data_manifest[0].split("/")[:-1]) + "/"

        if engine == "pyarrow":
            unload_location = self._unload_location
            kwargs = {
                "use_threads": True,
            }
        else:
            raise ProgrammingError("Engine must be `pyarrow`.")
        kwargs.update(self._kwargs)

        try:
            return pd.read_parquet(
                unload_location,
                engine=self._engine,
                storage_options={
                    "connection": self.connection,
                    "default_block_size": self._block_size,
                    "default_cache_type": self._cache_type,
                    "max_workers": self._max_workers,
                },
                **kwargs,
            )
        except Exception as e:
            _logger.exception(f"Failed to read {self.output_location}.")
            raise OperationalError(*e.args) from e

    def _read_parquet_schema(self, engine) -> Tuple[Dict[str, Any], ...]:
        if engine == "pyarrow":
            from pyarrow import parquet

            from pyathena.arrow.util import to_column_info

            if not self._unload_location:
                raise ProgrammingError("UnloadLocation is none or empty.")
            bucket, key = parse_output_location(self._unload_location)
            try:
                dataset = parquet.ParquetDataset(f"{bucket}/{key}", filesystem=self._fs)
                return to_column_info(dataset.schema)
            except Exception as e:
                _logger.exception(f"Failed to read schema {bucket}/{key}.")
                raise OperationalError(*e.args) from e
        else:
            raise ProgrammingError("Engine must be `pyarrow`.")

    def _as_pandas(self) -> Union["TextFileReader", "DataFrame"]:
        if self.is_unload:
            engine = self._get_parquet_engine()
            df = self._read_parquet(engine)
            if df.empty:
                self._metadata = ()
            else:
                self._metadata = self._read_parquet_schema(engine)
        else:
            df = self._read_csv()
        return df

    def _as_pandas_from_api(self) -> "DataFrame":
        """Build a DataFrame from GetQueryResults API.

        Used as a fallback when ``output_location`` is not available
        (e.g. managed query result storage).
        """
        import pandas as pd

        rows = self._fetch_all_rows()
        if not rows:
            return pd.DataFrame()
        description = self.description if self.description else []
        columns = [d[0] for d in description]
        return pd.DataFrame(self._rows_to_columnar(rows, columns))

    def as_pandas(self) -> Union[PandasDataFrameIterator, "DataFrame"]:
        if self._chunksize is None:
            return next(self._df_iter)
        return self._df_iter

    def iter_chunks(self) -> PandasDataFrameIterator:
        """Iterate over result chunks as pandas DataFrames.

        This method provides an iterator interface for processing large result sets.
        When chunksize is specified, it yields DataFrames in chunks for memory-efficient
        processing. When chunksize is not specified, it yields the entire result as a
        single DataFrame.

        Returns:
            PandasDataFrameIterator that yields pandas DataFrames for each chunk
            of rows, or the entire DataFrame if chunksize was not specified.

        Example:
            >>> # With chunking for large datasets
            >>> cursor = connection.cursor(PandasCursor, chunksize=50000)
            >>> cursor.execute("SELECT * FROM large_table")
            >>> for chunk in cursor.iter_chunks():
            ...     process_chunk(chunk)  # Each chunk is a pandas DataFrame
            >>>
            >>> # Without chunking - yields entire result as single chunk
            >>> cursor = connection.cursor(PandasCursor)
            >>> cursor.execute("SELECT * FROM small_table")
            >>> for df in cursor.iter_chunks():
            ...     process(df)  # Single DataFrame with all data
        """
        return self._df_iter

    def close(self) -> None:
        import pandas as pd

        super().close()
        self._df_iter = PandasDataFrameIterator(pd.DataFrame(), _no_trunc_date)
        self._iterrows = enumerate([])
        self._data_manifest = []
