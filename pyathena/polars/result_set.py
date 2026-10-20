# Copyright 2017 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Result set that reads Athena query results into Polars DataFrames."""

from __future__ import annotations

import csv
import importlib.util
import logging
import sys
import threading
from collections import abc
from collections.abc import Callable, Iterable, Iterator
from contextlib import AbstractContextManager, nullcontext
from functools import partial
from io import BytesIO, StringIO
from itertools import islice, repeat
from multiprocessing import cpu_count
from types import GeneratorType
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    cast,
)

import fsspec

from pyathena import OperationalError
from pyathena.arrow.reader import ParquetChunkReader
from pyathena.converter import Converter, _to_default
from pyathena.error import DataError, ProgrammingError
from pyathena.filesystem.s3 import S3FileSystem
from pyathena.model import AthenaQueryExecution
from pyathena.polars.converter import _to_datetimes
from pyathena.polars.util import to_column_info, to_microseconds
from pyathena.result_set import (
    _CLOSED_MESSAGE,
    _FETCH_FAILED_MESSAGE,
    AthenaResultSet,
    AthenaUnloadResultSetMetadataReader,
)
from pyathena.util import RetryConfig, override

if TYPE_CHECKING:
    import polars as pl
    from pyarrow import Table

    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)

_READ_FAILED_MESSAGE = "PolarsDataFrameIterator cannot be read after a failed read."


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

    # The number of rows whose values the row iterators convert at once.
    _ROW_BLOCK_SIZE: ClassVar[int] = 10_000

    def __init__(
        self,
        reader: Iterator[pl.DataFrame] | pl.DataFrame,
        converters: dict[str, Callable[[str | None], Any | None]],
        column_names: list[str],
        closed: threading.Event | None = None,
    ) -> None:
        """Initialize the iterator.

        Args:
            reader: Either a DataFrame iterator (for chunked) or a single DataFrame.
            converters: Dictionary mapping column names to converter functions.
            column_names: List of column names in order.
            closed: Set when the result set that created the iterator is closed,
                after which reads raise ``ProgrammingError``.
        """
        import polars as pl

        if isinstance(reader, pl.DataFrame):
            self._reader: Iterator[pl.DataFrame] = iter([reader])
        else:
            self._reader = reader
        self._converters = converters
        self._column_names = column_names
        self._closed = closed
        # Whether a read raised, after which the position of the next chunk is unknown.
        self._read_failed = False

    @override
    def __next__(self) -> pl.DataFrame:
        """Get the next DataFrame chunk.

        The iterator is closed at the end of the chunks and after an exception, and
        later reads after an exception raise ``ProgrammingError``.

        Returns:
            The next Polars DataFrame chunk.

        Raises:
            StopIteration: When no more chunks are available.
            ProgrammingError: If an earlier read failed, or the result set that
                created the iterator is closed.
        """
        if self._closed is not None and self._closed.is_set():
            raise ProgrammingError(_CLOSED_MESSAGE)
        if self._read_failed:
            raise ProgrammingError(_READ_FAILED_MESSAGE)
        try:
            return next(self._reader)
        except StopIteration:
            self.close()
            raise
        except BaseException:
            self._read_failed = True
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
        """Close the iterator and release resources; later reads get no chunks."""
        reader, self._reader = self._reader, iter(())
        if isinstance(reader, GeneratorType):
            reader.close()

    def iterrows(self) -> Iterator[tuple[int, dict[str, Any]]]:
        """Iterate over rows as (index, row_dict) tuples.

        Once the result set that created the iterator is closed, the rows of the
        chunk already read are still returned, and reading the next chunk raises
        ``ProgrammingError``.

        Yields:
            Tuple of (row_index, row_dict) for each row across all chunks.
        """
        for row_num, row in enumerate(self.itertuples()):
            yield (row_num, dict(zip(self._column_names, row, strict=True)))

    def itertuples(self) -> Iterator[tuple[Any, ...]]:
        """Iterate over rows as tuples of their converted values across all chunks.

        The tuples hold the values of the columns in the order of the column names
        that the iterator was created with, as ``iterrows()`` gives them. A name
        that a chunk does not have gives None to the name's converter. The next
        chunk is read when the rows of the current one run out. After an exception,
        such as a value that fails to convert, the iteration ends.

        Once the result set that created the iterator is closed, the rows of the
        chunk already read are still returned, and reading the next chunk raises
        ``ProgrammingError``.

        Yields:
            The rows of all remaining chunks.
        """
        # Resuming a block after an exception within a row would misalign its columns.
        for df in self:
            for block in self._iter_row_blocks(df):
                yield from block

    def _iter_row_blocks(self, df: pl.DataFrame) -> Iterator[Iterator[tuple[Any, ...]]]:
        """Iterate over the rows of a chunk in blocks of rows.

        A block takes the values of a column at once with ``Series.to_list()`` and
        applies the column's converter unless the column has none or has the default
        converter, which returns the values as they are. Blocks bound the values held
        at once for a large chunk.

        Args:
            df: The DataFrame chunk.

        Yields:
            An iterator of the rows of each block.

        Raises:
            DataError: While the rows are iterated, if a value fails to convert, such as
                a ``timestamp with time zone`` value with an unknown zone or a local
                time without one instant in its zone. The message names the column.
        """
        import polars as pl

        # Each column's name, its Series, or None for a name the chunk does not have,
        # and its converter, or None for the identity.
        columns: list[tuple[str, pl.Series | None, Callable[[Any], Any] | None]] = []
        for name in self._column_names:
            converter = self._converters.get(name)
            columns.append(
                (
                    name,
                    df.get_column(name, default=None),
                    None if converter is _to_default else converter,
                )
            )

        def take_values(block: pl.Series) -> Iterator[Any]:
            # A value that Python does not hold, such as a date in year 0, fails
            # to_list() for the whole block, so the values are taken one at a time
            # and the rows before it are still returned. Within a nested value,
            # Polars raises such a failure as a PanicException, a BaseException.
            for i in range(block.len()):
                yield block.slice(i, 1).to_list()[0]

        def convert(
            name: str, converter: Callable[[Any], Any], values: Iterable[Any]
        ) -> Iterator[Any]:
            # Unlike map(), a generator turns a StopIteration from the converter into a
            # RuntimeError instead of ending the rows early.
            for value in values:
                try:
                    converted = converter(value)
                except DataError as e:
                    raise DataError(f"Column {name!r}: {e}") from e
                yield converted

        size = self._ROW_BLOCK_SIZE
        for start in range(0, df.height, size):
            length = min(size, df.height - start)
            blocks: list[Iterable[Any]] = []
            for name, series, converter in columns:
                values: Iterable[Any]
                if series is None:
                    values = [None] * length
                else:
                    block = series.slice(start, length)
                    try:
                        values = block.to_list()
                    except (Exception, pl.exceptions.PanicException):
                        values = take_values(block)
                blocks.append(values if converter is None else convert(name, converter, values))
            # Without column names, each row is empty, as iter_rows() gave it.
            yield zip(*blocks, strict=True) if blocks else repeat((), length)

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


class AthenaPolarsResultSetMetadataReader(AthenaUnloadResultSetMetadataReader):
    """Reads the metadata of a Polars result set from the schema of its Parquet files.

    It owns the read options normalized for the result, and the storage options
    with which Polars' native object store reads the files. An UNLOAD result is
    described by the schema that ``polars.scan_parquet()`` collects from its
    files. ``AthenaPolarsResultSet`` reads the files with that schema
    (:attr:`parquet_schema`).
    """

    def __init__(
        self,
        connection: Connection[Any],
        query_execution: AthenaQueryExecution,
        retry_config: RetryConfig,
        unload: bool = False,
        unload_location: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the reader; it sends no request.

        Args:
            connection: The connection that ran the query.
            query_execution: The query execution whose result set to read.
            retry_config: The retry configuration for API calls.
            unload: Whether the cursor runs its queries as UNLOAD to Parquet.
            unload_location: The S3 location of the UNLOAD output.
            **kwargs: The arguments of the Polars read functions. ``schema=None`` is
                the same as no ``schema``.
        """
        super().__init__(
            connection,
            query_execution,
            retry_config,
            unload=unload,
            unload_location=unload_location,
        )
        # schema=None is Polars' default and the same as no schema, so that the result
        # set still reads, for example, the timestamps of UNLOAD at microseconds.
        self._read_kwargs = {k: v for k, v in kwargs.items() if k != "schema" or v is not None}
        # The schema of the Parquet files of an UNLOAD result, which read() reads; None
        # without data files.
        self._parquet_schema: pl.Schema | None = None

    @property
    def read_kwargs(self) -> dict[str, Any]:
        """The arguments of the Polars read functions, normalized for the result."""
        return self._read_kwargs

    @property
    def parquet_schema(self) -> pl.Schema | None:
        """The schema of the Parquet files that :meth:`read` read; None without data files.

        INT96 timestamps are nanosecond Datetimes in it.
        """
        return self._parquet_schema

    @staticmethod
    def resolve_storage_options(
        read_kwargs: dict[str, Any], default: Callable[[], dict[str, Any]]
    ) -> Any:
        """Get the storage options for a Polars read function.

        Args:
            read_kwargs: The read options of the result set (:attr:`read_kwargs`).
            default: Returns the storage options that the reader or the result set
                chooses. It is called only when ``execute()`` was not given
                ``storage_options``, so replaced options do not fetch credentials.

        Returns:
            The ``storage_options`` given to ``execute()``, or else the default.
        """
        if "storage_options" in read_kwargs:
            return read_kwargs["storage_options"]
        return default()

    @staticmethod
    def parquet_storage_options(connection: Connection[Any]) -> dict[str, Any]:
        """Get storage options for Polars reading via native object_store.

        Polars read_parquet, scan_parquet, and scan_csv use Rust's native object_store
        crate, which requires AWS credentials to be passed directly rather than through
        fsspec. The credentials are frozen when the options are created.

        Args:
            connection: The connection whose session and region the reads use.

        Returns:
            Dictionary with AWS credentials and region for S3 access.
        """
        credentials = connection.session.get_credentials()
        options: dict[str, Any] = {}
        if credentials:
            frozen_credentials = credentials.get_frozen_credentials()
            options["aws_access_key_id"] = frozen_credentials.access_key
            options["aws_secret_access_key"] = frozen_credentials.secret_key
            if frozen_credentials.token:
                options["aws_session_token"] = frozen_credentials.token
        if connection.region_name:
            options["aws_region"] = connection.region_name
        return options

    @override
    def _read_parquet_metadata(self) -> tuple[dict[str, Any], ...]:
        """Read the schema of the Parquet files of the UNLOAD result, and describe it.

        The schema is kept for the read of the result.

        Returns:
            The column metadata of the schema.

        Raises:
            OperationalError: If reading the schema fails.
        """
        import polars as pl

        location = cast(str, self._unload_location)
        try:
            # Use scan_parquet to get schema without reading all data
            lazy_df = pl.scan_parquet(
                location,
                storage_options=self.resolve_storage_options(
                    self._read_kwargs, lambda: self.parquet_storage_options(self._connection)
                ),
            )
            self._parquet_schema = lazy_df.collect_schema()
        except Exception as e:
            _logger.exception(f"Failed to read schema from {location}.")
            raise OperationalError(*e.args) from e
        return to_column_info(self._parquet_schema)


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

    _metadata_reader_class: ClassVar[type[AthenaPolarsResultSetMetadataReader]] = (
        AthenaPolarsResultSetMetadataReader
    )

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
            cache_type: The default fsspec cache type of the S3 filesystem that reads the results,
                such as ``"none"``, ``"bytes"``, ``"readahead"``, ``"blockcache"``, or
                ``"background"``; see ``S3FileSystem``. Without ``cache_type``, PyAthena's
                filesystem reads a Parquet file of a chunked UNLOAD result that is larger than
                32 MiB without a cache.
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
                through Polars' native object store. Column types for a CSV result are
                given as ``schema_overrides``; Polars' deprecated alias ``dtypes`` is not
                supported. ``schema=None`` is the same as no ``schema``.
        """
        # Set by close(), so that the chunk iterators of the result set raise.
        self._closed_event = threading.Event()
        super().__init__(
            connection=connection,
            converter=converter,
            query_execution=query_execution,
            # The fetch size can exceed the size of a GetQueryResults page, which the
            # base class validates.
            arraysize=1,
            retry_config=retry_config,
            # The metadata reader of the result set reads the metadata.
            _pre_fetch=False,
            result_set_type_hints=result_set_type_hints,
        )
        self._arraysize = arraysize
        self._block_size = block_size
        self._cache_type = cache_type
        self._max_workers = max_workers
        self._chunksize = chunksize
        reader = self._metadata_reader_class(
            connection,
            query_execution,
            retry_config,
            unload=unload,
            unload_location=unload_location,
            **kwargs,
        )
        self._metadata_reader = reader
        self._kwargs = reader.read_kwargs
        self._load_metadata()
        succeeded = self.state == AthenaQueryExecution.STATE_SUCCEEDED
        # The UNLOAD output that the reader found, for the read of the rows: the location,
        # the Parquet files from the data manifest, and their schema (None without
        # data files).
        self._unload_location = reader.unload_location
        self._data_manifest = reader.data_manifest
        self._parquet_schema = reader.parquet_schema

        import polars as pl

        # The whole result when it was not read in chunks.
        self._df: pl.DataFrame | None = None
        # Converters for the rows of self._df.
        self._df_converters: dict[str, Callable[[str | None], Any | None]] = {}
        if succeeded and self.output_location:
            if self._chunksize is None:
                self._df = self._as_polars()
                self._df_converters = self._get_converters(self._get_frame_column_names())
            else:
                self._df_iter = self._create_dataframe_iterator()
        elif succeeded:
            # Without a result file, as with managed query result storage, the rows from
            # GetQueryResults are read as a CSV result file, but not in chunks.
            self._df = self._read_csv()
            self._df_converters = self._get_converters(self._get_frame_column_names())
        else:
            self._df = pl.DataFrame()
        if self._df is not None:
            # A clone keeps assignments to the DataFrame from as_polars()
            # out of the rows that the fetch methods return.
            self._df_iter = PolarsDataFrameIterator(
                self._df.clone(),
                self._df_converters,
                self._get_frame_column_names(),
                self._closed_event,
            )
        self._row_iter: Iterator[tuple[Any, ...]] = self._df_iter.itertuples()

    def _read_kwargs(
        self, storage_options: Callable[[], dict[str, Any]], **defaults: Any
    ) -> dict[str, Any]:
        """Combine the arguments of a Polars read function with the ones given to ``execute()``.

        Args:
            storage_options: Returns the storage options that the result set chooses;
                the options given to ``execute()`` replace them (see
                ``AthenaPolarsResultSetMetadataReader.resolve_storage_options()``).
            **defaults: The other arguments that the result set chooses, such as
                ``separator``.

        Returns:
            The arguments for the read function. A value given to ``execute()`` replaces
            the one the result set chose, including the whole ``storage_options``.
        """
        return {
            **defaults,
            **self._kwargs,
            # The options that the result set keeps, so that a chunk read that starts
            # later does not need the metadata reader.
            "storage_options": AthenaPolarsResultSetMetadataReader.resolve_storage_options(
                self._kwargs, storage_options
            ),
        }

    @property
    def _csv_storage_options(self) -> dict[str, Any]:
        """Get storage options for reading a CSV result file via fsspec.

        ``_read_csv()`` opens the file with ``fsspec.open()``, which uses
        PyAthena's registered S3FileSystem, and passes the open file to Polars.

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
    def dtypes(self) -> dict[str, Any]:
        """Get Polars-compatible data types for result columns."""
        description = self.description if self.description else []
        return self._get_dtypes([d[0] for d in description])

    @property
    def converters(self) -> dict[str, Callable[[str | None], Any | None]]:
        """Get converter functions for each column.

        Returns:
            Dictionary mapping column names to their converter functions.
        """
        description = self.description if self.description else []
        return self._get_converters([d[0] for d in description])

    @property
    def _csv_dtypes(self) -> dict[str, Any]:
        """The Polars data types of the result columns, keyed by the header of a CSV file."""
        return self._get_dtypes(self._get_column_names())

    def _get_dtypes(self, column_names: list[str]) -> dict[str, Any]:
        """Get the Polars data types of the result columns.

        Args:
            column_names: The names of the result columns, in column order.

        Returns:
            The data types keyed by the given names.
        """
        description = self.description if self.description else []
        return {
            name: dtype
            for name, d in zip(column_names, description, strict=True)
            if (dtype := self._converter.get_dtype(d[1], d[4], d[5])) is not None
        }

    def _get_converters(
        self, column_names: list[str]
    ) -> dict[str, Callable[[str | None], Any | None]]:
        """Get the conversion functions of the result columns.

        Args:
            column_names: The names of the result columns, in column order.

        Returns:
            The conversion functions keyed by the given names.
        """
        description = self.description if self.description else []
        return {
            name: self._converter.get(d[1])
            for name, d in zip(column_names, description, strict=True)
        }

    def _get_csv_schema_overrides(self, has_header: bool) -> tuple[dict[str, Any], dict[str, Any]]:
        """Get the Polars data types for reading CSV data, with the timestamp columns as text.

        Athena writes up to 12 fractional digits, and Polars does not parse more than
        nine into a Datetime. A chunked read always reads the timestamp
        columns as text and converts them with ``_to_datetimes()``. A whole read does
        so when parsing them fails, and with ``ignore_errors`` given to ``execute()``,
        with which Polars reads the timestamps that it does not parse as null.

        Args:
            has_header: Whether the CSV data has a header. Without one, or with a
                ``with_column_names`` function given to ``execute()``, Polars does not
                name the columns by the header; ``schema_overrides`` given to
                ``execute()`` replaces these types; and with ``schema``, Polars ignores
                ``schema_overrides``. In these cases the result set does not read the
                timestamp columns as text and convert them.

        Returns:
            Tuple of (schema_overrides, timestamp_dtypes): the data types to read the
            columns with, and the Datetime dtypes of the timestamp columns read as text,
            both keyed by the header of a CSV file.
        """
        import polars as pl

        dtypes = self._csv_dtypes
        if (
            not has_header
            or self._kwargs.keys() & {"schema", "schema_overrides"}
            or self._kwargs.get("with_column_names") is not None
        ):
            return dtypes, {}
        timestamp_dtypes = {
            name: dtype
            for name, d in zip(self._get_column_names(), self.description or [], strict=True)
            if d[1] == "timestamp"
            and ((dtype := dtypes.get(name)) is pl.Datetime or isinstance(dtype, pl.Datetime))
        }
        return {**dtypes, **dict.fromkeys(timestamp_dtypes, pl.String)}, timestamp_dtypes

    def _get_column_names(self) -> list[str]:
        """Get the names of the result columns in a DataFrame.

        Columns with the same name are renamed as Polars renames them when it reads
        the header of a CSV file, such as ``x`` and ``x_duplicated_0``.

        Returns:
            List of column names.
        """
        import polars as pl

        description = self.description if self.description else []
        names = [d[0] for d in description]
        if len(set(names)) == len(names):
            return names
        header = StringIO()
        csv.writer(header, quoting=csv.QUOTE_ALL).writerow(names)
        return pl.read_csv(BytesIO(header.getvalue().encode()), n_rows=0).columns

    def _get_frame_column_names(self) -> list[str]:
        """Get the names of the result columns in the DataFrames that the result set reads.

        The ``new_columns`` given to ``execute()`` rename the columns of a CSV result
        file by position.

        Returns:
            List of column names.
        """
        names = self._get_column_names()
        new_columns = self._kwargs.get("new_columns")
        if not new_columns or (self.output_location and self.is_unload):
            return names
        return [*new_columns[: len(names)], *names[len(new_columns) :]]

    def _create_dataframe_iterator(self) -> PolarsDataFrameIterator:
        """Create a DataFrame iterator that reads the result file in chunks.

        The result set calls this method when it is created, in ``execute()`` or in the
        future that ``AsyncPolarsCursor.execute()`` returns. The requests that prepare
        the reads are made then, before the first fetch, and raise their errors there:
        the size check of a CSV result file here, and the data manifest and the Parquet
        schema of an UNLOAD result by the metadata reader before. The schema is passed
        to ``_iter_parquet_chunks()``. The chunks themselves are read lazily.

        Returns:
            PolarsDataFrameIterator that reads each chunk lazily.
        """
        reader: Iterator[pl.DataFrame]
        if not self.is_unload:
            reader = self._iter_csv_chunks()
        elif self._parquet_schema is not None:
            reader = self._iter_parquet_chunks(self._parquet_schema)
        else:
            reader = iter(())
        column_names = self._get_frame_column_names()
        return PolarsDataFrameIterator(
            reader, self._get_converters(column_names), column_names, self._closed_event
        )

    @override
    def fetchone(
        self,
    ) -> tuple[Any | None, ...] | dict[Any, Any | None] | None:
        # As is_closed does, inline on the per-row path.
        if self._connection is None:
            raise ProgrammingError(_CLOSED_MESSAGE)
        if self._fetch_failed:
            raise ProgrammingError(_FETCH_FAILED_MESSAGE)
        try:
            row = next(self._row_iter, None)
            if row is not None:
                self._count_rows(1)
        except BaseException:
            self._fetch_failed = True
            raise
        return row

    @override
    def _take_rows(self, size: int | None) -> list[tuple[Any | None, ...] | dict[Any, Any | None]]:
        """Take the next rows from the row iterator in bulk.

        The rows taken before an exception are counted in ``rownumber`` too.
        """
        rows: list[tuple[Any | None, ...] | dict[Any, Any | None]] = []
        try:
            # extend() keeps the rows that it appended before an exception, and
            # islice() takes no size above sys.maxsize.
            rows.extend(islice(self._row_iter, None if size is None else min(size, sys.maxsize)))
        finally:
            if rows:
                self._count_rows(len(rows))
        return rows

    def _is_csv_readable(self) -> bool:
        """Check if CSV output is available and can be read.

        Without an output location, the GetQueryResults rows are read as CSV.

        Returns:
            True if CSV data is available to read, False otherwise.
        """
        if self.output_location and not self.output_location.endswith((".csv", ".txt")):
            return False
        if self.substatement_type and self.substatement_type.upper() in (
            "UPDATE",
            "DELETE",
            "MERGE",
            "VACUUM_TABLE",
        ):
            return False
        return not self.output_location or self._get_content_length() != 0

    def _read_csv(self) -> pl.DataFrame:
        """Read query results from CSV file in S3, or the GetQueryResults rows as one without it.

        Returns:
            Polars DataFrame containing the CSV data.

        Raises:
            OperationalError: If reading the CSV data fails.
        """
        import polars as pl

        if not self._is_csv_readable():
            return pl.DataFrame()

        separator, has_header, new_columns = self._get_csv_params()
        kwargs = self._read_kwargs(
            lambda: self._csv_storage_options,
            separator=separator,
            has_header=has_header,
            schema_overrides=self._csv_dtypes,
        )
        if "schema_overrides" not in self._kwargs:
            # Renamed after reading, so that Polars matches the types to the header.
            kwargs.pop("new_columns", None)
        # Polars 2.0 reads a path with its native object store, which does not take
        # fsspec options, so the result file is opened through fsspec here and given
        # to Polars as a file, which Polars 1.x does not copy as it does bytes.
        storage_options = kwargs.pop("storage_options")
        if self.output_location:
            location = self.output_location
        else:
            source = self._fetch_all_rows_as_csv()
            if not source:
                return pl.DataFrame()
            location = "the GetQueryResults rows"

        try:
            # Each read calls open_csv() to open the file again and closes it after
            # reading; reading the same file again after seek(0) makes fsspec's cache
            # hold a second copy. It is created and called inside the try, so that
            # errors such as invalid storage options raise OperationalError.
            open_csv: Callable[[], AbstractContextManager[Any]] = (
                partial(fsspec.open, self.output_location, "rb", **(storage_options or {}))
                if self.output_location
                else partial(nullcontext, source)
            )
            schema_overrides, timestamp_dtypes = self._get_csv_schema_overrides(has_header)
            strict = not kwargs.get("ignore_errors", False)
            df: pl.DataFrame | None = None
            if strict or not timestamp_dtypes:
                try:
                    with open_csv() as data:
                        df = pl.read_csv(data, **kwargs)
                except pl.exceptions.ComputeError:
                    if not timestamp_dtypes:
                        raise
            if df is None:
                # The timestamp columns are read as text, again after parsing them
                # failed, or at once with ignore_errors, with which Polars reads the
                # timestamps that it does not parse as null instead of failing.
                kwargs["schema_overrides"] = schema_overrides
                with open_csv() as data:
                    texts = pl.read_csv(data, **kwargs)
                df = _to_datetimes(texts, timestamp_dtypes, strict=strict)
            if new_columns:
                df.columns = [*new_columns, *df.columns[len(new_columns) :]]
            return df
        except Exception as e:
            _logger.exception(f"Failed to read {location}.")
            raise OperationalError(*e.args) from e

    def _read_parquet(self, schema: pl.Schema) -> pl.DataFrame:
        """Read query results from Parquet files in S3.

        Timestamps are read at microseconds; see ``_int96_read_options()``. Polars 1.x
        cannot read them so: it reads them at nanoseconds, and they are cast to
        microseconds.

        The metadata reader must have found the unload location first.

        Args:
            schema: The schema of the Parquet files, which the metadata reader read.

        Returns:
            Polars DataFrame containing the Parquet data.

        Raises:
            OperationalError: If reading the Parquet files fails.
        """
        import polars as pl

        if self._unload_location is None:
            raise ProgrammingError("unload_location is not available.")

        options = self._int96_read_options(schema)
        dtypes = self._output_dtypes(schema, options)
        try:
            kwargs = self._read_kwargs(
                lambda: AthenaPolarsResultSetMetadataReader.parquet_storage_options(
                    self.connection
                ),
                **(options or {}),
            )
            df = self._cast(pl.read_parquet(self._unload_location, **kwargs), dtypes)
        except Exception as e:
            _logger.exception(f"Failed to read {self._unload_location}.")
            raise OperationalError(*e.args) from e
        if options is None and dtypes.keys() & set(df.columns):
            self._warn_nanosecond_read()
        return df

    def _int96_read_options(self, schema: pl.Schema) -> dict[str, Any] | None:
        """Get the Polars read arguments that read the INT96 timestamps of UNLOAD at microseconds.

        Polars reads INT96 at nanoseconds by default, which gives wrong values outside
        1677-09-21 to 2262-04-11. Polars 2.0 and later read INT96 at the time unit of
        the read schema, except in a Map, which the read schema therefore makes a List
        of Structs; ``_output_dtypes()`` restores it. Polars 1.x reads INT96 at
        nanoseconds whatever the schema (pola-rs/polars#29184, fixed in Polars 2.0.0
        by pola-rs/polars#29208). Maps: pola-rs/polars#30031.

        Args:
            schema: The schema of the Parquet files.

        Returns:
            No arguments if the files hold no nanosecond timestamps or ``execute()``
            was given a ``schema``, the ``schema`` argument with Polars 2.0 and later,
            or None with Polars 1.x, which cannot read them.
        """
        import polars as pl

        if "schema" in self._kwargs or all(
            to_microseconds(dtype) == dtype for dtype in schema.values()
        ):
            return {}
        if int(pl.__version__.split(".")[0]) < 2:
            return None
        return {
            "schema": {name: to_microseconds(dtype, maps=False) for name, dtype in schema.items()}
        }

    @staticmethod
    def _output_dtypes(schema: pl.Schema, options: dict[str, Any] | None) -> dict[str, Any]:
        """Get the output dtypes of the columns that a read gives other dtypes.

        Polars 1.x reads timestamps at nanoseconds, which are cast to microseconds,
        and the read schema of ``_int96_read_options()`` makes Maps Lists of Structs,
        which are restored (pola-rs/polars#30031).

        Args:
            schema: The schema of the Parquet files.
            options: The read arguments from ``_int96_read_options()``.

        Returns:
            The output dtypes by column name.
        """
        if options is None:
            read_dtypes = dict(schema)
        elif "schema" in options:
            read_dtypes = options["schema"]
        else:
            return {}
        return {
            name: dtype
            for name, read_dtype in read_dtypes.items()
            if (dtype := to_microseconds(schema[name])) != read_dtype
        }

    @staticmethod
    def _cast(df: pl.DataFrame, dtypes: dict[str, Any]) -> pl.DataFrame:
        """Cast the columns of a DataFrame that are in ``dtypes`` to their dtypes.

        Args:
            df: The DataFrame.
            dtypes: The dtypes by column name, from ``_output_dtypes()``.

        Returns:
            The DataFrame with the cast columns.
        """
        # Series, not pl.col(), so that a name such as "*" is not a selector.
        casts = [df.get_column(name).cast(dtype) for name, dtype in dtypes.items() if name in df]
        return df.with_columns(casts) if casts else df

    @staticmethod
    def _warn_nanosecond_read() -> None:
        """Warn that Polars 1.x reads the timestamps of UNLOAD at nanoseconds.

        Polars 2.0.0 fixed this; see ``_int96_read_options()``.
        """
        import polars as pl

        _logger.warning(
            f"Polars {pl.__version__} reads the Parquet timestamps of UNLOAD at "
            "nanoseconds, so those outside 1677-09-21 to 2262-04-11 are wrong. Upgrade to "
            "Polars 2.0 or later, or read the result in chunks with pyarrow installed and "
            "no read options given to execute(), to read them correctly."
        )

    def _as_polars(self) -> pl.DataFrame:
        """Load query results as a Polars DataFrame.

        Reads from Parquet for UNLOAD queries, otherwise from CSV.

        Returns:
            Polars DataFrame containing the query results.
        """
        import polars as pl

        if not self.is_unload:
            return self._read_csv()
        if self._parquet_schema is None:
            return pl.DataFrame()
        return self._read_parquet(self._parquet_schema)

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

        Raises:
            ProgrammingError: If the result set is closed.

        Example:
            >>> cursor = connection.cursor(PolarsCursor)
            >>> cursor.execute("SELECT * FROM my_table")
            >>> df = cursor.as_polars()
            >>> print(f"DataFrame has {df.height} rows")
            >>> filtered = df.filter(pl.col("value") > 100)
        """
        self._ensure_open()
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
            ProgrammingError: If the result set is closed.

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
            Tuple of (separator, has_header, new_columns). ``new_columns`` are the
            names of the first columns, which the readers set after reading as Polars
            sets the ``new_columns`` given to ``execute()``. With ``schema_overrides``
            given to ``execute()``, which replace the result set's types, Polars
            renames the columns of a CSV file itself.
        """
        if self.output_location and self.output_location.endswith(".txt"):
            separator = "\t"
            has_header = False
            new_columns: list[str] | None = self._get_frame_column_names()
        else:
            separator = ","
            has_header = True
            new_columns = (
                None if "schema_overrides" in self._kwargs else self._kwargs.get("new_columns")
            )
        return separator, has_header, new_columns

    def _iter_csv_chunks(self) -> Iterator[pl.DataFrame]:
        """Create an iterator that reads the CSV result file in chunks.

        Returns:
            An iterator of a Polars DataFrame for each chunk, read lazily.

        Raises:
            ProgrammingError: If output location is not set.
            OperationalError: If checking the size of the CSV file fails.
        """
        if not self._is_csv_readable():
            return iter(())

        if self.output_location is None:
            raise ProgrammingError("output_location is not available.")

        separator, has_header, new_columns = self._get_csv_params()
        schema_overrides, timestamp_dtypes = self._get_csv_schema_overrides(has_header)
        return self._scan_csv_chunks(
            self.output_location,
            self.connection,
            {
                "separator": separator,
                "has_header": has_header,
                "schema_overrides": schema_overrides,
            },
            timestamp_dtypes,
            new_columns,
        )

    def _scan_csv_chunks(
        self,
        location: str,
        connection: Connection[Any],
        defaults: dict[str, Any],
        timestamp_dtypes: dict[str, Any],
        new_columns: list[str] | None,
    ) -> Iterator[pl.DataFrame]:
        """Scan a CSV result file in chunks with Polars.

        Args:
            location: The output location.
            connection: The connection whose credentials the read uses. They are
                frozen when the first chunk is read.
            defaults: The arguments of ``polars.scan_csv()`` that the result set
                chooses; see ``_read_kwargs()``.
            timestamp_dtypes: The Datetime dtypes of the timestamp columns read as
                text; see ``_get_csv_schema_overrides()``.
            new_columns: The names of the first columns, set after reading; see
                ``_get_csv_params()``.

        Yields:
            Polars DataFrame for each chunk.

        Raises:
            OperationalError: If reading the CSV file fails.
        """
        import polars as pl

        try:
            # scan_csv uses Rust's native object_store (like scan_parquet),
            # not fsspec, so we use the same storage options as Parquet
            read_kwargs = self._read_kwargs(
                lambda: AthenaPolarsResultSetMetadataReader.parquet_storage_options(connection),
                **defaults,
            )
            if "schema_overrides" not in self._kwargs:
                # Renamed after reading, so that Polars matches the types to the header.
                read_kwargs.pop("new_columns", None)
            lazy_df = _to_datetimes(
                pl.scan_csv(location, **read_kwargs),
                timestamp_dtypes,
                # As Polars reads a value it cannot parse as null with ignore_errors.
                strict=not read_kwargs.get("ignore_errors", False),
            )
            for batch in lazy_df.collect_batches(chunk_size=self._chunksize):
                if new_columns:
                    batch.columns = [*new_columns, *batch.columns[len(new_columns) :]]
                yield batch
        except Exception as e:
            _logger.exception(f"Failed to read {location}.")
            raise OperationalError(*e.args) from e

    def _iter_parquet_chunks(self, schema: pl.Schema) -> Iterator[pl.DataFrame]:
        """Create an iterator that reads the Parquet files in chunks.

        Without read options given to ``execute()``, and with pyarrow installed, the
        files of the data manifest are read one at a time through PyAthena's
        filesystem with ``ParquetChunkReader``, so that memory use depends on the
        chunk size and on the row group being read, not on the size of the result.
        Otherwise Polars scans the unload location with the given options, decoding
        whole row groups ahead of the chunks it returns. Both read timestamps at
        microseconds, except that Polars 1.x scans them at nanoseconds and casts them,
        as ``_read_parquet()`` does.

        The metadata reader must have found the unload location first.

        Args:
            schema: The schema of the Parquet files, which the metadata reader read.

        Returns:
            An iterator of a Polars DataFrame for each chunk, read lazily.

        Raises:
            ProgrammingError: If the unload location is not set.
        """
        if self._unload_location is None:
            raise ProgrammingError("unload_location is not available.")

        if not self._kwargs and importlib.util.find_spec("pyarrow") is not None:
            return self._read_parquet_chunks(
                S3FileSystem(**self._csv_storage_options), self._data_manifest
            )
        return self._scan_parquet_chunks(self._unload_location, self.connection, schema)

    def _read_parquet_chunks(self, fs: S3FileSystem, paths: list[str]) -> Iterator[pl.DataFrame]:
        """Read the files of the data manifest in chunks with ``ParquetChunkReader``.

        Args:
            fs: The filesystem that reads the files.
            paths: The files of the data manifest.

        Yields:
            Polars DataFrame for each chunk.

        Raises:
            OperationalError: If reading the Parquet files fails.
        """
        import polars as pl

        try:
            with ParquetChunkReader(
                fs,
                paths,
                cast(int, self._chunksize),
                cache_type=self._cache_type,
            ) as reader:
                for table in reader:
                    yield cast("pl.DataFrame", pl.from_arrow(table, rechunk=False))
        except Exception as e:
            _logger.exception(f"Failed to read {self._unload_location}.")
            raise OperationalError(*e.args) from e

    def _scan_parquet_chunks(
        self,
        location: str,
        connection: Connection[Any],
        schema: pl.Schema,
    ) -> Iterator[pl.DataFrame]:
        """Scan the unload location in chunks with Polars and the read options.

        Args:
            location: The unload location.
            connection: The connection whose credentials the read uses. They are
                frozen when the first chunk is read.
            schema: The schema of the Parquet files.

        Yields:
            Polars DataFrame for each chunk.

        Raises:
            OperationalError: If reading the Parquet files fails.
        """
        import polars as pl

        try:
            options = self._int96_read_options(schema)
            dtypes = self._output_dtypes(schema, options)
            if options is None and dtypes:
                self._warn_nanosecond_read()
            lazy_df = pl.scan_parquet(
                location,
                **self._read_kwargs(
                    lambda: AthenaPolarsResultSetMetadataReader.parquet_storage_options(connection),
                    **(options or {}),
                ),
            )
            for batch in lazy_df.collect_batches(chunk_size=self._chunksize):
                yield self._cast(batch, dtypes)
        except Exception as e:
            _logger.exception(f"Failed to read {location}.")
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
            Its reads raise ``ProgrammingError`` once the result set is closed.

        Raises:
            ProgrammingError: If the result set is closed.

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
        self._ensure_open()
        if self._df is not None:
            return PolarsDataFrameIterator(
                self._df,
                self._df_converters,
                self._get_frame_column_names(),
                self._closed_event,
            )
        return self._df_iter

    @override
    def close(self) -> None:
        """Close the result set and release resources."""
        import polars as pl

        super().close()
        self._closed_event.set()
        self._df_iter.close()
        self._df = pl.DataFrame()
        self._df_iter = PolarsDataFrameIterator(self._df, {}, [])
        self._row_iter = iter(())
        self._data_manifest = []
        self._parquet_schema = None
