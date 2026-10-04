"""Result set that reads Athena query results into pandas DataFrames."""

from __future__ import annotations

import csv
import logging
from collections import abc
from collections.abc import Callable, Iterable, Iterator
from contextlib import ExitStack
from functools import partial
from io import BufferedReader, IOBase, StringIO, TextIOWrapper
from multiprocessing import cpu_count
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
)

from fsspec import open as filesystem_open

from pyathena import OperationalError
from pyathena.converter import Converter
from pyathena.error import ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.pandas.reader import _BINARY_NULL, BinaryCSVReader
from pyathena.result_set import AthenaResultSet
from pyathena.util import RetryConfig, override, parse_output_location

if TYPE_CHECKING:
    from pandas import DataFrame, Index, Series
    from pandas.io.parsers import TextFileReader

    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)


def _convert_binary_csv(converter: Callable[[str | None], Any], value: str) -> Any:
    return converter(None if value == _BINARY_NULL else value)


def _no_trunc_date(df: DataFrame) -> DataFrame:
    return df


def _read_csv_with_pyarrow(source: str | IOBase, read_csv_kwargs: dict[str, Any]) -> DataFrame:
    """Read a CSV result with pyarrow as ``pandas.read_csv(engine="pyarrow")`` does.

    pandas' PyArrow engine does not set ``newlines_in_values``, so pyarrow
    raises or returns wrong values when a quoted value containing a newline
    crosses one of its read blocks. This reads the file with that option and
    converts the table as pandas does for the options that
    ``AthenaPandasResultSet._reads_csv_with_pyarrow()`` accepts: NULL-typed
    columns become float64, integer columns without a ``dtype`` entry get
    NumPy integer types, the ``dtype`` mapping is applied before and after the
    ``parse_dates`` columns are parsed with ``pandas.to_datetime()``, and
    without ``future.infer_string``, strings become objects.

    Args:
        source: The result file.
        read_csv_kwargs: The pandas.read_csv() options built by
            ``AthenaPandasResultSet._get_csv_read_options()``.

    Returns:
        The result as a DataFrame.
    """
    import pandas as pd
    import pyarrow as pa
    from pyarrow import csv as pyarrow_csv

    header = read_csv_kwargs["header"]
    names = read_csv_kwargs["names"]
    null_values = list(read_csv_kwargs["na_values"])
    table = pyarrow_csv.read_csv(
        source,
        read_options=pyarrow_csv.ReadOptions(autogenerate_column_names=header is None),
        parse_options=pyarrow_csv.ParseOptions(
            delimiter=read_csv_kwargs["sep"],
            ignore_empty_lines=read_csv_kwargs["skip_blank_lines"],
            newlines_in_values=True,
        ),
        convert_options=pyarrow_csv.ConvertOptions(
            null_values=null_values, strings_can_be_null="" in null_values
        ),
    )
    schema = table.schema
    for index, type_ in enumerate(schema.types):
        if pa.types.is_null(type_):
            schema = schema.set(index, schema.field(index).with_type(pa.float64()))
    integer_dtypes = {
        pa.int8(): pd.Int8Dtype(),
        pa.int16(): pd.Int16Dtype(),
        pa.int32(): pd.Int32Dtype(),
        pa.int64(): pd.Int64Dtype(),
    }
    # Integers become nullable dtypes first so that a dtype entry converts them
    # without going through float64.
    df = table.cast(schema).to_pandas(types_mapper=integer_dtypes.get)
    if header is None:
        # pandas names the columns beyond the given names by their positions.
        df.columns = [str(index) for index in range(len(df.columns) - len(names))] + names
    dtype = dict(read_csv_kwargs["dtype"])
    for column in df.columns:
        # Integer columns without a dtype entry get NumPy integer types.
        if column not in dtype and df[column].dtype in integer_dtypes.values():
            dtype[column] = df[column].dtype.numpy_dtype
    dtype = {
        column: pd.api.types.pandas_dtype(value)
        for column, value in dtype.items()
        if column in df.columns
    }
    df = df.astype(dtype)
    if not pd.get_option("future.infer_string"):
        # Without the string dtype, pandas returns strings, and the string
        # categories of categorical columns, as objects.
        for index in range(len(df.columns)):
            values = df.iloc[:, index]
            if values.dtype == "str":
                df.isetitem(index, values.astype(object).fillna(None))
            elif isinstance(values.dtype, pd.CategoricalDtype) and (
                values.dtype.categories.dtype == "str"
            ):
                categories = values.dtype.categories.astype(object)
                df.isetitem(
                    index,
                    values.astype(pd.CategoricalDtype(categories, ordered=values.dtype.ordered)),
                )
    for column in read_csv_kwargs["parse_dates"]:
        if isinstance(column, int) and column not in df.columns:
            column = df.columns[column]
        if df[column].dtype.kind in "Mm":
            continue
        values = df[column].astype("string")
        try:
            df[column] = pd.to_datetime(values, utc=False)
        except (ValueError, TypeError):
            # pandas keeps the column as strings if it cannot parse it.
            df[column] = values.to_numpy(dtype=object, na_value=float("nan"))
    # pandas applies the dtype mapping again after parsing dates.
    df = df.astype(dtype)
    return df


class _JSONConverter:
    """A json converter for ``pandas.read_csv()`` that keeps NULL from making values floats.

    pandas infers a dtype from the values that a converter returns, so JSON numbers
    with NULL become float64. This converter returns ``NULL`` in place of None, which
    keeps the column object, and ``restore()`` puts None back after reading.
    """

    NULL: ClassVar[object] = object()

    __slots__ = ("_converter", "_has_null")

    def __init__(self, converter: Callable[[str | None], Any]) -> None:
        """Wrap a json conversion function.

        Args:
            converter: The conversion function, which returns None for NULL.
        """
        self._converter = converter
        self._has_null = False

    def __call__(self, value: str | None) -> Any:
        """Convert a CSV value.

        Args:
            value: The value as text.

        Returns:
            The converted value, or ``NULL`` in place of None.
        """
        converted = self._converter(value)
        if converted is None:
            self._has_null = True
            return self.NULL
        return converted

    def restore(self, df: DataFrame, name: Any) -> None:
        """Put None back in place of ``NULL`` in a DataFrame that ``read_csv()`` returned.

        Only does anything if this converter returned ``NULL`` since the last call,
        so call it for each DataFrame or chunk right after reading it.

        Args:
            df: The DataFrame or chunk.
            name: The name of the column that this converter converted, which can
                also be an index level.
        """
        if not self._has_null:
            return
        self._has_null = False

        import pandas as pd

        index = df.index
        if name in df.columns:
            df[name] = pd.Series(self._restore_values(df[name]), index=index, dtype=object)
        elif isinstance(index, pd.MultiIndex) and name in index.names:
            levels = [index.get_level_values(i) for i in range(index.nlevels)]
            i = index.names.index(name)
            levels[i] = pd.Index(self._restore_values(levels[i]), dtype=object, name=name)
            df.index = pd.MultiIndex.from_arrays(levels, names=index.names)
        elif index.name == name:
            df.index = pd.Index(self._restore_values(index), dtype=object, name=name)

    @classmethod
    def _restore_values(cls, values: Series | Index) -> list[Any]:
        return [None if v is cls.NULL else v for v in values.to_numpy()]


class PandasDataFrameIterator(abc.Iterator):  # type: ignore[type-arg]
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
        reader: TextFileReader | DataFrame,
        trunc_date: Callable[[DataFrame], DataFrame],
        csv_stream: IOBase | None = None,
    ) -> None:
        """Initialize the iterator.

        Args:
            reader: Either a TextFileReader (for chunked) or a single DataFrame.
            trunc_date: Function to apply to each chunk, such as date truncation.
            csv_stream: Optional CSV stream owned and closed by this iterator.
        """
        from pandas import DataFrame

        if isinstance(reader, DataFrame):
            self._reader = iter([reader])
        else:
            self._reader = reader
        self._trunc_date = trunc_date
        self._csv_stream = csv_stream

    @override
    def __next__(self) -> DataFrame:
        """Get the next DataFrame chunk.

        Returns:
            The next pandas DataFrame chunk with date truncation applied.

        Raises:
            StopIteration: When no more chunks are available.
        """
        try:
            df = next(self._reader)
            return self._trunc_date(df)
        except BaseException:
            self.close()
            raise

    @override
    def __iter__(self) -> PandasDataFrameIterator:
        """Return self as iterator."""
        return self

    def __enter__(self) -> PandasDataFrameIterator:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Context manager exit."""
        self.close()

    def close(self) -> None:
        """Close the iterator and release resources."""
        from pandas.io.parsers import TextFileReader

        reader = self._reader
        self._reader = iter(())
        try:
            if isinstance(reader, TextFileReader):
                reader.close()
        finally:
            if self._csv_stream is not None:
                self._csv_stream.close()

    def iterrows(self) -> Iterator[tuple[int, dict[str, Any]]]:
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

    def get_chunk(self, size: int | None = None) -> DataFrame:
        """Get a chunk of specified size.

        Args:
            size: Number of rows to retrieve. If None, returns entire chunk.

        Returns:
            DataFrame chunk, with date truncation applied as in iteration.
        """
        from pandas.io.parsers import TextFileReader

        try:
            if isinstance(self._reader, TextFileReader):
                return self._trunc_date(self._reader.get_chunk(size))
            return self._trunc_date(next(self._reader))
        except BaseException:
            self.close()
            raise

    def as_pandas(self) -> DataFrame:
        """Collect all remaining chunks into a single DataFrame.

        The chunks keep their index, so the result has the row numbers or the
        ``index_col`` values of the CSV file. Categorical columns and a categorical
        index stay categorical. Categories given in the dtype keep their order;
        when the chunks inferred different categories, they are inferred again from
        all chunks in sorted order. A whole-file read of a large file can order
        inferred categories differently, because pandas joins its internal parser
        blocks in the order they were read.

        Returns:
            Single pandas DataFrame containing all data.
        """
        import pandas as pd

        dfs: list[DataFrame] = list(self)
        if not dfs:
            return pd.DataFrame()
        if len(dfs) == 1:
            return dfs[0]
        df = pd.concat(dfs)
        # Each chunk infers its own categories, and concat turns categorical columns
        # and indexes whose categories differ into object or string ones.
        for column, dtype in dfs[0].dtypes.items():
            if isinstance(dtype, pd.CategoricalDtype) and not isinstance(
                df[column].dtype, pd.CategoricalDtype
            ):
                df[column] = df[column].astype(pd.CategoricalDtype(ordered=dtype.ordered))
        index_dtype = dfs[0].index.dtype
        if isinstance(index_dtype, pd.CategoricalDtype) and not isinstance(
            df.index.dtype, pd.CategoricalDtype
        ):
            df.index = df.index.astype(pd.CategoricalDtype(ordered=index_dtype.ordered))
        return df


class AthenaPandasResultSet(AthenaResultSet):
    """Result set that provides pandas DataFrame results with memory optimization.

    This result set handles CSV and Parquet result files from S3, converting them to
    pandas DataFrames with configurable chunking for memory-efficient processing.
    With ``auto_optimize_chunksize=True``, it chooses a chunk size based on file
    size, and it provides iterative processing capabilities for large datasets.

    Features:
        - Optional chunk size optimization based on file size
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
        >>> df = cursor.as_pandas()
        >>>
        >>> # Or iterate through chunks for memory efficiency
        >>> cursor = connection.cursor(PandasCursor, chunksize=50_000)
        >>> cursor.execute("SELECT * FROM large_table")
        >>> for chunk_df in cursor.iter_chunks():
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

    _INTEGER_TYPES: ClassVar[tuple[str, ...]] = ("tinyint", "smallint", "integer", "bigint")
    _PARSE_DATES: ClassVar[list[str]] = [
        "date",
        "time",
        "timestamp",
    ]
    # The pandas.read_csv() options given to execute() that _read_csv_with_pyarrow() reads.
    _PYARROW_READ_CSV_OPTIONS: ClassVar[frozenset[str]] = frozenset({"dtype", "parse_dates"})

    def __init__(
        self,
        connection: Connection[Any],
        converter: Converter,
        query_execution: AthenaQueryExecution,
        arraysize: int,
        retry_config: RetryConfig,
        keep_default_na: bool = False,
        na_values: Iterable[str] | None = ("",),
        quoting: int = 1,
        unload: bool = False,
        unload_location: str | None = None,
        engine: str = "auto",
        chunksize: int | None = None,
        block_size: int | None = None,
        cache_type: str | None = None,
        max_workers: int = (cpu_count() or 1) * 5,
        auto_optimize_chunksize: bool = False,
        result_set_type_hints: dict[str | int, str] | None = None,
        **kwargs,
    ) -> None:
        """Initialize AthenaPandasResultSet with pandas-specific configurations.

        Args:
            connection: Database connection instance.
            converter: Data type converter for Athena types to pandas types.
            query_execution: Query execution metadata from Athena.
            arraysize: Default number of rows that ``fetchmany()`` returns.
            retry_config: Retry configuration for the ``GetQueryResults`` calls and
                for the HeadObject and GetObject calls this result set makes. Result
                files are read through the connection's S3 filesystem, which uses the
                connection's retry configuration.
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
            result_set_type_hints: Athena type signatures for complex-type columns,
                keyed by column name (case-insensitive) or zero-based column index.
            **kwargs: Additional arguments passed to pandas.read_csv/read_parquet.
                A given ``storage_options``, even None, replaces PyAthena's S3 filesystem
                for reading the result files, and so does ``filesystem`` for UNLOAD results.
                The UNLOAD manifest is still read with the connection's S3 client, and the
                schema with PyAthena's filesystem.
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
        self._data_manifest: list[str] = []
        self._kwargs = kwargs
        self._fs = self._create_s3_file_system()
        self._csv_stream: IOBase | None = None
        # The converters that pandas.read_csv() applies, keyed by column name.
        self._csv_converters: dict[Any, Callable[[str | None], Any]] = {}

        # Cache time column names for efficient _trunc_date processing. _read_csv()
        # replaces them with the labels of the columns that it reads.
        description = self.description if self.description else []
        self._time_columns: list[Any] = [d[0] for d in description if d[1] == "time"]

        import pandas as pd

        # The whole result when it was not read in chunks.
        self._df: DataFrame | None = None
        if self.state == AthenaQueryExecution.STATE_SUCCEEDED and self.output_location:
            result = self._as_pandas()
            trunc_date = _no_trunc_date if self.is_unload else self._finish_csv_frame
            if isinstance(result, pd.DataFrame):
                self._df = trunc_date(result)
            else:
                self._df_iter = PandasDataFrameIterator(result, trunc_date, self._csv_stream)
        elif self.state == AthenaQueryExecution.STATE_SUCCEEDED:
            # GetQueryResults values are already converted and need no time truncation.
            self._df = self._as_pandas_from_api()
        else:
            self._df = pd.DataFrame()
        if self._df is not None:
            # A shallow copy keeps assignments to the DataFrame from as_pandas()
            # out of the rows that the fetch methods return. Mutable values in its
            # cells, such as lists from JSON columns, are still shared.
            self._df_iter = PandasDataFrameIterator(self._df.copy(deep=False), _no_trunc_date)
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
        self, file_size_bytes: int | None = None, chunksize: int | None = None
    ) -> str:
        """Determine the appropriate CSV engine based on configuration and compatibility.

        Args:
            file_size_bytes: Size of the CSV file in bytes. Only used for PyArrow
                compatibility checks (minimum file size threshold).
            chunksize: Chunksize parameter (overrides self._chunksize if provided).

        Returns:
            CSV engine name ('pyarrow', 'c', or 'python').
        """
        if self._engine == "python":
            return "python"

        # Use PyArrow only when explicitly requested and all compatibility
        # checks pass; otherwise fall through to the C engine default.
        if self._engine == "pyarrow":
            effective_chunksize = chunksize if chunksize is not None else self._chunksize
            is_compatible = (
                effective_chunksize is None
                and self._quoting == 1
                and not self.converters
                # The pyarrow engine does not rename columns with the same name, and
                # the column labels cannot be resolved for it (see
                # _get_csv_column_labels()).
                and not self._needs_csv_column_name_resolution(
                    [d[0] for d in self.description or []]
                )
                and (file_size_bytes is None or file_size_bytes >= self.PYARROW_MIN_FILE_SIZE_BYTES)
            )
            if is_compatible:
                try:
                    return self._get_available_engine(["pyarrow"])
                except ImportError:
                    pass

        return "c"

    def _get_available_engine(self, engine_candidates: list[str]) -> str:
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
                error_msgs += f"\n - {e!s}"

        available_engines = ", ".join(f"'{e}'" for e in engine_candidates)
        raise ImportError(
            f"Unable to find a usable engine; tried using: {available_engines}."
            f"Trying to import the above resulted in these errors:"
            f"{error_msgs}"
        )

    def _auto_determine_chunksize(self, file_size_bytes: int) -> int | None:
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

    def _create_s3_file_system(self):
        """Create PyAthena's ``S3FileSystem`` from the connection settings.

        Returns:
            The S3 filesystem for reading the query results.
        """
        from pyathena.filesystem.s3 import S3FileSystem

        # Not cached by fsspec so that the connection and the dircache are
        # released with the result set.
        return S3FileSystem(
            connection=self.connection,
            default_block_size=self._block_size,
            default_cache_type=self._cache_type,
            max_workers=self._max_workers,
            skip_instance_cache=True,
        )

    @property
    def dtypes(self) -> dict[str, type[Any]]:
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
    ) -> dict[Any | None, Callable[[str | None], Any | None]]:
        """The conversion functions for the result columns the converter maps, keyed by name."""
        description = self.description if self.description else []
        return {
            d[0]: self._converter.get(d[1]) for d in description if d[1] in self._converter.mappings
        }

    @property
    def parse_dates(self) -> list[Any | None]:
        """The names of the result columns with date, time, or timestamp types."""
        description = self.description if self.description else []
        return [d[0] for d in description if d[1] in self._PARSE_DATES]

    def _get_column_names(self) -> list[Any]:
        """Get the names of the result columns in the DataFrame.

        Columns with the same name are renamed as pandas renames them when it reads
        the header of a CSV file, such as ``x`` and ``x.1``.

        Returns:
            List of column names.
        """
        import pandas as pd

        description = self.description if self.description else []
        names = [d[0] for d in description]
        if len(set(names)) == len(names):
            return names
        return self._resolve_csv_column_names(names, {}, pd.read_csv)[0]

    def _finish_csv_frame(self, df: DataFrame) -> DataFrame:
        """Finish a DataFrame read from the CSV result file.

        Puts None back in the json columns and truncates the time columns.

        Args:
            df: The DataFrame or chunk that ``pandas.read_csv()`` returned.

        Returns:
            The same DataFrame.
        """
        for name, converter in self._csv_converters.items():
            if isinstance(converter, _JSONConverter):
                converter.restore(df, name)
        return self._trunc_date(df)

    def _get_csv_converter(self, type_: str) -> Callable[[str | None], Any]:
        """Get the converter that ``pandas.read_csv()`` applies to a column type.

        json columns get a ``_JSONConverter``, so that NULL does not make pandas infer
        a numeric dtype, and ``_finish_csv_frame()`` puts None back.

        Args:
            type_: The Athena type of the column.

        Returns:
            The conversion function.
        """
        converter = self._converter.get(type_)
        if type_ == "json":
            return _JSONConverter(converter)
        return converter

    def _trunc_date(self, df: DataFrame) -> DataFrame:
        if self._time_columns:
            # A NULL is None, as with the GetQueryResults fallback and the other types.
            truncated = df.loc[:, self._time_columns].apply(
                lambda r: r.dt.time.astype(object).where(r.notna(), None)
            )
            for time_col in self._time_columns:
                df.isetitem(df.columns.get_loc(time_col), truncated[time_col])
        return df

    @override
    def fetchone(
        self,
    ) -> tuple[Any | None, ...] | dict[Any, Any | None] | None:
        try:
            row = next(self._iterrows)
        except StopIteration:
            return None
        else:
            self._rownumber = row[0] + 1
            # By position, so that columns with the same name keep their own values.
            return tuple(row[1].values())

    def _read_csv(self) -> TextFileReader | DataFrame:
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
        read_csv_kwargs = self._get_csv_read_options(csv_engine, effective_chunksize)
        labels = self._get_csv_column_labels(csv_engine, read_csv_kwargs)
        if labels is not None:
            self._key_csv_columns_by_labels(read_csv_kwargs, labels)

        try:
            with ExitStack() as stack:
                source: str | IOBase = self.output_location
                binary_columns = self._configure_binary_csv_read(read_csv_kwargs, labels)
                if labels is not None:
                    # After _configure_binary_csv_read(), which checks for the header row.
                    self._read_csv_header_as_labels(read_csv_kwargs, csv_engine)
                self._csv_converters = read_csv_kwargs.get("converters") or {}
                if binary_columns:
                    # Given storage_options, even None, open the file through fsspec
                    # as pandas does.
                    storage_options = None
                    if "storage_options" in read_csv_kwargs:
                        storage_options = read_csv_kwargs.pop("storage_options") or {}
                    source = self._csv_stream = stack.enter_context(
                        self._open_binary_csv_stream(binary_columns, storage_options)
                    )
                elif "storage_options" not in read_csv_kwargs:
                    # With storage_options, pandas opens the file through fsspec.
                    source = self._csv_stream = stack.enter_context(
                        self._fs.open(self.output_location, mode="rb")
                    )
                if csv_engine == "pyarrow" and self._reads_csv_with_pyarrow():
                    result = _read_csv_with_pyarrow(source, read_csv_kwargs)
                else:
                    result = pd.read_csv(source, **read_csv_kwargs)
                if not isinstance(result, pd.DataFrame):
                    # The chunk iterator takes ownership of the stream.
                    stack.pop_all()

            # Log performance information for large files
            if length > self.LARGE_FILE_THRESHOLD_BYTES:
                mode = "chunked" if effective_chunksize else "full"
                chunksize = f" with chunksize={effective_chunksize}" if effective_chunksize else ""
                _logger.info(
                    f"Reading {length} bytes from S3 in {mode} mode "
                    f"using {csv_engine} engine{chunksize}"
                )

            return result

        except Exception as e:
            _logger.exception(f"Failed to read {self.output_location}.")
            raise OperationalError(*e.args) from e

    def _reads_csv_with_pyarrow(self) -> bool:
        """Whether ``_read_csv_with_pyarrow()`` reads the CSV result for the PyArrow engine.

        It reproduces ``pandas.read_csv(engine="pyarrow")`` for PyAthena's default NA
        values and for ``dtype`` as a mapping and ``parse_dates`` as a list given to
        ``execute()``. With other options, pandas reads the file.

        Returns:
            True if ``_read_csv_with_pyarrow()`` reads the result.
        """
        return (
            not self._keep_default_na
            and isinstance(self._na_values, (list, tuple))
            and list(self._na_values) == [""]
            and self._kwargs.keys() <= self._PYARROW_READ_CSV_OPTIONS
            and isinstance(self._kwargs.get("dtype", {}), dict)
            and isinstance(self._kwargs.get("parse_dates", []), list)
        )

    def _get_csv_read_options(self, csv_engine: str, chunksize: int | None) -> dict[str, Any]:
        """Build pandas options for Athena CSV or tab-separated results."""
        if self.output_location and self.output_location.endswith(".txt"):
            sep = "\t"
            header = None
            names = self._get_column_names()
        else:
            sep = ","
            header = 0
            names = None

        read_csv_kwargs: dict[str, Any] = {
            "sep": sep,
            "header": header,
            "names": names,
            "dtype": self.dtypes,
            "converters": {
                d[0]: self._get_csv_converter(d[1])
                for d in self.description or []
                if d[1] in self._converter.mappings
            },
            "parse_dates": self.parse_dates,
            "skip_blank_lines": False,
            "keep_default_na": self._keep_default_na,
            "na_values": self._na_values,
            "quoting": self._quoting,
            "chunksize": chunksize,
            "engine": csv_engine,
        }

        # Engine-specific compatibility adjustments
        if csv_engine == "pyarrow":
            # PyArrow doesn't support these pandas-specific options
            read_csv_kwargs.pop("quoting", None)
            read_csv_kwargs.pop("converters", None)

        read_csv_kwargs.update(self._kwargs)

        return read_csv_kwargs

    @staticmethod
    def _resolve_csv_column_names(
        column_names: list[Any],
        read_csv_kwargs: dict[str, Any],
        read_csv: Callable[..., DataFrame],
    ) -> tuple[list[Any], set[Any]]:
        """Resolve customized or duplicate column names with pandas' header parser."""
        header_buffer = StringIO()
        csv.writer(header_buffer, quoting=csv.QUOTE_ALL).writerow(column_names)
        header_options = {
            key: read_csv_kwargs[key]
            for key in (
                "sep",
                "delimiter",
                "names",
                "engine",
                "quoting",
                "quotechar",
                "doublequote",
                "escapechar",
                "skipinitialspace",
            )
            if key in read_csv_kwargs
        }
        column_names = read_csv(
            StringIO(header_buffer.getvalue()), header=0, nrows=0, **header_options
        ).columns.tolist()
        selected_names = set(column_names)
        if read_csv_kwargs.get("usecols") is not None:
            selected_names = set(
                read_csv(
                    StringIO(header_buffer.getvalue()),
                    header=0,
                    nrows=0,
                    usecols=read_csv_kwargs["usecols"],
                    **header_options,
                ).columns
            )
        return column_names, selected_names

    def _is_standard_csv_parsing(self, read_csv_kwargs: dict[str, Any]) -> bool:
        """Whether the options read a CSV result file as Athena writes it.

        Args:
            read_csv_kwargs: The options for ``pandas.read_csv()``.

        Returns:
            True if pandas reads the header and quoted fields of the file as written.
        """
        return not (
            not self.output_location
            or not self.output_location.endswith(".csv")
            or read_csv_kwargs.get("header") != 0
            or read_csv_kwargs.get("skiprows") is not None
            or read_csv_kwargs.get("dialect") is not None
            or read_csv_kwargs.get("quoting") == csv.QUOTE_NONE
            or read_csv_kwargs.get("quotechar", '"') != '"'
        )

    def _can_preserve_binary_csv_nulls(self, read_csv_kwargs: dict[str, Any]) -> bool:
        """Whether CSV settings support distinguishing binary NULL from empty values."""
        return (
            "varbinary" in self._converter.mappings
            and "converters" not in self._kwargs
            and self._is_standard_csv_parsing(read_csv_kwargs)
        )

    def _needs_csv_column_name_resolution(self, column_names: list[Any]) -> bool:
        """Whether pandas must resolve column names instead of using Athena metadata."""
        return (
            len(set(column_names)) != len(column_names)
            or not all(column_names)
            or bool(
                self._kwargs.keys()
                & {
                    "names",
                    "usecols",
                    "sep",
                    "delimiter",
                    "doublequote",
                    "escapechar",
                    "skipinitialspace",
                }
            )
        )

    def _get_csv_column_labels(
        self, csv_engine: str, read_csv_kwargs: dict[str, Any]
    ) -> list[Any] | None:
        """Get the labels that pandas gives the result columns when it reads the CSV file.

        Args:
            csv_engine: The CSV engine that reads the file.
            read_csv_kwargs: The options for ``pandas.read_csv()``.

        Returns:
            The label of each result column in the description order, with None for a
            column that ``usecols`` leaves out. None if the options do not read the file
            as Athena writes it, or if the labels do not match the result columns one
            to one, such as with fewer ``names``.
        """
        import pandas as pd

        # The pyarrow engine runs only when no labels need resolving (see
        # _get_csv_engine()), and does not support reading only the header.
        if csv_engine == "pyarrow" or not self._is_standard_csv_parsing(read_csv_kwargs):
            return None
        column_names = [d[0] for d in self.description or []]
        if not self._needs_csv_column_name_resolution(column_names):
            return column_names
        labels, selected_labels = self._resolve_csv_column_names(
            column_names, read_csv_kwargs, pd.read_csv
        )
        if len(labels) != len(column_names):
            return None
        return [label if label in selected_labels else None for label in labels]

    def _key_csv_columns_by_labels(
        self, read_csv_kwargs: dict[str, Any], labels: list[Any]
    ) -> None:
        """Key the column options of ``pandas.read_csv()`` by the labels of the columns.

        Columns with the same name keep their own converters and date parsing, and
        options that rename or select columns get the types of the columns they
        read. The ``dtype``, ``converters``, and ``parse_dates`` given to
        ``execute()`` are kept as they are.

        Args:
            read_csv_kwargs: The options for ``pandas.read_csv()``, updated in place.
            labels: The labels from ``_get_csv_column_labels()``.
        """
        description = self.description or []
        columns = [
            (label, d) for label, d in zip(labels, description, strict=True) if label is not None
        ]
        if "dtype" not in self._kwargs:
            read_csv_kwargs["dtype"] = {
                label: dtype
                for label, d in columns
                if (dtype := self._converter.get_dtype(d[1], d[4], d[5])) is not None
            }
        if "converters" not in self._kwargs:
            read_csv_kwargs["converters"] = {
                label: self._get_csv_converter(d[1])
                for label, d in columns
                if d[1] in self._converter.mappings
            }
        if "parse_dates" not in self._kwargs:
            read_csv_kwargs["parse_dates"] = [
                label for label, d in columns if d[1] in self._PARSE_DATES
            ]
        self._time_columns = [label for label, d in columns if d[1] == "time"]

    def _read_csv_header_as_labels(self, read_csv_kwargs: dict[str, Any], csv_engine: str) -> None:
        """Read the header of the CSV result file as the labels of columns with the same name.

        pandas gives a column that it renames, such as ``x.1``, the dtype of the first
        column with the name when it has none of its own. When PyAthena builds the
        dtypes, the columns are read under their labels, so that each keeps its own type.

        Args:
            read_csv_kwargs: The options for ``pandas.read_csv()``, updated in place.
            csv_engine: The CSV engine that reads the file.
        """
        import pandas as pd

        names = [d[0] for d in self.description or []]
        if (
            self._kwargs.keys() & {"dtype", "names"}
            or len(set(names)) == len(names)
            # pandas detects another delimiter, as with sep=None, from the header row.
            or (read_csv_kwargs.get("delimiter") or read_csv_kwargs.get("sep")) != ","
        ):
            return
        # pandas renames the names in a header row, and copies their dtypes, even with
        # names given, so the header row is skipped instead.
        read_csv_kwargs["names"] = self._resolve_csv_column_names(
            names, read_csv_kwargs, pd.read_csv
        )[0]
        read_csv_kwargs["header"] = None
        if csv_engine == "python":
            # The python engine skips lines, which a name with a newline spans.
            header = StringIO(newline="")
            csv.writer(header, quoting=csv.QUOTE_ALL, lineterminator="").writerow(names)
            read_csv_kwargs["skiprows"] = len(StringIO(header.getvalue(), newline="").readlines())
        else:
            # The C engine skips parsed rows.
            read_csv_kwargs["skiprows"] = 1

    def _configure_binary_csv_read(
        self, read_csv_kwargs: dict[str, Any], labels: list[Any] | None
    ) -> set[int]:
        """Wrap binary converters and return column positions needing NULL preservation.

        Args:
            read_csv_kwargs: The options for ``pandas.read_csv()``, whose converters
                are keyed by the column labels.
            labels: The labels from ``_get_csv_column_labels()``.

        Returns:
            The positions of the binary columns whose NULL fields the stream preserves.
        """
        if labels is None or not self._can_preserve_binary_csv_nulls(read_csv_kwargs):
            return set()

        description = self.description or []
        binary_columns = {
            i for i, d in enumerate(description) if d[1] == "varbinary" and labels[i] is not None
        }
        if binary_columns:
            converters = read_csv_kwargs["converters"]
            for index in binary_columns:
                label = labels[index]
                converters[label] = partial(_convert_binary_csv, converters[label])
        return binary_columns

    def _open_binary_csv_stream(
        self, binary_columns: set[int], storage_options: dict[str, Any] | None
    ) -> TextIOWrapper:
        """Open a stream that preserves binary NULL fields and original CSV newlines."""
        text_options: dict[str, Any] = {"mode": "rt", "encoding": "utf-8", "newline": ""}
        with ExitStack() as stack:
            if storage_options is None:
                source = stack.enter_context(self._fs.open(self.output_location, **text_options))
            else:
                source = stack.enter_context(
                    filesystem_open(self.output_location, **text_options, **storage_options)
                )
            reader = stack.enter_context(BinaryCSVReader(source, binary_columns))
            buffer = stack.enter_context(BufferedReader(reader))
            stream = TextIOWrapper(buffer, encoding="utf-8", newline="")
            stack.pop_all()
            return stream

    def _read_parquet(self, engine) -> DataFrame:
        import pandas as pd

        self._data_manifest = self._read_data_manifest()
        if not self._data_manifest:
            return pd.DataFrame()
        if not self._unload_location:
            self._unload_location = "/".join(self._data_manifest[0].split("/")[:-1]) + "/"

        if engine == "pyarrow":
            kwargs: dict[str, Any] = {"use_threads": True, **self._kwargs}
            # Given storage_options, even None, pandas opens the files itself,
            # as for CSV results.
            if "filesystem" not in kwargs and "storage_options" not in kwargs:
                kwargs["filesystem"] = self._fs
            if kwargs.get("filesystem") is None:
                unload_location = self._unload_location
            else:
                # pyarrow takes the path without the scheme with a filesystem.
                bucket, key = parse_output_location(self._unload_location)
                unload_location = f"{bucket}/{key}"
        else:
            raise ProgrammingError("Engine must be `pyarrow`.")

        try:
            return pd.read_parquet(unload_location, engine=self._engine, **kwargs)
        except Exception as e:
            _logger.exception(f"Failed to read {self.output_location}.")
            raise OperationalError(*e.args) from e

    def _read_parquet_schema(self, engine) -> tuple[dict[str, Any], ...]:
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

    def _as_pandas(self) -> TextFileReader | DataFrame:
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

    def _as_pandas_from_api(self, converter: Converter | None = None) -> DataFrame:
        """Build a DataFrame from GetQueryResults API.

        Used as a fallback when ``output_location`` is not available
        (e.g. managed query result storage).

        Args:
            converter: Type converter for result values. Defaults to
                ``DefaultTypeConverter`` if not specified.
        """
        import pandas as pd

        rows = self._fetch_all_rows(converter)
        if not rows:
            return pd.DataFrame()
        description = self.description if self.description else []
        # Positional, so that columns with the same name keep their own values.
        columns = [list(column) for column in zip(*rows, strict=True)]
        # Integer columns get the dtype that the CSV result file reads them with,
        # and json columns with NULL stay objects as there, so that NULL does not
        # make their values floats.
        data: dict[Any, Any] = {}
        for name, values, d in zip(self._get_column_names(), columns, description, strict=True):
            dtype = None
            if d[1] in self._INTEGER_TYPES:
                dtype = self._converter.get_dtype(d[1], d[4], d[5])
            elif d[1] == "json" and None in values:
                dtype = object
            data[name] = values if dtype is None else pd.array(values, dtype=dtype)
        return pd.DataFrame(data)

    def as_pandas(self) -> PandasDataFrameIterator | DataFrame:
        """Return the query results as a DataFrame or an iterator of DataFrame chunks.

        Returns:
            If ``chunksize`` is None, the DataFrame of the whole result, the same one
            on every call. When ``auto_optimize_chunksize`` chose a chunk size, one
            DataFrame that joins the chunks the result iterator has not yet yielded,
            which is the whole result only if neither the fetch methods nor
            ``iter_chunks()`` read from it before, and a later call returns an empty
            DataFrame. If ``chunksize`` is set, the iterator that ``iter_chunks()``
            returns.
        """
        if self._chunksize is None:
            if self._df is not None:
                return self._df
            return self._df_iter.as_pandas()
        return self.iter_chunks()

    def iter_chunks(self) -> PandasDataFrameIterator:
        """Iterate over result chunks as pandas DataFrames.

        This method provides an iterator interface for processing large result sets.
        When a CSV result is read in chunks, because chunksize is specified or
        ``auto_optimize_chunksize`` chose a chunk size, it yields DataFrames in chunks
        for memory-efficient processing. These chunks come from the same iterator as
        the fetch methods, so a chunk that one of them reads is not available to the
        other. Otherwise, each call returns a new iterator that yields the entire
        result as a single DataFrame, and the fetch methods keep their position.

        Returns:
            PandasDataFrameIterator that yields pandas DataFrames for each chunk
            of rows, or the entire DataFrame if the result was not read in chunks.

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
        if self._df is not None:
            return PandasDataFrameIterator(self._df, _no_trunc_date)
        return self._df_iter

    @override
    def close(self) -> None:
        import pandas as pd

        super().close()
        self._df_iter.close()
        self._df = pd.DataFrame()
        self._df_iter = PandasDataFrameIterator(self._df, _no_trunc_date)
        self._iterrows = enumerate([])
        self._data_manifest = []
