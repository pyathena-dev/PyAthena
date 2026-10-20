"""Result set that reads Athena query results into pandas DataFrames."""

from __future__ import annotations

import csv
import importlib
import logging
import sys
import threading
from collections import abc
from collections.abc import Callable, Iterable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from contextvars import copy_context
from datetime import datetime, time, timezone
from functools import partial
from io import BufferedReader, BytesIO, IOBase, StringIO, TextIOWrapper
from itertools import chain, islice
from multiprocessing import cpu_count
from types import GeneratorType
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    cast,
)

from fsspec import open as filesystem_open

from pyathena import OperationalError
from pyathena.arrow.reader import ParquetChunkReader
from pyathena.arrow.util import to_column_info
from pyathena.converter import Converter, _parse_time_zone, _to_datetime_with_tz, _to_time
from pyathena.error import DataError, ProgrammingError
from pyathena.filesystem.s3 import S3FileSystem
from pyathena.model import AthenaQueryExecution
from pyathena.pandas.reader import _BINARY_NULL, BinaryCSVReader
from pyathena.result_set import (
    _CLOSED_MESSAGE,
    _FETCH_FAILED_MESSAGE,
    AthenaResultSet,
    AthenaUnloadResultSetMetadataReader,
)
from pyathena.util import RetryConfig, override, parse_output_location

if TYPE_CHECKING:
    from pandas import DataFrame, Series
    from pandas.io.parsers import TextFileReader
    from pyarrow import Table
    from pyarrow.parquet import FileMetaData

    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)

_READ_FAILED_MESSAGE = "PandasDataFrameIterator cannot be read after a failed read."

# The Athena types whose CSV text AthenaPandasResultSet reads as text and converts after
# reading; see _TemporalConverter.
_CSV_TEMPORAL_TYPES = ("timestamp", "date", "timestamp with time zone", "time")
# A timestamp's text up to its sixth fractional digit, for years of four digits.
_TIMESTAMP_TEXT_LENGTH = len("2000-01-01 00:00:00.000000")


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

    Unlike pandas' PyArrow engine, columns with a string dtype are read as text,
    except in a header-less file, and a NumPy str dtype keeps missing values
    missing, as with pandas' C engine.

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
    string_columns = set()
    for column, value in read_csv_kwargs["dtype"].items():
        try:
            column_dtype = pd.api.types.pandas_dtype(value)
        except (TypeError, ValueError, NotImplementedError):
            # pandas validates only the entries of columns in the result.
            continue
        if isinstance(column_dtype, pd.StringDtype) or column_dtype.kind == "U":
            string_columns.add(column)
    # Columns with a string dtype keep their text instead of the type pyarrow infers,
    # so "007" stays "007" as with pandas' C engine. The fields of a header-less file
    # get their names only after reading, as pandas gives the names to the last
    # fields, so they keep the inferred types. pandas ignores dtype keys that are not
    # column names, such as positions.
    column_types = (
        {}
        if header is None
        else {column: pa.string() for column in string_columns if isinstance(column, str)}
    )
    table = pyarrow_csv.read_csv(
        source,
        read_options=pyarrow_csv.ReadOptions(autogenerate_column_names=header is None),
        parse_options=pyarrow_csv.ParseOptions(
            delimiter=read_csv_kwargs["sep"],
            ignore_empty_lines=read_csv_kwargs["skip_blank_lines"],
            newlines_in_values=True,
        ),
        convert_options=pyarrow_csv.ConvertOptions(
            column_types=column_types,
            null_values=null_values,
            strings_can_be_null="" in null_values,
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
    # astype() with a NumPy str dtype, which str means without future.infer_string,
    # turns missing values into strings such as "nan", so these columns are
    # converted after the dates instead.
    numpy_string_columns = {
        column
        for column, value in dtype.items()
        if not isinstance(value, pd.api.extensions.ExtensionDtype) and value.kind == "U"
    }
    dtype = {column: value for column, value in dtype.items() if column not in numpy_string_columns}
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
    for index, column in enumerate(df.columns):
        if column in numpy_string_columns:
            # Object strings with missing values kept, as pandas' C engine returns.
            values = df.iloc[:, index]
            strings = values.astype(str).astype(object)
            df.isetitem(index, strings.where(values.notna(), float("nan")))
    return df


def _replace_by_label(df: DataFrame, name: Any, convert: Callable[[Series], Series]) -> None:
    """Replace the values of a column or an index level of a DataFrame, given its label.

    Args:
        df: The DataFrame, updated in place.
        name: The label of a column, or the name of an index level or of the index. A
            partial label of MultiIndex columns is not replaced.
        convert: Returns the new values of the given values, with their index.
    """
    import pandas as pd

    index = df.index
    if name in df.columns:
        values = df[name]
        if isinstance(values, pd.Series):
            # Not a partial label of MultiIndex columns.
            df[name] = convert(values)
    elif isinstance(index, pd.MultiIndex) and name in index.names:
        levels = [index.get_level_values(i) for i in range(index.nlevels)]
        i = index.names.index(name)
        levels[i] = pd.Index(convert(pd.Series(levels[i])), name=name)
        df.index = pd.MultiIndex.from_arrays(levels, names=index.names)
    elif index.name == name:
        df.index = pd.Index(convert(pd.Series(index)), name=name)


class _JSONConverter:
    """A json converter for the text of a json column that ``pandas.read_csv()`` read.

    pandas applies a converter to each internal parser block and infers a dtype from
    the values that it returns in that block, so JSON numbers in a block without NULL
    would become numbers of that block's dtype while a block with NULL kept objects.
    pandas reads the json columns as text instead, and ``convert()`` converts each
    DataFrame or chunk as a whole after reading.
    """

    __slots__ = ("_converter",)

    def __init__(self, converter: Callable[[str | None], Any]) -> None:
        """Wrap a json conversion function.

        Args:
            converter: The conversion function, which returns None for NULL.
        """
        self._converter = converter

    @staticmethod
    def keep_text(value: str | None) -> str | None:
        """Return the text of a value, as the converter that ``pandas.read_csv()`` applies.

        Args:
            value: The text of the value, or None for a field that the python engine
                adds to a short row.

        Returns:
            The same value.
        """
        return value

    def convert(self, df: DataFrame, name: Any) -> None:
        """Convert the json text of a column in a DataFrame that ``read_csv()`` returned.

        Args:
            df: The DataFrame or chunk, updated in place.
            name: The label of the json column, which can also be an index level.
        """
        _replace_by_label(df, name, self._convert_values)

    def _convert_values(self, values: Series) -> Series:
        """Convert json text values.

        Args:
            values: The text values, with NaN for a value that pandas read as missing.
                Values that pandas already parsed, such as an index or dates, are kept.

        Returns:
            The converted values: objects with None if there is NULL, as a NULL would
            otherwise make JSON numbers floats, or else of the dtype that pandas infers.
        """
        import pandas as pd

        if values.dtype.kind in "mM":
            # pandas parsed the text as dates, as parse_dates given to execute() asks.
            return pd.Series(values.array, index=values.index)
        # Each value is one of three kinds:
        # - text, which pandas kept as read: the JSON text, converted here;
        # - a missing value: NULL, converted from an empty string, as pandas passes
        #   NULL to a converter;
        # - any other value, which pandas already parsed (an index label): kept.
        # A comprehension, rather than a function per value, keeps this loop fast.
        converted = pd.Series(
            [
                self._converter(v)
                if isinstance(v, str)
                else (self._converter("") if pd.isna(v) else v)
                for v in values.to_numpy()
            ],
            index=values.index,
            dtype=object,
        )
        return converted if converted.isna().any() else converted.infer_objects()


class _TemporalConverter:
    """A converter for the text of a temporal column that ``pandas.read_csv()`` read.

    ``timestamp``, ``date``, ``timestamp with time zone``, and ``time`` columns are
    read as text and converted as whole columns after reading, so that their dtypes
    depend only on the Athena type: ``datetime64[us]``, ``datetime64[us]``,
    ``datetime64[us, UTC]``, and ``object`` with ``time`` objects and None for NULL.
    Fractional digits beyond microseconds are truncated. A column that pandas or
    pyarrow already parsed, as when ``dtype=None`` given to ``execute()`` replaces
    PyAthena's dtype mapping, gets the same dtype.
    """

    __slots__ = ("_type",)

    def __init__(self, type_: str) -> None:
        """Create the converter of a column type.

        Args:
            type_: The Athena type of the column, one of ``_CSV_TEMPORAL_TYPES``.
        """
        self._type = type_

    @property
    def type(self) -> str:
        """The Athena type of the column."""
        return self._type

    def convert(self, df: DataFrame, name: Any) -> None:
        """Convert the text of a column in a DataFrame that ``read_csv()`` returned.

        Args:
            df: The DataFrame or chunk, updated in place.
            name: The label of the column, which can also be an index level.

        Raises:
            DataError: If a value fails to convert, such as a ``timestamp with time
                zone`` value with an unknown zone or a local time without one instant in
                its zone. The message names the column.
        """
        try:
            _replace_by_label(df, name, self._convert_values)
        except Exception as e:
            raise DataError(f"Column {name!r}: {e}") from e

    def _convert_values(self, values: Series) -> Series:
        """Convert the values of a column.

        Args:
            values: The text values, with missing values for NULL, or values that
                pandas already parsed as dates.

        Returns:
            The converted values, with the index of ``values``.
        """
        if self._type == "timestamp with time zone":
            return _to_utc_timestamps(values)
        if self._type == "time":
            return _to_times(values)
        return _to_timestamps(values, date=self._type == "date")


class _TimeZoneConverter:
    """A converter that ``pandas.read_csv()`` applies to each ``timestamp with time zone`` value.

    With ``parse_dates`` or ``dtype`` given to ``execute()``, or with a conversion
    function other than the default one, pandas applies the column's conversion
    function to each value as it reads, unless ``converters`` given to ``execute()``
    replaces the conversion functions. This names the column in the
    ``DataError`` of a value that fails to convert, as ``_TemporalConverter`` does.
    """

    __slots__ = ("_converter", "_label", "error")

    def __init__(self, converter: Callable[[str | None], Any], label: Any) -> None:
        """Wrap the conversion function of a column.

        Args:
            converter: The conversion function.
            label: The label of the column.
        """
        self._converter = converter
        self._label = label
        # The DataError that the converter raised, which a whole read raises as it is.
        self.error: DataError | None = None

    def __call__(self, value: str | None) -> Any:
        """Convert a value.

        Args:
            value: The text of the value.

        Returns:
            The converted value.

        Raises:
            DataError: If the value fails to convert, such as a value with an unknown
                zone or a local time without one instant in its zone. The message names
                the column.
        """
        try:
            return self._converter(value)
        except DataError as e:
            self.error = DataError(f"Column {self._label!r}: {e}")
            raise self.error from e


def _to_timestamps(values: Series, date: bool = False) -> Series:
    """Convert timestamp or date text to ``datetime64[us]``.

    Args:
        values: The text values, with missing values for NULL, or datetime values.
        date: Whether the values are dates, which have no time to truncate.

    Returns:
        The values as ``datetime64[us]``, with NaT for NULL.
    """
    import pandas as pd

    if values.dtype.kind == "M":
        return values.dt.as_unit("us")
    if isinstance(values.dtype, pd.StringDtype) and values.dtype.storage == "pyarrow":
        # pyarrow parses its strings without a Python object per value, as
        # pandas.to_datetime() would make.
        import pyarrow as pa
        import pyarrow.compute as pc

        array = pa.array(values.array)
        if date:
            parsed = pc.cast(pc.cast(array, pa.date32()), pa.timestamp("us"))
        else:
            # Truncate the fraction to microseconds, as Athena's years have four digits.
            text = pc.utf8_slice_codeunits(array, 0, _TIMESTAMP_TEXT_LENGTH)
            parsed = pc.cast(text, pa.timestamp("us"))
        return pd.Series(parsed.to_numpy(zero_copy_only=False), index=values.index)
    text = values.astype("string")
    if not date:
        text = text.str.slice(0, _TIMESTAMP_TEXT_LENGTH)
    return pd.to_datetime(text, format="ISO8601").dt.as_unit("us")


def _to_times(values: Series) -> Series:
    """Convert time text to ``time`` objects.

    pandas cannot infer the format of time text, which it would parse one value at a
    time with ``dateutil``.

    Args:
        values: The text values, with missing values for NULL, datetime values that
            pandas parsed, or ``time`` values that pyarrow parsed.

    Returns:
        The values as ``time`` objects, with None for NULL.
    """
    import pandas as pd

    if values.dtype.kind == "M":
        return values.dt.time.astype(object).where(values.notna(), None)
    return pd.Series(
        [
            _to_time(v) if isinstance(v, str) else v if isinstance(v, time) else None
            for v in values.to_numpy(dtype=object)
        ],
        index=values.index,
        dtype=object,
    )


def _to_utc_timestamps(values: Series) -> Series:
    """Convert ``timestamp with time zone`` text to ``datetime64[us, UTC]``.

    The local times of each zone are localized together. As for the fetch methods of
    the other cursors, a local time that the zone repeats or skips is not converted.

    Args:
        values: The text values, a local time followed by a zone name or a ``+HH:MM``
            or ``-HH:MM`` offset, with missing values for NULL, or aware datetime
            values.

    Returns:
        The instants as ``datetime64[us, UTC]``, with NaT for NULL.

    Raises:
        DataError: If a zone is unknown, or a local time does not identify one instant
            in its zone.
    """
    import numpy as np
    import pandas as pd

    if values.dtype.kind == "M":
        return values.dt.tz_convert("UTC").dt.as_unit("us")
    text = values if isinstance(values.dtype, pd.StringDtype) else values.astype("string")
    # The local time before the last space, and the zone after it. pandas runs these
    # regular expressions in pyarrow for pyarrow-backed strings, without an object
    # per value.
    local = _to_timestamps(text.str.replace(r" [^ ]*$", "", regex=True)).to_numpy(
        dtype="datetime64[us]"
    )
    # Integer codes of the zones, -1 for NULL, and the positions of each zone's values,
    # grouped once by a stable sort of the codes.
    codes, zone_names = pd.factorize(text.str.replace(r"^.* ", "", regex=True))
    order = np.argsort(codes, kind="stable")
    bounds = np.searchsorted(codes[order], np.arange(-1, len(zone_names) + 1))
    utc = np.full(len(values), np.datetime64("NaT", "us"))
    for code, zone_name in enumerate(zone_names):
        positions = order[bounds[code + 1] : bounds[code + 2]]
        zone = _parse_time_zone(zone_name)
        if isinstance(zone, timezone):
            utc[positions] = local[positions] - np.timedelta64(zone.utcoffset(None), "us")
            continue
        localized = pd.Series(local[positions]).dt.tz_localize(
            zone, ambiguous="NaT", nonexistent="NaT"
        )
        utc[positions] = (
            localized.dt.tz_convert("UTC").dt.tz_localize(None).to_numpy(dtype="datetime64[us]")
        )
        # pandas also reports local times before a zone's first transition as
        # nonexistent, so the values that it does not localize are converted one at a
        # time, which raises the DataError of a value without one instant.
        failed = positions[np.isnat(utc[positions])]
        if len(failed):
            offsets = [
                cast(datetime, _to_datetime_with_tz(value)).utcoffset()
                for value in text.iloc[failed]
            ]
            utc[failed] = local[failed] - np.array(offsets, dtype="timedelta64[us]")
    return pd.Series(utc, index=values.index).dt.tz_localize("UTC")


class PandasDataFrameIterator(abc.Iterator):  # type: ignore[type-arg]
    """Iterator for chunked DataFrame results from Athena queries.

    This class wraps either an iterator of DataFrame chunks, such as a pandas
    TextFileReader for CSV results or the chunks of an UNLOAD result, or a single
    DataFrame, providing a unified iterator interface. It applies
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

    # The number of rows whose values the row iterators convert at once.
    _ROW_BLOCK_SIZE: ClassVar[int] = 10_000

    def __init__(
        self,
        reader: TextFileReader | Iterator[DataFrame] | DataFrame,
        trunc_date: Callable[[DataFrame], DataFrame],
        csv_stream: IOBase | None = None,
        fetch_types: Mapping[Any, str] | None = None,
        closed: threading.Event | None = None,
    ) -> None:
        """Initialize the iterator.

        Args:
            reader: A TextFileReader or another iterator of DataFrame chunks, or a
                single DataFrame.
            trunc_date: Function to apply to each chunk, such as date truncation.
            csv_stream: Optional CSV stream owned and closed by this iterator.
            fetch_types: The labels of the datetime columns whose values the row
                iterators return as ``"datetime"`` or ``"date"`` objects, with None
                for NaT, instead of pandas ``Timestamp`` values.
            closed: Set when the result set that created the iterator is closed,
                after which reads raise ``ProgrammingError``.
        """
        from pandas import DataFrame

        if isinstance(reader, DataFrame):
            self._reader = iter([reader])
        else:
            self._reader = reader
        self._trunc_date = trunc_date
        self._csv_stream = csv_stream
        self._fetch_types: dict[Any, str] = dict(fetch_types or {})
        self._closed = closed
        # Whether a read raised, after which the position of the next chunk is unknown.
        self._read_failed = False

    @override
    def __next__(self) -> DataFrame:
        """Get the next DataFrame chunk.

        Returns:
            The next pandas DataFrame chunk with date truncation applied.

        Raises:
            StopIteration: When no more chunks are available.
            ProgrammingError: If an earlier read failed, or the result set that
                created the iterator is closed.
        """
        return self._read_chunk(next)

    def _read_chunk(self, read: Callable[[Any], DataFrame]) -> DataFrame:
        """Read the next chunk with date truncation applied.

        The iterator is closed at the end of the chunks and after an exception, and
        later reads after an exception raise ``ProgrammingError``.

        Args:
            read: The function that reads the chunk from the reader.

        Returns:
            The chunk.

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
            return self._trunc_date(read(self._reader))
        except StopIteration:
            self.close()
            raise
        except BaseException:
            self._read_failed = True
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
        """Close the iterator and release resources; later reads get no chunks."""
        from pandas.io.parsers import TextFileReader

        reader, self._reader = self._reader, iter(())
        csv_stream, self._csv_stream = self._csv_stream, None
        try:
            if isinstance(reader, (TextFileReader, GeneratorType)):
                reader.close()
        finally:
            if csv_stream is not None:
                csv_stream.close()

    def iterrows(self) -> Iterator[tuple[int, dict[str, Any]]]:
        """Iterate over rows as (index, row_dict) tuples.

        Row indices are continuous across all chunks, starting from 0.

        Once the result set that created the iterator is closed, the rows of the
        chunk already read are still returned, and reading the next chunk raises
        ``ProgrammingError``.

        Yields:
            Tuple of (row_index, row_dict) for each row across all chunks.
        """
        row_num = 0
        for df in self:
            columns = df.columns.tolist()
            for row in chain.from_iterable(self._iter_row_blocks(df)):
                yield (row_num, dict(zip(columns, row, strict=True)))
                row_num += 1

    def itertuples(self) -> Iterator[tuple[Any, ...]]:
        """Iterate over rows as tuples of their values across all chunks.

        The tuples hold the values of the columns by position, as
        ``DataFrame.itertuples(index=False, name=None)`` gives them, so columns
        with the same name keep their own values. The next chunk is read when the
        rows of the current one run out. After an exception, such as a value that
        fails to convert, the iteration ends.

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

    def _iter_row_blocks(self, df: DataFrame) -> Iterator[Iterator[tuple[Any, ...]]]:
        """Iterate over the rows of a chunk in blocks of rows.

        A block takes the values of a column at once with ``tolist()`` where that
        gives the values that iterating over the column gives: for columns of a
        NumPy array and of a pandas string dtype. The datetime columns of
        ``fetch_types`` become ``datetime`` or ``date`` objects, with None for NaT.
        The other columns, such as nullable integer, datetime, and
        ``pyarrow``-backed columns, are iterated over. Blocks bound the values held
        at once for a large chunk.

        Args:
            df: The DataFrame chunk.

        Yields:
            An iterator of the rows of each block.
        """
        import pandas as pd

        # Each column's values, and how a slice of them is converted: "list" with
        # tolist(), "datetime" or "date" to objects, or None to be iterated over.
        columns: list[tuple[Any, str | None]] = []
        for i in range(df.shape[1]):
            series = df.iloc[:, i]
            array = series.array
            fetch_type = self._fetch_types.get(df.columns[i])
            if fetch_type is not None and series.dtype.kind == "M":
                columns.append((array, fetch_type))
            elif isinstance(array.dtype, pd.StringDtype):
                columns.append((array, "list"))
            elif isinstance(array, pd.arrays.NumpyExtensionArray):
                # NumpyExtensionArray.to_numpy() would scan the whole column for missing values.
                columns.append((series.to_numpy(), "list"))
            else:
                columns.append((array, None))
        size = self._ROW_BLOCK_SIZE
        for start in range(0, len(df), size):
            yield zip(
                *(self._take_block(values, mode, start, start + size) for values, mode in columns),
                strict=True,
            )

    @staticmethod
    def _take_block(values: Any, mode: str | None, start: int, stop: int) -> Any:
        """Take a block of a column's values for the row iterators.

        Args:
            values: The column's array.
            mode: How the block is converted: ``"list"`` with ``tolist()``,
                ``"datetime"`` or ``"date"`` to objects with None for NaT, or None to
                be iterated over.
            start: The position of the first value.
            stop: The position after the last value.

        Returns:
            The values of the block.
        """
        block = values[start:stop]
        if mode is None:
            return block
        if mode == "list":
            return block.tolist()
        objects = block.date if mode == "date" else block.to_pydatetime()
        objects[block.isna()] = None
        return objects.tolist()

    def get_chunk(self, size: int | None = None) -> DataFrame:
        """Get a chunk of specified size.

        Args:
            size: Number of rows to retrieve. If None, returns entire chunk.

        Returns:
            DataFrame chunk, with date truncation applied as in iteration.

        Raises:
            StopIteration: When no more chunks are available.
            ProgrammingError: If an earlier read failed, or the result set that
                created the iterator is closed.
        """
        from pandas.io.parsers import TextFileReader

        return self._read_chunk(
            lambda reader: reader.get_chunk(size)
            if isinstance(reader, TextFileReader)
            else next(reader)
        )

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


class AthenaPandasResultSetMetadataReader(AthenaUnloadResultSetMetadataReader):
    """Reads the metadata of a pandas result set, and the footers of its Parquet files.

    It owns the read configuration that the metadata depends on: PyAthena's S3
    filesystem for the result files, the read options normalized for the result,
    the Parquet engine, and whether an UNLOAD result is read in chunks. A result
    read in chunks is described by the footer of its first file, read without
    listing the unload location; a result read whole by the schema of its
    dataset, which also has the partition columns of a partitioned UNLOAD.
    ``AthenaPandasResultSet`` takes the first footer
    (:meth:`take_parquet_first_footer`) and reads the footers of the other files
    (:meth:`read_parquet_footers`) for their row counts and the dtypes of the
    columns that hold NULL.
    """

    # The pandas.read_parquet() options given to execute() with which an UNLOAD result is
    # read in chunks; see reads_parquet_in_chunks().
    _PARQUET_CHUNK_OPTIONS: ClassVar[frozenset[str]] = frozenset({"columns", "dtype_backend"})

    def __init__(
        self,
        connection: Connection[Any],
        query_execution: AthenaQueryExecution,
        retry_config: RetryConfig,
        unload: bool = False,
        unload_location: str | None = None,
        engine: str = "auto",
        chunksize: int | None = None,
        block_size: int | None = None,
        cache_type: str | None = None,
        max_workers: int = (cpu_count() or 1) * 5,
        **kwargs: Any,
    ) -> None:
        """Initialize the reader and the filesystem of the result files; it sends no request.

        Args:
            connection: The connection that ran the query.
            query_execution: The query execution whose result set to read.
            retry_config: The retry configuration for API calls.
            unload: Whether the cursor runs its queries as UNLOAD to Parquet.
            unload_location: The S3 location of the UNLOAD output.
            engine: The parsing engine: ``"auto"``, ``"c"``, ``"python"``, or
                ``"pyarrow"``.
            chunksize: The number of rows per chunk, if the result is read in chunks.
            block_size: The S3 read block size of the filesystem.
            cache_type: The default fsspec cache type of the filesystem.
            max_workers: The maximum number of threads of the filesystem.
            **kwargs: The options of ``pandas.read_csv()`` or ``pandas.read_parquet()``.
                ``coerce_int96_timestamp_unit=None`` is dropped for an UNLOAD result.
        """
        super().__init__(
            connection,
            query_execution,
            retry_config,
            unload=unload,
            unload_location=unload_location,
        )
        self._engine = engine
        self._chunksize = chunksize
        # coerce_int96_timestamp_unit=None is pyarrow's default, nanoseconds, and the
        # same as no coerce_int96_timestamp_unit for an UNLOAD result, whose timestamps
        # are still read at microseconds. pandas.read_csv() still rejects it.
        if self.is_unload and kwargs.get("coerce_int96_timestamp_unit", "us") is None:
            del kwargs["coerce_int96_timestamp_unit"]
        self._read_kwargs = kwargs
        self._block_size = block_size
        self._cache_type = cache_type
        self._max_workers = max_workers
        # Created on first use, so that a description of a CSV result creates none.
        self._filesystem: S3FileSystem | None = None
        # The footer of the first file of an UNLOAD result read in chunks; None for one
        # read whole, and once taken.
        self._parquet_first_footer: FileMetaData | None = None

    @property
    def filesystem(self) -> S3FileSystem:
        """PyAthena's S3 filesystem that reads the result files, created on first use."""
        if self._filesystem is None:
            # Not cached by fsspec so that the connection and the dircache are released
            # with the result set.
            self._filesystem = S3FileSystem(
                connection=self._connection,
                default_block_size=self._block_size,
                default_cache_type=self._cache_type,
                max_workers=self._max_workers,
                skip_instance_cache=True,
            )
        return self._filesystem

    @property
    def read_kwargs(self) -> dict[str, Any]:
        """The read options of the result set, normalized for the result."""
        return self._read_kwargs

    @property
    def parquet_engine(self) -> str:
        """The Parquet engine, with ``"auto"`` resolved to an available one.

        Raises:
            ImportError: If pyarrow is not available for ``"auto"``.
        """
        if self._engine == "auto":
            return self.available_engine(["pyarrow"])
        return self._engine

    @staticmethod
    def available_engine(engine_candidates: list[str]) -> str:
        """Get the first available engine from a list of candidates.

        Args:
            engine_candidates: List of engine names to try in order.

        Returns:
            First available engine name.

        Raises:
            ImportError: If no engines are available.
        """
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

    def reads_parquet_in_chunks(self) -> bool:
        """Whether the options allow reading the UNLOAD result in chunks of ``chunksize`` rows.

        Chunks are read with ``ParquetChunkReader`` through PyAthena's filesystem when
        ``execute()`` was given at most these ``pandas.read_parquet()`` options:
        ``dtype_backend`` as ``"numpy_nullable"`` or ``"pyarrow"``, and ``columns`` as
        distinct names without dots, which :meth:`read` also requires to be top-level
        columns of the result. A name with a dot can select a nested field in
        ``pandas.read_parquet()``. With other options, the result is read whole, as
        without ``chunksize``, and pandas validates them.

        Returns:
            True if the options allow reading the UNLOAD result in chunks.
        """
        kwargs = self._read_kwargs
        if self._chunksize is None or not kwargs.keys() <= self._PARQUET_CHUNK_OPTIONS:
            return False
        if kwargs.get("dtype_backend", "numpy_nullable") not in ("numpy_nullable", "pyarrow"):
            return False
        columns = kwargs.get("columns")
        return columns is None or (
            isinstance(columns, (list, tuple))
            and len(columns) > 0
            and all(isinstance(c, str) and "." not in c for c in columns)
            and len(set(columns)) == len(columns)
        )

    def take_parquet_first_footer(self) -> FileMetaData | None:
        """Take the footer of the first file of an UNLOAD result read in chunks.

        Returns:
            The footer that :meth:`read` read, if the result is read in chunks;
            None for a result read whole, and on later calls.
        """
        footer, self._parquet_first_footer = self._parquet_first_footer, None
        return footer

    def read_parquet_footers(self, paths: list[str]) -> list[FileMetaData]:
        """Read the footers of Parquet files of the UNLOAD result.

        The footers are read concurrently, with no more threads than the connection pool
        of the S3 client.

        Args:
            paths: The paths of the files, from the data manifest.

        Returns:
            The footers of the files, in the order of ``paths``.

        Raises:
            OperationalError: If reading the footers fails.
        """
        if not paths:
            return []
        # More threads than the S3 client's connection pool would discard connections,
        # each with a warning from urllib3.
        workers = min(self._connection.s3_client.meta.config.max_pool_connections, len(paths))
        # Each read runs with a copy of this context, so that a stop of the retries in
        # this thread (see stop_retries_on()) stops those of the reads too.
        contexts = [copy_context() for _ in paths]
        try:
            # When the failure of a read reaches map()'s iterator, it cancels the reads
            # that have not started.
            with ThreadPoolExecutor(max_workers=workers) as executor:
                footers = list(
                    executor.map(
                        lambda context, path: context.run(self._read_parquet_footer, path),
                        contexts,
                        paths,
                    )
                )
        except Exception as e:
            _logger.exception(f"Failed to read {self._unload_location}.")
            raise OperationalError(*e.args) from e
        return footers

    @override
    def _read_parquet_metadata(self) -> tuple[dict[str, Any], ...]:
        """Read the metadata of the UNLOAD result, and decide whether it is read in chunks.

        A result whose options allow reading it in chunks (see
        :meth:`reads_parquet_in_chunks`), and whose ``columns`` are top-level
        columns, is read in chunks: it is described by the schema of the footer of
        its first file, which is kept for the read. Any other result is read whole
        with ``pandas.read_parquet()`` and described by the schema of its dataset.

        Returns:
            The column metadata.

        Raises:
            OperationalError: If reading the footer or the schema fails.
            ProgrammingError: If the engine is not ``"pyarrow"``.
        """
        if self.parquet_engine != "pyarrow":
            raise ProgrammingError("Engine must be `pyarrow`.")
        if self.reads_parquet_in_chunks():
            footer = self.read_parquet_footers(self._data_manifest[:1])[0]
            # The files of one UNLOAD query have the same schema.
            schema = footer.schema.to_arrow_schema()
            if set(self._read_kwargs.get("columns") or ()) <= set(schema.names):
                self._parquet_first_footer = footer
                return to_column_info(schema)
            # Columns that are not top-level columns of the result are read whole,
            # where pandas selects nested fields or raises for missing columns.
        return self._read_parquet_schema()

    def _read_parquet_schema(self) -> tuple[dict[str, Any], ...]:
        """Read the column metadata of the dataset of the Parquet files.

        Returns:
            The column metadata, with the partition columns of a partitioned UNLOAD.

        Raises:
            OperationalError: If reading the schema fails.
        """
        from pyarrow import parquet

        bucket, key = parse_output_location(cast(str, self._unload_location))
        try:
            dataset = parquet.ParquetDataset(f"{bucket}/{key}", filesystem=self.filesystem)
            return to_column_info(dataset.schema)
        except Exception as e:
            _logger.exception(f"Failed to read schema {bucket}/{key}.")
            raise OperationalError(*e.args) from e

    def _read_parquet_footer(self, path: str) -> FileMetaData:
        """Read the footer of a Parquet file.

        Args:
            path: The path of the file.

        Returns:
            The metadata of the file.
        """
        from pyarrow import parquet

        with self.filesystem.open(path, "rb") as f:
            return parquet.read_metadata(f)


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

    _metadata_reader_class: ClassVar[type[AthenaPandasResultSetMetadataReader]] = (
        AthenaPandasResultSetMetadataReader
    )

    # File size thresholds and chunking configuration - Public for user customization
    PYARROW_MIN_FILE_SIZE_BYTES: int = 100
    LARGE_FILE_THRESHOLD_BYTES: int = 50 * 1024 * 1024  # 50MB
    ESTIMATED_BYTES_PER_ROW: int = 100
    AUTO_CHUNK_THRESHOLD_LARGE: int = 2_000_000
    AUTO_CHUNK_THRESHOLD_MEDIUM: int = 1_000_000
    AUTO_CHUNK_SIZE_LARGE: int = 100_000
    AUTO_CHUNK_SIZE_MEDIUM: int = 50_000

    _PARSE_DATES: ClassVar[list[str]] = [
        "date",
        "time",
        "timestamp",
    ]
    # The block size of a CSV result file read with the background cache; see
    # _get_csv_open_options().
    CSV_BLOCK_SIZE: ClassVar[int] = 32 * 2**20
    # The pandas.read_csv() options given to execute() that _read_csv_with_pyarrow() reads.
    _PYARROW_READ_CSV_OPTIONS: ClassVar[frozenset[str]] = frozenset({"dtype", "parse_dates"})
    # The pandas.read_csv() options given to execute() that do not change how pandas reads
    # the header row, with which _read_csv_header_as_labels() replaces the header.
    _LABELED_HEADER_READ_CSV_OPTIONS: ClassVar[frozenset[str]] = frozenset(
        {
            "cache_dates",
            "converters",
            "date_format",
            "dayfirst",
            "decimal",
            "dtype_backend",
            "false_values",
            "float_precision",
            "index_col",
            "keep_default_na",
            "low_memory",
            "na_filter",
            "na_values",
            "nrows",
            "on_bad_lines",
            "parse_dates",
            "skipfooter",
            "thousands",
            "true_values",
            "usecols",
        }
    )

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
            cache_type: The default fsspec cache type of the S3 filesystem that reads the results,
                such as ``"none"``, ``"bytes"``, ``"readahead"``, ``"blockcache"``, or
                ``"background"``; see ``S3FileSystem``. Without ``block_size`` or ``cache_type``, a
                CSV result file is read in 32 MiB blocks with fsspec's background cache, which reads
                the next block while pandas parses the current one.
                Without ``cache_type``, PyAthena's filesystem reads a Parquet file of a chunked
                UNLOAD result that is larger than 32 MiB without a cache.
            max_workers: Maximum worker threads for parallel operations.
            auto_optimize_chunksize: Enable automatic chunksize determination
                                   for large files when chunksize is None.
            result_set_type_hints: Athena type signatures for complex-type columns,
                keyed by column name (case-insensitive) or zero-based column index.
            **kwargs: Additional arguments passed to pandas.read_csv/read_parquet.
                ``coerce_int96_timestamp_unit=None`` is the same as no
                ``coerce_int96_timestamp_unit`` for an UNLOAD result.
                A given ``storage_options``, even None, replaces PyAthena's S3 filesystem
                for reading the result files, and so does ``filesystem`` for UNLOAD results.
                The UNLOAD manifest is still read with the connection's S3 client, and the
                schema with PyAthena's filesystem.
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
        self._keep_default_na = keep_default_na
        self._na_values = na_values
        self._quoting = quoting
        self._engine = engine
        self._chunksize = chunksize
        self._block_size = block_size
        self._cache_type = cache_type
        self._auto_optimize_chunksize = auto_optimize_chunksize
        reader = self._metadata_reader_class(
            connection,
            query_execution,
            retry_config,
            unload=unload,
            unload_location=unload_location,
            engine=engine,
            chunksize=chunksize,
            block_size=block_size,
            cache_type=cache_type,
            max_workers=max_workers,
            **kwargs,
        )
        self._metadata_reader = reader
        self._kwargs = reader.read_kwargs
        self._fs = reader.filesystem
        # The whole-result dtypes of the columns of a chunked UNLOAD result that hold NULL,
        # which the reader adds from the footers that it reads.
        # The whole-result dtypes of the columns of a chunked UNLOAD result that hold NULL.
        self._parquet_null_dtypes: dict[str, Any] = {}
        self._csv_stream: IOBase | None = None
        # The converters of the json columns that _read_csv() reads as text, keyed by label.
        self._csv_json_converters: dict[Any, _JSONConverter] = {}
        # The converters of the temporal columns that _read_csv() reads as text, keyed by
        # label; see _get_csv_temporal_converters().
        self._csv_temporal_converters: dict[Any, _TemporalConverter] = {}

        self._load_metadata()
        # The UNLOAD output that the reader found, for the read of the rows.
        self._unload_location = reader.unload_location
        self._data_manifest = reader.data_manifest
        # Cache time column names for efficient _trunc_date processing. _read_csv()
        # replaces them with the labels of the columns that it reads.
        description = self.description if self.description else []
        self._time_columns: list[Any] = [d[0] for d in description if d[1] == "time"]

        import pandas as pd

        # The whole result when it was not read in chunks.
        self._df: DataFrame | None = None
        if self.state == AthenaQueryExecution.STATE_SUCCEEDED:
            if self.output_location:
                result = self._as_pandas()
                trunc_date = (
                    self._finish_parquet_frame if self.is_unload else self._finish_csv_frame
                )
            else:
                # Without a result file, as with managed query result storage, the rows
                # from GetQueryResults are read as a CSV result file, but not in chunks.
                result = self._read_csv()
                trunc_date = self._finish_csv_frame
            # The datetime columns that the fetch methods return as datetime or date.
            self._fetch_types = self._get_fetch_types()
            if isinstance(result, pd.DataFrame):
                self._df = trunc_date(result)
            else:
                self._df_iter = PandasDataFrameIterator(
                    result, trunc_date, self._csv_stream, self._fetch_types, self._closed_event
                )
        else:
            self._fetch_types = {}
            self._df = pd.DataFrame()
        if self._df is not None:
            # A shallow copy keeps assignments to the DataFrame from as_pandas()
            # out of the rows that the fetch methods return. Mutable values in its
            # cells, such as lists from JSON columns, are still shared.
            self._df_iter = PandasDataFrameIterator(
                self._df.copy(deep=False),
                _no_trunc_date,
                fetch_types=self._fetch_types,
                closed=self._closed_event,
            )
        self._row_iter: Iterator[tuple[Any, ...]] = self._df_iter.itertuples()

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
            # Header-less DDL results lose numeric-looking strings and their padding
            # when PyArrow infers types before applying the string dtypes.
            if self.output_location and self.output_location.endswith(".txt"):
                return "c"
            effective_chunksize = chunksize if chunksize is not None else self._chunksize
            is_compatible = (
                effective_chunksize is None
                and self._quoting == 1
                # pandas' PyArrow engine takes no converters. Those of the temporal
                # columns that are converted after reading are not used.
                and self._kwargs.get("converters") is None
                and not self.converters.keys()
                - self._get_csv_temporal_converters(
                    [(d[0], d) for d in self.description or []]
                ).keys()
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
                    return AthenaPandasResultSetMetadataReader.available_engine(["pyarrow"])
                except ImportError:
                    pass

        return "c"

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

        Converts the json and temporal columns and truncates the time columns.

        Args:
            df: The DataFrame or chunk that ``pandas.read_csv()`` returned.

        Returns:
            The same DataFrame.

        Raises:
            OperationalError: If converting a json column fails.
            DataError: If a ``timestamp with time zone`` value has an unknown zone or a
                local time without one instant, naming the column.
        """
        for name, converter in self._csv_json_converters.items():
            try:
                converter.convert(df, name)
            except Exception as e:
                _logger.exception(f"Failed to convert the json column {name!r}.")
                raise OperationalError(*e.args) from e
        for name, temporal_converter in self._csv_temporal_converters.items():
            temporal_converter.convert(df, name)
        return self._trunc_date(df)

    def _finish_parquet_frame(self, df: DataFrame) -> DataFrame:
        """Finish a DataFrame or chunk read from the Parquet files of an UNLOAD result.

        ``date`` columns, which pyarrow reads as ``date`` objects, become
        ``datetime64[us]``, as from a CSV result. With ``dtype_backend`` or
        ``coerce_int96_timestamp_unit`` given to ``execute()``, the columns keep the
        dtypes that these options give.

        Args:
            df: The DataFrame or chunk.

        Returns:
            The same DataFrame.
        """
        if self._kwargs.keys() & {"dtype_backend", "coerce_int96_timestamp_unit"}:
            return df
        dates = {d[0] for d in self.description or [] if d[1] == "date"}
        for i, column in enumerate(df.columns):
            if column in dates and df.dtypes.iloc[i].kind == "O":
                df.isetitem(i, df.iloc[:, i].astype("datetime64[us]"))
        return df

    def _get_fetch_types(self) -> dict[Any, str]:
        """Get the datetime columns that the fetch methods return as datetime or date.

        These are the temporal columns that PyAthena converts: those of
        ``_get_csv_temporal_converters()`` for a CSV result, and the ``timestamp`` and
        ``date`` columns of an UNLOAD result, unless ``dtype_backend`` or
        ``coerce_int96_timestamp_unit`` given to ``execute()`` set their dtypes.
        Columns that ``parse_dates`` given to ``execute()`` parsed keep pandas' values.

        Returns:
            ``"datetime"`` or ``"date"``, keyed by column label.
        """
        if not self.is_unload:
            return {
                label: "date" if converter.type == "date" else "datetime"
                for label, converter in self._csv_temporal_converters.items()
                if converter.type != "time"
            }
        if self._kwargs.keys() & {"dtype_backend", "coerce_int96_timestamp_unit"}:
            return {}
        return {
            d[0]: "date" if d[1] == "date" else "datetime"
            for d in self.description or []
            if d[1] in ("timestamp", "date")
        }

    def _get_csv_temporal_converters(
        self, columns: list[tuple[Any, tuple[Any, ...]]]
    ) -> dict[Any, _TemporalConverter]:
        """Get the converters of the temporal columns that are converted after reading.

        PyAthena converts the ``timestamp``, ``date``, ``timestamp with time zone``, and
        ``time`` columns after reading, unless ``parse_dates``, ``dtype``, or
        ``converters`` is given to ``execute()``, with which pandas reads them, or the
        converter maps the type to a conversion function other than the default
        ``timestamp with time zone`` one.

        Args:
            columns: The label and the description of each column that is read.

        Returns:
            The converters keyed by column label.
        """
        # None is the default of each option, as if it were not given.
        if any(self._kwargs.get(key) is not None for key in ("parse_dates", "dtype", "converters")):
            return {}
        mappings = self._converter.mappings
        temporal_converters = {}
        for label, d in columns:
            type_ = d[1]
            if type_ not in _CSV_TEMPORAL_TYPES:
                continue
            if type_ == "timestamp with time zone":
                if mappings.get(type_) is not _to_datetime_with_tz:
                    continue
            elif type_ in mappings:
                continue
            temporal_converters[label] = _TemporalConverter(type_)
        return temporal_converters

    def _get_csv_converter(
        self, type_: str, label: Any
    ) -> Callable[[str | None], Any] | _JSONConverter:
        """Get the converter for a column of the CSV result file.

        json columns get a ``_JSONConverter``, which ``_read_json_columns_as_text()``
        replaces in the converters for ``pandas.read_csv()``, and which converts the
        columns after reading. ``timestamp with time zone`` columns get a
        ``_TimeZoneConverter``, which names the column in a ``DataError``.

        Args:
            type_: The Athena type of the column.
            label: The label of the column.

        Returns:
            The conversion function, the ``_JSONConverter`` of a json column, or the
            ``_TimeZoneConverter`` of a ``timestamp with time zone`` column.
        """
        converter = self._converter.get(type_)
        if type_ == "json":
            return _JSONConverter(converter)
        if type_ == "timestamp with time zone":
            return _TimeZoneConverter(converter, label)
        return converter

    def _trunc_date(self, df: DataFrame) -> DataFrame:
        """Convert the time columns that pandas parsed as datetimes to ``time`` objects.

        pandas parses the time columns with ``parse_dates`` when ``parse_dates``,
        ``dtype``, or ``converters`` given to ``execute()`` keeps PyAthena from
        converting them after reading. A time column can also be an index level. Time
        columns that PyAthena converted after reading or that pandas did not parse are
        left as they are.

        Args:
            df: The DataFrame or chunk, updated in place.

        Returns:
            The same DataFrame.
        """
        for time_col in self._time_columns:
            if time_col in self._csv_temporal_converters:
                continue
            # A NULL is None, as with the GetQueryResults fallback and the other types.
            _replace_by_label(
                df,
                time_col,
                lambda values: _to_times(values) if values.dtype.kind == "M" else values,
            )
        return df

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

    def _read_csv(self) -> TextFileReader | DataFrame:
        """Read the CSV result file, or the GetQueryResults rows as one without it.

        The GetQueryResults rows are read as a whole, not in chunks.

        Returns:
            The DataFrame of the results, or a reader of their chunks.

        Raises:
            DataError: If a ``timestamp with time zone`` value that pandas converts as it
                reads fails to convert, naming the column.
            OperationalError: If reading the results fails.
        """
        import pandas as pd

        if self.output_location and not self.output_location.endswith((".csv", ".txt")):
            return pd.DataFrame()
        if self.substatement_type and self.substatement_type.upper() in (
            "UPDATE",
            "DELETE",
            "MERGE",
            "VACUUM_TABLE",
        ):
            return pd.DataFrame()
        if self.output_location:
            data = None
            length = self._get_content_length()
            location = self.output_location
        else:
            data = self._fetch_all_rows_as_csv()
            length = len(data)
            location = "the GetQueryResults rows"
        if length == 0:
            return pd.DataFrame()

        # Chunksize determination with user preference priority
        effective_chunksize = self._chunksize if data is None else None

        # Only auto-optimize if user hasn't specified chunksize AND auto_optimize is enabled
        if effective_chunksize is None and self._auto_optimize_chunksize and data is None:
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
                source: str | IOBase = location
                binary_columns = self._configure_binary_csv_read(read_csv_kwargs, labels)
                if labels is not None:
                    # After _configure_binary_csv_read(), which checks for the header row.
                    self._read_csv_header_as_labels(read_csv_kwargs, csv_engine)
                self._read_json_columns_as_text(read_csv_kwargs)
                if data is not None:
                    # The rows are in memory, with nothing to open with storage options.
                    read_csv_kwargs.pop("storage_options", None)
                    if binary_columns:
                        source = self._csv_stream = stack.enter_context(
                            self._open_binary_csv_stream(binary_columns, None, data)
                        )
                    else:
                        source = BytesIO(data)
                elif binary_columns:
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
                        self._fs.open(
                            self.output_location,
                            mode="rb",
                            **self._get_csv_open_options(),
                        )
                    )
                if csv_engine == "pyarrow" and self._reads_csv_with_pyarrow():
                    result = _read_csv_with_pyarrow(source, read_csv_kwargs)
                else:
                    result = pd.read_csv(source, **read_csv_kwargs)
                if not isinstance(result, pd.DataFrame):
                    # The chunk iterator takes ownership of the stream.
                    stack.pop_all()

            # Log performance information for large files
            if data is None and length > self.LARGE_FILE_THRESHOLD_BYTES:
                mode = "chunked" if effective_chunksize else "full"
                chunksize = f" with chunksize={effective_chunksize}" if effective_chunksize else ""
                _logger.info(
                    f"Reading {length} bytes from S3 in {mode} mode "
                    f"using {csv_engine} engine{chunksize}"
                )

            return result

        except Exception as e:
            # converters given to execute() can be of any type.
            converters = read_csv_kwargs.get("converters")
            if isinstance(converters, Mapping) and any(
                isinstance(converter, _TimeZoneConverter) and converter.error is e
                for converter in converters.values()
            ):
                raise
            _logger.exception(f"Failed to read {location}.")
            raise OperationalError(*e.args) from e

    def _read_json_columns_as_text(self, read_csv_kwargs: dict[str, Any]) -> None:
        """Read the json columns as text, which ``_finish_csv_frame()`` converts.

        Replaces the ``_JSONConverter`` converters for ``pandas.read_csv()`` with
        ``_JSONConverter.keep_text()``.

        Args:
            read_csv_kwargs: The options for ``pandas.read_csv()``, updated in place.
        """
        converters = read_csv_kwargs.get("converters") or {}
        self._csv_json_converters = {
            label: converter
            for label, converter in converters.items()
            if isinstance(converter, _JSONConverter)
        }
        if self._csv_json_converters:
            read_csv_kwargs["converters"] = {
                label: _JSONConverter.keep_text
                if isinstance(converter, _JSONConverter)
                else converter
                for label, converter in converters.items()
            }

    def _get_csv_open_options(self) -> dict[str, Any]:
        """Get the options to open a CSV result file with PyAthena's filesystem.

        Without ``block_size`` or ``cache_type`` from the cursor, the file is read
        with fsspec's background cache in blocks of ``CSV_BLOCK_SIZE`` bytes: the
        next block is read while pandas parses the current one, and two blocks are
        kept. Otherwise the filesystem's settings apply. Closing a partly read file
        waits for the block being read; the asyncio cursors close result sets in a
        thread.

        Returns:
            The keyword arguments for ``S3FileSystem.open()``.
        """
        if self._block_size is not None or self._cache_type is not None:
            return {}
        return {
            "block_size": self.CSV_BLOCK_SIZE,
            "cache_type": "background",
            "cache_options": {"maxblocks": 2},
        }

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
        """Build pandas options for Athena CSV or tab-separated results.

        The temporal columns of ``_get_csv_temporal_converters()`` are read as text, and
        ``_finish_csv_frame()`` converts them. The options given to ``execute()``
        replace the ones built here.

        Args:
            csv_engine: The CSV engine that reads the file.
            chunksize: The number of rows of each chunk, or None for a whole read.

        Returns:
            The options for ``pandas.read_csv()``.
        """
        if self.output_location and self.output_location.endswith(".txt"):
            sep = "\t"
            header = None
            names = self._get_column_names()
        else:
            sep = ","
            header = 0
            names = None

        description = self.description or []
        self._csv_temporal_converters = self._get_csv_temporal_converters(
            [(d[0], d) for d in description]
        )
        # The temporal columns that are converted after reading are read as text.
        # pandas' PyArrow engine turns NULL into "None" with the NumPy str dtype, which
        # str means without future.infer_string, so the PyArrow engine reads pyarrow
        # strings.
        temporal = self._csv_temporal_converters
        text_dtype = "string[pyarrow]" if csv_engine == "pyarrow" else "str"
        read_csv_kwargs: dict[str, Any] = {
            "sep": sep,
            "header": header,
            "names": names,
            "dtype": {**self.dtypes, **dict.fromkeys(temporal, text_dtype)},
            "converters": {
                d[0]: self._get_csv_converter(d[1], d[0])
                for d in description
                if d[1] in self._converter.mappings and d[0] not in self._csv_temporal_converters
            },
            "parse_dates": [name for name in self.parse_dates if name not in temporal],
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
            (self.output_location and not self.output_location.endswith(".csv"))
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
        ``execute()`` are kept as they are. The temporal columns that are converted
        after reading are read as text.

        Args:
            read_csv_kwargs: The options for ``pandas.read_csv()``, updated in place.
            labels: The labels from ``_get_csv_column_labels()``.
        """
        description = self.description or []
        columns = [
            (label, d) for label, d in zip(labels, description, strict=True) if label is not None
        ]
        self._csv_temporal_converters = self._get_csv_temporal_converters(columns)
        temporal = self._csv_temporal_converters
        if "dtype" not in self._kwargs:
            read_csv_kwargs["dtype"] = {
                **{
                    label: dtype
                    for label, d in columns
                    if (dtype := self._converter.get_dtype(d[1], d[4], d[5])) is not None
                },
                **dict.fromkeys(temporal, "str"),
            }
        if "converters" not in self._kwargs:
            read_csv_kwargs["converters"] = {
                label: self._get_csv_converter(d[1], label)
                for label, d in columns
                if d[1] in self._converter.mappings and label not in self._csv_temporal_converters
            }
        if "parse_dates" not in self._kwargs:
            read_csv_kwargs["parse_dates"] = [
                label for label, d in columns if d[1] in self._PARSE_DATES and label not in temporal
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
        if len(set(names)) == len(names):
            return
        if not self._kwargs.keys() <= self._LABELED_HEADER_READ_CSV_OPTIONS:
            # Other options, such as dtype, names, sep, comment, or encoding, keep the
            # header row as pandas reads it.
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
        self,
        binary_columns: set[int],
        storage_options: dict[str, Any] | None,
        data: bytes | None = None,
    ) -> TextIOWrapper:
        """Open a stream that preserves binary NULL fields and original CSV newlines.

        Args:
            binary_columns: Zero-based indexes of the binary columns.
            storage_options: The options to open the result file with through fsspec,
                or None to open it with PyAthena's filesystem.
            data: The CSV text to read in place of the result file.

        Returns:
            The text stream for ``pandas.read_csv()``.
        """
        text_options: dict[str, Any] = {"mode": "rt", "encoding": "utf-8", "newline": ""}
        with ExitStack() as stack:
            source: IOBase
            if data is not None:
                source = StringIO(data.decode("utf-8"), newline="")
            elif storage_options is None:
                source = stack.enter_context(
                    self._fs.open(
                        self.output_location,
                        **text_options,
                        **self._get_csv_open_options(),
                    )
                )
            else:
                source = stack.enter_context(
                    filesystem_open(self.output_location, **text_options, **storage_options)
                )
            reader = stack.enter_context(BinaryCSVReader(source, binary_columns))
            buffer = stack.enter_context(BufferedReader(reader))
            stream = TextIOWrapper(buffer, encoding="utf-8", newline="")
            stack.pop_all()
            return stream

    def _read_parquet(self) -> DataFrame:
        """Read the UNLOAD result whole with ``pandas.read_parquet()``.

        The options given to ``execute()`` are passed to ``pandas.read_parquet()``.
        Timestamps are read at microseconds unless ``coerce_int96_timestamp_unit`` is
        given.

        The metadata reader must have read the data manifest first, and checked
        that the engine is pyarrow.

        Returns:
            The DataFrame of the result, or an empty DataFrame without data files.

        Raises:
            OperationalError: If reading the Parquet files fails.
        """
        import pandas as pd

        if not self._data_manifest:
            return pd.DataFrame()
        # Derived from the data manifest by the metadata reader if not given.
        location = cast(str, self._unload_location)

        kwargs: dict[str, Any] = {
            "use_threads": True,
            # Athena writes timestamps as INT96, whose default unit, nanoseconds,
            # cannot hold years before 1677 or after 2262.
            "coerce_int96_timestamp_unit": "us",
            **self._kwargs,
        }
        # Given storage_options, even None, pandas opens the files itself,
        # as for CSV results.
        if "filesystem" not in kwargs and "storage_options" not in kwargs:
            kwargs["filesystem"] = self._fs
        if kwargs.get("filesystem") is None:
            unload_location = location
        else:
            # pyarrow takes the path without the scheme with a filesystem.
            bucket, key = parse_output_location(location)
            unload_location = f"{bucket}/{key}"

        try:
            return pd.read_parquet(unload_location, engine=self._engine, **kwargs)
        except Exception as e:
            _logger.exception(f"Failed to read {self.output_location}.")
            raise OperationalError(*e.args) from e

    def _iter_parquet_chunks(self) -> Iterator[DataFrame]:
        """Read the UNLOAD result in DataFrames of ``chunksize`` rows.

        See ``_convert_parquet_chunk()`` for the conversion of each chunk.
        ``_as_pandas()`` must have read the footers of the files first.

        Yields:
            The DataFrames of the result rows.

        Raises:
            OperationalError: If reading the Parquet files fails.
        """
        start = 0
        try:
            with ParquetChunkReader(
                self._fs,
                self._data_manifest,
                cast(int, self._chunksize),
                columns=self._kwargs.get("columns"),
                cache_type=self._cache_type,
            ) as reader:
                for table in reader:
                    df = self._convert_parquet_chunk(table, start)
                    start += len(df)
                    yield df
        except Exception as e:
            _logger.exception(f"Failed to read {self._unload_location}.")
            raise OperationalError(*e.args) from e

    def _convert_parquet_chunk(self, table: Table, start: int) -> DataFrame:
        """Convert a chunk of an UNLOAD result as ``pandas.read_parquet()`` converts a table.

        Without ``dtype_backend``, top-level integer and boolean columns that hold NULL
        anywhere in the result, according to the NULL counts of the Parquet
        statistics, take the ``float64`` and ``object`` dtypes they have in the whole
        result. A column without NULL counts, and the values of array, map, and struct
        columns, are converted as pyarrow converts the chunk, so their dtypes can
        depend on whether the chunk holds NULL. With ``dtype_backend``, the dtypes of
        top-level columns do not depend on NULL; with ``"pyarrow"``, those of nested
        values do not either.

        Args:
            table: The rows of the chunk.
            start: The index of the first row of the chunk in the result.

        Returns:
            The DataFrame of the chunk, indexed from ``start``.
        """
        import pandas as pd
        from pandas.io._util import arrow_table_to_pandas

        if "dtype_backend" in self._kwargs:
            df = arrow_table_to_pandas(table, dtype_backend=self._kwargs["dtype_backend"])
        else:
            df = arrow_table_to_pandas(table)
            df = df.astype({c: t for c, t in self._parquet_null_dtypes.items() if c in df})
        df.index = pd.RangeIndex(start, start + len(df))
        return df

    @staticmethod
    def _get_parquet_null_dtypes(footer: FileMetaData) -> dict[str, Any]:
        """Get the whole-result dtypes of the columns that hold NULL in a Parquet file.

        These are the top-level integer and boolean columns whose NULL counts show
        NULL; pyarrow converts them to ``float64`` and ``object``. A column without
        NULL counts is not included, so that its integers are not converted to
        ``float64`` without NULL.

        Args:
            footer: The metadata of the file.

        Returns:
            The dtypes keyed by column name.
        """
        import pyarrow as pa

        # Statistics are kept per leaf column; a top-level primitive column is the leaf
        # column whose path is its name.
        leaves = {footer.schema.column(i).path: i for i in range(footer.num_columns)}

        def holds_null(name: str) -> bool:
            return any(
                (statistics := footer.row_group(group).column(leaves[name]).statistics) is not None
                and statistics.has_null_count
                and statistics.null_count > 0
                for group in range(footer.num_row_groups)
            )

        return {
            field.name: "float64" if pa.types.is_integer(field.type) else object
            for field in footer.schema.to_arrow_schema()
            if (pa.types.is_integer(field.type) or pa.types.is_boolean(field.type))
            and holds_null(field.name)
        }

    def _as_pandas(self) -> TextFileReader | DataFrame | Iterator[DataFrame]:
        """Read the result file, or the Parquet files of an UNLOAD result.

        An UNLOAD result is read as its metadata reader decided: in chunks, after
        the footers of the other files, or whole.

        Returns:
            The DataFrame of the result, or a reader or an iterator of its chunks.

        Raises:
            OperationalError: If reading the result fails.
        """
        if not self.is_unload:
            return self._read_csv()
        reader = cast(AthenaPandasResultSetMetadataReader, self._reader)
        first_footer = reader.take_parquet_first_footer()
        if first_footer is not None:
            footers = [first_footer, *reader.read_parquet_footers(self._data_manifest[1:])]
            for footer in footers:
                self._parquet_null_dtypes.update(self._get_parquet_null_dtypes(footer))
            if not any(footer.num_rows for footer in footers):
                from pyarrow import parquet

                # Files without rows still have the columns that describe the result,
                # with INT96 timestamps at microseconds, as the chunks read them.
                with self._fs.open(self._data_manifest[0], "rb") as f:
                    schema = parquet.ParquetFile(
                        f, metadata=first_footer, coerce_int96_timestamp_unit="us"
                    ).schema_arrow
                table = schema.empty_table()
                columns = self._kwargs.get("columns")
                return self._convert_parquet_chunk(table.select(columns) if columns else table, 0)
            return self._iter_parquet_chunks()
        return self._read_parquet()

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

        Raises:
            ProgrammingError: If the result set is closed.
        """
        self._ensure_open()
        if self._chunksize is None:
            if self._df is not None:
                return self._df
            return self._df_iter.as_pandas()
        return self.iter_chunks()

    def iter_chunks(self) -> PandasDataFrameIterator:
        """Iterate over result chunks as pandas DataFrames.

        This method provides an iterator interface for processing large result sets.
        When the result is read in chunks, because chunksize is specified for a CSV
        result or for an UNLOAD result read with at most the ``columns`` (distinct
        top-level column names without dots) and ``dtype_backend`` options, or
        ``auto_optimize_chunksize`` chose a chunk size
        for a CSV result, it yields
        DataFrames in chunks for memory-efficient processing. These chunks come from
        the same iterator as the fetch methods, so a chunk that one of them reads is
        not available to the other. Otherwise, each call returns a new iterator that
        yields the entire result as a single DataFrame, and the fetch methods keep
        their position.

        Returns:
            PandasDataFrameIterator that yields pandas DataFrames for each chunk
            of rows, or the entire DataFrame if the result was not read in chunks.
            Its reads raise ``ProgrammingError`` once the result set is closed.

        Raises:
            ProgrammingError: If the result set is closed.

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
        self._ensure_open()
        if self._df is not None:
            return PandasDataFrameIterator(
                self._df, _no_trunc_date, fetch_types=self._fetch_types, closed=self._closed_event
            )
        return self._df_iter

    @override
    def close(self) -> None:
        import pandas as pd

        super().close()
        self._closed_event.set()
        self._df_iter.close()
        self._df = pd.DataFrame()
        self._df_iter = PandasDataFrameIterator(self._df, _no_trunc_date)
        self._row_iter = iter(())
        self._data_manifest = []
