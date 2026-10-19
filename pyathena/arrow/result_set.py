"""Result set that reads Athena query results into Apache Arrow Tables."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    cast,
)

from pyathena import DataError, OperationalError
from pyathena.arrow.converter import _to_null, _to_timestamp
from pyathena.arrow.util import to_column_info
from pyathena.converter import Converter, _to_default
from pyathena.filesystem.reader import ReadAheadStream
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet, AthenaUnloadResultSetMetadataReader
from pyathena.util import RetryConfig, override, parse_output_location

if TYPE_CHECKING:
    import polars as pl
    from pyarrow import Table
    from pyarrow.parquet import ParquetDataset

    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)


class AthenaArrowResultSetMetadataReader(AthenaUnloadResultSetMetadataReader):
    """Reads the metadata of an Arrow result set, and opens the dataset of its Parquet files.

    It owns the pyarrow S3 filesystem of the result files, which uses the
    credentials of the connection's session and the given timeouts. An UNLOAD
    result is described by the schema of the ``ParquetDataset`` of its files,
    which reads INT96 timestamps at microseconds and has the partition columns
    of a partitioned UNLOAD. ``AthenaArrowResultSet`` takes the dataset
    (:meth:`take_parquet_dataset`) and reads its table.
    """

    def __init__(
        self,
        connection: Connection[Any],
        query_execution: AthenaQueryExecution,
        retry_config: RetryConfig,
        unload: bool = False,
        unload_location: str | None = None,
        connect_timeout: float | None = None,
        request_timeout: float | None = None,
    ) -> None:
        """Initialize the reader and the filesystem of the result files; it sends no request.

        Args:
            connection: The connection that ran the query.
            query_execution: The query execution whose result set to read.
            retry_config: The retry configuration for API calls.
            unload: Whether the cursor runs its queries as UNLOAD to Parquet.
            unload_location: The S3 location of the UNLOAD output.
            connect_timeout: The connect timeout in seconds for the pyarrow S3 filesystem.
            request_timeout: The request timeout in seconds for the pyarrow S3 filesystem.
        """
        super().__init__(
            connection,
            query_execution,
            retry_config,
            unload=unload,
            unload_location=unload_location,
        )
        self._connect_timeout = connect_timeout
        self._request_timeout = request_timeout
        # Created on first use, so that a description of a CSV result creates none.
        self._filesystem: Any = None
        # The dataset of the Parquet files of an UNLOAD result, which read() opens; None
        # without data files, and once taken.
        self._parquet_dataset: ParquetDataset | None = None

    @property
    def filesystem(self) -> Any:
        """The pyarrow S3 filesystem that reads the result files, created on first use."""
        if self._filesystem is None:
            self._filesystem = self._create_s3_file_system()
        return self._filesystem

    def take_parquet_dataset(self) -> ParquetDataset | None:
        """Take the dataset of the Parquet files of the UNLOAD result.

        Returns:
            The dataset that :meth:`read` opened; None without data files, and on
            later calls.
        """
        dataset, self._parquet_dataset = self._parquet_dataset, None
        return dataset

    @override
    def _read_parquet_metadata(self) -> tuple[dict[str, Any], ...]:
        """Open the dataset of the Parquet files of the UNLOAD result, and describe it.

        The dataset reads timestamps at microseconds, and is kept for the read.

        Returns:
            The column metadata of the schema of the dataset.

        Raises:
            OperationalError: If opening the files or reading the schema fails.
        """
        from pyarrow import parquet

        bucket, key = parse_output_location(cast(str, self._unload_location))
        try:
            # Athena writes timestamps as INT96, whose default unit, nanoseconds, cannot
            # hold years before 1677 or after 2262.
            self._parquet_dataset = parquet.ParquetDataset(
                f"{bucket}/{key}", filesystem=self.filesystem, coerce_int96_timestamp_unit="us"
            )
            return to_column_info(self._parquet_dataset.schema)
        except Exception as e:
            _logger.exception(f"Failed to read schema {bucket}/{key}.")
            raise OperationalError(*e.args) from e

    def _create_s3_file_system(self) -> Any:
        """Create a pyarrow ``S3FileSystem`` with the credentials of the connection's session.

        Returns:
            The pyarrow S3 filesystem for reading the query results.
        """
        from pyarrow import fs

        kwargs: dict[str, Any] = {"region": self._connection.region_name}
        if self._connect_timeout is not None:
            kwargs["connect_timeout"] = self._connect_timeout
        if self._request_timeout is not None:
            kwargs["request_timeout"] = self._request_timeout
        credentials = self._connection.session.get_credentials()
        if credentials:
            frozen_credentials = credentials.get_frozen_credentials()
            kwargs["access_key"] = frozen_credentials.access_key
            kwargs["secret_key"] = frozen_credentials.secret_key
            kwargs["session_token"] = frozen_credentials.token
        return fs.S3FileSystem(**kwargs)


class AthenaArrowResultSet(AthenaResultSet):
    """Result set that provides Apache Arrow Table results with columnar optimization.

    This result set handles CSV and Parquet result files from S3, converting them to
    Apache Arrow Tables which provide efficient columnar data processing and memory
    usage. It's optimized for analytical workloads and large dataset operations.

    Features:
        - Efficient columnar data processing with Apache Arrow
        - Support for both CSV and Parquet result formats
        - Optimized memory usage for large datasets
        - Advanced timestamp parsing with multiple format support
        - Zero-copy operations where possible

    Attributes:
        DEFAULT_BLOCK_SIZE: Default block size for Arrow operations (128MB).

    Example:
        >>> # Used automatically by ArrowCursor
        >>> cursor = connection.cursor(ArrowCursor)
        >>> cursor.execute("SELECT * FROM large_table")
        >>>
        >>> # Get Arrow Table
        >>> table = cursor.as_arrow()
        >>>
        >>> # Convert to pandas if needed
        >>> df = table.to_pandas()
        >>>
        >>> # Or work with Arrow directly
        >>> print(f"Table has {table.num_rows} rows and {table.num_columns} columns")

    Note:
        This class is used internally by ArrowCursor and typically not
        instantiated directly by users. Requires pyarrow to be installed.
    """

    _metadata_reader_class: ClassVar[type[AthenaArrowResultSetMetadataReader]] = (
        AthenaArrowResultSetMetadataReader
    )

    DEFAULT_BLOCK_SIZE = 1024 * 1024 * 128

    _timestamp_parsers: ClassVar[list[str]] = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S %Z",
        "%Y-%m-%d %H:%M:%S %z",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S.%f %Z",
        "%Y-%m-%d %H:%M:%S.%f %z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%f %Z",
        "%Y-%m-%dT%H:%M:%S.%f %z",
    ]

    def __init__(
        self,
        connection: Connection[Any],
        converter: Converter,
        query_execution: AthenaQueryExecution,
        arraysize: int,
        retry_config: RetryConfig,
        block_size: int | None = None,
        unload: bool = False,
        unload_location: str | None = None,
        connect_timeout: float | None = None,
        request_timeout: float | None = None,
        result_set_type_hints: dict[str | int, str] | None = None,
        **kwargs,
    ) -> None:
        """Initialize the result set and load the query results into an Arrow Table.

        Args:
            connection: The connection that ran the query.
            converter: The converter for result values.
            query_execution: The query execution whose results to read.
            arraysize: The default ``fetchmany()`` size and the maximum number of rows per
                record batch that the fetch methods read from the table.
            retry_config: The retry configuration for API calls.
            block_size: The block size in bytes for reading CSV results. If not set,
                ``DEFAULT_BLOCK_SIZE`` is used.
            unload: Whether the query is an ``UNLOAD`` whose Parquet output is read
                instead of the CSV results.
            unload_location: The S3 location of the ``UNLOAD`` output. If None, it is
                derived from the first file in the data manifest.
            connect_timeout: The connect timeout in seconds for the pyarrow S3 filesystem.
            request_timeout: The request timeout in seconds for the pyarrow S3 filesystem.
            result_set_type_hints: Athena type signatures for complex-type columns,
                keyed by column name (case-insensitive) or zero-based column index.
            **kwargs: Additional keyword arguments, which are ignored.

        Raises:
            ProgrammingError: If ``query_execution`` is not given.
            OperationalError: If reading the query results fails.
        """
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
        self._block_size = block_size if block_size else self.DEFAULT_BLOCK_SIZE
        reader = self._metadata_reader_class(
            connection,
            query_execution,
            retry_config,
            unload=unload,
            unload_location=unload_location,
            connect_timeout=connect_timeout,
            request_timeout=request_timeout,
        )
        self._metadata_reader = reader
        self._fs = reader.filesystem
        self._load_metadata()
        succeeded = self.state == AthenaQueryExecution.STATE_SUCCEEDED
        # The UNLOAD output location that the reader found, given or derived.
        self._unload_location = reader.unload_location
        if succeeded and self.output_location:
            self._table = self._as_arrow()
        elif succeeded:
            # Without a result file, as with managed query result storage, the rows from
            # GetQueryResults are read as a CSV result file.
            self._table = self._read_csv()
        else:
            import pyarrow as pa

            self._table = pa.Table.from_pydict({})
        self._batches = iter(self._table.to_batches(arraysize))

    @property
    def timestamp_parsers(self) -> list[str]:
        """The timestamp formats for reading CSV results, starting with pyarrow's ``ISO8601``."""
        from pyarrow.csv import ISO8601

        return [ISO8601, *self._timestamp_parsers]

    @property
    def column_types(self) -> dict[str, type[Any]]:
        """The converter's types for the result columns it maps, keyed by column name."""
        description = self.description if self.description else []
        return {
            d[0]: dtype
            for d in description
            if (dtype := self._converter.get_dtype(d[1], d[4], d[5])) is not None
        }

    @property
    def converters(self) -> dict[str, Callable[[str | None], Any | None]]:
        """The conversion functions for the result columns, keyed by column name."""
        description = self.description if self.description else []
        return {d[0]: self._converter.get(d[1]) for d in description}

    @override
    def _fetch(self) -> None:
        """Buffer the rows of the next record batch that has any.

        A table can hold empty record batches between others, such as one of
        an empty result file, which would otherwise end the fetches early.

        Raises:
            DataError: If a value fails to convert, such as a ``timestamp with time
                zone`` value with an unknown zone or a local time without one instant in
                its zone. The message names the column.
        """
        rows = next((batch for batch in self._batches if batch.num_rows), None)
        if rows is None:
            return
        # Read the columns and their converters by position; to_pydict() and the
        # converters property keep one column per name.
        columns = [column.to_pylist() for column in rows.columns]
        description = self.description if self.description else []
        names = [d[0] for d in description]
        converters = [self._converter.get(d[1]) for d in description]

        def convert(row: tuple[Any, ...]) -> Iterator[Any]:
            # Unlike a loop that returns the values, a generator turns a StopIteration
            # from the converter into a RuntimeError instead of ending an iteration of
            # the rows.
            for name, converter, value in zip(names, converters, row, strict=False):
                try:
                    converted = converter(value)
                except DataError as e:
                    raise DataError(f"Column {name!r}: {e}") from e
                yield converted

        if any(converter is not _to_default for converter in converters):
            processed_rows = [tuple(convert(row)) for row in zip(*columns, strict=False)]
        else:
            processed_rows = list(zip(*columns, strict=False))
        self._rows.extend(processed_rows)

    def _open_csv(self, location: str) -> Any:
        """Open the CSV result file for ``pyarrow.csv.read_csv()``.

        The file is read in parts with several parts read ahead in parallel
        through the pyarrow S3 filesystem; see
        :class:`~pyathena.filesystem.reader.ReadAheadStream`.

        Args:
            location: The bucket and key of the file.

        Returns:
            The file as a pyarrow input stream, which closes the file when closed.
        """
        import pyarrow as pa

        file = self._fs.open_input_file(location)
        try:
            stream = ReadAheadStream(
                lambda offset, size: file.read_at(size, offset), file.size(), on_close=file.close
            )
        except BaseException:
            file.close()
            raise
        return pa.PythonFile(stream, mode="r")

    def _read_csv(self) -> Table:
        """Read the CSV result file, or the GetQueryResults rows as one without it.

        Returns:
            The Arrow Table of the results.

        Raises:
            OperationalError: If reading the results fails.
        """
        import pyarrow as pa
        from pyarrow import csv

        if self.output_location and not self.output_location.endswith((".csv", ".txt")):
            return pa.Table.from_pydict({})
        if self.substatement_type and self.substatement_type.upper() in (
            "UPDATE",
            "DELETE",
            "MERGE",
            "VACUUM_TABLE",
        ):
            return pa.Table.from_pydict({})
        if self.output_location:
            data = None
            length = self._get_content_length()
            location = "/".join(parse_output_location(self.output_location))
        else:
            data = self._fetch_all_rows_as_csv()
            length = len(data)
            location = "the GetQueryResults rows"
        description = self.description if self.description else []
        names = [d[0] for d in description]
        # pyarrow types every column with a name by its column_types entry, so columns
        # with the same name are read under their positions and get their names back
        # after reading.
        has_duplicate_names = len(set(names)) != len(names)
        if has_duplicate_names:
            column_names = [str(i) for i in range(len(names))]
            column_types = {
                str(i): dtype
                for i, d in enumerate(description)
                if (dtype := self._converter.get_dtype(d[1], d[4], d[5])) is not None
            }
        else:
            column_names = names
            column_types = self.column_types
        # Timestamp columns are read as text: pyarrow does not parse fractions finer
        # than the unit, and on some platforms its strptime fallbacks accept such a
        # value without its fraction.
        timestamp_types = {
            i: dtype
            for i, (name, d) in enumerate(zip(column_names, description, strict=True))
            if d[1] == "timestamp" and isinstance(dtype := column_types.get(name), pa.TimestampType)
        }
        binary_columns = {i for i, d in enumerate(description) if d[1] == "varbinary"}
        # Athena never writes an empty string for these types, so an empty string in
        # their text columns is NULL.
        null_text_columns = {
            i
            for i, d in enumerate(description)
            if d[1] in ("time", "time with time zone", "timestamp with time zone")
        }
        if length and self.output_location and self.output_location.endswith(".txt"):
            read_opts = csv.ReadOptions(
                skip_rows=0,
                column_names=column_names,
                block_size=self._block_size,
                use_threads=True,
            )
            parse_opts = csv.ParseOptions(
                delimiter="\t",
                quote_char=False,
                double_quote=False,
                escape_char=False,
            )
        elif length:
            read_opts = csv.ReadOptions(skip_rows=0, block_size=self._block_size, use_threads=True)
            if has_duplicate_names:
                read_opts.column_names = column_names
                # Skips the header as a parsed row; skip_rows would split a quoted name
                # that contains a newline.
                read_opts.skip_rows_after_names = 1
            parse_opts = csv.ParseOptions(
                delimiter=",",
                quote_char='"',
                # Athena writes a single-column row with a NULL value as an empty line.
                ignore_empty_lines=False,
                double_quote=True,
                escape_char=False,
                # A quoted value can contain a newline, so the reader must not split
                # blocks inside quotes.
                newlines_in_values=True,
            )
        else:
            return pa.Table.from_pydict({})

        try:
            with self._open_csv(location) if data is None else pa.BufferReader(data) as source:
                table = csv.read_csv(
                    source,
                    read_options=read_opts,
                    parse_options=parse_opts,
                    convert_options=csv.ConvertOptions(
                        strings_can_be_null=bool(binary_columns),
                        quoted_strings_can_be_null=False,
                        timestamp_parsers=self.timestamp_parsers,
                        column_types={
                            **column_types,
                            **{column_names[i]: pa.string() for i in timestamp_types},
                        },
                    ),
                )
            for index, type_ in timestamp_types.items():
                table = table.set_column(
                    index,
                    pa.field(table.schema.field(index).name, type_),
                    _to_timestamp(table.column(index), type_),
                )
            if has_duplicate_names:
                table = table.rename_columns(names)
            for index, field in enumerate(table.schema):
                if index in null_text_columns and pa.types.is_string(field.type):
                    table = table.set_column(index, field, _to_null(table.column(index)))
                elif (
                    binary_columns
                    and index not in binary_columns
                    and (pa.types.is_string(field.type) or pa.types.is_binary(field.type))
                ):
                    # Preserve the existing CSV behavior for non-binary Athena columns.
                    table = table.set_column(index, field, table.column(index).fill_null(""))
            return table
        except Exception as e:
            _logger.exception(f"Failed to read {location}.")
            raise OperationalError(*e.args) from e

    def _read_parquet(self) -> Table:
        """Read the UNLOAD result, with timestamps at microseconds.

        Reads the dataset that the metadata reader opened.

        Returns:
            The table of the result, or an empty table without data files.

        Raises:
            OperationalError: If reading the Parquet files fails.
        """
        import pyarrow as pa

        reader = cast(AthenaArrowResultSetMetadataReader, self._reader)
        dataset = reader.take_parquet_dataset()
        if dataset is None:
            return pa.Table.from_pydict({})
        try:
            return dataset.read(use_threads=True)
        except Exception as e:
            _logger.exception(f"Failed to read {self._unload_location}.")
            raise OperationalError(*e.args) from e

    def _as_arrow(self) -> Table:
        """Read the result file, or the Parquet files of an UNLOAD result.

        Returns:
            The table of the result.

        Raises:
            OperationalError: If reading the result fails.
        """
        return self._read_parquet() if self.is_unload else self._read_csv()

    def as_arrow(self) -> Table:
        """Return the query results as an Apache Arrow Table.

        Returns:
            The Arrow Table that holds the query results.

        Raises:
            ProgrammingError: If the result set is closed.
        """
        self._ensure_open()
        return self._table

    def as_polars(self) -> pl.DataFrame:
        """Return query results as a Polars DataFrame.

        Converts the Apache Arrow Table to a Polars DataFrame for
        interoperability with the Polars data processing library.

        Returns:
            Polars DataFrame containing all query results.

        Raises:
            ImportError: If polars is not installed.
            ProgrammingError: If the result set is closed.

        Example:
            >>> cursor = connection.cursor(ArrowCursor)
            >>> cursor.execute("SELECT * FROM my_table")
            >>> df = cursor.as_polars()
            >>> # Use with Polars operations
        """
        self._ensure_open()
        try:
            import polars as pl

            return pl.from_arrow(self._table)  # type: ignore[return-value]
        except ImportError as e:
            raise ImportError(
                "polars is required for as_polars(). Install it with: pip install polars"
            ) from e

    @override
    def close(self) -> None:
        import pyarrow as pa

        super().close()
        self._table = pa.Table.from_pydict({})
        self._batches = iter([])
