"""Result set that reads Athena query results into Apache Arrow Tables."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
)

from pyathena import OperationalError
from pyathena.arrow.converter import _to_timestamp
from pyathena.arrow.util import to_column_info
from pyathena.converter import Converter, _to_default
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet
from pyathena.util import RetryConfig, override, parse_output_location

if TYPE_CHECKING:
    import polars as pl
    from pyarrow import Table

    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)


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
            **kwargs: Additional keyword arguments, stored but not used.

        Raises:
            ProgrammingError: If ``query_execution`` is not given.
            OperationalError: If reading the query results fails.
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
        self._block_size = block_size if block_size else self.DEFAULT_BLOCK_SIZE
        self._unload = unload
        self._unload_location = unload_location
        self._connect_timeout = connect_timeout
        self._request_timeout = request_timeout
        self._kwargs = kwargs
        self._fs = self._create_s3_file_system()
        if self.state == AthenaQueryExecution.STATE_SUCCEEDED and self.output_location:
            self._table = self._as_arrow()
        elif self.state == AthenaQueryExecution.STATE_SUCCEEDED:
            # Without a result file, as with managed query result storage, the rows from
            # GetQueryResults are read as a CSV result file.
            self._table = self._read_csv()
        else:
            import pyarrow as pa

            self._table = pa.Table.from_pydict({})
        self._batches = iter(self._table.to_batches(arraysize))

    def _create_s3_file_system(self):
        """Create a pyarrow ``S3FileSystem`` from the connection settings.

        Returns:
            The pyarrow S3 filesystem for reading the query results.
        """
        from pyarrow import fs

        connection = self.connection

        # Build timeout parameters dict
        timeout_kwargs = {}
        if self._connect_timeout is not None:
            timeout_kwargs["connect_timeout"] = self._connect_timeout
        if self._request_timeout is not None:
            timeout_kwargs["request_timeout"] = self._request_timeout

        if connection._kwargs.get("role_arn"):
            external_id = connection._kwargs.get("external_id")
            fs = fs.S3FileSystem(
                role_arn=connection._kwargs["role_arn"],
                session_name=connection._kwargs["role_session_name"],
                external_id="" if external_id is None else external_id,
                load_frequency=connection._kwargs["duration_seconds"],
                region=connection.region_name,
                **timeout_kwargs,
            )
        elif connection.profile_name:
            profile = connection.session._session.full_config["profiles"][connection.profile_name]
            fs = fs.S3FileSystem(
                access_key=profile.get("aws_access_key_id", None),
                secret_key=profile.get("aws_secret_access_key", None),
                session_token=profile.get("aws_session_token", None),
                region=connection.region_name,
                **timeout_kwargs,
            )
        else:
            # Try explicit credentials first
            explicit_access_key = connection._kwargs.get("aws_access_key_id")
            explicit_secret_key = connection._kwargs.get("aws_secret_access_key")

            if explicit_access_key and explicit_secret_key:
                # Use explicitly provided credentials
                fs = fs.S3FileSystem(
                    access_key=explicit_access_key,
                    secret_key=explicit_secret_key,
                    session_token=connection._kwargs.get("aws_session_token"),
                    region=connection.region_name,
                    **timeout_kwargs,
                )
            else:
                # Fall back to dynamic credentials from boto3 session
                # This handles EC2 instance profiles, temporary credentials, etc.
                try:
                    credentials = connection.session._session.get_credentials()
                    if credentials:
                        fs = fs.S3FileSystem(
                            access_key=credentials.access_key,
                            secret_key=credentials.secret_key,
                            session_token=credentials.token,
                            region=connection.region_name,
                            **timeout_kwargs,
                        )
                    else:
                        # Fall back to default (no explicit credentials)
                        fs = fs.S3FileSystem(region=connection.region_name, **timeout_kwargs)
                except Exception:
                    # Fall back to default if credential retrieval fails
                    fs = fs.S3FileSystem(region=connection.region_name, **timeout_kwargs)

        return fs

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
        try:
            rows = next(self._batches)
        except StopIteration:
            return
        else:
            # Read the columns and their converters by position; to_pydict() and the
            # converters property keep one column per name.
            columns = [column.to_pylist() for column in rows.columns]
            description = self.description if self.description else []
            converters = [self._converter.get(d[1]) for d in description]
            if any(convert is not _to_default for convert in converters):
                processed_rows = [
                    tuple(convert(v) for convert, v in zip(converters, row, strict=False))
                    for row in zip(*columns, strict=False)
                ]
            else:
                processed_rows = list(zip(*columns, strict=False))
            self._rows.extend(processed_rows)

    @override
    def fetchone(
        self,
    ) -> tuple[Any | None, ...] | dict[Any, Any | None] | None:
        if not self._rows:
            self._fetch()
        if not self._rows:
            return None
        if self._rownumber is None:
            self._rownumber = 0
        self._rownumber += 1
        return self._rows.popleft()

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
            table = csv.read_csv(
                self._fs.open_input_stream(location) if data is None else pa.BufferReader(data),
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
            if binary_columns:
                for index, field in enumerate(table.schema):
                    if index not in binary_columns and (
                        pa.types.is_string(field.type) or pa.types.is_binary(field.type)
                    ):
                        # Preserve the existing CSV behavior for non-binary Athena columns.
                        table = table.set_column(index, field, table.column(index).fill_null(""))
            return table
        except Exception as e:
            _logger.exception(f"Failed to read {location}.")
            raise OperationalError(*e.args) from e

    def _read_parquet(self) -> Table:
        import pyarrow as pa
        from pyarrow import parquet

        manifests = self._read_data_manifest()
        if not manifests:
            return pa.Table.from_pydict({})
        if not self._unload_location:
            self._unload_location = "/".join(manifests[0].split("/")[:-1]) + "/"

        bucket, key = parse_output_location(self._unload_location)
        try:
            dataset = parquet.ParquetDataset(f"{bucket}/{key}", filesystem=self._fs)
            return dataset.read(use_threads=True)
        except Exception as e:
            _logger.exception(f"Failed to read {bucket}/{key}.")
            raise OperationalError(*e.args) from e

    def _as_arrow(self) -> Table:
        if self.is_unload:
            table = self._read_parquet()
            self._metadata = to_column_info(table.schema)
        else:
            table = self._read_csv()
        return table

    def as_arrow(self) -> Table:
        """Return the query results as an Apache Arrow Table.

        Returns:
            The Arrow Table that holds the query results.
        """
        return self._table

    def as_polars(self) -> pl.DataFrame:
        """Return query results as a Polars DataFrame.

        Converts the Apache Arrow Table to a Polars DataFrame for
        interoperability with the Polars data processing library.

        Returns:
            Polars DataFrame containing all query results.

        Raises:
            ImportError: If polars is not installed.

        Example:
            >>> cursor = connection.cursor(ArrowCursor)
            >>> cursor.execute("SELECT * FROM my_table")
            >>> df = cursor.as_polars()
            >>> # Use with Polars operations
        """
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
