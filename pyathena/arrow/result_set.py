# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

from pyathena import OperationalError
from pyathena.arrow.util import to_column_info
from pyathena.converter import Converter
from pyathena.error import ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet
from pyathena.util import RetryConfig, parse_output_location

if TYPE_CHECKING:
    import polars as pl
    from pyarrow import Table

    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


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
        >>> table = cursor.fetchall()
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

    _timestamp_parsers: List[str] = [
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
        connection: "Connection[Any]",
        converter: Converter,
        query_execution: AthenaQueryExecution,
        arraysize: int,
        retry_config: RetryConfig,
        block_size: Optional[int] = None,
        unload: bool = False,
        unload_location: Optional[str] = None,
        connect_timeout: Optional[float] = None,
        request_timeout: Optional[float] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            connection=connection,
            converter=converter,
            query_execution=query_execution,
            arraysize=1,  # Fetch one row to retrieve metadata
            retry_config=retry_config,
        )
        self._rows.clear()  # Clear pre_fetch data
        self._arraysize = arraysize
        self._block_size = block_size if block_size else self.DEFAULT_BLOCK_SIZE
        self._unload = unload
        self._unload_location = unload_location
        self._connect_timeout = connect_timeout
        self._request_timeout = request_timeout
        self._kwargs = kwargs
        self._fs = self.__s3_file_system()
        if self.state == AthenaQueryExecution.STATE_SUCCEEDED and self.output_location:
            self._table = self._as_arrow()
        elif self.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self._table = self._as_arrow_from_api()
        else:
            import pyarrow as pa

            self._table = pa.Table.from_pydict({})
        self._batches = iter(self._table.to_batches(arraysize))

    def __s3_file_system(self):
        from pyarrow import fs

        connection = self.connection

        # Build timeout parameters dict
        timeout_kwargs = {}
        if self._connect_timeout is not None:
            timeout_kwargs["connect_timeout"] = self._connect_timeout
        if self._request_timeout is not None:
            timeout_kwargs["request_timeout"] = self._request_timeout

        if "role_arn" in connection._kwargs and connection._kwargs["role_arn"]:
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
    def timestamp_parsers(self) -> List[str]:
        from pyarrow.csv import ISO8601

        return [ISO8601] + self._timestamp_parsers

    @property
    def column_types(self) -> Dict[str, Type[Any]]:
        description = self.description if self.description else []
        return {
            d[0]: dtype
            for d in description
            if (dtype := self._converter.get_dtype(d[1], d[4], d[5])) is not None
        }

    @property
    def converters(self) -> Dict[str, Callable[[Optional[str]], Optional[Any]]]:
        description = self.description if self.description else []
        return {d[0]: self._converter.get(d[1]) for d in description}

    def _fetch(self) -> None:
        try:
            rows = next(self._batches)
        except StopIteration:
            return
        else:
            dict_rows = rows.to_pydict()
            column_names = dict_rows.keys()
            processed_rows = [
                tuple(self.converters[k](v) for k, v in zip(column_names, row, strict=False))
                for row in zip(*dict_rows.values(), strict=False)
            ]
            self._rows.extend(processed_rows)

    def fetchone(
        self,
    ) -> Optional[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        if not self._rows:
            self._fetch()
        if not self._rows:
            return None
        if self._rownumber is None:
            self._rownumber = 0
        self._rownumber += 1
        return self._rows.popleft()

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

    def _read_csv(self) -> "Table":
        import pyarrow as pa
        from pyarrow import csv

        if not self.output_location:
            raise ProgrammingError("OutputLocation is none or empty.")
        if not self.output_location.endswith((".csv", ".txt")):
            return pa.Table.from_pydict({})
        if self.substatement_type and self.substatement_type.upper() in (
            "UPDATE",
            "DELETE",
            "MERGE",
            "VACUUM_TABLE",
        ):
            return pa.Table.from_pydict({})
        length = self._get_content_length()
        if length and self.output_location.endswith(".txt"):
            description = self.description if self.description else []
            column_names = [d[0] for d in description]
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
        elif length and self.output_location.endswith(".csv"):
            read_opts = csv.ReadOptions(skip_rows=0, block_size=self._block_size, use_threads=True)
            parse_opts = csv.ParseOptions(
                delimiter=",",
                quote_char='"',
                double_quote=True,
                escape_char=False,
            )
        else:
            return pa.Table.from_pydict({})

        bucket, key = parse_output_location(self.output_location)
        try:
            return csv.read_csv(
                self._fs.open_input_stream(f"{bucket}/{key}"),
                read_options=read_opts,
                parse_options=parse_opts,
                convert_options=csv.ConvertOptions(
                    quoted_strings_can_be_null=False,
                    timestamp_parsers=self.timestamp_parsers,
                    column_types=self.column_types,
                ),
            )
        except Exception as e:
            _logger.exception(f"Failed to read {bucket}/{key}.")
            raise OperationalError(*e.args) from e

    def _read_parquet(self) -> "Table":
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

    def _as_arrow(self) -> "Table":
        if self.is_unload:
            table = self._read_parquet()
            self._metadata = to_column_info(table.schema)
        else:
            table = self._read_csv()
        return table

    def _as_arrow_from_api(self, converter: Optional[Converter] = None) -> "Table":
        """Build an Arrow Table from GetQueryResults API.

        Used as a fallback when ``output_location`` is not available
        (e.g. managed query result storage).

        Args:
            converter: Type converter for result values. Defaults to
                ``DefaultTypeConverter`` if not specified.
        """
        import pyarrow as pa

        rows = self._fetch_all_rows(converter)
        if not rows:
            return pa.Table.from_pydict({})
        description = self.description if self.description else []
        columns = [d[0] for d in description]
        return pa.table(self._rows_to_columnar(rows, columns))

    def as_arrow(self) -> "Table":
        return self._table

    def as_polars(self) -> "pl.DataFrame":
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

    def close(self) -> None:
        import pyarrow as pa

        super().close()
        self._table = pa.Table.from_pydict({})
        self._batches = []
