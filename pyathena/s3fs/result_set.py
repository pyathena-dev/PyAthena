# Copyright 2017 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Result set that reads Athena CSV query results through an fsspec filesystem."""

from __future__ import annotations

import logging
from io import TextIOWrapper
from itertools import repeat
from typing import TYPE_CHECKING, Any, ClassVar

from fsspec import AbstractFileSystem

from pyathena.converter import Converter
from pyathena.error import OperationalError, ProgrammingError
from pyathena.filesystem.s3 import S3FileSystem
from pyathena.filesystem.s3_async import AioS3FileSystem
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet
from pyathena.s3fs.reader import AthenaCSVReader, CSVReader
from pyathena.util import RetryConfig, override, parse_output_location

if TYPE_CHECKING:
    from pyathena.connection import Connection

CSVReaderType = type[CSVReader]

_logger = logging.getLogger(__name__)


class AthenaS3FSResultSet(AthenaResultSet):
    """Result set that reads CSV results via S3FileSystem without pandas/pyarrow.

    This result set uses PyAthena's S3FileSystem to read query results from S3.
    It provides a lightweight alternative to pandas and arrow cursors when those
    dependencies are not needed.

    Features:
        - Lightweight CSV parsing via pluggable readers
        - Uses PyAthena's S3FileSystem for S3 access
        - No external dependencies beyond boto3
        - Memory-efficient streaming for large datasets

    Attributes:
        DEFAULT_BLOCK_SIZE: Default block size for S3 operations (128MB).

    Example:
        >>> # Used automatically by S3FSCursor
        >>> cursor = connection.cursor(S3FSCursor)
        >>> cursor.execute("SELECT * FROM my_table")
        >>>
        >>> # Fetch results
        >>> rows = cursor.fetchall()

    Note:
        This class is used internally by S3FSCursor and typically not
        instantiated directly by users.
    """

    DEFAULT_FETCH_SIZE: int = 1000
    DEFAULT_BLOCK_SIZE = 1024 * 1024 * 128
    # The block size of a CSV result file read with the background cache; see
    # _get_csv_open_options().
    CSV_BLOCK_SIZE: ClassVar[int] = 32 * 2**20

    def __init__(
        self,
        connection: Connection[Any],
        converter: Converter,
        query_execution: AthenaQueryExecution,
        arraysize: int,
        retry_config: RetryConfig,
        block_size: int | None = None,
        csv_reader: CSVReaderType | None = None,
        filesystem_class: type[AbstractFileSystem] | None = None,
        result_set_type_hints: dict[str | int, str] | None = None,
        **kwargs,
    ) -> None:
        """Initialize the result set and prepare to read the query results.

        Args:
            connection: The connection that ran the query.
            converter: The converter for result values.
            query_execution: The query execution whose results to read.
            arraysize: The number of rows read from the CSV results per fetch and the
                default ``fetchmany()`` size.
            retry_config: The retry configuration for API calls.
            block_size: The default block size in bytes for the filesystem. If not set,
                ``DEFAULT_BLOCK_SIZE`` is used, and PyAthena's ``S3FileSystem`` or
                ``AioS3FileSystem`` reads the CSV result file in 32 MiB blocks with fsspec's
                background cache, which reads the next block while rows are converted.
            csv_reader: The CSV reader class for the results. If None,
                ``AthenaCSVReader`` is used. Custom readers must satisfy the
                :class:`~pyathena.s3fs.reader.CSVReader` protocol.
            filesystem_class: The filesystem class for reading the results. If None,
                PyAthena's ``S3FileSystem`` is used.
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
            arraysize=1,  # Fetch one row to retrieve metadata
            retry_config=retry_config,
            result_set_type_hints=result_set_type_hints,
        )
        # Save pre-fetched rows (from Athena API) in case CSV reading is not available
        pre_fetched_rows = list(self._rows)
        self._rows.clear()
        self._arraysize = arraysize
        self._block_size = block_size
        self._csv_reader_class: CSVReaderType = csv_reader or AthenaCSVReader
        self._filesystem_class: type[AbstractFileSystem] = filesystem_class or S3FileSystem
        self._fs = self._create_s3_file_system()
        self._csv_reader: CSVReader | None = None

        if self.state == AthenaQueryExecution.STATE_SUCCEEDED and self.output_location:
            self._init_csv_reader()
        elif self.state == AthenaQueryExecution.STATE_SUCCEEDED:
            # Managed query result storage: no output_location, use API.
            # The converter reads text values, as from the CSV result file.
            rows = self._fetch_all_rows(self._converter)
            self._rows.extend(rows)

        # If CSV reader was not initialized (e.g., CTAS, DDL),
        # fall back to pre-fetched data from Athena API
        if not self._csv_reader and not self._rows and pre_fetched_rows:
            self._rows.extend(pre_fetched_rows)

    def _create_s3_file_system(self) -> AbstractFileSystem:
        """Create S3FileSystem using connection settings."""
        # Not cached by fsspec so that the connection and the dircache are
        # released with the result set.
        return self._filesystem_class(
            connection=self.connection,
            default_block_size=self._block_size or self.DEFAULT_BLOCK_SIZE,
            skip_instance_cache=True,
        )

    def _get_csv_open_options(self) -> dict[str, Any]:
        """Get the options to open the CSV result file with the filesystem.

        Without ``block_size`` from the cursor, PyAthena's ``S3FileSystem`` or
        ``AioS3FileSystem`` reads the file with fsspec's background cache in blocks
        of ``CSV_BLOCK_SIZE`` bytes: the next block is read while rows are
        converted, and two blocks are kept. Otherwise, and for another
        ``filesystem_class``, including a subclass, the filesystem's settings
        apply. Closing a partly read file waits for the block being read;
        the asyncio cursors close result sets in a thread.

        Returns:
            The keyword arguments for the filesystem's ``_open()``.
        """
        if self._block_size or type(self._fs) not in (S3FileSystem, AioS3FileSystem):
            return {}
        return {
            "block_size": self.CSV_BLOCK_SIZE,
            "cache_type": "background",
            "cache_options": {"maxblocks": 2},
        }

    def _init_csv_reader(self) -> None:
        """Initialize CSV reader for the output file."""
        if not self.output_location:
            raise ProgrammingError("OutputLocation is none or empty.")

        if not self.output_location.endswith((".csv", ".txt")):
            return

        # Skip for UPDATE/DELETE/MERGE/VACUUM operations
        if self.substatement_type and self.substatement_type.upper() in (
            "UPDATE",
            "DELETE",
            "MERGE",
            "VACUUM_TABLE",
        ):
            return

        length = self._get_content_length()
        if not length:
            return

        bucket, key = parse_output_location(self.output_location)
        path = f"{bucket}/{key}"

        try:
            csv_file = self._fs._open(path, mode="rb", **self._get_csv_open_options())
            try:
                text_wrapper = TextIOWrapper(csv_file, encoding="utf-8")

                if self.output_location.endswith(".txt"):
                    # Tab-separated format (no header row)
                    csv_reader = self._csv_reader_class(text_wrapper, delimiter="\t")
                else:
                    # Standard CSV format (has header row, skip it)
                    csv_reader = self._csv_reader_class(text_wrapper, delimiter=",")
                    next(csv_reader)
            except BaseException:
                # Also on KeyboardInterrupt, as nobody else can close the file.
                try:
                    csv_file.close()
                except Exception:
                    _logger.exception(f"Failed to close {path}.")
                raise
            self._csv_reader = csv_reader

        except Exception as e:
            _logger.exception(f"Failed to open {path}.")
            raise OperationalError(*e.args) from e

    @override
    def _fetch(self) -> None:
        """Fetch next batch of rows from CSV."""
        if not self._csv_reader:
            return

        col_names = self._column_names
        col_types = self._column_types
        if not col_names or not col_types:
            description = self.description if self.description else []
            col_names = tuple(d[0] for d in description)
            col_types = tuple(d[1] for d in description)
        col_hints = self._column_type_hints or repeat(None)

        rows_fetched = 0
        while rows_fetched < self._arraysize:
            try:
                row = next(self._csv_reader)
            except StopIteration:
                # Release the file and its read-ahead at the end of the results.
                self._csv_reader.close()
                self._csv_reader = None
                break

            self._rows.append(
                tuple(self._convert_values(self._converter, row, col_names, col_types, col_hints))
            )
            rows_fetched += 1

    @override
    def close(self) -> None:
        """Close the result set and release resources."""
        super().close()
        if self._csv_reader:
            self._csv_reader.close()
            self._csv_reader = None
