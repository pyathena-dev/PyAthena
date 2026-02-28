from __future__ import annotations

import logging
from io import TextIOWrapper
from typing import TYPE_CHECKING, Any

from fsspec import AbstractFileSystem

from pyathena.converter import Converter
from pyathena.error import OperationalError, ProgrammingError
from pyathena.filesystem.s3 import S3FileSystem
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet
from pyathena.s3fs.reader import AthenaCSVReader, DefaultCSVReader
from pyathena.util import RetryConfig, parse_output_location

if TYPE_CHECKING:
    from pyathena.connection import Connection

CSVReaderType = type[DefaultCSVReader] | type[AthenaCSVReader]

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
        result_set_type_hints: dict[str, str] | None = None,
        **kwargs,
    ) -> None:
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
        self._block_size = block_size if block_size else self.DEFAULT_BLOCK_SIZE
        self._csv_reader_class: CSVReaderType = csv_reader or AthenaCSVReader
        self._filesystem_class: type[AbstractFileSystem] = filesystem_class or S3FileSystem
        self._fs = self._create_s3_file_system()
        self._csv_reader: Any | None = None

        if self.state == AthenaQueryExecution.STATE_SUCCEEDED and self.output_location:
            self._init_csv_reader()
        elif self.state == AthenaQueryExecution.STATE_SUCCEEDED:
            # Managed query result storage: no output_location, use API
            rows = self._fetch_all_rows()
            self._rows.extend(rows)

        # If CSV reader was not initialized (e.g., CTAS, DDL),
        # fall back to pre-fetched data from Athena API
        if not self._csv_reader and not self._rows and pre_fetched_rows:
            self._rows.extend(pre_fetched_rows)

    def _create_s3_file_system(self) -> AbstractFileSystem:
        """Create S3FileSystem using connection settings."""
        return self._filesystem_class(
            connection=self.connection,
            default_block_size=self._block_size,
        )

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
            csv_file = self._fs._open(path, mode="rb")
            text_wrapper = TextIOWrapper(csv_file, encoding="utf-8")

            if self.output_location.endswith(".txt"):
                # Tab-separated format (no header row)
                self._csv_reader = self._csv_reader_class(text_wrapper, delimiter="\t")
            else:
                # Standard CSV format (has header row, skip it)
                self._csv_reader = self._csv_reader_class(text_wrapper, delimiter=",")
                next(self._csv_reader)

        except Exception as e:
            _logger.exception("Failed to open %s.", path)
            raise OperationalError(*e.args) from e

    def _fetch(self) -> None:
        """Fetch next batch of rows from CSV."""
        if not self._csv_reader:
            return

        description = self.description if self.description else []
        column_types = [d[1] for d in description]
        column_names = [d[0] for d in description]
        hints = self._result_set_type_hints

        rows_fetched = 0
        while rows_fetched < self._arraysize:
            try:
                row = next(self._csv_reader)
            except StopIteration:
                break

            # Convert row values using converters
            # AthenaCSVReader returns None for NULL values directly,
            # DefaultCSVReader returns empty string which needs conversion
            if self._csv_reader_class is DefaultCSVReader:
                converted_row = tuple(
                    self._converter.convert(
                        col_type,
                        value if value != "" else None,
                        type_hint=hints.get(col_name) if hints else None,
                    )
                    for col_type, col_name, value in zip(
                        column_types, column_names, row, strict=False
                    )
                )
            else:
                converted_row = tuple(
                    self._converter.convert(
                        col_type,
                        value,
                        type_hint=hints.get(col_name) if hints else None,
                    )
                    for col_type, col_name, value in zip(
                        column_types, column_names, row, strict=False
                    )
                )
            self._rows.append(converted_row)
            rows_fetched += 1

    def fetchone(
        self,
    ) -> tuple[Any | None, ...] | dict[Any, Any | None] | None:
        """Fetch the next row of the result set.

        Returns:
            A tuple representing the next row, or None if no more rows.
        """
        if not self._rows:
            self._fetch()
        if not self._rows:
            return None
        if self._rownumber is None:
            self._rownumber = 0
        self._rownumber += 1
        return self._rows.popleft()

    def fetchmany(
        self, size: int | None = None
    ) -> list[tuple[Any | None, ...] | dict[Any, Any | None]]:
        """Fetch the next set of rows of the result set.

        Args:
            size: Maximum number of rows to fetch. Defaults to arraysize.

        Returns:
            A list of tuples representing the rows.
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
    ) -> list[tuple[Any | None, ...] | dict[Any, Any | None]]:
        """Fetch all remaining rows of the result set.

        Returns:
            A list of tuples representing all remaining rows.
        """
        rows = []
        while True:
            row = self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def close(self) -> None:
        """Close the result set and release resources."""
        super().close()
        if self._csv_reader:
            self._csv_reader.close()
            self._csv_reader = None
