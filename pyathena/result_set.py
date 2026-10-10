"""Result sets for ``GetQueryResults`` and the cursor mixins that expose them."""

from __future__ import annotations

import collections
import logging
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)

from pyathena.common import BaseCursor, CursorIterator
from pyathena.converter import Converter, DefaultTypeConverter
from pyathena.error import DataError, OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.util import RetryConfig, override, parse_output_location, retry_api_call

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)


class AthenaResultSet(CursorIterator):
    """Result set for Athena query execution using the GetQueryResults API.

    This class provides a DB API 2.0 compliant result set implementation that
    fetches query results from Amazon Athena. It uses the GetQueryResults API
    to retrieve data in paginated chunks, converting each value according to
    its Athena data type.

    The result set exposes query execution metadata (timing, data scanned,
    state, etc.) through read-only properties, allowing inspection of query
    performance and status.

    This is the base result set implementation used by the standard Cursor.
    Specialized implementations exist for different output formats:
        - :class:`~pyathena.arrow.result_set.AthenaArrowResultSet`: Apache Arrow format
        - :class:`~pyathena.pandas.result_set.AthenaPandasResultSet`: Pandas DataFrame
        - :class:`~pyathena.s3fs.result_set.AthenaS3FSResultSet`: S3 file-based access

    Example:
        >>> cursor.execute("SELECT * FROM my_table")
        >>> result_set = cursor.result_set
        >>> print(f"Query ID: {result_set.query_id}")
        >>> print(f"Data scanned: {result_set.data_scanned_in_bytes} bytes")
        >>> for row in result_set:
        ...     print(row)

    See Also:
        AWS Athena GetQueryResults API:
        https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryResults.html
    """

    # https://docs.aws.amazon.com/athena/latest/ug/data-types.html
    # Athena complex types that benefit from type hint conversion.
    _COMPLEX_TYPES: frozenset[str] = frozenset({"array", "map", "row", "struct"})
    _DML_SUBSTATEMENT_TYPES: frozenset[str] = frozenset({"INSERT", "UPDATE", "DELETE", "MERGE"})

    def __init__(
        self,
        connection: Connection[Any],
        converter: Converter,
        query_execution: AthenaQueryExecution,
        arraysize: int,
        retry_config: RetryConfig,
        _pre_fetch: bool = True,
        result_set_type_hints: dict[str | int, str] | None = None,
    ) -> None:
        """Initialize the result set and fetch the first page if the query succeeded.

        Args:
            connection: The connection that ran the query.
            converter: The converter for result values.
            query_execution: The query execution whose results to read.
            arraysize: The number of rows per ``GetQueryResults`` page and the default
                ``fetchmany()`` size.
            retry_config: The retry configuration for API calls.
            _pre_fetch: Whether to fetch the first page here when the query succeeded.
                The async result set passes False and fetches it itself.
            result_set_type_hints: Athena type signatures for complex-type columns,
                keyed by column name (case-insensitive) or zero-based column index.

        Raises:
            ProgrammingError: If ``query_execution`` is not given.
            OperationalError: If fetching the first page fails.
        """
        super().__init__(arraysize=arraysize)
        self._connection: Connection[Any] | None = connection
        self._converter = converter
        self._query_execution: AthenaQueryExecution | None = query_execution
        if not self._query_execution:
            raise ProgrammingError("Required argument `query_execution` not found.")
        self._retry_config = retry_config
        self._hints_by_name: dict[str, str] = {}
        self._hints_by_index: dict[int, str] = {}
        if result_set_type_hints:
            for k, v in result_set_type_hints.items():
                if isinstance(k, int):
                    self._hints_by_index[k] = v
                else:
                    self._hints_by_name[k.lower()] = v

        self._metadata: tuple[dict[str, Any], ...] | None = None
        self._column_types: tuple[str, ...] | None = None
        self._column_names: tuple[str, ...] | None = None
        self._column_type_hints: tuple[str | None, ...] | None = None
        self._rows: collections.deque[tuple[Any | None, ...] | dict[Any, Any | None]] = (
            collections.deque()
        )
        self._next_token: str | None = None

        if self.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self._rownumber = 0
            if _pre_fetch:
                self._pre_fetch()

    @property
    def database(self) -> str | None:
        """The database in the ``QueryExecutionContext`` of the query."""
        if not self._query_execution:
            return None
        return self._query_execution.database

    @property
    def catalog(self) -> str | None:
        """The data catalog in the ``QueryExecutionContext`` of the query."""
        if not self._query_execution:
            return None
        return self._query_execution.catalog

    @property
    def query_id(self) -> str | None:
        """The ID of the query execution."""
        if not self._query_execution:
            return None
        return self._query_execution.query_id

    @property
    def query(self) -> str | None:
        """The SQL statement that the query execution ran."""
        if not self._query_execution:
            return None
        return self._query_execution.query

    @property
    def statement_type(self) -> str | None:
        """The ``StatementType`` of the query, such as ``DDL``, ``DML``, or ``UTILITY``."""
        if not self._query_execution:
            return None
        return self._query_execution.statement_type

    @property
    def substatement_type(self) -> str | None:
        """The ``SubstatementType`` of the query, such as ``INSERT`` or ``MERGE``."""
        if not self._query_execution:
            return None
        return self._query_execution.substatement_type

    @property
    def work_group(self) -> str | None:
        """The work group in which the query ran."""
        if not self._query_execution:
            return None
        return self._query_execution.work_group

    @property
    def execution_parameters(self) -> list[str]:
        """The ``ExecutionParameters`` values of the query."""
        if not self._query_execution:
            return []
        return self._query_execution.execution_parameters

    @property
    def state(self) -> str | None:
        """The state of the query execution, such as ``RUNNING`` or ``SUCCEEDED``."""
        if not self._query_execution:
            return None
        return self._query_execution.state

    @property
    def state_change_reason(self) -> str | None:
        """The ``StateChangeReason`` that gives further detail about the state."""
        if not self._query_execution:
            return None
        return self._query_execution.state_change_reason

    @property
    def submission_date_time(self) -> datetime | None:
        """The date and time when the query was submitted."""
        if not self._query_execution:
            return None
        return self._query_execution.submission_date_time

    @property
    def completion_date_time(self) -> datetime | None:
        """The date and time when the query completed."""
        if not self._query_execution:
            return None
        return self._query_execution.completion_date_time

    @property
    def error_category(self) -> int | None:
        """The ``ErrorCategory`` of the failure: 1 for system, 2 for user, 3 for other."""
        if not self._query_execution:
            return None
        return self._query_execution.error_category

    @property
    def error_type(self) -> int | None:
        """The ``ErrorType`` code of the query failure."""
        if not self._query_execution:
            return None
        return self._query_execution.error_type

    @property
    def retryable(self) -> bool | None:
        """Whether Athena reports the query failure as retryable."""
        if not self._query_execution:
            return None
        return self._query_execution.retryable

    @property
    def error_message(self) -> str | None:
        """The ``ErrorMessage`` that describes the query failure."""
        if not self._query_execution:
            return None
        return self._query_execution.error_message

    @property
    def data_scanned_in_bytes(self) -> int | None:
        """The number of bytes that the query scanned."""
        if not self._query_execution:
            return None
        return self._query_execution.data_scanned_in_bytes

    @property
    def engine_execution_time_in_millis(self) -> int | None:
        """The time in milliseconds that the query engine took to run the query."""
        if not self._query_execution:
            return None
        return self._query_execution.engine_execution_time_in_millis

    @property
    def query_queue_time_in_millis(self) -> int | None:
        """The time in milliseconds that the query waited in the queue."""
        if not self._query_execution:
            return None
        return self._query_execution.query_queue_time_in_millis

    @property
    def total_execution_time_in_millis(self) -> int | None:
        """The total time in milliseconds that Athena took to run the query."""
        if not self._query_execution:
            return None
        return self._query_execution.total_execution_time_in_millis

    @property
    def query_planning_time_in_millis(self) -> int | None:
        """The time in milliseconds that Athena took to plan the query."""
        if not self._query_execution:
            return None
        return self._query_execution.query_planning_time_in_millis

    @property
    def service_processing_time_in_millis(self) -> int | None:
        """The time in milliseconds that Athena took to publish the query results."""
        if not self._query_execution:
            return None
        return self._query_execution.service_processing_time_in_millis

    @property
    def output_location(self) -> str | None:
        """The S3 location of the query results."""
        if not self._query_execution:
            return None
        return self._query_execution.output_location

    @property
    def data_manifest_location(self) -> str | None:
        """The S3 location of the data manifest that lists the files the query wrote."""
        if not self._query_execution:
            return None
        return self._query_execution.data_manifest_location

    @property
    def reused_previous_result(self) -> bool | None:
        """Whether Athena reused a previous query result instead of running the query."""
        if not self._query_execution:
            return None
        return self._query_execution.reused_previous_result

    @property
    def is_unload(self) -> bool:
        """Check if the query is an UNLOAD statement.

        Returns:
            True if the query is an UNLOAD statement, False otherwise.
        """
        return bool(
            getattr(self, "_unload", False)
            and self.query
            and self.query.strip().upper().startswith("UNLOAD")
        )

    @property
    def encryption_option(self) -> str | None:
        """The ``EncryptionOption`` of the query results, such as ``SSE_S3`` or ``SSE_KMS``."""
        if not self._query_execution:
            return None
        return self._query_execution.encryption_option

    @property
    def kms_key(self) -> str | None:
        """The KMS key used to encrypt the query results."""
        if not self._query_execution:
            return None
        return self._query_execution.kms_key

    @property
    def expected_bucket_owner(self) -> str | None:
        """The AWS account ID expected to own the S3 bucket of the query results."""
        if not self._query_execution:
            return None
        return self._query_execution.expected_bucket_owner

    @property
    def s3_acl_option(self) -> str | None:
        """The ``S3AclOption`` of the query results, such as ``BUCKET_OWNER_FULL_CONTROL``."""
        if not self._query_execution:
            return None
        return self._query_execution.s3_acl_option

    @property
    def selected_engine_version(self) -> str | None:
        """The Athena engine version selected to run the query."""
        if not self._query_execution:
            return None
        return self._query_execution.selected_engine_version

    @property
    def effective_engine_version(self) -> str | None:
        """The Athena engine version that ran the query."""
        if not self._query_execution:
            return None
        return self._query_execution.effective_engine_version

    @property
    def result_reuse_enabled(self) -> bool | None:
        """Whether reuse of previous query results by age is enabled for the query."""
        if not self._query_execution:
            return None
        return self._query_execution.result_reuse_enabled

    @property
    def result_reuse_minutes(self) -> int | None:
        """The maximum age in minutes of a previous query result that Athena can reuse."""
        if not self._query_execution:
            return None
        return self._query_execution.result_reuse_minutes

    @property
    def description(
        self,
    ) -> list[tuple[str, str, None, None, int, int, str]] | None:
        """The DB API 2.0 column descriptions.

        None without result metadata, or for ``INSERT``, ``UPDATE``, ``DELETE``, and ``MERGE``.
        """
        if self._metadata is None or (
            self.substatement_type
            and self.substatement_type.upper() in self._DML_SUBSTATEMENT_TYPES
        ):
            return None
        return [
            (
                m["Name"],
                m["Type"],
                None,
                None,
                m["Precision"],
                m["Scale"],
                m["Nullable"],
            )
            for m in self._metadata
        ]

    @property
    def connection(self) -> Connection[Any]:
        """The connection of the result set; raises ``ProgrammingError`` if closed."""
        if self.is_closed:
            raise ProgrammingError("AthenaResultSet is closed.")
        return cast("Connection[Any]", self._connection)

    def _build_get_query_results_request(
        self, max_results: int, next_token: str | None = None
    ) -> dict[str, Any]:
        """Build a ``GetQueryResults`` request for the result set's query.

        Args:
            max_results: The maximum number of rows in the page.
            next_token: The token of the page to get; the first page if None.

        Returns:
            The request parameters.

        Raises:
            ProgrammingError: If the query ID is missing or the query has not
                succeeded.
        """
        if not self.query_id:
            raise ProgrammingError("QueryExecutionId is none or empty.")
        if self.state != AthenaQueryExecution.STATE_SUCCEEDED:
            raise ProgrammingError("QueryExecutionState is not SUCCEEDED.")
        request: dict[str, Any] = {
            "QueryExecutionId": self.query_id,
            "MaxResults": max_results,
        }
        if next_token:
            request["NextToken"] = next_token
        return request

    def _get_query_results(self, max_results: int, next_token: str | None = None) -> dict[str, Any]:
        """Get a page of query results with ``GetQueryResults``.

        Args:
            max_results: The maximum number of rows in the page.
            next_token: The token of the page to get; the first page if None.

        Returns:
            The ``GetQueryResults`` response.

        Raises:
            ProgrammingError: If the query ID is missing, the query has not
                succeeded, or the result set is closed.
            OperationalError: If the request fails.
        """
        request = self._build_get_query_results_request(max_results, next_token)
        if self.is_closed:
            raise ProgrammingError("AthenaResultSet is closed.")
        try:
            response = retry_api_call(
                self.connection.client.get_query_results,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to fetch result set.")
            raise OperationalError(*e.args) from e
        else:
            return cast(dict[str, Any], response)

    def _fetch(self) -> None:
        """Fetch the next page of rows into the result set.

        Raises:
            ProgrammingError: If there is no next page.
            OperationalError: If the request fails.
        """
        if not self._next_token:
            raise ProgrammingError("NextToken is none or empty.")
        response = self._get_query_results(self._arraysize, self._next_token)
        rows, self._next_token = self._parse_result_rows(response)
        self._process_rows(rows)

    def _pre_fetch(self) -> None:
        """Fetch the first page of rows along with the result metadata.

        Raises:
            ProgrammingError: If the query ID is missing, the query has not
                succeeded, or the result set is closed.
            OperationalError: If the request fails.
        """
        response = self._get_query_results(self._arraysize)
        self._process_metadata(response)
        self._process_update_count(response)
        rows, self._next_token = self._parse_result_rows(response)
        offset = 1 if rows and self._is_first_row_column_labels(rows) else 0
        self._process_rows(rows, offset)

    @override
    def fetchone(
        self,
    ) -> tuple[Any | None, ...] | dict[Any, Any | None] | None:
        if not self._rows and self._next_token:
            self._fetch()
        if not self._rows:
            return None
        if self._rownumber is None:
            self._rownumber = 0
        self._rownumber += 1
        return self._rows.popleft()

    @override
    def fetchmany(
        self, size: int | None = None
    ) -> list[tuple[Any | None, ...] | dict[Any, Any | None]]:
        """Fetch the next set of rows of the query result.

        Args:
            size: Maximum number of rows to fetch. If None or not positive,
                ``arraysize`` is used.

        Returns:
            The rows, fewer than ``size`` when the result is exhausted.
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

    @override
    def fetchall(
        self,
    ) -> list[tuple[Any | None, ...] | dict[Any, Any | None]]:
        """Fetch all remaining rows of the query result.

        Returns:
            The remaining rows.
        """
        rows = []
        while True:
            row = self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def _process_metadata(self, response: dict[str, Any]) -> None:
        result_set = response.get("ResultSet")
        if not result_set:
            raise DataError("KeyError `ResultSet`")
        metadata = result_set.get("ResultSetMetadata")
        if not metadata:
            raise DataError("KeyError `ResultSetMetadata`")
        column_info = metadata.get("ColumnInfo")
        if column_info is None:
            raise DataError("KeyError `ColumnInfo`")
        self._metadata = tuple(column_info)
        self._column_types = tuple(m.get("Type", "") for m in self._metadata)
        self._column_names = tuple(m.get("Name", "") for m in self._metadata)
        if (self._hints_by_name or self._hints_by_index) and any(
            t.lower() in self._COMPLEX_TYPES for t in self._column_types
        ):
            hints = tuple(
                self._resolve_type_hint(i, m.get("Name", "").lower(), t.lower())
                for i, (m, t) in enumerate(zip(self._metadata, self._column_types, strict=True))
            )
            if any(hints):
                self._column_type_hints = hints

    def _resolve_type_hint(
        self, index: int, col_name_lower: str, col_type_lower: str
    ) -> str | None:
        """Look up the type hint for a column by index then by name.

        Index-based hints take priority over name-based hints, allowing
        callers to disambiguate duplicate column names.

        Args:
            index: Zero-based column position.
            col_name_lower: Lowercased column name from metadata.
            col_type_lower: Lowercased column type from metadata.

        Returns:
            The type hint string, or None if the column has no hint or
            is not a complex type.
        """
        if col_type_lower not in self._COMPLEX_TYPES:
            return None
        hint = self._hints_by_index.get(index)
        if hint is not None:
            return hint
        return self._hints_by_name.get(col_name_lower)

    def _process_update_count(self, response: dict[str, Any]) -> None:
        update_count = response.get("UpdateCount")
        if (
            update_count is not None
            and self.substatement_type
            and self.substatement_type.upper()
            in (
                "INSERT",
                "UPDATE",
                "DELETE",
                "MERGE",
                "CREATE_TABLE_AS_SELECT",
            )
        ):
            self._rowcount = update_count

    def _get_rows(
        self,
        offset: int,
        metadata: tuple[Any, ...],
        rows: list[dict[str, Any]],
        converter: Converter | None = None,
    ) -> list[tuple[Any | None, ...] | dict[Any, Any | None]]:
        conv = converter or self._converter
        col_types = self._column_types
        col_hints = self._column_type_hints
        if col_hints and col_types:
            return [
                tuple(
                    conv.convert(col_type, row.get("VarCharValue"), type_hint=hint)
                    if hint
                    else conv.convert(col_type, row.get("VarCharValue"))
                    for col_type, row, hint in zip(
                        col_types, rows[i].get("Data", []), col_hints, strict=False
                    )
                )
                for i in range(offset, len(rows))
            ]
        if col_types:
            return [
                tuple(
                    conv.convert(col_type, row.get("VarCharValue"))
                    for col_type, row in zip(col_types, rows[i].get("Data", []), strict=False)
                )
                for i in range(offset, len(rows))
            ]
        return [
            tuple(
                conv.convert(meta.get("Type"), row.get("VarCharValue"))
                for meta, row in zip(metadata, rows[i].get("Data", []), strict=False)
            )
            for i in range(offset, len(rows))
        ]

    def _parse_result_rows(
        self, response: dict[str, Any]
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Parse a GetQueryResults response into raw rows and next token.

        Handles response validation and pagination token extraction.
        This is the shared parsing logic used by both ``_pre_fetch``
        (normal path) and ``_fetch_all_rows`` (API fallback).

        Args:
            response: Raw response dict from ``GetQueryResults`` API.

        Returns:
            Tuple of (rows, next_token).
        """
        result_set = response.get("ResultSet")
        if not result_set:
            raise DataError("KeyError `ResultSet`")
        rows = result_set.get("Rows")
        if rows is None:
            raise DataError("KeyError `Rows`")
        next_token = response.get("NextToken")
        return rows, next_token

    def _process_rows(self, rows: list[dict[str, Any]], offset: int = 0) -> None:
        if rows and self._metadata:
            processed_rows = self._get_rows(offset, self._metadata, rows)
            self._rows.extend(processed_rows)

    def _is_first_row_column_labels(self, rows: list[dict[str, Any]]) -> bool:
        first_row_data = rows[0].get("Data", [])
        for meta, data in zip(self._metadata or (), first_row_data, strict=False):
            if meta.get("Name") != data.get("VarCharValue"):
                return False
        return True

    def _iter_all_row_pages(self) -> Iterator[tuple[list[dict[str, Any]], int]]:
        """Fetch all rows via GetQueryResults API from the beginning.

        Paginates through all results using MaxResults=1000. This is for subclass result
        sets that need to fall back to the API when S3 output is not available (e.g.,
        managed query result storage).

        Yields:
            The rows of each page and the offset of their first data row, which is 1 when
            the first page starts with the column labels.
        """
        _logger.warning(
            "output_location is not available (e.g. managed query result storage). "
            "Falling back to GetQueryResults API. "
            "This may be slow for large result sets."
        )

        next_token: str | None = None
        first_page = True
        while True:
            response = self._get_query_results(self.DEFAULT_FETCH_SIZE, next_token)
            rows, next_token = self._parse_result_rows(response)
            # Only the first page starts with the column labels.
            offset = 1 if first_page and rows and self._is_first_row_column_labels(rows) else 0
            first_page = False
            yield rows, offset
            if not next_token:
                break

    def _fetch_all_rows(
        self,
        converter: Converter | None = None,
    ) -> list[tuple[Any | None, ...]]:
        """Fetch all rows via GetQueryResults API with type conversion.

        Args:
            converter: Type converter for result values. Defaults to
                ``DefaultTypeConverter`` if not specified.

        Returns:
            List of converted row tuples.

        Raises:
            ProgrammingError: If the metadata is not available.
        """
        metadata = self._metadata
        if metadata is None:
            raise ProgrammingError("Metadata is not available.")
        converter = converter or DefaultTypeConverter()
        all_rows: list[tuple[Any | None, ...]] = []
        for rows, offset in self._iter_all_row_pages():
            all_rows.extend(
                cast(
                    list[tuple[Any | None, ...]],
                    self._get_rows(offset, metadata, rows, converter),
                )
            )
        return all_rows

    def _fetch_all_rows_as_csv(self) -> bytes:
        """Fetch all rows via GetQueryResults API as the text of a CSV result file.

        GetQueryResults returns the same text for each value as the CSV result file
        has, and this writes it in the same format: a header of the column labels,
        each value quoted with its quotes doubled, NULL as an empty field, and a line
        feed after each row. The cursors that read CSV result files can then read it
        as one.

        Returns:
            The CSV text encoded in UTF-8, or empty bytes when the result has no columns.

        Raises:
            ProgrammingError: If the metadata is not available.
        """
        if self._metadata is None:
            raise ProgrammingError("Metadata is not available.")
        description = self.description if self.description else []
        if not description:
            return b""
        lines = [",".join('"' + d[0].replace('"', '""') + '"' for d in description)]
        for rows, offset in self._iter_all_row_pages():
            lines.extend(
                ",".join(
                    ""
                    if (value := data.get("VarCharValue")) is None
                    else '"' + value.replace('"', '""') + '"'
                    for data in row.get("Data", [])
                )
                for row in rows[offset:]
            )
        lines.append("")
        return "\n".join(lines).encode("utf-8")

    def _get_content_length(self) -> int:
        if not self.output_location:
            raise ProgrammingError("OutputLocation is none or empty.")
        bucket, key = parse_output_location(self.output_location)
        try:
            response = retry_api_call(
                self.connection.s3_client.head_object,
                config=self._retry_config,
                logger=_logger,
                Bucket=bucket,
                Key=key,
            )
        except Exception as e:
            _logger.exception("Failed to get content length.")
            raise OperationalError(*e.args) from e
        else:
            return cast(int, response["ContentLength"])

    def _read_data_manifest(self) -> list[str]:
        if not self.data_manifest_location:
            raise ProgrammingError("DataManifestLocation is none or empty.")
        bucket, key = parse_output_location(self.data_manifest_location)
        try:
            response = retry_api_call(
                self.connection.s3_client.get_object,
                config=self._retry_config,
                logger=_logger,
                Bucket=bucket,
                Key=key,
            )
        except Exception as e:
            _logger.exception(f"Failed to read {bucket}/{key}.")
            raise OperationalError(*e.args) from e
        else:
            manifest: str = response["Body"].read().decode("utf-8").strip()
            return manifest.split("\n") if manifest else []

    @property
    def is_closed(self) -> bool:
        """Whether the result set is closed."""
        return self._connection is None

    def close(self) -> None:
        """Close the result set and discard its query execution, metadata, and rows."""
        self._connection = None
        self._query_execution = None
        self._metadata = None
        self._column_types = None
        self._column_names = None
        self._rows.clear()
        self._next_token = None
        self._rownumber = None
        self._rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class AthenaDictResultSet(AthenaResultSet):
    """A result set that returns each row as a dictionary keyed by column name."""

    # You can override this to use OrderedDict or other dict-like types.
    dict_type: type[Any] = dict

    def __init__(self, *args: Any, dict_type: type[Any] | None = None, **kwargs: Any) -> None:
        """Initialize the result set with an optional row type for this instance.

        Args:
            *args: Positional arguments passed to the next ``__init__`` in the MRO.
            dict_type: The type used to build each row of this result set. If
                None, the class attribute ``dict_type`` is used.
            **kwargs: Keyword arguments passed to the next ``__init__`` in the MRO.
        """
        if dict_type is not None:
            self.dict_type = dict_type
        super().__init__(*args, **kwargs)

    @override
    def _get_rows(
        self,
        offset: int,
        metadata: tuple[Any, ...],
        rows: list[dict[str, Any]],
        converter: Converter | None = None,
    ) -> list[tuple[Any | None, ...] | dict[Any, Any | None]]:
        conv = converter or self._converter
        col_types = self._column_types
        col_names = self._column_names
        col_hints = self._column_type_hints
        if col_hints and col_types and col_names:
            return [
                self.dict_type(
                    (
                        name,
                        conv.convert(col_type, row.get("VarCharValue"), type_hint=hint)
                        if hint
                        else conv.convert(col_type, row.get("VarCharValue")),
                    )
                    for name, col_type, row, hint in zip(
                        col_names,
                        col_types,
                        rows[i].get("Data", []),
                        col_hints,
                        strict=False,
                    )
                )
                for i in range(offset, len(rows))
            ]
        if col_types and col_names:
            return [
                self.dict_type(
                    (
                        name,
                        conv.convert(col_type, row.get("VarCharValue")),
                    )
                    for name, col_type, row in zip(
                        col_names, col_types, rows[i].get("Data", []), strict=False
                    )
                )
                for i in range(offset, len(rows))
            ]
        return [
            self.dict_type(
                (
                    meta.get("Name"),
                    conv.convert(meta.get("Type"), row.get("VarCharValue")),
                )
                for meta, row in zip(metadata, rows[i].get("Data", []), strict=False)
            )
            for i in range(offset, len(rows))
        ]


class WithResultSet:
    """Mixin that keeps a cursor's query ID and result set.

    Provides the query ID, the result set and its properties, ``arraysize``,
    ``rownumber``, ``rowcount``, and ``close``. ``WithFetch`` and
    ``WithAsyncFetch`` list it before ``BaseCursor`` / ``AioBaseCursor`` and
    ``CursorIterator``, so that these members take precedence over theirs.
    """

    def __init__(self, *, arraysize: int | None = None, **kwargs) -> None:
        """Initialize the cursor with no query ID and no result set.

        Args:
            arraysize: Default number of rows per ``fetchmany()`` call,
                validated by the ``arraysize`` setter. If None,
                ``DEFAULT_FETCH_SIZE`` is used.
            **kwargs: Arguments passed to the next ``__init__`` in the MRO.

        Raises:
            ProgrammingError: If ``arraysize`` is outside the range the
                cursor's ``arraysize`` setter accepts.
        """
        super().__init__(**kwargs)
        if arraysize is not None:
            self.arraysize = arraysize
        self._query_id: str | None = None
        self._result_set: AthenaResultSet | None = None

    def _reset_state(self) -> None:
        self._rowcount = -1
        self.query_id = None
        if self.result_set and not self.result_set.is_closed:
            self.result_set.close()
        self.result_set = None

    @property
    def result_set(self) -> AthenaResultSet | None:
        """The result set of the last executed query.

        Returns:
            The result set, or None before a query succeeds or after a reset.
        """
        return self._result_set

    @result_set.setter
    def result_set(self, val: AthenaResultSet | None) -> None:
        self._result_set = val

    @property
    def has_result_set(self) -> bool:
        """Whether the cursor has a result set."""
        return self.result_set is not None

    @property
    def description(
        self,
    ) -> list[tuple[str, str, None, None, int, int, str]] | None:
        """The DB API 2.0 column descriptions of the result set, or None without one."""
        if not self.result_set:
            return None
        return self.result_set.description

    @property
    def database(self) -> str | None:
        """The database in the ``QueryExecutionContext`` of the query."""
        if not self.result_set:
            return None
        return self.result_set.database

    @property
    def catalog(self) -> str | None:
        """The data catalog in the ``QueryExecutionContext`` of the query."""
        if not self.result_set:
            return None
        return self.result_set.catalog

    @property
    def query_id(self) -> str | None:
        """The query execution ID of the last execution.

        With ``cache_size`` or ``cache_expiration_time``, this can be the ID of
        a previous execution whose result is reused.

        Returns:
            The query execution ID, or None if there is none since the last
            reset.
        """
        return self._query_id

    @query_id.setter
    def query_id(self, val: str | None) -> None:
        self._query_id = val

    def _set_interrupted_execution_id(self, execution_id: str) -> None:
        """Keep the ID of a query started by an interrupted start request.

        Args:
            execution_id: The query execution ID.
        """
        self.query_id = execution_id

    @property
    def query(self) -> str | None:
        """The SQL statement that the query execution ran."""
        if not self.result_set:
            return None
        return self.result_set.query

    @property
    def statement_type(self) -> str | None:
        """The ``StatementType`` of the query, such as ``DDL``, ``DML``, or ``UTILITY``."""
        if not self.result_set:
            return None
        return self.result_set.statement_type

    @property
    def substatement_type(self) -> str | None:
        """The ``SubstatementType`` of the query, such as ``INSERT`` or ``MERGE``."""
        if not self.result_set:
            return None
        return self.result_set.substatement_type

    @property
    def work_group(self) -> str | None:
        """The work group in which the query ran."""
        if not self.result_set:
            return None
        return self.result_set.work_group

    @property
    def execution_parameters(self) -> list[str]:
        """The ``ExecutionParameters`` values of the query."""
        if not self.result_set:
            return []
        return self.result_set.execution_parameters

    @property
    def state(self) -> str | None:
        """The state of the query execution, such as ``RUNNING`` or ``SUCCEEDED``."""
        if not self.result_set:
            return None
        return self.result_set.state

    @property
    def state_change_reason(self) -> str | None:
        """The ``StateChangeReason`` that gives further detail about the state."""
        if not self.result_set:
            return None
        return self.result_set.state_change_reason

    @property
    def submission_date_time(self) -> datetime | None:
        """The date and time when the query was submitted."""
        if not self.result_set:
            return None
        return self.result_set.submission_date_time

    @property
    def completion_date_time(self) -> datetime | None:
        """The date and time when the query completed."""
        if not self.result_set:
            return None
        return self.result_set.completion_date_time

    @property
    def error_category(self) -> int | None:
        """The ``ErrorCategory`` of the failure: 1 for system, 2 for user, 3 for other."""
        if not self.result_set:
            return None
        return self.result_set.error_category

    @property
    def error_type(self) -> int | None:
        """The ``ErrorType`` code of the query failure."""
        if not self.result_set:
            return None
        return self.result_set.error_type

    @property
    def retryable(self) -> bool | None:
        """Whether Athena reports the query failure as retryable."""
        if not self.result_set:
            return None
        return self.result_set.retryable

    @property
    def error_message(self) -> str | None:
        """The ``ErrorMessage`` that describes the query failure."""
        if not self.result_set:
            return None
        return self.result_set.error_message

    @property
    def data_scanned_in_bytes(self) -> int | None:
        """The number of bytes that the query scanned."""
        if not self.result_set:
            return None
        return self.result_set.data_scanned_in_bytes

    @property
    def engine_execution_time_in_millis(self) -> int | None:
        """The time in milliseconds that the query engine took to run the query."""
        if not self.result_set:
            return None
        return self.result_set.engine_execution_time_in_millis

    @property
    def query_queue_time_in_millis(self) -> int | None:
        """The time in milliseconds that the query waited in the queue."""
        if not self.result_set:
            return None
        return self.result_set.query_queue_time_in_millis

    @property
    def total_execution_time_in_millis(self) -> int | None:
        """The total time in milliseconds that Athena took to run the query."""
        if not self.result_set:
            return None
        return self.result_set.total_execution_time_in_millis

    @property
    def query_planning_time_in_millis(self) -> int | None:
        """The time in milliseconds that Athena took to plan the query."""
        if not self.result_set:
            return None
        return self.result_set.query_planning_time_in_millis

    @property
    def service_processing_time_in_millis(self) -> int | None:
        """The time in milliseconds that Athena took to publish the query results."""
        if not self.result_set:
            return None
        return self.result_set.service_processing_time_in_millis

    @property
    def output_location(self) -> str | None:
        """The S3 location of the query results."""
        if not self.result_set:
            return None
        return self.result_set.output_location

    @property
    def data_manifest_location(self) -> str | None:
        """The S3 location of the data manifest that lists the files the query wrote."""
        if not self.result_set:
            return None
        return self.result_set.data_manifest_location

    @property
    def reused_previous_result(self) -> bool | None:
        """Whether Athena reused a previous query result instead of running the query."""
        if not self.result_set:
            return None
        return self.result_set.reused_previous_result

    @property
    def encryption_option(self) -> str | None:
        """The ``EncryptionOption`` of the query results, such as ``SSE_S3`` or ``SSE_KMS``."""
        if not self.result_set:
            return None
        return self.result_set.encryption_option

    @property
    def kms_key(self) -> str | None:
        """The KMS key used to encrypt the query results."""
        if not self.result_set:
            return None
        return self.result_set.kms_key

    @property
    def expected_bucket_owner(self) -> str | None:
        """The AWS account ID expected to own the S3 bucket of the query results."""
        if not self.result_set:
            return None
        return self.result_set.expected_bucket_owner

    @property
    def s3_acl_option(self) -> str | None:
        """The ``S3AclOption`` of the query results, such as ``BUCKET_OWNER_FULL_CONTROL``."""
        if not self.result_set:
            return None
        return self.result_set.s3_acl_option

    @property
    def selected_engine_version(self) -> str | None:
        """The Athena engine version selected to run the query."""
        if not self.result_set:
            return None
        return self.result_set.selected_engine_version

    @property
    def effective_engine_version(self) -> str | None:
        """The Athena engine version that ran the query."""
        if not self.result_set:
            return None
        return self.result_set.effective_engine_version

    @property
    def result_reuse_enabled(self) -> bool | None:
        """Whether reuse of previous query results by age is enabled for the query."""
        if not self.result_set:
            return None
        return self.result_set.result_reuse_enabled

    @property
    def result_reuse_minutes(self) -> int | None:
        """The maximum age in minutes of a previous query result that Athena can reuse."""
        if not self.result_set:
            return None
        return self.result_set.result_reuse_minutes

    @property
    def rowcount(self) -> int:
        """Get the number of rows affected by the last operation.

        For SELECT statements, this returns -1 as per DB API 2.0 specification.
        For DML operations (INSERT, UPDATE, DELETE) and CTAS, this returns
        the number of affected rows. After a successful ``executemany()``,
        this is the sum across executions, or -1 if any count is unknown.

        Returns:
            The number of rows, or -1 if not applicable or unknown.
        """
        return self.result_set.rowcount if self.result_set else self._rowcount

    @property
    def arraysize(self) -> int:
        """The default number of rows per ``fetchmany()`` call.

        ``execute()`` passes it to the new result set, so a change applies to
        the result sets of later executions. Setting it to zero or a negative
        value raises ``ProgrammingError``.

        Returns:
            The default number of rows per ``fetchmany()`` call.
        """
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value <= 0:
            raise ProgrammingError("arraysize must be a positive integer value.")
        self._arraysize = value

    @property
    def rownumber(self) -> int | None:
        """The zero-based index of the next row in the result set.

        Returns:
            The row index, or None if there is no result set or the index is
            unknown.
        """
        return self.result_set.rownumber if self.result_set else None

    def close(self) -> None:
        """Close the cursor and release associated resources."""
        self._rowcount = -1
        if self.result_set and not self.result_set.is_closed:
            self.result_set.close()


class WithFetch(WithResultSet, BaseCursor, CursorIterator):
    """Base class of the sync SQL cursors.

    Combines ``WithResultSet`` with ``BaseCursor`` and ``CursorIterator``, and
    provides sync fetch, ``executemany``, ``cancel``, and sync iteration.

    Subclasses override ``execute()`` and optionally ``__init__`` and
    format-specific helpers.
    """

    @override
    def fetchone(
        self,
    ) -> tuple[Any | None, ...] | dict[Any, Any | None] | None:
        """Fetch the next row of the result set.

        Returns:
            The next row (a tuple, or a dict for dict cursors), or None if no
            more rows.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.fetchone()

    @override
    def fetchmany(
        self, size: int | None = None
    ) -> list[tuple[Any | None, ...] | dict[Any, Any | None]]:
        """Fetch multiple rows from the result set.

        Args:
            size: Maximum number of rows to fetch. If None or not positive,
                ``arraysize`` is used.

        Returns:
            The fetched rows.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.fetchmany(size)

    @override
    def fetchall(
        self,
    ) -> list[tuple[Any | None, ...] | dict[Any, Any | None]]:
        """Fetch all remaining rows from the result set.

        Returns:
            The remaining rows.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.fetchall()

    @override
    def executemany(
        self,
        operation: str,
        seq_of_parameters: list[dict[str, Any] | list[str] | None],
        **kwargs,
    ) -> None:
        """Execute a SQL query multiple times with different parameters.

        On success, ``rowcount`` is the sum of the affected row counts, or
        -1 if any execution has an unknown count. An empty parameter list
        sets it to 0. On failure, it is -1; earlier executions are not rolled
        back. Result sets are discarded.

        On failure, ``query_id`` retains the current query ID when available.
        If parameter iteration fails, this can identify the last successful
        execution.

        Args:
            operation: SQL query string to execute.
            seq_of_parameters: Sequence of parameter sets, one per execution.
            **kwargs: Additional keyword arguments passed to each ``execute()``.
                An empty parameter list makes no ``execute()`` call, so it does
                not report unsupported keyword names.
        """
        self._reset_state()
        rowcount = 0
        try:
            for parameters in seq_of_parameters:
                self.execute(operation, parameters, **kwargs)
                count = self.rowcount
                rowcount = rowcount + count if rowcount >= 0 and count >= 0 else -1
        except BaseException:
            # Keep the query ID available for diagnostics and explicit cancellation.
            self.close()
            self.result_set = None
            raise
        self._reset_state()
        self._rowcount = rowcount

    def cancel(self) -> None:
        """Cancel the currently executing query.

        Raises:
            ProgrammingError: If no query is currently executing.
        """
        if not self.query_id:
            raise ProgrammingError("QueryExecutionId is none or empty.")
        self._cancel(self.query_id)
