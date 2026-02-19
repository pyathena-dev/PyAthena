# -*- coding: utf-8 -*-
from __future__ import annotations

import collections
import logging
from abc import abstractmethod
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Deque,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

from pyathena.common import CursorIterator
from pyathena.converter import Converter, DefaultTypeConverter
from pyathena.error import DataError, OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.util import RetryConfig, parse_output_location, retry_api_call

if TYPE_CHECKING:
    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


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

    def __init__(
        self,
        connection: "Connection[Any]",
        converter: Converter,
        query_execution: AthenaQueryExecution,
        arraysize: int,
        retry_config: RetryConfig,
    ) -> None:
        super().__init__(arraysize=arraysize)
        self._connection: Optional["Connection[Any]"] = connection
        self._converter = converter
        self._query_execution: Optional[AthenaQueryExecution] = query_execution
        if not self._query_execution:
            raise ProgrammingError("Required argument `query_execution` not found.")
        self._retry_config = retry_config
        self._client = connection.session.client(
            "s3",
            region_name=connection.region_name,
            config=connection.config,
            **connection._client_kwargs,
        )

        self._metadata: Optional[Tuple[Dict[str, Any], ...]] = None
        self._rows: Deque[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]] = (
            collections.deque()
        )
        self._next_token: Optional[str] = None

        if self.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self._rownumber = 0
            self._pre_fetch()

    @property
    def database(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.database

    @property
    def catalog(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.catalog

    @property
    def query_id(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.query_id

    @property
    def query(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.query

    @property
    def statement_type(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.statement_type

    @property
    def substatement_type(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.substatement_type

    @property
    def work_group(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.work_group

    @property
    def execution_parameters(self) -> List[str]:
        if not self._query_execution:
            return []
        return self._query_execution.execution_parameters

    @property
    def state(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.state

    @property
    def state_change_reason(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.state_change_reason

    @property
    def submission_date_time(self) -> Optional[datetime]:
        if not self._query_execution:
            return None
        return self._query_execution.submission_date_time

    @property
    def completion_date_time(self) -> Optional[datetime]:
        if not self._query_execution:
            return None
        return self._query_execution.completion_date_time

    @property
    def error_category(self) -> Optional[int]:
        if not self._query_execution:
            return None
        return self._query_execution.error_category

    @property
    def error_type(self) -> Optional[int]:
        if not self._query_execution:
            return None
        return self._query_execution.error_type

    @property
    def retryable(self) -> Optional[bool]:
        if not self._query_execution:
            return None
        return self._query_execution.retryable

    @property
    def error_message(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.error_message

    @property
    def data_scanned_in_bytes(self) -> Optional[int]:
        if not self._query_execution:
            return None
        return self._query_execution.data_scanned_in_bytes

    @property
    def engine_execution_time_in_millis(self) -> Optional[int]:
        if not self._query_execution:
            return None
        return self._query_execution.engine_execution_time_in_millis

    @property
    def query_queue_time_in_millis(self) -> Optional[int]:
        if not self._query_execution:
            return None
        return self._query_execution.query_queue_time_in_millis

    @property
    def total_execution_time_in_millis(self) -> Optional[int]:
        if not self._query_execution:
            return None
        return self._query_execution.total_execution_time_in_millis

    @property
    def query_planning_time_in_millis(self) -> Optional[int]:
        if not self._query_execution:
            return None
        return self._query_execution.query_planning_time_in_millis

    @property
    def service_processing_time_in_millis(self) -> Optional[int]:
        if not self._query_execution:
            return None
        return self._query_execution.service_processing_time_in_millis

    @property
    def output_location(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.output_location

    @property
    def data_manifest_location(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.data_manifest_location

    @property
    def reused_previous_result(self) -> Optional[bool]:
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
    def encryption_option(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.encryption_option

    @property
    def kms_key(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.kms_key

    @property
    def expected_bucket_owner(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.expected_bucket_owner

    @property
    def s3_acl_option(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.s3_acl_option

    @property
    def selected_engine_version(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.selected_engine_version

    @property
    def effective_engine_version(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.effective_engine_version

    @property
    def result_reuse_enabled(self) -> Optional[bool]:
        if not self._query_execution:
            return None
        return self._query_execution.result_reuse_enabled

    @property
    def result_reuse_minutes(self) -> Optional[int]:
        if not self._query_execution:
            return None
        return self._query_execution.result_reuse_minutes

    @property
    def description(
        self,
    ) -> Optional[List[Tuple[str, str, None, None, int, int, str]]]:
        if self._metadata is None:
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
    def connection(self) -> "Connection[Any]":
        if self.is_closed:
            raise ProgrammingError("AthenaResultSet is closed.")
        return cast("Connection[Any]", self._connection)

    def __get_query_results(
        self, max_results: int, next_token: Optional[str] = None
    ) -> Dict[str, Any]:
        if not self.query_id:
            raise ProgrammingError("QueryExecutionId is none or empty.")
        if self.state != AthenaQueryExecution.STATE_SUCCEEDED:
            raise ProgrammingError("QueryExecutionState is not SUCCEEDED.")
        if self.is_closed:
            raise ProgrammingError("AthenaResultSet is closed.")
        request: Dict[str, Any] = {
            "QueryExecutionId": self.query_id,
            "MaxResults": max_results,
        }
        if next_token:
            request["NextToken"] = next_token
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
            return cast(Dict[str, Any], response)

    def __fetch(self, next_token: Optional[str] = None) -> Dict[str, Any]:
        return self.__get_query_results(self._arraysize, next_token)

    def _fetch(self) -> None:
        if not self._next_token:
            raise ProgrammingError("NextToken is none or empty.")
        response = self.__fetch(self._next_token)
        rows, self._next_token = self._parse_result_rows(response)
        self._process_rows(rows)

    def _pre_fetch(self) -> None:
        response = self.__fetch()
        self._process_metadata(response)
        self._process_update_count(response)
        rows, self._next_token = self._parse_result_rows(response)
        offset = 1 if rows and self._is_first_row_column_labels(rows) else 0
        self._process_rows(rows, offset)

    def fetchone(
        self,
    ) -> Optional[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        if not self._rows and self._next_token:
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

    def _process_metadata(self, response: Dict[str, Any]) -> None:
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

    def _process_update_count(self, response: Dict[str, Any]) -> None:
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
        metadata: Tuple[Any, ...],
        rows: List[Dict[str, Any]],
        converter: Optional[Converter] = None,
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        conv = converter or self._converter
        return [
            tuple(
                [
                    conv.convert(meta.get("Type"), row.get("VarCharValue"))
                    for meta, row in zip(metadata, rows[i].get("Data", []), strict=False)
                ]
            )
            for i in range(offset, len(rows))
        ]

    def _parse_result_rows(
        self, response: Dict[str, Any]
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
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

    def _process_rows(self, rows: List[Dict[str, Any]], offset: int = 0) -> None:
        if rows and self._metadata:
            processed_rows = self._get_rows(offset, self._metadata, rows)
            self._rows.extend(processed_rows)

    def _is_first_row_column_labels(self, rows: List[Dict[str, Any]]) -> bool:
        first_row_data = rows[0].get("Data", [])
        for meta, data in zip(self._metadata or (), first_row_data, strict=False):
            if meta.get("Name") != data.get("VarCharValue"):
                return False
        return True

    def _fetch_all_rows(
        self,
        converter: Optional[Converter] = None,
    ) -> List[Tuple[Optional[Any], ...]]:
        """Fetch all rows via GetQueryResults API with type conversion.

        Paginates through all results from the beginning using MaxResults=1000.
        Defaults to ``DefaultTypeConverter`` for string-to-Python type conversion,
        because subclass converters (e.g. Pandas/Arrow) are designed for S3 file
        reading and may not handle API result strings.

        This method is intended for use by subclass result sets that need to
        fall back to the API when S3 output is not available (e.g., managed
        query result storage).

        Args:
            converter: Type converter for result values. Defaults to
                ``DefaultTypeConverter`` if not specified.

        Returns:
            List of converted row tuples.
        """
        if self._metadata is None:
            raise ProgrammingError("Metadata is not available.")

        _logger.warning(
            "output_location is not available (e.g. managed query result storage). "
            "Falling back to GetQueryResults API. "
            "This may be slow for large result sets."
        )

        converter = converter or DefaultTypeConverter()
        all_rows: List[Tuple[Optional[Any], ...]] = []
        next_token: Optional[str] = None

        while True:
            response = self.__get_query_results(self.DEFAULT_FETCH_SIZE, next_token)
            rows, next_token = self._parse_result_rows(response)

            offset = 1 if rows and self._is_first_row_column_labels(rows) else 0
            all_rows.extend(
                cast(
                    List[Tuple[Optional[Any], ...]],
                    self._get_rows(offset, self._metadata, rows, converter),
                )
            )

            if not next_token:
                break

        return all_rows

    @staticmethod
    def _rows_to_columnar(
        rows: List[Tuple[Optional[Any], ...]],
        columns: List[str],
    ) -> Dict[str, List[Any]]:
        """Convert row-oriented data to columnar format.

        Args:
            rows: List of row tuples from ``_fetch_all_rows()``.
            columns: Column names in order.

        Returns:
            Dictionary mapping column names to lists of values.
        """
        columnar: Dict[str, List[Any]] = {col: [] for col in columns}
        for row in rows:
            for col, val in zip(columns, row, strict=False):
                columnar[col].append(val)
        return columnar

    def _get_content_length(self) -> int:
        if not self.output_location:
            raise ProgrammingError("OutputLocation is none or empty.")
        bucket, key = parse_output_location(self.output_location)
        try:
            response = retry_api_call(
                self._client.head_object,
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

    def _read_data_manifest(self) -> List[str]:
        if not self.data_manifest_location:
            raise ProgrammingError("DataManifestLocation is none or empty.")
        bucket, key = parse_output_location(self.data_manifest_location)
        try:
            response = retry_api_call(
                self._client.get_object,
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
        return self._connection is None

    def close(self) -> None:
        self._connection = None
        self._query_execution = None
        self._metadata = None
        self._rows.clear()
        self._next_token = None
        self._rownumber = None
        self._rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class AthenaDictResultSet(AthenaResultSet):
    # You can override this to use OrderedDict or other dict-like types.
    dict_type: Type[Any] = dict

    def _get_rows(
        self,
        offset: int,
        metadata: Tuple[Any, ...],
        rows: List[Dict[str, Any]],
        converter: Optional[Converter] = None,
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        conv = converter or self._converter
        return [
            self.dict_type(
                [
                    (
                        meta.get("Name"),
                        conv.convert(meta.get("Type"), row.get("VarCharValue")),
                    )
                    for meta, row in zip(metadata, rows[i].get("Data", []), strict=False)
                ]
            )
            for i in range(offset, len(rows))
        ]


class WithResultSet:
    def __init__(self):
        super().__init__()

    def _reset_state(self) -> None:
        self.query_id = None  # type: ignore
        if self.result_set and not self.result_set.is_closed:
            self.result_set.close()
        self.result_set = None  # type: ignore

    @property  # type: ignore
    @abstractmethod
    def result_set(self) -> Optional[AthenaResultSet]:
        raise NotImplementedError  # pragma: no cover

    @result_set.setter  # type: ignore
    @abstractmethod
    def result_set(self, val: Optional[AthenaResultSet]) -> None:
        raise NotImplementedError  # pragma: no cover

    @property
    def has_result_set(self) -> bool:
        return self.result_set is not None

    @property
    def description(
        self,
    ) -> Optional[List[Tuple[str, str, None, None, int, int, str]]]:
        if not self.result_set:
            return None
        return self.result_set.description

    @property
    def database(self) -> Optional[str]:
        if not self.result_set:
            return None
        return self.result_set.database

    @property
    def catalog(self) -> Optional[str]:
        if not self.result_set:
            return None
        return self.result_set.catalog

    @property  # type: ignore
    @abstractmethod
    def query_id(self) -> Optional[str]:
        raise NotImplementedError  # pragma: no cover

    @query_id.setter  # type: ignore
    @abstractmethod
    def query_id(self, val: Optional[str]) -> None:
        raise NotImplementedError  # pragma: no cover

    @property
    def query(self) -> Optional[str]:
        if not self.result_set:
            return None
        return self.result_set.query

    @property
    def statement_type(self) -> Optional[str]:
        if not self.result_set:
            return None
        return self.result_set.statement_type

    @property
    def substatement_type(self) -> Optional[str]:
        if not self.result_set:
            return None
        return self.result_set.substatement_type

    @property
    def work_group(self) -> Optional[str]:
        if not self.result_set:
            return None
        return self.result_set.work_group

    @property
    def execution_parameters(self) -> List[str]:
        if not self.result_set:
            return []
        return self.result_set.execution_parameters

    @property
    def state(self) -> Optional[str]:
        if not self.result_set:
            return None
        return self.result_set.state

    @property
    def state_change_reason(self) -> Optional[str]:
        if not self.result_set:
            return None
        return self.result_set.state_change_reason

    @property
    def submission_date_time(self) -> Optional[datetime]:
        if not self.result_set:
            return None
        return self.result_set.submission_date_time

    @property
    def completion_date_time(self) -> Optional[datetime]:
        if not self.result_set:
            return None
        return self.result_set.completion_date_time

    @property
    def error_category(self) -> Optional[int]:
        if not self.result_set:
            return None
        return self.result_set.error_category

    @property
    def error_type(self) -> Optional[int]:
        if not self.result_set:
            return None
        return self.result_set.error_type

    @property
    def retryable(self) -> Optional[bool]:
        if not self.result_set:
            return None
        return self.result_set.retryable

    @property
    def error_message(self) -> Optional[str]:
        if not self.result_set:
            return None
        return self.result_set.error_message

    @property
    def data_scanned_in_bytes(self) -> Optional[int]:
        if not self.result_set:
            return None
        return self.result_set.data_scanned_in_bytes

    @property
    def engine_execution_time_in_millis(self) -> Optional[int]:
        if not self.result_set:
            return None
        return self.result_set.engine_execution_time_in_millis

    @property
    def query_queue_time_in_millis(self) -> Optional[int]:
        if not self.result_set:
            return None
        return self.result_set.query_queue_time_in_millis

    @property
    def total_execution_time_in_millis(self) -> Optional[int]:
        if not self.result_set:
            return None
        return self.result_set.total_execution_time_in_millis

    @property
    def query_planning_time_in_millis(self) -> Optional[int]:
        if not self.result_set:
            return None
        return self.result_set.query_planning_time_in_millis

    @property
    def service_processing_time_in_millis(self) -> Optional[int]:
        if not self.result_set:
            return None
        return self.result_set.service_processing_time_in_millis

    @property
    def output_location(self) -> Optional[str]:
        if not self.result_set:
            return None
        return self.result_set.output_location

    @property
    def data_manifest_location(self) -> Optional[str]:
        if not self.result_set:
            return None
        return self.result_set.data_manifest_location

    @property
    def reused_previous_result(self) -> Optional[bool]:
        if not self.result_set:
            return None
        return self.result_set.reused_previous_result

    @property
    def encryption_option(self) -> Optional[str]:
        if not self.result_set:
            return None
        return self.result_set.encryption_option

    @property
    def kms_key(self) -> Optional[str]:
        if not self.result_set:
            return None
        return self.result_set.kms_key

    @property
    def expected_bucket_owner(self) -> Optional[str]:
        if not self.result_set:
            return None
        return self.result_set.expected_bucket_owner

    @property
    def s3_acl_option(self) -> Optional[str]:
        if not self.result_set:
            return None
        return self.result_set.s3_acl_option

    @property
    def selected_engine_version(self) -> Optional[str]:
        if not self.result_set:
            return None
        return self.result_set.selected_engine_version

    @property
    def effective_engine_version(self) -> Optional[str]:
        if not self.result_set:
            return None
        return self.result_set.effective_engine_version

    @property
    def result_reuse_enabled(self) -> Optional[bool]:
        if not self.result_set:
            return None
        return self.result_set.result_reuse_enabled

    @property
    def result_reuse_minutes(self) -> Optional[int]:
        if not self.result_set:
            return None
        return self.result_set.result_reuse_minutes

    @property
    def rowcount(self) -> int:
        """Get the number of rows affected by the last operation.

        For SELECT statements, this returns -1 as per DB API 2.0 specification.
        For DML operations (INSERT, UPDATE, DELETE) and CTAS, this returns
        the number of affected rows.

        Returns:
            The number of rows, or -1 if not applicable or unknown.
        """
        return self.result_set.rowcount if self.result_set else -1
