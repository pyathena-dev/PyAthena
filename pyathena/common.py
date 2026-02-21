# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import sys
import time
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union, cast

import pyathena
from pyathena.converter import Converter, DefaultTypeConverter
from pyathena.error import DatabaseError, OperationalError, ProgrammingError
from pyathena.formatter import Formatter
from pyathena.model import (
    AthenaCalculationExecution,
    AthenaCalculationExecutionStatus,
    AthenaCompression,
    AthenaDatabase,
    AthenaFileFormat,
    AthenaQueryExecution,
    AthenaTableMetadata,
)
from pyathena.util import RetryConfig, retry_api_call

if TYPE_CHECKING:
    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class CursorIterator(metaclass=ABCMeta):
    """Abstract base class providing iteration and result fetching capabilities for cursors.

    This mixin class provides common functionality for iterating through query results
    and managing cursor state. It implements the iterator protocol and provides
    standard fetch methods that conform to the DB API 2.0 specification.

    Attributes:
        DEFAULT_FETCH_SIZE: Default number of rows to fetch per request (1000).
        DEFAULT_RESULT_REUSE_MINUTES: Default minutes for Athena result reuse (60).
        arraysize: Number of rows to fetch with fetchmany() if size not specified.

    Note:
        This is an abstract base class used by concrete cursor implementations.
        It should not be instantiated directly.
    """

    # https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryResults.html
    # Valid Range: Minimum value of 1. Maximum value of 1000.
    DEFAULT_FETCH_SIZE: int = 1000
    # https://docs.aws.amazon.com/athena/latest/APIReference/API_ResultReuseByAgeConfiguration.html
    # Specifies, in minutes, the maximum age of a previous query result
    # that Athena should consider for reuse. The default is 60.
    DEFAULT_RESULT_REUSE_MINUTES = 60

    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.arraysize: int = kwargs.get("arraysize", self.DEFAULT_FETCH_SIZE)
        self._rownumber: Optional[int] = None
        self._rowcount: int = -1  # By default, return -1 to indicate that this is not supported.

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value <= 0 or value > self.DEFAULT_FETCH_SIZE:
            raise ProgrammingError(
                f"MaxResults is more than maximum allowed length {self.DEFAULT_FETCH_SIZE}."
            )
        self._arraysize = value

    @property
    def rownumber(self) -> Optional[int]:
        return self._rownumber

    @property
    def rowcount(self) -> int:
        return self._rowcount

    @abstractmethod
    def fetchone(self):
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def fetchmany(self):
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def fetchall(self):
        raise NotImplementedError  # pragma: no cover

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __iter__(self):
        return self


class BaseCursor(metaclass=ABCMeta):
    """Abstract base class for all PyAthena cursor implementations.

    This class provides the foundational functionality for executing SQL queries
    and calculations on Amazon Athena. It handles AWS API interactions, query
    execution management, metadata operations, and result polling.

    All concrete cursor implementations (Cursor, DictCursor, PandasCursor,
    ArrowCursor, SparkCursor, AsyncCursor) inherit from this base class and
    implement the abstract methods according to their specific use cases.

    Attributes:
        LIST_QUERY_EXECUTIONS_MAX_RESULTS: Maximum results per query listing API call (50).
        LIST_TABLE_METADATA_MAX_RESULTS: Maximum results per table metadata API call (50).
        LIST_DATABASES_MAX_RESULTS: Maximum results per database listing API call (50).

    Key Features:
        - Query execution and polling with configurable retry logic
        - Table and database metadata operations
        - Result caching and reuse capabilities
        - Encryption and security configuration support
        - Workgroup and catalog management
        - Query cancellation and interruption handling

    Example:
        This is an abstract base class and should not be instantiated directly.
        Use concrete implementations like Cursor or PandasCursor instead:

        >>> cursor = connection.cursor()  # Creates default Cursor
        >>> cursor.execute("SELECT * FROM my_table")
        >>> results = cursor.fetchall()

    Note:
        This class contains AWS service quotas as constants. These limits
        are enforced by the AWS Athena service and should not be modified.
    """

    # https://docs.aws.amazon.com/athena/latest/APIReference/API_ListQueryExecutions.html
    # Valid Range: Minimum value of 0. Maximum value of 50.
    LIST_QUERY_EXECUTIONS_MAX_RESULTS = 50
    # https://docs.aws.amazon.com/athena/latest/APIReference/API_ListTableMetadata.html
    # Valid Range: Minimum value of 1. Maximum value of 50.
    LIST_TABLE_METADATA_MAX_RESULTS = 50
    # https://docs.aws.amazon.com/athena/latest/APIReference/API_ListDatabases.html
    # Valid Range: Minimum value of 1. Maximum value of 50.
    LIST_DATABASES_MAX_RESULTS = 50

    def __init__(
        self,
        connection: "Connection[Any]",
        converter: Converter,
        formatter: Formatter,
        retry_config: RetryConfig,
        s3_staging_dir: Optional[str],
        schema_name: Optional[str],
        catalog_name: Optional[str],
        work_group: Optional[str],
        poll_interval: float,
        encryption_option: Optional[str],
        kms_key: Optional[str],
        kill_on_interrupt: bool,
        result_reuse_enable: bool,
        result_reuse_minutes: int,
        **kwargs,
    ) -> None:
        super().__init__()
        self._connection = connection
        self._converter = converter
        self._formatter = formatter
        self._retry_config = retry_config
        self._s3_staging_dir = s3_staging_dir
        self._schema_name = schema_name
        self._catalog_name = catalog_name
        self._work_group = work_group
        self._poll_interval = poll_interval
        self._encryption_option = encryption_option
        self._kms_key = kms_key
        self._kill_on_interrupt = kill_on_interrupt
        self._result_reuse_enable = result_reuse_enable
        self._result_reuse_minutes = result_reuse_minutes

    @staticmethod
    def get_default_converter(unload: bool = False) -> Union[DefaultTypeConverter, Any]:
        """Get the default type converter for this cursor class.

        Args:
            unload: Whether the converter is for UNLOAD operations. Some cursor
                   types may return different converters for UNLOAD operations.

        Returns:
            The default type converter instance for this cursor type.
        """
        return DefaultTypeConverter()

    @property
    def connection(self) -> "Connection[Any]":
        return self._connection

    def _build_start_query_execution_request(
        self,
        query: str,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        result_reuse_enable: Optional[bool] = None,
        result_reuse_minutes: Optional[int] = None,
        execution_parameters: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        request: Dict[str, Any] = {
            "QueryString": query,
            "QueryExecutionContext": {},
        }
        if self._schema_name:
            request["QueryExecutionContext"].update({"Database": self._schema_name})
        if self._catalog_name:
            request["QueryExecutionContext"].update({"Catalog": self._catalog_name})
        result_configuration: Dict[str, Any] = {}
        if self._s3_staging_dir or s3_staging_dir:
            result_configuration["OutputLocation"] = (
                s3_staging_dir if s3_staging_dir else self._s3_staging_dir
            )
        if self._work_group or work_group:
            request.update({"WorkGroup": work_group if work_group else self._work_group})
        if self._encryption_option:
            enc_conf = {
                "EncryptionOption": self._encryption_option,
            }
            if self._kms_key:
                enc_conf.update({"KmsKey": self._kms_key})
            result_configuration["EncryptionConfiguration"] = enc_conf
        if result_configuration:
            request["ResultConfiguration"] = result_configuration
        if self._result_reuse_enable or result_reuse_enable:
            reuse_conf = {
                "Enabled": result_reuse_enable
                if result_reuse_enable is not None
                else self._result_reuse_enable,
                "MaxAgeInMinutes": result_reuse_minutes
                if result_reuse_minutes is not None
                else self._result_reuse_minutes,
            }
            request["ResultReuseConfiguration"] = {"ResultReuseByAgeConfiguration": reuse_conf}
        if execution_parameters:
            request["ExecutionParameters"] = execution_parameters
        return request

    def _build_start_calculation_execution_request(
        self,
        session_id: str,
        code_block: str,
        description: Optional[str] = None,
        client_request_token: Optional[str] = None,
    ):
        request: Dict[str, Any] = {
            "SessionId": session_id,
            "CodeBlock": code_block,
        }
        if description:
            request.update({"Description": description})
        if client_request_token:
            request.update({"ClientRequestToken": client_request_token})
        return request

    def _build_list_query_executions_request(
        self,
        work_group: Optional[str],
        next_token: Optional[str] = None,
        max_results: Optional[int] = None,
    ) -> Dict[str, Any]:
        request: Dict[str, Any] = {
            "MaxResults": max_results if max_results else self.LIST_QUERY_EXECUTIONS_MAX_RESULTS
        }
        if self._work_group or work_group:
            request.update({"WorkGroup": work_group if work_group else self._work_group})
        if next_token:
            request.update({"NextToken": next_token})
        return request

    def _build_list_table_metadata_request(
        self,
        catalog_name: Optional[str],
        schema_name: Optional[str],
        expression: Optional[str] = None,
        next_token: Optional[str] = None,
        max_results: Optional[int] = None,
    ) -> Dict[str, Any]:
        request: Dict[str, Any] = {
            "CatalogName": catalog_name if catalog_name else self._catalog_name,
            "DatabaseName": schema_name if schema_name else self._schema_name,
            "MaxResults": max_results if max_results else self.LIST_TABLE_METADATA_MAX_RESULTS,
        }
        if expression:
            request.update({"Expression": expression})
        if next_token:
            request.update({"NextToken": next_token})
        if self._work_group:
            request.update({"WorkGroup": self._work_group})
        return request

    def _build_list_databases_request(
        self,
        catalog_name: Optional[str],
        next_token: Optional[str] = None,
        max_results: Optional[int] = None,
    ):
        request: Dict[str, Any] = {
            "CatalogName": catalog_name if catalog_name else self._catalog_name,
            "MaxResults": max_results if max_results else self.LIST_DATABASES_MAX_RESULTS,
        }
        if next_token:
            request.update({"NextToken": next_token})
        if self._work_group:
            request.update({"WorkGroup": self._work_group})
        return request

    def _list_databases(
        self,
        catalog_name: Optional[str],
        next_token: Optional[str] = None,
        max_results: Optional[int] = None,
    ) -> Tuple[Optional[str], List[AthenaDatabase]]:
        request = self._build_list_databases_request(
            catalog_name=catalog_name,
            next_token=next_token,
            max_results=max_results,
        )
        try:
            response = retry_api_call(
                self.connection._client.list_databases,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to list databases.")
            raise OperationalError(*e.args) from e
        else:
            return response.get("NextToken"), [
                AthenaDatabase({"Database": r}) for r in response.get("DatabaseList", [])
            ]

    def list_databases(
        self,
        catalog_name: Optional[str],
        max_results: Optional[int] = None,
    ) -> List[AthenaDatabase]:
        databases = []
        next_token = None
        while True:
            next_token, response = self._list_databases(
                catalog_name=catalog_name,
                next_token=next_token,
                max_results=max_results,
            )
            databases.extend(response)
            if not next_token:
                break
        return databases

    def _build_get_table_metadata_request(
        self,
        table_name: str,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        request: Dict[str, Any] = {
            "CatalogName": catalog_name if catalog_name else self._catalog_name,
            "DatabaseName": schema_name if schema_name else self._schema_name,
            "TableName": table_name,
        }
        if self._work_group:
            request.update({"WorkGroup": self._work_group})
        return request

    def _get_table_metadata(
        self,
        table_name: str,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        logging_: bool = True,
    ) -> AthenaTableMetadata:
        request = self._build_get_table_metadata_request(
            table_name=table_name,
            catalog_name=catalog_name,
            schema_name=schema_name,
        )
        try:
            response = retry_api_call(
                self._connection.client.get_table_metadata,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            if logging_:
                _logger.exception("Failed to get table metadata.")
            raise OperationalError(*e.args) from e
        else:
            return AthenaTableMetadata(response)

    def get_table_metadata(
        self,
        table_name: str,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        logging_: bool = True,
    ) -> AthenaTableMetadata:
        return self._get_table_metadata(
            table_name=table_name,
            catalog_name=catalog_name,
            schema_name=schema_name,
            logging_=logging_,
        )

    def _list_table_metadata(
        self,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        expression: Optional[str] = None,
        next_token: Optional[str] = None,
        max_results: Optional[int] = None,
    ) -> Tuple[Optional[str], List[AthenaTableMetadata]]:
        request = self._build_list_table_metadata_request(
            catalog_name=catalog_name,
            schema_name=schema_name,
            expression=expression,
            next_token=next_token,
            max_results=max_results,
        )
        try:
            response = retry_api_call(
                self.connection._client.list_table_metadata,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to list table metadata.")
            raise OperationalError(*e.args) from e
        else:
            return response.get("NextToken"), [
                AthenaTableMetadata({"TableMetadata": r})
                for r in response.get("TableMetadataList", [])
            ]

    def list_table_metadata(
        self,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        expression: Optional[str] = None,
        max_results: Optional[int] = None,
    ) -> List[AthenaTableMetadata]:
        metadata = []
        next_token = None
        while True:
            next_token, response = self._list_table_metadata(
                catalog_name=catalog_name,
                schema_name=schema_name,
                expression=expression,
                next_token=next_token,
                max_results=max_results,
            )
            metadata.extend(response)
            if not next_token:
                break
        return metadata

    def _get_query_execution(self, query_id: str) -> AthenaQueryExecution:
        request = {"QueryExecutionId": query_id}
        try:
            response = retry_api_call(
                self._connection.client.get_query_execution,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to get query execution.")
            raise OperationalError(*e.args) from e
        else:
            return AthenaQueryExecution(response)

    def _get_calculation_execution_status(self, query_id: str) -> AthenaCalculationExecutionStatus:
        request = {"CalculationExecutionId": query_id}
        try:
            response = retry_api_call(
                self._connection.client.get_calculation_execution_status,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to get calculation execution status.")
            raise OperationalError(*e.args) from e
        else:
            return AthenaCalculationExecutionStatus(response)

    def _get_calculation_execution(self, query_id: str) -> AthenaCalculationExecution:
        request = {"CalculationExecutionId": query_id}
        try:
            response = retry_api_call(
                self._connection.client.get_calculation_execution,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to get calculation execution.")
            raise OperationalError(*e.args) from e
        else:
            return AthenaCalculationExecution(response)

    def _batch_get_query_execution(self, query_ids: List[str]) -> List[AthenaQueryExecution]:
        try:
            response = retry_api_call(
                self.connection._client.batch_get_query_execution,
                config=self._retry_config,
                logger=_logger,
                QueryExecutionIds=query_ids,
            )
        except Exception as e:
            _logger.exception("Failed to batch get query execution.")
            raise OperationalError(*e.args) from e
        else:
            return [
                AthenaQueryExecution({"QueryExecution": r})
                for r in response.get("QueryExecutions", [])
            ]

    def _list_query_executions(
        self,
        work_group: Optional[str] = None,
        next_token: Optional[str] = None,
        max_results: Optional[int] = None,
    ) -> Tuple[Optional[str], List[AthenaQueryExecution]]:
        request = self._build_list_query_executions_request(
            work_group=work_group, next_token=next_token, max_results=max_results
        )
        try:
            response = retry_api_call(
                self.connection._client.list_query_executions,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to list query executions.")
            raise OperationalError(*e.args) from e
        else:
            next_token = response.get("NextToken")
            query_ids = response.get("QueryExecutionIds")
            if not query_ids:
                return next_token, []
            return next_token, self._batch_get_query_execution(query_ids)

    def __poll(self, query_id: str) -> Union[AthenaQueryExecution, AthenaCalculationExecution]:
        while True:
            query_execution = self._get_query_execution(query_id)
            if query_execution.state in [
                AthenaQueryExecution.STATE_SUCCEEDED,
                AthenaQueryExecution.STATE_FAILED,
                AthenaQueryExecution.STATE_CANCELLED,
            ]:
                return query_execution
            time.sleep(self._poll_interval)

    def _poll(self, query_id: str) -> Union[AthenaQueryExecution, AthenaCalculationExecution]:
        try:
            query_execution = self.__poll(query_id)
        except KeyboardInterrupt as e:
            if self._kill_on_interrupt:
                _logger.warning("Query canceled by user.")
                self._cancel(query_id)
                query_execution = self.__poll(query_id)
            else:
                raise e
        return query_execution

    def _find_previous_query_id(
        self,
        query: str,
        work_group: Optional[str],
        cache_size: int = 0,
        cache_expiration_time: int = 0,
    ) -> Optional[str]:
        query_id = None
        if cache_size == 0 and cache_expiration_time > 0:
            cache_size = sys.maxsize
        if cache_expiration_time > 0:
            expiration_time = datetime.now(timezone.utc) - timedelta(seconds=cache_expiration_time)
        else:
            expiration_time = datetime.now(timezone.utc)
        try:
            next_token = None
            while cache_size > 0:
                max_results = min(cache_size, self.LIST_QUERY_EXECUTIONS_MAX_RESULTS)
                cache_size -= max_results
                next_token, query_executions = self._list_query_executions(
                    work_group, next_token=next_token, max_results=max_results
                )
                for execution in sorted(
                    (
                        e
                        for e in query_executions
                        if e.state == AthenaQueryExecution.STATE_SUCCEEDED
                        and e.statement_type == AthenaQueryExecution.STATEMENT_TYPE_DML
                    ),
                    # https://github.com/python/mypy/issues/9656
                    key=lambda e: e.completion_date_time,  # type: ignore
                    reverse=True,
                ):
                    if (
                        cache_expiration_time > 0
                        and execution.completion_date_time
                        and execution.completion_date_time.astimezone(timezone.utc)
                        < expiration_time
                    ):
                        next_token = None
                        break
                    if execution.query == query:
                        query_id = execution.query_id
                        break
                if query_id or next_token is None:
                    break
        except Exception:
            _logger.warning("Failed to check the cache. Moving on without cache.", exc_info=True)
        return query_id

    def _prepare_query(
        self,
        operation: str,
        parameters: Optional[Union[Dict[str, Any], List[str]]] = None,
        paramstyle: Optional[str] = None,
    ) -> Tuple[str, Optional[List[str]]]:
        """Format query and build execution parameters. No I/O.

        Args:
            operation: SQL query string.
            parameters: Query parameters.
            paramstyle: Parameter style override.

        Returns:
            Tuple of (formatted_query, execution_parameters).
        """
        if pyathena.paramstyle == "qmark" or paramstyle == "qmark":
            query = operation
            execution_parameters = cast(Optional[List[str]], parameters)
        else:
            query = self._formatter.format(operation, cast(Optional[Dict[str, Any]], parameters))
            execution_parameters = None
        _logger.debug(query)
        return query, execution_parameters

    def _prepare_unload(
        self,
        operation: str,
        s3_staging_dir: Optional[str],
    ) -> Tuple[str, Optional[str]]:
        """Wrap operation with UNLOAD if enabled.

        Args:
            operation: SQL query string.
            s3_staging_dir: S3 location for query results.

        Returns:
            Tuple of (possibly-wrapped operation, unload_location or None).
        """
        if not getattr(self, "_unload", False):
            return operation, None
        s3_staging_dir = s3_staging_dir if s3_staging_dir else self._s3_staging_dir
        if not s3_staging_dir:
            raise ProgrammingError("If the unload option is used, s3_staging_dir is required.")
        return self._formatter.wrap_unload(
            operation,
            s3_staging_dir=s3_staging_dir,
            format_=AthenaFileFormat.FILE_FORMAT_PARQUET,
            compression=AthenaCompression.COMPRESSION_SNAPPY,
        )

    def _execute(
        self,
        operation: str,
        parameters: Optional[Union[Dict[str, Any], List[str]]] = None,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        cache_size: Optional[int] = 0,
        cache_expiration_time: Optional[int] = 0,
        result_reuse_enable: Optional[bool] = None,
        result_reuse_minutes: Optional[int] = None,
        paramstyle: Optional[str] = None,
    ) -> str:
        query, execution_parameters = self._prepare_query(operation, parameters, paramstyle)

        request = self._build_start_query_execution_request(
            query=query,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            execution_parameters=execution_parameters,
        )
        query_id = self._find_previous_query_id(
            query,
            work_group,
            cache_size=cache_size if cache_size else 0,
            cache_expiration_time=cache_expiration_time if cache_expiration_time else 0,
        )
        if query_id is None:
            try:
                query_id = retry_api_call(
                    self._connection.client.start_query_execution,
                    config=self._retry_config,
                    logger=_logger,
                    **request,
                ).get("QueryExecutionId")
            except Exception as e:
                _logger.exception("Failed to execute query.")
                raise DatabaseError(*e.args) from e
        return query_id

    def _calculate(
        self,
        session_id: str,
        code_block: str,
        description: Optional[str] = None,
        client_request_token: Optional[str] = None,
    ) -> str:
        request = self._build_start_calculation_execution_request(
            session_id=session_id,
            code_block=code_block,
            description=description,
            client_request_token=client_request_token,
        )
        try:
            calculation_id = retry_api_call(
                self._connection.client.start_calculation_execution,
                config=self._retry_config,
                logger=_logger,
                **request,
            ).get("CalculationExecutionId")
        except Exception as e:
            _logger.exception("Failed to execute calculation.")
            raise DatabaseError(*e.args) from e
        return cast(str, calculation_id)

    @abstractmethod
    def execute(
        self,
        operation: str,
        parameters: Optional[Union[Dict[str, Any], List[str]]] = None,
        **kwargs,
    ):
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def executemany(
        self,
        operation: str,
        seq_of_parameters: List[Optional[Union[Dict[str, Any], List[str]]]],
        **kwargs,
    ) -> None:
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError  # pragma: no cover

    def _cancel(self, query_id: str) -> None:
        request = {"QueryExecutionId": query_id}
        try:
            retry_api_call(
                self._connection.client.stop_query_execution,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to cancel query.")
            raise OperationalError(*e.args) from e

    def setinputsizes(self, sizes):  # noqa: B027
        """Does nothing by default"""
        pass

    def setoutputsize(self, size, column=None):  # noqa: B027
        """Does nothing by default"""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
