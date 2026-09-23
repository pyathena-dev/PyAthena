from __future__ import annotations

import logging
import sys
import time
from abc import ABCMeta, abstractmethod
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, TypeVar, cast

from botocore.exceptions import BotoCoreError, ClientError

import pyathena
from pyathena.converter import Converter, DefaultTypeConverter
from pyathena.error import DatabaseError, OperationalError, ProgrammingError
from pyathena.formatter import Formatter
from pyathena.glue import GlueMetadataCatalog
from pyathena.model import (
    AthenaCalculationExecution,
    AthenaCalculationExecutionStatus,
    AthenaCompression,
    AthenaDatabase,
    AthenaFileFormat,
    AthenaQueryExecution,
    AthenaTableMetadata,
)
from pyathena.options import ExecuteOptions
from pyathena.util import (
    THROTTLING_ERROR_CODES,
    RetryConfig,
    _get_error_code,
    _retry_api_call,
    retry_api_call,
)

if TYPE_CHECKING:
    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)

_T = TypeVar("_T")

OnPollCallback = Callable[[AthenaQueryExecution | AthenaCalculationExecutionStatus], None]
"""Type of the optional ``on_poll`` callback.

Invoked once per poll iteration with the current execution object: an
:class:`~pyathena.model.AthenaQueryExecution` for SQL queries, or an
:class:`~pyathena.model.AthenaCalculationExecutionStatus` for Spark calculations.
"""


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
        self._rownumber: int | None = None
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
    def rownumber(self) -> int | None:
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
        connection: Connection[Any],
        converter: Converter,
        formatter: Formatter,
        retry_config: RetryConfig,
        s3_staging_dir: str | None,
        schema_name: str | None,
        catalog_name: str | None,
        work_group: str | None,
        poll_interval: float,
        encryption_option: str | None,
        kms_key: str | None,
        kill_on_interrupt: bool,
        result_reuse_enable: bool,
        result_reuse_minutes: int,
        on_start_query_execution: Callable[[str], None] | None = None,
        on_poll: OnPollCallback | None = None,
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
        # ``on_start_query_execution`` is invoked by cursors whose ``execute()``
        # supports it (the synchronous and aio cursors). Async/Spark cursors return
        # the query id immediately through their execution model and do not invoke it.
        self._on_start_query_execution = on_start_query_execution
        self._on_poll = on_poll

    @staticmethod
    def get_default_converter(unload: bool = False) -> DefaultTypeConverter | Any:
        """Get the default type converter for this cursor class.

        Args:
            unload: Whether the converter is for UNLOAD operations. Some cursor
                   types may return different converters for UNLOAD operations.

        Returns:
            The default type converter instance for this cursor type.
        """
        return DefaultTypeConverter()

    @property
    def connection(self) -> Connection[Any]:
        return self._connection

    def _build_start_query_execution_request(
        self,
        query: str,
        work_group: str | None = None,
        s3_staging_dir: str | None = None,
        result_reuse_enable: bool | None = None,
        result_reuse_minutes: int | None = None,
        execution_parameters: list[str] | None = None,
    ) -> dict[str, Any]:
        request: dict[str, Any] = {
            "QueryString": query,
            "QueryExecutionContext": {},
        }
        if self._schema_name:
            request["QueryExecutionContext"].update({"Database": self._schema_name})
        if self._catalog_name:
            request["QueryExecutionContext"].update({"Catalog": self._catalog_name})
        result_configuration: dict[str, Any] = {}
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
        description: str | None = None,
        client_request_token: str | None = None,
    ):
        request: dict[str, Any] = {
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
        work_group: str | None,
        next_token: str | None = None,
        max_results: int | None = None,
    ) -> dict[str, Any]:
        request: dict[str, Any] = {
            "MaxResults": max_results if max_results else self.LIST_QUERY_EXECUTIONS_MAX_RESULTS
        }
        if self._work_group or work_group:
            request.update({"WorkGroup": work_group if work_group else self._work_group})
        if next_token:
            request.update({"NextToken": next_token})
        return request

    def _build_list_table_metadata_request(
        self,
        catalog_name: str | None,
        schema_name: str | None,
        expression: str | None = None,
        next_token: str | None = None,
        max_results: int | None = None,
    ) -> dict[str, Any]:
        request: dict[str, Any] = {
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

    def _glue_catalog_name(self, catalog_name: str | None) -> str | None:
        """The catalog to read from Glue when Athena throttles, or None."""
        catalog = catalog_name if catalog_name else self._catalog_name
        connection = self._connection
        if (
            connection.glue_metadata_fallback
            and not connection._glue_unreachable
            and GlueMetadataCatalog.supports(catalog)
        ):
            return catalog
        return None

    @staticmethod
    def _is_throttled(e: BaseException) -> bool:
        # Athena wraps Glue's own throttling in a MetadataException.
        return _get_error_code(e.__cause__ or e, unwrap_metadata=True) in THROTTLING_ERROR_CODES

    def _glue_request_failed(
        self, e: BotoCoreError | ClientError, description: str, absence_is_final: bool
    ) -> None:
        """Raise Glue's answer that a table is absent; otherwise log the failure.

        A request that could not reach Glue at all turns the fallback off for
        this connection, so later throttled requests do not wait for it again.
        """
        if (
            absence_is_final
            and isinstance(e, ClientError)
            and _get_error_code(e) == "EntityNotFoundException"
        ):
            raise OperationalError(*e.args) from e
        unreachable = isinstance(e, BotoCoreError)
        if unreachable:
            self._connection._glue_unreachable = True
        suffix = " and not using Glue again on this connection" if unreachable else ""
        _logger.warning(
            f"Glue request to {description} failed: {e}; retrying the Athena request{suffix}."
        )

    def _with_glue_fallback(
        self,
        catalog_name: str | None,
        athena_request: Callable[[Callable[[BaseException], bool] | None, bool], _T],
        glue_request: Callable[[GlueMetadataCatalog], _T],
        description: str,
        logging_: bool = True,
        absence_is_final: bool = False,
    ) -> _T:
        """Run a metadata request, answering its throttling from Glue.

        Athena rate-limits its metadata API per account, separately from Glue.
        In a Glue-backed catalog the first attempt stops at a throttled
        response and the request goes to Glue instead of through the retry
        policy; other errors keep the policy. When the Glue request fails, the
        Athena request continues with the policy. With ``absence_is_final``,
        Glue's answer that the table does not exist is raised instead.

        ``athena_request`` receives the predicate that stops its retries and
        whether to log a failure.
        """
        glue_catalog = self._glue_catalog_name(catalog_name)
        if glue_catalog is None:
            return athena_request(None, logging_)
        try:
            return athena_request(self._is_throttled, False)
        except OperationalError as e:
            if not self._is_throttled(e):
                if logging_:
                    _logger.exception(f"Failed to {description}.")
                raise
        _logger.warning(f"Request to {description} was throttled; reading it from Glue.")
        try:
            return glue_request(GlueMetadataCatalog(self._connection.glue_client, glue_catalog))
        except (BotoCoreError, ClientError) as e:
            self._glue_request_failed(e, description, absence_is_final)
        return athena_request(None, logging_)

    def _build_list_databases_request(
        self,
        catalog_name: str | None,
        next_token: str | None = None,
        max_results: int | None = None,
    ):
        request: dict[str, Any] = {
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
        catalog_name: str | None,
        next_token: str | None = None,
        max_results: int | None = None,
        logging_: bool = True,
        stop_on: Callable[[BaseException], bool] | None = None,
    ) -> tuple[str | None, list[AthenaDatabase]]:
        request = self._build_list_databases_request(
            catalog_name=catalog_name,
            next_token=next_token,
            max_results=max_results,
        )
        try:
            response = _retry_api_call(
                self.connection._client.list_databases,
                self._retry_config,
                _logger,
                stop_on,
                **request,
            )
        except Exception as e:
            if logging_:
                _logger.exception("Failed to list databases.")
            raise OperationalError(*e.args) from e
        else:
            return response.get("NextToken"), [
                AthenaDatabase({"Database": r}) for r in response.get("DatabaseList", [])
            ]

    def list_databases(
        self,
        catalog_name: str | None,
        max_results: int | None = None,
    ) -> list[AthenaDatabase]:
        # Pages already read are kept, so a retried request resumes after them.
        databases: list[AthenaDatabase] = []
        next_token = None

        def athena_request(
            stop_on: Callable[[BaseException], bool] | None, logging_: bool
        ) -> list[AthenaDatabase]:
            nonlocal next_token
            while True:
                next_token, response = self._list_databases(
                    catalog_name=catalog_name,
                    next_token=next_token,
                    max_results=max_results,
                    logging_=logging_,
                    stop_on=stop_on,
                )
                databases.extend(response)
                if not next_token:
                    return databases

        return self._with_glue_fallback(
            catalog_name, athena_request, GlueMetadataCatalog.list_databases, "list databases"
        )

    def _build_get_table_metadata_request(
        self,
        table_name: str,
        catalog_name: str | None = None,
        schema_name: str | None = None,
    ) -> dict[str, Any]:
        request: dict[str, Any] = {
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
        catalog_name: str | None = None,
        schema_name: str | None = None,
        logging_: bool = True,
        stop_on: Callable[[BaseException], bool] | None = None,
    ) -> AthenaTableMetadata:
        request = self._build_get_table_metadata_request(
            table_name=table_name,
            catalog_name=catalog_name,
            schema_name=schema_name,
        )
        try:
            response = _retry_api_call(
                self._connection.client.get_table_metadata,
                self._retry_config,
                _logger,
                stop_on,
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
        catalog_name: str | None = None,
        schema_name: str | None = None,
        logging_: bool = True,
    ) -> AthenaTableMetadata:
        schema_name = schema_name if schema_name else self._schema_name
        return self._with_glue_fallback(
            catalog_name,
            lambda stop_on, logging_: self._get_table_metadata(
                table_name=table_name,
                catalog_name=catalog_name,
                schema_name=schema_name,
                logging_=logging_,
                stop_on=stop_on,
            ),
            lambda glue: glue.get_table(schema_name, table_name),
            "get table metadata",
            logging_=logging_,
            absence_is_final=True,
        )

    def _list_table_metadata(
        self,
        catalog_name: str | None = None,
        schema_name: str | None = None,
        expression: str | None = None,
        next_token: str | None = None,
        max_results: int | None = None,
        logging_: bool = True,
        stop_on: Callable[[BaseException], bool] | None = None,
    ) -> tuple[str | None, list[AthenaTableMetadata]]:
        request = self._build_list_table_metadata_request(
            catalog_name=catalog_name,
            schema_name=schema_name,
            expression=expression,
            next_token=next_token,
            max_results=max_results,
        )
        try:
            response = _retry_api_call(
                self.connection._client.list_table_metadata,
                self._retry_config,
                _logger,
                stop_on,
                **request,
            )
        except Exception as e:
            if logging_:
                _logger.exception("Failed to list table metadata.")
            raise OperationalError(*e.args) from e
        else:
            return response.get("NextToken"), [
                AthenaTableMetadata({"TableMetadata": r})
                for r in response.get("TableMetadataList", [])
            ]

    def list_table_metadata(
        self,
        catalog_name: str | None = None,
        schema_name: str | None = None,
        expression: str | None = None,
        max_results: int | None = None,
        logging_: bool = True,
    ) -> list[AthenaTableMetadata]:
        schema_name = schema_name if schema_name else self._schema_name
        # Pages already read are kept, so a retried request resumes after them.
        metadata: list[AthenaTableMetadata] = []
        next_token = None

        def athena_request(
            stop_on: Callable[[BaseException], bool] | None, logging_: bool
        ) -> list[AthenaTableMetadata]:
            nonlocal next_token
            while True:
                next_token, response = self._list_table_metadata(
                    catalog_name=catalog_name,
                    schema_name=schema_name,
                    expression=expression,
                    next_token=next_token,
                    max_results=max_results,
                    logging_=logging_,
                    stop_on=stop_on,
                )
                metadata.extend(response)
                if not next_token:
                    return metadata

        return self._with_glue_fallback(
            catalog_name,
            athena_request,
            lambda glue: glue.list_tables(schema_name, expression),
            "list table metadata",
            logging_=logging_,
        )

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

    def _batch_get_query_execution(self, query_ids: list[str]) -> list[AthenaQueryExecution]:
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
        work_group: str | None = None,
        next_token: str | None = None,
        max_results: int | None = None,
    ) -> tuple[str | None, list[AthenaQueryExecution]]:
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

    def __poll(self, query_id: str) -> AthenaQueryExecution | AthenaCalculationExecution:
        while True:
            query_execution = self._get_query_execution(query_id)
            if self._on_poll:
                self._on_poll(query_execution)
            if query_execution.state in [
                AthenaQueryExecution.STATE_SUCCEEDED,
                AthenaQueryExecution.STATE_FAILED,
                AthenaQueryExecution.STATE_CANCELLED,
            ]:
                return query_execution
            time.sleep(self._poll_interval)

    def _poll(self, query_id: str) -> AthenaQueryExecution | AthenaCalculationExecution:
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
        work_group: str | None,
        cache_size: int = 0,
        cache_expiration_time: int = 0,
    ) -> str | None:
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
                    key=lambda e: e.completion_date_time,  # type: ignore[arg-type, return-value]
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
                    if (
                        execution.query == query
                        and execution.database == self._schema_name
                        and (execution.catalog or "").lower() == (self._catalog_name or "").lower()
                    ):
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
        parameters: dict[str, Any] | list[str] | None = None,
        paramstyle: str | None = None,
    ) -> tuple[str, list[str] | None]:
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
            execution_parameters = cast(list[str] | None, parameters)
        else:
            query = self._formatter.format(operation, cast(dict[str, Any] | None, parameters))
            execution_parameters = None
        _logger.debug(query)
        return query, execution_parameters

    def _prepare_unload(
        self,
        operation: str,
        s3_staging_dir: str | None,
    ) -> tuple[str, str | None]:
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

    def _call_on_start_query_execution(self, query_id: str, options: ExecuteOptions) -> None:
        """Invoke the connection-level and execute-level query-start callbacks.

        Both callbacks are invoked if set. Called by cursors whose execution
        model supports early access to the query ID (the synchronous and aio
        cursors) immediately after the StartQueryExecution API call.
        """
        if self._on_start_query_execution:
            self._on_start_query_execution(query_id)
        if options.on_start_query_execution:
            options.on_start_query_execution(query_id)

    def _execute(
        self,
        operation: str,
        parameters: dict[str, Any] | list[str] | None = None,
        work_group: str | None = None,
        s3_staging_dir: str | None = None,
        cache_size: int | None = None,
        cache_expiration_time: int | None = None,
        result_reuse_enable: bool | None = None,
        result_reuse_minutes: int | None = None,
        paramstyle: str | None = None,
        options: ExecuteOptions | None = None,
    ) -> str:
        # The individual keyword arguments are retained for backward compatibility
        # with external callers that predate ExecuteOptions (e.g. dbt-athena <= 1.10.x
        # calls _execute() with work_group/s3_staging_dir/cache_* keywords).
        options = ExecuteOptions.resolve(
            options,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
            cache_expiration_time=cache_expiration_time,
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            paramstyle=paramstyle,
        )
        query, execution_parameters = self._prepare_query(operation, parameters, options.paramstyle)

        request = self._build_start_query_execution_request(
            query=query,
            work_group=options.work_group,
            s3_staging_dir=options.s3_staging_dir,
            result_reuse_enable=options.result_reuse_enable,
            result_reuse_minutes=options.result_reuse_minutes,
            execution_parameters=execution_parameters,
        )
        query_id = self._find_previous_query_id(
            query,
            options.work_group,
            cache_size=options.cache_size,
            cache_expiration_time=options.cache_expiration_time,
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
        description: str | None = None,
        client_request_token: str | None = None,
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
        parameters: dict[str, Any] | list[str] | None = None,
        **kwargs,
    ):
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def executemany(
        self,
        operation: str,
        seq_of_parameters: list[dict[str, Any] | list[str] | None],
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

    def setoutputsize(self, size, column=None):  # noqa: B027
        """Does nothing by default"""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
