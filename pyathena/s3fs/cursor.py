from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, cast

from pyathena.common import CursorIterator
from pyathena.error import OperationalError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import WithFetch
from pyathena.s3fs.converter import DefaultS3FSTypeConverter
from pyathena.s3fs.result_set import AthenaS3FSResultSet, CSVReaderType

_logger = logging.getLogger(__name__)


class S3FSCursor(WithFetch):
    """Cursor for reading CSV results via S3FileSystem without pandas/pyarrow.

    This cursor uses Python's standard csv module and PyAthena's S3FileSystem
    to read query results from S3. It provides a lightweight alternative to
    pandas and arrow cursors when those dependencies are not needed.

    The cursor is especially useful for:
        - Environments where pandas/pyarrow installation is not desired
        - Simple queries where advanced data processing is not required
        - Memory-constrained environments

    Attributes:
        description: Sequence of column descriptions for the last query.
        rowcount: Number of rows affected by the last query (-1 for SELECT queries).
        arraysize: Default number of rows to fetch with fetchmany().

    Example:
        >>> from pyathena.s3fs.cursor import S3FSCursor
        >>> cursor = connection.cursor(S3FSCursor)
        >>> cursor.execute("SELECT * FROM my_table")
        >>> rows = cursor.fetchall()  # Returns list of tuples
        >>>
        >>> # Iterate over results
        >>> for row in cursor.execute("SELECT * FROM my_table"):
        ...     print(row)

        # Use with SQLAlchemy
        >>> from sqlalchemy import create_engine
        >>> engine = create_engine("awsathena+s3fs://...")
    """

    def __init__(
        self,
        s3_staging_dir: str | None = None,
        schema_name: str | None = None,
        catalog_name: str | None = None,
        work_group: str | None = None,
        poll_interval: float = 1,
        encryption_option: str | None = None,
        kms_key: str | None = None,
        kill_on_interrupt: bool = True,
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
        on_start_query_execution: Callable[[str], None] | None = None,
        csv_reader: CSVReaderType | None = None,
        **kwargs,
    ) -> None:
        """Initialize an S3FSCursor.

        Args:
            s3_staging_dir: S3 location for query results.
            schema_name: Default schema name.
            catalog_name: Default catalog name.
            work_group: Athena workgroup name.
            poll_interval: Query status polling interval in seconds.
            encryption_option: S3 encryption option (SSE_S3, SSE_KMS, CSE_KMS).
            kms_key: KMS key ARN for encryption.
            kill_on_interrupt: Cancel running query on keyboard interrupt.
            result_reuse_enable: Enable Athena query result reuse.
            result_reuse_minutes: Minutes to reuse cached results.
            on_start_query_execution: Callback invoked when query starts.
            csv_reader: CSV reader class to use for parsing results.
                Use AthenaCSVReader (default) to distinguish between NULL
                (unquoted empty) and empty string (quoted empty "").
                Use DefaultCSVReader for backward compatibility where empty
                strings are treated as NULL.
            **kwargs: Additional connection parameters.

        Example:
            >>> cursor = connection.cursor(S3FSCursor)
            >>> cursor.execute("SELECT * FROM my_table")
            >>>
            >>> # Use DefaultCSVReader for backward compatibility
            >>> from pyathena.s3fs.reader import DefaultCSVReader
            >>> cursor = connection.cursor(S3FSCursor, csv_reader=DefaultCSVReader)
        """
        super().__init__(
            s3_staging_dir=s3_staging_dir,
            schema_name=schema_name,
            catalog_name=catalog_name,
            work_group=work_group,
            poll_interval=poll_interval,
            encryption_option=encryption_option,
            kms_key=kms_key,
            kill_on_interrupt=kill_on_interrupt,
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            **kwargs,
        )
        self._on_start_query_execution = on_start_query_execution
        self._csv_reader = csv_reader

    @staticmethod
    def get_default_converter(
        unload: bool = False,
    ) -> DefaultS3FSTypeConverter:
        """Get the default type converter for S3FS cursor.

        Args:
            unload: Unused. S3FS cursor does not support UNLOAD operations.

        Returns:
            DefaultS3FSTypeConverter instance.
        """
        return DefaultS3FSTypeConverter()

    def execute(
        self,
        operation: str,
        parameters: dict[str, Any] | list[str] | None = None,
        work_group: str | None = None,
        s3_staging_dir: str | None = None,
        cache_size: int | None = 0,
        cache_expiration_time: int | None = 0,
        result_reuse_enable: bool | None = None,
        result_reuse_minutes: int | None = None,
        paramstyle: str | None = None,
        on_start_query_execution: Callable[[str], None] | None = None,
        result_set_type_hints: dict[str, str] | None = None,
        **kwargs,
    ) -> S3FSCursor:
        """Execute a SQL query and return results.

        Executes the SQL query on Amazon Athena and configures the result set
        for CSV-based output via S3FileSystem.

        Args:
            operation: SQL query string to execute.
            parameters: Query parameters for parameterized queries.
            work_group: Athena workgroup to use for this query.
            s3_staging_dir: S3 location for query results.
            cache_size: Number of queries to check for result caching.
            cache_expiration_time: Cache expiration time in seconds.
            result_reuse_enable: Enable Athena result reuse for this query.
            result_reuse_minutes: Minutes to reuse cached results.
            paramstyle: Parameter style ('qmark' or 'pyformat').
            on_start_query_execution: Callback called when query starts.
            result_set_type_hints: Optional dictionary mapping column names to
                Athena DDL type signatures for precise type conversion within
                complex types.
            **kwargs: Additional execution parameters.

        Returns:
            Self reference for method chaining.

        Example:
            >>> cursor.execute("SELECT * FROM my_table WHERE id = %(id)s", {"id": 123})
            >>> rows = cursor.fetchall()
        """
        self._reset_state()
        self.query_id = self._execute(
            operation,
            parameters=parameters,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
            cache_expiration_time=cache_expiration_time,
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            paramstyle=paramstyle,
        )

        # Call user callbacks immediately after start_query_execution
        if self._on_start_query_execution:
            self._on_start_query_execution(self.query_id)
        if on_start_query_execution:
            on_start_query_execution(self.query_id)

        query_execution = cast(AthenaQueryExecution, self._poll(self.query_id))
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self.result_set = AthenaS3FSResultSet(
                connection=self._connection,
                converter=self._converter,
                query_execution=query_execution,
                arraysize=self.arraysize,
                retry_config=self._retry_config,
                csv_reader=self._csv_reader,
                result_set_type_hints=result_set_type_hints,
                **kwargs,
            )
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self
