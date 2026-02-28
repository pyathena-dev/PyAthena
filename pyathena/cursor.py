from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, cast

from pyathena.common import CursorIterator
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaDictResultSet, AthenaResultSet, WithFetch

_logger = logging.getLogger(__name__)


class Cursor(WithFetch):
    """A DB API 2.0 compliant cursor for executing SQL queries on Amazon Athena.

    The Cursor class provides methods for executing SQL queries against Amazon Athena
    and retrieving results. It follows the Python Database API Specification v2.0
    (PEP 249) and provides familiar database cursor operations.

    This cursor returns results as tuples by default. For other data formats,
    consider using specialized cursor classes like PandasCursor or ArrowCursor.

    Attributes:
        description: Sequence of column descriptions for the last query.
        rowcount: Number of rows affected by the last query (-1 for SELECT queries).
        arraysize: Default number of rows to fetch with fetchmany().

    Example:
        >>> cursor = connection.cursor()
        >>> cursor.execute("SELECT name, age FROM users WHERE age > %s", (18,))
        >>> while True:
        ...     row = cursor.fetchone()
        ...     if not row:
        ...         break
        ...     print(f"Name: {row[0]}, Age: {row[1]}")

        >>> cursor.execute("CREATE TABLE test AS SELECT 1 as id, 'test' as name")
        >>> print(f"Created table, rows affected: {cursor.rowcount}")
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
        **kwargs,
    ) -> None:
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
        self._result_set_class = AthenaResultSet
        self._on_start_query_execution = on_start_query_execution

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

    def execute(
        self,
        operation: str,
        parameters: dict[str, Any] | list[str] | None = None,
        work_group: str | None = None,
        s3_staging_dir: str | None = None,
        cache_size: int = 0,
        cache_expiration_time: int = 0,
        result_reuse_enable: bool | None = None,
        result_reuse_minutes: int | None = None,
        paramstyle: str | None = None,
        on_start_query_execution: Callable[[str], None] | None = None,
        result_set_type_hints: dict[str, str] | None = None,
        **kwargs,
    ) -> Cursor:
        """Execute a SQL query.

        Args:
            operation: SQL query string to execute.
            parameters: Query parameters (optional).
            on_start_query_execution: Callback function called immediately after
                start_query_execution API is called.
                Function signature: (query_id: str) -> None
                This allows early access to query_id for
                monitoring/cancellation.
            result_set_type_hints: Optional dictionary mapping column names to
                Athena DDL type signatures for precise type conversion within
                complex types. For example:
                ``{"tags": "array(varchar)", "metadata": "map(varchar, integer)"}``
            **kwargs: Additional execution parameters.

        Returns:
            Self reference for method chaining.

        Example:
            >>> cursor.execute(
            ...     "SELECT * FROM table_with_complex_types",
            ...     result_set_type_hints={
            ...         "tags": "array(varchar)",
            ...         "metadata": "map(varchar, integer)",
            ...     }
            ... )
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
        # Both connection-level and execute-level callbacks are invoked if set
        if self._on_start_query_execution:
            self._on_start_query_execution(self.query_id)
        if on_start_query_execution:
            on_start_query_execution(self.query_id)

        query_execution = cast(AthenaQueryExecution, self._poll(self.query_id))
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self.result_set = self._result_set_class(
                self._connection,
                self._converter,
                query_execution,
                self.arraysize,
                self._retry_config,
                result_set_type_hints=result_set_type_hints,
            )
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self


class DictCursor(Cursor):
    """A cursor that returns query results as dictionaries instead of tuples.

    DictCursor provides the same functionality as the standard Cursor but
    returns rows as dictionaries where column names are keys. This makes
    it easier to access column values by name rather than position.

    Example:
        >>> cursor = connection.cursor(DictCursor)
        >>> cursor.execute("SELECT id, name, email FROM users LIMIT 1")
        >>> row = cursor.fetchone()
        >>> print(f"User: {row['name']} ({row['email']})")

        >>> cursor.execute("SELECT * FROM products")
        >>> for row in cursor.fetchall():
        ...     print(f"Product {row['id']}: {row['name']} - ${row['price']}")
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._result_set_class = AthenaDictResultSet
        if "dict_type" in kwargs:
            AthenaDictResultSet.dict_type = kwargs["dict_type"]
