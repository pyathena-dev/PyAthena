"""DB API 2.0 cursors that return rows as tuples or dictionaries."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, cast

from pyathena.common import CursorIterator
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.options import ExecuteOptions
from pyathena.result_set import AthenaDictResultSet, AthenaResultSet, WithFetch
from pyathena.util import override

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
        **kwargs,
    ) -> None:
        """Initialize a Cursor.

        Args:
            s3_staging_dir: S3 location for query results.
            schema_name: Default schema name.
            catalog_name: Default catalog name.
            work_group: Athena workgroup name.
            poll_interval: Query status polling interval in seconds.
            encryption_option: S3 encryption option (SSE_S3, SSE_KMS, CSE_KMS).
            kms_key: KMS key for encryption.
            kill_on_interrupt: Cancel the query when a ``KeyboardInterrupt`` interrupts
                ``execute()`` while it starts or waits for the query.
            result_reuse_enable: Enable Athena query result reuse.
            result_reuse_minutes: Maximum age in minutes of a reused result.
            **kwargs: Arguments forwarded to ``WithResultSet.__init__`` and
                ``BaseCursor.__init__``, such as ``arraysize``, ``connection``,
                ``converter``, ``formatter``, and ``retry_config``.
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
        self._result_set_class = AthenaResultSet

    @property  # type: ignore[explicit-override]  # python/mypy#15900
    @override
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value <= 0 or value > self.DEFAULT_FETCH_SIZE:
            raise ProgrammingError(
                f"MaxResults is more than maximum allowed length {self.DEFAULT_FETCH_SIZE}."
            )
        self._arraysize = value

    @override
    def execute(
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
        on_start_query_execution: Callable[[str], None] | None = None,
        result_set_type_hints: dict[str | int, str] | None = None,
        *,
        options: ExecuteOptions | None = None,
        **kwargs,
    ) -> Cursor:
        """Execute a SQL query.

        Args:
            operation: SQL query string to execute.
            parameters: Query parameters (optional).
            work_group: Athena workgroup to use for this query.
            s3_staging_dir: S3 location for query results.
            cache_size: Number of queries to check for result caching.
            cache_expiration_time: Cache expiration time in seconds.
            result_reuse_enable: Enable Athena result reuse for this query.
            result_reuse_minutes: Minutes to reuse cached results.
            paramstyle: Parameter style ('qmark' or 'pyformat').
            on_start_query_execution: Callback invoked with the query ID before ``execute()``
                waits for the query: after the ``StartQueryExecution`` call, or after a
                reusable query ID is found through ``cache_size``.
                Function signature: (query_id: str) -> None
                This allows early access to query_id for
                monitoring/cancellation.
            result_set_type_hints: Optional dictionary mapping column names to
                Athena DDL type signatures for precise type conversion within
                complex types. For example:
                ``{"tags": "array(varchar)", "metadata": "map(varchar, integer)"}``
            options: Shared execution options as an
                :class:`~pyathena.options.ExecuteOptions` instance. Individual
                keyword arguments take precedence over ``options`` fields.
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
        options = ExecuteOptions.resolve(
            options,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
            cache_expiration_time=cache_expiration_time,
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            paramstyle=paramstyle,
            on_start_query_execution=on_start_query_execution,
            result_set_type_hints=result_set_type_hints,
        )
        self.query_id = self._execute(
            operation,
            parameters=parameters,
            options=options,
        )

        # Call user callbacks immediately after start_query_execution
        self._call_on_start_query_execution(self.query_id, options)

        query_execution = cast(AthenaQueryExecution, self._poll(self.query_id))
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self.result_set = self._result_set_class(
                self._connection,
                self._converter,
                query_execution,
                self.arraysize,
                self._retry_config,
                result_set_type_hints=options.result_set_type_hints,
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
        """Initialize a DictCursor.

        Args:
            **kwargs: Arguments forwarded to ``Cursor.__init__``. If they include
                ``dict_type``, it is the type used to build each row of this
                cursor's result sets; other cursors are not affected.
        """
        self._dict_type: type[Any] | None = kwargs.get("dict_type")
        super().__init__(**kwargs)
        self._result_set_class = AthenaDictResultSet

    @property  # type: ignore[explicit-override]  # python/mypy#15900
    @override
    def _result_set_class(self) -> type[AthenaResultSet]:
        """The result set class this cursor instantiates for each query.

        Returns:
            The class last assigned to this property, or a subclass of it
            that carries this cursor's ``dict_type``.
        """
        return self._dict_result_set_class

    @_result_set_class.setter
    def _result_set_class(self, value: type[AthenaResultSet]) -> None:
        """Set the result set class this cursor instantiates for each query.

        If this cursor was given ``dict_type`` and ``value`` is a subclass of
        ``AthenaDictResultSet``, a subclass of ``value`` whose ``dict_type`` is
        that type is stored instead, so that ``value`` itself is not modified.

        Args:
            value: The result set class to instantiate.
        """
        if self._dict_type is not None and issubclass(value, AthenaDictResultSet):
            value = cast(
                type[AthenaResultSet],
                type(
                    value.__name__,
                    (value,),
                    {"__module__": value.__module__, "dict_type": self._dict_type},
                ),
            )
        self._dict_result_set_class = value
