# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Async SQLAlchemy dialect base and DBAPI adapters for PyAthena asyncio cursors."""

from __future__ import annotations

from collections import deque
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import pool
from sqlalchemy.engine import AdaptedConnection
from sqlalchemy.util.concurrency import await_only

import pyathena
from pyathena.aio.connection import AioConnection
from pyathena.aio.cursor import AioCursor
from pyathena.cursor import Cursor
from pyathena.error import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from pyathena.sqlalchemy.base import AthenaDialect
from pyathena.util import RetryConfig, override

if TYPE_CHECKING:
    from types import ModuleType

    from sqlalchemy import URL


_ASYNC_CURSOR_CLASSES: dict[Any, Any] = {Cursor: AioCursor}


class AsyncAdapt_pyathena_cursor:
    """Wraps any async PyAthena cursor with a sync DBAPI interface.

    SQLAlchemy's async engine uses greenlet-based ``await_only()`` to call
    async methods from synchronous code running inside the greenlet context.
    This adapter wraps an ``AioCursor`` (or variant) so that the dialect can
    use a normal synchronous DBAPI interface while the underlying I/O is async.
    """

    server_side = False
    __slots__ = ("_cursor", "_rows")

    def __init__(self, cursor: Any) -> None:
        """Initialize the adapter around an async cursor.

        Args:
            cursor: The async PyAthena cursor to wrap.
        """
        self._cursor = cursor
        self._rows: deque[Any] = deque()

    @property
    def description(self) -> Any:
        """The ``description`` of the wrapped cursor."""
        return self._cursor.description

    @property
    def rowcount(self) -> int:
        """The ``rowcount`` of the wrapped cursor."""
        return self._cursor.rowcount  # type: ignore[no-any-return]

    def close(self) -> None:
        """Close the wrapped cursor and discard any buffered rows."""
        self._cursor.close()
        self._rows.clear()

    def execute(self, operation: str, parameters: Any = None, **kwargs: Any) -> Any:
        """Execute a statement and buffer all of its result rows.

        When the statement produces a result set (the cursor has a
        ``description``), every row is fetched and buffered so that the fetch
        methods can return rows without awaiting.

        Args:
            operation: The SQL statement to execute.
            parameters: Parameters to bind to the statement.
            **kwargs: Additional keyword arguments forwarded to the wrapped
                cursor's ``execute()``.

        Returns:
            The value returned by the wrapped cursor's ``execute()``.
        """
        result = await_only(self._cursor.execute(operation, parameters, **kwargs))
        if self._cursor.description:
            self._rows = deque(await_only(self._cursor.fetchall()))
        else:
            self._rows.clear()
        return result

    def executemany(
        self,
        operation: str,
        seq_of_parameters: list[dict[str, Any] | list[str] | None],
        **kwargs: Any,
    ) -> None:
        """Execute a statement once for each parameter set.

        Any buffered rows are discarded first.

        Args:
            operation: The SQL statement to execute.
            seq_of_parameters: The parameter sets to bind, one per execution.
            **kwargs: Additional keyword arguments forwarded to the wrapped
                cursor's ``executemany()``.
        """
        self._rows.clear()
        await_only(self._cursor.executemany(operation, seq_of_parameters, **kwargs))

    def fetchone(self) -> Any:
        """Fetch the next buffered row.

        Returns:
            The next row, or ``None`` when no rows remain.
        """
        if self._rows:
            return self._rows.popleft()
        return None

    def fetchmany(self, size: int | None = None) -> Any:
        """Fetch up to ``size`` buffered rows.

        Args:
            size: Maximum number of rows to fetch. If ``None``, the wrapped
                cursor's ``arraysize`` is used, or 1 if it has none.

        Returns:
            A list of rows, empty when no rows remain.
        """
        if size is None:
            size = self._cursor.arraysize if hasattr(self._cursor, "arraysize") else 1
        return [self._rows.popleft() for _ in range(min(size, len(self._rows)))]

    def fetchall(self) -> Any:
        """Fetch all remaining buffered rows.

        Returns:
            A list of the remaining rows.
        """
        items = list(self._rows)
        self._rows.clear()
        return items

    def setinputsizes(self, sizes: Any) -> None:
        """Forward ``sizes`` to the wrapped cursor's ``setinputsizes()``.

        Args:
            sizes: Sequence of parameter types or sizes.
        """
        self._cursor.setinputsizes(sizes)

    async def _async_soft_close(self) -> None:
        return

    # PyAthena-specific methods used by AthenaDialect reflection
    def list_databases(self, *args: Any, **kwargs: Any) -> Any:
        """Await the wrapped cursor's ``list_databases()`` and return its result.

        Args:
            *args: Positional arguments forwarded to ``list_databases()``.
            **kwargs: Keyword arguments forwarded to ``list_databases()``.

        Returns:
            The result of the wrapped cursor's ``list_databases()``.
        """
        return await_only(self._cursor.list_databases(*args, **kwargs))

    def get_table_metadata(self, *args: Any, **kwargs: Any) -> Any:
        """Await the wrapped cursor's ``get_table_metadata()`` and return its result.

        Args:
            *args: Positional arguments forwarded to ``get_table_metadata()``.
            **kwargs: Keyword arguments forwarded to ``get_table_metadata()``.

        Returns:
            The result of the wrapped cursor's ``get_table_metadata()``.
        """
        return await_only(self._cursor.get_table_metadata(*args, **kwargs))

    def list_table_metadata(self, *args: Any, **kwargs: Any) -> Any:
        """Await the wrapped cursor's ``list_table_metadata()`` and return its result.

        Args:
            *args: Positional arguments forwarded to ``list_table_metadata()``.
            **kwargs: Keyword arguments forwarded to ``list_table_metadata()``.

        Returns:
            The result of the wrapped cursor's ``list_table_metadata()``.
        """
        return await_only(self._cursor.list_table_metadata(*args, **kwargs))

    def __enter__(self) -> AsyncAdapt_pyathena_cursor:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


class AsyncAdapt_pyathena_connection(AdaptedConnection):
    """Wraps ``AioConnection`` with a sync DBAPI interface.

    This adapted connection delegates ``cursor()`` to the underlying
    ``AioConnection`` and wraps each returned async cursor with
    ``AsyncAdapt_pyathena_cursor``.
    """

    __slots__ = ("_connection", "dbapi")

    def __init__(self, dbapi: AsyncAdapt_pyathena_dbapi, connection: AioConnection) -> None:
        """Initialize the adapted connection.

        Args:
            dbapi: The adapted DBAPI module that created this connection.
            connection: The ``AioConnection`` to wrap.
        """
        self.dbapi = dbapi
        self._connection = connection  # type: ignore[assignment]

    @property
    @override
    def driver_connection(self) -> AioConnection:
        return self._connection  # type: ignore[return-value]

    @property
    def catalog_name(self) -> str | None:
        """The catalog name of the wrapped connection."""
        return self._connection.catalog_name  # type: ignore[no-any-return]

    @property
    def schema_name(self) -> str | None:
        """The schema name of the wrapped connection."""
        return self._connection.schema_name  # type: ignore[no-any-return]

    @property
    def cursor_kwargs(self) -> dict[str, Any]:
        """The default cursor keyword arguments of the wrapped connection."""
        return self._connection.cursor_kwargs  # type: ignore[no-any-return]

    @property
    def retry_config(self) -> RetryConfig:
        """The retry configuration of the wrapped connection."""
        return self._connection.retry_config  # type: ignore[no-any-return]

    def cursor(self, cursor: Any = None, **kwargs: Any) -> AsyncAdapt_pyathena_cursor:
        """Create an async cursor on the wrapped connection and adapt it.

        A synchronous cursor class that has a registered async counterpart is
        replaced with that counterpart; any other value is passed through.

        Args:
            cursor: The cursor class to create, or ``None`` for the
                connection's default cursor class.
            **kwargs: Keyword arguments forwarded to the wrapped connection's
                ``cursor()``.

        Returns:
            The created cursor wrapped in ``AsyncAdapt_pyathena_cursor``.
        """
        # The shared dialect names a cursor class in its synchronous form; this
        # connection can only drive the async counterpart.
        raw_cursor = self._connection.cursor(_ASYNC_CURSOR_CLASSES.get(cursor, cursor), **kwargs)
        return AsyncAdapt_pyathena_cursor(raw_cursor)

    def _internal_cursor(self, cursor: Any) -> AsyncAdapt_pyathena_cursor:
        """Create a cursor with the wrapped connection's ``_internal_cursor()`` and adapt it.

        A synchronous cursor class is replaced as in ``cursor()``.

        Args:
            cursor: The cursor class to create.

        Returns:
            The created cursor wrapped in ``AsyncAdapt_pyathena_cursor``.
        """
        raw_cursor = self._connection._internal_cursor(_ASYNC_CURSOR_CLASSES.get(cursor, cursor))
        return AsyncAdapt_pyathena_cursor(raw_cursor)

    def close(self) -> None:
        """Close the wrapped connection."""
        self._connection.close()

    def commit(self) -> None:
        """Call ``commit()`` on the wrapped connection."""
        self._connection.commit()  # type: ignore[unused-coroutine]

    def rollback(self) -> None:
        """Do nothing, because Athena does not support transactions."""


class AsyncAdapt_pyathena_dbapi:
    """Fake DBAPI module for the async SQLAlchemy engine.

    SQLAlchemy expects ``import_dbapi()`` to return a module-like object
    with ``connect()``, ``paramstyle``, and the standard DBAPI exception
    hierarchy.  This class fulfils that contract while routing connections
    through ``AioConnection``.
    """

    paramstyle = "pyformat"
    Binary = pyathena.Binary
    BINARY = pyathena.BINARY

    # DBAPI exception hierarchy
    Error = Error
    Warning = pyathena.Warning
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    InternalError = InternalError
    OperationalError = OperationalError
    ProgrammingError = ProgrammingError
    IntegrityError = IntegrityError
    DataError = DataError
    NotSupportedError = NotSupportedError

    def connect(self, **kwargs: Any) -> AsyncAdapt_pyathena_connection:
        """Create an ``AioConnection`` and wrap it in an adapted connection.

        Args:
            **kwargs: Keyword arguments forwarded to ``AioConnection.create()``.

        Returns:
            The new connection wrapped in ``AsyncAdapt_pyathena_connection``.
        """
        connection = await_only(AioConnection.create(**kwargs))
        return AsyncAdapt_pyathena_connection(self, connection)


class AthenaAioDialect(AthenaDialect):
    """Base async SQLAlchemy dialect for Amazon Athena.

    Extends the synchronous ``AthenaDialect`` with async capability
    by setting ``is_async = True`` and providing an adapted DBAPI module
    that wraps ``AioConnection`` and async cursors via greenlet-based
    ``await_only()``.

    Subclasses (e.g. ``AthenaAioRestDialect``, ``AthenaAioPandasDialect``)
    register concrete ``awsathena+aio*`` drivers.

    See Also:
        :class:`~pyathena.sqlalchemy.base.AthenaDialect`: Synchronous base dialect.
        :class:`~pyathena.aio.connection.AioConnection`: Native async connection.
    """

    is_async = True
    supports_statement_cache = True

    @classmethod
    @override
    def get_pool_class(cls, url: URL) -> type:
        return pool.AsyncAdaptedQueuePool

    @classmethod
    @override
    def import_dbapi(cls) -> ModuleType:
        return AsyncAdapt_pyathena_dbapi()  # type: ignore[return-value]

    @classmethod
    @override
    def dbapi(cls) -> ModuleType:  # type: ignore[override]
        return AsyncAdapt_pyathena_dbapi()  # type: ignore[return-value]

    @override
    def create_connect_args(self, url: URL) -> tuple[tuple[str], MutableMapping[str, Any]]:
        opts = self._create_connect_args(url)
        self._connect_options = opts
        return cast(tuple[str], ()), opts

    @override
    def get_driver_connection(self, connection: Any) -> Any:
        return connection
