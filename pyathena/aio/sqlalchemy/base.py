# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, MutableMapping, Optional, Tuple, Union, cast

from sqlalchemy import pool
from sqlalchemy.engine import AdaptedConnection
from sqlalchemy.util.concurrency import await_only

import pyathena
from pyathena.aio.connection import AioConnection
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

if TYPE_CHECKING:
    from types import ModuleType

    from sqlalchemy import URL


class AsyncAdapt_pyathena_cursor:  # noqa: N801 - follows SQLAlchemy's internal async adapter naming convention (e.g. AsyncAdapt_asyncpg_dbapi)
    """Wraps any async PyAthena cursor with a sync DBAPI interface.

    SQLAlchemy's async engine uses greenlet-based ``await_only()`` to call
    async methods from synchronous code running inside the greenlet context.
    This adapter wraps an ``AioCursor`` (or variant) so that the dialect can
    use a normal synchronous DBAPI interface while the underlying I/O is async.
    """

    server_side = False
    __slots__ = ("_cursor",)

    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor

    @property
    def description(self) -> Any:
        return self._cursor.description

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount  # type: ignore[no-any-return]

    def close(self) -> None:
        self._cursor.close()

    def execute(self, operation: str, parameters: Any = None, **kwargs: Any) -> Any:
        return await_only(self._cursor.execute(operation, parameters, **kwargs))

    def executemany(
        self,
        operation: str,
        seq_of_parameters: List[Optional[Union[Dict[str, Any], List[str]]]],
        **kwargs: Any,
    ) -> None:
        for parameters in seq_of_parameters:
            await_only(self._cursor.execute(operation, parameters, **kwargs))

    def fetchone(self) -> Any:
        return await_only(self._cursor.fetchone())

    def fetchmany(self, size: Optional[int] = None) -> Any:
        return await_only(self._cursor.fetchmany(size))

    def fetchall(self) -> Any:
        return await_only(self._cursor.fetchall())

    def setinputsizes(self, sizes: Any) -> None:
        self._cursor.setinputsizes(sizes)

    # PyAthena-specific methods used by AthenaDialect reflection
    def list_databases(self, *args: Any, **kwargs: Any) -> Any:
        return await_only(self._cursor.list_databases(*args, **kwargs))

    def get_table_metadata(self, *args: Any, **kwargs: Any) -> Any:
        return await_only(self._cursor.get_table_metadata(*args, **kwargs))

    def list_table_metadata(self, *args: Any, **kwargs: Any) -> Any:
        return await_only(self._cursor.list_table_metadata(*args, **kwargs))

    def __enter__(self) -> "AsyncAdapt_pyathena_cursor":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


class AsyncAdapt_pyathena_connection(AdaptedConnection):  # noqa: N801 - follows SQLAlchemy's internal async adapter naming convention (e.g. AsyncAdapt_asyncpg_dbapi)
    """Wraps ``AioConnection`` with a sync DBAPI interface.

    This adapted connection delegates ``cursor()`` to the underlying
    ``AioConnection`` and wraps each returned async cursor with
    ``AsyncAdapt_pyathena_cursor``.
    """

    await_only_ = staticmethod(await_only)

    __slots__ = ("dbapi", "_connection")

    def __init__(self, dbapi: "AsyncAdapt_pyathena_dbapi", connection: AioConnection) -> None:
        self.dbapi = dbapi
        self._connection = connection

    @property
    def driver_connection(self) -> AioConnection:
        return self._connection  # type: ignore[no-any-return]

    @property
    def catalog_name(self) -> Optional[str]:
        return self._connection.catalog_name  # type: ignore[no-any-return]

    @property
    def schema_name(self) -> Optional[str]:
        return self._connection.schema_name  # type: ignore[no-any-return]

    def cursor(self) -> AsyncAdapt_pyathena_cursor:
        raw_cursor = self._connection.cursor()
        return AsyncAdapt_pyathena_cursor(raw_cursor)

    def close(self) -> None:
        self._connection.close()

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        pass


class AsyncAdapt_pyathena_dbapi:  # noqa: N801 - follows SQLAlchemy's internal async adapter naming convention (e.g. AsyncAdapt_asyncpg_dbapi)
    """Fake DBAPI module for the async SQLAlchemy engine.

    SQLAlchemy expects ``import_dbapi()`` to return a module-like object
    with ``connect()``, ``paramstyle``, and the standard DBAPI exception
    hierarchy.  This class fulfils that contract while routing connections
    through ``AioConnection``.
    """

    paramstyle = "pyformat"

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
        connection = await_only(AioConnection.create(**kwargs))
        return AsyncAdapt_pyathena_connection(self, connection)


class AthenaAioDialect(AthenaDialect):
    """Base async SQLAlchemy dialect for Amazon Athena.

    Extends the synchronous ``AthenaDialect`` with async capability
    by setting ``is_async = True`` and providing an adapted DBAPI module
    that wraps ``AioConnection`` and async cursors.

    Connection URL Format:
        ``awsathena+aiorest://{access_key}:{secret_key}@athena.{region}.amazonaws.com/{schema}``

    Example:
        >>> from sqlalchemy.ext.asyncio import create_async_engine
        >>> engine = create_async_engine(
        ...     "awsathena+aiorest://:@athena.us-west-2.amazonaws.com/default"
        ...     "?s3_staging_dir=s3://my-bucket/athena-results/"
        ... )

    See Also:
        :class:`~pyathena.sqlalchemy.base.AthenaDialect`: Synchronous base dialect.
        :class:`~pyathena.aio.connection.AioConnection`: Native async connection.
    """

    is_async = True
    supports_statement_cache = True

    @classmethod
    def get_pool_class(cls, url: "URL") -> type:
        return pool.AsyncAdaptedQueuePool

    @classmethod
    def import_dbapi(cls) -> "ModuleType":
        return AsyncAdapt_pyathena_dbapi()  # type: ignore[return-value]

    @classmethod
    def dbapi(cls) -> "ModuleType":  # type: ignore[override]
        return AsyncAdapt_pyathena_dbapi()  # type: ignore[return-value]

    def create_connect_args(self, url: "URL") -> Tuple[Tuple[str], MutableMapping[str, Any]]:
        opts = self._create_connect_args(url)
        self._connect_options = opts
        return cast(Tuple[str], ()), opts

    def get_driver_connection(self, connection: Any) -> Any:
        return connection
