# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, cast

from pyathena.aio.base import AioCursorBase
from pyathena.common import CursorIterator
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.s3fs.converter import DefaultS3FSTypeConverter
from pyathena.s3fs.result_set import AthenaS3FSResultSet, CSVReaderType

_logger = logging.getLogger(__name__)  # type: ignore


class AioS3FSCursor(AioCursorBase):
    """Native asyncio cursor that reads CSV results via S3FileSystem.

    Uses ``asyncio.to_thread()`` for result set creation and fetch operations
    because ``AthenaS3FSResultSet`` lazily streams rows from S3 via a CSV
    reader, making fetch calls blocking I/O.

    Example:
        >>> async with await pyathena.aconnect(...) as conn:
        ...     cursor = conn.cursor(AioS3FSCursor)
        ...     await cursor.execute("SELECT * FROM my_table")
        ...     row = await cursor.fetchone()
    """

    def __init__(
        self,
        s3_staging_dir: Optional[str] = None,
        schema_name: Optional[str] = None,
        catalog_name: Optional[str] = None,
        work_group: Optional[str] = None,
        poll_interval: float = 1,
        encryption_option: Optional[str] = None,
        kms_key: Optional[str] = None,
        kill_on_interrupt: bool = True,
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
        on_start_query_execution: Optional[Callable[[str], None]] = None,
        csv_reader: Optional[CSVReaderType] = None,
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
            on_start_query_execution=on_start_query_execution,
            **kwargs,
        )
        self._csv_reader = csv_reader
        self._result_set: Optional[AthenaS3FSResultSet] = None

    @staticmethod
    def get_default_converter(
        unload: bool = False,  # noqa: ARG004
    ) -> DefaultS3FSTypeConverter:
        """Get the default type converter for S3FS cursor.

        Args:
            unload: Unused. S3FS cursor does not support UNLOAD operations.

        Returns:
            DefaultS3FSTypeConverter instance.
        """
        return DefaultS3FSTypeConverter()

    async def execute(  # type: ignore[override]
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
        on_start_query_execution: Optional[Callable[[str], None]] = None,
        **kwargs,
    ) -> "AioS3FSCursor":
        """Execute a SQL query asynchronously via S3FileSystem CSV reader.

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
            **kwargs: Additional execution parameters.

        Returns:
            Self reference for method chaining.
        """
        self._reset_state()
        self.query_id = await self._execute(
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

        if self._on_start_query_execution:
            self._on_start_query_execution(self.query_id)
        if on_start_query_execution:
            on_start_query_execution(self.query_id)

        query_execution = await self._poll(self.query_id)
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self.result_set = await asyncio.to_thread(
                AthenaS3FSResultSet,
                connection=self._connection,
                converter=self._converter,
                query_execution=query_execution,
                arraysize=self.arraysize,
                retry_config=self._retry_config,
                csv_reader=self._csv_reader,
                **kwargs,
            )
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self

    async def fetchone(  # type: ignore[override]
        self,
    ) -> Optional[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch the next row of the result set.

        Wraps the synchronous fetch in ``asyncio.to_thread`` because
        ``AthenaS3FSResultSet`` reads rows lazily from S3.

        Returns:
            A tuple representing the next row, or None if no more rows.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaS3FSResultSet, self.result_set)
        return await asyncio.to_thread(result_set.fetchone)

    async def fetchmany(  # type: ignore[override]
        self, size: Optional[int] = None
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch multiple rows from the result set.

        Wraps the synchronous fetch in ``asyncio.to_thread`` because
        ``AthenaS3FSResultSet`` reads rows lazily from S3.

        Args:
            size: Maximum number of rows to fetch. Defaults to arraysize.

        Returns:
            List of tuples representing the fetched rows.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaS3FSResultSet, self.result_set)
        return await asyncio.to_thread(result_set.fetchmany, size)

    async def fetchall(  # type: ignore[override]
        self,
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch all remaining rows from the result set.

        Wraps the synchronous fetch in ``asyncio.to_thread`` because
        ``AthenaS3FSResultSet`` reads rows lazily from S3.

        Returns:
            List of tuples representing all remaining rows.

        Raises:
            ProgrammingError: If no result set is available.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaS3FSResultSet, self.result_set)
        return await asyncio.to_thread(result_set.fetchall)

    async def __anext__(self):
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row
