# Copyright 2017 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Native asyncio cursor that reads Athena CSV query results through ``AioS3FileSystem``."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from pyathena._kwargs import validate_kwargs
from pyathena.aio.common import WithAsyncFetch
from pyathena.common import CursorIterator
from pyathena.error import OperationalError
from pyathena.filesystem.s3_async import AioS3FileSystem
from pyathena.model import AthenaQueryExecution
from pyathena.options import ExecuteOptions
from pyathena.s3fs.converter import DefaultS3FSTypeConverter
from pyathena.s3fs.result_set import AthenaS3FSResultSet, CSVReaderType
from pyathena.util import override

_logger = logging.getLogger(__name__)


class AioS3FSCursor(WithAsyncFetch):
    """Native asyncio cursor that reads CSV results via AioS3FileSystem.

    Uses ``AioS3FileSystem`` for S3 operations, which replaces
    ``ThreadPoolExecutor`` parallelism with ``asyncio.gather`` +
    ``asyncio.to_thread``. Fetch operations are wrapped in
    ``asyncio.to_thread()`` because CSV reading is blocking I/O.

    Example:
        >>> async with await pyathena.aio_connect(...) as conn:
        ...     cursor = conn.cursor(AioS3FSCursor)
        ...     await cursor.execute("SELECT * FROM my_table")
        ...     row = await cursor.fetchone()
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
        csv_reader: CSVReaderType | None = None,
        **kwargs,
    ) -> None:
        """Initialize an AioS3FSCursor.

        Args:
            s3_staging_dir: S3 location for query results.
            schema_name: Default schema name.
            catalog_name: Default catalog name.
            work_group: Athena workgroup name.
            poll_interval: Query status polling interval in seconds.
            encryption_option: S3 encryption option for query results.
            kms_key: KMS key for encrypting query results.
            kill_on_interrupt: Cancel the query when the task is cancelled while
                ``execute()`` starts or waits for the query.
            result_reuse_enable: Whether to enable Athena query result reuse.
            result_reuse_minutes: Maximum age of a reused query result in minutes.
            csv_reader: CSV reader class for parsing the result files. If None,
                ``AthenaCSVReader`` is used, which distinguishes NULL from empty
                strings. ``DefaultCSVReader`` reads both as empty strings.
            **kwargs: Other cursor arguments, such as ``connection`` and ``arraysize``,
                passed to the parent ``__init__``.
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
        self._csv_reader = csv_reader
        self._result_set: AthenaS3FSResultSet | None = None

    @staticmethod
    @override
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

    @override
    async def execute(
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
    ) -> AioS3FSCursor:
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
            on_start_query_execution: Callback invoked with the query ID before ``execute()``
                waits for the query: after the ``StartQueryExecution`` call, or after a
                reusable query ID is found through ``cache_size``.
            result_set_type_hints: Athena type signatures for complex-type columns,
                keyed by column name (case-insensitive) or zero-based column index.
            options: Shared execution options as an
                :class:`~pyathena.options.ExecuteOptions` instance. Individual
                keyword arguments take precedence over ``options`` fields.
            **kwargs: Supported S3FS result-set overrides. Unknown names raise TypeError.
                ``block_size`` sets the read block size for this query, and
                ``csv_reader`` overrides the cursor's value.

        Returns:
            Self reference for method chaining.
        """
        validate_kwargs(f"{type(self).__name__}.execute", kwargs, ("block_size", "csv_reader"))
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
        self.query_id = await self._execute(
            operation,
            parameters=parameters,
            options=options,
        )

        # Call user callbacks immediately after start_query_execution
        self._call_on_start_query_execution(self.query_id, options)

        query_execution = await self._poll(self.query_id)
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self.result_set = await asyncio.to_thread(
                AthenaS3FSResultSet,
                connection=self._connection,
                converter=self._converter,
                query_execution=query_execution,
                arraysize=self.arraysize,
                retry_config=self._retry_config,
                csv_reader=kwargs.pop("csv_reader", self._csv_reader),
                filesystem_class=AioS3FileSystem,
                result_set_type_hints=options.result_set_type_hints,
                **kwargs,
            )
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self
