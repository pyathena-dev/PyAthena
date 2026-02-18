# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from concurrent.futures import Future
from multiprocessing import cpu_count
from typing import Any, Dict, List, Optional, Tuple, Union, cast

from pyathena import ProgrammingError
from pyathena.arrow.converter import (
    DefaultArrowTypeConverter,
    DefaultArrowUnloadTypeConverter,
)
from pyathena.arrow.result_set import AthenaArrowResultSet
from pyathena.async_cursor import AsyncCursor
from pyathena.common import CursorIterator
from pyathena.model import AthenaCompression, AthenaFileFormat, AthenaQueryExecution

_logger = logging.getLogger(__name__)  # type: ignore


class AsyncArrowCursor(AsyncCursor):
    """Asynchronous cursor that returns results in Apache Arrow format.

    This cursor extends AsyncCursor to provide asynchronous query execution
    with results returned as Apache Arrow Tables or RecordBatches. It's optimized
    for high-performance analytics workloads and interoperability with the
    Apache Arrow ecosystem.

    Features:
        - Asynchronous query execution with concurrent futures
        - Apache Arrow columnar data format for high performance
        - Memory-efficient processing of large datasets
        - Support for UNLOAD operations with Parquet output
        - Integration with pandas, Polars, and other Arrow-compatible libraries

    Attributes:
        arraysize: Number of rows to fetch per batch (configurable).

    Example:
        >>> from pyathena.arrow.async_cursor import AsyncArrowCursor
        >>>
        >>> cursor = connection.cursor(AsyncArrowCursor, unload=True)
        >>> query_id, future = cursor.execute("SELECT * FROM large_table")
        >>>
        >>> # Get result when ready
        >>> result_set = future.result()
        >>> arrow_table = result_set.as_arrow()
        >>>
        >>> # Convert to pandas if needed
        >>> df = arrow_table.to_pandas()
        >>>
        >>> # Convert to Polars if needed (requires polars)
        >>> polars_df = result_set.as_polars()

    Note:
        Requires pyarrow to be installed. UNLOAD operations generate
        Parquet files in S3 for optimal Arrow compatibility. For Polars
        interoperability, polars must be installed separately.
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
        max_workers: int = (cpu_count() or 1) * 5,
        arraysize: int = CursorIterator.DEFAULT_FETCH_SIZE,
        unload: bool = False,
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
        connect_timeout: Optional[float] = None,
        request_timeout: Optional[float] = None,
        **kwargs,
    ) -> None:
        """Initialize an AsyncArrowCursor.

        Args:
            s3_staging_dir: S3 location for query results.
            schema_name: Default schema name.
            catalog_name: Default catalog name.
            work_group: Athena workgroup name.
            poll_interval: Query status polling interval in seconds.
            encryption_option: S3 encryption option (SSE_S3, SSE_KMS, CSE_KMS).
            kms_key: KMS key ARN for encryption.
            kill_on_interrupt: Cancel running query on keyboard interrupt.
            max_workers: Maximum number of workers for concurrent execution.
            arraysize: Number of rows to fetch per batch.
            unload: Enable UNLOAD for high-performance Parquet output.
            result_reuse_enable: Enable Athena query result reuse.
            result_reuse_minutes: Minutes to reuse cached results.
            connect_timeout: Socket connection timeout in seconds for S3 operations.
                Defaults to AWS SDK default (typically 1 second) if not specified.
            request_timeout: Request timeout in seconds for S3 operations.
                Defaults to AWS SDK default (typically 3 seconds) if not specified.
                Increase this value if you experience timeout errors when using
                role assumption with STS or have high latency to S3.
            **kwargs: Additional connection parameters.

        Example:
            >>> # Use higher timeouts for role assumption scenarios
            >>> cursor = connection.cursor(
            ...     AsyncArrowCursor,
            ...     connect_timeout=10.0,
            ...     request_timeout=30.0
            ... )
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
            max_workers=max_workers,
            arraysize=arraysize,
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            **kwargs,
        )
        self._unload = unload
        self._connect_timeout = connect_timeout
        self._request_timeout = request_timeout

    @staticmethod
    def get_default_converter(
        unload: bool = False,
    ) -> Union[DefaultArrowTypeConverter, DefaultArrowUnloadTypeConverter, Any]:
        if unload:
            return DefaultArrowUnloadTypeConverter()
        return DefaultArrowTypeConverter()

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value <= 0:
            raise ProgrammingError("arraysize must be a positive integer value.")
        self._arraysize = value

    def _collect_result_set(
        self,
        query_id: str,
        unload_location: Optional[str] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> AthenaArrowResultSet:
        if kwargs is None:
            kwargs = {}
        query_execution = cast(AthenaQueryExecution, self._poll(query_id))
        return AthenaArrowResultSet(
            connection=self._connection,
            converter=self._converter,
            query_execution=query_execution,
            arraysize=self._arraysize,
            retry_config=self._retry_config,
            unload=self._unload,
            unload_location=unload_location,
            connect_timeout=self._connect_timeout,
            request_timeout=self._request_timeout,
            **kwargs,
        )

    def execute(
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
        **kwargs,
    ) -> Tuple[str, "Future[Union[AthenaArrowResultSet, Any]]"]:
        if self._unload:
            s3_staging_dir = s3_staging_dir if s3_staging_dir else self._s3_staging_dir
            if not s3_staging_dir:
                raise ProgrammingError("If the unload option is used, s3_staging_dir is required.")
            operation, unload_location = self._formatter.wrap_unload(
                operation,
                s3_staging_dir=s3_staging_dir,
                format_=AthenaFileFormat.FILE_FORMAT_PARQUET,
                compression=AthenaCompression.COMPRESSION_SNAPPY,
            )
        else:
            unload_location = None
        query_id = self._execute(
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
        return (
            query_id,
            self._executor.submit(
                self._collect_result_set,
                query_id,
                unload_location,
                kwargs,
            ),
        )
