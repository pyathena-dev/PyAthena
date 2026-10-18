# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Readers of the result files of the Arrow cursor."""

from __future__ import annotations

import threading
from collections import abc
from collections.abc import Generator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import closing
from contextvars import copy_context
from typing import TYPE_CHECKING, Any, cast

from pyathena.util import override, stop_retries_on, wait_with_retry_stop

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem
    from pyarrow import RecordBatch, Table


class ParquetChunkReader(abc.Iterator):  # type: ignore[type-arg]
    """Iterator over Parquet files as Arrow tables of a fixed number of rows.

    The files are decoded one at a time, in the given order, one row group at a
    time, with ``pyarrow.parquet.ParquetFile.iter_batches()``. Memory use
    therefore depends on the chunk size and on one row group of the file being
    read, not on the size of the result. Every table has ``chunksize`` rows except
    the last, which has the remaining rows; rows carry over from one row group and
    file to the next.

    A file of at most ``MAX_PREFETCH_SIZE`` bytes is read whole, ahead of
    decoding: it is opened without a cache, and PyAthena's filesystem splits the
    read into parallel range requests of its block size. The next file is read in
    a background thread while the current one is decoded. A larger file is opened with
    ``open()``, by default without the filesystem's read-ahead cache, and the
    column chunks of each row group are pre-buffered: pyarrow reads them in large
    coalesced ranges, which PyAthena's filesystem splits into parallel requests,
    and holds them until the row group is decoded.

    The files must have the same schema, as the files of one UNLOAD query do. INT96
    timestamps, which Athena writes, are read at microseconds.

    Example:
        >>> with fs.open(manifest) as f:
        ...     paths = f.read().decode().split()
        >>> with ParquetChunkReader(fs, paths, chunksize=100_000) as reader:
        ...     for table in reader:
        ...         process(table)
    """

    #: The largest file that is read whole, ahead of decoding.
    MAX_PREFETCH_SIZE: int = 32 * 2**20

    def __init__(
        self,
        fs: AbstractFileSystem,
        paths: Sequence[str],
        chunksize: int,
        columns: Sequence[str] | None = None,
        cache_type: str | None = None,
    ) -> None:
        """Create a reader of the given files.

        Args:
            fs: The filesystem that opens the files.
            paths: The paths of the files, in reading order.
            chunksize: The number of rows of each table but the last.
            columns: The names of the top-level columns to read; all columns if None.
            cache_type: The fsspec cache type that opens a file larger than
                ``MAX_PREFETCH_SIZE``. If None, ``"none"``, which reads only the
                ranges that pyarrow requests.

        Raises:
            ValueError: If ``chunksize`` is not positive or ``columns`` is empty.
                Reading raises ``ValueError`` as well when a file has no top-level
                column of a name in ``columns``.
        """
        if chunksize < 1:
            raise ValueError(f"chunksize must be positive, got {chunksize}.")
        if columns is not None and not columns:
            raise ValueError("columns must not be empty.")
        self._fs = fs
        self._paths = list(paths)
        self._chunksize = chunksize
        self._columns = list(columns) if columns is not None else None
        self._cache_type = cache_type or "none"
        self._tables = self._read_tables()

    @override
    def __next__(self) -> Table:
        """Read the next table.

        Returns:
            The next ``chunksize`` rows, or the remaining rows at the end.

        Raises:
            StopIteration: When every row has been read.
        """
        return next(self._tables)

    @override
    def __iter__(self) -> ParquetChunkReader:
        """Return the reader itself."""
        return self

    def __enter__(self) -> ParquetChunkReader:
        """Return the reader, which the context closes."""
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """Close the reader."""
        self.close()

    def close(self) -> None:
        """Stop reading and close the file being read; later calls read nothing."""
        self._tables.close()

    def _read_tables(self) -> Generator[Table]:
        """Cut the record batches of the files into tables of ``chunksize`` rows.

        Yields:
            The tables of rows, the last with the remaining rows.
        """
        import pyarrow as pa

        pending: list[RecordBatch] = []
        rows = 0
        with closing(self._read_batches()) as batches:
            for batch in batches:
                pending.append(batch)
                rows += batch.num_rows
                while rows >= self._chunksize:
                    table = pa.Table.from_batches(pending)
                    yield table.slice(0, self._chunksize)
                    rest = table.slice(self._chunksize)
                    pending, rows = rest.to_batches(), rest.num_rows
        if rows:
            yield pa.Table.from_batches(pending)

    def _read_batches(self) -> Generator[RecordBatch]:
        """Read the record batches of the files, in order, without empty batches.

        Yields:
            The record batches of at most ``chunksize`` rows.
        """
        with closing(self._open_files()) as sources:
            for source in sources:
                with source:
                    yield from self._read_file(source)

    def _read_file(self, source: Any) -> Generator[RecordBatch]:
        """Read the record batches of one file.

        Args:
            source: The file, as ``_open()`` opened it.

        Yields:
            The non-empty record batches of at most ``chunksize`` rows.

        Raises:
            ValueError: If the file has no top-level column of a name in ``columns``.
        """
        import pyarrow.parquet as pq

        parquet_file = pq.ParquetFile(source, pre_buffer=True, coerce_int96_timestamp_unit="us")
        if self._columns is not None and (
            missing := set(self._columns) - set(parquet_file.schema_arrow.names)
        ):
            # pyarrow would select nested fields by dotted paths, or no column, whose
            # tables keep their rows when sliced.
            raise ValueError(f"No top-level columns {sorted(missing)}.")
        for row_group in range(parquet_file.num_row_groups):
            # One row group at a time, so that only its column chunks are pre-buffered.
            for batch in parquet_file.iter_batches(
                batch_size=self._chunksize, row_groups=[row_group], columns=self._columns
            ):
                if batch.num_rows:
                    yield batch

    def _open_files(self) -> Generator[Any]:
        """Open the files in order, each next file in a background thread.

        The receiver closes each file it receives. A file opened ahead and not
        received, because reading stopped or failed, is closed here.

        Each file is opened in a copy of the context of the read that opens it
        ahead, and a stop of the retries of the read that receives it (see
        :func:`~pyathena.util.stop_retries_on`) stops those of the open too. The
        retries of an open not received are stopped when the files stop being
        read.

        Yields:
            The files, as ``_open()`` opens them.
        """
        if not self._paths:
            return
        # One worker opens the one file read ahead: at most the file being decoded and
        # the next one are held, which bounds memory to two files.
        with ThreadPoolExecutor(max_workers=1) as executor:
            ahead: tuple[Future[Any], threading.Event] | None = self._open_ahead(
                executor, self._paths[0]
            )
            try:
                for next_path in [*self._paths[1:], None]:
                    # Open the next file only after this one opened.
                    future, stop = cast("tuple[Future[Any], threading.Event]", ahead)
                    source = wait_with_retry_stop(future, stop=stop)
                    ahead = None if next_path is None else self._open_ahead(executor, next_path)
                    yield source
            finally:
                if ahead is not None:
                    future, stop = ahead
                    stop.set()
                    if not future.cancel() and future.exception() is None:
                        future.result().close()

    def _open_ahead(
        self, executor: ThreadPoolExecutor, path: str
    ) -> tuple[Future[Any], threading.Event]:
        """Start opening a file in the background, in a copy of this context.

        Args:
            executor: The executor that opens the file.
            path: The path of the file.

        Returns:
            The future of ``_open()``, and the event that stops the retries of
            its requests.
        """
        stop = threading.Event()
        return executor.submit(copy_context().run, self._open, path, stop), stop

    def _open(self, path: str, stop: threading.Event) -> Any:
        """Open a file for ``ParquetFile``.

        Args:
            path: The path of the file.
            stop: The event that stops the retries of the requests.

        Returns:
            A buffer of the whole file if it has at most ``MAX_PREFETCH_SIZE``
            bytes, or else the file opened for reading with the cache type.
        """
        import pyarrow as pa

        with stop_retries_on(stop):
            if self._fs.size(path) <= self.MAX_PREFETCH_SIZE:
                with self._fs.open(path, "rb", cache_type="none") as f:
                    return pa.BufferReader(f.read())
            return self._fs.open(path, "rb", cache_type=self._cache_type)
