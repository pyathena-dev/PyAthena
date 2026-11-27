# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Sequential reading of a file with parallel range reads ahead of the consumer."""

from __future__ import annotations

import io
from collections import deque
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Any

from pyathena.filesystem.util import override


class ReadAheadStream(io.RawIOBase):
    """Sequential stream over a file that reads ranges ahead in parallel.

    The file is read in parts of ``part_size`` bytes with a range-read function,
    and up to ``max_parts`` parts are read in a thread pool ahead of the consumer,
    so the transfer is not limited to one request at a time. Parts are returned in
    order, and at most ``max_parts`` parts plus the one being consumed are held in
    memory. A read that returns fewer bytes than requested raises ``OSError``, and
    an error from the range-read function is raised by the read that reaches its
    part; every later read raises the same error.

    Closing the stream cancels the queued reads, waits for the running ones, and
    then calls ``on_close``.

    Example:
        >>> file = fs.open_input_file(path)
        >>> stream = ReadAheadStream(
        ...     lambda offset, size: file.read_at(size, offset), file.size(), on_close=file.close
        ... )
        >>> with pyarrow.PythonFile(stream, mode="r") as f:
        ...     table = pyarrow.csv.read_csv(f)
    """

    #: Bytes requested by one range read unless ``part_size`` is given.
    DEFAULT_PART_SIZE: int = 8 * 2**20
    #: The most parts read ahead at once unless ``max_parts`` is given.
    DEFAULT_MAX_PARTS: int = 16

    def __init__(
        self,
        read_range: Callable[[int, int], bytes],
        size: int,
        part_size: int | None = None,
        max_parts: int | None = None,
        on_close: Callable[[], None] | None = None,
    ) -> None:
        """Initialize the stream without reading the file.

        Args:
            read_range: A function that reads ``size`` bytes of the file from
                ``offset``, called as ``read_range(offset, size)`` from several
                threads at once.
            size: The size of the file in bytes.
            part_size: Bytes requested by one ``read_range()`` call; if None,
                ``DEFAULT_PART_SIZE``.
            max_parts: The most parts read ahead of the consumer at once; if
                None, ``DEFAULT_MAX_PARTS``.
            on_close: A function called once when the stream is closed, after the
                running reads have finished, such as the ``close()`` of the file.

        Raises:
            ValueError: If ``part_size`` or ``max_parts`` is not positive.
        """
        super().__init__()
        part_size = self.DEFAULT_PART_SIZE if part_size is None else part_size
        max_parts = self.DEFAULT_MAX_PARTS if max_parts is None else max_parts
        try:
            if part_size < 1:
                raise ValueError(f"part_size must be positive, got {part_size}.")
            if max_parts < 1:
                raise ValueError(f"max_parts must be positive, got {max_parts}.")
        except BaseException:
            # Closed as io.RawIOBase, so that finalizing the stream does not call
            # close() of this class, nor on_close.
            super().close()
            raise
        self._read_range = read_range
        self._size = size
        self._part_size = part_size
        self._max_parts = max_parts
        self._on_close = on_close
        self._executor = ThreadPoolExecutor(max_workers=max_parts)
        self._parts: deque[tuple[int, Future[bytes]]] = deque()
        self._next_offset = 0
        self._position = 0
        self._buffer = memoryview(b"")
        self._error: BaseException | None = None
        self._closing = False

    @override
    def readable(self) -> bool:
        return True

    @override
    def tell(self) -> int:
        return self._position

    @override
    def readinto(self, buffer: Any) -> int:  # type: ignore[override]
        """Fill ``buffer`` from the parts read ahead.

        Args:
            buffer: A writable buffer.

        Returns:
            The number of bytes read, less than the size of ``buffer`` only at
            the end of the file.

        Raises:
            ValueError: If the stream is closed, or its closing was interrupted.
        """
        if self.closed or self._closing:
            raise ValueError("I/O operation on closed file.")
        view = memoryview(buffer).cast("B")
        filled = 0
        while filled < len(view):
            if not self._buffer and not self._next_part():
                break
            size = min(len(view) - filled, len(self._buffer))
            view[filled : filled + size] = self._buffer[:size]
            self._buffer = self._buffer[size:]
            filled += size
        self._position += filled
        return filled

    @override
    def close(self) -> None:
        """Cancel the queued reads, wait for the running ones, and call ``on_close``.

        If the wait is interrupted, the stream stays open and ``on_close`` is not
        called, so that the file is not closed under the running reads; closing
        again waits for them. Reads raise ``ValueError`` from then on.
        """
        if self.closed:
            return
        self._closing = True
        self._executor.shutdown(wait=False, cancel_futures=True)
        # The running reads are awaited by their futures, not by joining the threads,
        # which an interrupt can leave marked as finished on Python 3.11. The
        # cancelled ones are left out: wait() never counts them as done.
        wait([part for _, part in self._parts if not part.cancelled()])
        self._parts.clear()
        self._buffer = memoryview(b"")
        try:
            if self._on_close is not None:
                self._on_close()
        finally:
            super().close()

    def _next_part(self) -> bool:
        """Wait for the next part and make it the buffer being consumed.

        Returns:
            False at the end of the file.

        Raises:
            OSError: If the part has fewer bytes than requested.
        """
        if self._error is not None:
            raise self._error
        self._read_ahead()
        if not self._parts:
            return False
        expected, part = self._parts[0]
        # An interrupt of the wait leaves the part queued for the next read.
        error = part.exception()
        self._parts.popleft()
        if error is None:
            data = part.result()
            if len(data) != expected:
                error = OSError(f"Read {len(data)} bytes of a range of {expected} bytes.")
        if error is not None:
            # The part is gone, so a later read must not continue past it.
            self._error = error
            raise error
        self._buffer = memoryview(data)
        self._read_ahead()
        return True

    def _read_ahead(self) -> None:
        """Start reading parts until ``max_parts`` are in flight or the file is read."""
        while len(self._parts) < self._max_parts and self._next_offset < self._size:
            size = min(self._part_size, self._size - self._next_offset)
            part = self._executor.submit(self._read_range, self._next_offset, size)
            self._parts.append((size, part))
            self._next_offset += size
