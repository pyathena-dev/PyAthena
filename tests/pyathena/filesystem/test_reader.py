# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import gc
import threading
from concurrent.futures import Future, ThreadPoolExecutor, wait

import pyarrow as pa
import pyarrow.csv as pa_csv
import pytest

from pyathena.filesystem.reader import ReadAheadStream
from tests.pyathena.util import EVENT_TIMEOUT, ImmediateExecutor, poll_until


class _RecordingRangeFile:
    """A file over bytes that records the ranges it reads."""

    def __init__(self, data, short=False, error_at=None, gate=None):
        self._data = data
        self._short = short
        self._error_at = error_at
        self._gate = gate
        self.gated_read_started = threading.Event()
        self.reads = []
        self.closes = 0
        # The offsets of the reads in the order they ended, and "close" when closed.
        self.events = []

    def read_range(self, offset, size):
        self.reads.append((offset, size))
        if self._gate is not None and offset > 0:
            self.gated_read_started.set()
            assert self._gate.wait(EVENT_TIMEOUT)
        self.events.append(offset)
        if offset == self._error_at:
            raise OSError("read failed")
        return self._data[offset : offset + size - (1 if self._short else 0)]

    @property
    def closed(self):
        return self.closes > 0

    def close(self):
        self.events.append("close")
        self.closes += 1

    def assert_closed_after_reads(self, stream, waiting):
        """Close a stream in a thread while the gated reads are blocked, and check
        that the file is closed after the reads end.

        The reads are released once the close waits for them or, if it does not
        wait, has closed the file, which then precedes their end.

        Args:
            stream: The stream over the file.
            waiting: The event from ``_signal_close_wait()``.
        """
        closer = threading.Thread(target=stream.close, daemon=True)
        try:
            closer.start()
            poll_until(
                lambda: waiting.is_set() or self.closed,
                "close() neither waited for the reads nor closed the file",
            )
        finally:
            self._gate.set()
        closer.join(EVENT_TIMEOUT)
        assert not closer.is_alive()
        assert self.events[-1] == "close"
        assert sorted(self.events[:-1]) == sorted(offset for offset, _ in self.reads)

    def open_stream(self, **kwargs):
        """Open a stream over the file that closes it."""
        return ReadAheadStream(self.read_range, len(self._data), on_close=self.close, **kwargs)


class _InterruptedFuture(Future):
    """A future whose first wait for the outcome is interrupted."""

    def __init__(self):
        super().__init__()
        self.interrupted = False

    def exception(self, timeout=None):
        if not self.interrupted:
            self.interrupted = True
            raise KeyboardInterrupt
        return super().exception(timeout)


class _InterruptedWaitExecutor(ImmediateExecutor):
    """An executor that runs each call when it is submitted, whose first wait for the
    outcome of the first call is interrupted."""

    def __init__(self, max_workers=None):
        super().__init__(max_workers)
        self.submitted = False

    def _new_future(self):
        if self.submitted:
            return super()._new_future()
        self.submitted = True
        return _InterruptedFuture()


def _signal_close_wait(monkeypatch, interrupts=0):
    """Make the wait of ``ReadAheadStream.close()`` for the running reads observable.

    Args:
        monkeypatch: The pytest monkeypatch fixture.
        interrupts: The number of first waits that raise ``KeyboardInterrupt``.

    Returns:
        An event set when a wait that is not interrupted starts.
    """
    waiting = threading.Event()
    remaining = [KeyboardInterrupt() for _ in range(interrupts)]

    def signalling_wait(futures, *args, **kwargs):
        if remaining:
            raise remaining.pop()
        waiting.set()
        return wait(futures, *args, **kwargs)

    monkeypatch.setattr("pyathena.filesystem.reader.wait", signalling_wait)
    return waiting


class TestReadAheadStream:
    """Tests for reading a file sequentially with parallel range reads."""

    @pytest.mark.parametrize("size", [0, 1, 6, 7, 21, 25])
    @pytest.mark.parametrize("read_size", [1, 5, 7, 100, -1])
    def test_read(self, size, read_size):
        data = bytes(range(256))[:size]
        file = _RecordingRangeFile(data)
        stream = file.open_stream(part_size=7, max_parts=2)
        chunks = []
        while chunk := stream.read(read_size):
            # Every read is full except at the end of the file.
            assert len(chunk) == read_size or sum(map(len, chunks)) + len(chunk) == size
            chunks.append(chunk)
        assert b"".join(chunks) == data
        assert stream.tell() == size
        assert sorted(file.reads) == [(o, min(7, size - o)) for o in range(0, size, 7)]
        stream.close()
        assert file.closed

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"part_size": 0}, "part_size"),
            ({"max_parts": 0}, "max_parts"),
        ],
    )
    @pytest.mark.filterwarnings("error::pytest.PytestUnraisableExceptionWarning")
    def test_invalid_options(self, kwargs, message):
        closed = []
        with pytest.raises(ValueError, match=message):
            ReadAheadStream(lambda o, s: b"", 10, on_close=lambda: closed.append(1), **kwargs)
        # The stream that failed to initialize is closed without an error when it is
        # finalized, and without calling on_close.
        gc.collect()
        assert closed == []

    def test_parts_in_flight(self, monkeypatch):
        """At most max_parts parts are requested ahead of the one being consumed."""
        monkeypatch.setattr("pyathena.filesystem.reader.ThreadPoolExecutor", ImmediateExecutor)
        file = _RecordingRangeFile(bytes(100))
        stream = file.open_stream(part_size=10, max_parts=3)
        assert file.reads == []
        assert stream.read(1) == bytes(1)
        # Three parts were requested, the first is being consumed, and one more follows.
        assert file.reads == [(0, 10), (10, 10), (20, 10), (30, 10)]
        assert stream.read(9) == bytes(9)
        assert len(file.reads) == 4
        stream.close()

    def test_short_read(self):
        stream = _RecordingRangeFile(bytes(20), short=True).open_stream(part_size=10, max_parts=2)
        with pytest.raises(OSError, match="Read 9 bytes of a range of 10 bytes"):
            stream.read(20)
        stream.close()

    def test_read_error(self):
        """An error is raised by the read that reaches the failed part."""
        file = _RecordingRangeFile(bytes(30), error_at=10)
        stream = file.open_stream(part_size=10, max_parts=2)
        assert stream.read(10) == bytes(10)
        with pytest.raises(OSError, match="read failed"):
            stream.read(10)
        # A later read does not continue past the failed part.
        with pytest.raises(OSError, match="read failed"):
            stream.read(10)
        stream.close()
        assert file.closed

    def test_close_before_end(self):
        """Closing cancels the queued reads and closes the file."""
        file = _RecordingRangeFile(bytes(1000))
        stream = file.open_stream(part_size=10, max_parts=2)
        assert stream.read(5) == bytes(5)
        stream.close()
        assert file.closed
        assert stream.closed
        assert len(file.reads) <= 3
        stream.close()
        # The file is closed once.
        assert file.closes == 1
        with pytest.raises(ValueError, match="I/O operation on closed file"):
            stream.read(5)

    def test_interrupted_wait(self, monkeypatch):
        """An interrupt while waiting for a part leaves the part to the next read."""
        monkeypatch.setattr(
            "pyathena.filesystem.reader.ThreadPoolExecutor", _InterruptedWaitExecutor
        )
        data = bytes(range(30))
        file = _RecordingRangeFile(data)
        stream = file.open_stream(part_size=10, max_parts=2)
        with pytest.raises(KeyboardInterrupt):
            stream.read(10)
        assert stream.read(10) == data[:10]
        assert stream.read(5) == data[10:15]
        assert sorted(file.reads) == [(0, 10), (10, 10), (20, 10)]
        stream.close()

    def test_interrupted_close(self, monkeypatch):
        """An interrupted close leaves the file open, and closing again waits for the
        running reads before closing the file."""
        gate = threading.Event()
        file = _RecordingRangeFile(bytes(100), gate=gate)
        stream = file.open_stream(part_size=10, max_parts=2)
        waiting = _signal_close_wait(monkeypatch, interrupts=1)
        assert stream.read(10) == bytes(10)
        assert file.gated_read_started.wait(EVENT_TIMEOUT)
        with pytest.raises(KeyboardInterrupt):
            stream.close()
        assert not file.closed
        assert not stream.closed
        # The stream reads nothing more once closing started.
        with pytest.raises(ValueError, match="I/O operation on closed file"):
            stream.read(10)
        file.assert_closed_after_reads(stream, waiting)
        assert stream.closed

    def test_close_with_queued_reads(self, monkeypatch):
        """Closing cancels a part that no worker has started and does not wait for it."""
        monkeypatch.setattr(
            "pyathena.filesystem.reader.ThreadPoolExecutor",
            lambda max_workers: ThreadPoolExecutor(max_workers=1),
        )
        gate = threading.Event()
        file = _RecordingRangeFile(bytes(100), gate=gate)
        stream = file.open_stream(part_size=10, max_parts=2)
        waiting = _signal_close_wait(monkeypatch)
        # The one worker blocks in the read of the second part, and the third waits.
        assert stream.read(10) == bytes(10)
        assert file.gated_read_started.wait(EVENT_TIMEOUT)
        file.assert_closed_after_reads(stream, waiting)
        assert (20, 10) not in file.reads

    def test_close_waits_for_running_reads(self, monkeypatch):
        """Closing waits for the reads in flight before closing the file."""
        gate = threading.Event()
        file = _RecordingRangeFile(bytes(100), gate=gate)
        stream = file.open_stream(part_size=10, max_parts=2)
        waiting = _signal_close_wait(monkeypatch)
        assert stream.read(10) == bytes(10)
        assert file.gated_read_started.wait(EVENT_TIMEOUT)
        file.assert_closed_after_reads(stream, waiting)

    def test_csv(self):
        """pyarrow reads a CSV through the stream with its own block size."""
        data = b"a,b\n" + b"".join(f"{i},x{i}\n".encode() for i in range(10_000))
        file = _RecordingRangeFile(data)
        read_options = pa_csv.ReadOptions(block_size=4096)
        with pa.PythonFile(file.open_stream(part_size=7, max_parts=3), mode="r") as source:
            table = pa_csv.read_csv(source, read_options=read_options)
        assert table.equals(pa_csv.read_csv(pa.BufferReader(data), read_options=read_options))
        assert file.closed
