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

    def __init__(self, data, short=False, error_at=None, gate=None, gates=None):
        self._data = data
        self._short = short
        self._error_at = error_at
        self._gate = gate
        # An event per offset that blocks the read at that offset until set.
        self._gates = gates or {}
        self.gated_read_started = threading.Event()
        self.started = {offset: threading.Event() for offset in self._gates}
        self.reads = []
        self.closes = 0
        # The offsets of the reads in the order they ended, and "close" when closed.
        self.events = []
        # Set when the file is closed, and by signal_close_wait() when close()
        # starts to wait for the reads.
        self.close_progressed = threading.Event()
        self.file_closed = threading.Event()

    def read_range(self, offset, size):
        self.reads.append((offset, size))
        if self._gate is not None and offset > 0:
            self.gated_read_started.set()
            assert self._gate.wait(EVENT_TIMEOUT)
        if offset in self._gates:
            self.started[offset].set()
            assert self._gates[offset].wait(EVENT_TIMEOUT)
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
        self.close_progressed.set()
        self.file_closed.set()

    def assert_closed_after_reads(self, stream):
        """Check that closing the stream while the gated reads block closes the file last.

        The stream is closed in a thread, and the reads are released once the
        close waits for them (see :meth:`signal_close_wait`) or, if it does not
        wait, has closed the file, which then precedes their end.

        Args:
            stream: The stream over the file.
        """
        errors = []

        def close():
            try:
                stream.close()
            except BaseException as e:
                errors.append(e)

        closer = threading.Thread(target=close, daemon=True)
        closer.start()
        assert self.close_progressed.wait(EVENT_TIMEOUT), (
            "close() neither waited for the reads nor closed the file"
        )
        self._gate.set()
        closer.join(EVENT_TIMEOUT)
        assert not closer.is_alive()
        assert not errors
        # Every read that started ended before the file was closed.
        assert self.events[-1] == "close"
        assert len(self.events) == len(self.reads) + 1

    def signal_close_wait(self, monkeypatch, interrupts=0):
        """Set ``close_progressed`` when ``ReadAheadStream.close()`` starts to wait for the reads.

        Args:
            monkeypatch: The pytest monkeypatch fixture.
            interrupts: The number of first waits that raise ``KeyboardInterrupt``.
        """
        remaining = interrupts

        def signalling_wait(futures, *args, **kwargs):
            nonlocal remaining
            if remaining:
                remaining -= 1
                raise KeyboardInterrupt
            self.close_progressed.set()
            return wait(futures, *args, **kwargs)

        monkeypatch.setattr("pyathena.filesystem.reader.wait", signalling_wait)

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


@pytest.fixture
def gate():
    """An event that blocks the gated reads of a ``_RecordingRangeFile`` until set.

    It is set at teardown, so that a test that fails before releasing the reads
    does not leave them blocked.
    """
    gate = threading.Event()
    yield gate
    gate.set()


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
        "kwargs",
        [{"part_size": 0}, {"max_parts": 0}],
        ids=["kwargs0-part_size", "kwargs1-max_parts"],
    )
    @pytest.mark.filterwarnings("error::pytest.PytestUnraisableExceptionWarning")
    def test_invalid_options(self, kwargs):
        closed = []
        with pytest.raises(ValueError):  # noqa: PT011
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
        with pytest.raises(OSError):  # noqa: PT011
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

    def test_interrupted_close(self, monkeypatch, gate):
        """An interrupted close leaves the file open, and closing again waits for the
        running reads before closing the file."""
        file = _RecordingRangeFile(bytes(100), gate=gate)
        stream = file.open_stream(part_size=10, max_parts=2)
        file.signal_close_wait(monkeypatch, interrupts=1)
        assert stream.read(10) == bytes(10)
        assert file.gated_read_started.wait(EVENT_TIMEOUT)
        with pytest.raises(KeyboardInterrupt):
            stream.close()
        assert not file.closed
        assert not stream.closed
        # The stream reads nothing more once closing started.
        with pytest.raises(ValueError, match="I/O operation on closed file"):
            stream.read(10)
        file.assert_closed_after_reads(stream)
        assert stream.closed

    def test_close_with_queued_reads(self, monkeypatch, gate):
        """Closing cancels a part that no worker has started and does not wait for it."""
        monkeypatch.setattr(
            "pyathena.filesystem.reader.ThreadPoolExecutor",
            lambda max_workers: ThreadPoolExecutor(max_workers=1),
        )
        file = _RecordingRangeFile(bytes(100), gate=gate)
        stream = file.open_stream(part_size=10, max_parts=2)
        file.signal_close_wait(monkeypatch)
        # The one worker blocks in the read of the second part, and the third waits.
        assert stream.read(10) == bytes(10)
        assert file.gated_read_started.wait(EVENT_TIMEOUT)
        file.assert_closed_after_reads(stream)
        assert (20, 10) not in file.reads

    @pytest.mark.parametrize("first", [10, 20])
    def test_close_waits_for_running_reads(self, monkeypatch, first):
        """Closing waits for every read in flight, not only the first one to end,
        before closing the file, whichever read ends first."""
        gates = {10: threading.Event(), 20: threading.Event()}
        (second,) = set(gates) - {first}
        file = _RecordingRangeFile(bytes(100), gates=gates)
        stream = file.open_stream(part_size=10, max_parts=2)
        file.signal_close_wait(monkeypatch)
        errors = []

        def close():
            try:
                stream.close()
            except BaseException as e:
                errors.append(e)

        closer = threading.Thread(target=close, daemon=True)
        try:
            assert stream.read(10) == bytes(10)
            # Both reads ahead run, each held by its gate.
            assert all(file.started[offset].wait(EVENT_TIMEOUT) for offset in gates)
            closer.start()
            assert file.close_progressed.wait(EVENT_TIMEOUT)
            gates[first].set()
            poll_until(lambda: first in file.events, "the first read did not end")
            # The other read still runs, so the file stays open.
            assert not file.file_closed.wait(0.2)
            gates[second].set()
        finally:
            for event in gates.values():
                event.set()
            if closer.ident is None:
                stream.close()
            else:
                closer.join(EVENT_TIMEOUT)
        assert not closer.is_alive()
        assert not errors
        assert file.events == [0, first, second, "close"]

    def test_csv(self):
        """pyarrow reads a CSV through the stream with its own block size."""
        data = b"a,b\n" + b"".join(f"{i},x{i}\n".encode() for i in range(10_000))
        file = _RecordingRangeFile(data)
        read_options = pa_csv.ReadOptions(block_size=4096)
        with pa.PythonFile(file.open_stream(part_size=7, max_parts=3), mode="r") as source:
            table = pa_csv.read_csv(source, read_options=read_options)
        assert table.equals(pa_csv.read_csv(pa.BufferReader(data), read_options=read_options))
        assert file.closed
