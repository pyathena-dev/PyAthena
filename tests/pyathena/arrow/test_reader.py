# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import os
import threading
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.spec import AbstractBufferedFile

from pyathena.arrow.reader import ParquetChunkReader
from pyathena.util import RetryConfig, stop_retries_on
from tests.pyathena.util import EVENT_TIMEOUT, ImmediateExecutor, ThrottledOperation, retried


class _RecordingFile(AbstractBufferedFile):
    """A local file read through fsspec's caches, recording the ranges it fetches."""

    def __init__(self, fs, path, **kwargs):
        with open(path, "rb") as f:
            self.data = f.read()
        super().__init__(fs, path, size=len(self.data), **kwargs)

    def _fetch_range(self, start, end):
        end = min(end, len(self.data))
        self.fs.fetched.append((start, end))
        return self.data[start:end]


class _RecordingFileSystem(AbstractFileSystem):
    """Local files opened as buffered files, like ``S3File``, with a "bytes" cache by default."""

    # A new instance for every test, so that its record starts empty.
    cachable = False

    def __init__(self, block_size):
        super().__init__()
        self.block_size = block_size
        self.fetched = []
        self.cache_types = []

    def size(self, path):
        return os.path.getsize(path)

    def _open(self, path, mode="rb", block_size=None, cache_type="bytes", **kwargs):
        self.cache_types.append(cache_type)
        return _RecordingFile(
            self, path, block_size=block_size or self.block_size, cache_type=cache_type, **kwargs
        )


class _ThrottledFileSystem:
    """Local files and one throttled file, whose requests are retried as PyAthena retries them.

    The backoff wait after the first attempt takes at least ``EVENT_TIMEOUT`` seconds.
    """

    THROTTLED_PATH = "s3://bucket/throttled.parquet"

    def __init__(self, operation):
        self.local = LocalFileSystem()
        self.request = retried(operation, RetryConfig(attempt=2, multiplier=EVENT_TIMEOUT))

    def size(self, path):
        if path == self.THROTTLED_PATH:
            return self.request()
        return self.local.size(path)

    def open(self, path, mode="rb", **kwargs):
        return self.local.open(path, mode, **kwargs)


class TestParquetChunkReader:
    """Tests for reading Parquet files in tables of a fixed number of rows."""

    @pytest.fixture(params=["prefetch", "stream"])
    def max_prefetch_size(self, request, monkeypatch):
        """Read the files whole ahead of decoding, or open them for streaming."""
        if request.param == "stream":
            monkeypatch.setattr(ParquetChunkReader, "MAX_PREFETCH_SIZE", -1)
        return request.param

    @pytest.fixture
    def opened_files(self, monkeypatch):
        """A local filesystem that records the files it opens, which are opened ahead at once.

        A file opened ahead at once is never cancelled.
        """
        fs = LocalFileSystem()
        opened = []
        original_open = fs.open

        def open_(path, mode="rb", **kwargs):
            f = original_open(path, mode, **kwargs)
            opened.append(f)
            return f

        monkeypatch.setattr(fs, "open", open_)
        monkeypatch.setattr("pyathena.arrow.reader.ThreadPoolExecutor", ImmediateExecutor)
        return fs, opened

    @pytest.fixture
    def paths(self, tmp_path):
        """Three files of 3, 0, and 7 rows; the last has row groups of 2 rows."""
        paths = []
        for index, (start, rows, row_group_size) in enumerate(
            [(0, 3, None), (3, 0, None), (3, 7, 2)]
        ):
            path = tmp_path / f"{index}.parquet"
            table = pa.table(
                {
                    "id": pa.array(range(start, start + rows), pa.int64()),
                    "name": pa.array(
                        [f"name-{i}" for i in range(start, start + rows)], pa.string()
                    ),
                }
            )
            pq.write_table(table, path, row_group_size=row_group_size)
            paths.append(str(path))
        return paths

    @pytest.mark.parametrize(
        ("chunksize", "expected"),
        [(1, [1] * 10), (4, [4, 4, 2]), (5, [5, 5]), (10, [10]), (100, [10])],
    )
    def test_chunk_sizes(self, paths, max_prefetch_size, chunksize, expected):
        with ParquetChunkReader(LocalFileSystem(), paths, chunksize) as reader:
            tables = list(reader)
        assert [t.num_rows for t in tables] == expected
        combined = pa.concat_tables(tables)
        assert combined.column("id").to_pylist() == list(range(10))
        assert combined.column("name").to_pylist() == [f"name-{i}" for i in range(10)]

    def test_columns(self, paths, max_prefetch_size):
        with ParquetChunkReader(LocalFileSystem(), paths, 4, columns=["name"]) as reader:
            tables = list(reader)
        assert all(t.column_names == ["name"] for t in tables)
        assert pa.concat_tables(tables).column("name").to_pylist() == [
            f"name-{i}" for i in range(10)
        ]

    @pytest.mark.parametrize("columns", [["missing"], ["id", "missing"], ["name.x"]])
    def test_columns_not_top_level(self, paths, max_prefetch_size, columns):
        with (
            ParquetChunkReader(LocalFileSystem(), paths, 1, columns=columns) as reader,
            pytest.raises(ValueError),  # noqa: PT011
        ):
            next(reader)

    @pytest.mark.parametrize("max_prefetch_size", ["stream"], indirect=True)
    def test_releases_files_opened_ahead(self, paths, max_prefetch_size, opened_files):
        """Files opened ahead are closed when reading fails."""
        # The third file fails while the files after it may be opened ahead.
        with open(paths[2], "rb") as f:
            valid = f.read()
        with open(paths[2], "wb") as f:
            f.write(valid[: len(valid) // 2] + b"\x00" * 64 + valid[-8:])
        fs, opened = opened_files
        with pytest.raises((OSError, ValueError)):
            list(ParquetChunkReader(fs, [*paths, paths[0], paths[0]], 2))
        # A file after the failed third one was opened ahead.
        assert len(opened) > 3
        assert all(f.closed for f in opened)

    def test_prefetched_file(self, paths):
        """A file read whole is fetched in one read of exactly the file."""
        fs = _RecordingFileSystem(block_size=32 * 2**10)
        with ParquetChunkReader(fs, paths[2:], 10) as reader:
            assert next(reader).num_rows == 7
        # One read of exactly the file, which PyAthena's filesystem splits into blocks.
        assert fs.fetched == [(0, os.path.getsize(paths[2]))]

    @pytest.mark.parametrize("cache_type", [None, "bytes"], ids=["default", "bytes"])
    def test_streamed_file_row_groups(self, tmp_path, monkeypatch, cache_type):
        """A streamed file is read one row group at a time, when needed, with the cache type."""
        monkeypatch.setattr(ParquetChunkReader, "MAX_PREFETCH_SIZE", -1)
        path = tmp_path / "groups.parquet"
        columns = {f"c{i}": pa.array([os.urandom(256) for _ in range(300)]) for i in range(3)}
        pq.write_table(pa.table(columns), path, compression="none", row_group_size=100)
        fs = _RecordingFileSystem(block_size=32 * 2**10)
        with ParquetChunkReader(fs, [str(path)], 50, cache_type=cache_type) as reader:
            assert next(reader).num_rows == 50
            fetched = len(fs.fetched)
            assert sum(table.num_rows for table in reader) == 250
        # The later row groups are fetched after the first chunk.
        assert len(fs.fetched) > fetched
        # Without a cache type, only the ranges that pyarrow requests are read.
        assert set(fs.cache_types) == {cache_type or "none"}

    def test_no_files(self):
        assert list(ParquetChunkReader(LocalFileSystem(), [], 4)) == []

    @pytest.mark.parametrize("chunksize", [0, -1])
    def test_invalid_chunksize(self, paths, chunksize):
        with pytest.raises(ValueError):  # noqa: PT011
            ParquetChunkReader(LocalFileSystem(), paths, chunksize)

    def test_close(self, paths, max_prefetch_size, opened_files):
        """close() closes the file being read and the files opened ahead."""
        fs, opened = opened_files
        reader = ParquetChunkReader(fs, paths, 2)
        assert next(reader).num_rows == 2
        reader.close()
        # The file after the first one was opened ahead.
        assert len(opened) > 1
        assert all(f.closed for f in opened)
        assert list(reader) == []

    @pytest.mark.parametrize("stopped", ["opening", "receiving"])
    def test_open_ahead_stopped(self, paths, stopped):
        """GH-1296: a stop of the read that opens or receives a file opened ahead stops the open."""
        operation = ThrottledOperation(operation_name="HeadObject")
        opening, receiving = threading.Event(), threading.Event()
        reader = ParquetChunkReader(
            _ThrottledFileSystem(operation), [paths[0], _ThrottledFileSystem.THROTTLED_PATH], 2
        )
        started = time.monotonic()
        with stop_retries_on(opening):
            assert next(reader).num_rows == 2
        assert operation.started.wait(EVENT_TIMEOUT)
        (opening if stopped == "opening" else receiving).set()
        with stop_retries_on(receiving), pytest.raises(ClientError) as caught:
            next(reader)
        assert time.monotonic() - started < EVENT_TIMEOUT
        assert caught.value.operation_name == "HeadObject"
        assert operation.calls == 1

    def test_close_stops_open_ahead(self, paths):
        """GH-1296: close() stops the retries of a file being opened ahead."""
        operation = ThrottledOperation(operation_name="HeadObject")
        reader = ParquetChunkReader(
            _ThrottledFileSystem(operation), [paths[0], _ThrottledFileSystem.THROTTLED_PATH], 2
        )
        started = time.monotonic()
        assert next(reader).num_rows == 2
        assert operation.started.wait(EVENT_TIMEOUT)
        reader.close()
        assert time.monotonic() - started < EVENT_TIMEOUT
        assert operation.calls == 1
