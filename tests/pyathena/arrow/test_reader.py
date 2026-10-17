# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.spec import AbstractBufferedFile

from pyathena.arrow.reader import ParquetChunkReader
from tests.pyathena.util import ImmediateExecutor


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

    def size(self, path):
        return os.path.getsize(path)

    def _open(self, path, mode="rb", block_size=None, cache_type="bytes", **kwargs):
        return _RecordingFile(
            self, path, block_size=block_size or self.block_size, cache_type=cache_type, **kwargs
        )


class TestParquetChunkReader:
    """Tests for reading Parquet files in tables of a fixed number of rows."""

    @pytest.fixture(params=["prefetch", "stream"])
    def max_prefetch_size(self, request, monkeypatch):
        """Read the files whole ahead of decoding, or open them for streaming."""
        if request.param == "stream":
            monkeypatch.setattr(ParquetChunkReader, "MAX_PREFETCH_SIZE", -1)
        return request.param

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

    def test_releases_files_opened_ahead(self, paths, monkeypatch):
        """Files opened ahead are closed when reading fails."""
        monkeypatch.setattr(ParquetChunkReader, "MAX_PREFETCH_SIZE", -1)
        # The third file fails while the files after it may be opened ahead.
        with open(paths[2], "rb") as f:
            valid = f.read()
        with open(paths[2], "wb") as f:
            f.write(valid[: len(valid) // 2] + b"\x00" * 64 + valid[-8:])
        fs = LocalFileSystem()
        opened = []
        original_open = fs.open

        def open_(path, mode="rb", **kwargs):
            f = original_open(path, mode, **kwargs)
            opened.append(f)
            return f

        monkeypatch.setattr(fs, "open", open_)
        # Open ahead at once, so that a file opened ahead is never cancelled.
        monkeypatch.setattr("pyathena.arrow.reader.ThreadPoolExecutor", ImmediateExecutor)
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
        """A streamed file is read one row group at a time, when needed."""
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

    def test_no_files(self):
        assert list(ParquetChunkReader(LocalFileSystem(), [], 4)) == []

    @pytest.mark.parametrize("chunksize", [0, -1])
    def test_invalid_chunksize(self, paths, chunksize):
        with pytest.raises(ValueError):  # noqa: PT011
            ParquetChunkReader(LocalFileSystem(), paths, chunksize)

    def test_close(self, paths, max_prefetch_size, monkeypatch):
        """close() closes the file being read and the files opened ahead."""
        fs = LocalFileSystem()
        opened = []
        original_open = fs.open

        def open_(path, mode="rb", **kwargs):
            f = original_open(path, mode, **kwargs)
            opened.append(f)
            return f

        monkeypatch.setattr(fs, "open", open_)
        # Open ahead at once, so that a file opened ahead is never cancelled.
        monkeypatch.setattr("pyathena.arrow.reader.ThreadPoolExecutor", ImmediateExecutor)
        reader = ParquetChunkReader(fs, paths, 2)
        assert next(reader).num_rows == 2
        reader.close()
        # The file after the first one was opened ahead.
        assert len(opened) > 1
        assert all(f.closed for f in opened)
        assert list(reader) == []
