# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import inspect
import logging
import shutil
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from io import BytesIO
from unittest.mock import MagicMock, PropertyMock, patch

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fsspec.implementations.local import LocalFileSystem

from pyathena.converter import _to_date, _to_default
from pyathena.error import OperationalError
from pyathena.model import AthenaQueryExecution
from pyathena.polars.converter import DefaultPolarsTypeConverter
from pyathena.polars.result_set import AthenaPolarsResultSet, PolarsDataFrameIterator
from pyathena.util import RetryConfig
from tests.pyathena.util import INT96_UNLOAD_FILE, INT96_VALUES

_ROWS_BEFORE_FAILURE = 300_000
# A timestamp column of a CSV result file with 12 fractional digits and NULL, as text and
# as read into a Datetime column with the default time unit.
_TIMESTAMP_TEXTS = pl.Series(["2020-01-02 03:04:05.123456789012", None], dtype=pl.String)
_TRUNCATED_TIMESTAMPS = pl.Series(
    [datetime(2020, 1, 2, 3, 4, 5, 123456), None], dtype=pl.Datetime("us")
)
# Polars 2.0 and later read Parquet maps as Map, and INT96 at the unit of the schema.
_POLARS_2 = int(pl.__version__.split(".")[0]) >= 2
_MAP = (
    pl.Map(pl.String, pl.Datetime("us"))
    if _POLARS_2
    else pl.List(pl.Struct({"key": pl.String, "value": pl.Datetime("us")}))
)
# The ways to read an UNLOAD result: whole, in chunks with ParquetChunkReader, and in
# chunks that Polars scans with the read options.
_UNLOAD_READS = [
    pytest.param(None, {}, id="whole"),
    pytest.param(3, {}, id="chunks"),
    pytest.param(3, {"low_memory": True}, id="scanned-chunks"),
]
# The warnings of Polars 1.x reads of timestamps with pyarrow and at nanoseconds.
_PYARROW_READ = "so they are read with pyarrow instead"
_NANOSECOND_READ = "so those outside 1677-09-21 to 2262-04-11 are wrong"


def _chunked_result_set() -> AthenaPolarsResultSet:
    """Create a chunked result set without calling Athena.

    Returns:
        An AthenaPolarsResultSet with only the attributes used by the chunk readers.
    """
    result_set = AthenaPolarsResultSet.__new__(AthenaPolarsResultSet)  # bypass __init__
    result_set._chunksize = 10_000
    result_set._cache_type = None
    result_set._kwargs = {}
    result_set._metadata = None
    result_set._connection = MagicMock()
    return result_set


def _read_all(result: pl.DataFrame | Iterator[pl.DataFrame]) -> pl.DataFrame:
    """Read the result of a whole read or a chunk reader into one DataFrame.

    Args:
        result: A DataFrame, or an iterator of DataFrame chunks.

    Returns:
        The DataFrame, or the chunks concatenated.
    """
    return result if isinstance(result, pl.DataFrame) else pl.concat(list(result))


def _fetching_result_set(chunks, names, arraysize, converters=None) -> AthenaPolarsResultSet:
    """Create a result set of a succeeded query whose chunks are ``chunks``, without AWS.

    Args:
        chunks: An iterable of DataFrame chunks.
        names: The column names of the rows.
        arraysize: The result set's arraysize.
        converters: The converters keyed by column name; identities by default.

    Returns:
        An AthenaPolarsResultSet that reads ``chunks`` as its chunked result.
    """
    query_execution = MagicMock(
        state=AthenaQueryExecution.STATE_SUCCEEDED, output_location="s3://bucket/result.csv"
    )
    with (
        patch.object(AthenaPolarsResultSet, "_pre_fetch"),
        patch.object(
            AthenaPolarsResultSet,
            "_create_dataframe_iterator",
            return_value=PolarsDataFrameIterator(iter(chunks), converters or {}, names),
        ),
    ):
        return AthenaPolarsResultSet(
            connection=MagicMock(),
            converter=DefaultPolarsTypeConverter(),
            query_execution=query_execution,
            arraysize=arraysize,
            retry_config=RetryConfig(),
            chunksize=10_000,
        )


def _read_unload_result(tmp_path, chunksize, kwargs, table=None):
    """Read a Parquet file of an UNLOAD result through the result set, without AWS.

    Args:
        table: The table to write as Athena writes it, with INT96 timestamps and no
            Arrow schema, or None for the file that Athena wrote, ``INT96_UNLOAD_FILE``.

    Returns:
        The DataFrames that were read: the whole result, or its chunks.
    """
    path = tmp_path / "0.parquet"
    if table is None:
        shutil.copy(INT96_UNLOAD_FILE, path)
    else:
        pq.write_table(table, path, use_deprecated_int96_timestamps=True, store_schema=False)
    result_set = _chunked_result_set()
    result_set._chunksize = chunksize
    result_set._kwargs = dict(kwargs)
    result_set._unload_location = f"{tmp_path}/"
    with (
        patch.object(AthenaPolarsResultSet, "_read_data_manifest", return_value=[str(path)]),
        patch.object(
            AthenaPolarsResultSet, "is_unload", new_callable=PropertyMock, return_value=True
        ),
        patch.object(AthenaPolarsResultSet, "_get_parquet_storage_options", return_value={}),
        patch.object(
            AthenaPolarsResultSet,
            "_csv_storage_options",
            new_callable=PropertyMock,
            return_value={},
        ),
        patch("pyathena.polars.result_set.S3FileSystem", lambda **_: LocalFileSystem()),
    ):
        if chunksize is None:
            return [result_set._as_polars()]
        assert result_set._prepare_parquet_location()
        return list(result_set._iter_parquet_chunks(result_set._read_parquet_schema()))


class TestAthenaPolarsResultSet:
    def test_fetch_across_chunks(self):
        """The fetch methods continue across chunks, reading a chunk when its rows are needed."""
        read = []

        def chunks():
            for start in (0, 4, 8):
                read.append(start)
                stop = min(start + 4, 10)
                yield pl.DataFrame(
                    {"n": range(start, stop), "s": [f"v{i}" for i in range(start, stop)]}
                )

        result_set = _fetching_result_set(chunks(), ["n", "s"], arraysize=3)
        assert result_set.rownumber == 0
        assert result_set.fetchone() == (0, "v0")
        assert result_set.rownumber == 1
        assert result_set.fetchmany(3) == [(1, "v1"), (2, "v2"), (3, "v3")]
        assert result_set.rownumber == 4
        assert read == [0]
        assert result_set.fetchmany(5) == [(i, f"v{i}") for i in range(4, 9)]
        assert result_set.rownumber == 9
        assert read == [0, 4, 8]
        assert result_set.fetchmany() == [(9, "v9")]
        assert result_set.rownumber == 10
        assert result_set.fetchmany() == []
        assert result_set.fetchall() == []
        assert result_set.fetchone() is None
        assert result_set.rownumber == 10

    def test_fetchall_across_chunks(self):
        """fetchall() returns the remaining rows of all chunks, skipping empty chunks."""
        chunks = [
            pl.DataFrame({"n": [0, 1]}),
            pl.DataFrame({"n": []}, schema={"n": pl.Int64}),
            pl.DataFrame({"n": [2, 3]}),
        ]
        result_set = _fetching_result_set(chunks, ["n"], arraysize=3)
        assert result_set.fetchone() == (0,)
        assert result_set.fetchall() == [(1,), (2,), (3,)]
        assert result_set.rownumber == 4
        assert result_set.fetchall() == []
        assert result_set.rownumber == 4

    @pytest.mark.parametrize(
        ("dates", "method", "rownumber"),
        [
            (["2024-01-01", "bad", "2024-01-03"], "fetchone", 1),
            (["2024-01-01", "bad", "2024-01-03"], "fetchmany", 1),
            (["2024-01-01", "bad", "2024-01-03"], "fetchall", 1),
            (["2024-01-01", "2024-01-02", "bad"], "fetchmany", 2),
            (["2024-01-01", "2024-01-02", "bad"], "fetchall", 2),
        ],
    )
    def test_fetch_after_conversion_error(self, dates, method, rownumber):
        """After a value fails to convert, the fetch methods return no more rows.

        ``rownumber`` counts the rows taken before the error, as fetching row by row did.
        """
        chunks = [
            pl.DataFrame({"n": [0, 1, 2], "d": dates}),
            pl.DataFrame({"n": [3], "d": ["2024-01-04"]}),
        ]
        result_set = _fetching_result_set(
            chunks, ["n", "d"], arraysize=3, converters={"d": _to_date}
        )
        assert result_set.fetchone() == (0, date(2024, 1, 1))
        with pytest.raises(ValueError, match="bad"):
            getattr(result_set, method)()
        assert result_set.rownumber == rownumber
        assert result_set.fetchone() is None
        assert result_set.fetchmany() == []
        assert result_set.fetchall() == []
        assert result_set.rownumber == rownumber

    def test_fetch_after_chunks_run_out(self):
        """The rows of a chunk that a fetch started stay after iter_chunks() runs out."""
        size = PolarsDataFrameIterator._ROW_BLOCK_SIZE * 2
        result_set = _fetching_result_set([pl.DataFrame({"n": range(size)})], ["n"], arraysize=3)
        assert result_set.fetchone() == (0,)
        assert list(result_set.iter_chunks()) == []
        assert result_set.fetchall() == [(i,) for i in range(1, size)]
        assert result_set.rownumber == size

    def test_close_during_fetchall(self):
        """A fetchall() in another thread does not set rownumber after close()."""
        requested = threading.Event()
        closed = threading.Event()

        class Chunks:
            def __init__(self):
                self._chunks = iter([pl.DataFrame({"n": [0, 1]}), pl.DataFrame({"n": [2, 3]})])
                self._calls = 0

            def __iter__(self):
                return self

            def __next__(self):
                self._calls += 1
                if self._calls == 2:
                    requested.set()
                    assert closed.wait(10)
                return next(self._chunks)

        result_set = _fetching_result_set(Chunks(), ["n"], arraysize=3)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(result_set.fetchall)
            assert requested.wait(10)
            result_set.close()
            closed.set()
            # The fetch still returns the chunk that it was reading.
            assert future.result(10) == [(0,), (1,), (2,), (3,)]
        assert result_set.rownumber is None

    def test_close_during_fetchall_generator(self):
        """A close() during a fetch from a generator, such as a chunked UNLOAD, does not raise."""
        reading, release = threading.Event(), threading.Event()

        def chunks():
            yield pl.DataFrame({"n": [0, 1]})
            reading.set()
            release.wait(10)
            yield pl.DataFrame({"n": [2, 3]})
            yield pl.DataFrame({"n": [4]})

        reader = chunks()
        result_set = _fetching_result_set(reader, ["n"], arraysize=3)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(result_set.fetchall)
            assert reading.wait(10)
            result_set.close()
            release.set()
            assert future.result(10) == [(0,), (1,), (2,), (3,)]
        assert inspect.getgeneratorstate(reader) == inspect.GEN_CLOSED

    @pytest.mark.parametrize(
        ("unload", "kwargs", "credentials"),
        [(False, {}, (0, 1)), (True, {}, (1, 1)), (True, {"low_memory": True}, (1, 2))],
        ids=["csv", "parquet-chunk-reader", "scan-parquet"],
    )
    def test_close_during_first_read(self, tmp_path, unload, kwargs, credentials):
        """A close() as a fetch starts its first read leaves the fetch its chunk.

        A fetch in another thread, such as one of AioPolarsCursor that keeps running
        after its task is cancelled, can start its first read as close() lands. The
        credentials of the Polars scans are frozen at the first read, so ``credentials``
        counts them after the result set is created and after the read; the schema of
        an UNLOAD result is read when it is created.
        """
        if unload:
            pl.DataFrame({"a": [1, 2, 3]}).write_parquet(tmp_path / "0.parquet")
            manifest = str(tmp_path / "0.parquet")
            output_location = "s3://bucket/result.csv"
        else:
            (tmp_path / "result.csv").write_text('"a"\n"1"\n"2"\n"3"\n')
            manifest = ""
            output_location = str(tmp_path / "result.csv")
        connection = MagicMock(region_name=None)
        connection.session.get_credentials.return_value = None
        connection.s3_client.get_object.side_effect = lambda **_: {
            "Body": BytesIO(manifest.encode())
        }
        query_execution = MagicMock(
            state=AthenaQueryExecution.STATE_SUCCEEDED,
            output_location=output_location,
            data_manifest_location="s3://bucket/manifest.csv",
            query="UNLOAD (SELECT a FROM t) TO 's3://bucket/unload/' WITH (format = 'PARQUET')"
            if unload
            else "SELECT a FROM t",
        )
        metadata = (
            {"Name": "a", "Type": "integer", "Precision": 10, "Scale": 0, "Nullable": "UNKNOWN"},
        )
        with (
            patch.object(
                AthenaPolarsResultSet,
                "_pre_fetch",
                autospec=True,
                side_effect=lambda rs: setattr(rs, "_metadata", metadata),
            ),
            patch.object(AthenaPolarsResultSet, "_get_content_length", return_value=16),
            patch("pyathena.polars.result_set.S3FileSystem", lambda **_: LocalFileSystem()),
        ):
            result_set = AthenaPolarsResultSet(
                connection=connection,
                converter=DefaultPolarsTypeConverter(),
                query_execution=query_execution,
                arraysize=3,
                retry_config=RetryConfig(),
                unload=unload,
                chunksize=2,
                **kwargs,
            )
        assert connection.session.get_credentials.call_count == credentials[0]
        reading, release = threading.Event(), threading.Event()
        reader = result_set._df_iter._reader

        def blocked():
            reading.set()
            assert release.wait(10)
            yield from reader

        result_set._df_iter._reader = blocked()
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(result_set.fetchall)
            assert reading.wait(10)
            result_set.close()
            release.set()
            assert future.result(10) == [(1,), (2,)]
        assert connection.session.get_credentials.call_count == credentials[1]

    def test_fetchmany_size_above_maxsize(self):
        """A size above sys.maxsize returns the remaining rows."""
        result_set = _fetching_result_set([pl.DataFrame({"n": [1, 2]})], ["n"], arraysize=3)
        assert result_set.fetchmany(10**100) == [(1,), (2,)]
        assert result_set.rownumber == 2

    def test_fetchmany_size_not_integer(self):
        """A size that is not an integer raises TypeError without consuming rows."""
        result_set = _fetching_result_set([pl.DataFrame({"n": [1, 2]})], ["n"], arraysize=3)
        with pytest.raises(TypeError):
            result_set.fetchmany(1.5)
        assert result_set.fetchall() == [(1,), (2,)]

    def test_fetch_after_close(self):
        """The fetch methods return no rows after close()."""
        result_set = _fetching_result_set([pl.DataFrame({"n": [1, 2]})], ["n"], arraysize=3)
        result_set.close()
        assert result_set.fetchone() is None
        assert result_set.fetchmany() == []
        assert result_set.fetchall() == []
        assert result_set.rownumber is None

    def test_iter_csv_chunks_raises_when_read_fails_partway(self, tmp_path):
        """A CSV read that fails partway through the data raises instead of ending early."""
        path = tmp_path / "result.csv"
        path.write_text(
            "a\n" + "".join(f"{i}\n" for i in range(_ROWS_BEFORE_FAILURE)) + "not-a-number\n"
        )
        result_set = _chunked_result_set()
        with (
            patch.object(
                AthenaPolarsResultSet,
                "output_location",
                new_callable=PropertyMock,
                return_value=str(path),
            ),
            patch.object(
                AthenaPolarsResultSet,
                "_csv_dtypes",
                new_callable=PropertyMock,
                return_value={"a": pl.Int64},
            ),
            patch.object(AthenaPolarsResultSet, "_get_parquet_storage_options", return_value={}),
            patch.object(AthenaPolarsResultSet, "_is_csv_readable", return_value=True),
            pytest.raises(OperationalError, match="not-a-number"),
        ):
            list(result_set._iter_csv_chunks())

    @pytest.mark.parametrize(
        "read_options",
        [{}, {"low_memory": True}],
        ids=["parquet-chunk-reader", "scan-parquet"],
    )
    def test_iter_parquet_chunks_raises_when_read_fails_partway(self, tmp_path, read_options):
        """A Parquet read that fails partway through the data raises instead of ending early.

        Without read options, the files of the data manifest are read with
        ParquetChunkReader; with them, Polars scans the unload location.
        """
        pl.DataFrame({"a": range(_ROWS_BEFORE_FAILURE)}).write_parquet(
            tmp_path / "0.parquet", row_group_size=10_000
        )
        valid = (tmp_path / "0.parquet").read_bytes()
        # Keep the footer magic but corrupt the metadata so the second file fails to read.
        (tmp_path / "1.parquet").write_bytes(valid[: len(valid) // 2] + b"\x00" * 64 + valid[-8:])
        result_set = _chunked_result_set()
        result_set._kwargs = dict(read_options)
        result_set._unload_location = f"{tmp_path}/"
        result_set._data_manifest = [str(tmp_path / "0.parquet"), str(tmp_path / "1.parquet")]
        chunks: list[pl.DataFrame] = []
        with (
            patch.object(AthenaPolarsResultSet, "_get_parquet_storage_options", return_value={}),
            patch.object(
                AthenaPolarsResultSet,
                "_csv_storage_options",
                new_callable=PropertyMock,
                return_value={},
            ),
            patch("pyathena.polars.result_set.S3FileSystem", lambda **_: LocalFileSystem()),
            patch("polars.scan_parquet", wraps=pl.scan_parquet) as scan_parquet,
            pytest.raises(OperationalError) as error,
        ):
            # extend() keeps the chunks read before the failure.
            chunks.extend(result_set._iter_parquet_chunks(pl.Schema({"a": pl.Int64})))
        # The error comes from the corrupt file, after the valid one was read.
        assert not isinstance(error.value.__cause__, AttributeError)
        assert scan_parquet.called is bool(read_options)
        if not read_options:
            assert sum(chunk.height for chunk in chunks) == _ROWS_BEFORE_FAILURE

    @pytest.mark.parametrize("reader", ["_read_csv", "_iter_csv_chunks"])
    def test_csv_read_kwargs_replace_defaults(self, tmp_path, reader):
        """Read arguments given to execute() replace the ones the result set chooses."""
        path = tmp_path / "result.csv"
        path.write_text("1;x\n2;y\n")
        # Polars names headerless columns from column_1 before 2.0 and from column_0 since.
        first, second = pl.read_csv(b"1;x\n", separator=";", has_header=False).columns
        result_set = _chunked_result_set()
        result_set._kwargs = {
            "separator": ";",
            "has_header": False,
            "schema_overrides": {first: pl.Utf8},
        }
        with (
            patch.object(
                AthenaPolarsResultSet,
                "output_location",
                new_callable=PropertyMock,
                return_value=str(path),
            ),
            patch.object(
                AthenaPolarsResultSet,
                "_csv_dtypes",
                new_callable=PropertyMock,
                return_value={"1;x": pl.Int64},
            ),
            patch.object(
                AthenaPolarsResultSet,
                "_csv_storage_options",
                new_callable=PropertyMock,
                return_value={},
            ),
            patch.object(AthenaPolarsResultSet, "_get_parquet_storage_options", return_value={}),
            patch.object(AthenaPolarsResultSet, "_is_csv_readable", return_value=True),
        ):
            df = _read_all(getattr(result_set, reader)())
        assert df.to_dict(as_series=False) == {first: ["1", "2"], second: ["x", "y"]}

    @pytest.mark.parametrize("reader", ["_read_csv", "_iter_csv_chunks"])
    def test_txt_new_columns_with_schema_overrides(self, tmp_path, reader):
        """schema_overrides given with new_columns reach Polars with them for a .txt file."""
        path = tmp_path / "result.txt"
        path.write_text("001\tx\n")
        result_set = _chunked_result_set()
        # Polars 2.0 rejects new_columns that do not name every column of the file.
        result_set._kwargs = {"new_columns": ["z", "w"], "schema_overrides": {"z": pl.Utf8}}
        with (
            patch.object(
                AthenaPolarsResultSet,
                "output_location",
                new_callable=PropertyMock,
                return_value=str(path),
            ),
            patch.object(AthenaPolarsResultSet, "_get_column_names", return_value=["a", "b"]),
            patch.object(
                AthenaPolarsResultSet, "_csv_dtypes", new_callable=PropertyMock, return_value={}
            ),
            patch.object(
                AthenaPolarsResultSet,
                "_csv_storage_options",
                new_callable=PropertyMock,
                return_value={},
            ),
            patch.object(AthenaPolarsResultSet, "_get_parquet_storage_options", return_value={}),
            patch.object(AthenaPolarsResultSet, "_is_csv_readable", return_value=True),
        ):
            df = _read_all(getattr(result_set, reader)())
        assert df.to_dict(as_series=False) == {"z": ["001"], "w": ["x"]}

    @pytest.mark.parametrize(
        ("reader", "function"),
        [
            ("_iter_csv_chunks", "scan_csv"),
            ("_read_parquet", "read_parquet"),
            ("_iter_parquet_chunks", "scan_parquet"),
            ("_read_parquet_schema", "scan_parquet"),
        ],
    )
    def test_storage_options_replace_defaults(self, reader, function):
        """storage_options given to execute() replace PyAthena's without computing them."""
        result_set = _chunked_result_set()
        result_set._unload_location = "s3://bucket/unload/"
        result_set._kwargs = {"storage_options": {"anon": True}}
        with (
            patch.object(
                AthenaPolarsResultSet,
                "output_location",
                new_callable=PropertyMock,
                return_value="s3://bucket/result.csv",
            ),
            patch.object(
                AthenaPolarsResultSet, "_csv_dtypes", new_callable=PropertyMock, return_value={}
            ),
            patch.object(
                AthenaPolarsResultSet,
                "_csv_storage_options",
                new_callable=PropertyMock,
                side_effect=AssertionError("replaced storage options were computed"),
            ),
            patch.object(
                AthenaPolarsResultSet,
                "_get_parquet_storage_options",
                side_effect=AssertionError("replaced storage options were computed"),
            ),
            patch.object(AthenaPolarsResultSet, "_is_csv_readable", return_value=True),
            patch.object(AthenaPolarsResultSet, "_prepare_parquet_location", return_value=True),
            patch(f"polars.{function}") as read,
            patch("pyathena.polars.result_set.to_column_info"),
        ):
            # The Parquet readers take the schema of the files.
            args = (pl.Schema(),) if reader in ("_read_parquet", "_iter_parquet_chunks") else ()
            result = getattr(result_set, reader)(*args)
            if not isinstance(result, (pl.DataFrame, tuple)):
                list(result)
        assert read.call_args.kwargs["storage_options"] == {"anon": True}

    def test_csv_storage_options_replace_defaults(self):
        """storage_options given to execute() open a CSV result through fsspec, not Polars."""
        result_set = _chunked_result_set()
        result_set._kwargs = {"storage_options": {"anon": True}}
        with (
            patch.object(
                AthenaPolarsResultSet,
                "output_location",
                new_callable=PropertyMock,
                return_value="s3://bucket/result.csv",
            ),
            patch.object(
                AthenaPolarsResultSet, "_csv_dtypes", new_callable=PropertyMock, return_value={}
            ),
            patch.object(
                AthenaPolarsResultSet,
                "_csv_storage_options",
                new_callable=PropertyMock,
                side_effect=AssertionError("replaced storage options were computed"),
            ),
            patch.object(AthenaPolarsResultSet, "_is_csv_readable", return_value=True),
            patch("pyathena.polars.result_set.fsspec.open") as open_,
        ):
            open_.return_value.__enter__.return_value.read.return_value = b"a\n1\n"
            df = result_set._read_csv()
        open_.assert_called_once_with("s3://bucket/result.csv", "rb", anon=True)
        assert df.to_dict(as_series=False) == {"a": [1]}

    @pytest.mark.parametrize(
        ("reader", "kwargs", "expected"),
        [
            ("_read_csv", {}, {"t": _TRUNCATED_TIMESTAMPS}),
            ("_iter_csv_chunks", {}, {"t": _TRUNCATED_TIMESTAMPS}),
            ("_read_csv", {"columns": ["v"]}, {}),
            ("_read_csv", {"new_columns": ["t2", "v2"]}, {"t2": _TRUNCATED_TIMESTAMPS}),
            ("_iter_csv_chunks", {"new_columns": ["t2", "v2"]}, {"t2": _TRUNCATED_TIMESTAMPS}),
            ("_read_csv", {"schema": {"t": pl.String, "v": pl.String}}, {"t": _TIMESTAMP_TEXTS}),
            (
                "_iter_csv_chunks",
                {"schema": {"t": pl.String, "v": pl.String}},
                {"t": _TIMESTAMP_TEXTS},
            ),
            (
                "_read_csv",
                {"with_column_names": lambda names: [n.upper() for n in names]},
                OperationalError,
            ),
            (
                "_iter_csv_chunks",
                {"with_column_names": lambda names: [n.upper() for n in names]},
                OperationalError,
            ),
        ],
    )
    def test_read_csv_truncates_timestamps(self, tmp_path, reader, kwargs, expected):
        """Timestamps are read as text and truncated to the time unit.

        A whole read does so when parsing them fails; a chunked read always does.

        With ``with_column_names`` given to execute(), Polars renames the columns as it
        reads them, so they are not read as text, and the read fails. With ``schema``,
        the columns have the types it gives. ``scan_csv()`` does not take ``columns``.
        """
        path = tmp_path / "result.csv"
        path.write_bytes(b'"t","v"\n"2020-01-02 03:04:05.123456789012","x"\n,"y"\n')
        result_set = _chunked_result_set()
        result_set._query_execution = None
        result_set._converter = DefaultPolarsTypeConverter()
        result_set._kwargs = kwargs
        result_set._metadata = tuple(
            {"Name": n, "Type": t, "Precision": 3, "Scale": 0, "Nullable": "UNKNOWN"}
            for n, t in (("t", "timestamp"), ("v", "varchar"))
        )
        with (
            patch.object(
                AthenaPolarsResultSet,
                "output_location",
                new_callable=PropertyMock,
                return_value=str(path),
            ),
            patch.object(
                AthenaPolarsResultSet,
                "_csv_storage_options",
                new_callable=PropertyMock,
                return_value={},
            ),
            patch.object(AthenaPolarsResultSet, "_get_parquet_storage_options", return_value={}),
            patch.object(AthenaPolarsResultSet, "_is_csv_readable", return_value=True),
        ):
            if expected is OperationalError:
                with pytest.raises(OperationalError):
                    _read_all(getattr(result_set, reader)())
                return
            df = _read_all(getattr(result_set, reader)())
        for name, values in expected.items():
            assert df[name].equals(values, check_dtypes=True)
        if not expected:
            assert df.columns == ["v"]

    @pytest.mark.parametrize("reader", ["_read_csv", "_iter_csv_chunks"])
    def test_read_csv_ignore_errors(self, tmp_path, reader):
        """With ignore_errors, timestamp text that is not a timestamp becomes null.

        Timestamps with more than nine fractional digits, which Polars itself reads as
        null with ignore_errors, are still truncated.
        """
        path = tmp_path / "result.csv"
        path.write_bytes(b'"t"\n"2020-01-02 03:04:05.123456789012"\n"invalid"\n')
        result_set = _chunked_result_set()
        result_set._query_execution = None
        result_set._converter = DefaultPolarsTypeConverter()
        result_set._kwargs = {"ignore_errors": True}
        result_set._metadata = (
            {"Name": "t", "Type": "timestamp", "Precision": 3, "Scale": 0, "Nullable": "UNKNOWN"},
        )
        with (
            patch.object(
                AthenaPolarsResultSet,
                "output_location",
                new_callable=PropertyMock,
                return_value=str(path),
            ),
            patch.object(
                AthenaPolarsResultSet,
                "_csv_storage_options",
                new_callable=PropertyMock,
                return_value={},
            ),
            patch.object(AthenaPolarsResultSet, "_get_parquet_storage_options", return_value={}),
            patch.object(AthenaPolarsResultSet, "_is_csv_readable", return_value=True),
        ):
            df = _read_all(getattr(result_set, reader)())
        assert df["t"].equals(
            pl.Series([datetime(2020, 1, 2, 3, 4, 5, 123456), None], dtype=pl.Datetime("us")),
            check_dtypes=True,
        )

    @pytest.mark.parametrize(("chunksize", "kwargs"), _UNLOAD_READS)
    def test_read_parquet_int96(self, tmp_path, caplog, chunksize, kwargs):
        """INT96 timestamps that Athena writes are read at microseconds at any level.

        Polars 1.x reads INT96 only at nanoseconds: it reads a whole result without
        read options with pyarrow, and scans with them at nanoseconds, giving wrong
        values outside 1677-09-21 to 2262-04-11; both log a warning.
        """
        with caplog.at_level(logging.WARNING, logger="pyathena.polars.result_set"):
            df = pl.concat(_read_unload_result(tmp_path, chunksize, kwargs)).sort("id")
        timestamp = pl.Datetime("us")
        assert df.drop("id").schema == pl.Schema(
            {
                "ts": timestamp,
                "arr": pl.List(timestamp),
                "st": pl.Struct({"t": timestamp}),
                "mp": _MAP,
            }
        )
        scanned_by_polars_1 = not _POLARS_2 and chunksize is not None and bool(kwargs)
        assert (_NANOSECOND_READ in caplog.text) is scanned_by_polars_1
        assert (_PYARROW_READ in caplog.text) is (
            not _POLARS_2 and chunksize is None and not kwargs
        )
        if scanned_by_polars_1:
            return
        assert df["ts"].to_list() == INT96_VALUES
        assert df["arr"].to_list() == [[v] for v in INT96_VALUES]
        assert df["st"].to_list() == [{"t": v} for v in INT96_VALUES]
        assert df["mp"].to_list() == (
            [{"k": v} for v in INT96_VALUES]
            if _POLARS_2
            else [[{"key": "k", "value": v}] for v in INT96_VALUES]
        )

    @pytest.mark.skipif(_POLARS_2, reason="Polars 2.0 reads INT96 at microseconds.")
    @pytest.mark.parametrize(
        ("chunksize", "kwargs", "pyarrow", "warned"),
        [
            (None, {}, False, True),
            (3, {}, False, True),
            (None, {"low_memory": True}, True, True),
            (None, {"columns": ["id"]}, True, False),
        ],
        ids=["whole-without-pyarrow", "chunks-without-pyarrow", "read-options", "no-timestamps"],
    )
    def test_read_parquet_int96_polars_1(
        self, tmp_path, caplog, chunksize, kwargs, pyarrow, warned
    ):
        """Polars 1.x reads timestamps at nanoseconds without pyarrow or with read options.

        They are cast to microseconds, and a warning is logged if the read has any.
        """
        timestamp = datetime(2020, 1, 2, 3, 4, 5, 123000)
        table = pa.table({"id": [1], "ts": pa.array([timestamp], pa.timestamp("us"))})
        with (
            patch(
                "pyathena.polars.result_set.importlib.util.find_spec",
                return_value=MagicMock() if pyarrow else None,
            ),
            caplog.at_level(logging.WARNING, logger="pyathena.polars.result_set"),
        ):
            df = pl.concat(_read_unload_result(tmp_path, chunksize, kwargs, table=table))
        if warned:
            assert df.schema["ts"] == pl.Datetime("us")
            assert df["ts"].to_list() == [timestamp]
        assert df["id"].to_list() == [1]
        assert (_NANOSECOND_READ in caplog.text) is warned

    @pytest.mark.skipif(not _POLARS_2, reason="Polars 1.x has no Map type.")
    @pytest.mark.parametrize(
        ("chunksize", "kwargs"), [(None, {}), (3, {"low_memory": True})], ids=["whole", "scanned"]
    )
    def test_read_parquet_int96_map_named_like_a_selector(self, tmp_path, chunksize, kwargs):
        """A Map is restored by its name, which Polars would read as a selector in pl.col()."""
        timestamp = datetime(1600, 1, 2, 3, 4, 5, 123000)
        table = pa.table(
            {
                "id": [1],
                "*": pa.array([[("k", timestamp)]], pa.map_(pa.string(), pa.timestamp("us"))),
            }
        )
        df = pl.concat(_read_unload_result(tmp_path, chunksize, kwargs, table=table))
        assert df.schema == pl.Schema({"id": pl.Int64, "*": _MAP})
        assert df.row(0) == (1, {"k": timestamp})

    @pytest.mark.skipif(not _POLARS_2, reason="Polars 1.x reads INT96 at nanoseconds.")
    @pytest.mark.parametrize("chunksize", [None, 3], ids=["whole", "scanned-chunks"])
    def test_read_parquet_int96_schema_option(self, tmp_path, chunksize):
        """A schema given to execute() replaces the one that reads INT96 at microseconds."""
        timestamp = pl.Datetime("us")
        schema = {
            "id": pl.Int32,
            "ts": pl.Datetime("ms"),
            "arr": pl.List(timestamp),
            "st": pl.Struct({"t": timestamp}),
            "mp": pl.List(pl.Struct({"key": pl.String, "value": timestamp})),
        }
        kwargs = {"schema": schema} if chunksize is None else {"schema": schema, "low_memory": True}
        df = pl.concat(_read_unload_result(tmp_path, chunksize, kwargs)).sort("id")
        assert df.schema == pl.Schema(schema)
        assert df["ts"].to_list() == INT96_VALUES
        assert df["mp"].to_list() == [[{"key": "k", "value": v}] for v in INT96_VALUES]

    @pytest.mark.parametrize(("chunksize", "kwargs"), [(None, {}), (3, {"low_memory": True})])
    def test_read_parquet_storage_options_error(self, chunksize, kwargs):
        """A failure to get the storage options, such as of credentials, is OperationalError."""
        result_set = _chunked_result_set()
        result_set._chunksize = chunksize
        result_set._kwargs = dict(kwargs)
        result_set._unload_location = "s3://bucket/unload/"
        schema = pl.Schema({"i": pl.Int64})
        with patch.object(
            AthenaPolarsResultSet,
            "_get_parquet_storage_options",
            side_effect=RuntimeError("credentials"),
        ):
            if chunksize is None:
                with pytest.raises(OperationalError, match="credentials"):
                    result_set._read_parquet(schema)
            else:
                chunks = result_set._iter_parquet_chunks(schema)
                with pytest.raises(OperationalError, match="credentials"):
                    next(chunks)

    def test_read_parquet_without_timestamps(self, tmp_path, caplog):
        """A result without timestamps is read by Polars with read options and no warning."""
        table = pa.table({"i": [1, None]})
        with caplog.at_level(logging.WARNING, logger="pyathena.polars.result_set"):
            (df,) = _read_unload_result(tmp_path, None, {"low_memory": True}, table=table)
        assert df["i"].to_list() == [1, None]
        assert not caplog.text


class TestPolarsDataFrameIterator:
    @pytest.mark.parametrize(
        "series",
        [
            pl.Series([True, None, False]),
            pl.Series([1, None, 3], dtype=pl.Int8),
            pl.Series([1, None, 2**62]),
            pl.Series([1.5, None, float("inf")], dtype=pl.Float32),
            pl.Series(["a", None, ""]),
            pl.Series([Decimal("1.23"), None, Decimal("-4.50")], dtype=pl.Decimal(10, 2)),
            pl.Series([date(2024, 1, 1), None, date(1, 1, 1)]),
            pl.Series([datetime(2024, 1, 2, 3, 4, 5, 123456), None, datetime(1970, 1, 1)]),
            pl.Series([datetime(2024, 1, 2, tzinfo=UTC), None, None]).dt.convert_time_zone(
                "Asia/Tokyo"
            ),
            pl.Series([timedelta(seconds=1), None, timedelta(days=-1)]),
            pl.Series([b"\x00\x01", None, b""]),
            pl.Series([[1, 2], None, []]),
            pl.Series([{"a": 1, "b": "x"}, None, {"a": None, "b": None}]),
            pl.Series([[{"a": [1]}], None, [{"a": None}]]),
            pl.Series([None, None, None], dtype=pl.Null),
            pl.Series([1.5, None, float("nan")]),
            pl.Series([2**64 - 1, None, 0], dtype=pl.UInt64),
            pl.Series([2**100, None, -1], dtype=pl.Int128),
            pl.Series([datetime(2024, 1, 2, 3, 4, 5, 123456), None, None]).cast(pl.Datetime("ns")),
            pl.Series([time(1, 2, 3), None, time(23, 59, 59, 999999)]),
            pl.Series([[1, 2], None, [3, 4]], dtype=pl.Array(pl.Int64, 2)),
            pl.Series(["x", None, "y"], dtype=pl.Categorical),
            pl.Series(["x", None, "y"], dtype=pl.Enum(["x", "y"])),
            pl.Series([object, None, 1], dtype=pl.Object),
        ],
        ids=lambda series: str(series.dtype),
    )
    def test_itertuples_values(self, series):
        """The rows hold the values that DataFrame.iter_rows() gives, with their types."""
        df = pl.DataFrame({"c": series, "n": [10, 11, 12]})
        expected = [(row["c"], row["n"]) for row in df.iter_rows(named=True)]

        rows = list(PolarsDataFrameIterator(df, {}, ["c", "n"]).itertuples())

        assert [[(type(v), repr(v)) for v in row] for row in rows] == [
            [(type(v), repr(v)) for v in row] for row in expected
        ]

    def test_itertuples_converters(self):
        """Each column's converter applies to its values, by the iterator's column names.

        A name that the chunk does not have gives None to its converter, and a column
        that the names do not include is left out.
        """
        df = pl.DataFrame({"d": ["2024-01-01", None], "s": ["a", None], "extra": [1, 2]})
        converters = {"d": _to_date, "s": _to_default, "missing": lambda v: ("missing", v)}

        rows = list(PolarsDataFrameIterator(df, converters, ["s", "missing", "d"]).itertuples())

        assert rows == [("a", ("missing", None), date(2024, 1, 1)), (None, ("missing", None), None)]

    def test_itertuples_converter_stop_iteration(self):
        """A StopIteration from a converter raises instead of ending the rows of its block."""

        def convert(value):
            if value == "bad":
                raise StopIteration
            return value

        chunks = [pl.DataFrame({"s": ["good", "bad", "lost"]}), pl.DataFrame({"s": ["later"]})]
        rows = PolarsDataFrameIterator(iter(chunks), {"s": convert}, ["s"]).itertuples()

        assert next(rows) == ("good",)
        with pytest.raises(RuntimeError):
            next(rows)
        assert list(rows) == []

    def test_itertuples_value_python_does_not_hold(self):
        """The rows before a value that Python does not hold are returned before it raises."""
        # 0 is 1970-01-01, and -719528 is 0000-01-01, a year that Python's date does not hold.
        dates = pl.Series([0, 0, -719528, 0], dtype=pl.Int32).cast(pl.Date)
        df = pl.DataFrame({"d": dates, "n": [1, 2, 3, 4]})
        rows = PolarsDataFrameIterator(df, {}, ["d", "n"]).itertuples()

        assert [next(rows), next(rows)] == [(date(1970, 1, 1), 1), (date(1970, 1, 1), 2)]
        with pytest.raises(ValueError, match="year"):
            next(rows)
        assert list(rows) == []

    def test_itertuples_nested_value_python_does_not_hold(self):
        """The rows before a nested value that Python does not hold are returned too.

        Polars raises the failure within a struct as a PanicException.
        """
        dates = pl.Series([0, 0, -719528], dtype=pl.Int32).cast(pl.Date)
        df = pl.DataFrame({"s": pl.DataFrame({"d": dates}).to_struct(), "n": [1, 2, 3]})
        rows = PolarsDataFrameIterator(df, {}, ["s", "n"]).itertuples()

        assert [next(rows), next(rows)] == [
            ({"d": date(1970, 1, 1)}, 1),
            ({"d": date(1970, 1, 1)}, 2),
        ]
        with pytest.raises(pl.exceptions.PanicException):
            next(rows)
        assert list(rows) == []

    def test_itertuples_large_chunk(self):
        """The rows of a chunk larger than a block of rows keep their order."""
        size = PolarsDataFrameIterator._ROW_BLOCK_SIZE * 2 + 1
        df = pl.DataFrame({"n": range(size), "s": [f"v{i}" for i in range(size)]})

        rows = list(PolarsDataFrameIterator(df, {"s": _to_default}, ["n", "s"]).itertuples())

        assert rows == [(i, f"v{i}") for i in range(size)]

    def test_itertuples_without_column_names(self):
        """Without column names, each row of a chunk is empty."""
        df = pl.DataFrame({"n": [1, 2]})

        assert list(PolarsDataFrameIterator(df, {}, []).itertuples()) == [(), ()]
        assert list(PolarsDataFrameIterator(df, {}, []).iterrows()) == [(0, {}), (1, {})]

    def test_iterrows_continuous_row_numbers(self):
        """Row numbers continue across chunks, with each row keyed by the column names."""
        chunks = [pl.DataFrame({"n": [1, 2]}), pl.DataFrame({"n": [3]})]

        rows = list(PolarsDataFrameIterator(iter(chunks), {}, ["n"]).iterrows())

        assert rows == [(0, {"n": 1}), (1, {"n": 2}), (2, {"n": 3})]

    @pytest.mark.parametrize(
        "reader",
        ["dataframe", "generator"],
        ids=["dataframe", "generator"],
    )
    def test_close_stops_iteration(self, reader):
        """A closed iterator yields nothing for either reader kind."""
        reader = (
            pl.DataFrame({"a": [1, 2]})
            if reader == "dataframe"
            else (pl.DataFrame({"a": [1]}) for _ in range(2))
        )
        df_iter = PolarsDataFrameIterator(reader, {}, ["a"])
        df_iter.close()
        assert list(df_iter) == []

    @pytest.mark.parametrize("close_error", [None, RuntimeError("close failed")])
    def test_close_during_read(self, caplog, close_error):
        """A close during a read in another thread is deferred until the read completes."""
        reading, release = threading.Event(), threading.Event()

        def chunks():
            try:
                yield pl.DataFrame({"a": [1]})
                reading.set()
                release.wait(10)
                yield pl.DataFrame({"a": [2]})
                yield pl.DataFrame({"a": [3]})
            finally:
                if close_error:
                    raise close_error

        reader = chunks()
        df_iter = PolarsDataFrameIterator(reader, {}, ["a"])
        next(df_iter)
        with ThreadPoolExecutor(1) as executor:
            future = executor.submit(next, df_iter)
            assert reading.wait(10)
            df_iter.close()
            with pytest.raises(StopIteration):
                next(df_iter)
            assert inspect.getgeneratorstate(reader) == inspect.GEN_RUNNING
            release.set()
            assert future.result(10)["a"].to_list() == [2]
        assert inspect.getgeneratorstate(reader) == inspect.GEN_CLOSED
        assert ("Failed to close the chunk reader" in caplog.text) == bool(close_error)
        assert list(df_iter) == []
