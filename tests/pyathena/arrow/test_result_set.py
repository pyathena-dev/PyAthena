# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
import os
import shutil
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from pyarrow import fs as pyarrow_fs

from pyathena.arrow.converter import DefaultArrowTypeConverter
from pyathena.arrow.result_set import AthenaArrowResultSet
from pyathena.connection import Connection
from pyathena.model import AthenaQueryExecution
from pyathena.util import RetryConfig
from tests.pyathena.util import INT96_ARROW_TYPES, INT96_UNLOAD_FILE, INT96_VALUES


def _read_unload_result(tmp_path):
    """Read the Parquet file that Athena wrote for UNLOAD through the result set, without AWS."""
    unload = tmp_path / "bucket" / "unload"
    unload.mkdir(parents=True)
    shutil.copy(INT96_UNLOAD_FILE, unload / "0.parquet")
    result_set = AthenaArrowResultSet.__new__(AthenaArrowResultSet)  # bypass __init__
    result_set._fs = pyarrow_fs.SubTreeFileSystem(str(tmp_path), pyarrow_fs.LocalFileSystem())
    result_set._unload_location = "s3://bucket/unload/"
    with patch.object(
        AthenaArrowResultSet, "_read_data_manifest", return_value=["s3://bucket/unload/0.parquet"]
    ):
        return result_set._read_parquet()


class TestAthenaArrowResultSet:
    def test_fetch_after_close(self):
        """No AWS calls; the query execution and the filesystem are mocked."""
        with patch.object(AthenaArrowResultSet, "_create_s3_file_system"):
            result_set = AthenaArrowResultSet(
                connection=MagicMock(),
                converter=DefaultArrowTypeConverter(),
                query_execution=MagicMock(state=AthenaQueryExecution.STATE_FAILED),
                arraysize=1,
                retry_config=RetryConfig(),
            )
        result_set.close()
        assert result_set.fetchone() is None
        assert result_set.fetchmany() == []
        assert result_set.fetchall() == []

    def test_fetch_across_batches(self):
        """The buffer is refilled one record batch at a time.

        No AWS calls; the first page request and the result table are mocked.
        """
        with (
            patch.object(AthenaArrowResultSet, "_pre_fetch"),
            patch.object(AthenaArrowResultSet, "_create_s3_file_system"),
            patch.object(
                AthenaArrowResultSet, "_as_arrow", return_value=pa.table({"a": [1, 2, 3]})
            ),
        ):
            result_set = AthenaArrowResultSet(
                connection=MagicMock(),
                converter=DefaultArrowTypeConverter(),
                query_execution=MagicMock(
                    state=AthenaQueryExecution.STATE_SUCCEEDED,
                    output_location="s3://bucket/query.csv",
                ),
                arraysize=1,
                retry_config=RetryConfig(),
            )
        assert result_set.fetchmany(2) == [(1,), (2,)]
        assert result_set.fetchone() == (3,)
        assert result_set.fetchmany() == []
        assert result_set.fetchall() == []
        assert result_set.fetchone() is None
        assert result_set.rownumber == 3

    @pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall", "iter"])
    @pytest.mark.parametrize(
        ("chunks", "expected"),
        [
            ([[1, 2, 3], [], [4]], [1, 2, 3, 4]),
            ([[], [1, 2]], [1, 2]),
            ([[1], [], [], [2]], [1, 2]),
        ],
        ids=["middle", "leading", "consecutive"],
    )
    def test_fetch_across_empty_batches(self, method, chunks, expected):
        """Empty record batches before or between others do not end the fetches.

        A table read from result files that include an empty one has such
        batches; the fetches used to stop at the first one. to_batches() yields
        no empty batch after the last rows. No AWS calls; the first page
        request and the result table are mocked.
        """
        table = pa.concat_tables([pa.table({"a": pa.array(chunk, pa.int64())}) for chunk in chunks])
        with (
            patch.object(AthenaArrowResultSet, "_pre_fetch"),
            patch.object(AthenaArrowResultSet, "_create_s3_file_system"),
            patch.object(AthenaArrowResultSet, "_as_arrow", return_value=table),
        ):
            result_set = AthenaArrowResultSet(
                connection=MagicMock(),
                converter=DefaultArrowTypeConverter(),
                query_execution=MagicMock(
                    state=AthenaQueryExecution.STATE_SUCCEEDED,
                    output_location="s3://bucket/query.csv",
                ),
                arraysize=2,
                retry_config=RetryConfig(),
            )
        assert 0 in [batch.num_rows for batch in table.to_batches(2)]
        if method == "fetchone":
            rows = list(iter(result_set.fetchone, None))
        elif method == "fetchmany":
            rows = result_set.fetchmany(10)
        elif method == "fetchall":
            rows = result_set.fetchall()
        else:
            rows = list(result_set)
        assert rows == [(value,) for value in expected]
        assert result_set.rownumber == len(expected)
        assert result_set.fetchone() is None

    def test_read_csv_timestamps_with_the_same_name(self):
        """Timestamp columns are converted by position, also with names that repeat.

        No AWS calls; the GetQueryResults rows are mocked.
        """
        result_set = AthenaArrowResultSet.__new__(AthenaArrowResultSet)  # bypass __init__
        result_set._query_execution = None
        result_set._converter = DefaultArrowTypeConverter()
        result_set._block_size = AthenaArrowResultSet.DEFAULT_BLOCK_SIZE
        result_set._metadata = tuple(
            {"Name": n, "Type": t, "Precision": 3, "Scale": 0, "Nullable": "UNKNOWN"}
            for n, t in (("x", "varchar"), ("x", "timestamp"), ("y", "timestamp"))
        )
        data = b'"x","x","y"\n"a","2020-01-02 03:04:05.123456789","2020-01-02 03:04:05.1"\n"b",,\n'
        with patch.object(AthenaArrowResultSet, "_fetch_all_rows_as_csv", return_value=data):
            table = result_set._read_csv()
        assert table.column_names == ["x", "x", "y"]
        assert table.schema.types == [pa.string(), pa.timestamp("us"), pa.timestamp("us")]
        rows = list(zip(*(column.to_pylist() for column in table.columns), strict=True))
        assert rows == [
            ("a", datetime(2020, 1, 2, 3, 4, 5, 123456), datetime(2020, 1, 2, 3, 4, 5, 100000)),
            ("b", None, None),
        ]

    @pytest.mark.parametrize(
        ("connection_kwargs", "expected_access_key"),
        [
            ({}, "DEFAULTKEY"),
            ({"profile_name": "static"}, "STATICKEY"),
            ({"profile_name": "process"}, "PROCESSKEY"),
            (
                {"profile_name": "process", "role_arn": "arn:aws:iam::123456789012:role/r"},
                "ROLEKEY",
            ),
        ],
        ids=["default", "static", "credential_process", "role_arn"],
    )
    def test_create_s3_file_system_uses_session_credentials(
        self, monkeypatch, tmp_path, connection_kwargs, expected_access_key
    ):
        """The pyarrow filesystem reads with the credentials of the connection's session.

        No AWS calls; the profiles come from temporary config files, and the role
        assumption is mocked.
        """
        for key in list(os.environ):
            if key.startswith("AWS_"):
                monkeypatch.delenv(key)
        process = tmp_path / "credential_process.py"
        process.write_text(
            'print(\'{"Version": 1, "AccessKeyId": "PROCESSKEY", "SecretAccessKey": "secret"}\')\n'
        )
        config = tmp_path / "config"
        config.write_text(
            "[default]\naws_access_key_id = DEFAULTKEY\naws_secret_access_key = secret\n"
            "[profile static]\naws_access_key_id = STATICKEY\naws_secret_access_key = secret\n"
            f'[profile process]\ncredential_process = "{sys.executable}" "{process}"\n'
        )
        monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
        monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "credentials"))
        role_credentials = {
            "AccessKeyId": "ROLEKEY",
            "SecretAccessKey": "secret",
            "SessionToken": "token",
        }
        with patch.object(Connection, "_assume_role", return_value=role_credentials):
            connection = Connection(
                region_name="us-east-1", s3_staging_dir="s3://bucket/path/", **connection_kwargs
            )
        with connection:
            result_set = AthenaArrowResultSet.__new__(AthenaArrowResultSet)  # bypass __init__
            result_set._connection = connection
            result_set._connect_timeout = None
            result_set._request_timeout = None
            fs = result_set._create_s3_file_system()

        # pyarrow exposes the filesystem options only through its pickle support.
        options = fs.__reduce__()[1][0]
        assert options["access_key"] == expected_access_key
        assert options["role_arn"] == ""

    def test_open_csv_closes_file_when_stream_fails(self):
        """The input file is closed if the read-ahead stream cannot be created."""
        result_set = AthenaArrowResultSet.__new__(AthenaArrowResultSet)  # bypass __init__
        result_set._fs = MagicMock()
        file = result_set._fs.open_input_file.return_value
        file.size.side_effect = OSError("HEAD failed")
        with pytest.raises(OSError, match="HEAD failed"):
            result_set._open_csv("bucket/key.csv")
        file.close.assert_called_once_with()

    def test_read_parquet_int96(self, tmp_path):
        """INT96 timestamps that Athena writes are read at microseconds at any level."""
        table = _read_unload_result(tmp_path).sort_by("id")
        assert table.schema.types[1:] == INT96_ARROW_TYPES
        assert table.drop_columns("id").to_pydict() == {
            "ts": INT96_VALUES,
            "arr": [[v] for v in INT96_VALUES],
            "st": [{"t": v} for v in INT96_VALUES],
            "mp": [[("k", v)] for v in INT96_VALUES],
        }
