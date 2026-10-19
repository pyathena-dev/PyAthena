# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
import os
import sys
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from pyathena.arrow.converter import DefaultArrowTypeConverter
from pyathena.arrow.result_set import AthenaArrowResultSet
from pyathena.connection import Connection
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.util import RetryConfig


class TestAthenaArrowResultSet:
    def test_fetch_after_close(self):
        """GH-1207: the fetches and table methods of a closed result set raise.

        No AWS calls; the metadata read and the result table are mocked.
        """
        with (
            patch.object(AthenaArrowResultSet, "_read_metadata"),
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
        assert result_set.fetchone() == (1,)
        result_set.close()
        for fetch in (
            result_set.fetchone,
            result_set.fetchmany,
            result_set.fetchall,
            result_set.as_arrow,
            result_set.as_polars,
        ):
            with pytest.raises(ProgrammingError):
                fetch()

    def test_csv_file_closed_when_stream_fails(self):
        """The CSV result file is closed when reading it cannot start, as when HEAD fails.

        No AWS calls; the Athena and S3 clients and pyarrow's S3 filesystem are mocked.
        """
        connection = MagicMock()
        connection.client.get_query_results.return_value = {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [
                        {
                            "Name": "a",
                            "Type": "integer",
                            "Precision": 10,
                            "Scale": 0,
                            "Nullable": "UNKNOWN",
                        }
                    ]
                },
                "Rows": [],
            }
        }
        connection.s3_client.head_object.return_value = {"ContentLength": 4}
        with patch("pyarrow.fs.S3FileSystem") as file_system:
            file = file_system.return_value.open_input_file.return_value
            file.size.side_effect = OSError("HEAD failed")
            with pytest.raises(OperationalError):
                AthenaArrowResultSet(
                    connection=connection,
                    converter=DefaultArrowTypeConverter(),
                    query_execution=MagicMock(
                        state=AthenaQueryExecution.STATE_SUCCEEDED,
                        output_location="s3://bucket/query.csv",
                        substatement_type="SELECT",
                    ),
                    arraysize=1,
                    retry_config=RetryConfig(),
                )
        assert file.close.called

    @pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall", "iter", "mixed"])
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
        no empty batch after the last rows. The fetch methods can be mixed, and
        return nothing after the last row. No AWS calls; the metadata read
        and the result table are mocked.
        """
        table = pa.concat_tables([pa.table({"a": pa.array(chunk, pa.int64())}) for chunk in chunks])
        with (
            patch.object(AthenaArrowResultSet, "_read_metadata"),
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
        elif method == "iter":
            rows = list(result_set)
        else:
            rows = result_set.fetchmany(2)
            row = result_set.fetchone()
            assert row == ((expected[2],) if len(expected) > 2 else None)
            if row is not None:
                rows.append(row)
            rows += result_set.fetchall()
        assert rows == [(value,) for value in expected]
        assert result_set.rownumber == len(expected)
        assert result_set.fetchmany() == []
        assert result_set.fetchall() == []
        assert result_set.fetchone() is None

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
