# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from io import BytesIO
from unittest.mock import MagicMock, patch

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from fsspec.implementations.memory import MemoryFileSystem

from pyathena.arrow.converter import DefaultArrowTypeConverter
from pyathena.arrow.result_set import AthenaArrowResultSet
from pyathena.connection import Connection
from pyathena.filesystem.s3_executor import S3ThreadPoolExecutor
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet
from pyathena.util import RetryConfig


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

    @pytest.mark.parametrize("unload", [False, True])
    def test_s3_workers_read_files(self, unload):
        """Read real CSV/Parquet bytes through the S3 file and Arrow adapter offline."""
        if unload:
            output = pa.BufferOutputStream()
            pq.write_table(pa.table({"value": [1]}), output)
            data = output.getvalue().to_pybytes()
            key = "unload/part.parquet"
        else:
            data = b'"value"\n1\n'
            key = "result.csv"
        model_client = boto3.client(
            "s3", region_name="us-east-1", aws_access_key_id="dummy", aws_secret_access_key="dummy"
        )
        client = MagicMock()
        client.meta.service_model = model_client.meta.service_model
        client.meta.method_to_api_mapping = model_client.meta.method_to_api_mapping
        model_client.close()

        def head_object(**kwargs):
            if kwargs["Key"] != key:
                raise ClientError(
                    {"Error": {"Code": "404"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
                    "HeadObject",
                )
            return {"ContentLength": len(data), "LastModified": datetime(2026, 1, 1, tzinfo=UTC)}

        def get_object(**kwargs):
            start, end = map(int, kwargs["Range"].removeprefix("bytes=").split("-"))
            body = data[start : end + 1]
            return {"Body": StreamingBody(BytesIO(body), len(body))}

        client.head_object.side_effect = head_object
        client.get_object.side_effect = get_object
        client.list_objects_v2.return_value = {
            "Contents": [{"Key": key, "Size": len(data)}],
            "IsTruncated": False,
        }
        connection = MagicMock(s3_client=client, retry_config=RetryConfig(attempt=1))
        execution = MagicMock(
            state=AthenaQueryExecution.STATE_FAILED,
            output_location=f"s3://bucket/{key}",
            substatement_type=None,
        )
        result = AthenaArrowResultSet(
            connection=connection,
            converter=DefaultArrowTypeConverter(),
            query_execution=execution,
            arraysize=1,
            retry_config=RetryConfig(),
            s3_max_workers=2,
            unload=unload,
            unload_location="s3://bucket/unload/",
        )
        execution.state = AthenaQueryExecution.STATE_SUCCEEDED
        result._metadata = (
            {"Name": "value", "Type": "integer", "Precision": 0, "Scale": 0, "Nullable": "UNKNOWN"},
        )
        with (
            patch.object(result, "_read_data_manifest", return_value=[f"s3://bucket/{key}"]),
            patch(
                "pyathena.filesystem.s3.S3ThreadPoolExecutor", wraps=S3ThreadPoolExecutor
            ) as executor,
        ):
            table = result._as_arrow()
        assert table.to_pylist() == [{"value": 1}]
        assert executor.call_count > 0
        assert all(call.kwargs["max_workers"] == 2 for call in executor.call_args_list)
        assert result._fs.handler.fs.max_workers == 2
        client.get_object.assert_called()
        client.close.assert_not_called()

    @pytest.mark.parametrize("failure", [None, "filesystem", "read"])
    def test_s3_workers_owned_client_cleanup(self, failure):
        """Dedicated timeout clients close after eager reads, including failures."""
        connection = MagicMock(region_name="us-east-1")
        connection.s3_config = Config(max_pool_connections=17, read_timeout=60)
        connection._s3_client_kwargs = {"use_ssl": True}
        owned_client = connection.session.client.return_value
        filesystem = MemoryFileSystem(skip_instance_cache=True)
        execution = MagicMock(
            state=AthenaQueryExecution.STATE_SUCCEEDED, output_location="s3://bucket/result.csv"
        )
        with (
            patch.object(AthenaResultSet, "_pre_fetch"),
            patch("pyathena.arrow.result_set.S3FileSystem", return_value=filesystem) as factory,
            patch.object(
                AthenaArrowResultSet, "_as_arrow", return_value=pa.table({"value": [1]})
            ) as read,
        ):
            if failure == "filesystem":
                factory.side_effect = RuntimeError("filesystem failed")
            elif failure == "read":
                read.side_effect = RuntimeError("read failed")
            kwargs = {
                "connection": connection,
                "converter": DefaultArrowTypeConverter(),
                "query_execution": execution,
                "arraysize": 1,
                "retry_config": RetryConfig(),
                "s3_max_workers": 2,
                "connect_timeout": 1.5,
                "request_timeout": 4.5,
            }
            if failure:
                with pytest.raises(RuntimeError, match="failed"):
                    AthenaArrowResultSet(**kwargs)
            else:
                result = AthenaArrowResultSet(**kwargs)
                assert result.as_arrow().to_pylist() == [{"value": 1}]
        config = connection.session.client.call_args.kwargs["config"]
        assert config.connect_timeout == 1.5
        assert config.read_timeout == 4.5
        assert config.max_pool_connections == 17
        assert connection.s3_config.read_timeout == 60
        assert connection.session.client.call_args.kwargs["use_ssl"] is True
        assert factory.call_args.kwargs["s3_client"] is owned_client
        owned_client.close.assert_called_once_with()
        connection.s3_client.close.assert_not_called()

    def test_s3_workers_native_default(self):
        """The default still builds the native Arrow filesystem, without a boto3 client."""
        connection = MagicMock(region_name="us-east-1", profile_name=None)
        connection._kwargs = {"aws_access_key_id": "dummy", "aws_secret_access_key": "dummy"}
        with patch("pyarrow.fs.S3FileSystem") as native:
            AthenaArrowResultSet(
                connection=connection,
                converter=DefaultArrowTypeConverter(),
                query_execution=MagicMock(state=AthenaQueryExecution.STATE_FAILED),
                arraysize=1,
                retry_config=RetryConfig(),
            )
        native.assert_called_once_with(
            access_key="dummy", secret_key="dummy", session_token=None, region="us-east-1"
        )
        connection.session.client.assert_not_called()

    def test_s3_workers_serializes_client_creation(self):
        """Dedicated and shared S3 clients use the same creation lock."""
        session = MagicMock()
        connection = Connection(
            session=session, region_name="us-east-1", s3_staging_dir="s3://bucket/path/"
        )
        created = []

        def create_client(*args, **kwargs):
            assert args == ("s3",)
            assert connection._s3_client_lock.locked()
            client = MagicMock()
            created.append(client)
            return client

        session.client.side_effect = create_client
        barrier = threading.Barrier(4)

        def create_result(index):
            barrier.wait(timeout=5)
            if index == 0:
                return connection.s3_client
            return AthenaArrowResultSet(
                connection=connection,
                converter=DefaultArrowTypeConverter(),
                query_execution=MagicMock(state=AthenaQueryExecution.STATE_FAILED),
                arraysize=1,
                retry_config=RetryConfig(),
                s3_max_workers=2,
                request_timeout=4.5,
            )

        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(create_result, range(4)))
        assert len(created) == 4
        shared_client = results[0]
        for client in created:
            if client is shared_client:
                client.close.assert_not_called()
            else:
                client.close.assert_called_once_with()
        assert not connection._s3_client_lock.locked()
