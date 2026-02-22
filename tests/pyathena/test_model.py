import copy
from datetime import datetime

from pyathena.model import (
    AthenaCalculationExecution,
    AthenaCalculationExecutionStatus,
    AthenaCompression,
    AthenaFileFormat,
    AthenaPartitionTransform,
    AthenaQueryExecution,
    AthenaRowFormatSerde,
    AthenaSessionStatus,
    AthenaTableMetadata,
)


class TestAthenaQueryExecution:
    def test_init(self):
        actual = AthenaQueryExecution(
            {
                "QueryExecution": {
                    "QueryExecutionId": "12345678-90ab-cdef-1234-567890abcdef",
                    "Query": "SELECT * FROM test_table",
                    "StatementType": "DML",
                    "ResultConfiguration": {
                        "OutputLocation": "s3://bucket/path/to/output/",
                        "EncryptionConfiguration": {
                            "EncryptionOption": "test_encryption_option",
                            "KmsKey": "test_kms_key",
                        },
                        "ExpectedBucketOwner": "test-bucket-owner",
                        "AclConfiguration": {"S3AclOption": "BUCKET_OWNER_FULL_CONTROL"},
                    },
                    "ResultReuseConfiguration": {
                        "ResultReuseByAgeConfiguration": {"Enabled": True, "MaxAgeInMinutes": 5}
                    },
                    "QueryExecutionContext": {
                        "Database": "test_database",
                        "Catalog": "test_catalog",
                    },
                    "Status": {
                        "State": "SUCCEEDED",
                        "StateChangeReason": "test_reason",
                        "SubmissionDateTime": datetime(2019, 1, 2, 3, 4, 5),
                        "CompletionDateTime": datetime(2019, 9, 8, 7, 6, 5),
                        "AthenaError": {
                            "ErrorCategory": 2,
                            "ErrorType": 1001,
                            "Retryable": True,
                            "ErrorMessage": "test_error_message",
                        },
                    },
                    "Statistics": {
                        "EngineExecutionTimeInMillis": 234567890,
                        "DataScannedInBytes": 1234567890,
                        "DataManifestLocation": "s3://bucket/path/to/data_manifest/",
                        "TotalExecutionTimeInMillis": 4567890,
                        "QueryQueueTimeInMillis": 34567890,
                        "QueryPlanningTimeInMillis": 567890,
                        "ServiceProcessingTimeInMillis": 67890,
                        "ResultReuseInformation": {"ReusedPreviousResult": True},
                    },
                    "WorkGroup": "test_work_group",
                    "EngineVersion": {
                        "SelectedEngineVersion": "Athena engine version 2",
                        "EffectiveEngineVersion": "Athena engine version 2",
                    },
                    "ExecutionParameters": [
                        "param1",
                        "param2",
                    ],
                }
            }
        )
        assert actual.database == "test_database"
        assert actual.catalog == "test_catalog"
        assert actual.query_id, "12345678-90ab-cdef-1234-567890abcdef"
        assert actual.query == "SELECT * FROM test_table"
        assert actual.statement_type == "DML"
        assert actual.work_group == "test_work_group"
        assert actual.execution_parameters == ["param1", "param2"]
        assert actual.state == "SUCCEEDED"
        assert actual.state_change_reason == "test_reason"
        assert actual.submission_date_time == datetime(2019, 1, 2, 3, 4, 5)
        assert actual.completion_date_time == datetime(2019, 9, 8, 7, 6, 5)
        assert actual.error_category == 2
        assert actual.error_type == 1001
        assert actual.retryable
        assert actual.error_message == "test_error_message"
        assert actual.data_scanned_in_bytes == 1234567890
        assert actual.engine_execution_time_in_millis == 234567890
        assert actual.query_queue_time_in_millis == 34567890
        assert actual.total_execution_time_in_millis == 4567890
        assert actual.query_planning_time_in_millis == 567890
        assert actual.service_processing_time_in_millis == 67890
        assert actual.output_location == "s3://bucket/path/to/output/"
        assert actual.data_manifest_location == "s3://bucket/path/to/data_manifest/"
        assert actual.reused_previous_result
        assert actual.encryption_option == "test_encryption_option"
        assert actual.kms_key == "test_kms_key"
        assert actual.expected_bucket_owner == "test-bucket-owner"
        assert actual.s3_acl_option == AthenaQueryExecution.S3_ACL_OPTION_BUCKET_OWNER_FULL_CONTROL
        assert actual.selected_engine_version == "Athena engine version 2"
        assert actual.effective_engine_version == "Athena engine version 2"
        assert actual.result_reuse_enabled
        assert actual.result_reuse_minutes == 5


class TestAthenaCalculationExecution:
    def test_init(self):
        actual = AthenaCalculationExecution(
            {
                "CalculationExecutionId": "calculation_id",
                "SessionId": "session_id",
                "Description": "description",
                "WorkingDirectory": "working_directory",
                "Status": {
                    "SubmissionDateTime": datetime(2015, 1, 1, 1, 1, 1),
                    "CompletionDateTime": datetime(2016, 1, 1, 1, 1, 1),
                    "State": "COMPLETED",
                    "StateChangeReason": "reason",
                },
                "Statistics": {"DpuExecutionInMillis": 123, "Progress": "progress"},
                "Result": {
                    "StdOutS3Uri": "s3://bucket/path/to/stdout",
                    "StdErrorS3Uri": "s3://bucket/path/to/stderror",
                    "ResultS3Uri": "s3://bucket/path/to/result",
                    "ResultType": "type",
                },
            }
        )
        assert actual.calculation_id == "calculation_id"
        assert actual.session_id == "session_id"
        assert actual.description == "description"
        assert actual.working_directory == "working_directory"
        assert actual.submission_date_time == datetime(2015, 1, 1, 1, 1, 1)
        assert actual.completion_date_time == datetime(2016, 1, 1, 1, 1, 1)
        assert actual.state == AthenaCalculationExecutionStatus.STATE_COMPLETED
        assert actual.state_change_reason == "reason"
        assert actual.dpu_execution_in_millis == 123
        assert actual.progress == "progress"
        assert actual.std_out_s3_uri == "s3://bucket/path/to/stdout"
        assert actual.std_error_s3_uri == "s3://bucket/path/to/stderror"
        assert actual.result_s3_uri == "s3://bucket/path/to/result"
        assert actual.result_type == "type"


class TestAthenaSessionStatus:
    def test_init(self):
        actual = AthenaSessionStatus(
            {
                "SessionId": "session_id",
                "Status": {
                    "StartDateTime": datetime(2015, 1, 1, 1, 1, 1),
                    "LastModifiedDateTime": datetime(2016, 1, 1, 1, 1, 1),
                    "EndDateTime": datetime(2017, 1, 1, 1, 1, 1),
                    "IdleSinceDateTime": datetime(2018, 1, 1, 1, 1, 1),
                    "State": "IDLE",
                    "StateChangeReason": "reason",
                },
            }
        )
        assert actual.session_id == "session_id"
        assert actual.state == AthenaSessionStatus.STATE_IDLE
        assert actual.state_change_reason == "reason"
        assert actual.start_date_time == datetime(2015, 1, 1, 1, 1, 1)
        assert actual.last_modified_date_time == datetime(2016, 1, 1, 1, 1, 1)
        assert actual.end_date_time == datetime(2017, 1, 1, 1, 1, 1)
        assert actual.idle_since_date_time == datetime(2018, 1, 1, 1, 1, 1)


class TestAthenaTableMetadata:
    def _create_table_metadata(self):
        return {
            "TableMetadata": {
                "Name": "test_name",
                "CreateTime": datetime(2015, 1, 2, 3, 4, 5),
                "LastAccessTime": datetime(2015, 9, 8, 7, 6, 5),
                "TableType": "test_table_type",
                "Columns": [
                    {"Name": "test_name_1", "Type": "test_type_1", "Comment": "test_comment_1"},
                    {"Name": "test_name_2", "Type": "test_type_2", "Comment": "test_comment_2"},
                ],
                "PartitionKeys": [
                    {"Name": "test_name_1", "Type": "test_type_1", "Comment": "test_comment_1"},
                    {"Name": "test_name_2", "Type": "test_type_2", "Comment": "test_comment_2"},
                ],
                "Parameters": {"comment": "test_comment"},
            }
        }

    def _create_table_metadata_parameters_text(self):
        return {
            "EXTERNAL": "TRUE",
            "has_encrypted_data": "false",
            "inputformat": "org.apache.hadoop.mapred.TextInputFormat",
            "location": "s3://bucket/path/to",
            "outputformat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "presto_query_id": "20220101_123456_98765_abcde",
            "serde.serialization.lib": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            "write.compression": "SNAPPY",
        }

    def _create_table_metadata_parameters_json(self):
        return {
            "EXTERNAL": "TRUE",
            "inputformat": "org.apache.hadoop.mapred.TextInputFormat",
            "location": "s3://bucket/path/to",
            "outputformat": "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat",
            "serde.param.serialization.format": "1",
            "serde.param.write.compression": "GZIP",
            "serde.serialization.lib": "org.openx.data.jsonserde.JsonSerDe",
            "transient_lastDdlTime": "1234567890",
        }

    def _create_table_metadata_parameters_json_hcatalog(self):
        return {
            "EXTERNAL": "TRUE",
            "has_encrypted_data": "false",
            "inputformat": "org.apache.hadoop.mapred.TextInputFormat",
            "location": "s3://bucket/path/to",
            "outputformat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "presto_query_id": "20220101_123456_98765_abcde",
            "serde.serialization.lib": "org.apache.hive.hcatalog.data.JsonSerDe",
            "write.compression": "SNAPPY",
        }

    def _create_table_metadata_parameters_parquet(self):
        return {
            "EXTERNAL": "TRUE",
            "inputformat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "location": "s3://bucket/path/to",
            "outputformat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "parquet.compress": "SNAPPY",
            "serde.param.serialization.format": "1",
            "serde.serialization.lib": (
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
            ),
            "transient_lastDdlTime": "1234567890",
        }

    def _create_table_metadata_parameters_orc(self):
        return {
            "EXTERNAL": "TRUE",
            "has_encrypted_data": "false",
            "inputformat": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
            "location": "s3://bucket/path/to",
            "orc.compress": "SNAPPY",
            "outputformat": "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
            "presto_query_id": "20220101_123456_98765_abcde",
            "serde.serialization.lib": "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
        }

    def _create_table_metadata_parameters_avro(self):
        return {
            "EXTERNAL": "TRUE",
            "has_encrypted_data": "false",
            "inputformat": "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
            "location": "s3://bucket/path/to",
            "outputformat": "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
            "presto_query_id": "20220101_123456_98765_abcde",
            "serde.serialization.lib": "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
        }

    def test_init(self):
        actual = AthenaTableMetadata(self._create_table_metadata())
        assert actual.name == "test_name"
        assert actual.create_time == datetime(2015, 1, 2, 3, 4, 5)
        assert actual.last_access_time == datetime(2015, 9, 8, 7, 6, 5)
        assert actual.table_type == "test_table_type"

        columns = actual.columns
        assert len(columns) == 2
        assert columns[0].name == "test_name_1"
        assert columns[0].type == "test_type_1"
        assert columns[0].comment == "test_comment_1"
        assert columns[1].name == "test_name_2"
        assert columns[1].type == "test_type_2"
        assert columns[1].comment == "test_comment_2"

        partition_keys = actual.partition_keys
        assert len(partition_keys) == 2
        assert partition_keys[0].name == "test_name_1"
        assert partition_keys[0].type == "test_type_1"
        assert partition_keys[0].comment == "test_comment_1"
        assert partition_keys[1].name == "test_name_2"
        assert partition_keys[1].type == "test_type_2"
        assert partition_keys[1].comment == "test_comment_2"

        assert actual.parameters == {"comment": "test_comment"}
        assert actual.comment == "test_comment"
        assert actual.location is None
        assert actual.input_format is None
        assert actual.output_format is None
        assert actual.serde_serialization_lib is None
        assert actual.compression is None

    def test_init_text(self):
        response = copy.deepcopy(self._create_table_metadata())
        parameters = copy.deepcopy(self._create_table_metadata_parameters_text())
        response["TableMetadata"]["Parameters"] = parameters
        actual = AthenaTableMetadata(response)
        assert actual.parameters == parameters
        assert actual.comment is None
        assert actual.location == "s3://bucket/path/to"
        assert actual.input_format == "org.apache.hadoop.mapred.TextInputFormat"
        assert actual.output_format == "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
        assert (
            actual.serde_serialization_lib == "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
        )
        assert actual.compression == "SNAPPY"
        assert actual.row_format == "SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"
        assert (
            actual.file_format == "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"
        )

    def test_init_json(self):
        response = copy.deepcopy(self._create_table_metadata())
        parameters = copy.deepcopy(self._create_table_metadata_parameters_json())
        response["TableMetadata"]["Parameters"] = parameters
        actual = AthenaTableMetadata(response)
        assert actual.parameters == parameters
        assert actual.comment is None
        assert actual.location == "s3://bucket/path/to"
        assert actual.input_format == "org.apache.hadoop.mapred.TextInputFormat"
        assert actual.output_format == "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat"
        assert actual.serde_serialization_lib == "org.openx.data.jsonserde.JsonSerDe"
        assert actual.compression == "GZIP"
        assert actual.serde_properties == {
            "serialization.format": "1",
            "write.compression": "GZIP",
        }
        for key in actual.table_properties:
            assert not key.startswith("serde.param.")
        assert actual.row_format == "SERDE 'org.openx.data.jsonserde.JsonSerDe'"
        assert (
            actual.file_format == "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'"
        )

    def test_init_json_hcatalog(self):
        response = copy.deepcopy(self._create_table_metadata())
        parameters = copy.deepcopy(self._create_table_metadata_parameters_json_hcatalog())
        response["TableMetadata"]["Parameters"] = parameters
        actual = AthenaTableMetadata(response)
        assert actual.parameters == parameters
        assert actual.comment is None
        assert actual.location == "s3://bucket/path/to"
        assert actual.input_format == "org.apache.hadoop.mapred.TextInputFormat"
        assert actual.output_format == "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
        assert actual.serde_serialization_lib == "org.apache.hive.hcatalog.data.JsonSerDe"
        assert actual.compression == "SNAPPY"
        assert not actual.serde_properties
        for key in actual.table_properties:
            assert not key.startswith("serde.param.")
        assert actual.row_format == "SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'"
        assert (
            actual.file_format == "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"
        )

    def test_init_parquet(self):
        response = copy.deepcopy(self._create_table_metadata())
        parameters = copy.deepcopy(self._create_table_metadata_parameters_parquet())
        response["TableMetadata"]["Parameters"] = parameters
        actual = AthenaTableMetadata(response)
        assert actual.parameters == parameters
        assert actual.comment is None
        assert actual.location == "s3://bucket/path/to"
        assert (
            actual.input_format == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
        )
        assert (
            actual.output_format == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
        )
        assert (
            actual.serde_serialization_lib
            == "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        )
        assert actual.compression == "SNAPPY"
        assert actual.serde_properties == {
            "serialization.format": "1",
        }
        for key in actual.table_properties:
            assert not key.startswith("serde.param.")
        assert (
            actual.row_format
            == "SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
        )
        assert (
            actual.file_format
            == "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
        )

    def test_init_orc(self):
        response = copy.deepcopy(self._create_table_metadata())
        parameters = copy.deepcopy(self._create_table_metadata_parameters_orc())
        response["TableMetadata"]["Parameters"] = parameters
        actual = AthenaTableMetadata(response)
        assert actual.parameters == parameters
        assert actual.comment is None
        assert actual.location == "s3://bucket/path/to"
        assert actual.input_format == "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
        assert actual.output_format == "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"
        assert actual.serde_serialization_lib == "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
        assert actual.compression == "SNAPPY"
        assert not actual.serde_properties
        for key in actual.table_properties:
            assert not key.startswith("serde.param.")
        assert actual.row_format == "SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'"
        assert (
            actual.file_format == "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'"
        )

    def test_init_avro(self):
        response = copy.deepcopy(self._create_table_metadata())
        parameters = copy.deepcopy(self._create_table_metadata_parameters_avro())
        response["TableMetadata"]["Parameters"] = parameters
        actual = AthenaTableMetadata(response)
        assert actual.parameters == parameters
        assert actual.comment is None
        assert actual.location == "s3://bucket/path/to"
        assert actual.input_format == "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat"
        assert actual.output_format == "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"
        assert actual.serde_serialization_lib == "org.apache.hadoop.hive.serde2.avro.AvroSerDe"
        assert actual.compression is None
        assert not actual.serde_properties
        for key in actual.table_properties:
            assert not key.startswith("serde.param.")
        assert actual.row_format == "SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'"
        assert (
            actual.file_format
            == "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'"
        )


class TestAthenaFileFormat:
    def test_is_parquet(self):
        assert AthenaFileFormat.is_parquet("parquet")
        assert AthenaFileFormat.is_parquet("PARQUET")
        assert not AthenaFileFormat.is_parquet("")
        assert not AthenaFileFormat.is_parquet("foobar")

    def test_is_orc(self):
        assert AthenaFileFormat.is_orc("orc")
        assert AthenaFileFormat.is_orc("ORC")
        assert not AthenaFileFormat.is_orc("")
        assert not AthenaFileFormat.is_orc("foobar")


class TestAthenaRowFormatSerde:
    def test_is_parquet(self):
        assert AthenaRowFormatSerde.is_parquet(
            "SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
        )
        assert AthenaRowFormatSerde.is_parquet(
            "SerDe 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
        )
        assert AthenaRowFormatSerde.is_parquet(
            "Serde 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
        )
        assert AthenaRowFormatSerde.is_parquet(
            "serde 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
        )
        assert not AthenaRowFormatSerde.is_parquet("")
        assert not AthenaRowFormatSerde.is_parquet(
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        )
        assert not AthenaRowFormatSerde.is_parquet("foobar")

    def test_is_orc(self):
        assert AthenaRowFormatSerde.is_orc("SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'")
        assert AthenaRowFormatSerde.is_orc("SerDe 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'")
        assert AthenaRowFormatSerde.is_orc("Serde 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'")
        assert AthenaRowFormatSerde.is_orc("serde 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'")
        assert not AthenaRowFormatSerde.is_orc("")
        assert not AthenaRowFormatSerde.is_orc("org.apache.hadoop.hive.ql.io.orc.OrcSerde")
        assert not AthenaRowFormatSerde.is_orc("foobar")


class TestAthenaCompression:
    def test_is_valid(self):
        assert AthenaCompression.is_valid("snappy")
        assert AthenaCompression.is_valid("SNAPPY")
        assert not AthenaCompression.is_valid("")
        assert not AthenaCompression.is_valid("foobar")


class TestAthenaPartitionTransform:
    def test_is_valid(self):
        assert AthenaPartitionTransform.is_valid("year")
        assert AthenaPartitionTransform.is_valid("YEAR")
        assert AthenaPartitionTransform.is_valid("month")
        assert AthenaPartitionTransform.is_valid("MONTH")
        assert AthenaPartitionTransform.is_valid("day")
        assert AthenaPartitionTransform.is_valid("DAY")
        assert AthenaPartitionTransform.is_valid("hour")
        assert AthenaPartitionTransform.is_valid("HOUR")
        assert AthenaPartitionTransform.is_valid("bucket")
        assert AthenaPartitionTransform.is_valid("BUCKET")
        assert AthenaPartitionTransform.is_valid("truncate")
        assert AthenaPartitionTransform.is_valid("TRUNCATE")
        assert not AthenaPartitionTransform.is_valid("")
        assert not AthenaPartitionTransform.is_valid("foobar")
