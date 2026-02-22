from __future__ import annotations

import logging
import re
from datetime import datetime
from re import Pattern
from typing import Any

from pyathena.error import DataError

_logger = logging.getLogger(__name__)


class AthenaQueryExecution:
    """Represents an Athena query execution with status and metadata.

    This class encapsulates information about a query execution in Amazon Athena,
    including its current state, statistics, error information, and result metadata.
    It's primarily used internally by PyAthena cursors but can be useful for
    monitoring and debugging query execution.

    Query States:
        - QUEUED: Query is waiting to be executed
        - RUNNING: Query is currently executing
        - SUCCEEDED: Query completed successfully
        - FAILED: Query execution failed
        - CANCELLED: Query was cancelled

    Statement Types:
        - DDL: Data Definition Language (CREATE, DROP, ALTER)
        - DML: Data Manipulation Language (SELECT, INSERT, UPDATE, DELETE)
        - UTILITY: Utility statements (SHOW, DESCRIBE, EXPLAIN)

    Example:
        >>> # Typically accessed through cursor execution
        >>> cursor.execute("SELECT COUNT(*) FROM my_table")
        >>> query_execution = cursor._last_query_execution  # Internal access
        >>> print(f"Query ID: {query_execution.query_id}")
        >>> print(f"State: {query_execution.state}")
        >>> print(f"Data scanned: {query_execution.data_scanned_in_bytes} bytes")

    See Also:
        AWS Athena QueryExecution API reference:
        https://docs.aws.amazon.com/athena/latest/APIReference/API_QueryExecution.html
    """

    STATE_QUEUED: str = "QUEUED"
    STATE_RUNNING: str = "RUNNING"
    STATE_SUCCEEDED: str = "SUCCEEDED"
    STATE_FAILED: str = "FAILED"
    STATE_CANCELLED: str = "CANCELLED"

    STATEMENT_TYPE_DDL: str = "DDL"
    STATEMENT_TYPE_DML: str = "DML"
    STATEMENT_TYPE_UTILITY: str = "UTILITY"

    ENCRYPTION_OPTION_SSE_S3: str = "SSE_S3"
    ENCRYPTION_OPTION_SSE_KMS: str = "SSE_KMS"
    ENCRYPTION_OPTION_CSE_KMS: str = "CSE_KMS"

    ERROR_CATEGORY_SYSTEM: int = 1
    ERROR_CATEGORY_USER: int = 2
    ERROR_CATEGORY_OTHER: int = 3

    S3_ACL_OPTION_BUCKET_OWNER_FULL_CONTROL = "BUCKET_OWNER_FULL_CONTROL"

    def __init__(self, response: dict[str, Any]) -> None:
        query_execution = response.get("QueryExecution")
        if not query_execution:
            raise DataError("KeyError `QueryExecution`")

        query_execution_context = query_execution.get("QueryExecutionContext", {})
        self._database: str | None = query_execution_context.get("Database")
        self._catalog: str | None = query_execution_context.get("Catalog")

        self._query_id: str | None = query_execution.get("QueryExecutionId")
        if not self._query_id:
            raise DataError("KeyError `QueryExecutionId`")
        self._query: str | None = query_execution.get("Query")
        if not self._query:
            raise DataError("KeyError `Query`")
        self._statement_type: str | None = query_execution.get("StatementType")
        self._substatement_type: str | None = query_execution.get("SubstatementType")
        self._work_group: str | None = query_execution.get("WorkGroup")
        self._execution_parameters: list[str] = query_execution.get("ExecutionParameters", [])

        status = query_execution.get("Status")
        if not status:
            raise DataError("KeyError `Status`")
        self._state: str | None = status.get("State")
        self._state_change_reason: str | None = status.get("StateChangeReason")
        self._submission_date_time: datetime | None = status.get("SubmissionDateTime")
        self._completion_date_time: datetime | None = status.get("CompletionDateTime")
        athena_error = status.get("AthenaError", {})
        self._error_category: int | None = athena_error.get("ErrorCategory")
        self._error_type: int | None = athena_error.get("ErrorType")
        self._retryable: bool | None = athena_error.get("Retryable")
        self._error_message: str | None = athena_error.get("ErrorMessage")

        statistics = query_execution.get("Statistics", {})
        self._data_scanned_in_bytes: int | None = statistics.get("DataScannedInBytes")
        self._engine_execution_time_in_millis: int | None = statistics.get(
            "EngineExecutionTimeInMillis", None
        )
        self._query_queue_time_in_millis: int | None = statistics.get(
            "QueryQueueTimeInMillis", None
        )
        self._total_execution_time_in_millis: int | None = statistics.get(
            "TotalExecutionTimeInMillis", None
        )
        self._query_planning_time_in_millis: int | None = statistics.get(
            "QueryPlanningTimeInMillis", None
        )
        self._service_processing_time_in_millis: int | None = statistics.get(
            "ServiceProcessingTimeInMillis", None
        )
        self._data_manifest_location: str | None = statistics.get("DataManifestLocation")
        reuse_info = statistics.get("ResultReuseInformation", {})
        self._reused_previous_result: bool | None = reuse_info.get("ReusedPreviousResult")

        result_conf = query_execution.get("ResultConfiguration", {})
        self._output_location: str | None = result_conf.get("OutputLocation")
        encryption_conf = result_conf.get("EncryptionConfiguration", {})
        self._encryption_option: str | None = encryption_conf.get("EncryptionOption")
        self._kms_key: str | None = encryption_conf.get("KmsKey")
        self._expected_bucket_owner: str | None = result_conf.get("ExpectedBucketOwner")
        acl_conf = result_conf.get("AclConfiguration", {})
        self._s3_acl_option: str | None = acl_conf.get("S3AclOption")

        engine_version = query_execution.get("EngineVersion", {})
        self._selected_engine_version: str | None = engine_version.get(
            "SelectedEngineVersion", None
        )
        self._effective_engine_version: str | None = engine_version.get(
            "EffectiveEngineVersion", None
        )

        reuse_conf = query_execution.get("ResultReuseConfiguration", {})
        reuse_age_conf = reuse_conf.get("ResultReuseByAgeConfiguration", {})
        self._result_reuse_enabled: bool | None = reuse_age_conf.get("Enabled")
        self._result_reuse_minutes: int | None = reuse_age_conf.get("MaxAgeInMinutes")

    @property
    def database(self) -> str | None:
        return self._database

    @property
    def catalog(self) -> str | None:
        return self._catalog

    @property
    def query_id(self) -> str | None:
        return self._query_id

    @property
    def query(self) -> str | None:
        return self._query

    @property
    def statement_type(self) -> str | None:
        return self._statement_type

    @property
    def substatement_type(self) -> str | None:
        return self._substatement_type

    @property
    def work_group(self) -> str | None:
        return self._work_group

    @property
    def execution_parameters(self) -> list[str]:
        return self._execution_parameters

    @property
    def state(self) -> str | None:
        return self._state

    @property
    def state_change_reason(self) -> str | None:
        return self._state_change_reason

    @property
    def submission_date_time(self) -> datetime | None:
        return self._submission_date_time

    @property
    def completion_date_time(self) -> datetime | None:
        return self._completion_date_time

    @property
    def error_category(self) -> int | None:
        return self._error_category

    @property
    def error_type(self) -> int | None:
        return self._error_type

    @property
    def retryable(self) -> bool | None:
        return self._retryable

    @property
    def error_message(self) -> str | None:
        return self._error_message

    @property
    def data_scanned_in_bytes(self) -> int | None:
        return self._data_scanned_in_bytes

    @property
    def engine_execution_time_in_millis(self) -> int | None:
        return self._engine_execution_time_in_millis

    @property
    def query_queue_time_in_millis(self) -> int | None:
        return self._query_queue_time_in_millis

    @property
    def total_execution_time_in_millis(self) -> int | None:
        return self._total_execution_time_in_millis

    @property
    def query_planning_time_in_millis(self) -> int | None:
        return self._query_planning_time_in_millis

    @property
    def service_processing_time_in_millis(self) -> int | None:
        return self._service_processing_time_in_millis

    @property
    def output_location(self) -> str | None:
        return self._output_location

    @property
    def data_manifest_location(self) -> str | None:
        return self._data_manifest_location

    @property
    def reused_previous_result(self) -> bool | None:
        return self._reused_previous_result

    @property
    def encryption_option(self) -> str | None:
        return self._encryption_option

    @property
    def kms_key(self) -> str | None:
        return self._kms_key

    @property
    def expected_bucket_owner(self) -> str | None:
        return self._expected_bucket_owner

    @property
    def s3_acl_option(self) -> str | None:
        return self._s3_acl_option

    @property
    def selected_engine_version(self) -> str | None:
        return self._selected_engine_version

    @property
    def effective_engine_version(self) -> str | None:
        return self._effective_engine_version

    @property
    def result_reuse_enabled(self) -> bool | None:
        return self._result_reuse_enabled

    @property
    def result_reuse_minutes(self) -> int | None:
        return self._result_reuse_minutes


class AthenaCalculationExecutionStatus:
    """Status information for an Athena calculation execution.

    This class represents the current state and statistics of a calculation
    execution in Amazon Athena's notebook or interactive session environment.
    It tracks the calculation's lifecycle from creation through completion.

    Calculation States:
        - CREATING: Calculation is being created
        - CREATED: Calculation has been created
        - QUEUED: Calculation is waiting to execute
        - RUNNING: Calculation is currently executing
        - CANCELING: Calculation is being cancelled
        - CANCELED: Calculation was cancelled
        - COMPLETED: Calculation completed successfully
        - FAILED: Calculation execution failed

    See Also:
        AWS Athena CalculationExecutionStatus API reference:
        https://docs.aws.amazon.com/athena/latest/APIReference/API_CalculationStatus.html
    """

    STATE_CREATING: str = "CREATING"
    STATE_CREATED: str = "CREATED"
    STATE_QUEUED: str = "QUEUED"
    STATE_RUNNING: str = "RUNNING"
    STATE_CANCELING: str = "CANCELING"
    STATE_CANCELED: str = "CANCELED"
    STATE_COMPLETED: str = "COMPLETED"
    STATE_FAILED: str = "FAILED"

    def __init__(self, response: dict[str, Any]) -> None:
        status = response.get("Status")
        if not status:
            raise DataError("KeyError `Status`")
        self._state: str | None = status.get("State")
        self._state_change_reason: str | None = status.get("StateChangeReason")
        self._submission_date_time: datetime | None = status.get("SubmissionDateTime")
        self._completion_date_time: datetime | None = status.get("CompletionDateTime")

        statistics = response.get("Statistics")
        if not statistics:
            raise DataError("KeyError `Statistics`")
        self._dpu_execution_in_millis: int | None = statistics.get("DpuExecutionInMillis")
        self._progress: str | None = statistics.get("Progress")

    @property
    def state(self) -> str | None:
        return self._state

    @property
    def state_change_reason(self) -> str | None:
        return self._state_change_reason

    @property
    def submission_date_time(self) -> datetime | None:
        return self._submission_date_time

    @property
    def completion_date_time(self) -> datetime | None:
        return self._completion_date_time

    @property
    def dpu_execution_in_millis(self) -> int | None:
        return self._dpu_execution_in_millis

    @property
    def progress(self) -> str | None:
        return self._progress


class AthenaCalculationExecution(AthenaCalculationExecutionStatus):
    """Represents a complete Athena calculation execution with status and results.

    This class extends AthenaCalculationExecutionStatus to include additional
    information about the calculation execution, including session details,
    working directory, and result locations in S3.

    Attributes are inherited from AthenaCalculationExecutionStatus for state
    and timing information.

    See Also:
        AWS Athena CalculationExecution API reference:
        https://docs.aws.amazon.com/athena/latest/APIReference/API_CalculationSummary.html
    """

    def __init__(self, response: dict[str, Any]) -> None:
        super().__init__(response)

        self._calculation_id: str | None = response.get("CalculationExecutionId")
        if not self._calculation_id:
            raise DataError("KeyError `CalculationExecutionId`")
        self._session_id: str | None = response.get("SessionId")
        if not self._session_id:
            raise DataError("KeyError `SessionId`")
        self._description: str | None = response.get("Description")
        self._working_directory: str | None = response.get("WorkingDirectory")

        # If cancelled, the result does not exist.
        result = response.get("Result", {})
        self._std_out_s3_uri: str | None = result.get("StdOutS3Uri")
        self._std_error_s3_uri: str | None = result.get("StdErrorS3Uri")
        self._result_s3_uri: str | None = result.get("ResultS3Uri")
        self._result_type: str | None = result.get("ResultType")

    @property
    def calculation_id(self) -> str | None:
        return self._calculation_id

    @property
    def session_id(self) -> str | None:
        return self._session_id

    @property
    def description(self) -> str | None:
        return self._description

    @property
    def working_directory(self) -> str | None:
        return self._working_directory

    @property
    def std_out_s3_uri(self) -> str | None:
        return self._std_out_s3_uri

    @property
    def std_error_s3_uri(self) -> str | None:
        return self._std_error_s3_uri

    @property
    def result_s3_uri(self) -> str | None:
        return self._result_s3_uri

    @property
    def result_type(self) -> str | None:
        return self._result_type


class AthenaSessionStatus:
    """Status information for an Athena interactive session.

    This class represents the current state of an interactive session in
    Amazon Athena, used for notebook and Spark workloads. Sessions provide
    a persistent environment for running multiple calculations.

    Session States:
        - CREATING: Session is being created
        - CREATED: Session has been created
        - IDLE: Session is idle and ready for calculations
        - BUSY: Session is executing a calculation
        - TERMINATING: Session is being terminated
        - TERMINATED: Session has been terminated
        - DEGRADED: Session is in a degraded state
        - FAILED: Session creation or execution failed

    See Also:
        AWS Athena Session API reference:
        https://docs.aws.amazon.com/athena/latest/APIReference/API_SessionStatus.html
    """

    STATE_CREATING: str = "CREATING"
    STATE_CREATED: str = "CREATED"
    STATE_IDLE: str = "IDLE"
    STATE_BUSY: str = "BUSY"
    STATE_TERMINATING: str = "TERMINATING"
    STATE_TERMINATED: str = "TERMINATED"
    STATE_DEGRADED: str = "DEGRADED"
    STATE_FAILED: str = "FAILED"

    def __init__(self, response: dict[str, Any]) -> None:
        self._session_id: str | None = response.get("SessionId")

        status = response.get("Status")
        if not status:
            raise DataError("KeyError `Status`")
        self._state: str | None = status.get("State")
        self._state_change_reason: str | None = status.get("StateChangeReason")
        self._start_date_time: datetime | None = status.get("StartDateTime")
        self._last_modified_date_time: datetime | None = status.get("LastModifiedDateTime")
        self._end_date_time: datetime | None = status.get("EndDateTime")
        self._idle_since_date_time: datetime | None = status.get("IdleSinceDateTime")

    @property
    def session_id(self) -> str | None:
        return self._session_id

    @property
    def state(self) -> str | None:
        return self._state

    @property
    def state_change_reason(self) -> str | None:
        return self._state_change_reason

    @property
    def start_date_time(self) -> datetime | None:
        return self._start_date_time

    @property
    def last_modified_date_time(self) -> datetime | None:
        return self._last_modified_date_time

    @property
    def end_date_time(self) -> datetime | None:
        return self._end_date_time

    @property
    def idle_since_date_time(self) -> datetime | None:
        return self._idle_since_date_time


class AthenaDatabase:
    """Represents an Athena database (schema) and its metadata.

    This class encapsulates information about a database in the AWS Glue
    Data Catalog that is accessible through Amazon Athena. Databases serve
    as containers for tables and views.

    See Also:
        AWS Athena Database API reference:
        https://docs.aws.amazon.com/athena/latest/APIReference/API_Database.html
    """

    def __init__(self, response):
        database = response.get("Database")
        if not database:
            raise DataError("KeyError `Database`")

        self._name: str | None = database.get("Name")
        self._description: str | None = database.get("Description")
        self._parameters: dict[str, str] = database.get("Parameters", {})

    @property
    def name(self) -> str | None:
        return self._name

    @property
    def description(self) -> str | None:
        return self._description

    @property
    def parameters(self) -> dict[str, str]:
        return self._parameters


class AthenaTableMetadataColumn:
    """Represents a column definition in an Athena table.

    This class contains information about a single column in a table,
    including its name, data type, and optional comment.

    See Also:
        AWS Athena Column API reference:
        https://docs.aws.amazon.com/athena/latest/APIReference/API_Column.html
    """

    def __init__(self, response):
        self._name: str | None = response.get("Name")
        self._type: str | None = response.get("Type")
        self._comment: str | None = response.get("Comment")

    @property
    def name(self) -> str | None:
        return self._name

    @property
    def type(self) -> str | None:
        return self._type

    @property
    def comment(self) -> str | None:
        return self._comment


class AthenaTableMetadataPartitionKey:
    """Represents a partition key definition in an Athena table.

    This class contains information about a partition key column,
    which is used to organize data in partitioned tables for
    improved query performance.

    See Also:
        AWS Athena Column API reference:
        https://docs.aws.amazon.com/athena/latest/APIReference/API_Column.html
    """

    def __init__(self, response):
        self._name: str | None = response.get("Name")
        self._type: str | None = response.get("Type")
        self._comment: str | None = response.get("Comment")

    @property
    def name(self) -> str | None:
        return self._name

    @property
    def type(self) -> str | None:
        return self._type

    @property
    def comment(self) -> str | None:
        return self._comment


class AthenaTableMetadata:
    """Represents comprehensive metadata for an Athena table.

    This class contains detailed information about a table in the AWS Glue
    Data Catalog, including columns, partition keys, storage format,
    serialization library, and various table properties.

    The class provides convenient properties for accessing common table
    attributes like location, file format, compression, and SerDe configuration.

    See Also:
        AWS Athena TableMetadata API reference:
        https://docs.aws.amazon.com/athena/latest/APIReference/API_TableMetadata.html
    """

    def __init__(self, response):
        table_metadata = response.get("TableMetadata")
        if not table_metadata:
            raise DataError("KeyError `TableMetadata`")

        self._name: str | None = table_metadata.get("Name")
        self._create_time: datetime | None = table_metadata.get("CreateTime")
        self._last_access_time: datetime | None = table_metadata.get("LastAccessTime")
        self._table_type: str | None = table_metadata.get("TableType")

        columns = table_metadata.get("Columns", [])
        self._columns: list[AthenaTableMetadataColumn] = []
        for column in columns:
            self._columns.append(AthenaTableMetadataColumn(column))

        partition_keys = table_metadata.get("PartitionKeys", [])
        self._partition_keys: list[AthenaTableMetadataPartitionKey] = []
        for key in partition_keys:
            self._partition_keys.append(AthenaTableMetadataPartitionKey(key))

        self._parameters: dict[str, str] = table_metadata.get("Parameters", {})

    @property
    def name(self) -> str | None:
        return self._name

    @property
    def create_time(self) -> datetime | None:
        return self._create_time

    @property
    def last_access_time(self) -> datetime | None:
        return self._last_access_time

    @property
    def table_type(self) -> str | None:
        return self._table_type

    @property
    def columns(self) -> list[AthenaTableMetadataColumn]:
        return self._columns

    @property
    def partition_keys(self) -> list[AthenaTableMetadataPartitionKey]:
        return self._partition_keys

    @property
    def parameters(self) -> dict[str, str]:
        return self._parameters

    @property
    def comment(self) -> str | None:
        return self._parameters.get("comment")

    @property
    def location(self) -> str | None:
        return self._parameters.get("location")

    @property
    def input_format(self) -> str | None:
        return self._parameters.get("inputformat")

    @property
    def output_format(self) -> str | None:
        return self._parameters.get("outputformat")

    @property
    def row_format(self) -> str | None:
        serde = self.serde_serialization_lib
        if serde:
            return f"SERDE '{serde}'"
        return None

    @property
    def file_format(self) -> str | None:
        input = self.input_format
        output = self.output_format
        if input and output:
            return f"INPUTFORMAT '{input}' OUTPUTFORMAT '{output}'"
        return None

    @property
    def serde_serialization_lib(self) -> str | None:
        return self._parameters.get("serde.serialization.lib")

    @property
    def compression(self) -> str | None:
        if "write.compression" in self._parameters:  # text or json
            return self._parameters["write.compression"]
        if "serde.param.write.compression" in self._parameters:  # text or json
            return self._parameters["serde.param.write.compression"]
        if "parquet.compress" in self._parameters:  # parquet
            return self._parameters["parquet.compress"]
        if "orc.compress" in self._parameters:  # orc
            return self._parameters["orc.compress"]
        return None

    @property
    def serde_properties(self) -> dict[str, str]:
        return {
            k.replace("serde.param.", ""): v
            for k, v in self._parameters.items()
            if k.startswith("serde.param.")
        }

    @property
    def table_properties(self) -> dict[str, str]:
        return {k: v for k, v in self._parameters.items() if not k.startswith("serde.param.")}


class AthenaFileFormat:
    """Constants and utilities for Athena supported file formats.

    This class provides constants for file formats supported by Amazon Athena
    and utility methods to check format types. These are commonly used when
    creating tables or configuring UNLOAD operations.

    Supported formats:
        - SEQUENCEFILE: Hadoop SequenceFile format
        - TEXTFILE: Plain text files (default)
        - RCFILE: Record Columnar File format
        - ORC: Optimized Row Columnar format
        - PARQUET: Apache Parquet columnar format
        - AVRO: Apache Avro format
        - ION: Amazon Ion format

    Example:
        >>> from pyathena.model import AthenaFileFormat
        >>>
        >>> # Check if format is Parquet
        >>> if AthenaFileFormat.is_parquet("PARQUET"):
        ...     print("Using columnar format")
        >>>
        >>> # Use in UNLOAD operations
        >>> format_type = AthenaFileFormat.FILE_FORMAT_PARQUET
        >>> sql = f"UNLOAD (...) TO 's3://bucket/path/' WITH (format = '{format_type}')"
        >>> cursor.execute(sql)

    See Also:
        AWS Documentation on supported file formats:
        https://docs.aws.amazon.com/athena/latest/ug/supported-serdes.html
    """

    FILE_FORMAT_SEQUENCEFILE: str = "SEQUENCEFILE"
    FILE_FORMAT_TEXTFILE: str = "TEXTFILE"
    FILE_FORMAT_RCFILE: str = "RCFILE"
    FILE_FORMAT_ORC: str = "ORC"
    FILE_FORMAT_PARQUET: str = "PARQUET"
    FILE_FORMAT_AVRO: str = "AVRO"
    FILE_FORMAT_ION: str = "ION"

    @staticmethod
    def is_parquet(value: str) -> bool:
        return value.upper() == AthenaFileFormat.FILE_FORMAT_PARQUET

    @staticmethod
    def is_orc(value: str) -> bool:
        return value.upper() == AthenaFileFormat.FILE_FORMAT_ORC


class AthenaRowFormatSerde:
    """Row format serializer/deserializer (SerDe) constants for Athena tables.

    This class provides constants for the various SerDe libraries that can be
    used to serialize and deserialize data in Athena tables. SerDes define how
    data is read from and written to underlying storage formats.

    The class also provides utility methods to detect specific SerDe types
    from table metadata strings.

    Supported SerDes:
        - CSV: OpenCSVSerde for CSV files
        - REGEX: RegexSerDe for regex-parsed text files
        - LAZY_SIMPLE: LazySimpleSerDe for simple delimited text
        - CLOUD_TRAIL: CloudTrailSerde for AWS CloudTrail logs
        - GROK: GrokSerDe for grok pattern parsing
        - JSON: JsonSerDe for JSON data (OpenX implementation)
        - JSON_HCATALOG: JsonSerDe for JSON data (HCatalog implementation)
        - PARQUET: ParquetHiveSerDe for Parquet files
        - ORC: OrcSerde for ORC files
        - AVRO: AvroSerDe for Avro files

    See Also:
        AWS Athena SerDe Reference:
        https://docs.aws.amazon.com/athena/latest/ug/serde-reference.html
    """

    PATTERN_ROW_FORMAT_SERDE: Pattern[str] = re.compile(r"^(?i:serde) '(?P<serde>.+)'$")

    ROW_FORMAT_SERDE_CSV: str = "org.apache.hadoop.hive.serde2.OpenCSVSerde"
    ROW_FORMAT_SERDE_REGEX: str = "org.apache.hadoop.hive.serde2.RegexSerDe"
    ROW_FORMAT_SERDE_LAZY_SIMPLE: str = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
    ROW_FORMAT_SERDE_CLOUD_TRAIL: str = "com.amazon.emr.hive.serde.CloudTrailSerde"
    ROW_FORMAT_SERDE_GROK: str = "com.amazonaws.glue.serde.GrokSerDe"
    ROW_FORMAT_SERDE_JSON: str = "org.openx.data.jsonserde.JsonSerDe"
    ROW_FORMAT_SERDE_JSON_HCATALOG: str = "org.apache.hive.hcatalog.data.JsonSerDe"
    ROW_FORMAT_SERDE_PARQUET: str = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    ROW_FORMAT_SERDE_ORC: str = "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
    ROW_FORMAT_SERDE_AVRO: str = "org.apache.hadoop.hive.serde2.avro.AvroSerDe"

    @staticmethod
    def is_parquet(value: str) -> bool:
        match = AthenaRowFormatSerde.PATTERN_ROW_FORMAT_SERDE.search(value)
        if match:
            serde = match.group("serde")
            if serde == AthenaRowFormatSerde.ROW_FORMAT_SERDE_PARQUET:
                return True
        return False

    @staticmethod
    def is_orc(value: str) -> bool:
        match = AthenaRowFormatSerde.PATTERN_ROW_FORMAT_SERDE.search(value)
        if match:
            serde = match.group("serde")
            if serde == AthenaRowFormatSerde.ROW_FORMAT_SERDE_ORC:
                return True
        return False


class AthenaCompression:
    """Constants and utilities for Athena supported compression formats.

    This class provides constants for compression formats supported by Amazon Athena
    and utility methods to validate compression types. These are commonly used when
    creating tables, configuring UNLOAD operations, or optimizing data storage.

    Supported compression formats:
        - BZIP2: BZIP2 compression
        - DEFLATE: DEFLATE compression
        - GZIP: GZIP compression (most common)
        - LZ4: LZ4 fast compression
        - LZO: LZO compression
        - SNAPPY: Snappy compression (good for Parquet)
        - ZLIB: ZLIB compression
        - ZSTD: Zstandard compression

    Example:
        >>> from pyathena.model import AthenaCompression
        >>>
        >>> # Validate compression format
        >>> if AthenaCompression.is_valid("GZIP"):
        ...     print("Valid compression format")
        >>>
        >>> # Use in UNLOAD operations
        >>> compression = AthenaCompression.COMPRESSION_GZIP
        >>> sql = f"UNLOAD (...) TO 's3://bucket/path/' WITH (compression = '{compression}')"
        >>> cursor.execute(sql)

    See Also:
        AWS Documentation on compression formats:
        https://docs.aws.amazon.com/athena/latest/ug/compression-formats.html

        Best practices for data compression in Athena:
        https://docs.aws.amazon.com/athena/latest/ug/compression-support.html
    """

    COMPRESSION_BZIP2: str = "BZIP2"
    COMPRESSION_DEFLATE: str = "DEFLATE"
    COMPRESSION_GZIP: str = "GZIP"
    COMPRESSION_LZ4: str = "LZ4"
    COMPRESSION_LZO: str = "LZO"
    COMPRESSION_SNAPPY: str = "SNAPPY"
    COMPRESSION_ZLIB: str = "ZLIB"
    COMPRESSION_ZSTD: str = "ZSTD"

    @staticmethod
    def is_valid(value: str) -> bool:
        return value.upper() in [
            AthenaCompression.COMPRESSION_BZIP2,
            AthenaCompression.COMPRESSION_DEFLATE,
            AthenaCompression.COMPRESSION_GZIP,
            AthenaCompression.COMPRESSION_LZ4,
            AthenaCompression.COMPRESSION_LZO,
            AthenaCompression.COMPRESSION_SNAPPY,
            AthenaCompression.COMPRESSION_ZLIB,
            AthenaCompression.COMPRESSION_ZSTD,
        ]


class AthenaPartitionTransform:
    """Partition transform constants for Iceberg tables in Athena.

    This class provides constants for partition transforms used with Apache
    Iceberg tables in Athena. Partition transforms allow you to create
    derived partition values from source column data, enabling more flexible
    and efficient partitioning strategies.

    Transforms:
        - year: Extract year from a timestamp/date column
        - month: Extract year and month from a timestamp/date column
        - day: Extract year, month, and day from a timestamp/date column
        - hour: Extract year, month, day, and hour from a timestamp column
        - bucket: Hash partition into N buckets
        - truncate: Truncate values to a specified width

    Example:
        Iceberg table with partition transforms::

            CREATE TABLE my_table (
                id bigint,
                ts timestamp,
                category string
            )
            PARTITIONED BY (month(ts), bucket(16, category))

    See Also:
        AWS Athena Iceberg Partitioning:
        https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html
    """

    PARTITION_TRANSFORM_YEAR: str = "year"
    PARTITION_TRANSFORM_MONTH: str = "month"
    PARTITION_TRANSFORM_DAY: str = "day"
    PARTITION_TRANSFORM_HOUR: str = "hour"
    PARTITION_TRANSFORM_BUCKET: str = "bucket"
    PARTITION_TRANSFORM_TRUNCATE: str = "truncate"

    @staticmethod
    def is_valid(value: str) -> bool:
        return value.lower() in [
            AthenaPartitionTransform.PARTITION_TRANSFORM_YEAR,
            AthenaPartitionTransform.PARTITION_TRANSFORM_MONTH,
            AthenaPartitionTransform.PARTITION_TRANSFORM_DAY,
            AthenaPartitionTransform.PARTITION_TRANSFORM_HOUR,
            AthenaPartitionTransform.PARTITION_TRANSFORM_BUCKET,
            AthenaPartitionTransform.PARTITION_TRANSFORM_TRUNCATE,
        ]
