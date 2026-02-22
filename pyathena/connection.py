from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    TypeVar,
    cast,
    overload,
)

from boto3.session import Session
from botocore.config import Config

import pyathena
from pyathena.common import BaseCursor, CursorIterator
from pyathena.converter import Converter
from pyathena.cursor import Cursor
from pyathena.error import NotSupportedError, ProgrammingError
from pyathena.formatter import DefaultParameterFormatter, Formatter
from pyathena.util import RetryConfig

if TYPE_CHECKING:
    from botocore.client import BaseClient

_logger = logging.getLogger(__name__)


ConnectionCursor = TypeVar("ConnectionCursor", bound=BaseCursor)
FunctionalCursor = TypeVar("FunctionalCursor", bound=BaseCursor)


class Connection(Generic[ConnectionCursor]):
    """A DB API 2.0 compliant connection to Amazon Athena.

    The Connection class represents a database session and provides methods to
    create cursors for executing SQL queries against Amazon Athena. It handles
    authentication, session management, and query result storage in S3.

    This class follows the Python Database API Specification v2.0 (PEP 249)
    and provides a familiar interface for database operations.

    Attributes:
        s3_staging_dir: S3 location where query results are stored.
        region_name: AWS region name.
        schema_name: Default database/schema name for queries.
        catalog_name: Data catalog name (typically "awsdatacatalog").
        work_group: Athena workgroup name.
        poll_interval: Interval in seconds for polling query status.
        encryption_option: S3 encryption option for query results.
        kms_key: KMS key for encryption when applicable.
        kill_on_interrupt: Whether to cancel queries on interrupt signals.
        result_reuse_enable: Whether to enable Athena's result reuse feature.
        result_reuse_minutes: Minutes to reuse cached results.

    Example:
        >>> conn = Connection(
        ...     s3_staging_dir='s3://my-bucket/staging/',
        ...     region_name='us-east-1',
        ...     schema_name='mydatabase'
        ... )
        >>> with conn:
        ...     cursor = conn.cursor()
        ...     cursor.execute("SELECT COUNT(*) FROM mytable")
        ...     result = cursor.fetchone()

    Note:
        Either s3_staging_dir or work_group must be specified. If using a
        workgroup, it must have a result location configured unless
        s3_staging_dir is also provided. For workgroups with managed query
        result storage, pass ``s3_staging_dir=""`` to skip the environment
        variable fallback.
    """

    _ENV_S3_STAGING_DIR: str = "AWS_ATHENA_S3_STAGING_DIR"
    _ENV_WORK_GROUP: str = "AWS_ATHENA_WORK_GROUP"
    _SESSION_PASSING_ARGS: ClassVar[list[str]] = [
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "region_name",
        "botocore_session",
        "profile_name",
    ]
    _CLIENT_PASSING_ARGS: ClassVar[list[str]] = [
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "api_version",
        "use_ssl",
        "verify",
        "endpoint_url",
        "region_name",
        "config",
    ]

    @overload
    def __init__(
        self: Connection[Cursor],
        s3_staging_dir: str | None = ...,
        region_name: str | None = ...,
        schema_name: str | None = ...,
        catalog_name: str | None = ...,
        work_group: str | None = ...,
        poll_interval: float = ...,
        encryption_option: str | None = ...,
        kms_key: str | None = ...,
        profile_name: str | None = ...,
        role_arn: str | None = ...,
        role_session_name: str = ...,
        external_id: str | None = ...,
        serial_number: str | None = ...,
        duration_seconds: int = ...,
        converter: Converter | None = ...,
        formatter: Formatter | None = ...,
        retry_config: RetryConfig | None = ...,
        cursor_class: None = ...,
        cursor_kwargs: dict[str, Any] | None = ...,
        kill_on_interrupt: bool = ...,
        session: Session | None = ...,
        config: Config | None = ...,
        result_reuse_enable: bool = ...,
        result_reuse_minutes: int = ...,
        on_start_query_execution: Callable[[str], None] | None = ...,
        **kwargs,
    ) -> None: ...

    @overload
    def __init__(
        self: Connection[ConnectionCursor],
        s3_staging_dir: str | None = ...,
        region_name: str | None = ...,
        schema_name: str | None = ...,
        catalog_name: str | None = ...,
        work_group: str | None = ...,
        poll_interval: float = ...,
        encryption_option: str | None = ...,
        kms_key: str | None = ...,
        profile_name: str | None = ...,
        role_arn: str | None = ...,
        role_session_name: str = ...,
        external_id: str | None = ...,
        serial_number: str | None = ...,
        duration_seconds: int = ...,
        converter: Converter | None = ...,
        formatter: Formatter | None = ...,
        retry_config: RetryConfig | None = ...,
        cursor_class: type[ConnectionCursor] = ...,
        cursor_kwargs: dict[str, Any] | None = ...,
        kill_on_interrupt: bool = ...,
        session: Session | None = ...,
        config: Config | None = ...,
        result_reuse_enable: bool = ...,
        result_reuse_minutes: int = ...,
        on_start_query_execution: Callable[[str], None] | None = ...,
        **kwargs,
    ) -> None: ...

    def __init__(
        self,
        s3_staging_dir: str | None = None,
        region_name: str | None = None,
        schema_name: str | None = "default",
        catalog_name: str | None = "awsdatacatalog",
        work_group: str | None = None,
        poll_interval: float = 1,
        encryption_option: str | None = None,
        kms_key: str | None = None,
        profile_name: str | None = None,
        role_arn: str | None = None,
        role_session_name: str = f"PyAthena-session-{int(time.time())}",
        external_id: str | None = None,
        serial_number: str | None = None,
        duration_seconds: int = 3600,
        converter: Converter | None = None,
        formatter: Formatter | None = None,
        retry_config: RetryConfig | None = None,
        cursor_class: type[ConnectionCursor] | None = None,
        cursor_kwargs: dict[str, Any] | None = None,
        kill_on_interrupt: bool = True,
        session: Session | None = None,
        config: Config | None = None,
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
        on_start_query_execution: Callable[[str], None] | None = None,
        **kwargs,
    ) -> None:
        """Initialize a new Athena database connection.

        Args:
            s3_staging_dir: S3 location to store query results. Required if not
                using workgroups or if workgroup doesn't have result location.
                Pass an empty string to explicitly disable S3 staging and skip
                the ``AWS_ATHENA_S3_STAGING_DIR`` environment variable fallback.
                This is required when connecting to a workgroup with managed
                query result storage enabled.
            region_name: AWS region name. Uses default region if not specified.
            schema_name: Default database/schema name. Defaults to "default".
            catalog_name: Data catalog name. Defaults to "awsdatacatalog".
            work_group: Athena workgroup name. Can substitute for s3_staging_dir
                if workgroup has result location configured.
            poll_interval: Seconds between query status polls. Defaults to 1.0.
            encryption_option: S3 encryption for results ("SSE_S3", "SSE_KMS", "CSE_KMS").
            kms_key: KMS key ID when using SSE_KMS or CSE_KMS encryption.
            profile_name: AWS profile name for authentication.
            role_arn: IAM role ARN to assume for authentication.
            role_session_name: Session name when assuming IAM role.
            external_id: External ID for role assumption (if required by role).
            serial_number: MFA device serial number for role assumption.
            duration_seconds: Role session duration in seconds. Defaults to 3600.
            converter: Custom type converter. Uses DefaultTypeConverter if None.
            formatter: Custom parameter formatter. Uses DefaultParameterFormatter if None.
            retry_config: Retry configuration for API calls. Uses default if None.
            cursor_class: Default cursor class for this connection.
            cursor_kwargs: Default keyword arguments for cursor creation.
            kill_on_interrupt: Cancel running queries on interrupt. Defaults to True.
            session: Pre-configured boto3 Session. Creates new session if None.
            config: Boto3 Config object for client configuration.
            result_reuse_enable: Enable Athena query result reuse. Defaults to False.
            result_reuse_minutes: Minutes to reuse cached results.
            on_start_query_execution: Callback function called when query starts.
            **kwargs: Additional arguments passed to boto3 Session and client.

        Raises:
            ProgrammingError: If neither s3_staging_dir nor work_group is provided.

        Note:
            Either s3_staging_dir or work_group must be specified. Environment
            variables AWS_ATHENA_S3_STAGING_DIR and AWS_ATHENA_WORK_GROUP are
            checked if parameters are not provided.

            When using a workgroup with managed query result storage, pass
            ``s3_staging_dir=""`` to prevent the environment variable fallback
            from sending a ``ResultConfiguration`` that conflicts with
            ``ManagedQueryResultsConfiguration``.
        """
        self._kwargs = {
            **kwargs,
            "role_arn": role_arn,
            "role_session_name": role_session_name,
            "external_id": external_id,
            "serial_number": serial_number,
            "duration_seconds": duration_seconds,
        }
        if s3_staging_dir is not None:
            self.s3_staging_dir: str | None = s3_staging_dir or None
        else:
            self.s3_staging_dir = os.getenv(self._ENV_S3_STAGING_DIR)
        self.region_name = region_name
        self.schema_name = schema_name
        self.catalog_name = catalog_name
        if work_group:
            self.work_group: str | None = work_group
        else:
            self.work_group = os.getenv(self._ENV_WORK_GROUP)
        self.poll_interval = poll_interval
        self.encryption_option = encryption_option
        self.kms_key = kms_key
        self.profile_name = profile_name
        self.config: Config | None = config if config else Config()

        if not self.s3_staging_dir and not self.work_group:
            raise ProgrammingError("Required argument `s3_staging_dir` or `work_group` not found.")

        if self.s3_staging_dir and not self.s3_staging_dir.endswith("/"):
            self.s3_staging_dir = f"{self.s3_staging_dir}/"

        if session:
            self._session = session
        else:
            if role_arn:
                creds = self._assume_role(
                    profile_name=self.profile_name,
                    region_name=self.region_name,
                    role_arn=role_arn,
                    role_session_name=role_session_name,
                    external_id=external_id,
                    serial_number=serial_number,
                    duration_seconds=duration_seconds,
                )
                self.profile_name = None
                self._kwargs.update(
                    {
                        "aws_access_key_id": creds["AccessKeyId"],
                        "aws_secret_access_key": creds["SecretAccessKey"],
                        "aws_session_token": creds["SessionToken"],
                    }
                )
            elif serial_number:
                creds = self._get_session_token(
                    profile_name=self.profile_name,
                    region_name=self.region_name,
                    serial_number=serial_number,
                    duration_seconds=duration_seconds,
                )
                self.profile_name = None
                self._kwargs.update(
                    {
                        "aws_access_key_id": creds["AccessKeyId"],
                        "aws_secret_access_key": creds["SecretAccessKey"],
                        "aws_session_token": creds["SessionToken"],
                    }
                )
            self._session = Session(
                region_name=self.region_name,
                profile_name=self.profile_name,
                **self._session_kwargs,
            )

        if not self.config.user_agent_extra or (
            pyathena.user_agent_extra not in self.config.user_agent_extra
        ):
            self.config.user_agent_extra = (
                f"{pyathena.user_agent_extra}"
                f"{' ' + self.config.user_agent_extra if self.config.user_agent_extra else ''}"
            )
        self._client = self._session.client(
            "athena", region_name=self.region_name, config=self.config, **self._client_kwargs
        )
        self._converter = converter
        self._formatter = formatter if formatter else DefaultParameterFormatter()
        self._retry_config = retry_config if retry_config else RetryConfig()
        self.cursor_class = cursor_class if cursor_class else cast(type[ConnectionCursor], Cursor)
        self.cursor_kwargs = cursor_kwargs if cursor_kwargs else {}
        self.kill_on_interrupt = kill_on_interrupt
        self.result_reuse_enable = result_reuse_enable
        self.result_reuse_minutes = result_reuse_minutes
        self.on_start_query_execution = on_start_query_execution

    def _assume_role(
        self,
        profile_name: str | None,
        region_name: str | None,
        role_arn: str,
        role_session_name: str,
        external_id: str | None,
        serial_number: str | None,
        duration_seconds: int,
    ) -> dict[str, Any]:
        """Assume an IAM role and return temporary credentials.

        Uses AWS STS to assume the specified IAM role and obtain temporary
        security credentials. Supports multi-factor authentication (MFA)
        when a serial number is provided.

        Args:
            profile_name: AWS profile name to use for the STS client.
            region_name: AWS region for the STS client.
            role_arn: ARN of the IAM role to assume.
            role_session_name: Name for the role session.
            external_id: External ID for additional security when assuming role.
            serial_number: MFA device serial number. If provided, prompts for MFA code.
            duration_seconds: Duration of the temporary credentials in seconds.

        Returns:
            Dictionary containing temporary AWS credentials with keys:
            'AccessKeyId', 'SecretAccessKey', 'SessionToken', 'Expiration'.

        Note:
            When MFA is required (serial_number provided), this method will
            prompt for an MFA token code via input().
        """
        session = Session(
            region_name=region_name, profile_name=profile_name, **self._session_kwargs
        )
        client = session.client(
            "sts", region_name=region_name, config=self.config, **self._client_kwargs
        )
        request = {
            "RoleArn": role_arn,
            "RoleSessionName": role_session_name,
            "DurationSeconds": duration_seconds,
        }
        if external_id:
            request.update(
                {
                    "ExternalId": external_id,
                }
            )
        if serial_number:
            token_code = input("Enter the MFA code: ")
            request.update(
                {
                    "SerialNumber": serial_number,
                    "TokenCode": token_code,
                }
            )
        response = client.assume_role(**request)
        creds: dict[str, Any] = response["Credentials"]
        return creds

    def _get_session_token(
        self,
        profile_name: str | None,
        region_name: str | None,
        serial_number: str | None,
        duration_seconds: int,
    ) -> dict[str, Any]:
        """Get session token using MFA authentication.

        Obtains temporary security credentials by providing MFA authentication.
        This is used when MFA is required but role assumption is not needed.

        Args:
            profile_name: AWS profile name to use for the STS client.
            region_name: AWS region for the STS client.
            serial_number: MFA device serial number.
            duration_seconds: Duration of the temporary credentials in seconds.

        Returns:
            Dictionary containing temporary AWS credentials with keys:
            'AccessKeyId', 'SecretAccessKey', 'SessionToken', 'Expiration'.

        Note:
            This method will prompt for an MFA token code via input().
        """
        session = Session(profile_name=profile_name, **self._session_kwargs)
        client = session.client(
            "sts", region_name=region_name, config=self.config, **self._client_kwargs
        )
        token_code = input("Enter the MFA code: ")
        request = {
            "DurationSeconds": duration_seconds,
            "SerialNumber": serial_number,
            "TokenCode": token_code,
        }
        response = client.get_session_token(**request)
        creds: dict[str, Any] = response["Credentials"]
        return creds

    @property
    def _session_kwargs(self) -> dict[str, Any]:
        """Get session keyword arguments for AWS Session creation.

        Returns:
            Dictionary of filtered keyword arguments that are valid for
            boto3 Session constructor.
        """
        return {k: v for k, v in self._kwargs.items() if k in self._SESSION_PASSING_ARGS}

    @property
    def _client_kwargs(self) -> dict[str, Any]:
        """Get client keyword arguments for AWS client creation.

        Returns:
            Dictionary of filtered keyword arguments that are valid for
            boto3 client constructor.
        """
        return {k: v for k, v in self._kwargs.items() if k in self._CLIENT_PASSING_ARGS}

    @property
    def session(self) -> Session:
        """Get the boto3 session used for AWS API calls.

        Returns:
            The configured boto3 Session object.
        """
        return self._session

    @property
    def client(self) -> BaseClient:
        """Get the boto3 Athena client used for query operations.

        Returns:
            The configured boto3 Athena client.
        """
        return self._client

    @property
    def retry_config(self) -> RetryConfig:
        """Get the retry configuration for AWS API calls.

        Returns:
            The RetryConfig object that controls retry behavior for failed requests.
        """
        return self._retry_config

    def __enter__(self):
        """Enter the runtime context for the connection.

        Returns:
            Self for use in context manager protocol.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the runtime context and close the connection.

        Args:
            exc_type: Exception type if an exception occurred.
            exc_val: Exception value if an exception occurred.
            exc_tb: Exception traceback if an exception occurred.
        """
        self.close()

    @overload
    def cursor(self, cursor: None = ..., **kwargs) -> ConnectionCursor: ...

    @overload
    def cursor(self, cursor: type[FunctionalCursor], **kwargs) -> FunctionalCursor: ...

    def cursor(
        self, cursor: type[FunctionalCursor] | None = None, **kwargs
    ) -> FunctionalCursor | ConnectionCursor:
        """Create a new cursor object for executing queries.

        Creates and returns a cursor object that can be used to execute SQL
        queries against Amazon Athena. The cursor inherits connection settings
        but can be customized with additional parameters.

        Args:
            cursor: Custom cursor class to use. If not provided, uses the
                connection's default cursor class.
            **kwargs: Additional keyword arguments to pass to the cursor
                constructor. These override connection defaults.

        Returns:
            A cursor object that can execute SQL queries.

        Example:
            >>> cursor = connection.cursor()
            >>> cursor.execute("SELECT * FROM my_table LIMIT 10")
            >>> results = cursor.fetchall()

            # Using a custom cursor type
            >>> from pyathena.pandas.cursor import PandasCursor
            >>> pandas_cursor = connection.cursor(PandasCursor)
            >>> df = pandas_cursor.execute("SELECT * FROM my_table").fetchall()
        """
        kwargs.update(self.cursor_kwargs)
        _cursor = cursor or self.cursor_class
        converter = kwargs.pop("converter", self._converter)
        if not converter:
            converter = _cursor.get_default_converter(kwargs.get("unload", False))
        return _cursor(
            connection=self,
            converter=converter,
            formatter=kwargs.pop("formatter", self._formatter),
            retry_config=kwargs.pop("retry_config", self._retry_config),
            s3_staging_dir=kwargs.pop("s3_staging_dir", self.s3_staging_dir),
            schema_name=kwargs.pop("schema_name", self.schema_name),
            catalog_name=kwargs.pop("catalog_name", self.catalog_name),
            work_group=kwargs.pop("work_group", self.work_group),
            poll_interval=kwargs.pop("poll_interval", self.poll_interval),
            encryption_option=kwargs.pop("encryption_option", self.encryption_option),
            kms_key=kwargs.pop("kms_key", self.kms_key),
            kill_on_interrupt=kwargs.pop("kill_on_interrupt", self.kill_on_interrupt),
            result_reuse_enable=kwargs.pop("result_reuse_enable", self.result_reuse_enable),
            result_reuse_minutes=kwargs.pop("result_reuse_minutes", self.result_reuse_minutes),
            on_start_query_execution=kwargs.pop(
                "on_start_query_execution", self.on_start_query_execution
            ),
            **kwargs,
        )

    def close(self) -> None:
        """Close the connection.

        Closes the database connection. This method is provided for DB API 2.0
        compatibility. Since Athena connections are stateless, this method
        currently does not perform any actual cleanup operations.

        Note:
            This method is called automatically when using the connection
            as a context manager (with statement).
        """

    def commit(self) -> None:
        """Commit any pending transaction.

        This method is provided for DB API 2.0 compatibility. Since Athena
        does not support transactions, this method does nothing.

        Note:
            Athena queries are auto-committed and cannot be rolled back.
        """

    def rollback(self) -> None:
        """Rollback any pending transaction.

        This method is required by DB API 2.0 but is not supported by Athena
        since Athena does not support transactions.

        Raises:
            NotSupportedError: Always raised since transactions are not supported.
        """
        raise NotSupportedError
