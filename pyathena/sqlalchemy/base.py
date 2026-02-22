from __future__ import annotations

import contextlib
import re
from collections.abc import Mapping, MutableMapping
from re import Pattern
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)

import botocore
from sqlalchemy import exc, schema, text, types, util
from sqlalchemy.engine import Engine, reflection
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.interfaces import ExecutionContext
from sqlalchemy.sql.compiler import (
    DDLCompiler,
    GenericTypeCompiler,
    IdentifierPreparer,
    SQLCompiler,
)

import pyathena
from pyathena.sqlalchemy.compiler import (
    AthenaDDLCompiler,
    AthenaStatementCompiler,
    AthenaTypeCompiler,
)
from pyathena.sqlalchemy.preparer import AthenaDMLIdentifierPreparer
from pyathena.sqlalchemy.types import (
    TINYINT,
    AthenaDate,
    AthenaStruct,
    AthenaTimestamp,
    get_double_type,
)
from pyathena.sqlalchemy.util import _HashableDict
from pyathena.util import strtobool

if TYPE_CHECKING:
    from types import ModuleType

    from sqlalchemy import (
        URL,
        ClauseElement,
        Connection,
        PoolProxiedConnection,
    )
    from sqlalchemy.engine.interfaces import (
        ReflectedForeignKeyConstraint,
        ReflectedIndex,
        ReflectedPrimaryKeyConstraint,
    )
    from sqlalchemy.sql.schema import SchemaItem


ischema_names: dict[str, type[Any]] = {
    "boolean": types.BOOLEAN,
    "float": types.FLOAT,
    "double": get_double_type(),
    "real": types.FLOAT,
    "tinyint": TINYINT,
    "smallint": types.SMALLINT,
    "integer": types.INTEGER,
    "int": types.INTEGER,
    "bigint": types.BIGINT,
    "decimal": types.DECIMAL,
    "char": types.CHAR,
    "varchar": types.VARCHAR,
    "string": types.String,
    "date": types.DATE,
    "timestamp": types.TIMESTAMP,
    "binary": types.BINARY,
    "varbinary": types.BINARY,
    "array": types.String,
    "map": types.String,
    "struct": AthenaStruct,
    "row": AthenaStruct,
    "json": types.JSON,
}


class AthenaDialect(DefaultDialect):
    """SQLAlchemy dialect for Amazon Athena.

    This dialect enables SQLAlchemy to communicate with Amazon Athena,
    allowing you to use SQLAlchemy's ORM and Core features with Athena
    as the backend database engine.

    The dialect handles Athena-specific SQL syntax, data type mapping,
    and schema reflection. It supports table creation with Athena-specific
    options like file format, compression, and partitioning.

    Connection URL Format:
        ``awsathena+rest://{access_key}:{secret_key}@athena.{region}.amazonaws.com/{schema}``

    Query Parameters:
        - s3_staging_dir: S3 location for query results (required)
        - work_group: Athena workgroup name
        - catalog_name: Data catalog name (default: AwsDataCatalog)
        - poll_interval: Query status polling interval in seconds

    Example:
        >>> from sqlalchemy import create_engine
        >>> engine = create_engine(
        ...     "awsathena+rest://:@athena.us-west-2.amazonaws.com/default"
        ...     "?s3_staging_dir=s3://my-bucket/athena-results/"
        ... )
        >>> with engine.connect() as conn:
        ...     result = conn.execute(text("SELECT * FROM my_table"))

    Dialect Options:
        Table-level options (prefix with ``awsathena_``):
            - location: S3 location for table data
            - compression: Compression format (SNAPPY, GZIP, etc.)
            - file_format: File format (PARQUET, ORC, etc.)
            - row_format: Row format specification
            - tblproperties: Table properties dictionary

        Column-level options:
            - partition: Mark column as partition key
            - cluster: Mark column as clustering key

    See Also:
        SQLAlchemy Dialects:
        https://docs.sqlalchemy.org/en/20/dialects/
    """

    name: str = "awsathena"
    preparer: type[IdentifierPreparer] = AthenaDMLIdentifierPreparer
    statement_compiler: type[SQLCompiler] = AthenaStatementCompiler
    ddl_compiler: type[DDLCompiler] = AthenaDDLCompiler
    type_compiler: type[GenericTypeCompiler] = AthenaTypeCompiler
    default_paramstyle: str = pyathena.paramstyle
    cte_follows_insert: bool = True
    supports_alter: bool = False
    supports_pk_autoincrement: bool | None = False
    supports_default_values: bool = False
    supports_empty_insert: bool = False
    supports_multivalues_insert: bool = True
    supports_native_decimal: bool = True
    supports_native_boolean: bool = True
    supports_unicode_statements: bool | None = True
    supports_unicode_binds: bool | None = True
    supports_statement_cache: bool = True
    returns_unicode_strings: bool | None = True
    description_encoding: bool | None = None
    postfetch_lastrowid: bool = False
    construct_arguments: list[tuple[type[SchemaItem | ClauseElement], Mapping[str, Any]]] | None = [  # noqa: RUF012
        (
            schema.Table,
            {
                "location": None,
                "compression": None,
                "row_format": None,
                "file_format": None,
                "serdeproperties": None,
                "tblproperties": None,
                "bucket_count": None,
            },
        ),
        (
            schema.Column,
            {
                "partition": False,
                "partition_transform": None,
                "partition_transform_bucket_count": None,
                "partition_transform_truncate_length": None,
                "cluster": False,
            },
        ),
    ]

    colspecs: dict[type[Any], type[Any]] = {  # noqa: RUF012
        types.DATE: AthenaDate,
        types.DATETIME: AthenaTimestamp,
        types.TIMESTAMP: AthenaTimestamp,
    }

    ischema_names: dict[str, type[Any]] = ischema_names

    _connect_options: dict[str, Any] = {}  # type: ignore[override]  # noqa: RUF012
    _pattern_column_type: Pattern[str] = re.compile(r"^([a-zA-Z]+)(?:$|[\(|<](.+)[\)|>]$)")

    def __init__(self, json_deserializer=None, json_serializer=None, **kwargs):
        DefaultDialect.__init__(self, **kwargs)
        self._json_deserializer = json_deserializer
        self._json_serializer = json_serializer

    @classmethod
    def import_dbapi(cls) -> ModuleType:
        return pyathena

    @classmethod
    def dbapi(cls) -> ModuleType:  # type: ignore[override]
        return pyathena

    def _raw_connection(self, connection: Engine | Connection) -> PoolProxiedConnection:
        if isinstance(connection, Engine):
            return connection.raw_connection()
        return connection.connection

    def create_connect_args(self, url: URL) -> tuple[tuple[str], MutableMapping[str, Any]]:
        # Connection string format:
        #   awsathena+rest://
        #   {aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/
        #   {schema_name}?s3_staging_dir={s3_staging_dir}&...
        self._connect_options = self._create_connect_args(url)
        return cast(tuple[str], ()), self._connect_options

    def _create_connect_args(self, url: URL) -> dict[str, Any]:
        opts: dict[str, Any] = {
            "aws_access_key_id": url.username if url.username else None,
            "aws_secret_access_key": url.password if url.password else None,
            "region_name": re.sub(
                r"^athena\.([a-z0-9-]+)\.amazonaws\.(com|com.cn)$", r"\1", url.host
            )
            if url.host
            else None,
            "schema_name": url.database if url.database else "default",
        }
        opts.update(url.query)
        if "verify" in opts:
            verify = opts["verify"]
            # If a ValueError occurs, it is probably the file name of the CA certificate being used.
            with contextlib.suppress(ValueError):
                verify = bool(strtobool(verify))
            opts.update({"verify": verify})
        if "duration_seconds" in opts:
            opts.update({"duration_seconds": int(opts["duration_seconds"])})
        if "poll_interval" in opts:
            opts.update({"poll_interval": float(opts["poll_interval"])})
        if "kill_on_interrupt" in opts:
            opts.update({"kill_on_interrupt": bool(strtobool(opts["kill_on_interrupt"]))})
        if "result_reuse_enable" in opts:
            opts.update({"result_reuse_enable": bool(strtobool(opts["result_reuse_enable"]))})
        if "result_reuse_minutes" in opts:
            opts.update({"result_reuse_minutes": int(opts["result_reuse_minutes"])})
        return opts

    @reflection.cache
    def _get_schemas(self, connection, **kw):
        raw_connection = self._raw_connection(connection)
        catalog = raw_connection.catalog_name  # type: ignore[union-attr]
        with raw_connection.driver_connection.cursor() as cursor:  # type: ignore[union-attr]
            try:
                return cursor.list_databases(catalog)
            except pyathena.error.OperationalError as e:
                cause = e.__cause__
                if (
                    isinstance(cause, botocore.exceptions.ClientError)
                    and cause.response["Error"]["Code"] == "InvalidRequestException"
                ):
                    return []
                raise

    @reflection.cache
    def _get_table(self, connection, table_name: str, schema: str | None = None, **kw):
        raw_connection = self._raw_connection(connection)
        schema = schema if schema else raw_connection.schema_name  # type: ignore[union-attr]
        with raw_connection.driver_connection.cursor() as cursor:  # type: ignore[union-attr]
            try:
                return cursor.get_table_metadata(table_name, schema_name=schema, logging_=False)
            except pyathena.error.OperationalError as e:
                cause = e.__cause__
                if (
                    isinstance(cause, botocore.exceptions.ClientError)
                    and cause.response["Error"]["Code"] == "MetadataException"
                ):
                    raise exc.NoSuchTableError(table_name) from e
                raise

    @reflection.cache
    def _get_tables(self, connection, schema: str | None = None, **kw):
        raw_connection = self._raw_connection(connection)
        schema = schema if schema else raw_connection.schema_name  # type: ignore[union-attr]
        with raw_connection.driver_connection.cursor() as cursor:  # type: ignore[union-attr]
            return cursor.list_table_metadata(schema_name=schema)

    def get_schema_names(self, connection, **kw):
        schemas = self._get_schemas(connection, **kw)
        return [s.name for s in schemas]

    def get_table_names(self, connection: Connection, schema: str | None = None, **kw):
        # Tables created by Athena are always classified as `EXTERNAL_TABLE`,
        # but Athena can also query tables classified as `MANAGED_TABLE`, `EXTERNAL`, or `customer`.
        # Managed Tables are created by default when creating tables via Spark when
        # Glue has been enabled as the Hive Metastore for Elastic Map Reduce (EMR) clusters.
        # With Athena Federation, tables in the database that are connected to Athena via lambda
        # function, is classified as `EXTERNAL` and fully queryable
        tables = self._get_tables(connection, schema, **kw)
        return [
            t.name
            for t in tables
            if t.table_type in ["EXTERNAL_TABLE", "MANAGED_TABLE", "EXTERNAL", "customer"]
        ]

    def get_view_names(self, connection: Connection, schema: str | None = None, **kw):
        tables = self._get_tables(connection, schema, **kw)
        return [t.name for t in tables if t.table_type == "VIRTUAL_VIEW"]

    def get_table_comment(
        self, connection: Connection, table_name: str, schema: str | None = None, **kw
    ):
        metadata = self._get_table(connection, table_name, schema=schema, **kw)
        return {"text": metadata.comment}

    def get_table_options(
        self, connection: Connection, table_name: str, schema: str | None = None, **kw
    ):
        metadata = self._get_table(connection, table_name, schema=schema, **kw)
        # TODO The metadata retrieved from the API does not seem to include bucketing information.
        return {
            "awsathena_location": metadata.location,
            "awsathena_compression": metadata.compression,
            "awsathena_row_format": metadata.row_format,
            "awsathena_file_format": metadata.file_format,
            "awsathena_serdeproperties": _HashableDict(metadata.serde_properties),
            "awsathena_tblproperties": _HashableDict(metadata.table_properties),
        }

    def has_table(self, connection: Connection, table_name: str, schema: str | None = None, **kw):
        try:
            columns = self.get_columns(connection, table_name, schema)
            return bool(columns)
        except exc.NoSuchTableError:
            return False

    @reflection.cache
    def get_view_definition(
        self, connection: Connection, view_name: str, schema: str | None = None, **kw
    ):
        raw_connection = self._raw_connection(connection)
        schema = schema if schema else raw_connection.schema_name  # type: ignore[union-attr]
        query = f"""SHOW CREATE VIEW "{schema}"."{view_name}";"""
        try:
            res = connection.scalars(text(query))
        except exc.OperationalError as e:
            raise exc.NoSuchTableError(f"{schema}.{view_name}") from e
        else:
            return "\n".join(res)

    @reflection.cache
    def get_columns(self, connection: Connection, table_name: str, schema: str | None = None, **kw):
        metadata = self._get_table(connection, table_name, schema=schema, **kw)
        columns = [
            {
                "name": c.name,
                "type": self._get_column_type(c.type),
                "nullable": True,
                "default": None,
                "autoincrement": False,
                "comment": c.comment,
                "dialect_options": {"awsathena_partition": None},
            }
            for c in metadata.columns
        ]
        columns += [
            {
                "name": c.name,
                "type": self._get_column_type(c.type),
                "nullable": True,
                "default": None,
                "autoincrement": False,
                "comment": c.comment,
                "dialect_options": {"awsathena_partition": True},
            }
            for c in metadata.partition_keys
        ]
        return columns

    def _get_column_type(self, type_: str):
        match = self._pattern_column_type.match(type_)
        if match:
            name = match.group(1).lower()
            length = match.group(2)
        else:
            name = type_.lower()
            length = None

        if name in self.ischema_names:
            col_type = self.ischema_names[name]
        else:
            util.warn(f"Did not recognize type '{type_}'")
            col_type = types.NullType

        args = []
        if length:
            if col_type is types.DECIMAL:
                precision, scale = length.split(",")
                args = [int(precision), int(scale)]
            elif col_type is types.CHAR or col_type is types.VARCHAR:
                args = [int(length)]

        return col_type(*args)

    def get_foreign_keys(
        self, connection: Connection, table_name: str, schema: str | None = None, **kw
    ) -> list[ReflectedForeignKeyConstraint]:
        # Athena has no support for foreign keys.
        return []  # pragma: no cover

    def get_pk_constraint(
        self, connection: Connection, table_name: str, schema: str | None = None, **kw
    ) -> ReflectedPrimaryKeyConstraint:
        # Athena has no support for primary keys.
        return {"name": None, "constrained_columns": []}  # pragma: no cover

    def get_indexes(
        self, connection: Connection, table_name: str, schema: str | None = None, **kw
    ) -> list[ReflectedIndex]:
        # Athena has no support for indexes.
        return []  # pragma: no cover

    def do_execute(self, cursor, statement, parameters, context=None):
        on_start_query_execution = None
        if isinstance(context, ExecutionContext):
            execution_options = context.execution_options
            if execution_options is not None:
                on_start_query_execution = execution_options.get("on_start_query_execution")

        if on_start_query_execution is not None:
            cursor.execute(statement, parameters, on_start_query_execution=on_start_query_execution)
        else:
            cursor.execute(statement, parameters)

    def do_rollback(self, dbapi_connection: PoolProxiedConnection) -> None:
        # No transactions for Athena
        pass  # pragma: no cover

    def _check_unicode_returns(
        self, connection: Connection, additional_tests: list[Any] | None = None
    ) -> bool:
        # Requests gives back Unicode strings
        return True  # pragma: no cover

    def _check_unicode_description(self, connection: Connection) -> bool:
        # Requests gives back Unicode strings
        return True  # pragma: no cover
