from __future__ import annotations

import contextlib
import logging
import re
from collections.abc import Mapping, MutableMapping
from re import Pattern
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)

from sqlalchemy import exc, schema, types, util
from sqlalchemy.engine import Engine, reflection
from sqlalchemy.engine.default import DefaultDialect, DefaultExecutionContext
from sqlalchemy.engine.interfaces import ExecutionContext
from sqlalchemy.sql.compiler import (
    DDLCompiler,
    GenericTypeCompiler,
    IdentifierPreparer,
    SQLCompiler,
)

import pyathena
from pyathena.cursor import Cursor
from pyathena.sqlalchemy.compiler import (
    AthenaDDLCompiler,
    AthenaStatementCompiler,
    AthenaTypeCompiler,
)
from pyathena.sqlalchemy.preparer import AthenaDMLIdentifierPreparer
from pyathena.sqlalchemy.types import (
    TINYINT,
    AthenaArray,
    AthenaBinary,
    AthenaDate,
    AthenaMap,
    AthenaStruct,
    AthenaTimestamp,
    get_double_type,
)
from pyathena.sqlalchemy.util import _HashableDict, _split_type_arguments
from pyathena.util import (
    THROTTLING_ERROR_CODES,
    RetryConfig,
    _get_error_code,
    _without_retries,
    strtobool,
)

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

_logger = logging.getLogger(__name__)


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
    "array": AthenaArray,
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
    max_identifier_length: int = 255
    cte_follows_insert: bool = True
    supports_alter: bool = False
    supports_pk_autoincrement: bool | None = False
    supports_default_values: bool = False
    supports_empty_insert: bool = False
    supports_multivalues_insert: bool = True
    # Render executemany inserts as multi-row INSERT statements. Athena has no
    # RETURNING, so batching must also apply to inserts without it. The page
    # size keeps typical rows well below Athena's 262,144-byte query limit.
    use_insertmanyvalues: bool = True
    use_insertmanyvalues_wo_returning: bool = True
    insertmanyvalues_page_size: int = 100
    # Coerce these options from engine_from_config string values.
    engine_config_types: Mapping[str, Any] = util.immutabledict(
        {
            **DefaultDialect.engine_config_types,
            "insertmanyvalues_page_size": util.asint,
            "use_insertmanyvalues": util.asbool,
        }
    )
    supports_sane_rowcount: bool = True
    supports_sane_multi_rowcount: bool = True
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
        types.LargeBinary: AthenaBinary,
        types.BINARY: AthenaBinary,
        types.VARBINARY: AthenaBinary,
        types.ARRAY: AthenaArray,
        types.Date: AthenaDate,
        types.DateTime: AthenaTimestamp,
    }

    ischema_names: dict[str, type[Any]] = ischema_names

    _connect_options: dict[str, Any] = {}  # type: ignore[override]  # noqa: RUF012
    _pattern_column_type: Pattern[str] = re.compile(r"^([a-zA-Z]+)(?:$|[\(|<](.+)[\)|>]$)")
    # Metadata failures that information_schema answers better than a retry.
    # Throttling, because one query costs less than the retry ladder, and a
    # MetadataException that survived unwrapping, because a federated catalog
    # reports a missing table in its connector's words rather than in Glue's
    # EntityNotFoundException envelope.
    _FALLBACK_ERROR_CODES: tuple[str, ...] = (*THROTTLING_ERROR_CODES, "MetadataException")

    # Engine options that the connection URL query can also set.
    _URL_ENGINE_OPTIONS: tuple[str, ...] = ("insertmanyvalues_page_size", "use_insertmanyvalues")

    def __init__(self, json_deserializer=None, json_serializer=None, **kwargs):
        DefaultDialect.__init__(self, **kwargs)
        self._json_deserializer = json_deserializer
        self._json_serializer = json_serializer
        # create_engine passes only the options its caller gave; those take
        # precedence over the same options in the URL query.
        self._explicit_engine_options = frozenset(
            name for name in self._URL_ENGINE_OPTIONS if name in kwargs
        )

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
        return cast(tuple[str], ()), self._create_connect_args(url)

    def _create_connect_args(self, url: URL) -> dict[str, Any]:
        """Build ``pyathena.connect()`` arguments from a SQLAlchemy URL.

        Query parameters are passed through, with the known boolean, integer
        and float options converted from their string form. The
        ``insertmanyvalues_page_size`` and ``use_insertmanyvalues`` parameters
        configure this dialect instead and are not passed through; the same
        options given to ``create_engine`` take precedence.

        Args:
            url: The SQLAlchemy URL.

        Returns:
            The connection arguments.
        """
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
        if "glue_metadata_fallback" in opts:
            opts.update({"glue_metadata_fallback": bool(strtobool(opts["glue_metadata_fallback"]))})
        if "result_reuse_minutes" in opts:
            opts.update({"result_reuse_minutes": int(opts["result_reuse_minutes"])})
        # Remove these URL options even when an explicit create_engine value
        # overrides them, and parse them only when they apply.
        page_size = opts.pop("insertmanyvalues_page_size", None)
        if (
            page_size is not None
            and "insertmanyvalues_page_size" not in self._explicit_engine_options
        ):
            self.insertmanyvalues_page_size = int(page_size)
        use_insertmanyvalues = opts.pop("use_insertmanyvalues", None)
        if (
            use_insertmanyvalues is not None
            and "use_insertmanyvalues" not in self._explicit_engine_options
        ):
            self.use_insertmanyvalues = bool(strtobool(use_insertmanyvalues))
        # Store on the dialect so compilers can consult connection options
        # (e.g. catalog_name for S3 Tables detection). Assigned here rather than
        # in create_connect_args because subclass dialects call this method
        # directly and mutate the returned dict afterwards; sharing the same
        # object keeps _connect_options in sync with their updates.
        self._connect_options = opts
        return opts

    @staticmethod
    def _cursor_option(raw_connection: PoolProxiedConnection, name: str) -> Any:
        """Return a cursor option, letting ``cursor_kwargs`` override the connection default."""
        return raw_connection.cursor_kwargs.get(name, getattr(raw_connection, name))

    @staticmethod
    def _fold_table_name(catalog: str | None, name: str) -> str:
        """Glue lowercases table names, so ``AwsDataCatalog`` lookups fold case."""
        return name.lower() if (catalog or "").lower() == "awsdatacatalog" else name

    @reflection.cache
    def _get_schemas(self, connection, **kw):
        raw_connection = self._raw_connection(connection)
        catalog = self._cursor_option(raw_connection, "catalog_name")
        with raw_connection.driver_connection.cursor() as cursor:  # type: ignore[union-attr]
            try:
                return cursor.list_databases(catalog)
            except pyathena.error.OperationalError as e:
                if _get_error_code(e.__cause__ or e) == "InvalidRequestException":
                    return []
                raise

    def _get_table(self, connection, table_name: str, schema: str | None = None, **kw):
        raw_connection = self._raw_connection(connection)
        catalog = self._cursor_option(raw_connection, "catalog_name")
        schema = schema if schema else self._cursor_option(raw_connection, "schema_name")
        name = self._fold_table_name(catalog, str(table_name))
        # Key by the metadata request, not the reflection method's arguments.
        # Listings and individual lookups share positive results in this Inspector.
        info_cache = kw.get("info_cache")
        if info_cache is None:
            info_cache = {}
        cache_key = ("pyathena_table_metadata", catalog, schema, name)
        metadata = info_cache.get(cache_key)
        if metadata is not None:
            return metadata
        try:
            with raw_connection.driver_connection.cursor() as cursor:  # type: ignore[union-attr]
                metadata = self._lookup_table(cursor, schema, name, table_name)
        except pyathena.error.OperationalError as e:
            # A federated catalog reports a missing table in its connector's own
            # words, so ask information_schema whether it exists, as column
            # reflection does. Throttling still propagates: the query cannot
            # supply a comment or options, only absence.
            code = _get_error_code(e.__cause__ or e, unwrap_metadata=True)
            if not self._is_connector_error(code, catalog):
                raise
            # Columns already reflected from information_schema show the table exists.
            columns_key = ("pyathena_information_schema_columns", catalog, schema, name)
            if info_cache.get(columns_key) is not None or (
                self._columns_from_information_schema(raw_connection, schema, name)
            ):
                raise
            raise exc.NoSuchTableError(table_name) from e
        info_cache[cache_key] = metadata
        return metadata

    @staticmethod
    def _lookup_table(cursor: Any, schema: str | None, name: str, table_name: str) -> Any:
        """Fetch one table's metadata, raising ``NoSuchTableError`` when it is absent."""
        try:
            # GetTableMetadata limits table names to 128 characters, while
            # Athena SQL and ListTableMetadata support longer table names.
            if len(table_name) > 128:
                lowered = name.lower()
                expression = re.escape(lowered)
                # ListTableMetadata limits its regex filter to 256 characters.
                # Unusual catalog names can exceed that after regex escaping.
                listed = cursor.list_table_metadata(
                    schema_name=schema,
                    expression=expression if len(expression) <= 256 else None,
                    logging_=False,
                )
                metadata = next(
                    (m for m in listed if m.name is not None and m.name.lower() == lowered),
                    None,
                )
                if metadata is None:
                    raise exc.NoSuchTableError(table_name)
                return metadata
            return cursor.get_table_metadata(table_name, schema_name=schema, logging_=False)
        except pyathena.error.OperationalError as e:
            if _get_error_code(e.__cause__ or e, unwrap_metadata=True) == (
                "EntityNotFoundException"
            ):
                raise exc.NoSuchTableError(table_name) from e
            raise

    def _get_columns(self, connection, table_name: str, schema: str | None = None, **kw):
        raw_connection = self._raw_connection(connection)
        catalog = self._cursor_option(raw_connection, "catalog_name")
        schema = schema if schema else self._cursor_option(raw_connection, "schema_name")
        name = self._fold_table_name(catalog, str(table_name))
        info_cache = kw.get("info_cache")
        if info_cache is None:
            info_cache = {}
        # Columns already reflected from information_schema stay in use until
        # Inspector.clear_cache(), even if a later listing seeds full metadata.
        columns_key = ("pyathena_information_schema_columns", catalog, schema, name)
        columns = info_cache.get(columns_key)
        if columns is not None:
            return columns
        metadata_key = ("pyathena_table_metadata", catalog, schema, name)
        metadata = info_cache.get(metadata_key)
        if metadata is not None:
            return self._columns_from_metadata(metadata)
        # A metadata request the fallback can answer switches to
        # information_schema at once instead of waiting out the retry policy; the
        # query answers existence and columns, while table comments and options
        # still need the API. Other retryable codes keep the connection's policy.
        # Connection.cursor() applies cursor_kwargs last, so a retry_config given
        # there still runs its own retries before the fallback.
        retry_config = self._without_fallback_retries(
            raw_connection.retry_config  # type: ignore[union-attr]
        )
        with raw_connection.driver_connection.cursor(  # type: ignore[union-attr]
            retry_config=retry_config
        ) as cursor:
            try:
                metadata = self._lookup_table(cursor, schema, name, table_name)
            except pyathena.error.OperationalError as e:
                code = _get_error_code(e.__cause__ or e, unwrap_metadata=True)
                if not self._is_fallback_error(code, catalog):
                    raise
                _logger.warning(
                    f"Table metadata request for {table_name} failed with {code}; "
                    "reflecting columns from information_schema."
                )
                columns = self._columns_from_information_schema(raw_connection, schema, name)
                if not columns:
                    raise exc.NoSuchTableError(table_name) from e
                info_cache[columns_key] = columns
                return columns
        info_cache[metadata_key] = metadata
        return self._columns_from_metadata(metadata)

    @staticmethod
    def _is_fallback_error(code: str | None, catalog: str | None) -> bool:
        """Whether the information_schema fallback answers this failed request.

        The codes are those in ``_FALLBACK_ERROR_CODES``: throttling always, as
        one query costs less than the retry ladder, and a ``MetadataException``
        where ``_is_connector_error`` accepts it.
        """
        return code in THROTTLING_ERROR_CODES or AthenaDialect._is_connector_error(code, catalog)

    @staticmethod
    def _is_connector_error(code: str | None, catalog: str | None) -> bool:
        """Whether ``information_schema`` may decide absence for this error.

        A ``MetadataException`` that survived unwrapping only outside the Glue
        Data Catalog. Glue states missing tables and permission failures in an
        envelope this client recognizes, so an unrecognized one there has an
        unknown cause, and answering it from ``information_schema`` would report
        a table the caller merely cannot see as absent. A federated catalog has
        no such envelope: it reports a missing table in its connector's own
        words, which cannot be recognized at all.
        """
        return code == "MetadataException" and (catalog or "").lower() != "awsdatacatalog"

    @classmethod
    def _without_fallback_retries(cls, retry_config: RetryConfig) -> RetryConfig:
        """Copy a policy without the codes the information_schema fallback answers.

        Retrying those spends the policy's whole budget on a question one query
        settles; specific wrapped Glue codes stay retryable.

        Args:
            retry_config: The connection's retry policy.

        Returns:
            A new policy without ``_FALLBACK_ERROR_CODES``.
        """
        return _without_retries(retry_config, cls._FALLBACK_ERROR_CODES)

    @staticmethod
    def _internal_cursor(raw_connection: PoolProxiedConnection) -> Any:
        """Open an API cursor for the queries this dialect parses itself.

        Reflection reads these rows directly, so they must not arrive in the
        result format chosen for user queries: a DataFrame cursor reports a NULL
        or blank value as NaN, as an empty string, or as a dropped row depending
        on its backend and on UNLOAD.

        The converter is pinned too: a connection-level one chosen for a
        DataFrame cursor would otherwise be applied to this one.

        The async connection adapter maps ``Cursor`` to its own counterpart and
        returns its wrapper, so this is typed by the interface used here rather
        than by the class requested.
        """
        return raw_connection.driver_connection.cursor(  # type: ignore[union-attr]
            Cursor, converter=Cursor.get_default_converter()
        )

    def _column(self, name: str | None, type_: str, comment: str | None, partition: bool | None):
        return {
            "name": name,
            "type": self._get_column_type(type_),
            "nullable": True,
            "default": None,
            "autoincrement": False,
            # An empty comment is no comment, whichever path reported it.
            "comment": comment or None,
            "dialect_options": {"awsathena_partition": partition},
        }

    def _columns_from_metadata(self, metadata: Any):
        return [self._column(c.name, c.type, c.comment, None) for c in metadata.columns] + [
            self._column(c.name, c.type, c.comment, True) for c in metadata.partition_keys
        ]

    def _columns_from_information_schema(
        self, raw_connection: PoolProxiedConnection, schema: str | None, table_name: str
    ):
        # Athena resolves identifiers case-insensitively and information_schema
        # reports lowercase names; plain equality keeps the filter pushed down.
        # The answer must reflect the catalog now, so query result reuse is off.
        schema = str(schema).lower().replace("'", "''")
        table_name = table_name.lower().replace("'", "''")
        with self._internal_cursor(raw_connection) as cursor:
            cursor.execute(
                "SELECT ordinal_position, column_name, data_type, comment, extra_info "
                "FROM information_schema.columns "
                f"WHERE table_schema = '{schema}' AND table_name = '{table_name}'",
                result_reuse_enable=False,
            )
            rows = cursor.fetchall()
        # Sort here: the query has no ORDER BY, so its result order is Athena's.
        # The comment is still normalized at this boundary: a converter given in
        # cursor_kwargs is applied after the one _internal_cursor() pins, and one
        # written for a DataFrame cursor reports a missing value as NaN.
        return [
            self._column(
                column_name,
                # Athena exposes Hive STRING as unbounded VARCHAR in information_schema.
                "string" if data_type == "varchar" else data_type,
                comment if isinstance(comment, str) else None,
                extra_info == "partition key" or None,
            )
            for _, column_name, data_type, comment, extra_info in sorted(
                rows, key=lambda row: int(row[0])
            )
        ]

    def _get_tables(self, connection, schema: str | None = None, **kw):
        raw_connection = self._raw_connection(connection)
        catalog = self._cursor_option(raw_connection, "catalog_name")
        schema = schema if schema else self._cursor_option(raw_connection, "schema_name")
        info_cache = kw.get("info_cache")
        if info_cache is None:
            info_cache = {}
        cache_key = ("pyathena_table_metadata_list", catalog, schema)
        tables = info_cache.get(cache_key)
        if tables is not None:
            return tables
        with raw_connection.driver_connection.cursor() as cursor:  # type: ignore[union-attr]
            tables = cursor.list_table_metadata(schema_name=schema)
        info_cache[cache_key] = tables
        for metadata in tables:
            if metadata.name is None:
                continue
            name = self._fold_table_name(catalog, metadata.name)
            # Preserve earlier reflection results until Inspector.clear_cache().
            info_cache.setdefault(("pyathena_table_metadata", catalog, schema, name), metadata)
        return tables

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
        # An empty comment is no comment here too; the DDL compiler skips one.
        return {"text": metadata.comment or None}

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

    @reflection.cache
    def has_table(self, connection: Connection, table_name: str, schema: str | None = None, **kw):
        try:
            return bool(self.get_columns(connection, table_name, schema, **kw))
        except exc.NoSuchTableError:
            return False

    @reflection.cache
    def get_view_definition(
        self, connection: Connection, view_name: str, schema: str | None = None, **kw
    ):
        raw_connection = self._raw_connection(connection)
        schema = schema if schema else self._cursor_option(raw_connection, "schema_name")
        query = f"""SHOW CREATE VIEW "{schema}"."{view_name}";"""
        with self._internal_cursor(raw_connection) as cursor:
            try:
                cursor.execute(query)
            except pyathena.error.OperationalError as e:
                # Athena runs SHOW CREATE VIEW for a missing view and fails the
                # query, which the cursor reports without an underlying API
                # error. execute() also fetches the first result page, and a
                # failed API call there carries its error as the cause: that is
                # a failed read of a view that exists, not a missing one. Any
                # query that ends without success is still read as absence, as
                # before; its state is not kept and its error codes do not
                # single out a missing view.
                if e.__cause__ is not None:
                    raise
                raise exc.NoSuchTableError(f"{schema}.{view_name}") from e
            rows = cursor.fetchall()
        # Athena returns the definition one line per row and blank lines as
        # empty values, which are part of the definition.
        return "\n".join(row[0] or "" for row in rows)

    @reflection.cache
    def get_columns(self, connection: Connection, table_name: str, schema: str | None = None, **kw):
        return self._get_columns(connection, table_name, schema=schema, **kw)

    def _get_column_type(self, type_: str, _nested: bool = False):
        type_ = type_.strip()
        match = self._pattern_column_type.match(type_)
        if match:
            name = match.group(1).lower()
            length = match.group(2)
        else:
            name = type_.lower()
            length = None

        if name == "array":
            try:
                return AthenaArray(self._get_column_type(length, _nested=True) if length else None)
            except (TypeError, ValueError):
                util.warn(f"Did not recognize type '{type_}'")
                return types.NullType()
        if _nested and name == "map" and length:
            key, value = _split_type_arguments(length)
            return AthenaMap(
                self._get_column_type(key, _nested=True),
                self._get_column_type(value, _nested=True),
            )
        if _nested and name in ("row", "struct") and length:
            fields = []
            for field in _split_type_arguments(length):
                pattern = (
                    r'\s*("(?:[^"]|"")*"|`(?:[^`]|``)*`|[^:]+)\s*:\s*(.+)'
                    if name == "struct"
                    else r'\s*("(?:[^"]|"")*"|`(?:[^`]|``)*`|[^\s:]+)(?:\s*:\s*|\s+)(.+)'
                )
                match = re.fullmatch(
                    pattern,
                    field,
                )
                if match is None:
                    raise ValueError(f"Invalid ROW field: {field!r}")
                field_name, field_type = match.groups()
                field_name = field_name.strip()
                if field_name[0] in ('"', "`"):
                    quote = field_name[0]
                    field_name = field_name[1:-1].replace(quote * 2, quote)
                fields.append((field_name, self._get_column_type(field_type, _nested=True)))
            return AthenaStruct(*fields)

        if name in self.ischema_names:
            col_type = self.ischema_names[name]
        else:
            util.warn(f"Did not recognize type '{type_}'")
            col_type = types.NullType

        args = []
        if length:
            if col_type is types.DECIMAL:
                args = [int(arg) for arg in length.split(",")]
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
        """Execute a statement with the DB API cursor.

        SQLAlchemy calls this once per page of an "insertmanyvalues" insert.
        For those pages, the execution context's row count accumulates the
        cursor row counts, so that ``CursorResult.rowcount`` reports the total
        like ``Cursor.executemany``, or -1 if any page has an unknown count.

        Args:
            cursor: The DB API cursor.
            statement: The SQL statement.
            parameters: The statement parameters.
            context: The SQLAlchemy execution context, if any.
        """
        on_start_query_execution = None
        if isinstance(context, ExecutionContext):
            execution_options = context.execution_options
            if execution_options is not None:
                on_start_query_execution = execution_options.get("on_start_query_execution")

        if on_start_query_execution is not None:
            cursor.execute(statement, parameters, on_start_query_execution=on_start_query_execution)
        else:
            cursor.execute(statement, parameters)

        # An executemany context reaches do_execute only for insertmanyvalues
        # pages; other executemany statements go through do_executemany.
        if isinstance(context, DefaultExecutionContext) and context.executemany:
            total = context._rowcount
            count = cursor.rowcount
            if total is None:
                context._rowcount = count
            else:
                context._rowcount = total + count if total >= 0 and count >= 0 else -1

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
