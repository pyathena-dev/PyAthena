# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Tuple, Union, cast

from sqlalchemy import exc, types, util
from sqlalchemy.sql.compiler import (
    DDLCompiler,
    GenericTypeCompiler,
    IdentifierPreparer,
    SQLCompiler,
)
from sqlalchemy.sql.elements import BindParameter
from sqlalchemy.sql.schema import Column

from pyathena.model import (
    AthenaFileFormat,
    AthenaPartitionTransform,
    AthenaRowFormatSerde,
)
from pyathena.sqlalchemy.preparer import AthenaDDLIdentifierPreparer
from pyathena.sqlalchemy.types import AthenaArray, AthenaMap, AthenaStruct

if TYPE_CHECKING:
    from sqlalchemy import (
        Cast,
        CheckConstraint,
        ForeignKeyConstraint,
        PrimaryKeyConstraint,
        Table,
        UniqueConstraint,
    )
    from sqlalchemy.sql.ddl import CreateTable
    from sqlalchemy.sql.functions import Function
    from sqlalchemy.sql.selectable import GenerativeSelect

    from pyathena.sqlalchemy.base import AthenaDialect

    _DialectArgDict = Mapping[str, Any]
    CreateColumn = Any


class AthenaTypeCompiler(GenericTypeCompiler):
    """Type compiler for Amazon Athena SQL types.

    This compiler translates SQLAlchemy type objects into Athena-compatible
    SQL type strings for use in DDL statements. It handles the mapping between
    SQLAlchemy's portable types and Athena's specific type syntax.

    Athena has specific requirements for type names that differ from standard
    SQL. For example, FLOAT maps to REAL in CAST expressions, and various
    string types (TEXT, NCHAR, NVARCHAR) all map to STRING.

    The compiler also supports Athena-specific complex types:
    - STRUCT/ROW: Nested record types with named fields
    - MAP: Key-value pair collections
    - ARRAY: Ordered collections of elements

    See Also:
        AWS Athena Data Types:
        https://docs.aws.amazon.com/athena/latest/ug/data-types.html
    """

    def visit_FLOAT(self, type_: types.Float[Any], **kw: Any) -> str:  # noqa: N802
        return self.visit_REAL(type_, **kw)  # type: ignore[arg-type]

    def visit_REAL(self, type_: types.REAL[Any], **kw: Any) -> str:  # noqa: N802
        return "FLOAT"

    def visit_DOUBLE(self, type_, **kw) -> str:  # noqa: N802
        return "DOUBLE"

    def visit_DOUBLE_PRECISION(self, type_, **kw) -> str:  # noqa: N802
        return "DOUBLE"

    def visit_NUMERIC(self, type_: types.Numeric[Any], **kw: Any) -> str:  # noqa: N802
        return self.visit_DECIMAL(type_, **kw)  # type: ignore[arg-type]

    def visit_DECIMAL(self, type_: types.DECIMAL[Any], **kw: Any) -> str:  # noqa: N802
        if type_.precision is None:
            return "DECIMAL"
        if type_.scale is None:
            return f"DECIMAL({type_.precision})"
        return f"DECIMAL({type_.precision}, {type_.scale})"

    def visit_TINYINT(self, type_: types.Integer, **kw: Any) -> str:  # noqa: N802
        return "TINYINT"

    def visit_INTEGER(self, type_: types.Integer, **kw: Any) -> str:  # noqa: N802
        return "INTEGER"

    def visit_SMALLINT(self, type_: types.SmallInteger, **kw: Any) -> str:  # noqa: N802
        return "SMALLINT"

    def visit_BIGINT(self, type_: types.BigInteger, **kw: Any) -> str:  # noqa: N802
        return "BIGINT"

    def visit_TIMESTAMP(self, type_: types.TIMESTAMP, **kw: Any) -> str:  # noqa: N802
        return "TIMESTAMP"

    def visit_DATETIME(self, type_: types.DateTime, **kw: Any) -> str:  # noqa: N802
        return self.visit_TIMESTAMP(type_, **kw)  # type: ignore[arg-type]

    def visit_DATE(self, type_: types.Date, **kw: Any) -> str:  # noqa: N802
        return "DATE"

    def visit_TIME(self, type_: types.Time, **kw: Any) -> str:  # noqa: N802
        raise exc.CompileError(f"Data type `{type_}` is not supported")

    def visit_CLOB(self, type_: types.CLOB, **kw: Any) -> str:  # noqa: N802
        return self.visit_BINARY(type_, **kw)  # type: ignore[arg-type]

    def visit_NCLOB(self, type_: types.Text, **kw: Any) -> str:  # noqa: N802
        return self.visit_BINARY(type_, **kw)  # type: ignore[arg-type]

    def visit_CHAR(self, type_: types.CHAR, **kw: Any) -> str:  # noqa: N802
        if type_.length:
            return self._render_string_type("CHAR", type_.length, type_.collation)
        return "STRING"

    def visit_NCHAR(self, type_: types.NCHAR, **kw: Any) -> str:  # noqa: N802
        return self.visit_CHAR(type_, **kw)  # type: ignore[arg-type]

    def visit_VARCHAR(self, type_: types.String, **kw: Any) -> str:  # noqa: N802
        if type_.length:
            return self._render_string_type("VARCHAR", type_.length, type_.collation)
        return "STRING"

    def visit_NVARCHAR(self, type_: types.NVARCHAR, **kw: Any) -> str:  # noqa: N802
        return self.visit_VARCHAR(type_, **kw)  # type: ignore[arg-type]

    def visit_TEXT(self, type_: types.Text, **kw: Any) -> str:  # noqa: N802
        return "STRING"

    def visit_BLOB(self, type_: types.LargeBinary, **kw: Any) -> str:  # noqa: N802
        return self.visit_BINARY(type_, **kw)  # type: ignore[arg-type]

    def visit_BINARY(self, type_: types.BINARY, **kw: Any) -> str:  # noqa: N802
        return "BINARY"

    def visit_VARBINARY(self, type_: types.VARBINARY, **kw: Any) -> str:  # noqa: N802
        return self.visit_BINARY(type_, **kw)  # type: ignore[arg-type]

    def visit_BOOLEAN(self, type_: types.Boolean, **kw: Any) -> str:  # noqa: N802
        return "BOOLEAN"

    def visit_JSON(self, type_: types.JSON, **kw: Any) -> str:  # noqa: N802
        return "JSON"

    def visit_string(self, type_, **kw):  # noqa: N802
        return "STRING"

    def visit_unicode(self, type_, **kw):  # noqa: N802
        return "STRING"

    def visit_unicode_text(self, type_, **kw):  # noqa: N802
        return "STRING"

    def visit_null(self, type_, **kw):  # noqa: N802
        return "NULL"

    def visit_tinyint(self, type_, **kw):  # noqa: N802
        return self.visit_TINYINT(type_, **kw)

    def visit_enum(self, type_, **kw):
        return self.visit_string(type_, **kw)

    def visit_struct(self, type_, **kw):  # noqa: N802
        if isinstance(type_, AthenaStruct):
            if type_.fields:
                field_specs = []
                for field_name, field_type in type_.fields.items():
                    field_type_str = self.process(field_type, **kw)
                    field_specs.append(f"{field_name} {field_type_str}")
                return f"ROW({', '.join(field_specs)})"
            return "ROW()"
        return "ROW()"

    def visit_STRUCT(self, type_, **kw):  # noqa: N802
        return self.visit_struct(type_, **kw)

    def visit_map(self, type_, **kw):  # noqa: N802
        if isinstance(type_, AthenaMap):
            key_type_str = self.process(type_.key_type, **kw)
            value_type_str = self.process(type_.value_type, **kw)
            return f"MAP<{key_type_str}, {value_type_str}>"
        return "MAP<STRING, STRING>"

    def visit_MAP(self, type_, **kw):  # noqa: N802
        return self.visit_map(type_, **kw)

    def visit_array(self, type_, **kw):  # noqa: N802
        if isinstance(type_, AthenaArray):
            item_type_str = self.process(type_.item_type, **kw)
            return f"ARRAY<{item_type_str}>"
        return "ARRAY<STRING>"

    def visit_ARRAY(self, type_, **kw):  # noqa: N802
        return self.visit_array(type_, **kw)


class AthenaStatementCompiler(SQLCompiler):
    """SQL statement compiler for Amazon Athena queries.

    This compiler generates Athena-compatible SQL statements from SQLAlchemy
    expression constructs. It handles Athena-specific SQL syntax including:

    - Function name mapping (e.g., char_length -> length)
    - Lambda expressions in functions like filter()
    - CAST expressions with Athena type requirements
    - OFFSET/LIMIT clause ordering (Athena uses OFFSET before LIMIT)
    - Time travel hints (FOR TIMESTAMP AS OF, FOR VERSION AS OF)

    The compiler ensures that generated SQL is compatible with Presto/Trino
    syntax used by Athena engine versions 2 and 3.

    See Also:
        AWS Athena SQL Reference:
        https://docs.aws.amazon.com/athena/latest/ug/ddl-sql-reference.html
    """

    def visit_char_length_func(self, fn: "Function[Any]", **kw: Any) -> str:
        return f"length{self.function_argspec(fn, **kw)}"

    def visit_filter_func(self, fn: "Function[Any]", **kw: Any) -> str:
        """Compile Athena filter() function with lambda expressions.

        Supports syntax: filter(array_expr, lambda_expr)
        Example: filter(ARRAY[1, 2, 3], x -> x > 1)
        """
        if len(fn.clauses.clauses) != 2:
            raise exc.CompileError(
                f"filter() function expects exactly 2 arguments, got {len(fn.clauses.clauses)}"
            )

        array_expr = fn.clauses.clauses[0]
        lambda_expr = fn.clauses.clauses[1]

        # Process the array expression normally
        array_sql = self.process(array_expr, **kw)

        # Process lambda expression - handle string literals as lambda expressions
        if isinstance(lambda_expr, BindParameter) and isinstance(lambda_expr.value, str):
            # Handle string literal lambda expressions like 'x -> x > 0'
            lambda_sql = lambda_expr.value
        else:
            # Process as regular SQL expression
            lambda_sql = self.process(lambda_expr, **kw)

        return f"filter({array_sql}, {lambda_sql})"

    def visit_cast(self, cast: "Cast[Any]", **kwargs):
        if (isinstance(cast.type, types.VARCHAR) and cast.type.length is None) or isinstance(
            cast.type, types.String
        ):
            type_clause = "VARCHAR"
        elif isinstance(cast.type, types.CHAR) and cast.type.length is None:
            type_clause = "CHAR"
        elif isinstance(cast.type, (types.BINARY, types.VARBINARY)):
            type_clause = "VARBINARY"
        elif isinstance(cast.type, (types.FLOAT, types.Float, types.REAL)):
            # https://docs.aws.amazon.com/athena/latest/ug/data-types.html
            # In Athena, use float in DDL statements like CREATE TABLE
            # and real in SQL functions like SELECT CAST.
            type_clause = "REAL"
        else:
            type_clause = cast.typeclause._compiler_dispatch(self, **kwargs)
        return f"CAST({cast.clause._compiler_dispatch(self, **kwargs)} AS {type_clause})"

    def limit_clause(self, select: "GenerativeSelect", **kw):
        text = []
        if select._offset_clause is not None:
            text.append(" OFFSET " + self.process(select._offset_clause, **kw))
        if select._limit_clause is not None:
            text.append(" LIMIT " + self.process(select._limit_clause, **kw))
        return "\n".join(text)

    def get_from_hint_text(self, table, text):
        return text

    def format_from_hint_text(self, sqltext, table, hint, iscrud):
        hint_upper = hint.upper()
        if (
            any(
                [
                    hint_upper.startswith("FOR TIMESTAMP AS OF"),
                    hint_upper.startswith("FOR SYSTEM_TIME AS OF"),
                    hint_upper.startswith("FOR VERSION AS OF"),
                    hint_upper.startswith("FOR SYSTEM_VERSION AS OF"),
                ]
            )
            and "AS" in sqltext
        ):
            _, alias = sqltext.split(" AS ", 1)
            return f"{table.original.fullname} {hint} AS {alias}"

        return f"{sqltext} {hint}"


class AthenaDDLCompiler(DDLCompiler):
    """DDL compiler for Amazon Athena CREATE TABLE and related statements.

    This compiler generates Athena-compatible DDL statements including support
    for Athena-specific table options:

    - External table creation (EXTERNAL keyword for Hive-style tables)
    - Iceberg table creation (managed tables with ACID support)
    - File formats: PARQUET, ORC, TEXTFILE, JSON, AVRO, etc.
    - Row formats with SerDe specifications
    - Compression settings for various file formats
    - Table locations in S3
    - Partitioning (both Hive-style and Iceberg transforms)
    - Bucketing/clustering for optimized queries

    The compiler uses backtick quoting for DDL identifiers (different from
    DML which uses double quotes) and handles Athena's reserved words.

    Example:
        A table created with this compiler might generate::

            CREATE EXTERNAL TABLE IF NOT EXISTS my_schema.my_table (
                id INT,
                name STRING
            )
            PARTITIONED BY (
                dt STRING
            )
            STORED AS PARQUET
            LOCATION 's3://my-bucket/my-table/'
            TBLPROPERTIES ('parquet.compress' = 'SNAPPY')

    See Also:
        AWS Athena CREATE TABLE:
        https://docs.aws.amazon.com/athena/latest/ug/create-table.html
    """

    @property
    def preparer(self) -> IdentifierPreparer:
        return self._preparer

    @preparer.setter
    def preparer(self, value: IdentifierPreparer):
        pass

    def __init__(
        self,
        dialect: "AthenaDialect",
        statement: "CreateTable",
        schema_translate_map: Optional[Dict[Optional[str], Optional[str]]] = None,
        render_schema_translate: bool = False,
        compile_kwargs: Optional[Dict[str, Any]] = None,
    ):
        self._preparer = AthenaDDLIdentifierPreparer(dialect)
        super().__init__(
            dialect=dialect,
            statement=statement,
            render_schema_translate=render_schema_translate,
            schema_translate_map=schema_translate_map,
            compile_kwargs=compile_kwargs or util.immutabledict(),
        )

    def _escape_comment(self, value: str) -> str:
        value = value.replace("\\", "\\\\").replace("'", r"\'")
        # DDL statements raise a KeyError if the placeholders aren't escaped
        if self.dialect.identifier_preparer._double_percents:
            value = value.replace("%", "%%")
        return f"'{value}'"

    def _get_comment_specification(self, comment: str) -> str:
        return f"COMMENT {self._escape_comment(comment)}"

    def _get_bucket_count(
        self, dialect_opts: "_DialectArgDict", connect_opts: Mapping[str, Any]
    ) -> Optional[str]:
        if dialect_opts["bucket_count"]:
            bucket_count = dialect_opts["bucket_count"]
        elif connect_opts:
            bucket_count = connect_opts.get("bucket_count")
        else:
            bucket_count = None
        return cast(str, bucket_count) if bucket_count is not None else None

    def _get_file_format(
        self, dialect_opts: "_DialectArgDict", connect_opts: Mapping[str, Any]
    ) -> Optional[str]:
        if dialect_opts["file_format"]:
            file_format = dialect_opts["file_format"]
        elif connect_opts:
            file_format = connect_opts.get("file_format")
        else:
            file_format = None
        return cast(Optional[str], file_format)

    def _get_file_format_specification(
        self, dialect_opts: "_DialectArgDict", connect_opts: Mapping[str, Any]
    ) -> str:
        file_format = self._get_file_format(dialect_opts, connect_opts)
        text = []
        if file_format:
            text.append(f"STORED AS {file_format}")
        return "\n".join(text)

    def _get_row_format(
        self, dialect_opts: "_DialectArgDict", connect_opts: Mapping[str, Any]
    ) -> Optional[str]:
        if dialect_opts["row_format"]:
            row_format = dialect_opts["row_format"]
        elif connect_opts:
            row_format = connect_opts.get("row_format")
        else:
            row_format = None
        return cast(Optional[str], row_format)

    def _get_row_format_specification(
        self, dialect_opts: "_DialectArgDict", connect_opts: Mapping[str, Any]
    ) -> str:
        row_format = self._get_row_format(dialect_opts, connect_opts)
        text = []
        if row_format:
            text.append(f"ROW FORMAT {row_format}")
        return "\n".join(text)

    def _get_serde_properties(
        self, dialect_opts: "_DialectArgDict", connect_opts: Mapping[str, Any]
    ) -> Optional[Union[str, Dict[str, Any]]]:
        if dialect_opts["serdeproperties"]:
            serde_properties = dialect_opts["serdeproperties"]
        elif connect_opts:
            serde_properties = connect_opts.get("serdeproperties")
        else:
            serde_properties = None
        return cast(Optional[str], serde_properties)

    def _get_serde_properties_specification(
        self, dialect_opts: "_DialectArgDict", connect_opts: Mapping[str, Any]
    ) -> str:
        serde_properties = self._get_serde_properties(dialect_opts, connect_opts)
        text = []
        if serde_properties:
            text.append("WITH SERDEPROPERTIES (")
            if isinstance(serde_properties, dict):
                text.append(",\n".join([f"\t'{k}' = '{v}'" for k, v in serde_properties.items()]))
            else:
                text.append(serde_properties)
            text.append(")")
        return "\n".join(text)

    def _get_table_location(
        self, table: "Table", dialect_opts: "_DialectArgDict", connect_opts: Mapping[str, Any]
    ) -> Optional[str]:
        if dialect_opts["location"]:
            location = cast(str, dialect_opts["location"])
            location += "/" if not location.endswith("/") else ""
        elif connect_opts:
            base_location = (
                cast(str, connect_opts["location"])
                if "location" in connect_opts
                else cast(str, connect_opts.get("s3_staging_dir"))
            )
            schema = table.schema if table.schema else connect_opts["schema_name"]
            location = f"{base_location}{schema}/{table.name}/"
        else:
            location = None
        return location

    def _get_table_location_specification(
        self, table: "Table", dialect_opts: "_DialectArgDict", connect_opts: Mapping[str, Any]
    ) -> str:
        location = self._get_table_location(table, dialect_opts, connect_opts)
        text = []
        if location:
            text.append(f"LOCATION '{location}'")
        else:
            if connect_opts:
                raise exc.CompileError(
                    "`location` or `s3_staging_dir` parameter is required in the connection string"
                )
            raise exc.CompileError(
                "The location of the table should be specified "
                "by the dialect keyword argument `awsathena_location`"
            )
        return "\n".join(text)

    def _get_table_properties(
        self, dialect_opts: "_DialectArgDict", connect_opts: Mapping[str, Any]
    ) -> Optional[Union[Dict[str, str], str]]:
        if dialect_opts["tblproperties"]:
            table_properties = cast(str, dialect_opts["tblproperties"])
        elif connect_opts:
            table_properties = cast(str, connect_opts.get("tblproperties"))
        else:
            table_properties = None
        return table_properties

    def _get_compression(
        self, dialect_opts: "_DialectArgDict", connect_opts: Mapping[str, Any]
    ) -> Optional[str]:
        if dialect_opts["compression"]:
            compression = cast(str, dialect_opts["compression"])
        elif connect_opts:
            compression = cast(str, connect_opts.get("compression"))
        else:
            compression = None
        return compression

    def _get_table_properties_specification(
        self, dialect_opts: "_DialectArgDict", connect_opts: Mapping[str, Any]
    ) -> str:
        properties = self._get_table_properties(dialect_opts, connect_opts)
        if properties:
            if isinstance(properties, dict):
                table_properties = [",\n".join([f"\t'{k}' = '{v}'" for k, v in properties.items()])]
            else:
                table_properties = [properties]
        else:
            table_properties = []

        compression = self._get_compression(dialect_opts, connect_opts)
        if compression:
            file_format = self._get_file_format(dialect_opts, connect_opts)
            row_format = self._get_row_format(dialect_opts, connect_opts)
            if file_format:
                if file_format == AthenaFileFormat.FILE_FORMAT_PARQUET:
                    table_properties.append(f"\t'parquet.compress' = '{compression}'")
                elif file_format == AthenaFileFormat.FILE_FORMAT_ORC:
                    table_properties.append(f"\t'orc.compress' = '{compression}'")
                else:
                    table_properties.append(f"\t'write.compress' = '{compression}'")
            elif row_format:
                if AthenaRowFormatSerde.is_parquet(row_format):
                    table_properties.append(f"\t'parquet.compress' = '{compression}'")
                elif AthenaRowFormatSerde.is_orc(row_format):
                    table_properties.append(f"\t'orc.compress' = '{compression}'")
                else:
                    table_properties.append(f"\t'write.compress' = '{compression}'")

        text = []
        if table_properties:
            text.append("TBLPROPERTIES (")
            text.append(",\n".join(table_properties))
            text.append(")")
        return "\n".join(text)

    def get_column_specification(self, column: "Column[Any]", **kwargs) -> str:
        if type(column.type) in [types.Integer, types.INTEGER, types.INT]:
            # https://docs.aws.amazon.com/athena/latest/ug/create-table.html
            # In Data Definition Language (DDL) queries like CREATE TABLE,
            # use the int keyword to represent an integer
            type_ = "INT"
        else:
            type_ = self.dialect.type_compiler.process(column.type, type_expression=column)
        text = [f"{self.preparer.format_column(column)} {type_}"]
        if column.comment:
            text.append(f"{self._get_comment_specification(column.comment)}")
        return " ".join(text)

    def visit_check_constraint(self, constraint: "CheckConstraint", **kw: Any) -> str:
        return ""

    def visit_column_check_constraint(self, constraint: "CheckConstraint", **kw: Any) -> str:
        return ""

    def visit_foreign_key_constraint(self, constraint: "ForeignKeyConstraint", **kw: Any) -> str:
        return ""

    def visit_primary_key_constraint(self, constraint: "PrimaryKeyConstraint", **kw: Any) -> str:
        return ""

    def visit_unique_constraint(self, constraint: "UniqueConstraint", **kw: Any) -> str:
        return ""

    def _get_connect_option_partitions(self, connect_opts: Mapping[str, Any]) -> List[str]:
        if connect_opts:
            partition = cast(str, connect_opts.get("partition"))
            partitions = partition.split(",") if partition else []
        else:
            partitions = []
        return partitions

    def _get_connect_option_buckets(self, connect_opts: Mapping[str, Any]) -> List[str]:
        if connect_opts:
            bucket = cast(str, connect_opts.get("cluster"))
            buckets = bucket.split(",") if bucket else []
        else:
            buckets = []
        return buckets

    def _prepared_partitions(self, column: Column[Any]):
        # https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html#querying-iceberg-partitioning
        column_dialect_opts = column.dialect_options["awsathena"]
        partition_transform = column_dialect_opts["partition_transform"]

        column_name = self.preparer.format_column(column)
        transform_column = None

        partitions = []

        if partition_transform:
            if AthenaPartitionTransform.is_valid(partition_transform):
                if partition_transform == AthenaPartitionTransform.PARTITION_TRANSFORM_BUCKET:
                    bucket_count = column_dialect_opts["partition_transform_bucket_count"]
                    if bucket_count:
                        transform_column = f"{bucket_count}, {column_name}"
                elif partition_transform == AthenaPartitionTransform.PARTITION_TRANSFORM_TRUNCATE:
                    truncate_length = column_dialect_opts["partition_transform_truncate_length"]
                    if truncate_length:
                        transform_column = f"{truncate_length}, {column_name}"
                else:
                    transform_column = column_name

                if transform_column:
                    partitions.append(f"\t{partition_transform}({transform_column})")
        else:
            partitions.append(f"\t{column_name}")

        return partitions

    def _prepared_columns(
        self,
        table: "Table",
        is_iceberg: bool,
        create_columns: List["CreateColumn"],
        connect_opts: Mapping[str, Any],
    ) -> Tuple[List[str], List[str], List[str]]:
        columns, partitions, buckets = [], [], []
        conn_partitions = self._get_connect_option_partitions(connect_opts)
        conn_buckets = self._get_connect_option_buckets(connect_opts)
        for create_column in create_columns:
            column = create_column.element
            column_dialect_opts = column.dialect_options["awsathena"]
            try:
                processed = self.process(create_column)
                if processed is not None:
                    if (
                        column_dialect_opts["partition"]
                        or column.name in conn_partitions
                        or f"{table.name}.{column.name}" in conn_partitions
                    ):
                        # https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html#querying-iceberg-partitioning
                        if is_iceberg:
                            partitions.extend(self._prepared_partitions(column=column))
                            columns.append(f"\t{processed}")
                        else:
                            partitions.append(f"\t{processed}")
                    else:
                        columns.append(f"\t{processed}")
                    if (
                        column_dialect_opts["cluster"]
                        or column.name in conn_buckets
                        or f"{table.name}.{column.name}" in conn_buckets
                    ):
                        buckets.append(f"\t{self.preparer.format_column(column)}")
            except exc.CompileError as e:
                raise exc.CompileError(
                    f"(in table '{table.description}', column '{column.name}'): {e.args[0]}"
                ) from e
        return columns, partitions, buckets

    def visit_create_table(self, create: "CreateTable", **kwargs) -> str:
        table = create.element
        dialect_opts = table.dialect_options["awsathena"]
        dialect = cast("AthenaDialect", self.dialect)
        connect_opts = dialect._connect_options

        table_properties = self._get_table_properties_specification(
            dialect_opts, connect_opts
        ).lower()
        is_iceberg = False
        if ("table_type" in table_properties) and ("iceberg" in table_properties):
            is_iceberg = True

        # https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html
        text = ["\nCREATE TABLE"] if is_iceberg else ["\nCREATE EXTERNAL TABLE"]

        if create.if_not_exists:
            text.append("IF NOT EXISTS")
        text.append(self.preparer.format_table(table))
        text.append("(")
        text = [" ".join(text)]

        columns, partitions, buckets = self._prepared_columns(
            table, is_iceberg, create.columns, connect_opts
        )
        text.append(",\n".join(columns))
        text.append(")")

        if table.comment:
            text.append(self._get_comment_specification(table.comment))

        if partitions:
            text.append("PARTITIONED BY (")
            text.append(",\n".join(partitions))
            text.append(")")

        bucket_count = self._get_bucket_count(dialect_opts, connect_opts)
        if buckets and bucket_count:
            text.append("CLUSTERED BY (")
            text.append(",\n".join(buckets))
            text.append(f") INTO {bucket_count} BUCKETS")

        text.append(f"{self.post_create_table(table)}\n")
        return "\n".join(text)

    def post_create_table(self, table: "Table") -> str:
        dialect_opts: "_DialectArgDict" = table.dialect_options["awsathena"]
        dialect = cast("AthenaDialect", self.dialect)
        connect_opts = dialect._connect_options
        text = [
            self._get_row_format_specification(dialect_opts, connect_opts),
            self._get_serde_properties_specification(dialect_opts, connect_opts),
            self._get_file_format_specification(dialect_opts, connect_opts),
            self._get_table_location_specification(table, dialect_opts, connect_opts),
            self._get_table_properties_specification(dialect_opts, connect_opts),
        ]
        return "\n".join([t for t in text if t])
