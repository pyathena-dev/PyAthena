# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
"""The shared tables and views the PyAthena test session creates.

Each table is defined once here. The session setup generates its DDL and its
data file from the definition.
"""

import csv
import io
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

import pyarrow as pa
import pyarrow.parquet as pq

_TEXT_FORMAT = (
    "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' STORED AS TEXTFILE"
)


@dataclass(frozen=True)
class Column:
    """A table column.

    Attributes:
        name: The column name.
        athena_type: The column type in Athena DDL.
        arrow_type: The Arrow type of the column in a Parquet data file.
        comment: The column comment.
    """

    name: str
    athena_type: str
    arrow_type: pa.DataType
    comment: str | None = None

    def ddl(self) -> str:
        """Return the column definition for ``CREATE TABLE``.

        Returns:
            The column name, type, and comment.
        """
        comment = f" COMMENT '{self.comment}'" if self.comment else ""
        return f"{self.name} {self.athena_type}{comment}"


@dataclass(frozen=True)
class Table:
    """A table with its data.

    Attributes:
        name: The table name.
        columns: The data columns.
        rows: The rows, as tuples of Python values in column order.
        storage: ``"parquet"`` or ``"text"`` (tab-separated values).
        partitions: The partition columns. The session adds no partitions.
        comment: The table comment.
        tblproperties: The table properties.
    """

    name: str
    columns: tuple[Column, ...]
    rows: tuple[tuple[Any, ...], ...] = ()
    storage: Literal["parquet", "text"] = "parquet"
    partitions: tuple[Column, ...] = ()
    comment: str | None = None
    tblproperties: tuple[tuple[str, str], ...] = ()

    def create_statement(self, schema: str, location: str) -> str:
        """Return the ``CREATE EXTERNAL TABLE`` statement.

        Args:
            schema: The schema to create the table in.
            location: The S3 location of the table data, ending with a slash.

        Returns:
            The statement.
        """
        columns = ",\n    ".join(c.ddl() for c in self.columns)
        clauses = [f"CREATE EXTERNAL TABLE {schema}.{self.name} (\n    {columns}\n)"]
        if self.comment:
            clauses.append(f"COMMENT '{self.comment}'")
        if self.partitions:
            clauses.append(f"PARTITIONED BY ({', '.join(c.ddl() for c in self.partitions)})")
        clauses.append(_TEXT_FORMAT if self.storage == "text" else "STORED AS PARQUET")
        clauses.append(f"LOCATION '{location}'")
        if self.tblproperties:
            properties = ", ".join(f"'{k}'='{v}'" for k, v in self.tblproperties)
            clauses.append(f"TBLPROPERTIES ({properties})")
        return "\n".join(clauses)

    def data_file(self) -> tuple[str, bytes] | None:
        """Return the table's data file.

        Returns:
            The file name and content, or None for a table without rows.
        """
        if not self.rows:
            return None
        if self.storage == "text":
            lines = ["\t".join(_to_text(v) for v in row) for row in self.rows]
            return "data.tsv", "".join(f"{line}\n" for line in lines).encode()
        schema = pa.schema([(c.name, c.arrow_type) for c in self.columns])
        table = pa.Table.from_pylist(
            [dict(zip(schema.names, row, strict=True)) for row in self.rows], schema=schema
        )
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        return "data.parquet", buffer.getvalue()


@dataclass(frozen=True)
class View:
    """A view.

    Attributes:
        name: The view name.
        query: The view query; ``{schema}`` is replaced with the schema name.
    """

    name: str
    query: str

    def create_statement(self, schema: str) -> str:
        """Return the ``CREATE VIEW`` statement.

        Args:
            schema: The schema to create the view in.

        Returns:
            The statement.
        """
        return f"CREATE VIEW {schema}.{self.name} AS {self.query.format(schema=schema)}"


def _to_text(value: Any) -> str:
    """Format a scalar value for a tab-separated text file.

    Args:
        value: The value.

    Returns:
        The text; an empty string for None.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


TABLES = (
    # A text table: the reflection tests assert its SerDe and delimiters.
    Table(
        "one_row",
        (Column("number_of_rows", "INT", pa.int32(), comment="some comment"),),
        rows=((1,),),
        storage="text",
        comment="table comment",
    ),
    # A text table: tests read it without ORDER BY and expect the file order,
    # which Athena does not keep for a Parquet file of this size.
    Table(
        "many_rows",
        (Column("a", "INT", pa.int32()),),
        rows=tuple((i,) for i in range(10000)),
        storage="text",
    ),
    Table(
        "one_row_complex",
        (
            Column("col_boolean", "BOOLEAN", pa.bool_()),
            Column("col_tinyint", "TINYINT", pa.int8()),
            Column("col_smallint", "SMALLINT", pa.int16()),
            Column("col_int", "INT", pa.int32()),
            Column("col_bigint", "BIGINT", pa.int64()),
            Column("col_float", "FLOAT", pa.float32()),
            Column("col_double", "DOUBLE", pa.float64()),
            Column("col_string", "STRING", pa.string()),
            Column("col_varchar", "VARCHAR(10)", pa.string()),
            Column("col_timestamp", "TIMESTAMP", pa.timestamp("ms")),
            Column("col_date", "DATE", pa.date32()),
            Column("col_binary", "BINARY", pa.binary()),
            Column("col_array", "ARRAY<int>", pa.list_(pa.int32())),
            Column("col_map", "MAP<int, int>", pa.map_(pa.int32(), pa.int32())),
            Column(
                "col_struct",
                "STRUCT<a: int, b: int>",
                pa.struct([("a", pa.int32()), ("b", pa.int32())]),
            ),
            Column("col_decimal", "DECIMAL(10,1)", pa.decimal128(10, 1)),
        ),
        rows=(
            (
                True,
                127,
                32767,
                2147483647,
                9223372036854775807,
                0.5,
                0.25,
                "a string",
                "varchar",
                datetime(2017, 1, 1, 0, 0, 0),
                date(2017, 1, 2),
                b"123",
                [1, 2],
                [(1, 2), (3, 4)],
                {"a": 1, "b": 2},
                Decimal("0.1"),
            ),
        ),
    ),
    Table(
        "partition_table",
        (Column("a", "STRING", pa.string()),),
        partitions=(Column("b", "INT", pa.int32()),),
    ),
    Table(
        "integer_na_values",
        (Column("a", "INT", pa.int32()), Column("b", "INT", pa.int32())),
        rows=((1, 2), (1, None), (None, None)),
    ),
    Table(
        "boolean_na_values",
        (Column("a", "BOOLEAN", pa.bool_()), Column("b", "BOOLEAN", pa.bool_())),
        rows=((True, False), (False, None), (None, None)),
    ),
    Table(
        "parquet_with_compression",
        (Column("a", "INT", pa.int32()),),
        tblproperties=(("parquet.compression", "SNAPPY"),),
    ),
)

VIEWS = (
    View("view_one_row", "SELECT * FROM {schema}.one_row"),
    View("v_one_row", "SELECT number_of_rows FROM {schema}.one_row"),
)

# The Spark tests read this CSV file from ``<schema>/spark_group_by/``.
SPARK_GROUP_BY = (("name", "count"), ("foo", 1), ("bar", 2), ("bar", 3), ("foo", 4))


def spark_group_by_csv() -> bytes:
    """Return ``SPARK_GROUP_BY`` as a CSV file with a header row.

    Returns:
        The file content.
    """
    buffer = io.StringIO()
    csv.writer(buffer, lineterminator="\n").writerows(SPARK_GROUP_BY)
    return buffer.getvalue().encode()
