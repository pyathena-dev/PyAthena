import textwrap
import uuid
from datetime import date, datetime
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from pyathena import OperationalError
from pyathena.pandas.util import (
    as_pandas,
    generate_ddl,
    get_chunks,
    reset_index,
    to_sql,
)
from tests import ENV


def test_get_chunks():
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    actual1 = get_chunks(df)
    assert [len(a) for a in actual1] == [5]
    actual2 = get_chunks(df, chunksize=2)
    assert [len(a) for a in actual2] == [2, 2, 1]
    actual3 = get_chunks(df, chunksize=10)
    assert [len(a) for a in actual3] == [5]

    # empty
    assert list(get_chunks(pd.DataFrame())) == []

    # invalid
    with pytest.raises(ValueError, match="Chunk size argument must be greater than zero"):
        list(get_chunks(df, chunksize=0))
    with pytest.raises(ValueError, match="Chunk size argument must be greater than zero"):
        list(get_chunks(df, chunksize=-1))


def test_reset_index():
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    reset_index(df)
    assert list(df.columns) == ["index", "a"]

    df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    reset_index(df, index_label="__index__")
    assert list(df.columns) == ["__index__", "a"]

    df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    with pytest.raises(ValueError, match="Duplicate name"):
        reset_index(df, index_label="a")


def test_as_pandas(cursor):
    cursor.execute(
        """
        SELECT
          col_boolean
          , col_tinyint
          , col_smallint
          , col_int
          , col_bigint
          , col_float
          , col_double
          , col_string
          , col_timestamp
          , CAST(col_timestamp AS time) AS col_time
          , col_date
          , col_binary
          , col_array
          , CAST(col_array AS json) AS col_array_json
          , col_map
          , CAST(col_map AS json) AS col_map_json
          , col_struct
          , col_decimal
        FROM one_row_complex
        """
    )
    df = as_pandas(cursor)
    rows = [
        (
            row["col_boolean"],
            row["col_tinyint"],
            row["col_smallint"],
            row["col_int"],
            row["col_bigint"],
            row["col_float"],
            row["col_double"],
            row["col_string"],
            row["col_timestamp"],
            row["col_time"],
            row["col_date"],
            row["col_binary"],
            row["col_array"],
            row["col_array_json"],
            row["col_map"],
            row["col_map_json"],
            row["col_struct"],
            row["col_decimal"],
        )
        for _, row in df.iterrows()
    ]
    expected = [
        (
            True,
            127,
            32767,
            2147483647,
            9223372036854775807,
            0.5,
            0.25,
            "a string",
            datetime(2017, 1, 1, 0, 0, 0),
            datetime(2017, 1, 1, 0, 0, 0).time(),
            date(2017, 1, 2),
            b"123",
            [1, 2],
            [1, 2],
            {"1": "2", "3": "4"},
            {"1": 2, "3": 4},
            {"a": "1", "b": "2"},
            Decimal("0.1"),
        )
    ]
    assert rows == expected


def test_as_pandas_integer_na_values(cursor):
    cursor.execute(
        """
        SELECT * FROM integer_na_values
        """
    )
    df = as_pandas(cursor, coerce_float=True)
    rows = [(row["a"], row["b"]) for _, row in df.iterrows()]
    # TODO AssertionError: Lists differ:
    #  [(1.0, 2.0), (1.0, nan), (nan, nan)] != [(1.0, 2.0), (1.0, nan), (nan, nan)]
    # assert rows == [
    #     (1.0, 2.0),
    #     (1.0, np.nan),
    #     (np.nan, np.nan),
    # ])
    np.testing.assert_array_equal(rows, [(1, 2), (1, np.nan), (np.nan, np.nan)])


def test_as_pandas_boolean_na_values(cursor):
    cursor.execute(
        """
        SELECT * FROM boolean_na_values
        """
    )
    df = as_pandas(cursor)
    rows = [(row["a"], row["b"]) for _, row in df.iterrows()]
    assert rows == [(True, False), (False, None), (None, None)]


def test_generate_ddl():
    df = pd.DataFrame(
        {
            "col_int": np.int32([1]),
            "col_bigint": np.int64([12345]),
            "col_float": np.float32([1.0]),
            "col_double": np.float64([1.2345]),
            "col_string": ["a"],
            "col_boolean": np.bool_([True]),
            "col_timestamp": [datetime(2020, 1, 1, 0, 0, 0)],
            "col_date": [date(2020, 12, 31)],
            "col_timedelta": [np.timedelta64(1, "D")],
            "col_binary": [b"foobar"],
        }
    )
    # Explicitly specify column order
    df = df[
        [
            "col_int",
            "col_bigint",
            "col_float",
            "col_double",
            "col_string",
            "col_boolean",
            "col_timestamp",
            "col_date",
            "col_timedelta",
            "col_binary",
        ]
    ]

    actual = generate_ddl(df, "test_table", "s3://bucket/path/to/", "test_schema")
    assert (
        actual.strip()
        == textwrap.dedent(
            """
        CREATE EXTERNAL TABLE IF NOT EXISTS `test_schema`.`test_table` (
        `col_int` INT,
        `col_bigint` BIGINT,
        `col_float` FLOAT,
        `col_double` DOUBLE,
        `col_string` STRING,
        `col_boolean` BOOLEAN,
        `col_timestamp` TIMESTAMP,
        `col_date` DATE,
        `col_timedelta` BIGINT,
        `col_binary` BINARY
        )
        STORED AS PARQUET
        LOCATION 's3://bucket/path/to/'
        """
        ).strip()
    )

    # compression
    actual = generate_ddl(
        df,
        "test_table",
        "s3://bucket/path/to/",
        "test_schema",
        compression="snappy",
    )
    assert (
        actual.strip()
        == textwrap.dedent(
            """
        CREATE EXTERNAL TABLE IF NOT EXISTS `test_schema`.`test_table` (
        `col_int` INT,
        `col_bigint` BIGINT,
        `col_float` FLOAT,
        `col_double` DOUBLE,
        `col_string` STRING,
        `col_boolean` BOOLEAN,
        `col_timestamp` TIMESTAMP,
        `col_date` DATE,
        `col_timedelta` BIGINT,
        `col_binary` BINARY
        )
        STORED AS PARQUET
        LOCATION 's3://bucket/path/to/'
        TBLPROPERTIES ('parquet.compress'='SNAPPY')
        """
        ).strip()
    )

    # partitions
    actual = generate_ddl(
        df,
        "test_table",
        "s3://bucket/path/to/",
        "test_schema",
        partitions=["col_int"],
    )
    assert (
        actual.strip()
        == textwrap.dedent(
            """
        CREATE EXTERNAL TABLE IF NOT EXISTS `test_schema`.`test_table` (
        `col_bigint` BIGINT,
        `col_float` FLOAT,
        `col_double` DOUBLE,
        `col_string` STRING,
        `col_boolean` BOOLEAN,
        `col_timestamp` TIMESTAMP,
        `col_date` DATE,
        `col_timedelta` BIGINT,
        `col_binary` BINARY
        )
        PARTITIONED BY (
        `col_int` INT
        )
        STORED AS PARQUET
        LOCATION 's3://bucket/path/to/'
        """
        ).strip()
    )

    # multiple partitions
    actual = generate_ddl(
        df,
        "test_table",
        "s3://bucket/path/to/",
        "test_schema",
        partitions=["col_int", "col_string"],
    )
    assert (
        actual.strip()
        == textwrap.dedent(
            """
        CREATE EXTERNAL TABLE IF NOT EXISTS `test_schema`.`test_table` (
        `col_bigint` BIGINT,
        `col_float` FLOAT,
        `col_double` DOUBLE,
        `col_boolean` BOOLEAN,
        `col_timestamp` TIMESTAMP,
        `col_date` DATE,
        `col_timedelta` BIGINT,
        `col_binary` BINARY
        )
        PARTITIONED BY (
        `col_int` INT,
        `col_string` STRING
        )
        STORED AS PARQUET
        LOCATION 's3://bucket/path/to/'
        """
        ).strip()
    )

    # complex
    df = pd.DataFrame({"col_complex": np.complex128([1.0, 2.0, 3.0, 4.0, 5.0])})
    with pytest.raises(ValueError, match="is not supported"):
        generate_ddl(df, "test_table", "s3://bucket/path/to/")

    # time
    df = pd.DataFrame({"col_time": [datetime(2020, 1, 1, 0, 0, 0).time()]}, index=["i"])
    with pytest.raises(ValueError, match="is not supported"):
        generate_ddl(df, "test_table", "s3://bucket/path/to/")


def test_to_sql(cursor):
    df = pd.DataFrame(
        {
            "col_int": np.int32([1]),
            "col_bigint": np.int64([12345]),
            "col_float": np.float32([1.0]),
            "col_double": np.float64([1.2345]),
            "col_string": ["a"],
            "col_boolean": np.bool_([True]),
            "col_timestamp": [datetime(2020, 1, 1, 0, 0, 0)],
            "col_date": [date(2020, 12, 31)],
            "col_binary": b"foobar",
        }
    )
    # Explicitly specify column order
    df = df[
        [
            "col_int",
            "col_bigint",
            "col_float",
            "col_double",
            "col_string",
            "col_boolean",
            "col_timestamp",
            "col_date",
            "col_binary",
        ]
    ]
    table_name = f"""to_sql_{str(uuid.uuid4()).replace("-", "")}"""
    location = f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/"
    to_sql(
        df,
        table_name,
        cursor._connection,
        location,
        schema=ENV.schema,
        if_exists="fail",
        compression="snappy",
    )
    # table already exists
    with pytest.raises(OperationalError):
        to_sql(
            df,
            table_name,
            cursor._connection,
            location,
            schema=ENV.schema,
            if_exists="fail",
            compression="snappy",
        )
    # replace
    to_sql(
        df,
        table_name,
        cursor._connection,
        location,
        schema=ENV.schema,
        if_exists="replace",
        compression="snappy",
    )

    cursor.execute(f"SELECT * FROM {table_name}")
    assert cursor.fetchall() == [
        (
            1,
            12345,
            1.0,
            1.2345,
            "a",
            True,
            datetime(2020, 1, 1, 0, 0, 0),
            date(2020, 12, 31),
            b"foobar",
        )
    ]
    assert [(d[0], d[1]) for d in cursor.description] == [
        ("col_int", "integer"),
        ("col_bigint", "bigint"),
        ("col_float", "float"),
        ("col_double", "double"),
        ("col_string", "varchar"),
        ("col_boolean", "boolean"),
        ("col_timestamp", "timestamp"),
        ("col_date", "date"),
        ("col_binary", "varbinary"),
    ]

    # append
    to_sql(
        df,
        table_name,
        cursor._connection,
        location,
        schema=ENV.schema,
        if_exists="append",
        compression="snappy",
    )
    cursor.execute(f"SELECT * FROM {table_name}")
    assert cursor.fetchall() == [
        (
            1,
            12345,
            1.0,
            1.2345,
            "a",
            True,
            datetime(2020, 1, 1, 0, 0, 0),
            date(2020, 12, 31),
            b"foobar",
        ),
        (
            1,
            12345,
            1.0,
            1.2345,
            "a",
            True,
            datetime(2020, 1, 1, 0, 0, 0),
            date(2020, 12, 31),
            b"foobar",
        ),
    ]


def test_to_sql_with_index(cursor):
    df = pd.DataFrame({"col_int": np.int32([1])})
    table_name = f"""to_sql_{str(uuid.uuid4()).replace("-", "")}"""
    location = f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/"
    to_sql(
        df,
        table_name,
        cursor._connection,
        location,
        schema=ENV.schema,
        if_exists="fail",
        compression="snappy",
        index=True,
        index_label="col_index",
    )
    cursor.execute(f"SELECT * FROM {table_name}")
    assert cursor.fetchall() == [(0, 1)]
    assert [(d[0], d[1]) for d in cursor.description] == [
        ("col_index", "bigint"),
        ("col_int", "integer"),
    ]


def test_to_sql_with_partitions(cursor):
    df = pd.DataFrame(
        {
            "col_int": np.int32(range(10)),
            "col_bigint": np.int64([12345 for _ in range(10)]),
            "col_string": ["a" for _ in range(10)],
        }
    )
    table_name = f"""to_sql_{str(uuid.uuid4()).replace("-", "")}"""
    location = f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/"
    to_sql(
        df,
        table_name,
        cursor._connection,
        location,
        schema=ENV.schema,
        partitions=["col_int"],
        if_exists="fail",
        compression="snappy",
    )
    cursor.execute(f"SHOW PARTITIONS {table_name}")
    assert sorted(cursor.fetchall()) == [(f"col_int={i}",) for i in range(10)]
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    assert cursor.fetchall() == [(10,)]


def test_to_sql_with_multiple_partitions(cursor):
    df = pd.DataFrame(
        {
            "col_int": np.int32(range(10)),
            "col_bigint": np.int64([12345 for _ in range(10)]),
            "col_string": ["a" for _ in range(5)] + ["b" for _ in range(5)],
        }
    )
    table_name = f"""to_sql_{str(uuid.uuid4()).replace("-", "")}"""
    location = f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/"
    to_sql(
        df,
        table_name,
        cursor._connection,
        location,
        schema=ENV.schema,
        partitions=["col_int", "col_string"],
        if_exists="fail",
        compression="snappy",
    )
    cursor.execute(f"SHOW PARTITIONS {table_name}")
    assert sorted(cursor.fetchall()), [(f"col_int={i}/col_string=a",) for i in range(5)] + [
        (f"col_int={i}/col_string=b",) for i in range(5, 10)
    ]
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    assert cursor.fetchall() == [(10,)]


def test_to_sql_invalid_args(cursor):
    df = pd.DataFrame({"col_int": np.int32([1])})
    table_name = f"""to_sql_{str(uuid.uuid4()).replace("-", "")}"""
    location = f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/"
    # invalid if_exists
    with pytest.raises(ValueError, match="is not valid for if_exists"):
        to_sql(
            df,
            table_name,
            cursor._connection,
            location,
            schema=ENV.schema,
            if_exists="foobar",
            compression="snappy",
        )
    # invalid compression
    with pytest.raises(ValueError, match="is not valid for compression"):
        to_sql(
            df,
            table_name,
            cursor._connection,
            location,
            schema=ENV.schema,
            if_exists="fail",
            compression="foobar",
        )

    # invalid partition key (None)
    with pytest.raises(ValueError, match="is None, no data will be written") as exc_info:
        to_sql(
            df,
            table_name,
            cursor._connection,
            location,
            schema=ENV.schema,
            if_exists="fail",
            compression="snappy",
            partitions=[None],
        )
    assert (
        str(exc_info.value)
        == "Partition key: `None` is None, no data will be written to the table."
    )
    # invalid partition key value (None)
    df_with_none = pd.DataFrame({"col_int": np.int32([1]), "partition_key": [None]})
    with pytest.raises(ValueError, match="contains None values") as exc_info:
        to_sql(
            df_with_none,
            table_name,
            cursor._connection,
            location,
            schema=ENV.schema,
            if_exists="fail",
            compression="snappy",
            partitions=["partition_key"],
        )
    assert str(exc_info.value) == (
        "Partition key: `partition_key` contains None values, no data will be written to the table."
    )
