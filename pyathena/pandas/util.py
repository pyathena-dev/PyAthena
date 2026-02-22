from __future__ import annotations

import concurrent
import logging
import textwrap
import uuid
from collections import OrderedDict
from collections.abc import Callable, Iterator
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor
from copy import deepcopy
from multiprocessing import cpu_count
from typing import (
    TYPE_CHECKING,
    Any,
)

from boto3 import Session

from pyathena import OperationalError
from pyathena.model import AthenaCompression
from pyathena.util import RetryConfig, parse_output_location, retry_api_call

if TYPE_CHECKING:
    from pandas import DataFrame, Series

    from pyathena.connection import Connection
    from pyathena.cursor import Cursor

_logger = logging.getLogger(__name__)


def get_chunks(df: DataFrame, chunksize: int | None = None) -> Iterator[DataFrame]:
    """Split a DataFrame into chunks of specified size.

    Args:
        df: The DataFrame to split into chunks.
        chunksize: Number of rows per chunk. If None, yields the entire DataFrame.

    Yields:
        DataFrame chunks of the specified size.

    Raises:
        ValueError: If chunksize is less than or equal to zero.
    """
    rows = len(df)
    if rows == 0:
        return
    if chunksize is None:
        chunksize = rows
    elif chunksize <= 0:
        raise ValueError("Chunk size argument must be greater than zero")

    chunks = int(rows / chunksize) + 1
    for i in range(chunks):
        start_i = i * chunksize
        end_i = min((i + 1) * chunksize, rows)
        if start_i >= end_i:
            break
        yield df[start_i:end_i]


def reset_index(df: DataFrame, index_label: str | None = None) -> None:
    """Reset the DataFrame index and add it as a column.

    Args:
        df: The DataFrame to reset the index on (modified in-place).
        index_label: Name for the index column. Defaults to "index".

    Raises:
        ValueError: If the index name conflicts with existing column names.
    """
    df.index.name = index_label if index_label else "index"
    try:
        df.reset_index(inplace=True)
    except ValueError as e:
        raise ValueError("Duplicate name in index/columns") from e


def as_pandas(cursor: Cursor, coerce_float: bool = False) -> DataFrame:
    """Convert cursor results to a pandas DataFrame.

    Fetches all remaining rows from the cursor and converts them to a
    DataFrame with column names from the cursor description.

    Args:
        cursor: A PyAthena cursor with executed query results.
        coerce_float: If True, attempt to convert non-string columns to float.

    Returns:
        A DataFrame containing the query results, or an empty DataFrame
        if no results are available.
    """
    from pandas import DataFrame

    description = cursor.description
    if not description:
        return DataFrame()
    names = [metadata[0] for metadata in description]
    return DataFrame.from_records(cursor.fetchall(), columns=names, coerce_float=coerce_float)


def to_sql_type_mappings(col: Series) -> str:
    """Map a pandas Series data type to an Athena SQL type.

    Infers the appropriate Athena SQL type based on the pandas Series dtype.
    Used when creating tables from DataFrames.

    Args:
        col: A pandas Series to determine the SQL type for.

    Returns:
        The Athena SQL type name (e.g., "STRING", "BIGINT", "DOUBLE").

    Raises:
        ValueError: If the data type is not supported (complex, time).
    """
    import pandas as pd

    col_type = pd.api.types.infer_dtype(col, skipna=True)
    if col_type == "datetime64" or col_type == "datetime":
        return "TIMESTAMP"
    if col_type == "timedelta":
        return "INT"
    if col_type == "timedelta64":
        return "BIGINT"
    if col_type == "floating":
        if col.dtype == "float32":
            return "FLOAT"
        return "DOUBLE"
    if col_type == "integer":
        if col.dtype == "int32":
            return "INT"
        return "BIGINT"
    if col_type == "boolean":
        return "BOOLEAN"
    if col_type == "date":
        return "DATE"
    if col_type == "bytes":
        return "BINARY"
    if col_type in ["complex", "time"]:
        raise ValueError(f"Data type `{col_type}` is not supported")
    return "STRING"


def to_parquet(
    df: DataFrame,
    bucket_name: str,
    prefix: str,
    retry_config: RetryConfig,
    session_kwargs: dict[str, Any],
    client_kwargs: dict[str, Any],
    compression: str | None = None,
    flavor: str = "spark",
) -> str:
    """Write a DataFrame to S3 as a Parquet file.

    Converts the DataFrame to Apache Arrow format and writes it to S3
    as a Parquet file with a UUID-based filename.

    Args:
        df: The DataFrame to write.
        bucket_name: S3 bucket name.
        prefix: S3 key prefix (path within the bucket).
        retry_config: Configuration for API call retries.
        session_kwargs: Arguments for creating a boto3 Session.
        client_kwargs: Arguments for creating the S3 client.
        compression: Parquet compression codec (e.g., "snappy", "gzip").
        flavor: Parquet flavor for compatibility ("spark" or "hive").

    Returns:
        The S3 URI of the written Parquet file.
    """
    import pyarrow as pa
    from pyarrow import parquet as pq

    session = Session(**session_kwargs)
    client = session.resource("s3", **client_kwargs)
    bucket = client.Bucket(bucket_name)
    table = pa.Table.from_pandas(df)
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression=compression, flavor=flavor)
    response = retry_api_call(
        bucket.put_object,
        config=retry_config,
        Body=buf.getvalue().to_pybytes(),
        Key=prefix + str(uuid.uuid4()),
    )
    return f"s3://{response.bucket_name}/{response.key}"


def to_sql(
    df: DataFrame,
    name: str,
    conn: Connection[Any],
    location: str,
    schema: str = "default",
    index: bool = False,
    index_label: str | None = None,
    partitions: list[str] | None = None,
    chunksize: int | None = None,
    if_exists: str = "fail",
    compression: str | None = None,
    flavor: str = "spark",
    type_mappings: Callable[[Series], str] = to_sql_type_mappings,
    executor_class: type[ThreadPoolExecutor | ProcessPoolExecutor] = ThreadPoolExecutor,
    max_workers: int = (cpu_count() or 1) * 5,
    repair_table=True,
) -> None:
    """Write a DataFrame to an Athena table backed by Parquet files in S3.

    Creates an external Athena table from a DataFrame by writing the data
    as Parquet files to S3 and executing the appropriate DDL statements.
    Supports partitioning, compression, and parallel uploads.

    Args:
        df: The DataFrame to write to Athena.
        name: Name of the table to create.
        conn: PyAthena connection object.
        location: S3 location for the table data (e.g., "s3://bucket/path/").
        schema: Database schema name. Defaults to "default".
        index: If True, include the DataFrame index as a column.
        index_label: Name for the index column if index=True.
        partitions: List of column names to use as partition keys.
        chunksize: Number of rows per Parquet file. None for single file.
        if_exists: Action if table exists: "fail", "replace", or "append".
        compression: Parquet compression codec (e.g., "snappy", "gzip").
        flavor: Parquet flavor for compatibility ("spark" or "hive").
        type_mappings: Function to map pandas types to SQL types.
        executor_class: Executor class for parallel uploads.
        max_workers: Maximum number of parallel upload workers.
        repair_table: If True, run ALTER TABLE ADD PARTITION for partitioned tables.

    Raises:
        ValueError: If if_exists is invalid, compression is unsupported,
            or partition keys contain None values.
        OperationalError: If if_exists="fail" and table already exists.
    """
    if if_exists not in ("fail", "replace", "append"):
        raise ValueError(f"`{if_exists}` is not valid for if_exists")
    if compression is not None and not AthenaCompression.is_valid(compression):
        raise ValueError(f"`{compression}` is not valid for compression")
    if partitions is None:
        partitions = []
    if not location.endswith("/"):
        location += "/"
    for partition_key in partitions:
        if partition_key is None:
            raise ValueError(
                f"Partition key: `{partition_key}` is None, no data will be written to the table."
            )
        if df[partition_key].isnull().any():
            raise ValueError(
                f"Partition key: `{partition_key}` contains None values, "
                "no data will be written to the table."
            )

    bucket_name, key_prefix = parse_output_location(location)
    bucket = conn.session.resource(
        "s3", region_name=conn.region_name, **conn._client_kwargs
    ).Bucket(bucket_name)
    cursor = conn.cursor()

    table = cursor.execute(
        textwrap.dedent(
            f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            AND table_name = '{name}'
            """
        )
    ).fetchall()
    if if_exists == "fail":
        if table:
            raise OperationalError(f"Table `{schema}.{name}` already exists.")
    elif if_exists == "replace" and table:
        cursor.execute(
            textwrap.dedent(
                f"""
                DROP TABLE `{schema}`.`{name}`
                """
            )
        )
        objects = bucket.objects.filter(Prefix=key_prefix)
        if list(objects.limit(1)):
            objects.delete()

    if index:
        reset_index(df, index_label)
    with executor_class(max_workers=max_workers) as e:
        futures: list[concurrent.futures.Future[Any]] = []
        session_kwargs = deepcopy(conn._session_kwargs)
        session_kwargs.update({"profile_name": conn.profile_name})
        client_kwargs = deepcopy(conn._client_kwargs)
        client_kwargs.update({"region_name": conn.region_name})
        partition_prefixes = []
        if partitions:
            for keys, group in df.groupby(by=partitions, observed=True):
                keys = keys if isinstance(keys, tuple) else (keys,)
                group = group.drop(partitions, axis=1)
                partition_prefix = "/".join(
                    [f"{key}={val}" for key, val in zip(partitions, keys, strict=False)]
                )
                partition_condition = ", ".join(
                    [f"`{key}` = '{val}'" for key, val in zip(partitions, keys, strict=False)]
                )
                partition_prefixes.append(
                    (
                        partition_condition,
                        f"{location}{partition_prefix}/",
                    )
                )
                futures.extend(
                    e.submit(
                        to_parquet,
                        chunk,
                        bucket_name,
                        f"{key_prefix}{partition_prefix}/",
                        conn._retry_config,
                        session_kwargs,
                        client_kwargs,
                        compression,
                        flavor,
                    )
                    for chunk in get_chunks(group, chunksize)
                )
        else:
            futures.extend(
                e.submit(
                    to_parquet,
                    chunk,
                    bucket_name,
                    key_prefix,
                    conn._retry_config,
                    session_kwargs,
                    client_kwargs,
                    compression,
                    flavor,
                )
                for chunk in get_chunks(df, chunksize)
            )
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            _logger.info("to_parquet: %s", result)

    ddl = generate_ddl(
        df=df,
        name=name,
        location=location,
        schema=schema,
        partitions=partitions,
        compression=compression,
        type_mappings=type_mappings,
    )
    _logger.info(ddl)
    cursor.execute(ddl)
    if partitions and repair_table:
        for partition in partition_prefixes:
            add_partition = textwrap.dedent(
                f"""
                ALTER TABLE `{schema}`.`{name}`
                ADD IF NOT EXISTS PARTITION ({partition[0]}) LOCATION '{partition[1]}'
                """
            )
            _logger.info(add_partition)
            cursor.execute(add_partition)


def get_column_names_and_types(df: DataFrame, type_mappings) -> OrderedDict[str, str]:
    """Extract column names and their SQL types from a DataFrame.

    Args:
        df: The DataFrame to extract column information from.
        type_mappings: Function to map pandas types to SQL types.

    Returns:
        An OrderedDict mapping column names to their SQL type strings.
    """
    return OrderedDict(
        (str(df.columns[i]), type_mappings(df.iloc[:, i])) for i in range(len(df.columns))
    )


def generate_ddl(
    df: DataFrame,
    name: str,
    location: str,
    schema: str = "default",
    partitions: list[str] | None = None,
    compression: str | None = None,
    type_mappings: Callable[[Series], str] = to_sql_type_mappings,
) -> str:
    """Generate CREATE EXTERNAL TABLE DDL for a DataFrame.

    Creates DDL for an external Athena table with Parquet storage format
    based on the DataFrame's schema.

    Args:
        df: The DataFrame to generate DDL for.
        name: Name of the table to create.
        location: S3 location for the table data.
        schema: Database schema name. Defaults to "default".
        partitions: List of column names to use as partition keys.
        compression: Parquet compression codec for TBLPROPERTIES.
        type_mappings: Function to map pandas types to SQL types.

    Returns:
        The CREATE EXTERNAL TABLE DDL statement as a string.
    """
    if partitions is None:
        partitions = []
    column_names_and_types = get_column_names_and_types(df, type_mappings)
    ddl = f"CREATE EXTERNAL TABLE IF NOT EXISTS `{schema}`.`{name}` (\n"
    ddl += ",\n".join(
        [
            f"`{col}` {type_}"
            for col, type_ in column_names_and_types.items()
            if col not in partitions
        ]
    )
    ddl += "\n)\n"
    if partitions:
        ddl += "PARTITIONED BY (\n"
        ddl += ",\n".join([f"`{p}` {column_names_and_types[p]}" for p in partitions])
        ddl += "\n)\n"
    ddl += "STORED AS PARQUET\n"
    ddl += f"LOCATION '{location}'\n"
    if compression:
        ddl += f"TBLPROPERTIES ('parquet.compress'='{compression.upper()}')\n"
    return ddl
