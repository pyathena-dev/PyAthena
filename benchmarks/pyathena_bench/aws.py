# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""AWS preparation and cleanup constrained to a benchmark stack."""

from __future__ import annotations

import json
import re
import time
import uuid
import warnings
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from pyathena_bench.config import Settings, identifier, select_sql, write_json


def session(settings: Settings) -> Any:
    return boto3.Session(profile_name=settings.profile, region_name=settings.region)


def client(session_: Any, service: str) -> Any:
    return session_.client(
        service,
        config=Config(
            connect_timeout=10, read_timeout=60, retries={"mode": "standard", "max_attempts": 5}
        ),
    )


def stack_resources(session_: Any, stack: str) -> dict[str, str]:
    data = client(session_, "cloudformation").describe_stacks(StackName=stack)["Stacks"][0]
    tags = {item["Key"]: item["Value"] for item in data.get("Tags", [])}
    if tags.get("Purpose") != "pyathena-benchmark":
        raise ValueError("Stack must have Purpose=pyathena-benchmark tag")
    result = {item["OutputKey"]: item["OutputValue"] for item in data.get("Outputs", [])}
    required = {
        "Bucket",
        "ScratchDatabase",
        "SourceBucket",
        "SourcePrefix",
        "SourceDatabase",
        "SourceTable",
        "WorkGroup",
        "AutoScalingGroup",
    }
    if not required <= result.keys():
        raise ValueError("Stack outputs are incomplete")
    if (
        result["SourceBucket"] == result["Bucket"]
        or result["ScratchDatabase"] == result["SourceDatabase"]
    ):
        raise ValueError("Scratch resources must differ from source resources")
    identifier(result["ScratchDatabase"])
    result["StackId"] = data["StackId"]
    return result


def preflight(settings: Settings, stack: str) -> dict[str, str]:
    session_ = session(settings)
    resources = stack_resources(session_, stack)
    for key, value in (
        ("SourceDatabase", settings.database),
        ("SourceTable", settings.table),
        ("WorkGroup", settings.workgroup),
    ):
        if resources[key] != value:
            raise ValueError(f"Configuration does not match stack {key}")
    glue = client(session_, "glue")
    table = glue.get_table(DatabaseName=settings.database, Name=settings.table)["Table"]
    expected = f"s3://{resources['SourceBucket']}/{resources['SourcePrefix'].strip('/')}"
    partition = glue.get_partition(
        DatabaseName=settings.database,
        TableName=settings.table,
        PartitionValues=[settings.download_date],
    )["Partition"]
    for descriptor in (table["StorageDescriptor"], partition["StorageDescriptor"]):
        location = descriptor["Location"].rstrip("/")
        if location != expected and not location.startswith(expected + "/"):
            raise ValueError("Source location is outside the stack's read-only grant")
    if table.get("PartitionKeys") != [{"Name": "download_date", "Type": "string"}]:
        raise ValueError("Expected only a download_date string partition")
    workgroup = client(session_, "athena").get_work_group(WorkGroup=settings.workgroup)["WorkGroup"]
    configuration = workgroup["Configuration"]
    if workgroup["State"] != "ENABLED" or configuration.get("EnforceWorkGroupConfiguration"):
        raise ValueError("Workgroup must be enabled and allow a dedicated output location")
    if configuration.get("ManagedQueryResultsConfiguration", {}).get("Enabled"):
        raise ValueError("Benchmarks require S3 query results")
    return resources


def execute_query(
    settings: Settings,
    resources: dict[str, str],
    manifest: dict[str, Any],
    path: Path,
    sql: str,
    output: str,
) -> dict[str, Any]:
    athena = client(session(settings), "athena")
    token = uuid.uuid4().hex
    entry: dict[str, Any] = {"sql": sql, "token": token, "output": output}
    manifest.setdefault("queries", []).append(entry)
    write_json(path, manifest)
    response = athena.start_query_execution(
        QueryString=sql,
        ClientRequestToken=token,
        WorkGroup=settings.workgroup,
        QueryExecutionContext={"Database": resources["ScratchDatabase"]},
        ResultConfiguration={"OutputLocation": output},
        ResultReuseConfiguration={"ResultReuseByAgeConfiguration": {"Enabled": False}},
    )
    query_id = response["QueryExecutionId"]
    entry["query_id"] = query_id
    write_json(path, manifest)
    deadline = time.monotonic() + settings.timeout_seconds
    try:
        while time.monotonic() < deadline:
            response = athena.get_query_execution(QueryExecutionId=query_id)
            state = response["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                # Boto3 timestamps need an explicit serialization boundary.
                entry["execution"] = json.loads(json.dumps(response, default=str))
                write_json(path, manifest)
                return response
            if state in {"FAILED", "CANCELLED"}:
                raise RuntimeError(str(response["QueryExecution"]["Status"]))
            time.sleep(settings.poll_interval)
        raise TimeoutError(f"Query {query_id} timed out")
    except BaseException:
        athena.stop_query_execution(QueryExecutionId=query_id)
        raise


def prepare(settings: Settings, stack: str, path: Path, scales: list[str]) -> dict[str, Any]:
    if path.exists():
        raise ValueError("Manifest already exists; use a new path for a new input snapshot")
    resources = preflight(settings, stack)
    run_id = uuid.uuid4().hex
    manifest: dict[str, Any] = {
        "version": 1,
        "run_id": run_id,
        "region": settings.region,
        "source": {
            "database": settings.database,
            "table": settings.table,
            "download_date": settings.download_date,
        },
        "resources": resources,
        "scales": {},
        "queries": [],
        "fixtures": {},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json(path, manifest)
    root = f"s3://{resources['Bucket']}/runs/{run_id}/"
    for scale in scales:
        count = settings.scales[scale]
        table = f"b_{run_id}_{identifier(scale)}"
        location = f"{root}inputs/{scale}/"
        sql = (
            f'CREATE TABLE "{resources["ScratchDatabase"]}"."{table}" '
            "WITH (format='PARQUET', write_compression='SNAPPY', "
            f"external_location='{location}') AS "
            f'SELECT * FROM "{settings.database}"."{settings.table}" '
            f"WHERE download_date = '{settings.download_date}' LIMIT {count}"
        )
        item: dict[str, Any] = {
            "table": table,
            "rows": count,
            "location": location,
            "sql": sql,
            "ready": False,
        }
        manifest["scales"][scale] = item
        write_json(path, manifest)
        execute_query(settings, resources, manifest, path, sql, root + "prepare/")
        response = execute_query(
            settings,
            resources,
            manifest,
            path,
            f'SELECT count(*) FROM "{resources["ScratchDatabase"]}"."{table}"',
            root + "prepare/",
        )
        rows = client(session(settings), "athena").get_query_results(
            QueryExecutionId=response["QueryExecution"]["QueryExecutionId"]
        )["ResultSet"]["Rows"]
        actual = int(rows[1]["Data"][0]["VarCharValue"])
        if actual != count:
            raise ValueError(f"Expected {count} rows for {scale}, got {actual}")
        item["schema"] = client(session(settings), "glue").get_table(
            DatabaseName=resources["ScratchDatabase"], Name=table
        )["Table"]["StorageDescriptor"]["Columns"]
        item["ready"] = True
        write_json(path, manifest)
    return manifest


def validate_manifest(settings: Settings, manifest: dict[str, Any]) -> dict[str, str]:
    if manifest.get("version") != 1 or manifest.get("region") != settings.region:
        raise ValueError("Manifest version or region mismatch")
    if not re.fullmatch(r"[0-9a-f]{32}", manifest.get("run_id", "")):
        raise ValueError("Invalid run ID")
    expected = manifest["resources"]
    actual = stack_resources(session(settings), expected["StackId"])
    if actual != expected:
        raise ValueError("Stack resources changed since preparation")
    if (
        actual["WorkGroup"] != settings.workgroup
        or actual["SourceDatabase"] != settings.database
        or actual["SourceTable"] != settings.table
    ):
        raise ValueError("Configuration does not match the manifest's stack")
    if manifest.get("source") != {
        "database": settings.database,
        "table": settings.table,
        "download_date": settings.download_date,
    }:
        raise ValueError("Source configuration changed since preparation")
    prefix = f"b_{manifest['run_id']}_"
    for scale, item in manifest["scales"].items():
        if item["table"] != prefix + identifier(scale):
            raise ValueError("Table is outside the manifest namespace")
    return actual


def prepare_fixtures(
    settings: Settings, manifest: dict[str, Any], path: Path, scales: list[str], shape: str
) -> None:
    resources = validate_manifest(settings, manifest)
    validate_inputs(settings, manifest, scales)
    for scale in scales:
        for transport in ("csv", "unload"):
            key = f"{scale}-{shape}-{transport}"
            if key in manifest["fixtures"]:
                continue
            sql = select_sql(
                resources["ScratchDatabase"], manifest["scales"][scale]["table"], shape
            )
            root = (
                f"s3://{resources['Bucket']}/runs/{manifest['run_id']}/fixtures/{uuid.uuid4().hex}/"
            )
            unload = root + "parquet/" if transport == "unload" else None
            if unload:
                sql = f"UNLOAD ({sql}) TO '{unload}' WITH (format='PARQUET', compression='SNAPPY')"
            response = execute_query(settings, resources, manifest, path, sql, root + "output/")
            manifest["fixtures"][key] = {
                "response": json.loads(json.dumps(response, default=str)),
                "unload_location": unload,
            }
            write_json(path, manifest)


def validate_inputs(settings: Settings, manifest: dict[str, Any], scales: list[str]) -> None:
    if manifest.get("cleaned") or any(
        not manifest["scales"].get(scale, {}).get("ready") for scale in scales
    ):
        raise ValueError("Prepare all requested scales before running")
    for scale in scales:
        if manifest["scales"][scale]["rows"] != settings.scales[scale]:
            raise ValueError("Prepared row count differs from current configuration")


def cancel_queries(settings: Settings, query_ids: set[str]) -> list[str]:
    errors = []
    athena = client(session(settings), "athena")
    for query_id in query_ids:
        try:
            state = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"][
                "Status"
            ]["State"]
            if state in {"QUEUED", "RUNNING"}:
                athena.stop_query_execution(QueryExecutionId=query_id)
        except Exception as exc:
            errors.append(f"{query_id}: {exc}")
    return errors


def cleanup(
    settings: Settings, manifest: dict[str, Any], path: Path, trials_only: bool = False
) -> None:
    """Remove this run's data after checking the current stack identity."""
    resources = validate_manifest(settings, manifest)
    session_ = session(settings)
    bucket = resources["Bucket"]
    run_id = manifest["run_id"]
    root = f"runs/{run_id}/"
    prefix = root + "trials/" if trials_only else root
    athena = client(session_, "athena")
    # Discover queries whose worker died before returning its query ID.
    # The query prefix is checked against live Athena output metadata.
    ids: set[str] = set()
    for entry in manifest["queries"]:
        if "query_id" not in entry:
            continue
        try:
            query = athena.get_query_execution(QueryExecutionId=entry["query_id"])["QueryExecution"]
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "InvalidRequestException":
                raise
            # History expires independently of S3 data. Still scan live history below.
            warnings.warn(
                f"Query metadata unavailable for {entry['query_id']}; checking live history",
                stacklevel=2,
            )
            continue
        if query.get("WorkGroup") != settings.workgroup or not query.get(
            "ResultConfiguration", {}
        ).get("OutputLocation", "").startswith(f"s3://{bucket}/{root}"):
            raise ValueError("Manifest contains a query outside this run")
        if query["Status"]["State"] in {"RUNNING", "QUEUED"}:
            ids.add(entry["query_id"])
    for page in athena.get_paginator("list_query_executions").paginate(
        WorkGroup=settings.workgroup
    ):
        query_ids = page["QueryExecutionIds"]
        for offset in range(0, len(query_ids), 50):
            batch = athena.batch_get_query_execution(
                QueryExecutionIds=query_ids[offset : offset + 50]
            )
            if batch.get("UnprocessedQueryExecutionIds"):
                raise RuntimeError(
                    f"Could not inspect query history: {batch['UnprocessedQueryExecutionIds']}"
                )
            for query in batch["QueryExecutions"]:
                if (
                    query.get("WorkGroup") == settings.workgroup
                    and query.get("ResultConfiguration", {})
                    .get("OutputLocation", "")
                    .startswith(f"s3://{bucket}/{root}")
                    and query["Status"]["State"] in {"RUNNING", "QUEUED"}
                ):
                    ids.add(query["QueryExecutionId"])
    errors = cancel_queries(settings, ids)
    if errors:
        raise RuntimeError("Could not cancel queries: " + "; ".join(errors))
    deadline = time.monotonic() + settings.timeout_seconds
    for query_id in ids:
        while athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"][
            "State"
        ] in {"RUNNING", "QUEUED"}:
            if time.monotonic() >= deadline:
                raise TimeoutError("Wait for query cancellation before cleanup")
            time.sleep(settings.poll_interval)
    glue = client(session_, "glue")
    tables: list[str] = []
    input_tables = {item["table"] for item in manifest["scales"].values()}
    for page in glue.get_paginator("get_tables").paginate(
        DatabaseName=resources["ScratchDatabase"]
    ):
        tables.extend(
            t["Name"]
            for t in page["TableList"]
            if t["Name"].startswith(f"b_{run_id}_")
            and (
                not trials_only
                or (
                    re.fullmatch(f"b_{run_id}_[0-9a-f]{{32}}", t["Name"])
                    and t["Name"] not in input_tables
                )
            )
        )
    for table in tables:
        glue.delete_table(DatabaseName=resources["ScratchDatabase"], Name=table)
    s3 = client(session_, "s3")
    for page in s3.get_paginator("list_multipart_uploads").paginate(Bucket=bucket, Prefix=prefix):
        for upload in page.get("Uploads", []):
            s3.abort_multipart_upload(Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"])
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if objects:
            response = s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
            if response.get("Errors"):
                raise RuntimeError(str(response["Errors"]))
    if not trials_only:
        manifest["cleaned"] = True
    write_json(path, manifest)
