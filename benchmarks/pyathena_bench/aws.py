# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""AWS preparation and cleanup constrained to a benchmark stack."""

from __future__ import annotations

import io
import json
import re
import time
import uuid
import warnings
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from pyathena.util import parse_output_location
from pyathena_bench.config import Settings, identifier, select_sql, write_json


def session(settings: Settings) -> Any:
    return boto3.Session(profile_name=settings.profile, region_name=settings.region)


def client(session_: Any, service: str, max_retries: int = 5) -> Any:
    """Create a client that retries in botocore's standard mode.

    Args:
        session_: Session that creates the client.
        service: Service name.
        max_retries: Retries per request after its first attempt, which
            botocore's ``max_attempts`` setting counts; each waits with jittered
            exponential backoff of up to 20 seconds.

    Returns:
        The client.
    """
    return session_.client(
        service,
        config=Config(
            connect_timeout=10,
            read_timeout=60,
            retries={"mode": "standard", "max_attempts": max_retries},
        ),
    )


def split_location(location: str) -> tuple[str, str]:
    """Split an ``s3://bucket/prefix/`` location.

    Args:
        location: S3 location ending with a slash.

    Returns:
        The bucket and the key prefix, which is empty or ends with a slash.

    Raises:
        ValueError: If the location is not an S3 location ending with a slash.
    """
    if not re.fullmatch(r"s3://[a-z0-9][a-z0-9.-]+[a-z0-9]/([^/]+/)*", location):
        raise ValueError(f"Invalid S3 location: {location!r}")
    bucket, _, prefix = location[len("s3://") :].partition("/")
    return bucket, prefix


def run_locations(resources: dict[str, str], run_id: str) -> dict[str, str]:
    """Return the S3 prefixes of a prepared run.

    Args:
        resources: Stack resources from :func:`stack_resources`.
        run_id: Run ID of the manifest.

    Returns:
        ``scratch``, below which inputs and fixtures are, and ``results``, below
        which trial outputs are (``trials/<trial>/``); both ``s3://`` prefixes
        ending with a slash, the same when the results location is the scratch
        bucket.
    """
    return {
        "scratch": f"s3://{resources['Bucket']}/runs/{run_id}/",
        "results": f"{resources['Results']}runs/{run_id}/",
    }


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
        "Results",
    }
    if not required <= result.keys():
        raise ValueError("Stack outputs are incomplete")
    if (
        result["SourceBucket"] == result["Bucket"]
        or result["ScratchDatabase"] == result["SourceDatabase"]
    ):
        raise ValueError("Scratch resources must differ from source resources")
    identifier(result["ScratchDatabase"])
    if split_location(result["Results"])[0] == result["SourceBucket"]:
        raise ValueError("Results must not be written to the source bucket")
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
    objects = run_locations(actual, manifest["run_id"])["scratch"] + "objects/"
    for scale, item in manifest.get("objects", {}).items():
        if not item["location"].startswith(f"{objects}{identifier(scale)}-"):
            raise ValueError("Object is outside the manifest namespace")
    sets = run_locations(actual, manifest["run_id"])["scratch"] + "object-sets/"
    for scale, item in manifest.get("object_sets", {}).items():
        root = f"{sets}{identifier(scale)}-"
        if not (item["files"].startswith(root) and item["listing"].startswith(root)):
            raise ValueError("Object set is outside the manifest namespace")
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
                resources["ScratchDatabase"], manifest["scales"][scale]["table"], shape, transport
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


class ZeroReader(io.RawIOBase):
    """A readable stream of a fixed number of zero bytes."""

    def __init__(self, size: int) -> None:
        """Initialize the stream.

        Args:
            size: Number of bytes to read before the end of the stream.
        """
        self.remaining = size

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        """Fill the start of a buffer with zero bytes.

        Args:
            buffer: Writable buffer.

        Returns:
            The number of bytes filled, 0 at the end of the stream.
        """
        view = memoryview(buffer).cast("B")
        count = min(len(view), self.remaining)
        view[:count] = bytes(count)
        self.remaining -= count
        return count


def prepare_objects(
    settings: Settings, manifest: dict[str, Any], path: Path, scales: list[str]
) -> None:
    """Upload the filesystem suite's source objects that the manifest lacks.

    Each object holds the configured number of zero bytes; S3 stores and
    transfers the bytes uncompressed. It is uploaded with boto3, not with the
    measured filesystems, below the run's scratch prefix, which ``cleanup``
    removes, and recorded under ``objects`` in the manifest.

    Args:
        settings: Configuration with the filesystem sizes.
        manifest: Prepared manifest, updated in place.
        path: Manifest file, rewritten after each upload.
        scales: Filesystem scale names.

    Raises:
        ValueError: If the manifest was cleaned.
    """
    resources = validate_manifest(settings, manifest)
    if manifest.get("cleaned"):
        raise ValueError("The manifest's resources were cleaned")
    objects = manifest.setdefault("objects", {})
    root = run_locations(resources, manifest["run_id"])["scratch"]
    s3 = client(session(settings), "s3")
    for scale in scales:
        if scale in objects:
            continue
        size = settings.filesystem_sizes[scale]
        location = f"{root}objects/{identifier(scale)}-{uuid.uuid4().hex}"
        bucket, key = parse_output_location(location)
        s3.upload_fileobj(ZeroReader(size), bucket, key)
        if s3.head_object(Bucket=bucket, Key=key)["ContentLength"] != size:
            raise ValueError(f"Uploaded object for {scale} has an unexpected size")
        objects[scale] = {"location": location, "size": size}
        write_json(path, manifest)


def prepare_object_sets(
    settings: Settings, manifest: dict[str, Any], path: Path, scales: list[str]
) -> None:
    """Upload the objects suite's object sets that the manifest lacks.

    A set has ``count`` objects of ``size`` zero bytes under its ``files``
    prefix, the first uploaded and the rest copied from it, and ``count``
    directories of ``listing_keys_per_object`` empty objects under its
    ``listing`` prefix. The uploads run with boto3 in parallel threads, below
    the run's scratch prefix, which ``cleanup`` removes, and the set is
    recorded under ``object_sets`` in the manifest once its objects are
    counted.

    Args:
        settings: Configuration with the objects suite's counts and sizes.
        manifest: Prepared manifest, updated in place.
        path: Manifest file, rewritten after each set.
        scales: Objects scale names.

    Raises:
        ValueError: If the manifest was cleaned, or an uploaded set does not
            hold the expected objects.
    """
    resources = validate_manifest(settings, manifest)
    if manifest.get("cleaned"):
        raise ValueError("The manifest's resources were cleaned")
    sets = manifest.setdefault("object_sets", {})
    root = run_locations(resources, manifest["run_id"])["scratch"]
    s3 = client(session(settings), "s3")
    for scale in scales:
        if scale in sets:
            continue
        count = settings.objects_counts[scale]
        size = settings.objects_size
        keys = count * settings.objects_listing_keys_per_object
        location = f"{root}object-sets/{identifier(scale)}-{uuid.uuid4().hex}/"
        files, listing = f"{location}files/", f"{location}listing/"
        bucket, first = parse_output_location(f"{files}{0:06d}")
        s3.upload_fileobj(ZeroReader(size), bucket, first)

        def copy(key: str, bucket: str = bucket, first: str = first) -> None:
            s3.copy_object(
                Bucket=bucket,
                Key=key,
                CopySource={"Bucket": bucket, "Key": first},
                TaggingDirective="REPLACE",
                AnnotationDirective="EXCLUDE",
            )

        def put(key: str, bucket: str = bucket) -> None:
            s3.put_object(Bucket=bucket, Key=key, Body=b"")

        with ThreadPoolExecutor(32) as pool:
            list(
                pool.map(
                    copy, (parse_output_location(f"{files}{i:06d}")[1] for i in range(1, count))
                )
            )
            list(
                pool.map(
                    put,
                    (
                        parse_output_location(f"{listing}{i:06d}/{j:03d}")[1]
                        for i in range(count)
                        for j in range(settings.objects_listing_keys_per_object)
                    ),
                )
            )
        if count_objects(s3, files) != (count, count * size):
            raise ValueError(f"Uploaded files of {scale} have an unexpected count or size")
        if count_objects(s3, listing) != (keys, 0):
            raise ValueError(f"Uploaded listing keys of {scale} have an unexpected count")
        sets[scale] = {
            "files": files,
            "listing": listing,
            "count": count,
            "size": size,
            "keys": keys,
        }
        write_json(path, manifest)


def count_objects(s3: Any, prefix: str) -> tuple[int, int]:
    """Count the objects below a prefix and sum their sizes.

    Args:
        s3: boto3 S3 client.
        prefix: ``s3://`` prefix.

    Returns:
        The object count and the total size in bytes.
    """
    bucket, key = parse_output_location(prefix)
    count = total = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key):
        for item in page.get("Contents", []):
            count += 1
            total += item["Size"]
    return count, total


def validate_object_sets(settings: Settings, manifest: dict[str, Any], scales: list[str]) -> None:
    """Check that the objects suite's sets are prepared as configured.

    Args:
        settings: Configuration with the objects suite's counts and sizes.
        manifest: Manifest with the prepared ``object_sets``.
        scales: Objects scale names.

    Raises:
        ValueError: If the manifest was cleaned, a set is missing, or its
            count, size, or keys differ from the configuration.
    """
    sets = manifest.get("object_sets", {})
    if manifest.get("cleaned") or any(scale not in sets for scale in scales):
        raise ValueError("Prepare all requested object sets before running")
    for scale in scales:
        expected = {
            "count": settings.objects_counts[scale],
            "size": settings.objects_size,
            "keys": settings.objects_counts[scale] * settings.objects_listing_keys_per_object,
        }
        if {k: sets[scale][k] for k in expected} != expected:
            raise ValueError("Prepared object set differs from current configuration")


def validate_objects(settings: Settings, manifest: dict[str, Any], scales: list[str]) -> None:
    """Check that the filesystem suite's source objects are prepared as configured.

    Args:
        settings: Configuration with the filesystem sizes.
        manifest: Manifest with the prepared ``objects``.
        scales: Filesystem scale names.

    Raises:
        ValueError: If the manifest was cleaned, an object is missing, or its
            size differs from the configuration.
    """
    objects = manifest.get("objects", {})
    if manifest.get("cleaned") or any(scale not in objects for scale in scales):
        raise ValueError("Prepare all requested filesystem objects before running")
    if any(objects[scale]["size"] != settings.filesystem_sizes[scale] for scale in scales):
        raise ValueError("Prepared object size differs from current configuration")


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


def active_queries(
    athena: Any, workgroup: str, prefixes: tuple[str, ...], ids: set[str] | None = None
) -> set[str]:
    """Find queued or running queries whose output location starts with a prefix.

    This scans the workgroup's live history, so it also finds queries whose
    IDs were never reported by the process that started them.

    Args:
        athena: Athena client.
        workgroup: Workgroup whose history is scanned.
        prefixes: S3 output location prefixes to match.
        ids: Set that receives each match as it is found, so that the matches
            found before an error remain available; a new set by default.

    Returns:
        IDs of matching queries that are still queued or running.

    Raises:
        RuntimeError: If part of the history cannot be inspected; this is
            raised after the rest of the history was scanned, or instead of
            the error of a later request.
    """
    if ids is None:
        ids = set()
    unprocessed: list[dict[str, Any]] = []
    try:
        for page in athena.get_paginator("list_query_executions").paginate(WorkGroup=workgroup):
            query_ids = page["QueryExecutionIds"]
            for offset in range(0, len(query_ids), 50):
                batch = athena.batch_get_query_execution(
                    QueryExecutionIds=query_ids[offset : offset + 50]
                )
                unprocessed += batch.get("UnprocessedQueryExecutionIds", [])
                for query in batch.get("QueryExecutions", []):
                    if (
                        query.get("WorkGroup") == workgroup
                        and query.get("ResultConfiguration", {})
                        .get("OutputLocation", "")
                        .startswith(prefixes)
                        and query["Status"]["State"] in {"RUNNING", "QUEUED"}
                    ):
                        ids.add(query["QueryExecutionId"])
    except Exception as exc:
        if not unprocessed:
            raise
        # A later error, such as throttling, must not hide the uninspected IDs.
        raise RuntimeError(
            f"Could not inspect query history: {unprocessed}; then {type(exc).__name__}: {exc}"
        ) from exc
    if unprocessed:
        raise RuntimeError(f"Could not inspect query history: {unprocessed}")
    return ids


def cleanup(
    settings: Settings, manifest: dict[str, Any], path: Path, trials_only: bool = False
) -> None:
    """Remove this run's data after checking the current stack identity.

    The run's objects are under ``runs/<run_id>/`` in the scratch bucket
    (inputs and fixtures) and in the stack's results location (trial outputs);
    both are cleaned.
    """
    resources = validate_manifest(settings, manifest)
    session_ = session(settings)
    run_id = manifest["run_id"]
    roots = tuple(sorted(set(run_locations(resources, run_id).values())))
    locations = [split_location(root) for root in roots]
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
        ).get("OutputLocation", "").startswith(roots):
            raise ValueError("Manifest contains a query outside this run")
        if query["Status"]["State"] in {"RUNNING", "QUEUED"}:
            ids.add(entry["query_id"])
    ids |= active_queries(athena, settings.workgroup, roots)
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
    for bucket, root in locations:
        prefix = root + "trials/" if trials_only else root
        for page in s3.get_paginator("list_multipart_uploads").paginate(
            Bucket=bucket, Prefix=prefix
        ):
            for upload in page.get("Uploads", []):
                s3.abort_multipart_upload(
                    Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"]
                )
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if objects:
                response = s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
                if response.get("Errors"):
                    raise RuntimeError(str(response["Errors"]))
    if not trials_only:
        manifest["cleaned"] = True
    write_json(path, manifest)
