# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Batch operations over many S3 objects through PyAthena's filesystems and s3fs."""

from __future__ import annotations

import asyncio
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from botocore.config import Config
from fsspec.asyn import _run_coros_in_chunks, sync

from pyathena import connect
from pyathena.util import parse_output_location
from pyathena_bench.adapters import heartbeat
from pyathena_bench.cases import Case
from pyathena_bench.config import Settings
from pyathena_bench.filesystem import filesystem

# The copies name both directives, so that S3 neither reads the tags nor the
# annotations of the sources, which the generated objects do not have.
COPY_DIRECTIVES = {"TaggingDirective": "REPLACE", "AnnotationDirective": "EXCLUDE"}
# Threads of the untimed preparation and checks of a trial.
SETUP_WORKERS = 32


def object_paths(prefix: str, count: int) -> list[str]:
    """Return the paths of an object set's files.

    Args:
        prefix: The set's ``s3://`` prefix, with a trailing slash.
        count: Number of objects.

    Returns:
        The paths, in key order.
    """
    return [f"{prefix}{i:06d}" for i in range(count)]


def listing_keys(prefix: str, count: int, keys_per_object: int) -> list[str]:
    """Return the paths of an object set's listing keys.

    The keys lie in ``count`` directories of ``keys_per_object`` keys each.

    Args:
        prefix: The set's listing prefix, with a trailing slash.
        count: Number of objects of the set, which is the number of directories.
        keys_per_object: Keys in each directory.

    Returns:
        The paths, in key order.
    """
    return [f"{prefix}{i:06d}/{j:03d}" for i in range(count) for j in range(keys_per_object)]


def batch(fs: Any, case: Case, paths: list[str], targets: list[str], local: Path, size: int) -> Any:
    """Run the case's batch operation in the way its API is used.

    The synchronous filesystem and s3fs run on the calling thread, and the
    asyncio filesystem's coroutines are awaited on the running event loop.
    The ``info`` fan-out, which fsspec has no batch for, runs the synchronous
    filesystem's ``info()`` in a thread pool of the case's concurrency, and
    the coroutines of the other two in batches of it.

    Args:
        fs: The filesystem of the case's API.
        case: Objects case; the transport is the operation.
        paths: The set's object paths, or the paths to delete for ``rm``.
        targets: The paths that a ``put``, ``pipe``, or ``copy`` creates.
        local: Directory of the local files of ``get`` and ``put``.
        size: Object size in bytes, which a ``pipe`` writes.

    Returns:
        A coroutine for the ``aio`` API, or the operation's result.
    """
    n = case.max_workers
    operation = case.transport
    if operation == "info":
        if case.api == "sync":
            with ThreadPoolExecutor(n) as pool:
                return list(pool.map(fs.info, paths))
        coros = [fs._info(path) for path in paths]
        if case.api == "aio":
            return _run_coros_in_chunks(coros, batch_size=n)
        return sync(fs.loop, _run_coros_in_chunks, coros, batch_size=n)
    if operation == "find":
        prefix = paths[0]
        return fs._find(prefix) if case.api == "aio" else fs.find(prefix)
    if operation == "rm":
        # The batch size is not an option of the deletes: PyAthena's and
        # s3fs's send DeleteObjects requests of 1,000 keys.
        return fs._rm(paths) if case.api == "aio" else fs.rm(paths)
    options: dict[str, Any] = {} if case.api == "sync" else {"batch_size": n}
    if operation == "cat":
        return fs._cat(paths, **options) if case.api == "aio" else fs.cat(paths, **options)
    if operation == "get":
        locals_ = [str(local / os.path.basename(path)) for path in paths]
        if case.api == "aio":
            return fs._get(paths, locals_, **options)
        return fs.get(paths, locals_, **options)
    if operation == "put":
        locals_ = [str(local / os.path.basename(path)) for path in targets]
        if case.api == "aio":
            return fs._put(locals_, targets, **options)
        return fs.put(locals_, targets, **options)
    if operation == "pipe":
        values = dict.fromkeys(targets, bytes(size))
        return fs._pipe(values, **options) if case.api == "aio" else fs.pipe(values, **options)
    if operation == "copy":
        options.update(COPY_DIRECTIVES)
        if case.api == "aio":
            return fs._copy(paths, targets, **options)
        return fs.copy(paths, targets, **options)
    raise ValueError(f"Unknown operation {operation}")


def prepare_trial(
    s3: Any, case: Case, source_paths: list[str], targets: list[str], local: Path, size: int
) -> None:
    """Create what the case's operation needs, before the measurement.

    A ``put`` reads sparse local files of the object size; an ``rm`` deletes
    objects that the trial copies from the set with boto3.

    Args:
        s3: boto3 S3 client.
        case: Objects case.
        source_paths: The set's object paths.
        targets: The trial's target paths.
        local: Directory of the trial's local files.
        size: Object size in bytes.
    """
    local.mkdir(parents=True, exist_ok=True)
    if case.transport == "put":
        for target in targets:
            with open(local / os.path.basename(target), "wb") as stream:
                stream.truncate(size)
    elif case.transport == "rm":
        copy_objects(s3, source_paths, targets)


def copy_objects(s3: Any, sources: list[str], targets: list[str]) -> None:
    """Copy objects with boto3 in parallel, outside a measurement.

    Args:
        s3: boto3 S3 client.
        sources: Source paths.
        targets: Target paths, pairwise.
    """

    def copy(pair: tuple[str, str]) -> None:
        source, target = pair
        source_bucket, source_key = parse_output_location(source)
        bucket, key = parse_output_location(target)
        s3.copy_object(
            Bucket=bucket,
            Key=key,
            CopySource={"Bucket": source_bucket, "Key": source_key},
            **COPY_DIRECTIVES,
        )

    with ThreadPoolExecutor(SETUP_WORKERS) as pool:
        list(pool.map(copy, zip(sources, targets, strict=True)))


def list_objects(s3: Any, prefix: str) -> tuple[int, int]:
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


def delete_objects(s3: Any, prefix: str) -> None:
    """Delete the objects below a prefix in requests of 1,000 keys.

    Args:
        s3: boto3 S3 client.
        prefix: ``s3://`` prefix.

    Raises:
        RuntimeError: If a request reports keys it did not delete.
    """
    bucket, key = parse_output_location(prefix)
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key):
        keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if keys:
            response = s3.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
            if response.get("Errors"):
                raise RuntimeError(str(response["Errors"]))


def check_result(
    s3: Any, case: Case, result: Any, count: int, size: int, target_prefix: str, local: Path
) -> dict[str, int]:
    """Check the operation's outcome after the measurement.

    Args:
        s3: boto3 S3 client.
        case: Objects case.
        result: The operation's result.
        count: Expected object count.
        size: Object size in bytes.
        target_prefix: Prefix of the objects that the trial created.
        local: Directory of the trial's local files.

    Returns:
        The ``objects`` counted, and the ``bytes`` of the operations that
        move the objects' bytes.
    """
    operation = case.transport
    if operation == "cat":
        return {"objects": len(result), "bytes": sum(len(v) for v in result.values())}
    if operation == "get":
        files = list(local.iterdir())
        return {"objects": len(files), "bytes": sum(p.stat().st_size for p in files)}
    if operation in {"put", "pipe", "copy"}:
        objects, total = list_objects(s3, target_prefix)
        return {"objects": objects, "bytes": total}
    if operation == "rm":
        objects, _ = list_objects(s3, target_prefix)
        return {"objects": count - objects}
    return {"objects": len(result)}


async def run_objects(
    settings: Settings,
    case: Case,
    session_: Any,
    object_set: dict[str, Any],
    destination: str,
    temp_dir: Path,
) -> dict[str, Any]:
    """Measure one batch operation of an objects case in the running trial process.

    The filesystem comes from :func:`filesystem`. The trial prepares the
    operation's inputs with boto3 and local files, measures the operation, and
    checks and removes what it created, outside the measurement; the objects
    of a failed preparation are removed too, and a removal that fails makes
    the trial an error. The CPU time of the process during the operation is
    recorded as ``cpu_seconds``, also when the operation or its check fails.
    A write of the ``aio`` API that a failed batch leaves running in a thread
    may create an object after the removal; the run's ``cleanup`` removes it.

    Args:
        settings: Configuration with the region.
        case: Objects case.
        session_: boto3 session.
        object_set: The prepared set of the scale: ``files`` and ``listing``
            prefixes, ``count``, ``size``, and ``keys``.
        destination: Prefix of the objects that the trial creates, which it
            deletes after checking them.
        temp_dir: The trial's temporary directory, which its parent process
            measures and removes.

    Returns:
        A trial result with one operation in ``queries``; its status is
        ``count_mismatch`` or ``size_mismatch`` if the outcome differs, and
        ``error`` with the error's text if the preparation, the operation,
        its check, or the removal failed.
    """
    setup_start = time.perf_counter()
    connection = connect(
        session=session_,
        region_name=settings.region,
        work_group=settings.workgroup,
        config=Config(connect_timeout=10, read_timeout=60),
        s3_config=Config(max_pool_connections=case.pool_connections),
    )
    fs = filesystem(settings, case, session_, connection)
    s3 = connection.s3_client
    count, size = object_set["count"], object_set["size"]
    files = object_paths(object_set["files"], count)
    targets = object_paths(destination, count)
    local = temp_dir / "objects"
    paths = files
    if case.transport == "find":
        paths = [object_set["listing"]]
        count = object_set["keys"]
    elif case.transport == "rm":
        paths = targets
    samples: list[float] = []
    stop = asyncio.Event()
    monitor: asyncio.Task[None] | None = None
    # The operation's wall and CPU time, also of one that failed.
    timing: dict[str, float] = {}
    query: dict[str, Any]
    try:
        prepare_trial(s3, case, files, targets, local, size)
        monitor = asyncio.create_task(heartbeat(settings.heartbeat_interval, stop, samples))
        setup_seconds = time.perf_counter() - setup_start
        await asyncio.sleep(0)
        start = time.perf_counter()
        cpu_start = time.process_time()
        try:
            try:
                result = batch(fs, case, paths, targets, local, size)
                if case.api == "aio":
                    result = await result
            finally:
                timing = {
                    "total_seconds": time.perf_counter() - start,
                    "cpu_seconds": time.process_time() - cpu_start,
                }
                # The event loop's lag is sampled during the operation only.
                stop.set()
                await monitor
            outcome = check_result(s3, case, result, count, size, destination, local)
        except Exception as exc:
            query = {"status": "error", "error": f"{type(exc).__name__}: {exc}", **timing}
        else:
            expected_bytes = count * size
            status = "ok"
            if outcome["objects"] != count:
                status = "count_mismatch"
            elif "bytes" in outcome and outcome["bytes"] != expected_bytes:
                status = "size_mismatch"
            query = {
                "status": status,
                **outcome,
                "expected_objects": count,
                "expected_bytes": expected_bytes if "bytes" in outcome else None,
                **timing,
            }
    except Exception as exc:
        # The preparation failed; the removal below takes what it created.
        setup_seconds = time.perf_counter() - setup_start
        query = {"status": "error", "error": f"{type(exc).__name__}: {exc}", "total_seconds": 0.0}
    finally:
        stop.set()
        if monitor is not None:
            await monitor
        try:
            # Removed, so that the trials do not accumulate objects and local files.
            if case.transport in {"put", "pipe", "copy", "rm"}:
                delete_objects(s3, destination)
            shutil.rmtree(local, ignore_errors=True)
        except Exception as exc:
            query["status"] = "error"
            query["error"] = f"Removal failed: {type(exc).__name__}: {exc}"
        finally:
            connection.close()
    return {
        "status": "ok" if query["status"] == "ok" else "error",
        "setup_seconds": setup_seconds,
        "batch_seconds": query["total_seconds"],
        "queries": [query],
        "successful_queries_per_second": (
            1 / query["total_seconds"] if query["status"] == "ok" else 0.0
        ),
        "loop_lag_seconds": samples if case.api == "aio" else None,
    }
