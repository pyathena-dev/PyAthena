# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""S3 transfers through PyAthena's synchronous and asyncio filesystems, and s3fs."""

from __future__ import annotations

import asyncio
import os
import time
from functools import partial
from pathlib import Path
from typing import Any

from botocore.config import Config

from pyathena import connect
from pyathena.filesystem.s3 import S3FileSystem
from pyathena.filesystem.s3_async import AioS3FileSystem
from pyathena.util import parse_output_location
from pyathena_bench.adapters import heartbeat
from pyathena_bench.cases import Case
from pyathena_bench.config import Settings

try:
    import s3fs
except ImportError:  # The s3fs API runs in the s3fs environment; see fleet.API_ENVIRONMENTS.
    s3fs = None  # type: ignore[assignment]


def read_object(fs: Any, path: str, block_size: int) -> int:
    """Read a whole object in one call, as parallel range requests of ``block_size``.

    Args:
        fs: The filesystem.
        path: Object to read.
        block_size: Size of each range request.

    Returns:
        The number of bytes read.
    """
    with fs.open(path, "rb", block_size=block_size) as stream:
        return len(stream.read())


def write_object(fs: Any, path: str, size: int, block_size: int) -> int:
    """Write an object of zero bytes as a multipart upload with parts of ``block_size``.

    Args:
        fs: The filesystem.
        path: Object to write.
        size: Size of the object in bytes.
        block_size: Size of each part, and of the zero bytes written at a time.

    Returns:
        The number of bytes written.
    """
    block = memoryview(bytes(block_size))
    with fs.open(path, "wb", block_size=block_size) as stream:
        remaining = size
        while remaining:
            remaining -= stream.write(block[: min(remaining, block_size)])
    return size


def get_object(fs: Any, path: str, local: Path, **options: Any) -> int:
    """Download an object to a local file with ``get_file()``.

    Args:
        fs: The filesystem.
        path: Object to download.
        local: Local file to write.
        **options: The filesystem's options of the download.

    Returns:
        The size of the local file in bytes.
    """
    fs.get_file(path, str(local), **options)
    return os.path.getsize(local)


def put_object(fs: Any, path: str, local: Path, size: int, **options: Any) -> int:
    """Upload a local file of ``size`` zero bytes with ``put_file()``.

    The local file is sparse, so that no disk read bounds the upload.

    Args:
        fs: The filesystem.
        path: Object to write.
        local: Local file to create and upload.
        size: Size of the file in bytes.
        **options: The filesystem's options of the upload.

    Returns:
        The number of bytes uploaded.
    """
    with open(local, "wb") as stream:
        stream.truncate(size)
    fs.put_file(str(local), path, **options)
    return size


def copy_options(case: Case, block_size: int) -> dict[str, Any]:
    """Return the ``cp_file()`` options of a case.

    Args:
        case: Filesystem case.
        block_size: Size of each copied part.

    Returns:
        Keyword arguments of the API's ``cp_file()``.
    """
    if case.api == "s3fs":
        # s3fs copies the parts one at a time, whatever its max_concurrency.
        return {"block": block_size}
    # The generated objects have no tags or annotations, so neither is copied:
    # copying them would read them first, which the benchmark role cannot.
    return {
        "max_workers": case.max_workers,
        "block_size": block_size,
        "TaggingDirective": "REPLACE",
        "AnnotationDirective": "EXCLUDE",
    }


async def transfer(
    fs: Any, case: Case, source: str, destination: str, size: int, block_size: int, local: Path
) -> int:
    """Run the case's operation in the way its API is used.

    The synchronous filesystem and s3fs run on the calling thread; s3fs waits
    for each operation, a coroutine on fsspec's IO loop. As ``AioS3FSCursor``
    reads result files, the asyncio filesystem's file and local-file transfers
    run in ``asyncio.to_thread()``: its files dispatch their range requests
    and part uploads through ``S3AioExecutor`` to the filesystem's event loop,
    and its ``get_file()`` and ``put_file()`` run the synchronous filesystem's
    in the filesystem's thread pool. Its copy is a coroutine awaited on the
    trial's event loop.

    Args:
        fs: ``S3FileSystem`` for the ``sync`` API, ``AioS3FileSystem`` for
            ``aio``, and ``s3fs.S3FileSystem`` for ``s3fs``.
        case: Filesystem case; the transport is the operation, ``read``,
            ``get``, ``write``, ``put``, or ``copy``.
        source: Object to read, download, or copy.
        destination: Object to write, upload, or copy to.
        size: Size of the object in bytes.
        block_size: Size of each range request, part, or copied part.
        local: Local file of a ``get`` or ``put``, in a directory of the trial.

    Returns:
        The number of bytes transferred.
    """
    if case.transport == "copy":
        options = copy_options(case, block_size)
        if case.api == "aio":
            await fs._cp_file(source, destination, **options)
        else:
            fs.cp_file(source, destination, **options)
        return size
    # s3fs names the size of its parts and range requests chunksize.
    file_options = {"chunksize" if case.api == "s3fs" else "block_size": block_size}
    operations = {
        "read": partial(read_object, fs, source, block_size),
        "get": partial(get_object, fs, source, local, **file_options),
        "write": partial(write_object, fs, destination, size, block_size),
        "put": partial(put_object, fs, destination, local, size, **file_options),
    }
    operation = operations[case.transport]
    if case.api == "aio":
        return await asyncio.to_thread(operation)
    return operation()


def missing_api_reason(case: Case) -> str | None:
    """Return why the case's API cannot run in this environment.

    Args:
        case: Benchmark case.

    Returns:
        The reason, or None if the API's library is installed.
    """
    if case.api == "s3fs" and s3fs is None:
        return "s3fs is not installed in this environment"
    return None


def filesystem(settings: Settings, case: Case, session_: Any, connection: Any) -> Any:
    """Create the filesystem of a case, with its S3 client.

    The PyAthena filesystems share the connection's S3 client, whose
    connection pool has the case's size. s3fs creates its own client from the
    session's credentials, with a connection pool of the same size, and takes
    the case's ``max_workers`` as its ``max_concurrency``, the number of
    parallel range requests of ``get_file()`` and parts of ``put_file()``.
    Its client is connected here, as the PyAthena filesystems take the
    connection's client when they are created, so that no transfer measures
    the creation of a client.

    Args:
        settings: Configuration with the region.
        case: Filesystem case.
        session_: boto3 session.
        connection: PyAthena connection of the trial.

    Returns:
        The filesystem.

    Raises:
        ValueError: If the case has no max_workers or connection pool size.
        RuntimeError: If the ``s3fs`` API runs where s3fs is not installed.
    """
    if case.max_workers is None or case.pool_connections is None:
        raise ValueError("A filesystem case needs max_workers and pool_connections")
    if case.api == "s3fs":
        if s3fs is None:
            raise RuntimeError("The s3fs API needs the s3fs environment; see the README")
        credentials = session_.get_credentials().get_frozen_credentials()
        fs = s3fs.S3FileSystem(
            key=credentials.access_key,
            secret=credentials.secret_key,
            token=credentials.token,
            client_kwargs={"region_name": settings.region},
            config_kwargs={
                "connect_timeout": 10,
                "read_timeout": 60,
                "max_pool_connections": case.pool_connections,
            },
            max_concurrency=case.max_workers,
            skip_instance_cache=True,
        )
        fs.connect()
        return fs
    filesystem_class = AioS3FileSystem if case.api == "aio" else S3FileSystem
    return filesystem_class(
        connection=connection, max_workers=case.max_workers, skip_instance_cache=True
    )


async def run_filesystem(
    settings: Settings,
    case: Case,
    session_: Any,
    source: str,
    destination: str,
    size: int,
    temp_dir: Path,
) -> dict[str, Any]:
    """Measure one transfer of a filesystem case in the running trial process.

    The event loop keeps Python's default executor, so ``asyncio.to_thread()``
    runs with its default number of threads. The filesystem comes from
    :func:`filesystem`; the connection's S3 client checks the written, uploaded,
    or copied object with every API. A ``get`` or ``put`` goes through the
    local file ``object`` in ``temp_dir``, removed after the measurement, once
    the event loop's lag is no longer sampled.

    Args:
        settings: Configuration with the block size and region.
        case: Filesystem case.
        session_: boto3 session.
        source: Prepared object of ``size`` bytes, read, downloaded, or copied.
        destination: Object that a write, upload, or copy creates and the
            trial deletes after checking its size.
        size: Size of the object in bytes.
        temp_dir: The trial's temporary directory, which its parent process
            measures and removes.

    Returns:
        A trial result with one transfer in ``queries``; its status is
        ``size_mismatch`` if the transferred or resulting size differs.

    Raises:
        ValueError: If the case has no max_workers or connection pool size.
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
    local = temp_dir / "object"
    samples: list[float] = []
    stop = asyncio.Event()
    monitor = asyncio.create_task(heartbeat(settings.heartbeat_interval, stop, samples))
    setup_seconds = time.perf_counter() - setup_start
    query: dict[str, Any]
    try:
        await asyncio.sleep(0)
        start = time.perf_counter()
        try:
            try:
                transferred = await transfer(
                    fs, case, source, destination, size, settings.filesystem_block_size, local
                )
                elapsed = time.perf_counter() - start
            finally:
                # The event loop's lag is sampled during the transfer only.
                stop.set()
                await monitor
                local.unlink(missing_ok=True)
            # Checked after the measurement: the written, uploaded, or copied object
            # is complete.
            if case.transport not in {"read", "get"}:
                bucket, key = parse_output_location(destination)
                transferred = connection.s3_client.head_object(Bucket=bucket, Key=key)[
                    "ContentLength"
                ]
                # Removed, so that the trials do not accumulate copies of the object.
                connection.s3_client.delete_object(Bucket=bucket, Key=key)
        except Exception as exc:
            query = {
                "status": "error",
                "error": f"{type(exc).__name__}: {exc}",
                "total_seconds": time.perf_counter() - start,
            }
        else:
            query = {
                "status": "ok" if transferred == size else "size_mismatch",
                "bytes": transferred,
                "expected_bytes": size,
                "total_seconds": elapsed,
            }
    finally:
        stop.set()
        await monitor
        connection.close()
    return {
        "status": "ok" if query["status"] == "ok" else "error",
        "setup_seconds": setup_seconds,
        "batch_seconds": query["total_seconds"],
        "queries": [query],
        "successful_queries_per_second": (query["status"] == "ok") / query["total_seconds"],
        "loop_lag_seconds": samples if case.api == "aio" else None,
    }
