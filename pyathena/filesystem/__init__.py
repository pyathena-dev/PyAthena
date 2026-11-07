# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""fsspec filesystem implementations for Amazon S3."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, TypeVar, overload

import fsspec
from fsspec import AbstractFileSystem

from pyathena.filesystem.s3 import S3FileSystem

if TYPE_CHECKING:
    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)

_FileSystemT = TypeVar("_FileSystemT", bound=AbstractFileSystem)


@overload
def s3_filesystem(connection: Connection[Any], **kwargs: Any) -> S3FileSystem: ...


@overload
def s3_filesystem(
    connection: Connection[Any], filesystem_class: type[_FileSystemT], **kwargs: Any
) -> _FileSystemT: ...


def s3_filesystem(
    connection: Connection[Any],
    filesystem_class: type[AbstractFileSystem] = S3FileSystem,
    **kwargs: Any,
) -> AbstractFileSystem:
    """Create an S3 filesystem with the S3 client and retry policy of a connection.

    The filesystem borrows ``connection.s3_client``, which the connection
    shares with its result sets, and uses ``connection.retry_config``.
    Closing the filesystem does not close the client. A filesystem created
    this way cannot be pickled; see :class:`~pyathena.filesystem.s3.S3FileSystem`.

    Example:
        >>> from pyathena import connect
        >>> from pyathena.filesystem import s3_filesystem
        >>> fs = s3_filesystem(connect(s3_staging_dir="s3://my-bucket/path/"))

    Args:
        connection: The PyAthena connection.
        filesystem_class: ``S3FileSystem``, ``AioS3FileSystem``, or a subclass of
            either.
        **kwargs: The other arguments of the filesystem, except the arguments
            from which it builds a client.

    Returns:
        The filesystem.
    """
    return filesystem_class(
        client=connection.s3_client, retry_config=connection.retry_config, **kwargs
    )


def register_s3_filesystem() -> None:
    """Register PyAthena's S3 filesystem as fsspec's "s3" / "s3a" protocols.

    PyAthena registers its own filesystem so that the pandas/polars result
    sets can read query results from S3 without depending on s3fs. The
    registration replaces fsspec's default lazy mapping of the "s3" protocol
    to s3fs, which means ``fsspec.filesystem("s3")`` returns PyAthena's
    implementation and s3fs-specific settings (e.g., the ``S3FS_LOGGING_LEVEL``
    environment variable) have no effect. The registered class is synchronous
    (``async_impl`` is False): like fsspec's other synchronous filesystems, it
    accepts and ignores ``asynchronous=True``. ``AioS3FileSystem`` is the
    asynchronous filesystem.

    A filesystem class that has already been registered explicitly is also
    overwritten, with a warning log. To restore another implementation,
    re-register it after importing ``pyathena.pandas`` / ``pyathena.polars``::

        fsspec.register_implementation("s3", s3fs.S3FileSystem, clobber=True)
    """
    for protocol in ("s3", "s3a"):
        registered = fsspec.registry.get(protocol)
        if registered is not None and registered is not S3FileSystem:
            _logger.warning(
                f"The fsspec {protocol!r} protocol is already registered as "
                f"{registered.__module__}.{registered.__qualname__} and will be overwritten by "
                f"{S3FileSystem.__module__}.{S3FileSystem.__qualname__}."
            )
        _logger.debug(f"Registering {S3FileSystem} as the fsspec {protocol!r} protocol.")
        fsspec.register_implementation(protocol, S3FileSystem, clobber=True)
