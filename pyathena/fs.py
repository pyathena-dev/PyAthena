# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""The S3 filesystems of PyAthena connections."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar, overload

from fsspec import AbstractFileSystem

from pyathena.filesystem.s3 import S3FileSystem

if TYPE_CHECKING:
    from pyathena.connection import Connection

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
    shares with its result sets, and uses ``connection.retry_config``. A
    policy with the default ``exceptions`` also retries the transient S3 error
    codes (:attr:`~pyathena.filesystem.s3_core.S3Core.RETRY_ERROR_CODES`).
    Closing the filesystem does not close the client. A filesystem created
    this way cannot be pickled; see :class:`~pyathena.filesystem.s3.S3FileSystem`.

    Example:
        >>> from pyathena import connect
        >>> from pyathena.fs import s3_filesystem
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
