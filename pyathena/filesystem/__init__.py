import logging

import fsspec

from pyathena.filesystem.s3 import S3FileSystem

_logger = logging.getLogger(__name__)


def register_s3_filesystem() -> None:
    """Register PyAthena's S3 filesystem as fsspec's "s3" / "s3a" protocols.

    PyAthena registers its own filesystem so that the pandas/polars result
    sets can read query results from S3 without depending on s3fs. The
    registration replaces fsspec's default lazy mapping of the "s3" protocol
    to s3fs, which means ``fsspec.filesystem("s3")`` returns PyAthena's
    implementation and s3fs-specific settings (e.g., the ``S3FS_LOGGING_LEVEL``
    environment variable) have no effect.

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
