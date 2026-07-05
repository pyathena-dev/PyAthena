import logging

import fsspec

_logger = logging.getLogger(__name__)

_S3_FILESYSTEM_CLASS = "pyathena.filesystem.s3.S3FileSystem"


def register_s3_filesystem(clobber: bool = True) -> None:
    """Register PyAthena's S3 filesystem as fsspec's "s3" / "s3a" protocols.

    PyAthena registers its own filesystem so that the pandas/polars result
    sets can read query results from S3 without depending on s3fs. The
    registration replaces fsspec's default lazy mapping of the "s3" protocol
    to s3fs, which means ``fsspec.filesystem("s3")`` returns PyAthena's
    implementation and s3fs-specific settings (e.g., the ``S3FS_LOGGING_LEVEL``
    environment variable) have no effect.

    A filesystem class that has already been registered explicitly (present
    in ``fsspec.registry``) is left untouched, so registering another
    implementation before importing ``pyathena.pandas`` / ``pyathena.polars``
    opts out of the replacement. To restore s3fs afterwards::

        fsspec.register_implementation("s3", s3fs.S3FileSystem, clobber=True)

    Args:
        clobber: Whether to replace an existing lazy registration of the
            protocols.
    """
    for protocol in ("s3", "s3a"):
        if protocol in fsspec.registry:
            # A filesystem class has been registered explicitly
            # (e.g., the user registered s3fs); leave it alone.
            _logger.debug(
                "The %r protocol is already registered in the fsspec registry, skipping.",
                protocol,
            )
            continue
        _logger.debug("Registering %s as the fsspec %r protocol.", _S3_FILESYSTEM_CLASS, protocol)
        fsspec.register_implementation(protocol, _S3_FILESYSTEM_CLASS, clobber=clobber)
