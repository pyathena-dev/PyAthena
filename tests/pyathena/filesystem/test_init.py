# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import logging
from types import SimpleNamespace

import boto3
import fsspec
import pytest
from fsspec.registry import _registry, known_implementations

from pyathena.filesystem import register_s3_filesystem, s3_filesystem
from pyathena.filesystem.s3 import S3FileSystem
from pyathena.filesystem.s3_retry import S3RetryConfig


class _DummyFileSystem:
    pass


def _warnings(caplog):
    """Return the captured records of level ``WARNING`` or higher.

    Args:
        caplog: The pytest caplog fixture.

    Returns:
        The records, of any logger.
    """
    return [record for record in caplog.records if record.levelno >= logging.WARNING]


@pytest.fixture
def registry_state():
    registry = dict(_registry)
    known = {k: dict(v) for k, v in known_implementations.items()}
    yield
    _registry.clear()
    _registry.update(registry)
    known_implementations.clear()
    known_implementations.update(known)


def test_register_s3_filesystem(registry_state):
    _registry.pop("s3", None)
    _registry.pop("s3a", None)

    register_s3_filesystem()
    assert fsspec.registry["s3"] is S3FileSystem
    assert fsspec.registry["s3a"] is S3FileSystem


def test_register_s3_filesystem_overwrites_existing_registration(registry_state, caplog):
    fsspec.register_implementation("s3", _DummyFileSystem, clobber=True)
    # Only the s3 registration is replaced.
    fsspec.register_implementation("s3a", S3FileSystem, clobber=True)

    with caplog.at_level(logging.WARNING, logger="pyathena.filesystem"):
        register_s3_filesystem()
    # An explicitly registered filesystem class is overwritten with a warning.
    assert fsspec.registry["s3"] is S3FileSystem
    assert [(r.name, r.levelno) for r in _warnings(caplog)] == [
        ("pyathena.filesystem", logging.WARNING)
    ]

    # Re-registering PyAthena's own filesystem does not warn.
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="pyathena.filesystem"):
        register_s3_filesystem()
    assert not _warnings(caplog)


def test_s3_filesystem_is_deprecated():
    # GH-1388: the function moved to pyathena.fs; the old name warns at its
    # caller and builds the same filesystem from the connection.
    client = boto3.client(
        "s3", region_name="us-east-1", aws_access_key_id="dummy", aws_secret_access_key="dummy"
    )
    connection = SimpleNamespace(s3_client=client, retry_config=S3RetryConfig(attempt=3))
    with pytest.warns(DeprecationWarning, match=r"Use pyathena\.fs\.s3_filesystem") as record:
        fs = s3_filesystem(connection, max_workers=2)
    with fs:
        assert record[0].filename == __file__
        assert fs.core.client is client
        assert fs.core.retry_config.attempt == 3
        assert fs.max_workers == 2
