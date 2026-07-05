import logging

import fsspec
import pytest
from fsspec.registry import _registry, known_implementations

from pyathena.filesystem import register_s3_filesystem
from pyathena.filesystem.s3 import S3FileSystem


class _DummyFileSystem:
    pass


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

    with caplog.at_level(logging.WARNING, logger="pyathena.filesystem"):
        register_s3_filesystem()
    # An explicitly registered filesystem class is overwritten with a warning.
    assert fsspec.registry["s3"] is S3FileSystem
    assert "will be overwritten" in caplog.text

    # Re-registering PyAthena's own filesystem does not warn.
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="pyathena.filesystem"):
        register_s3_filesystem()
    assert not caplog.text
