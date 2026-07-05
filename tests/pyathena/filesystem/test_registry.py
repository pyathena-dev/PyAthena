import fsspec
import pytest
from fsspec.registry import _registry, known_implementations

from pyathena.filesystem import register_s3_filesystem
from pyathena.filesystem.s3 import S3FileSystem


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
    known_implementations["s3"] = {"class": "s3fs.S3FileSystem", "err": "Install s3fs"}
    known_implementations.pop("s3a", None)

    register_s3_filesystem()
    assert known_implementations["s3"]["class"] == "pyathena.filesystem.s3.S3FileSystem"
    assert known_implementations["s3a"]["class"] == "pyathena.filesystem.s3.S3FileSystem"


def test_register_s3_filesystem_respects_explicit_registration(registry_state):
    # A filesystem class registered explicitly in the live fsspec registry
    # (e.g., the user registered s3fs) is left untouched.
    fsspec.register_implementation("s3", S3FileSystem, clobber=True)
    known_implementations["s3"] = {"class": "s3fs.S3FileSystem", "err": "Install s3fs"}

    register_s3_filesystem()
    assert known_implementations["s3"]["class"] == "s3fs.S3FileSystem"
    assert fsspec.registry["s3"] is S3FileSystem
