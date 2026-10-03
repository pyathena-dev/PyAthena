<!--
Copyright 2026 The PyAthena authors

Licensed under the MIT License.
See LICENSE or https://opensource.org/licenses/MIT.

SPDX-License-Identifier: MIT
-->

(filesystem)=

# S3 filesystem

PyAthena ships its own [fsspec](https://filesystem-spec.readthedocs.io/en/latest/)-compatible
filesystem implementation for Amazon S3 (`S3FileSystem`), built on boto3, with an API
surface compatible with [s3fs](https://github.com/fsspec/s3fs) for users migrating from it.

The filesystem is used internally by the pandas, Polars, and S3FS result sets to read query results
from S3, and can also be used independently for S3 file operations.

## fsspec registration

Importing `pyathena.pandas` or `pyathena.polars` registers `S3FileSystem` as the fsspec
`s3` / `s3a` protocols via `pyathena.filesystem.register_s3_filesystem`. This replaces
fsspec's default lazy mapping of the `s3` protocol to s3fs, which means
`fsspec.filesystem("s3")` returns PyAthena's implementation and s3fs-specific settings
(such as the `S3FS_LOGGING_LEVEL` environment variable) have no effect.

A filesystem class that has already been registered explicitly is also overwritten,
with a warning log, so that the replacement is diagnosable. To restore another
implementation, re-register it afterwards:

```python
import fsspec
import s3fs

import pyathena.pandas  # Registers PyAthena's S3FileSystem.

fsspec.register_implementation("s3", s3fs.S3FileSystem, clobber=True)
```

## Basic usage

The filesystem can be constructed from a PyAthena connection, or directly with
s3fs-compatible credential arguments:

```python
from pyathena import connect
from pyathena.filesystem.s3 import S3FileSystem

fs = S3FileSystem(connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                           region_name="us-west-2"))

# Or with direct credentials (s3fs-compatible arguments).
fs = S3FileSystem(key="YOUR_ACCESS_KEY", secret="YOUR_SECRET_KEY")

# Or anonymously for public buckets.
fs = S3FileSystem(anon=True)
```

Standard fsspec operations work as expected:

```python
fs.ls("s3://YOUR_S3_BUCKET/path/to/")
fs.find("s3://YOUR_S3_BUCKET/path/to/")
fs.exists("s3://YOUR_S3_BUCKET/path/to/object")
fs.info("s3://YOUR_S3_BUCKET/path/to/object")

with fs.open("s3://YOUR_S3_BUCKET/path/to/object", "rb") as f:
    data = f.read()

fs.pipe("s3://YOUR_S3_BUCKET/path/to/object", b"data")
fs.cat("s3://YOUR_S3_BUCKET/path/to/object")
fs.cp("s3://YOUR_S3_BUCKET/src", "s3://YOUR_S3_BUCKET/dst")
fs.rm("s3://YOUR_S3_BUCKET/path/to/", recursive=True)
```

Writes with `pipe`/`pipe_file` issue a single PutObject request for data up to the
block size (5 MiB by default); larger data is uploaded as a parallel multipart upload
through the buffered file path. Inside an
[fsspec transaction](https://filesystem-spec.readthedocs.io/en/latest/features.html#transactions),
writes are deferred until the transaction commits and are discarded on rollback.

The block size for writing, given by the `block_size` argument of `open` or by the
filesystem's `default_block_size`, must be between 5 MiB and 5 GiB, inclusive, the part
size limits of a multipart upload. Otherwise, `open` raises `ValueError`.

A multipart upload consists of at most 10,000 parts, one per block, so a write with
the default block size can upload up to about 48.8 GiB (10,000 × 5 MiB). To write a
larger object, use a block size of at least its size divided by 10,000, either with
the `block_size` argument of `open` and `pipe` or with the `default_block_size`
argument of `S3FileSystem`. In an append, the parts copied from the existing object
also count toward the limit. A write that would need more parts raises `ValueError`
and aborts its multipart upload. Multipart copies with `cp` use parts large enough
to stay within the limit.

## Error translation

S3 error responses are translated into standard Python exceptions, so filesystem
operations raise natural errors instead of botocore's `ClientError`:

| S3 error | Python exception |
| --- | --- |
| `404` / `NoSuchKey` / `NoSuchBucket` | `FileNotFoundError` |
| `403` / `AccessDenied` | `PermissionError` |
| `BucketAlreadyExists` / `BucketAlreadyOwnedByYou` | `FileExistsError` |
| `RequestTimeout` | `TimeoutError` |
| Others | `OSError` with the matching `errno` |

## Object metadata, tags, and ACLs

```python
# User-defined metadata (x-amz-meta-*).
fs.setxattr("s3://YOUR_S3_BUCKET/path/to/object", attr1="value1")
metadata = fs.metadata("s3://YOUR_S3_BUCKET/path/to/object")
metadata["attr1"]        # User-defined metadata via the mapping interface.
metadata.content_type    # System-defined metadata as typed properties.
fs.getxattr("s3://YOUR_S3_BUCKET/path/to/object", "attr1")

# Object tagging.
fs.put_tags("s3://YOUR_S3_BUCKET/path/to/object", {"tag1": "value1"})
fs.put_tags("s3://YOUR_S3_BUCKET/path/to/object", {"tag2": "value2"}, mode="m")  # Merge.
fs.get_tags("s3://YOUR_S3_BUCKET/path/to/object")

# Canned ACLs.
fs.chmod("s3://YOUR_S3_BUCKET/path/to/object", "bucket-owner-full-control")
fs.chmod("s3://YOUR_S3_BUCKET/path/to/", "private", recursive=True)
```

Note that `setxattr` rewrites the object by copying it onto itself (S3 does not allow
updating the metadata of an existing object in place), which updates its last-modified
time.

## Multipart upload management

Incomplete multipart uploads continue to accrue storage costs until they are completed
or aborted. The filesystem can discover and abort them:

```python
uploads = fs.list_multipart_uploads("s3://YOUR_S3_BUCKET")
for upload in uploads:
    print(upload.key, upload.upload_id, upload.initiated)

# Abort all incomplete uploads under a bucket or key prefix.
fs.clear_multipart_uploads("s3://YOUR_S3_BUCKET/path/to/")
```

## Versioning

With `version_aware=True`, reads pin the object version observed at open time, so a
file handle keeps returning consistent data even if the object is overwritten while
reading. Explicit versions can always be read with the `?versionId=` suffix or the
`version_id` argument.

```python
fs = S3FileSystem(
    connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/", region_name="us-west-2"),
    version_aware=True,
)

with fs.open("s3://YOUR_S3_BUCKET/path/to/object", "rb") as f:
    data = f.read()  # Pinned to the version observed at open time.

# List all versions of the objects under a prefix.
fs.ls("s3://YOUR_S3_BUCKET/path/to/", versions=True, detail=True)

# Typed version information, including delete markers if requested.
versions = fs.object_version_info("s3://YOUR_S3_BUCKET/path/to/object")
for version in versions:
    print(version.version_id, version.is_latest, version.last_modified)
```

Version-aware operations require the `s3:GetObjectVersion` and
`s3:ListBucketVersions` permissions.

## Bucket lifecycle

Bucket creation and deletion are infrastructure-level changes and are disabled by
default: `mkdir`/`makedirs` and `rmdir` raise `PermissionError` when they would
create or delete a bucket. Pass the opt-in flags to enable them:

```python
fs = S3FileSystem(
    connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/", region_name="us-west-2"),
    allow_bucket_creation=True,
    allow_bucket_deletion=True,
)
fs.mkdir("s3://YOUR_NEW_BUCKET")
fs.rmdir("s3://YOUR_NEW_BUCKET")  # The bucket must be empty.
```

Creating a key prefix under an existing bucket requires no operation (S3 has no real
directories below the bucket level) and is always a no-op.

## Async filesystem

`AioS3FileSystem` provides the same functionality on top of fsspec's
`AsyncFileSystem`, dispatching parallel operations through the asyncio event loop.
See {ref}`aio-s3-filesystem` for details.
