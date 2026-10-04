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

The filesystem can be constructed from a PyAthena connection, whose S3 client it
then uses (see "S3 client" in [Usage](usage.md)), or directly with
s3fs-compatible credential arguments:

```python
from pyathena import connect
from pyathena.filesystem.s3 import S3FileSystem

fs = S3FileSystem(connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                           region_name="us-west-2"))

# Or with direct credentials (s3fs-compatible arguments).
fs = S3FileSystem(key="YOUR_ACCESS_KEY", secret="YOUR_SECRET_KEY")

# Or with a named profile.
fs = S3FileSystem(profile="YOUR_PROFILE")

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
With the `compression` argument (a codec of fsspec, or `"infer"` from the extension of
the path), `pipe`/`pipe_file` compress the data before uploading it, and the sizes in
this section apply to the compressed data.

The block size for writing, given by the `block_size` argument of `open` or by the
filesystem's `default_block_size`, must be between 5 MiB and 5 GiB, inclusive, the part
size limits of a multipart upload. Otherwise, `open` raises `ValueError`.

A multipart upload consists of at most 10,000 parts. `put` and `pipe` upload one part
per block, so with the default block size they can upload up to about 48.8 GiB
(10,000 × 5 MiB). To upload a larger object, use a block size of at least its size
divided by 10,000, either with the `block_size` argument of `put`, `pipe`, and `open`
or with the `default_block_size` argument of `S3FileSystem`. `put` and `pipe` check
the size before uploading anything and raise `ValueError` with the minimum block size
if the data needs more parts. A file written with `open` can take more parts, because
each write that fills the buffer uploads the data beyond its last full block as a
separate part when that data is at least 5 MiB. In an append, the parts copied from
the existing object also count toward the limit. A write with `open` that reaches the
limit raises `ValueError` and aborts its multipart upload. Multipart copies with `cp`
use parts large enough to stay within the limit.

`cp` copies an object larger than 5 GiB with a multipart upload instead of a single
CopyObject request, with the same result as CopyObject. If HeadObject reports a size
of at most 5 GiB when the copy starts, for example because the size that `cp` found
came from a cached listing, the object is copied with CopyObject instead. CopyObject parameters given as
keyword arguments are sent to the multipart requests that accept them, such as
`CopySourceIfMatch` to each part copy. With the default `COPY` value of
`MetadataDirective`, `TaggingDirective`, and `AnnotationDirective`, the content headers
(such as `ContentType`) and user-defined metadata, the tags, and the annotations of the
source are copied, and the values given for them are ignored, as CopyObject does. A
`REPLACE` directive uses the given values instead, and `AnnotationDirective="EXCLUDE"`
skips the annotations. Copying the tags needs `s3:GetObjectTagging` on the source, and
copying the annotations needs `s3:ListObjectAnnotations` and `s3:GetObjectAnnotation` on
the source and `s3:PutObjectAnnotation` on the destination. A source without a
`?versionId=` suffix whose HeadObject reports a version ID other than `null` is copied
from that version, which needs `s3:GetObjectVersion` and, to copy the tags,
`s3:GetObjectVersionTagging` on the source. A `null` version is not pinned. The annotations are listed before anything is written and copied after the
upload completes, so the destination exists without them until the last one is
written. If an annotation fails to copy, the error is raised and the destination is
kept. A failed part copy aborts the multipart upload. So does an interrupt, or the
cancellation of an `AioS3FileSystem` copy, unless the upload has already completed. A
CreateMultipartUpload request and the part copies in flight finish first, and so does
the CompleteMultipartUpload request of an `AioS3FileSystem` copy.

Paths are normalized as in fsspec, which drops a trailing slash, so `info`, `isfile`,
and `open` treat `s3://YOUR_S3_BUCKET/dir/` as `s3://YOUR_S3_BUCKET/dir`: the object
`dir` if it exists, and otherwise the directory `dir`. An object whose key ends in a
slash, such as a folder marker, is therefore not a file for these methods. Opening
`dir/` for reading reads the object `dir` or raises `FileNotFoundError`. Opening it for
writing, `pipe`, `pipe_file`, and `put_file` write the object `dir`. A path with a
`?versionId=` suffix keeps the slash and refers to the object. `find`, and `ls` of the directory, list the object as a file
entry. `cat_file` uses the key as written. Without a `?versionId=` suffix, it reads
such an object without a range, with a non-empty range of non-negative offsets, or with
a negative `start` and no `end`, and raises `FileNotFoundError` for other ranges.

S3 request parameters, such as `ContentType`, `ServerSideEncryption`, or `RequestPayer`,
can be given to `open`, `pipe`, and `put` as keyword arguments or in
`s3_additional_kwargs`, and to all of them through the `s3_additional_kwargs` argument
of `S3FileSystem`. The parameters of a call take precedence over those of the
filesystem. A file sends each of its requests only the parameters that the S3 operation
accepts, so, for example, `ServerSideEncryption` for writes is not sent with reads. A
`pipe` of data up to the block size sends its parameters with a single PutObject
request as given. `put` sets `ContentType` from the file extension unless the call or
the filesystem gives one.

```python
fs = S3FileSystem(s3_additional_kwargs={"ServerSideEncryption": "AES256"})
with fs.open("s3://YOUR_S3_BUCKET/path/to/data.csv", "wb", ContentType="text/csv") as f:
    f.write(b"col1\n1\n")
```

`info` and `exists` accept `ExpectedBucketOwner`, `RequestPayer`, and the
`SSECustomer*` parameters of an object encrypted with a customer-provided key (SSE-C)
as keyword arguments, and ignore other request parameters. The lookups of the object
by `open`, and the existence check of `pipe` with `mode="create"`, send these
parameters of the file or the write. A lookup with them uses only the cached results of
lookups with the same values, not cached listings.

```python
sse_c = {"SSECustomerAlgorithm": "AES256", "SSECustomerKey": YOUR_32_BYTE_KEY}
with fs.open("s3://YOUR_S3_BUCKET/path/to/encrypted.csv", "rb", **sse_c) as f:
    data = f.read()
```

## Error translation

S3 error responses are translated into standard Python exceptions, so filesystem
operations raise natural errors instead of botocore's `ClientError`:

| S3 error | Python exception |
| --- | --- |
| `404` / `NoSuchKey` / `NoSuchBucket` | `FileNotFoundError` |
| `403` / `AccessDenied` | `PermissionError` |
| `BucketAlreadyExists` / `BucketAlreadyOwnedByYou` | `FileExistsError` |
| `PreconditionFailed` of an `If-None-Match` condition | `FileExistsError` |
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
time. The copy keeps the system-defined metadata (such as `ContentType` and
`CacheControl`), the storage class, and the server-side encryption algorithm and KMS
key of the object; parameters given in `copy_kwargs` take precedence over them, and any
encryption parameter replaces all of the kept encryption settings. HeadObject does not
return the KMS encryption context, so pass it in `copy_kwargs` together with the other
encryption parameters. An `Expires` value that botocore cannot parse as a date is not
kept. A path with a `?versionId=` suffix raises `ValueError`, since the metadata of an
existing version cannot be changed.

## Multipart upload management

Incomplete multipart uploads continue to accrue storage costs until they are completed
or aborted. The filesystem can discover and abort them:

```python
uploads = fs.list_multipart_uploads("s3://YOUR_S3_BUCKET")
for upload in uploads:
    print(upload.key, upload.upload_id, upload.initiated)

# Abort all incomplete uploads to a key and the keys under it.
fs.clear_multipart_uploads("s3://YOUR_S3_BUCKET/path/to/")
```

## Versioning

With `version_aware=True`, reads pin the object version observed at open time, so a
file handle keeps returning consistent data even if the object is overwritten while
reading. The file path carries the pinned version as a `?versionId=` suffix, which
`metadata()`, `getxattr()` and `url()` of the file also use. Explicit versions can
always be read with the `?versionId=` suffix or the `version_id` argument. Only a
`?versionId=` (or `?versionID=`, `?versionid=`, `?version_id=`) query at the end of a
path is a version; any other `?` is part of the key. A path with a version is not a
glob pattern: `copy()`, `mv()` and `get()` copy that version to a destination named
after its key.

```python
fs = S3FileSystem(
    connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/", region_name="us-west-2"),
    version_aware=True,
)

with fs.open("s3://YOUR_S3_BUCKET/path/to/object", "rb") as f:
    data = f.read()  # Pinned to the version observed at open time.

# List all versions of the objects under a prefix. Each version is named
# "bucket/key?versionId=<id>", except the "null" version, which is named by its key.
fs.ls("s3://YOUR_S3_BUCKET/path/to/", versions=True)

# Typed version information, including delete markers if requested.
versions = fs.object_version_info("s3://YOUR_S3_BUCKET/path/to/object")
for version in versions:
    print(version.version_id, version.is_latest, version.last_modified)
```

Version-aware operations require the `s3:GetObjectVersion` and
`s3:ListBucketVersions` permissions.

Moving a `?versionId=null` source onto its own key copies that version and
deletes the source version when bucket versioning is enabled. If versioning has
never been enabled or is suspended, the move leaves the source in place: copying
onto the key would replace the null version that the subsequent deletion removes.
The same distinction applies to move conflict checks.

A move compares a null version with its unversioned key using `GetBucketVersioning`,
which requires `s3:GetBucketVersioning`. Each relevant bucket is looked up once
during planning; other moves do not make this request. The result is not cached
between moves. A failed lookup stops the move before any copy or deletion.
Directory buckets do not support versioning, so their null versions are treated
as the key itself without a versioning lookup.

The opt-in S3 versioning integration tests create dedicated temporary buckets and
check unversioned, enabled, and suspended states. See [Testing](testing.md) for the
command and required test permissions.

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

## Typed S3 operations

`S3FileSystem.core` is an `S3Core`, the typed operations that the filesystem sends
its listing, lookup, delete, multipart upload and copy requests with. It can also be
built on a boto3 S3 client. Each operation sends one request (one per page for the
iterators and `list_object_annotations()`); `plan_multipart_copy()` and
`copy_object_annotation()`, described below, send several. The requests are sent with
the retry policy. An operation raises `FileNotFoundError` for a missing bucket or
multipart upload, or for a missing object or version that it reads, and caches
nothing. Requests sent through `fs.core` do not invalidate the filesystem's cache: call
`fs.invalidate_cache()` after a change, or make it through the filesystem.

```python
import boto3

from pyathena.filesystem.s3_core import S3Core
from pyathena.filesystem.s3_path import S3Path

core = fs.core  # or S3Core(boto3.client("s3"))

metadata = core.head_object(S3Path.parse("s3://YOUR_S3_BUCKET/path/to/object"))
print(metadata.content_length, metadata.version_id)

for page in core.list_objects("YOUR_S3_BUCKET", prefix="path/to/", delimiter="/"):
    print([o.key for o in page.objects], [p.prefix for p in page.common_prefixes])
```

`list_object_versions()` and `list_buckets()` return pages in the same way.

`delete_objects()` deletes an `S3DeleteBatch`, the objects of one bucket that one
DeleteObjects request accepts. `S3DeleteBatch.from_paths()` groups paths into batches
of up to 1,000 objects per bucket. The objects that S3 could not delete are in the
`errors` of the returned `S3DeleteResult`, not raised; as in S3, deleting a key that
does not exist is not an error.

```python
from pyathena.filesystem.s3_core import S3DeleteBatch

paths = [
    S3Path.parse("s3://YOUR_S3_BUCKET/path/to/a"),
    S3Path.parse("s3://YOUR_S3_BUCKET/path/to/b"),
]
for batch in S3DeleteBatch.from_paths(paths):
    result = core.delete_objects(batch)
    for error in result.errors:
        print(error)  # path (code: message)
```

### Multipart writer

`S3MultipartWriter` provides synchronous multipart requests and part planning without fsspec.
It is the writer used by `S3File`; applications can also construct it on an `S3Core`.
The constructor validates the destination and block size without sending a request.
The caller owns the buffer and executor, collects part results in number order, and waits for running requests before completing or aborting.
Serialize initiation and finalization; part requests can run in parallel after initiation.

```python
from io import BytesIO

from pyathena.filesystem.s3_writer import S3MultipartWriter

writer = S3MultipartWriter(
    core,
    S3Path("YOUR_S3_BUCKET", "path/to/object"),
    block_size=5 * 2**20,
    request_kwargs={"ChecksumAlgorithm": "SHA256"},
)
writer.initiate()
try:
    parts = [
        writer.upload_part(number, body)
        for number, body in writer.iter_parts(BytesIO(b"your data"))
    ]
    result = writer.complete(parts)
except BaseException:
    writer.abort()
    raise
```

`iter_parts()` consumes a blocking binary stream from its current position and produces numbered part bodies without sending requests.
It merges a short tail into the preceding block and splits the result if it reaches the maximum part size.
A stream shorter than the minimum part size must be the final part.
`iter_copy_parts(size)` produces numbered source ranges for copying an existing object before appending data.
A source smaller than the minimum part size must be the final part; `S3File` reads a small existing object into its buffer when appending.
Both planners accept `first_part_number` to include previously copied or uploaded parts in the 10,000-part limit.
`upload_part_copy()` sends a planned range with an exclusive end; `None` sends no range of its own, as with the core primitive.

The writer filters inherited and per-request parameters by operation; per-request parameters take precedence.
It retains the upload identity after completion and after an abort failure, and clears it only after a successful abort.
The caller can therefore retry a failed abort.
The writer does not invalidate the filesystem cache; call `fs.invalidate_cache()` after a direct write.

When an `S3File` wait for multipart creation is interrupted, it waits for any creation that has already started and recovers the successful response before aborting.
It drops the buffer and closes the file before waiting, so a later close or deferred commit cannot upload the interrupted write.
It suppresses Ctrl-C during the blocking recovery wait and re-raises the original error when cleanup finishes.
An additional interruption outside that wait can stop cleanup; once the upload identity is known, `discard()` can retry the abort.
An abort failure retains the upload identity for `discard()` to retry.
Cancelling an async buffered write through `asyncio.to_thread` still allows its writer thread to finish.

### Multipart requests

`create_multipart_upload()`, `upload_part()`, `upload_part_copy()`,
`complete_multipart_upload()` and `abort_multipart_upload()` send the requests of a
multipart upload. `part_ranges()` sends no request: it splits an object into the byte
ranges of the parts that copy it, by the part limits `MULTIPART_UPLOAD_MIN_PART_SIZE` (5 MiB),
`MULTIPART_UPLOAD_MAX_PART_SIZE` (5 GiB) and `MULTIPART_UPLOAD_MAX_PARTS` (10,000) of
`S3Core`.

`copy_object()` copies an object with one CopyObject request, which accepts objects
up to `MULTIPART_UPLOAD_MAX_PART_SIZE`. For a larger object, `plan_multipart_copy()`
reads the source and returns an `S3MultipartCopyPlan`: the version to copy, the byte
ranges of the parts, the parameters of each multipart upload request, and the
annotations to copy, so that the multipart upload writes the metadata, tags and
annotations that CopyObject would. It sends HeadObject, then GetObjectTagging and
ListObjectAnnotations unless the directives or the source exclude them, and writes
nothing. If HeadObject reports a size that fits in one CopyObject request, nothing else
is read, and the plan's `fits_single_request` says to copy with `copy_object()`
instead. `copy_object_annotation()` copies one annotation onto the destination after
the upload completes, with GetObjectAnnotation and PutObjectAnnotation. The
filesystems' `cp_file()`, `copy()` and `mv()` run these plans.

The multipart primitives take the `S3MultipartUpload` returned by creation,
keeping its bucket, key, upload ID and checksum configuration together.
The core retains no upload state.
`upload_part()` passes the upload's checksum algorithm to the SDK;
completion selects the matching part checksum and sends the upload's
`ChecksumType` when present, including `FULL_OBJECT`.
Without a creation algorithm, completion sends the ETag and part number without
checksums that the SDK may add to part uploads.
`abort_multipart_upload()` also accepts uploads returned by
`list_multipart_uploads()`.

```python
upload = core.create_multipart_upload(
    S3Path("YOUR_S3_BUCKET", "path/to/object"), ChecksumAlgorithm="SHA256"
)
try:
    part = core.upload_part(upload, 1, b"data")
    result = core.complete_multipart_upload(upload, [part])
except Exception:
    core.abort_multipart_upload(upload)
    raise
```

## Path pairing

`S3PathPairing` pairs the paths of one `copy()`, `get()` or `mv()`: `mv()` always, and
`copy()` and `get()` when a source has a version ID and the destination is one path
(fsspec pairs the others). Its `delete_paths()` splits the paths of an `rm()` into
those deleted as given and those expanded. The pairing is fsspec's, except that a
path with a version ID names that version, and its destination is named after its key.

A pairing is a frozen dataclass of `path1`, `path2`, `recursive` and `maxdepth`, and
holds no filesystem. The filesystems read from it what to look up (`expands`,
`skips_directories`, `looks_up_destination` and `conflict_candidates()`), expand the
sources, leave out the directories when `skips_directories` is true, look up the
paths, and pass the results to `copy_pairs()` and `move_pairs()`. A `sources`,
`destination_is_dir` or `missing` that a rule needs and that is not passed raises
`ValueError`.

For moves that compare a null version with its unversioned key, the caller also
looks up bucket versioning and passes the names of the versioning-enabled buckets
as `versioning_enabled_buckets` to both `conflict_candidates()` and `move_pairs()`.
Pass a collection of bucket names, such as a set, rather than a single string.
The default empty collection treats null versions as their keys, as in unversioned
or suspended buckets. The model makes no AWS requests.

```python
from pyathena.filesystem.s3_path_pairing import S3PathPairing

pairing = S3PathPairing("s3://YOUR_S3_BUCKET/src/", "s3://YOUR_S3_BUCKET/dst/", recursive=True)
sources = fs.expand_path(pairing.path1, recursive=pairing.recursive)
for source, destination in pairing.copy_pairs(sources):
    print(source, "->", destination)
```

## Async filesystem

`AioS3FileSystem` provides the same functionality on top of fsspec's
`AsyncFileSystem`, dispatching parallel operations through the asyncio event loop.
See {ref}`aio-s3-filesystem` for details.
