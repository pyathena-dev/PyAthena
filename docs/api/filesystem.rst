..
   Copyright 2025 The PyAthena authors

   Licensed under the MIT License.
   See LICENSE or https://opensource.org/licenses/MIT.

   SPDX-License-Identifier: MIT

.. _api_filesystem:

File System Integration
=======================

This section covers S3 filesystem integration and object management.

S3 FileSystem
-------------

.. autoclass:: pyathena.filesystem.s3.S3FileSystem
   :members:

.. autoclass:: pyathena.filesystem.s3.S3File
   :members:

Async S3 FileSystem
-------------------

.. autoclass:: pyathena.filesystem.s3_async.AioS3FileSystem
   :members:

.. autoclass:: pyathena.filesystem.s3_async.AioS3File
   :members:

S3 Executor
-----------

.. autoclass:: pyathena.filesystem.s3_executor.S3Executor
   :members:

.. autoclass:: pyathena.filesystem.s3_executor.S3ThreadPoolExecutor
   :members:

.. autoclass:: pyathena.filesystem.s3_executor.S3AioExecutor
   :members:

S3 Paths
--------

.. autoclass:: pyathena.filesystem.s3_path.S3Path
   :members:

.. autoclass:: pyathena.filesystem.s3_path_pairing.S3PathPairing
   :members:

S3 Core
-------

.. autoclass:: pyathena.filesystem.s3_core.S3Core
   :members:

.. autoclass:: pyathena.filesystem.s3_core.S3ObjectSummary
   :members:

.. autoclass:: pyathena.filesystem.s3_core.S3CommonPrefix
   :members:

.. autoclass:: pyathena.filesystem.s3_core.S3Bucket
   :members:

.. autoclass:: pyathena.filesystem.s3_core.S3ListObjectsPage
   :members:

.. autoclass:: pyathena.filesystem.s3_core.S3ListObjectVersionsPage
   :members:

.. autoclass:: pyathena.filesystem.s3_core.S3ListBucketsPage
   :members:

.. autoclass:: pyathena.filesystem.s3_core.S3DeleteBatch
   :members:

.. autoclass:: pyathena.filesystem.s3_core.S3DeleteResult
   :members:

.. autoclass:: pyathena.filesystem.s3_core.S3DeleteError
   :members:

.. autoclass:: pyathena.filesystem.s3_core.S3MultipartCopyPlan
   :members:

S3 Objects
----------

.. autoclass:: pyathena.filesystem.s3_object.S3Object
   :members:

.. autoclass:: pyathena.filesystem.s3_object.S3ObjectType
   :members:

.. autoclass:: pyathena.filesystem.s3_object.S3StorageClass
   :members:

.. autoclass:: pyathena.filesystem.s3_object.S3Metadata
   :members:

.. autoclass:: pyathena.filesystem.s3_object.S3ObjectVersion
   :members:

S3 Upload Operations
--------------------

.. autoclass:: pyathena.filesystem.s3_object.S3PutObject
   :members:

.. autoclass:: pyathena.filesystem.s3_object.S3MultipartUpload
   :members:

.. autoclass:: pyathena.filesystem.s3_object.S3MultipartUploadPart
   :members:

.. autoclass:: pyathena.filesystem.s3_object.S3CompleteMultipartUpload
   :members: