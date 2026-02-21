.. _api_aio:

Native Asyncio
==============

This section covers the native asyncio connection, cursors, and base classes.

Connection
----------

.. automodule:: pyathena
   :members: aconnect

.. autoclass:: pyathena.aio.connection.AioConnection
   :members:
   :inherited-members:

Aio Cursors
-----------

.. autoclass:: pyathena.aio.cursor.AioCursor
   :members:
   :inherited-members:

.. autoclass:: pyathena.aio.cursor.AioDictCursor
   :members:
   :inherited-members:

Aio Result Set
--------------

.. autoclass:: pyathena.aio.result_set.AthenaAioResultSet
   :members:
   :inherited-members:

.. autoclass:: pyathena.aio.result_set.AthenaAioDictResultSet
   :members:
   :inherited-members:

Aio Base Classes
----------------

.. autoclass:: pyathena.aio.common.AioBaseCursor
   :members:
   :inherited-members:

.. autoclass:: pyathena.aio.common.WithAsyncFetch
   :members:
   :inherited-members:

Aio Pandas Cursor
-----------------

.. autoclass:: pyathena.aio.pandas.cursor.AioPandasCursor
   :members:
   :inherited-members:

Aio Arrow Cursor
----------------

.. autoclass:: pyathena.aio.arrow.cursor.AioArrowCursor
   :members:
   :inherited-members:

Aio Polars Cursor
-----------------

.. autoclass:: pyathena.aio.polars.cursor.AioPolarsCursor
   :members:
   :inherited-members:

Aio S3FS Cursor
---------------

.. autoclass:: pyathena.aio.s3fs.cursor.AioS3FSCursor
   :members:
   :inherited-members:

Aio Spark Cursor
----------------

.. autoclass:: pyathena.aio.spark.cursor.AioSparkCursor
   :members:
   :inherited-members:
