.. _api_arrow:

Apache Arrow Integration
========================

This section covers Apache Arrow-specific cursors, result sets, and data converters.

Arrow Cursors
-------------

.. autoclass:: pyathena.arrow.cursor.ArrowCursor
   :members:
   :inherited-members:

.. autoclass:: pyathena.arrow.async_cursor.AsyncArrowCursor
   :members:
   :inherited-members:

Arrow Result Set
----------------

.. autoclass:: pyathena.arrow.result_set.AthenaArrowResultSet
   :members:
   :inherited-members:

Arrow Data Converters
----------------------

.. autoclass:: pyathena.arrow.converter.DefaultArrowTypeConverter
   :members:

.. autoclass:: pyathena.arrow.converter.DefaultArrowUnloadTypeConverter
   :members:

Arrow Utilities
---------------

.. autofunction:: pyathena.arrow.util.to_column_info

.. autofunction:: pyathena.arrow.util.get_athena_type