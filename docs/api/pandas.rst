.. _api_pandas:

Pandas Integration
==================

This section covers pandas-specific cursors, result sets, and data converters.

Pandas Cursors
--------------

.. autoclass:: pyathena.pandas.cursor.PandasCursor
   :members:
   :inherited-members:

.. autoclass:: pyathena.pandas.async_cursor.AsyncPandasCursor
   :members:
   :inherited-members:

Pandas Result Set
-----------------

.. autoclass:: pyathena.pandas.result_set.AthenaPandasResultSet
   :members:
   :inherited-members:

.. autoclass:: pyathena.pandas.result_set.PandasDataFrameIterator
   :members:

Pandas Data Converters
-----------------------

.. autoclass:: pyathena.pandas.converter.DefaultPandasTypeConverter
   :members:

.. autoclass:: pyathena.pandas.converter.DefaultPandasUnloadTypeConverter
   :members:

Pandas Utilities
----------------

.. autofunction:: pyathena.pandas.util.get_chunks

.. autofunction:: pyathena.pandas.util.reset_index

.. autofunction:: pyathena.pandas.util.as_pandas

.. autofunction:: pyathena.pandas.util.to_sql_type_mappings

.. autofunction:: pyathena.pandas.util.to_parquet

.. autofunction:: pyathena.pandas.util.to_sql

.. autofunction:: pyathena.pandas.util.get_column_names_and_types

.. autofunction:: pyathena.pandas.util.generate_ddl