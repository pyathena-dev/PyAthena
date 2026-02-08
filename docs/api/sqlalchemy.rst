.. _api_sqlalchemy:

SQLAlchemy Integration
======================

This section covers SQLAlchemy dialect implementations for Amazon Athena.

Dialects
--------

.. autoclass:: pyathena.sqlalchemy.rest.AthenaRestDialect
   :members:
   :inherited-members:

.. autoclass:: pyathena.sqlalchemy.pandas.AthenaPandasDialect
   :members:
   :inherited-members:

.. autoclass:: pyathena.sqlalchemy.arrow.AthenaArrowDialect
   :members:
   :inherited-members:

Type System
-----------

.. autoclass:: pyathena.sqlalchemy.types.AthenaTimestamp
   :members:

.. autoclass:: pyathena.sqlalchemy.types.AthenaDate
   :members:

.. autoclass:: pyathena.sqlalchemy.types.Tinyint
   :members:

.. autoclass:: pyathena.sqlalchemy.types.AthenaStruct
   :members:

.. autoclass:: pyathena.sqlalchemy.types.AthenaMap
   :members:

.. autoclass:: pyathena.sqlalchemy.types.AthenaArray
   :members:

Compilers
---------

.. autoclass:: pyathena.sqlalchemy.compiler.AthenaTypeCompiler
   :members:

.. autoclass:: pyathena.sqlalchemy.compiler.AthenaStatementCompiler
   :members:

.. autoclass:: pyathena.sqlalchemy.compiler.AthenaDDLCompiler
   :members:

Identifier Preparers
--------------------

.. autoclass:: pyathena.sqlalchemy.preparer.AthenaDMLIdentifierPreparer
   :members:

.. autoclass:: pyathena.sqlalchemy.preparer.AthenaDDLIdentifierPreparer
   :members:
