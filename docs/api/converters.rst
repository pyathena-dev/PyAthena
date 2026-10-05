..
   Copyright 2025 The PyAthena authors

   Licensed under the MIT License.
   See LICENSE or https://opensource.org/licenses/MIT.

   SPDX-License-Identifier: MIT

.. _api_converters:

Data Conversion
===============

This section covers data type converters and parameter formatters.

Type Converters
---------------

.. autoclass:: pyathena.converter.Converter
   :members:

.. autoclass:: pyathena.converter.DefaultTypeConverter
   :members:

Parameter Formatters
--------------------

In 4.0, the private ``pyathena.formatter._escape_presto`` helper is removed.
Callers of that helper can use ``pyathena.formatter._escape_trino`` instead.

.. autoclass:: pyathena.formatter.Formatter
   :members:

.. autoclass:: pyathena.formatter.DefaultParameterFormatter
   :members:
