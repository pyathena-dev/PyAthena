..
   Copyright 2025 The PyAthena authors

   Licensed under the MIT License.
   See LICENSE or https://opensource.org/licenses/MIT.

   SPDX-License-Identifier: MIT

.. _api_utilities:

Utilities and Configuration
===========================

This section covers utility functions, retry configuration, and helper classes.

Retry Configuration
-------------------

.. autoclass:: pyathena.util.RetryConfig
   :members:

Utility Functions
-----------------

.. autofunction:: pyathena.util.retry_api_call

.. autofunction:: pyathena.util.is_retryable_error

.. autofunction:: pyathena.util.parse_output_location

.. autofunction:: pyathena.util.strtobool

Common Base Classes
-------------------

.. autoclass:: pyathena.common.CursorIterator
   :members:

.. autoclass:: pyathena.common.BaseCursor
   :members: