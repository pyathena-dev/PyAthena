# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from pyathena.arrow.converter import DefaultArrowUnloadTypeConverter


class TestDefaultArrowUnloadTypeConverter:
    def test_convert_delegates_to_default(self):
        """convert() dispatches through the default converter instead of returning None."""
        converter = DefaultArrowUnloadTypeConverter()
        assert converter.convert("varchar", "hello") == "hello"
