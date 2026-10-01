# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
import pytest

from pyathena.aio.common import AioBaseCursor, WithAsyncFetch
from pyathena.common import BaseCursor, CursorIterator
from pyathena.result_set import WithFetch, WithResultSet


class TestWithResultSet:
    def test_is_mixin(self):
        assert WithResultSet.__bases__ == (object,)

    @pytest.mark.parametrize(
        ("cursor_base", "base"),
        [(WithFetch, BaseCursor), (WithAsyncFetch, AioBaseCursor)],
    )
    def test_precedes_cursor_bases(self, cursor_base, base):
        # Listed first, so that its members take precedence over the cursor bases'.
        assert cursor_base.__bases__ == (WithResultSet, base, CursorIterator)
