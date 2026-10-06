# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import polars as pl
import pytest

from pyathena.polars.result_set import PolarsDataFrameIterator


class TestPolarsDataFrameIterator:
    @pytest.mark.parametrize(
        "reader",
        [pl.DataFrame({"a": [1, 2]}), (df for df in [pl.DataFrame({"a": [1]})] * 2)],
        ids=["dataframe", "generator"],
    )
    def test_close_stops_iteration(self, reader):
        """A closed iterator yields nothing for either reader kind."""
        df_iter = PolarsDataFrameIterator(reader, {}, ["a"])
        df_iter.close()
        assert list(df_iter) == []
