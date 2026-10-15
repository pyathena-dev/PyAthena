# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import polars as pl
import pytest

from pyathena.polars.util import get_athena_type


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        (pl.List(pl.Int32), ("array", 0, 0)),
        (pl.List(pl.Struct({"x": pl.Int32})), ("array", 0, 0)),
        (pl.Struct({"x": pl.Int32}), ("row", 0, 0)),
        (pl.Struct({"x": pl.List(pl.Int32)}), ("row", 0, 0)),
    ],
)
def test_get_athena_type_nested(dtype, expected):
    assert get_athena_type(dtype) == expected


@pytest.mark.skipif(not hasattr(pl, "Map"), reason="Polars 1.x has no Map type")
def test_get_athena_type_map():
    assert get_athena_type(pl.Map(pl.String, pl.Int32)) == ("map", 0, 0)
    assert get_athena_type(pl.Map(pl.String, pl.List(pl.Int32))) == ("map", 0, 0)
    assert get_athena_type(pl.List(pl.Map(pl.String, pl.Int32))) == ("array", 0, 0)
