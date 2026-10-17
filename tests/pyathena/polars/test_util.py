# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import polars as pl
import pytest

from pyathena.polars.util import get_athena_type, holds_datetimes, to_microseconds

_MAP = getattr(pl, "Map", None)
_requires_map = pytest.mark.skipif(_MAP is None, reason="Polars 1.x has no Map type")


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


@_requires_map
def test_get_athena_type_map():
    assert get_athena_type(pl.Map(pl.String, pl.Int32)) == ("map", 0, 0)
    assert get_athena_type(pl.Map(pl.String, pl.List(pl.Int32))) == ("map", 0, 0)
    assert get_athena_type(pl.List(pl.Map(pl.String, pl.Int32))) == ("array", 0, 0)


_NS, _US = pl.Datetime("ns"), pl.Datetime("us")


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        (_NS, _US),
        (pl.Datetime("ns", "UTC"), pl.Datetime("us", "UTC")),
        (pl.Datetime("ms"), pl.Datetime("ms")),
        (pl.Int64, pl.Int64),
        (pl.List(_NS), pl.List(_US)),
        (pl.Struct({"a": _NS, "b": pl.List(_NS)}), pl.Struct({"a": _US, "b": pl.List(_US)})),
    ],
)
def test_to_microseconds(dtype, expected):
    assert to_microseconds(dtype) == expected
    assert to_microseconds(dtype, maps=False) == expected


@_requires_map
def test_to_microseconds_map():
    """A Map is kept, or with maps=False becomes a List of Structs of its key and value."""
    dtype = pl.List(pl.Map(_NS, pl.Struct({"t": _NS})))
    assert to_microseconds(dtype) == pl.List(pl.Map(_US, pl.Struct({"t": _US})))
    assert to_microseconds(dtype, maps=False) == pl.List(
        pl.List(pl.Struct({"key": _US, "value": pl.Struct({"t": _US})}))
    )


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        (_US, True),
        (pl.Date, False),
        (pl.List(pl.Struct({"a": pl.Int8, "b": pl.List(_NS)})), True),
        (pl.Struct({"a": pl.Int8, "b": pl.List(pl.String)}), False),
    ],
)
def test_holds_datetimes(dtype, expected):
    assert holds_datetimes(dtype) is expected


@_requires_map
def test_holds_datetimes_map():
    assert holds_datetimes(pl.Map(_US, pl.Int8))
    assert holds_datetimes(pl.Map(pl.String, pl.List(_NS)))
    assert not holds_datetimes(pl.Map(pl.String, pl.Int8))
