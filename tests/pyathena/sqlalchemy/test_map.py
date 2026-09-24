# Copyright 2025 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from sqlalchemy import (
    Integer,
    String,
)
from sqlalchemy.sql import sqltypes

from pyathena.sqlalchemy.types import (
    MAP,
    AthenaMap,
)


class TestAthenaMap:
    def test_creation_with_defaults(self):
        map_type = AthenaMap()
        assert isinstance(map_type.key_type, sqltypes.String)
        assert isinstance(map_type.value_type, sqltypes.String)

    def test_creation_with_type_classes(self):
        map_type = AthenaMap(String, Integer)
        assert isinstance(map_type.key_type, sqltypes.String)
        assert isinstance(map_type.value_type, sqltypes.Integer)

    def test_creation_with_type_instances(self):
        map_type = AthenaMap(String(), Integer())
        assert isinstance(map_type.key_type, sqltypes.String)
        assert isinstance(map_type.value_type, sqltypes.Integer)

    def test_python_type(self):
        map_type = AthenaMap()
        assert map_type.python_type is dict

    def test_visit_name(self):
        map_type = AthenaMap()
        assert map_type.__visit_name__ == "map"

    def test_map_uppercase_visit_name(self):
        map_type = MAP()
        assert map_type.__visit_name__ == "MAP"

    def test_mixed_type_definitions(self):
        map_type = AthenaMap(String, Integer())
        assert isinstance(map_type.key_type, sqltypes.String)
        assert isinstance(map_type.value_type, sqltypes.Integer)
