# Copyright 2025 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import pytest
from sqlalchemy import (
    Integer,
    String,
)
from sqlalchemy.sql import sqltypes

from pyathena.sqlalchemy.types import AthenaStruct


class TestAthenaStruct:
    def test_invalid_field_specification(self):
        with pytest.raises(ValueError, match="Invalid field specification"):
            AthenaStruct(123)  # Invalid field type

    def test_mixed_field_definitions(self):
        struct_type = AthenaStruct("name", ("age", Integer), ("active", String()))
        assert len(struct_type.fields) == 3
        assert isinstance(struct_type.fields["name"], sqltypes.String)
        assert isinstance(struct_type.fields["age"], sqltypes.Integer)
        assert isinstance(struct_type.fields["active"], sqltypes.String)
