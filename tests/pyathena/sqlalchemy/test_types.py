# -*- coding: utf-8 -*-
import pytest
from sqlalchemy import Integer, String, types
from sqlalchemy.sql import sqltypes

from pyathena.sqlalchemy.types import (
    ARRAY,
    MAP,
    STRUCT,
    AthenaArray,
    AthenaMap,
    AthenaStruct,
    get_double_type,
)


class TestAthenaStruct:
    def test_creation_with_strings(self):
        struct_type = AthenaStruct("name", "age")
        assert "name" in struct_type.fields
        assert "age" in struct_type.fields
        assert isinstance(struct_type.fields["name"], sqltypes.String)
        assert isinstance(struct_type.fields["age"], sqltypes.String)

    def test_creation_with_tuples(self):
        struct_type = AthenaStruct(("name", String), ("age", Integer))
        assert "name" in struct_type.fields
        assert "age" in struct_type.fields
        assert isinstance(struct_type.fields["name"], sqltypes.String)
        assert isinstance(struct_type.fields["age"], sqltypes.Integer)

    def test_creation_with_type_instances(self):
        struct_type = AthenaStruct(("name", String()), ("age", Integer()))
        assert "name" in struct_type.fields
        assert "age" in struct_type.fields
        assert isinstance(struct_type.fields["name"], sqltypes.String)
        assert isinstance(struct_type.fields["age"], sqltypes.Integer)

    def test_field_access_by_key(self):
        struct_type = AthenaStruct(("name", String), ("age", Integer))
        name_field = struct_type["name"]
        assert isinstance(name_field, sqltypes.String)

    def test_python_type(self):
        struct_type = AthenaStruct(("name", String))
        assert struct_type.python_type is dict

    def test_invalid_field_specification(self):
        with pytest.raises(ValueError):
            AthenaStruct(123)  # Invalid field type

    def test_visit_name(self):
        struct_type = AthenaStruct()
        assert struct_type.__visit_name__ == "struct"

    def test_struct_uppercase_visit_name(self):
        struct_type = STRUCT()
        assert struct_type.__visit_name__ == "STRUCT"

    def test_empty_struct(self):
        struct_type = AthenaStruct()
        assert len(struct_type.fields) == 0

    def test_mixed_field_definitions(self):
        struct_type = AthenaStruct("name", ("age", Integer), ("active", String()))
        assert len(struct_type.fields) == 3
        assert isinstance(struct_type.fields["name"], sqltypes.String)
        assert isinstance(struct_type.fields["age"], sqltypes.Integer)
        assert isinstance(struct_type.fields["active"], sqltypes.String)

    def test_field_access_nonexistent_key(self):
        struct_type = AthenaStruct(("name", String))
        with pytest.raises(KeyError):
            struct_type["nonexistent"]


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


class TestAthenaArray:
    def test_creation_with_default(self):
        array_type = AthenaArray()
        assert isinstance(array_type.item_type, sqltypes.String)

    def test_creation_with_type_class(self):
        array_type = AthenaArray(Integer)
        assert isinstance(array_type.item_type, sqltypes.Integer)

    def test_creation_with_type_instance(self):
        array_type = AthenaArray(Integer())
        assert isinstance(array_type.item_type, sqltypes.Integer)

    def test_creation_with_string_type(self):
        array_type = AthenaArray(String)
        assert isinstance(array_type.item_type, sqltypes.String)

    def test_python_type(self):
        array_type = AthenaArray()
        assert array_type.python_type is list

    def test_visit_name(self):
        array_type = AthenaArray()
        assert array_type.__visit_name__ == "array"

    def test_array_uppercase_visit_name(self):
        array_type = ARRAY()
        assert array_type.__visit_name__ == "ARRAY"

    def test_array_with_complex_type(self):
        array_type = AthenaArray(AthenaStruct(("name", String), ("age", Integer)))
        assert isinstance(array_type.item_type, AthenaStruct)
        assert "name" in array_type.item_type.fields
        assert "age" in array_type.item_type.fields

    def test_array_with_nested_array(self):
        array_type = AthenaArray(AthenaArray(Integer))
        assert isinstance(array_type.item_type, AthenaArray)
        assert isinstance(array_type.item_type.item_type, sqltypes.Integer)

    def test_array_with_map_type(self):
        array_type = AthenaArray(AthenaMap(String, Integer))
        assert isinstance(array_type.item_type, AthenaMap)
        assert isinstance(array_type.item_type.key_type, sqltypes.String)
        assert isinstance(array_type.item_type.value_type, sqltypes.Integer)


def test_get_double_type():
    from pyathena.sqlalchemy.base import ischema_names

    result = get_double_type()
    if hasattr(types, "DOUBLE"):
        assert result is types.DOUBLE
    else:
        assert result is types.FLOAT
    assert ischema_names["double"] is result
