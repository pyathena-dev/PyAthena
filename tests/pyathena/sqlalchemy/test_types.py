from sqlalchemy import (
    types,
)

from pyathena.sqlalchemy.types import (
    get_double_type,
)


def test_get_double_type():
    from pyathena.sqlalchemy.base import ischema_names

    result = get_double_type()
    if hasattr(types, "DOUBLE"):
        assert result is types.DOUBLE
    else:
        assert result is types.FLOAT
    assert ischema_names["double"] is result
