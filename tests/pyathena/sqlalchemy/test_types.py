from sqlalchemy import (
    types,
)

from pyathena.sqlalchemy.types import (
    get_double_type,
)


def test_get_double_type():
    from pyathena.sqlalchemy.base import ischema_names

    assert get_double_type() is types.DOUBLE
    assert ischema_names["double"] is types.DOUBLE
