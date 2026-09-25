"""Athena DATE and TIMESTAMP types and literal conversion."""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy.sql.type_api import TypeEngine

from pyathena.formatter import _timestamp_literal

if TYPE_CHECKING:
    from sqlalchemy import Dialect
    from sqlalchemy.sql.type_api import _LiteralProcessorType


class AthenaTimestamp(TypeEngine[datetime]):
    """SQLAlchemy type for Athena TIMESTAMP values.

    This type handles the conversion of Python datetime objects to Athena's
    TIMESTAMP literal syntax. When used in queries, datetime values are
    rendered as ``TIMESTAMP 'YYYY-MM-DD HH:MM:SS.mmm'``, or with six
    fractional digits (``timestamp(6)``) when the value has a sub-millisecond
    part. Iceberg tables store microseconds; Hive tables store milliseconds.

    Example:
        >>> from sqlalchemy import Column, Table, MetaData
        >>> from pyathena.sqlalchemy.types import AthenaTimestamp
        >>> metadata = MetaData()
        >>> events = Table('events', metadata,
        ...     Column('event_time', AthenaTimestamp)
        ... )
    """

    __visit_name__ = "TIMESTAMP"

    @staticmethod
    def process(value: datetime | Any | None) -> str:
        """Render a value as an Athena TIMESTAMP literal.

        Args:
            value: A datetime, or any other value rendered with ``str()``.

        Returns:
            The TIMESTAMP literal.
        """
        if isinstance(value, datetime):
            return _timestamp_literal(value)
        return f"TIMESTAMP '{value!s}'"

    def literal_processor(self, dialect: Dialect) -> _LiteralProcessorType[datetime] | None:
        return self.process


class AthenaDate(TypeEngine[date]):
    """SQLAlchemy type for Athena DATE values.

    This type handles the conversion of Python date objects to Athena's
    DATE literal syntax. When used in queries, date values are rendered
    as ``DATE 'YYYY-MM-DD'``.

    Example:
        >>> from sqlalchemy import Column, Table, MetaData
        >>> from pyathena.sqlalchemy.types import AthenaDate
        >>> metadata = MetaData()
        >>> orders = Table('orders', metadata,
        ...     Column('order_date', AthenaDate)
        ... )
    """

    __visit_name__ = "DATE"

    @staticmethod
    def process(value: date | Any) -> str:
        # datetime is a subclass of date, so this branch also covers datetime,
        # which is truncated to its date part.
        if isinstance(value, date):
            return f"DATE '{value:%Y-%m-%d}'"
        return f"DATE '{value!s}'"

    def literal_processor(self, dialect: Dialect) -> _LiteralProcessorType[date] | None:
        return self.process
