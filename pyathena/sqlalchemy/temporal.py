"""Athena DATE and TIMESTAMP types and literal conversion."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from functools import partial
from typing import TYPE_CHECKING, Any

from sqlalchemy import types
from sqlalchemy.sql.type_api import TypeEngine

from pyathena.formatter import _date_literal, _escape_trino, _timestamp_literal

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

    With a ``precision``, literals render that many fractional digits and
    casts render ``TIMESTAMP(precision)``; without one, casts render
    ``TIMESTAMP(6)``. ``CREATE TABLE`` always renders ``TIMESTAMP``.

    Example:
        >>> from sqlalchemy import Column, Table, MetaData, cast, select
        >>> from pyathena.sqlalchemy.types import AthenaTimestamp
        >>> metadata = MetaData()
        >>> events = Table('events', metadata,
        ...     Column('event_time', AthenaTimestamp)
        ... )
        >>> millis = select(cast(events.c.event_time, AthenaTimestamp(precision=3)))
    """

    __visit_name__ = "TIMESTAMP"

    def __init__(self, precision: int | None = None) -> None:
        """Initialize the type.

        Args:
            precision: The number of fractional-second digits, from 0 to 12,
                or None for the default rendering.

        Raises:
            ValueError: If ``precision`` is outside 0 to 12.
        """
        if precision is not None and not 0 <= precision <= 12:
            raise ValueError(f"TIMESTAMP precision must be between 0 and 12: {precision}")
        self.precision = precision

    @property
    def python_type(self) -> type[datetime]:
        """The Python type of TIMESTAMP values.

        Returns:
            ``datetime.datetime``.
        """
        return datetime

    @staticmethod
    def process(
        value: datetime | Any | None,
        quote: Callable[[str], str] = _escape_trino,
        precision: int | None = None,
    ) -> str:
        """Render a value as an Athena TIMESTAMP literal.

        Args:
            value: A datetime, or any other value rendered with ``str()``.
            quote: The function quoting a value that is not a datetime.
            precision: The number of fractional-second digits for a datetime,
                or None for the default rendering.

        Returns:
            The TIMESTAMP literal.
        """
        if isinstance(value, datetime):
            return _timestamp_literal(value, precision)
        return f"TIMESTAMP {quote(str(value))}"

    def literal_processor(self, dialect: Dialect) -> _LiteralProcessorType[datetime] | None:
        """Return the literal renderer for the dialect.

        Args:
            dialect: The dialect compiling the statement.

        Returns:
            A function rendering a value as a TIMESTAMP literal.
        """
        return partial(self.process, quote=_string_quote(dialect), precision=self.precision)


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

    @property
    def python_type(self) -> type[date]:
        """The Python type of DATE values.

        Returns:
            ``datetime.date``.
        """
        return date

    @staticmethod
    def process(value: date | Any, quote: Callable[[str], str] = _escape_trino) -> str:
        """Render a value as an Athena DATE literal.

        Args:
            value: A date, or any other value rendered with ``str()``.
            quote: The function quoting a value that is not a date.

        Returns:
            The DATE literal.
        """
        # datetime is a subclass of date, so this branch also covers datetime,
        # which is truncated to its date part.
        if isinstance(value, date):
            return _date_literal(value)
        return f"DATE {quote(str(value))}"

    def literal_processor(self, dialect: Dialect) -> _LiteralProcessorType[date] | None:
        """Return the literal renderer for the dialect.

        Args:
            dialect: The dialect compiling the statement.

        Returns:
            A function rendering a value as a DATE literal.
        """
        return partial(self.process, quote=_string_quote(dialect))


def _string_quote(dialect: Dialect) -> Callable[[str], str]:
    """Return the dialect's string literal renderer.

    It also doubles ``%`` for dialects whose paramstyle needs it.

    Args:
        dialect: The dialect compiling the statement.

    Returns:
        A function rendering a string as a quoted SQL literal.
    """
    return types.String().literal_processor(dialect) or _escape_trino
