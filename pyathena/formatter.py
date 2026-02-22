from __future__ import annotations

import logging
import textwrap
import uuid
from abc import ABCMeta, abstractmethod
from collections.abc import Callable
from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from pyathena.error import ProgrammingError
from pyathena.model import AthenaCompression, AthenaFileFormat

_logger = logging.getLogger(__name__)


class Formatter(metaclass=ABCMeta):
    """Abstract base class for formatting Python values for SQL queries.

    Formatters handle the conversion of Python objects to SQL-compatible
    string representations for use in parameterized queries. They ensure
    proper escaping and formatting of values based on their types.

    This class provides a framework for mapping Python types to formatting
    functions and handles the formatting process during query preparation.

    Attributes:
        mappings: Dictionary mapping Python types to formatting functions.
        default: Default formatting function for unmapped types.
    """

    def __init__(
        self,
        mappings: dict[type[Any], Callable[[Formatter, Callable[[str], str], Any], Any]],
        default: Callable[[Formatter, Callable[[str], str], Any], Any] | None = None,
    ) -> None:
        self._mappings = mappings
        self._default = default

    @property
    def mappings(
        self,
    ) -> dict[type[Any], Callable[[Formatter, Callable[[str], str], Any], Any]]:
        """Get the current parameter formatting mappings.

        Returns:
            Dictionary mapping Python types to formatting functions.
        """
        return self._mappings

    def get(self, type_) -> Callable[[Formatter, Callable[[str], str], Any], Any] | None:
        """Get the formatting function for a specific Python type.

        Args:
            type_: The Python value to get formatter for.

        Returns:
            The formatting function for the type, or the default formatter if not found.
        """
        return self.mappings.get(type(type_), self._default)

    def set(
        self,
        type_: type[Any],
        formatter: Callable[[Formatter, Callable[[str], str], Any], Any],
    ) -> None:
        self.mappings[type_] = formatter

    def remove(self, type_: type[Any]) -> None:
        self.mappings.pop(type_, None)

    def update(
        self, mappings: dict[type[Any], Callable[[Formatter, Callable[[str], str], Any], Any]]
    ) -> None:
        self.mappings.update(mappings)

    @abstractmethod
    def format(self, operation: str, parameters: dict[str, Any] | None = None) -> str:
        raise NotImplementedError  # pragma: no cover

    @staticmethod
    def wrap_unload(
        operation: str,
        s3_staging_dir: str,
        format_: str = AthenaFileFormat.FILE_FORMAT_PARQUET,
        compression: str = AthenaCompression.COMPRESSION_SNAPPY,
    ) -> tuple[str, str | None]:
        """Wrap a SELECT query with UNLOAD statement for high-performance result retrieval.

        Transforms SELECT or WITH queries into UNLOAD statements that export results
        directly to S3 in optimized formats (Parquet, ORC) with compression. This
        approach is significantly faster than standard CSV-based result retrieval
        for large datasets and preserves data types more accurately.

        Args:
            operation: SQL query to wrap. Must be a SELECT or WITH statement.
            s3_staging_dir: Base S3 directory for storing UNLOAD results.
            format_: Output file format. Defaults to Parquet for optimal performance.
            compression: Compression algorithm. Defaults to Snappy for balanced
                       compression ratio and speed.

        Returns:
            Tuple containing:
            - Modified UNLOAD query string
            - S3 location where results will be stored (None if not SELECT/WITH)

        Example:
            >>> query = "SELECT * FROM sales WHERE year = 2023"
            >>> unload_query, location = Formatter.wrap_unload(
            ...     query, "s3://my-bucket/results/"
            ... )
            >>> print(unload_query)
            UNLOAD (
                SELECT * FROM sales WHERE year = 2023
            )
            TO 's3://my-bucket/results/unload/20231215/uuid//'
            WITH (
                format = 'PARQUET',
                compression = 'SNAPPY'
            )

        Note:
            Only SELECT and WITH statements are wrapped. Other statement types
            are returned unchanged with location=None.
        """
        if not operation or not operation.strip():
            raise ProgrammingError("Query is none or empty.")

        operation_upper = operation.strip().upper()
        if operation_upper.startswith(("SELECT", "WITH")):
            now = datetime.now(timezone.utc).strftime("%Y%m%d")
            location = f"{s3_staging_dir}unload/{now}/{uuid.uuid4()!s}/"
            operation = textwrap.dedent(
                f"""
                UNLOAD (
                \t{operation.strip()}
                )
                TO '{location}'
                WITH (
                \tformat = '{format_}',
                \tcompression = '{compression}'
                )
                """
            )
        else:
            location = None
        return operation, location


def _escape_presto(val: str) -> str:
    escaped = val.replace("'", "''")
    return f"'{escaped}'"


def _escape_hive(val: str) -> str:
    escaped = (
        val.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\r", "\\r")
        .replace("\n", "\\n")
        .replace("\t", "\\t")
    )
    return f"'{escaped}'"


def _format_none(formatter: Formatter, escaper: Callable[[str], str], val: Any) -> Any:
    return "null"


def _format_default(formatter: Formatter, escaper: Callable[[str], str], val: Any) -> Any:
    return val


def _format_date(formatter: Formatter, escaper: Callable[[str], str], val: Any) -> Any:
    return f"DATE '{val:%Y-%m-%d}'"


def _format_datetime(formatter: Formatter, escaper: Callable[[str], str], val: Any) -> Any:
    return f"""TIMESTAMP '{val.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}'"""


def _format_bool(formatter: Formatter, escaper: Callable[[str], str], val: Any) -> Any:
    return str(val)


def _format_str(formatter: Formatter, escaper: Callable[[str], str], val: Any) -> Any:
    return escaper(val)


def _format_seq(formatter: Formatter, escaper: Callable[[str], str], val: Any) -> Any:
    results = []
    for v in val:
        func = formatter.get(v)
        if not func:
            raise TypeError(f"{type(v)} is not defined formatter.")
        formatted = func(formatter, escaper, v)
        if not isinstance(
            formatted,
            (str,),
        ):
            # force string format
            if isinstance(
                formatted,
                (
                    float,
                    Decimal,
                ),
            ):
                formatted = f"{formatted:f}"
            else:
                formatted = f"{formatted}"
        results.append(formatted)
    return f"""({", ".join(results)})"""


def _format_decimal(formatter: Formatter, escaper: Callable[[str], str], val: Any) -> Any:
    escaped = escaper(f"{val:f}")
    return f"DECIMAL {escaped}"


_DEFAULT_FORMATTERS: dict[type[Any], Callable[[Formatter, Callable[[str], str], Any], Any]] = {
    type(None): _format_none,
    date: _format_date,
    datetime: _format_datetime,
    int: _format_default,
    float: _format_default,
    Decimal: _format_decimal,
    bool: _format_bool,
    str: _format_str,
    list: _format_seq,
    set: _format_seq,
    tuple: _format_seq,
}


class DefaultParameterFormatter(Formatter):
    """Default implementation of the Formatter for SQL parameter formatting.

    This formatter provides standard formatting for common Python types used
    in SQL parameters. It handles proper escaping and quoting to prevent
    SQL injection and ensure valid SQL syntax.

    Supported types:
        - None: Converts to SQL NULL
        - Strings: Properly escaped and quoted
        - Numbers: int, float, Decimal
        - Dates and times: date, datetime, time
        - Booleans: Converted to SQL boolean literals
        - Sequences: list, tuple, set (for IN clauses)

    Example:
        >>> formatter = DefaultParameterFormatter()
        >>> sql = formatter.format(
        ...     "SELECT * FROM users WHERE name = %(name)s AND age > %(age)s",
        ...     {"name": "John's Data", "age": 25}
        ... )
        >>> print(sql)
        SELECT * FROM users WHERE name = 'John''s Data' AND age > 25
    """

    def __init__(self) -> None:
        super().__init__(mappings=deepcopy(_DEFAULT_FORMATTERS), default=None)

    def format(self, operation: str, parameters: dict[str, Any] | None = None) -> str:
        if not operation or not operation.strip():
            raise ProgrammingError("Query is none or empty.")
        operation = operation.strip()

        operation_upper = operation.upper()
        if operation_upper.startswith(("SELECT", "WITH", "INSERT", "UPDATE", "MERGE")):
            escaper = _escape_presto
        else:
            escaper = _escape_hive

        kwargs: dict[str, Any] | None = None
        if parameters is not None:
            kwargs = {}
            if not parameters:
                pass
            elif isinstance(parameters, dict):
                for k, v in parameters.items():
                    func = self.get(v)
                    if not func:
                        raise TypeError(f"{type(v)} is not defined formatter.")
                    kwargs.update({k: func(self, escaper, v)})
            else:
                raise ProgrammingError(
                    f"Unsupported parameter (Support for dict only): {parameters}"
                )

        return (operation % kwargs).strip() if kwargs is not None else operation.strip()
