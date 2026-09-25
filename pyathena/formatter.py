from __future__ import annotations

import logging
import re
import textwrap
import uuid
from abc import ABCMeta, abstractmethod
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Literal

from pyathena.error import ProgrammingError
from pyathena.model import AthenaCompression, AthenaFileFormat

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _ComplexParameter:
    """Typed complex value supplied by the SQLAlchemy dialect."""

    constructor: Literal["ARRAY", "MAP", "ROW", "JSON_PARSE", "FROM_HEX"]
    values: tuple[Any, ...]


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


def _escape_trino(val: str) -> str:
    escaped = val.replace("'", "''")
    return f"'{escaped}'"


def _escape_presto(val: str) -> str:
    # Backward-compatible alias. Athena's engine is Trino (engine v3; formerly
    # Presto in engine v1/v2), and Trino and Presto escape string literals
    # identically -- a single quote is doubled and a backslash is not an escape
    # character. `_escape_trino` is the canonical name; `_escape_presto` is kept
    # so external callers that import it (e.g. dbt-athena) keep working.
    # Deprecated; candidate for removal in a future major release.
    return _escape_trino(val)


_LEADING_COMMENT_PATTERN = re.compile(
    r"^(?:\s*(?:/\*.*?\*/|--[^\n]*(?:\n|$)))+",
    re.DOTALL,
)

# Statements executed by the Hive DDL engine, where the backslash escaping
# convention of _escape_hive() is the correct one. Everything else is assumed
# to be executed by Trino, which requires single quotes to be doubled and
# treats a backslash inside a string literal as an ordinary character.
#
# Defaulting to the Trino escaper is deliberate: it is the fail-safe
# direction. Doubling a quote in a Hive statement is at worst a formatting
# bug, because the Hive lexer concatenates adjacent string literals, while
# backslash-escaping a quote in a Trino statement lets the parameter value
# terminate the string literal and inject SQL.
#
# CREATE TABLE is only treated as Hive DDL when it is not a CTAS statement.
# CREATE TABLE ... AS SELECT and CREATE VIEW are executed by Trino.
_HIVE_STATEMENT_PATTERN = re.compile(
    r"""^(?:
          ALTER\s+(?:DATABASE|SCHEMA|TABLE)\b
        | CREATE\s+(?:DATABASE|SCHEMA)\b
        | CREATE\s+(?:EXTERNAL\s+)?TABLE\b(?!.*\bAS\b)
        | DROP\s+(?:DATABASE|SCHEMA|TABLE)\b
        | MSCK\s+REPAIR\b
        | SHOW\b
        | DESC(?:RIBE)?\b
    )""",
    re.IGNORECASE | re.VERBOSE | re.DOTALL,
)


def _strip_leading_comments(operation: str) -> str:
    """Remove leading SQL comments and whitespace.

    Statement type detection must not be defeated by a leading comment such
    as ``/* generated by ... */ DELETE FROM ...``.
    """
    match = _LEADING_COMMENT_PATTERN.match(operation)
    if match:
        operation = operation[match.end() :]
    return operation.lstrip()


def _get_escaper(operation: str) -> Callable[[str], str]:
    """Select the escaper matching the engine that will parse the statement."""
    if _HIVE_STATEMENT_PATTERN.match(_strip_leading_comments(operation)):
        return _escape_hive
    return _escape_trino


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


def _timestamp_literal(value: datetime) -> str:
    """Render a datetime as an Athena TIMESTAMP literal.

    Values with a sub-millisecond part keep all six fractional digits and
    produce a ``timestamp(6)`` literal. Other values use three digits and
    produce a ``timestamp(3)`` literal.

    Args:
        value: The datetime to render. Its time zone, if any, is not rendered.

    Returns:
        The TIMESTAMP literal.
    """
    text = value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if value.microsecond % 1000 == 0:
        text = text[:-3]
    return f"TIMESTAMP '{text}'"


def _format_datetime(formatter: Formatter, escaper: Callable[[str], str], val: Any) -> Any:
    return _timestamp_literal(val)


def _format_bool(formatter: Formatter, escaper: Callable[[str], str], val: Any) -> Any:
    return str(val)


def _format_str(formatter: Formatter, escaper: Callable[[str], str], val: Any) -> Any:
    return escaper(val)


def _format_binary(
    formatter: Formatter, escaper: Callable[[str], str], val: bytes | bytearray | memoryview
) -> str:
    return f"X'{val.hex()}'"


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


def _format_complex(
    formatter: Formatter, escaper: Callable[[str], str], val: _ComplexParameter
) -> str:
    items = []
    for value in val.values:
        if isinstance(value, (bytes, bytearray)):
            items.append(f"X'{value.hex()}'")
            continue
        processor = formatter.get(value)
        if processor is None:
            raise TypeError(f"{type(value)} is not defined formatter.")
        items.append(str(processor(formatter, escaper, value)))
    opening, closing = ("[", "]") if val.constructor == "ARRAY" else ("(", ")")
    return f"{val.constructor}{opening}{', '.join(items)}{closing}"


_DEFAULT_FORMATTERS: dict[type[Any], Callable[[Formatter, Callable[[str], str], Any], Any]] = {
    _ComplexParameter: _format_complex,
    type(None): _format_none,
    date: _format_date,
    datetime: _format_datetime,
    int: _format_default,
    float: _format_default,
    Decimal: _format_decimal,
    bool: _format_bool,
    str: _format_str,
    bytes: _format_binary,
    bytearray: _format_binary,
    memoryview: _format_binary,
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
        - Binary data: bytes, bytearray, memoryview as hexadecimal literals
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

        escaper = _get_escaper(operation)

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
