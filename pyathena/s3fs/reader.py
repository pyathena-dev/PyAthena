from __future__ import annotations

import csv
from collections.abc import Iterator
from typing import Any


class DefaultCSVReader(Iterator[list[str]]):
    """CSV reader using Python's standard csv module.

    This reader wraps Python's standard csv.reader and treats empty fields
    as empty strings. It does not distinguish between NULL and empty strings
    in Athena's CSV output - both become empty strings.

    Use this reader when you need backward compatibility with the behavior
    where empty strings are treated the same as NULL values.

    Example:
        >>> from io import StringIO
        >>> reader = DefaultCSVReader(StringIO(',"",text'))
        >>> list(reader)
        [['', '', 'text']]  # Both NULL and empty string become ''

    Note:
        The default reader for S3FSCursor is AthenaCSVReader, which
        distinguishes between NULL and empty string values.
    """

    def __init__(self, file_obj: Any, delimiter: str = ",") -> None:
        """Initialize the reader.

        Args:
            file_obj: File-like object to read from.
            delimiter: Field delimiter character.
        """
        self._file: Any | None = file_obj
        self._reader = csv.reader(file_obj, delimiter=delimiter)

    def __iter__(self) -> DefaultCSVReader:
        """Iterate over rows in the CSV file."""
        return self

    def __next__(self) -> list[str]:
        """Read and parse the next line.

        Returns:
            List of field values as strings.

        Raises:
            StopIteration: When end of file is reached or reader is closed.
        """
        if self._file is None:
            raise StopIteration
        row = next(self._reader)
        # Python's csv.reader returns [] for empty lines; normalize to ['']
        # to represent a single empty field (consistent with single-value handling)
        if not row:
            return [""]
        return row

    def close(self) -> None:
        """Close the underlying file object."""
        if self._file is not None:
            self._file.close()
            self._file = None

    def __enter__(self) -> DefaultCSVReader:
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager and close resources."""
        self.close()


class AthenaCSVReader(Iterator[list[str | None]]):
    """CSV reader that distinguishes between NULL and empty string.

    This is the default reader for S3FSCursor.

    Athena's CSV output format distinguishes NULL values from empty strings:
    - NULL: unquoted empty field (e.g., `,,` or `,field`)
    - Empty string: quoted empty field (e.g., `,"",` or `,"",field`)

    Python's standard csv module parses both as empty strings, losing this
    distinction. This reader preserves the difference by returning None for
    NULL values and empty string for quoted empty values.

    Example:
        >>> from io import StringIO
        >>> reader = AthenaCSVReader(StringIO(',"",text'))
        >>> list(reader)
        [[None, '', 'text']]  # NULL and empty string are distinguished

    Note:
        Use DefaultCSVReader if you need backward compatibility where both
        NULL and empty string are treated as empty string.
    """

    def __init__(self, file_obj: Any, delimiter: str = ",") -> None:
        """Initialize the reader.

        Args:
            file_obj: File-like object to read from.
            delimiter: Field delimiter character.
        """
        self._file: Any | None = file_obj
        self._delimiter = delimiter

    def __iter__(self) -> AthenaCSVReader:
        """Iterate over rows in the CSV file."""
        return self

    def __next__(self) -> list[str | None]:
        """Read and parse the next line.

        Returns:
            List of field values, with None for NULL and '' for empty string.

        Raises:
            StopIteration: When end of file is reached or reader is closed.
        """
        if self._file is None:
            raise StopIteration
        line = self._file.readline()
        if not line:
            raise StopIteration

        # Handle multi-line quoted fields: keep reading until quotes are balanced
        # Track quote state incrementally - only scan each new line once
        in_quotes = self._check_quote_state(line)
        while in_quotes:
            next_line = self._file.readline()
            if not next_line:
                # EOF reached with unclosed quote; parse what we have
                break
            line += next_line
            # Only scan the new line, passing current quote state
            in_quotes = self._check_quote_state(next_line, in_quotes)

        return self._parse_line(line.rstrip("\r\n"))

    def _check_quote_state(self, text: str, starting_state: bool = False) -> bool:
        """Check quote state after processing text.

        Args:
            text: Text to scan for quotes.
            starting_state: Whether we start inside a quoted field.

        Returns:
            True if we end inside an unclosed quote.
        """
        in_quotes = starting_state
        i = 0
        while i < len(text):
            if text[i] == '"':
                if in_quotes and i + 1 < len(text) and text[i + 1] == '"':
                    # Escaped quote inside quoted field, skip both
                    i += 2
                    continue
                in_quotes = not in_quotes
            i += 1
        return in_quotes

    def _parse_line(self, line: str) -> list[str | None]:
        """Parse a single CSV line preserving NULL vs empty string distinction.

        Args:
            line: Raw CSV line without trailing newline.

        Returns:
            List of field values.
        """
        # Empty line = single NULL field (e.g., SELECT NULL produces empty data line)
        if not line:
            return [None]

        fields: list[str | None] = []
        pos = 0
        length = len(line)

        while pos < length:
            if line[pos] == '"':
                # Quoted field
                value, pos = self._parse_quoted_field(line, pos)
                fields.append(value)
            else:
                # Unquoted field
                value, pos = self._parse_unquoted_field(line, pos)
                # Unquoted empty field = NULL
                fields.append(None if value == "" else value)

        # Handle trailing empty field (line ends with delimiter)
        if line and line[-1] == self._delimiter:
            fields.append(None)

        return fields

    def _parse_quoted_field(self, line: str, pos: int) -> tuple[str, int]:
        """Parse a quoted field starting at pos.

        Args:
            line: The CSV line.
            pos: Starting position (at the opening quote).

        Returns:
            Tuple of (field value, next position after delimiter).
        """
        pos += 1  # Skip opening quote
        value_parts = []
        length = len(line)

        while pos < length:
            if line[pos] == '"':
                if pos + 1 < length and line[pos + 1] == '"':
                    # Escaped quote
                    value_parts.append('"')
                    pos += 2
                else:
                    # End of quoted field
                    pos += 1  # Skip closing quote
                    break
            else:
                value_parts.append(line[pos])
                pos += 1

        # Skip delimiter if present
        if pos < length and line[pos] == self._delimiter:
            pos += 1

        return "".join(value_parts), pos

    def _parse_unquoted_field(self, line: str, pos: int) -> tuple[str, int]:
        """Parse an unquoted field starting at pos.

        Args:
            line: The CSV line.
            pos: Starting position.

        Returns:
            Tuple of (field value, next position after delimiter).
        """
        start = pos
        length = len(line)

        while pos < length and line[pos] != self._delimiter:
            pos += 1

        value = line[start:pos]

        # Skip delimiter if present
        if pos < length and line[pos] == self._delimiter:
            pos += 1

        return value, pos

    def close(self) -> None:
        """Close the underlying file object."""
        if self._file is not None:
            self._file.close()
            self._file = None

    def __enter__(self) -> AthenaCSVReader:
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager and close resources."""
        self.close()
