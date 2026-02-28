from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any

from pyathena.converter import (
    _DEFAULT_CONVERTERS,
    Converter,
    _to_default,
)

_logger = logging.getLogger(__name__)


class DefaultS3FSTypeConverter(Converter):
    """Type converter for S3FS Cursor results.

    This converter is specifically designed for the S3FSCursor and provides
    type conversion for CSV-based result files read via the S3 FileSystem.
    It converts Athena data types to Python types using the standard
    converter mappings.

    The converter uses the same mappings as DefaultTypeConverter, providing
    consistent behavior with the standard Cursor while using the S3FileSystem
    for file access.

    Example:
        >>> from pyathena.s3fs.converter import DefaultS3FSTypeConverter
        >>> converter = DefaultS3FSTypeConverter()
        >>>
        >>> # Used automatically by S3FSCursor
        >>> cursor = connection.cursor(S3FSCursor)
        >>> # converter is applied automatically to results

    Note:
        This converter is used by default in S3FSCursor.
        Most users don't need to instantiate it directly.
    """

    def __init__(self) -> None:
        super().__init__(
            mappings=deepcopy(_DEFAULT_CONVERTERS),
            default=_to_default,
        )

    def convert(self, type_: str, value: str | None, type_hint: str | None = None) -> Any | None:
        """Convert a string value to the appropriate Python type.

        Looks up the converter function for the given Athena type and applies
        it to the value. If the value is None, returns None without conversion.

        Args:
            type_: The Athena data type name (e.g., "integer", "varchar", "date").
            value: The string value to convert, or None.
            type_hint: Optional Athena DDL type signature for precise complex type
                conversion (e.g., "array(varchar)").

        Returns:
            The converted Python value, or None if the input value was None.
        """
        if value is None:
            return None
        if type_hint:
            from pyathena.converter import DefaultTypeConverter

            # Delegate to DefaultTypeConverter for type_hint-based conversion
            dtc = DefaultTypeConverter()
            return dtc.convert(type_, value, type_hint=type_hint)
        converter = self.get(type_)
        return converter(value)
