# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from __future__ import annotations

import re
from io import RawIOBase
from typing import Any

from pyathena.s3fs.reader import AthenaCSVReader

_BINARY_NULL = "__PYATHENA_BINARY_NULL__"
_CSV_FIELD = re.compile(r'(?:^|,)(?P<value>"[^"]*(?:""[^"]*)*"|[^,]*)')


class BinaryCSVReader(RawIOBase):
    """Preserve binary NULL fields before pandas discards CSV quoting information.

    The marker cannot occur in Athena's hexadecimal encoding of binary values.
    Only unquoted empty binary fields are rewritten; all other CSV text is
    passed through unchanged. Records are streamed to support chunked reads.
    """

    def __init__(self, stream: Any, binary_columns: set[int]) -> None:
        super().__init__()
        self._reader = AthenaCSVReader(stream)
        self._binary_columns = binary_columns
        self._header = True
        self._buffer = b""

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if not len(buffer):
            return 0
        if not self._buffer:
            try:
                record = self._reader._read_record()
            except StopIteration:
                self._reader.close()
                return 0
            if self._header:
                self._header = False
            else:
                parts: list[str] = []
                start = 0
                for index, field in enumerate(_CSV_FIELD.finditer(record.rstrip("\r\n"))):
                    if index in self._binary_columns and not field.group("value"):
                        pos = field.start("value")
                        parts.extend((record[start:pos], _BINARY_NULL))
                        start = pos
                parts.append(record[start:])
                record = "".join(parts)
            self._buffer = record.encode("utf-8")
        size = min(len(buffer), len(self._buffer))
        buffer[:size] = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return size

    def close(self) -> None:
        try:
            self._reader.close()
        finally:
            super().close()
