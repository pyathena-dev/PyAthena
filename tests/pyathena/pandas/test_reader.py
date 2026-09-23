# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import csv
from io import BufferedReader, StringIO, TextIOWrapper

import pytest

from pyathena.pandas.reader import _BINARY_NULL, BinaryCSVReader


class TestBinaryCSVReader:
    """Tests for preserving binary NULL fields in Athena CSV results."""

    @pytest.mark.parametrize("buffer_size", [1, 7, 8192])
    def test_null_vs_empty_binary(self, buffer_size):
        source = StringIO(
            '"first","text","last"\n,"comma, quote"" and\r\nnewline",""\n"00 ff","日本語",\n'
        )
        with TextIOWrapper(
            BufferedReader(BinaryCSVReader(source, {0, 2}), buffer_size), newline=""
        ) as stream:
            assert list(csv.reader(stream)) == [
                ["first", "text", "last"],
                [_BINARY_NULL, 'comma, quote" and\r\nnewline', ""],
                ["00 ff", "日本語", _BINARY_NULL],
            ]
        assert source.closed

    def test_close_before_eof(self):
        source = StringIO('"value"\n"00 ff"\n')
        with BinaryCSVReader(source, {0}) as stream:
            assert stream.read(1) == b'"'
        assert source.closed

    def test_preserves_other_fields(self):
        source = StringIO('"binary","text"\r\n,unquoted\r\n"",\r\n')
        with BinaryCSVReader(source, {0}) as stream:
            assert stream.read().decode() == (
                f'"binary","text"\r\n{_BINARY_NULL},unquoted\r\n"",\r\n'
            )
