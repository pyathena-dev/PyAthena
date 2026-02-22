from collections.abc import Iterator
from io import StringIO

from pyathena.s3fs.reader import AthenaCSVReader, DefaultCSVReader


class TestDefaultCSVReader:
    """Tests for DefaultCSVReader using Python's standard csv module."""

    def test_basic_parsing(self):
        data = StringIO("a,b,c\n1,2,3\n")
        reader = DefaultCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [["a", "b", "c"], ["1", "2", "3"]]

    def test_tab_delimiter(self):
        data = StringIO("a\tb\tc\n1\t2\t3\n")
        reader = DefaultCSVReader(data, delimiter="\t")
        rows = list(reader)
        assert rows == [["a", "b", "c"], ["1", "2", "3"]]

    def test_empty_field_returns_empty_string(self):
        """DefaultCSVReader returns empty string for both NULL and empty string."""
        # Both ,, (NULL) and ,"", (empty string) become ''
        data = StringIO('a,,b,"",c\n')
        reader = DefaultCSVReader(data, delimiter=",")
        rows = list(reader)
        # Standard csv.reader treats both as empty strings
        assert rows == [["a", "", "b", "", "c"]]

    def test_quoted_field_with_comma(self):
        data = StringIO('"a,b",c\n')
        reader = DefaultCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [["a,b", "c"]]

    def test_quoted_field_with_newline(self):
        data = StringIO('"line1\nline2",b\n')
        reader = DefaultCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [["line1\nline2", "b"]]

    def test_escaped_quote(self):
        data = StringIO('"a""b",c\n')
        reader = DefaultCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [['a"b', "c"]]

    def test_empty_file(self):
        data = StringIO("")
        reader = DefaultCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == []

    def test_empty_line(self):
        """Empty line returns single empty string (e.g., SELECT NULL)."""
        # Python's csv.reader returns [] for empty lines; we normalize to ['']
        data = StringIO("\n")
        reader = DefaultCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [[""]]

    def test_implements_iterator_protocol(self):
        """DefaultCSVReader implements collections.abc.Iterator."""
        data = StringIO("a,b\n")
        reader = DefaultCSVReader(data, delimiter=",")
        assert isinstance(reader, Iterator)

    def test_close(self):
        """close() releases file resources."""
        data = StringIO("a,b\n")
        reader = DefaultCSVReader(data, delimiter=",")
        reader.close()
        # After close, iteration should stop immediately
        rows = list(reader)
        assert rows == []

    def test_context_manager(self):
        """Reader can be used as context manager."""
        data = StringIO("a,b\n1,2\n")
        with DefaultCSVReader(data, delimiter=",") as reader:
            rows = list(reader)
        assert rows == [["a", "b"], ["1", "2"]]
        # After exiting context, reader should be closed
        assert list(reader) == []


class TestAthenaCSVReader:
    """Tests for AthenaCSVReader that distinguishes NULL from empty string."""

    def test_basic_parsing(self):
        data = StringIO("a,b,c\n1,2,3\n")
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [["a", "b", "c"], ["1", "2", "3"]]

    def test_tab_delimiter(self):
        data = StringIO("a\tb\tc\n1\t2\t3\n")
        reader = AthenaCSVReader(data, delimiter="\t")
        rows = list(reader)
        assert rows == [["a", "b", "c"], ["1", "2", "3"]]

    def test_null_vs_empty_string(self):
        """AthenaCSVReader distinguishes NULL (unquoted empty) from empty string (quoted empty)."""
        # ,, is NULL, ,"", is empty string
        data = StringIO('a,,b,"",c\n')
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        # Field at index 1 is NULL (unquoted), field at index 3 is empty string (quoted)
        assert rows == [["a", None, "b", "", "c"]]

    def test_null_at_start(self):
        """NULL value at the start of a line."""
        data = StringIO(",b,c\n")
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [[None, "b", "c"]]

    def test_null_at_end(self):
        """NULL value at the end of a line (trailing comma)."""
        data = StringIO("a,b,\n")
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [["a", "b", None]]

    def test_no_trailing_null(self):
        """No trailing NULL when line ends with value."""
        data = StringIO("a,b,c\n")
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [["a", "b", "c"]]

    def test_empty_string_at_start(self):
        """Empty string at the start of a line."""
        data = StringIO('"",b,c\n')
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [["", "b", "c"]]

    def test_empty_string_at_end(self):
        """Empty string at the end of a line."""
        data = StringIO('a,b,""\n')
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [["a", "b", ""]]

    def test_all_nulls(self):
        """Row with all NULL values."""
        data = StringIO(",,\n")
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [[None, None, None]]

    def test_all_empty_strings(self):
        """Row with all empty string values."""
        data = StringIO('"","",""\n')
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [["", "", ""]]

    def test_quoted_field_with_comma(self):
        data = StringIO('"a,b",c\n')
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [["a,b", "c"]]

    def test_quoted_field_with_delimiter(self):
        """Quoted field containing the tab delimiter."""
        data = StringIO('"a\tb"\tc\n')
        reader = AthenaCSVReader(data, delimiter="\t")
        rows = list(reader)
        assert rows == [["a\tb", "c"]]

    def test_escaped_quote(self):
        """Escaped quote inside quoted field."""
        data = StringIO('"a""b",c\n')
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [['a"b', "c"]]

    def test_empty_file(self):
        data = StringIO("")
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == []

    def test_mixed_null_and_values(self):
        """Multiple rows with mixed NULL and regular values."""
        data = StringIO('1,,"text"\n,4,\n')
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [["1", None, "text"], [None, "4", None]]

    def test_crlf_line_endings(self):
        """Handle Windows-style line endings."""
        data = StringIO("a,b,c\r\n1,2,3\r\n")
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [["a", "b", "c"], ["1", "2", "3"]]

    def test_single_null_empty_line(self):
        """Empty line represents a single NULL value (e.g., SELECT NULL)."""
        # Athena outputs empty line for single NULL value after header
        data = StringIO("\n")
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [[None]]

    def test_quoted_field_with_newline(self):
        """Quoted field containing a newline character."""
        data = StringIO('"line1\nline2",b\n')
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [["line1\nline2", "b"]]

    def test_quoted_field_with_multiple_newlines(self):
        """Quoted field containing multiple newline characters."""
        data = StringIO('"line1\nline2\nline3",b,c\n')
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [["line1\nline2\nline3", "b", "c"]]

    def test_quoted_field_with_crlf(self):
        """Quoted field containing CRLF line ending."""
        data = StringIO('"line1\r\nline2",b\n')
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [["line1\r\nline2", "b"]]

    def test_multiple_rows_with_multiline_field(self):
        """Multiple rows where some have multi-line quoted fields."""
        data = StringIO('a,b\n"multi\nline",c\nd,e\n')
        reader = AthenaCSVReader(data, delimiter=",")
        rows = list(reader)
        assert rows == [["a", "b"], ["multi\nline", "c"], ["d", "e"]]

    def test_implements_iterator_protocol(self):
        """AthenaCSVReader implements collections.abc.Iterator."""
        data = StringIO("a,b\n")
        reader = AthenaCSVReader(data, delimiter=",")
        assert isinstance(reader, Iterator)

    def test_close(self):
        """close() releases file resources."""
        data = StringIO("a,b\n")
        reader = AthenaCSVReader(data, delimiter=",")
        reader.close()
        # After close, iteration should stop immediately
        rows = list(reader)
        assert rows == []

    def test_context_manager(self):
        """Reader can be used as context manager."""
        data = StringIO("a,b\n1,2\n")
        with AthenaCSVReader(data, delimiter=",") as reader:
            rows = list(reader)
        assert rows == [["a", "b"], ["1", "2"]]
        # After exiting context, reader should be closed
        assert list(reader) == []
