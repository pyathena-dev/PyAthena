from pyathena.polars.converter import DefaultPolarsUnloadTypeConverter


class TestDefaultPolarsUnloadTypeConverter:
    def test_convert_delegates_to_default(self):
        """convert() dispatches through the default converter instead of returning None."""
        converter = DefaultPolarsUnloadTypeConverter()
        assert converter.convert("varchar", "hello") == "hello"
