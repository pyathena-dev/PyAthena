from pyathena.pandas.converter import (
    DefaultPandasTypeConverter,
    DefaultPandasUnloadTypeConverter,
)


class TestDefaultPandasTypeConverter:
    def test_convert_delegates_to_mapping(self):
        """convert() dispatches through self.get(type_) instead of returning None.

        Verifies both the explicit mapping path (boolean → _to_boolean)
        and the default converter path (varchar → _to_default), plus
        None passthrough.
        """
        converter = DefaultPandasTypeConverter()
        assert converter.convert("boolean", "true") is True
        assert converter.convert("varchar", "hello") == "hello"
        assert converter.convert("varchar", None) is None


class TestDefaultPandasUnloadTypeConverter:
    def test_convert_delegates_to_default(self):
        """convert() dispatches through the default converter instead of returning None."""
        converter = DefaultPandasUnloadTypeConverter()
        assert converter.convert("varchar", "hello") == "hello"
