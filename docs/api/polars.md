(api_polars)=

# Polars Integration

This section covers Polars-specific cursors, result sets, and data converters.

## Polars Cursors

```{eval-rst}
.. autoclass:: pyathena.polars.cursor.PolarsCursor
   :members:
   :inherited-members:
```

```{eval-rst}
.. autoclass:: pyathena.polars.async_cursor.AsyncPolarsCursor
   :members:
   :inherited-members:
```

## Polars Result Set

```{eval-rst}
.. autoclass:: pyathena.polars.result_set.AthenaPolarsResultSet
   :members:
   :inherited-members:
```

```{eval-rst}
.. autoclass:: pyathena.polars.result_set.PolarsDataFrameIterator
   :members:
```

## Polars Data Converters

```{eval-rst}
.. autoclass:: pyathena.polars.converter.DefaultPolarsTypeConverter
   :members:
```

```{eval-rst}
.. autoclass:: pyathena.polars.converter.DefaultPolarsUnloadTypeConverter
   :members:
```

## Polars Utilities

```{eval-rst}
.. autofunction:: pyathena.polars.util.to_column_info
```

```{eval-rst}
.. autofunction:: pyathena.polars.util.get_athena_type
```
