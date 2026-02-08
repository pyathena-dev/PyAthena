(api_polars)=

# Polars Integration

This section covers Polars-specific cursors, result sets, and data converters.

## Polars Cursors

```{autoclass} pyathena.polars.cursor.PolarsCursor
:members:
:inherited-members:
```

```{autoclass} pyathena.polars.async_cursor.AsyncPolarsCursor
:members:
:inherited-members:
```

## Polars Result Set

```{autoclass} pyathena.polars.result_set.AthenaPolarsResultSet
:members:
:inherited-members:
```

```{autoclass} pyathena.polars.result_set.PolarsDataFrameIterator
:members:
```

## Polars Data Converters

```{autoclass} pyathena.polars.converter.DefaultPolarsTypeConverter
:members:
```

```{autoclass} pyathena.polars.converter.DefaultPolarsUnloadTypeConverter
:members:
```

## Polars Utilities

```{autofunction} pyathena.polars.util.to_column_info
```

```{autofunction} pyathena.polars.util.get_athena_type
```
