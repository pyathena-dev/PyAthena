(api_pandas)=

# Pandas Integration

This section covers pandas-specific cursors, result sets, and data converters.

## Pandas Cursors

```{eval-rst}
.. autoclass:: pyathena.pandas.cursor.PandasCursor
   :members:
   :inherited-members:
```

```{eval-rst}
.. autoclass:: pyathena.pandas.async_cursor.AsyncPandasCursor
   :members:
   :inherited-members:
```

## Pandas Result Set

```{eval-rst}
.. autoclass:: pyathena.pandas.result_set.AthenaPandasResultSet
   :members:
   :inherited-members:
```

```{eval-rst}
.. autoclass:: pyathena.pandas.result_set.PandasDataFrameIterator
   :members:
```

## Pandas Data Converters

```{eval-rst}
.. autoclass:: pyathena.pandas.converter.DefaultPandasTypeConverter
   :members:
```

```{eval-rst}
.. autoclass:: pyathena.pandas.converter.DefaultPandasUnloadTypeConverter
   :members:
```

## Pandas Utilities

```{eval-rst}
.. autofunction:: pyathena.pandas.util.get_chunks
```

```{eval-rst}
.. autofunction:: pyathena.pandas.util.reset_index
```

```{eval-rst}
.. autofunction:: pyathena.pandas.util.as_pandas
```

```{eval-rst}
.. autofunction:: pyathena.pandas.util.to_sql_type_mappings
```

```{eval-rst}
.. autofunction:: pyathena.pandas.util.to_parquet
```

```{eval-rst}
.. autofunction:: pyathena.pandas.util.to_sql
```

```{eval-rst}
.. autofunction:: pyathena.pandas.util.get_column_names_and_types
```

```{eval-rst}
.. autofunction:: pyathena.pandas.util.generate_ddl
```
