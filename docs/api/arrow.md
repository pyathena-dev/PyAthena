(api_arrow)=

# Apache Arrow Integration

This section covers Apache Arrow-specific cursors, result sets, and data converters.

## Arrow Cursors

```{eval-rst}
.. autoclass:: pyathena.arrow.cursor.ArrowCursor
   :members:
   :inherited-members:
```

```{eval-rst}
.. autoclass:: pyathena.arrow.async_cursor.AsyncArrowCursor
   :members:
   :inherited-members:
```

## Arrow Result Set

```{eval-rst}
.. autoclass:: pyathena.arrow.result_set.AthenaArrowResultSet
   :members:
   :inherited-members:
```

## Arrow Data Converters

```{eval-rst}
.. autoclass:: pyathena.arrow.converter.DefaultArrowTypeConverter
   :members:
```

```{eval-rst}
.. autoclass:: pyathena.arrow.converter.DefaultArrowUnloadTypeConverter
   :members:
```

## Arrow Utilities

```{eval-rst}
.. autofunction:: pyathena.arrow.util.to_column_info
```

```{eval-rst}
.. autofunction:: pyathena.arrow.util.get_athena_type
```
