(api_errors)=

# Exception Handling

This section covers all PyAthena exception classes and error handling.

## Exception Hierarchy

```{eval-rst}
.. automodule:: pyathena.error
   :members:
   :show-inheritance:
```

## Base Exceptions

```{eval-rst}
.. autoclass:: pyathena.error.Error
   :members:
```

```{eval-rst}
.. autoclass:: pyathena.error.Warning
   :members:
```

## Interface Errors

```{eval-rst}
.. autoclass:: pyathena.error.InterfaceError
   :members:
```

```{eval-rst}
.. autoclass:: pyathena.error.DatabaseError
   :members:
```

## Data Errors

```{eval-rst}
.. autoclass:: pyathena.error.DataError
   :members:
```

```{eval-rst}
.. autoclass:: pyathena.error.IntegrityError
   :members:
```

```{eval-rst}
.. autoclass:: pyathena.error.InternalError
   :members:
```

## Operational Errors

```{eval-rst}
.. autoclass:: pyathena.error.OperationalError
   :members:
```

```{eval-rst}
.. autoclass:: pyathena.error.ProgrammingError
   :members:
```

```{eval-rst}
.. autoclass:: pyathena.error.NotSupportedError
   :members:
```
