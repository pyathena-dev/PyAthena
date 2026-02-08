(api_errors)=

# Exception Handling

This section covers all PyAthena exception classes and error handling.

## Exception Hierarchy

```{automodule} pyathena.error
:members:
:show-inheritance:
```

## Base Exceptions

```{autoclass} pyathena.error.Error
:members:
```

```{autoclass} pyathena.error.Warning
:members:
```

## Interface Errors

```{autoclass} pyathena.error.InterfaceError
:members:
```

```{autoclass} pyathena.error.DatabaseError
:members:
```

## Data Errors

```{autoclass} pyathena.error.DataError
:members:
```

```{autoclass} pyathena.error.IntegrityError
:members:
```

```{autoclass} pyathena.error.InternalError
:members:
```

## Operational Errors

```{autoclass} pyathena.error.OperationalError
:members:
```

```{autoclass} pyathena.error.ProgrammingError
:members:
```

```{autoclass} pyathena.error.NotSupportedError
:members:
```
