(api_sqlalchemy)=

# SQLAlchemy Integration

This section covers SQLAlchemy dialect implementations for Amazon Athena.

## Dialects

```{eval-rst}
.. autoclass:: pyathena.sqlalchemy.rest.AthenaRestDialect
   :members:
   :inherited-members:
```

```{eval-rst}
.. autoclass:: pyathena.sqlalchemy.pandas.AthenaPandasDialect
   :members:
   :inherited-members:
```

```{eval-rst}
.. autoclass:: pyathena.sqlalchemy.arrow.AthenaArrowDialect
   :members:
   :inherited-members:
```

## Type System

```{eval-rst}
.. autoclass:: pyathena.sqlalchemy.types.AthenaTimestamp
   :members:
```

```{eval-rst}
.. autoclass:: pyathena.sqlalchemy.types.AthenaDate
   :members:
```

```{eval-rst}
.. autoclass:: pyathena.sqlalchemy.types.Tinyint
   :members:
```

```{eval-rst}
.. autoclass:: pyathena.sqlalchemy.types.AthenaStruct
   :members:
```

```{eval-rst}
.. autoclass:: pyathena.sqlalchemy.types.AthenaMap
   :members:
```

```{eval-rst}
.. autoclass:: pyathena.sqlalchemy.types.AthenaArray
   :members:
```

## Compilers

```{eval-rst}
.. autoclass:: pyathena.sqlalchemy.compiler.AthenaTypeCompiler
   :members:
```

```{eval-rst}
.. autoclass:: pyathena.sqlalchemy.compiler.AthenaStatementCompiler
   :members:
```

```{eval-rst}
.. autoclass:: pyathena.sqlalchemy.compiler.AthenaDDLCompiler
   :members:
```

## Identifier Preparers

```{eval-rst}
.. autoclass:: pyathena.sqlalchemy.preparer.AthenaDMLIdentifierPreparer
   :members:
```

```{eval-rst}
.. autoclass:: pyathena.sqlalchemy.preparer.AthenaDDLIdentifierPreparer
   :members:
```
