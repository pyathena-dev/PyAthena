(pyathena)=

![PyAthena](_static/header.png)

# PyAthena

PyAthena is a Python [DB API 2.0 (PEP 249)](https://www.python.org/dev/peps/pep-0249/) client for [Amazon Athena](https://docs.aws.amazon.com/athena/latest/APIReference/Welcome.html).

## Quick Start

```bash
pip install PyAthena
```

```python
from pyathena import connect

cursor = connect(
    s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
    region_name="us-west-2"
).cursor()

cursor.execute("SELECT 1")
print(cursor.fetchone())
```

(documentation)=

## Documentation

:::{dropdown} Getting Started

```{toctree}
:maxdepth: 2
:caption: Getting Started

introduction
usage
```

:::

:::{dropdown} Cursors

```{toctree}
:maxdepth: 2
:caption: Cursors

cursor
pandas
arrow
polars
s3fs
spark
aio
```

:::

:::{dropdown} Integrations

```{toctree}
:maxdepth: 2
:caption: Integrations

sqlalchemy
```

:::

:::{dropdown} Advanced Topics

```{toctree}
:maxdepth: 2
:caption: Advanced Topics

null_handling
testing
```

:::

:::{dropdown} API Reference

```{toctree}
:maxdepth: 2
:caption: API Reference

api
```

:::
