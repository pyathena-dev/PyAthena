# PyAthena

<div align="center">

<img src="docs/_static/icon.png" alt="PyAthena logo" width="250">

[![PyPI - Version](https://badge.fury.io/py/pyathena.svg)](https://badge.fury.io/py/pyathena)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/PyAthena.svg)](https://pypi.org/project/PyAthena/)
[![PyPI - Downloads](https://static.pepy.tech/badge/pyathena/month)](https://pepy.tech/project/pyathena)
[![CI - Test](https://github.com/laughingman7743/PyAthena/actions/workflows/test.yaml/badge.svg)](https://github.com/laughingman7743/PyAthena/actions/workflows/test.yaml)
[![CD - Docs](https://github.com/laughingman7743/PyAthena/actions/workflows/docs.yaml/badge.svg)](https://github.com/laughingman7743/PyAthena/actions/workflows/docs.yaml)
[![License - MIT](https://img.shields.io/pypi/l/PyAthena.svg)](https://github.com/laughingman7743/PyAthena/blob/master/LICENSE)
[![linting - Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![types - Mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)

</div>

PyAthena is a Python [DB API 2.0 (PEP 249)](https://www.python.org/dev/peps/pep-0249/) client for [Amazon Athena](https://docs.aws.amazon.com/athena/latest/APIReference/Welcome.html).

-----

## Requirements

* Python

  - CPython 3.10, 3.11, 3.12, 3.13, 3.14

## Installation

```bash
$ pip install PyAthena
```

Extra packages:

| Package    | Install command                      | Version  |
|------------|--------------------------------------|----------|
| SQLAlchemy | `pip install PyAthena[SQLAlchemy]`   | >=1.0.0  |
| Pandas     | `pip install PyAthena[Pandas]`       | >=1.3.0  |
| Arrow      | `pip install PyAthena[Arrow]`        | >=10.0.0 |
| Polars     | `pip install PyAthena[Polars]`       | >=1.0.0  |

## Usage

```python
from pyathena import connect

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()
cursor.execute("SELECT * FROM one_row")
print(cursor.description)
print(cursor.fetchall())
```

## License

[MIT license](LICENSE)

Many of the implementations in this library are based on [PyHive](https://github.com/dropbox/PyHive), thanks for [PyHive](https://github.com/dropbox/PyHive).

## Links

- Documentation: https://laughingman7743.github.io/PyAthena/
- PyPI Releases: https://pypi.org/project/PyAthena/
- Source Code: https://github.com/laughingman7743/PyAthena/
- Issue Tracker: https://github.com/laughingman7743/PyAthena/issues

## Logo

The PyAthena logo was generated using [Nano-Banana Pro](https://deepmind.google/models/gemini-image/pro/) (Gemini 3 Pro Image).
