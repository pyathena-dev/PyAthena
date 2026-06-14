RUFF_VERSION := "0.14.14"
TOX_VERSION := "4.34.1"

# List available recipes
default:
    @just --list

# Auto-fix formatting and imports
format:
    # TODO: https://github.com/astral-sh/uv/issues/5903
    uvx ruff@{{RUFF_VERSION}} check --select I --fix .
    uvx ruff@{{RUFF_VERSION}} format .

# Lint + format check + mypy
lint:
    uvx ruff@{{RUFF_VERSION}} check .
    uvx ruff@{{RUFF_VERSION}} format --check .
    uv run mypy .

# Run tests: just test (pyathena|sqla|sqla-async)
test target:
    @just _test-{{target}}

_test-pyathena: lint
    uv run pytest -n 8 --cov pyathena --cov-report html --cov-report term tests/pyathena/

_test-sqla:
    uv run pytest -n 8 --cov pyathena --cov-report html --cov-report term tests/sqlalchemy/

_test-sqla-async:
    uv run pytest -n 8 --cov pyathena --cov-report html --cov-report term tests/sqlalchemy/ --dburi async

# Run tests across multiple Python versions with tox
tox:
    uvx tox@{{TOX_VERSION}} -c pyproject.toml run

# Docs: just docs (build|lint|format)
docs target:
    @just _docs-{{target}}

_docs-build:
    uv run sphinx-multiversion docs docs/_build/html
    echo '<meta http-equiv="refresh" content="0; url=./master/index.html">' > docs/_build/html/index.html
    echo 'pyathena.dev' > docs/_build/html/CNAME
    touch docs/_build/html/.nojekyll

_docs-lint:
    mise exec -- markdownlint-cli2

_docs-format:
    mise exec -- markdownlint-cli2 --fix

# Install development tools
tool:
    uv tool install ruff@{{RUFF_VERSION}}
    uv tool install tox@{{TOX_VERSION}} --with tox-uv --with tox-gh-actions
