# Copyright 2024 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

RUFF_VERSION := "0.14.14"
TOX_VERSION := "4.34.1"
# Rerun a test once when Athena fails a query with a service-side error (#804).
PYTEST_RERUN := "--reruns 1 --rerun-show-tracebacks --only-rerun 'Amazon Athena experienced an internal error' --only-rerun 'Invalid S3 request'"

# List available recipes
default:
    @just --list

# Link the main checkout's gitignored .env into a worktree
worktree-env:
    scripts/worktree-env.sh

# Auto-fix formatting and imports
format:
    # TODO: https://github.com/astral-sh/uv/issues/5903
    uvx ruff@{{RUFF_VERSION}} check --select I --fix .
    uvx ruff@{{RUFF_VERSION}} format .

# Lint, format check, mypy, and CloudFormation validation
lint:
    uvx ruff@{{RUFF_VERSION}} check .
    uvx ruff@{{RUFF_VERSION}} format --check .
    uv run mypy .
    uv run cfn-lint cloudformation/*.yaml

# Run tests: just test (pyathena|sqla|sqla-async)
test target="help":
    @just _test-{{ if target =~ "^-" { "help" } else { target } }}

_test-help:
    @echo "Usage: just test <target>"
    @echo ""
    @echo "Targets:"
    @echo "  pyathena    Run unit tests (runs lint first)"
    @echo "  sqla        Run SQLAlchemy dialect tests"
    @echo "  sqla-async  Run SQLAlchemy async dialect tests"

_test-pyathena: lint
    uv run pytest -n 8 {{PYTEST_RERUN}} --cov pyathena --cov-report html --cov-report term tests/pyathena/

_test-sqla:
    uv run pytest -n 8 {{PYTEST_RERUN}} --cov pyathena --cov-report html --cov-report term tests/sqlalchemy/

_test-sqla-async:
    uv run pytest -n 8 {{PYTEST_RERUN}} --cov pyathena --cov-report html --cov-report term tests/sqlalchemy/ --dburi async

# Run tests across multiple Python versions with tox
tox:
    uvx tox@{{TOX_VERSION}} -c pyproject.toml run

# Docs: just docs (build|lint|format)
docs target="help":
    @just _docs-{{ if target =~ "^-" { "help" } else { target } }}

_docs-help:
    @echo "Usage: just docs <target>"
    @echo ""
    @echo "Targets:"
    @echo "  build   Build the Sphinx documentation site (docs/_build/html)"
    @echo "  lint    Lint Markdown with markdownlint-cli2"
    @echo "  format  Auto-fix Markdown with markdownlint-cli2"

_docs-build:
    uv run sphinx-multiversion docs docs/_build/html
    echo '<meta http-equiv="refresh" content="0; url=./master/index.html">' > docs/_build/html/index.html
    echo 'pyathena.dev' > docs/_build/html/CNAME
    touch docs/_build/html/.nojekyll

_docs-lint:
    mise exec -- markdownlint-cli2

_docs-format:
    mise exec -- markdownlint-cli2 --fix

# Benchmark workspace member with its own Python 3.12 environment
BENCHMARK_UV := "UV_PROJECT_ENVIRONMENT='" + justfile_directory() + "/benchmarks/.venv' UV_PYTHON=3.12 uv run --directory benchmarks --locked"

# Standalone benchmark checks; these do not execute Athena queries
benchmark target="lint":
    @just _benchmark-{{ target }}

_benchmark-format:
    uvx ruff@{{RUFF_VERSION}} check --config benchmarks/pyproject.toml --select I --fix benchmarks
    uvx ruff@{{RUFF_VERSION}} format --config benchmarks/pyproject.toml benchmarks

_benchmark-lint:
    uvx ruff@{{RUFF_VERSION}} check --config benchmarks/pyproject.toml benchmarks
    uvx ruff@{{RUFF_VERSION}} format --check --config benchmarks/pyproject.toml benchmarks
    {{BENCHMARK_UV}} mypy
    {{BENCHMARK_UV}} cfn-lint cloudformation/benchmark.yaml

_benchmark-test: _benchmark-lint
    {{BENCHMARK_UV}} pytest

# Install development tools
tool:
    uv tool install ruff@{{RUFF_VERSION}}
    uv tool install tox@{{TOX_VERSION}} --with tox-uv --with tox-gh-actions

# Check repository scripts without accessing AWS
scripts: lint
    mise exec -- shellcheck scripts/*.sh
    mise exec -- actionlint
    uv run --locked python -m pytest scripts/tests/ -q
