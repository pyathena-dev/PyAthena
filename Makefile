RUFF_VERSION := 0.14.14
TOX_VERSION := 4.34.1

.PHONY: fmt
fmt:
	# TODO: https://github.com/astral-sh/uv/issues/5903
	uvx ruff@$(RUFF_VERSION) check --select I --fix .
	uvx ruff@$(RUFF_VERSION) format .

.PHONY: chk
chk:
	uvx ruff@$(RUFF_VERSION) check .
	uvx ruff@$(RUFF_VERSION) format --check .
	uv run mypy .

.PHONY: test
test: chk
	uv run pytest -n 8 --cov pyathena --cov-report html --cov-report term tests/pyathena/

.PHONY: test-sqla
test-sqla:
	uv run pytest -n 8 --cov pyathena --cov-report html --cov-report term tests/sqlalchemy/

.PHONY: test-sqla-async
test-sqla-async:
	uv run pytest -n 8 --cov pyathena --cov-report html --cov-report term tests/sqlalchemy/ --dburi async

.PHONY: tox
tox:
	uvx tox@$(TOX_VERSION) -c pyproject.toml run

.PHONY: docs
docs:
	uv run sphinx-multiversion docs docs/_build/html
	echo '<meta http-equiv="refresh" content="0; url=./master/index.html">' > docs/_build/html/index.html
	echo 'pyathena.dev' > docs/_build/html/CNAME
	touch docs/_build/html/.nojekyll

.PHONY: tool
tool:
	uv tool install ruff@$(RUFF_VERSION)
	uv tool install tox@$(TOX_VERSION) --with tox-uv --with tox-gh-actions
