# PyAthena Development Guide for AI Assistants

## Project Overview
PyAthena is a Python DB API 2.0 (PEP 249) compliant client for Amazon Athena. See `pyproject.toml` for Python version support and dependencies.

## Rules and Constraints

### Git Workflow
- **NEVER** commit directly to `master` — always create a feature branch and PR
- Create PRs as drafts: `gh pr create --draft`

### Import Rules
- **NEVER** use runtime imports (inside functions, methods, or conditional blocks)
- All imports must be at the top of the file, after the license header
- Exception: the existing codebase uses runtime imports for optional dependencies (`pyarrow`, `pandas`, etc.) in source code. For new code, use `TYPE_CHECKING` instead when possible

### Code Quality — Always Run Before Committing
```bash
make fmt   # Auto-fix formatting and imports
make lint   # Lint + format check + mypy
```

### Testing
```bash
# ALWAYS run `make lint` first — tests will fail if lint doesn't pass
make test       # Unit tests (runs chk first)
make test-sqla  # SQLAlchemy dialect tests
```

Tests require AWS environment variables. Use a `.env` file (gitignored):
```bash
AWS_DEFAULT_REGION=<region>
AWS_ATHENA_S3_STAGING_DIR=s3://<bucket>/<path>/
AWS_ATHENA_WORKGROUP=<workgroup>
AWS_ATHENA_SPARK_WORKGROUP=<spark-workgroup>
```
```bash
export $(cat .env | xargs) && uv run pytest tests/pyathena/test_file.py -v
```

- Tests mirror source structure under `tests/pyathena/`
- Use pytest fixtures from `conftest.py`
- New features require tests; changes to SQLAlchemy dialects must pass `make test-sqla`

## Architecture — Key Design Decisions

These are non-obvious conventions that can't be discovered by reading code alone.

### PEP 249 Compliance
All cursor types must implement: `execute()`, `fetchone()`, `fetchmany()`, `fetchall()`, `close()`. New cursor features must follow the DB API 2.0 specification.

### Cursor Module Pattern
Each cursor type lives in its own subpackage (`pandas/`, `arrow/`, `polars/`, `s3fs/`, `spark/`) with a consistent structure: `cursor.py`, `async_cursor.py`, `converter.py`, `result_set.py`. When adding features, consider impact on all cursor types.

### Filesystem (fsspec) Compatibility
`pyathena/filesystem/s3.py` implements fsspec's `AbstractFileSystem`. When modifying:
- Match `s3fs` library behavior where possible (users migrate from it)
- Use `delimiter="/"` in S3 API calls to minimize requests
- Handle edge cases: empty paths, trailing slashes, bucket-only paths

### Version Management
Versions are derived from git tags via `hatch-vcs` — never edit `pyathena/_version.py` manually.

### Google-style Docstrings
Use Google-style docstrings for public methods. See existing code for examples.
