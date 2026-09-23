<!--
Copyright 2025 The PyAthena authors

Licensed under the MIT License.
See LICENSE or https://opensource.org/licenses/MIT.

SPDX-License-Identifier: MIT
-->

# PyAthena Development Guide for AI Assistants

## Project Overview

PyAthena is a Python DB API 2.0 (PEP 249) compliant client for Amazon Athena. See `pyproject.toml` for Python version support and dependencies.

Follow [CONTRIBUTING.md](CONTRIBUTING.md) and the [contribution guide](docs/contributing.md) for issue-first discussion, contributor validation, AI-assisted work, and new-file license headers.
Preserve existing and upstream copyright notices; do not apply the new original-file header to adapted code without checking provenance.

## Rules and Constraints

### Git Workflow

- **NEVER** commit directly to `master` — always create a feature branch and PR
- Create PRs as drafts: `gh pr create --draft`
- Use a dedicated worktree for each PR and follow the repository's PR template.
- For PR delivery, follow [development-workflow](.agents/skills/development-workflow/SKILL.md): create the draft, complete two distinct self-reviews, collect an independent review, and check current CI before Ready.
- Run [self-review](.agents/skills/self-review/SKILL.md) for behavior and implementation, then [self-review-round-two](.agents/skills/self-review-round-two/SKILL.md) for compatibility, operational consequences, and factual claims.
- Follow [independent-review](.agents/skills/independent-review/SKILL.md) after both rounds; Codex-authored changes use Claude Code `claude-fable-5-1`, profile `max`, effort `high`, never Enterprise.
- Post new review records inline on relevant diff lines using the GitHub review API's `comments` array with an empty review body; record repairs with the review-comment reply API.
- Use `gh` for GitHub operations, and write commits, PR text, and review comments in English.
- A review-only request keeps its specified scope and side-effect limits; it does not start PR delivery.

### Agent Skill Entry Points

The canonical skill definitions live in `.agents/skills/`.
Claude Code loads the same definitions through linked `SKILL.md` files under `.claude/skills/`; `CLAUDE.md` also links to this guide.
Use `/development-workflow`, `/self-review`, `/self-review-round-two`, or `/independent-review` in Claude Code.
Edit the canonical files when maintaining these skills so both agents use the same procedure.

### Import Rules

- **NEVER** use runtime imports (inside functions, methods, or conditional blocks)
- All imports must be at the top of the file, after the license header
- Exception: the existing codebase uses runtime imports for optional dependencies (`pyarrow`, `pandas`, etc.) in source code. For new code, use `TYPE_CHECKING` instead when possible

### Code Quality — Always Run Before Committing

```bash
just format   # Auto-fix formatting and imports
just lint   # Python lint, format check, mypy, and CloudFormation validation
```

### Testing

```bash
# ALWAYS run `just lint` first — tests will fail if lint doesn't pass
just test pyathena    # PyAthena tests, including AWS integration (runs lint first)
just test sqla        # SQLAlchemy compliance suite
just test sqla-async  # SQLAlchemy async compliance suite
```

Tests require AWS environment variables. Use a `.env` file (gitignored):

```bash
AWS_DEFAULT_REGION=<region>
AWS_ATHENA_S3_STAGING_DIR=s3://<bucket>/<path>/
AWS_ATHENA_WORKGROUP=<workgroup>
AWS_ATHENA_SPARK_WORKGROUP=<spark-workgroup>
```

In a git worktree, run `just worktree-env` once to link the main checkout's
gitignored `.env` into the worktree.

```bash
uv run --env-file .env pytest -n 1 tests/pyathena/test_cursor.py -v
```

- Tests mirror source structure under `tests/pyathena/`
- Use pytest fixtures from `conftest.py`
- New features require tests. SQLAlchemy dialect changes must pass the relevant PyAthena tests under `tests/pyathena/sqlalchemy/` as well as `just test sqla`; async changes also need `tests/pyathena/aio/sqlalchemy/` and `just test sqla-async`. Both PyAthena test directories are included in `just test pyathena`.

#### Test Conventions

- **Class-based tests** for integration tests that use fixtures (cursors, engines): `class TestCursor:` with methods like `def test_fetchone(self, cursor):`
- **Standalone functions** for unit tests of pure logic (converters, parsers, utils): `def test_to_struct_json_formats(input_value, expected):`
- Test file naming mirrors source: `pyathena/parser.py` → `tests/pyathena/test_parser.py`
- **Fixtures**: Cursor/engine fixtures are defined in `conftest.py` and injected by name (e.g., `cursor`, `engine`, `async_cursor`). Use `indirect=True` parametrization to pass connection options:

  ```python
  @pytest.mark.parametrize("engine", [{"driver": "rest"}], indirect=True)
  def test_query(self, engine):
      engine, conn = engine
  ```

- **Parametrize** with `@pytest.mark.parametrize(("input", "expected"), [...])` for data-driven tests
- **Integration tests** (need AWS) use cursor/engine fixtures with real Athena queries; **unit tests** (no AWS) call functions directly with test data

### Markdown Lint

`docs/**/*.md`, `benchmarks/*.md`, `scripts/**/*.md`, `cloudformation/**/*.md`, and project-root `*.md` files are linted with [markdownlint-cli2](https://github.com/DavidAnson/markdownlint-cli2). The config lives at `.markdownlint-cli2.jsonc`. CI runs lint + Sphinx build on PRs that touch docs (`.github/workflows/docs-lint.yaml`).

`markdownlint-cli2` is pinned in `.mise.toml`, so [`mise`](https://mise.jdx.dev/) installs the exact version used in CI. Run locally:

```bash
mise install          # one-time: installs markdownlint-cli2
just docs lint        # check
just docs format      # auto-fix what's possible
just docs build       # build the Sphinx site under docs/_build/html
```

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
