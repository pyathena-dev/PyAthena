# PyAthena Development Guide for AI Assistants

## Project Overview
PyAthena is a Python DB API 2.0 (PEP 249) compliant client library for Amazon Athena. It enables Python applications to execute SQL queries against data stored in S3 using AWS Athena's serverless query engine.

**License**: MIT  
**Version**: See `pyathena/__init__.py`  
**Python Support**: See `requires-python` in `pyproject.toml`

## Key Architectural Principles

### 1. DB API 2.0 Compliance
- Strictly follow PEP 249 specifications for all cursor and connection implementations
- Maintain compatibility with standard Python database usage patterns
- All cursor implementations must support the standard methods: `execute()`, `fetchone()`, `fetchmany()`, `fetchall()`, `close()`

### 2. Multiple Cursor Types
The project supports different cursor implementations for various use cases:
- **Standard Cursor** (`pyathena.cursor.Cursor`): Basic DB API cursor
- **Async Cursor** (`pyathena.async_cursor.AsyncCursor`): For asynchronous operations
- **Pandas Cursor** (`pyathena.pandas.cursor.PandasCursor`): Returns results as DataFrames
- **Arrow Cursor** (`pyathena.arrow.cursor.ArrowCursor`): Returns results in Apache Arrow format
- **Spark Cursor** (`pyathena.spark.cursor.SparkCursor`): For PySpark integration

### 3. Type System and Conversion
- Data type conversion is handled in `pyathena/converter.py`
- Custom converters can be registered for specific Athena data types
- Always preserve type safety and handle NULL values appropriately
- Follow the type mapping defined in the converters for each cursor type

## Development Guidelines

### Git Workflow

**CRITICAL: Never Commit Directly to Master Branch**
- **NEVER** commit directly to the `master` branch
- **ALWAYS** create a feature branch for any changes
- **ALWAYS** create a Pull Request (PR) for review
- Use descriptive branch names (e.g., `feature/add-converter`, `fix/null-handling`)
- Create PRs as drafts using `gh pr create --draft`

### Code Style and Quality

#### Import Guidelines
**CRITICAL: Runtime Imports are Prohibited**
- **NEVER** use `import` or `from ... import` statements inside functions, methods, or conditional blocks
- **ALWAYS** place all imports at the top of the file, after the license header and module docstring
- This applies to all files: source code, tests, scripts, documentation examples
- Runtime imports cause issues with static analysis, code completion, dependency tracking, and can mask import errors

**Bad Examples:**
```python
def my_function():
    from some_module import something  # NEVER do this
    import os  # NEVER do this
    if condition:
        from optional import feature  # NEVER do this
```

**Good Examples:**
```python
# At the top of the file, after license header
from __future__ import annotations

import os
from some_module import something
from typing import Optional

# Optional dependencies can be handled with TYPE_CHECKING
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from optional import feature

def my_function():
    # Use imported modules here
    return something.process()
```

**Exception for Optional Dependencies**: The PyAthena codebase does use runtime imports for optional dependencies like `pyarrow` and `pandas` in the main source code. However, when contributing new code or modifying tests, avoid runtime imports unless absolutely necessary for optional dependency handling.

#### Commands
```bash
# Format code (auto-fix imports and format)
make fmt

# Run all checks (lint, format check, type check)
make chk

# Run tests (includes running checks first)
make test

# Run SQLAlchemy-specific tests
make test-sqla

# Run full test suite with tox
make tox

# Build documentation
make docs
```

#### Docstring Style
Use Google style docstrings for all public methods and complex internal methods:

```python
def method_name(self, param1: str, param2: Optional[int] = None) -> List[str]:
    """Brief description of what the method does.

    Longer description if needed, explaining the method's behavior,
    edge cases, or important details.

    Args:
        param1: Description of the first parameter.
        param2: Description of the optional parameter.

    Returns:
        Description of the return value.

    Raises:
        ValueError: When invalid parameters are provided.
    """
```

### Testing Requirements

#### General Guidelines
1. **Unit Tests**: All new features must include unit tests
2. **Integration Tests**: Test actual AWS Athena interactions when modifying query execution logic
3. **SQLAlchemy Compliance**: Ensure SQLAlchemy dialect tests pass when modifying dialect code
4. **Mock AWS Services**: Use `moto` or similar for testing AWS interactions without real resources
5. **LINT First**: **ALWAYS** run `make chk` before running tests - ensure code passes all quality checks first

#### Local Testing Environment
To run tests locally, you need to set the following environment variables:

```bash
export AWS_DEFAULT_REGION=<your-region>
export AWS_ATHENA_S3_STAGING_DIR=s3://<your-bucket>/<path>/
export AWS_ATHENA_WORKGROUP=<your-workgroup>
export AWS_ATHENA_SPARK_WORKGROUP=<your-spark-workgroup>
```

**Using .env file (Recommended)**:
Create a `.env` file in the project root (already in `.gitignore`) with your AWS settings, then load it before running tests:

```bash
# Load .env and run tests
export $(cat .env | xargs) && uv run pytest tests/pyathena/test_file.py -v
```

**CRITICAL: Pre-test Requirements**
```bash
# ALWAYS run quality checks first - tests will fail if code doesn't pass lint
make chk

# Only after lint passes, install dependencies and run tests
uv sync
export $(cat .env | xargs) && uv run pytest tests/pyathena/test_file.py -v
```

#### Writing Tests
- Place tests in `tests/pyathena/` mirroring the source structure
- Use pytest fixtures for common setup (see `conftest.py`)
- Test both success and error cases
- For filesystem operations, test edge cases like empty results, missing files, etc.

Example test structure:
```python
def test_find_maxdepth(self, fs):
    """Test find with maxdepth parameter."""
    # Setup test data
    dir_ = f"s3://{ENV.s3_staging_bucket}/test_path"
    fs.touch(f"{dir_}/file0.txt")
    fs.touch(f"{dir_}/level1/file1.txt")
    
    # Test maxdepth=0
    result = fs.find(dir_, maxdepth=0)
    assert len(result) == 1
    assert fs._strip_protocol(f"{dir_}/file0.txt") in result
    
    # Test edge cases and error conditions
    with pytest.raises(ValueError):
        fs.find("s3://", maxdepth=0)
```

#### Test Organization
- Group related tests in classes (e.g., `TestS3FileSystem`)
- Use descriptive test names that explain what is being tested
- Keep tests focused and independent
- Clean up test data after each test when using real AWS resources

### Common Development Tasks

#### Adding a New Feature
1. Check if it aligns with DB API 2.0 specifications
2. Consider impact on all cursor types (standard, pandas, arrow, spark)
3. Update type hints and ensure mypy passes
4. Add comprehensive tests
5. Update documentation if adding public APIs

#### Modifying Query Execution
- The core query execution logic is in `cursor.py` and `async_cursor.py`
- Always handle query cancellation properly (SIGINT should cancel running queries)
- Respect the `kill_on_interrupt` parameter
- Maintain compatibility with Athena engine versions 2 and 3

#### Working with AWS Services
- All AWS interactions use `boto3`
- Credentials are managed through standard AWS credential chain
- Always handle AWS exceptions appropriately (see `error.py`)
- S3 operations for result retrieval are in `result_set.py`

### Project Structure Conventions

```
pyathena/
├── {cursor_type}/         # Cursor-specific implementations
│   ├── __init__.py
│   ├── cursor.py          # Cursor implementation
│   ├── converter.py       # Type converters
│   └── result_set.py      # Result handling
│
├── sqlalchemy/            # SQLAlchemy dialect implementations
│   ├── base.py           # Base dialect
│   ├── {dialect}.py      # Specific dialects (rest, pandas, arrow)
│   └── requirements.py   # SQLAlchemy requirements
│
└── filesystem/           # S3 filesystem abstractions
    ├── s3.py             # S3FileSystem implementation (fsspec compatible)
    └── s3_object.py      # S3 object representations
```

### Important Implementation Details

#### Parameter Formatting
- Two parameter styles supported: `pyformat` (default) and `qmark`
- Parameter formatting logic in `formatter.py`
- PyFormat: `%(name)s` style
- Qmark: `?` style
- Always escape special characters in parameter values

#### Result Set Handling
- Results are typically staged in S3 (configured via `s3_staging_dir`)
- Large result sets should be streamed, not loaded entirely into memory
- Different result set implementations for different data formats (CSV, JSON, Parquet)

#### Error Handling
- All exceptions inherit from `pyathena.error.Error`
- Follow DB API 2.0 exception hierarchy
- Provide meaningful error messages that include Athena query IDs when available

#### S3 FileSystem Operations
- `S3FileSystem` implements fsspec's `AbstractFileSystem` interface
- Key methods include `ls()`, `find()`, `get()`, `put()`, `rm()`, etc.
- `find()` method supports:
  - `maxdepth`: Limits directory traversal depth (uses recursive approach for efficiency)
  - `withdirs`: Controls whether directories are included in results (default: False)
- Cache management uses `(path, delimiter)` as key to handle different listing modes
- Always extract reusable logic into helper methods (e.g., `_extract_parent_directories()`)

When implementing filesystem methods:
1. **Consider s3fs compatibility** - Many users migrate from s3fs, so matching its behavior is important
2. **Optimize for S3's API** - Use delimiter="/" for recursive operations to minimize API calls
3. **Handle edge cases** - Empty paths, trailing slashes, bucket-only paths
4. **Test with real S3** - Mock tests may not catch S3-specific behaviors

### Performance Considerations
1. **Result Caching**: Utilize Athena's result reuse feature (engine v3) when possible
2. **Batch Operations**: Support `executemany()` for bulk operations
3. **Memory Efficiency**: Stream large results instead of loading all into memory
4. **Connection Pooling**: Connections are relatively lightweight, but avoid creating excessive connections

### Security Best Practices
1. **Never log sensitive data** (credentials, query results with PII)
2. **Support encryption** (SSE-S3, SSE-KMS, CSE-KMS) for S3 operations
3. **Validate and sanitize** all user inputs, especially in query construction
4. **Use parameterized queries** to prevent SQL injection

### Debugging Tips
1. Enable debug logging: `logging.getLogger("pyathena").setLevel(logging.DEBUG)`
2. Check Athena query history in AWS Console for failed queries
3. Verify S3 permissions for both staging directory and data access
4. Use `EXPLAIN` or `SHOW` statements to debug query plans

### Common Pitfalls to Avoid
1. Don't assume all Athena data types map directly to Python types
2. Remember that Athena queries are asynchronous - always wait for completion
3. Handle the case where S3 results might be deleted or inaccessible
4. Don't forget to close cursors and connections to clean up resources
5. Be aware of Athena service quotas and rate limits

### Release Process
1. Update version in `pyathena/__init__.py`
2. Ensure all tests pass
3. Create a git tag for the release
4. Build and publish to PyPI

## Contact and Resources
- **Repository**: https://github.com/laughingman7743/PyAthena
- **Documentation**: https://laughingman7743.github.io/PyAthena/
- **Issues**: Report bugs or request features via GitHub Issues
- **AWS Athena Docs**: https://docs.aws.amazon.com/athena/
