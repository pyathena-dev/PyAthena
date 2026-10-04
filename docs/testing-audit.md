<!--
Copyright 2026 The PyAthena authors

Licensed under the MIT License.
See LICENSE or https://opensource.org/licenses/MIT.

SPDX-License-Identifier: MIT
-->

# Test conventions audit

This report records the source audit for [issue #1079](https://github.com/pyathena-dev/PyAthena/issues/1079).
The baseline is commit `200762088e7b0bac45054f22e32a16aa0ce95dbb`.
Source references below identify that baseline, so later edits do not change their meaning.
The accompanying changes clarify the conventions and address the findings listed here.

## Reviewed areas

The inventory covers all 83 tracked files under `tests/`: 80 Python files, two Jinja query templates, and the SQLAlchemy profiles file.
The Python inventory includes 17 package initializers and all three `conftest.py` files.
Structural inspection covered class and function organization, fixture arguments and decorators, parameter construction, helper placement, assertions, and synchronous and asynchronous counterparts.
Focused source tracing checked the setup and expected-value patterns identified by that inspection.
The complete file inventory appears below.

| Area | Files | Conventions examined |
| --- | ---: | --- |
| Shared PyAthena tests and helpers | 15 | Cursor integration classes; standalone conversion helpers; object-oriented parser, model, result-set, and connection tests; session data and resource setup |
| Native asyncio tests and fixtures | 18 | Async cursor and dialect classes, async result-set helpers, per-test cursor contexts, connection teardown, and backend-specific cases |
| Arrow | 6 | Cursor classes, converter functions and classes, schema and precision assertions, mocked result-set setup |
| pandas | 7 | Cursor classes, function-oriented utility tests, reader lifetimes, CSV options and expected frames, dtype and option-context coverage |
| Polars | 5 | Cursor and converter tests, typed local files, failure after partial reads, and one-shot iterator inputs |
| S3FS cursors and readers | 4 | Cursor fixtures, custom converters, CSV NULL versus empty-string assertions, and reader context managers |
| Spark | 4 | Session and calculation fixtures, event-based concurrency helpers, cancellation assertions, and Future-based async behavior |
| Filesystem | 9 | Class fixtures, stubbed provider calls, path/value parametrization, sync/async operations, cleanup, and tests without observable assertions |
| PyAthena SQLAlchemy tests | 8 | Compiler and type unit tests, engine integration classes, local fixture reuse, SQL expression parameters, and reflected metadata assertions |
| SQLAlchemy compliance suite | 4 | Upstream inheritance, combinations and requirements, plugin hooks, sync/async selection, and explicit skips |
| Shared environment initializer | 1 | Required environment values and per-process schema identity |
| Query templates | 2 | Database setup/teardown, schema interpolation, and license notices |

The compliance suite imports additional tests from the installed SQLAlchemy package.
Those upstream implementations and generated cases are outside the project-owned source inventory.
The audit does not establish complete runtime coverage or the correctness of every assertion.
Unmerged worktrees, including the pandas JSON changes associated with issue #1078, are outside this baseline.

## Findings addressed

The priorities describe coverage and maintenance impact, rather than product-defect severity.

| Priority | Baseline evidence | Impact | Disposition |
| --- | --- | --- | --- |
| High | [tests/pyathena/filesystem/test_s3.py:4432][sync-du] and [tests/pyathena/filesystem/test_s3_async.py:1491][async-du] | Both `test_du` bodies contain only `pass`. A broken disk-usage implementation still produces two passing tests. | Replace both placeholders with real S3 file-size, total, depth, and single-file assertions. Preserve the names, use the existing filesystem fixtures, and remove the test objects in `finally`. |
| Medium | [tests/pyathena/pandas/test_result_set.py:265][csv-helper] and [tests/pyathena/pandas/test_result_set.py:305][csv-params] | Parameter evaluation repeatedly patches result-set properties and derives CSV options, including repeated default dtype construction. An option-building error prevents collection of unrelated tests in the module. | Store input metadata and options in the parameters. Build the options once per invocation, inside the pandas option context. Remove the unused base-initializer patch; `__new__` already bypasses `__init__`. |
| Medium | [tests/pyathena/pandas/test_result_set.py:391][csv-oracle] | The expectation reads the CSV with two engines and conditionally replaces columns and missing values. The reader has to reconstruct the intended contract from this algorithm. | Define typed expected frames from explicit values for the 13 existing cases. Keep both `future.infer_string` settings, `check_exact=True`, column/index checks, and the difference between replacing a dtype mapping and overriding individual entries. |
| Medium | [tests/pyathena/polars/test_result_set.py:259][polars-reader] | A generator is constructed in the parameter definition. Repeating the case can reuse a closed reader, making the post-close empty result a vacuous assertion. | Parameterize the reader kind and construct a fresh DataFrame or generator during each test invocation. Keep both case IDs. |
| Low | [tests/pyathena/pandas/test_result_set.py:79][filesystem-identities] | Module-level mocks are used only as filesystem identities while `read_parquet` is patched. They introduce mutable mock state without asserting any mock behavior. | Use distinct sentinels while preserving the identity and option-precedence assertions. |
| Low | [tests/pyathena/pandas/test_result_set.py:439][unused-dtypes] | The expected CSV parse contributes only a column-name comparison whose expected name is already literal. | Assert the explicit column name and preserved `007` value directly. |
| Medium | [AGENTS.md:84][grouping-rule], [tests/pyathena/test_parser.py:18][parser-group], and [tests/pyathena/pandas/test_util.py:62][utility-integration] | The grouping guidance does not explain established object-oriented unit classes or function-oriented utility tests that use AWS fixtures. Applying it mechanically changes test selection without improving the assertions. | Clarify the default forms and their purposeful exceptions in AGENTS.md and the testing guide. Retain the existing grouping and fixture scopes. |

The all-types expectation shares a typed literal frame across cases.
Its time-only field still uses pandas' datetime conversion to supply today's date, as the existing input requires.
It does not parse a reference CSV or reproduce PyAthena's CSV conversion loop.

## Intentional differences retained

| Evidence | Rationale and disposition |
| --- | --- |
| [tests/pyathena/test_converter.py:19][converter-functions] and [tests/pyathena/test_parser.py:18][parser-group] | Standalone conversion functions and classes grouping a parser object's behavior are both useful. Preserve both forms. |
| [tests/pyathena/pandas/test_util.py:451][utility-writes] | Utility functions that write DataFrames use real cursor fixtures. Their function-oriented grouping does not make them offline tests or require a class conversion. |
| [tests/pyathena/pandas/test_result_set.py:52][whole-read] | Comparing joined chunks with a whole-file pandas read directly expresses the iterator's compatibility contract. Keep this library oracle. |
| [tests/pyathena/pandas/test_result_set.py:139][ddl-read] | The DDL test combines C-engine comparison with literal numeric-looking names, engine selection, and stream-closure assertions. These observable assertions keep the comparison tied to its specific contract. |
| [tests/pyathena/pandas/test_cursor.py:1945][pandas-converter] and [tests/pyathena/s3fs/test_cursor.py:543][s3fs-converter] | These parameters construct separate, configured converter values for explicit default/managed cases. Unlike a one-shot reader, they are not consumed. Keep the fixture inputs and managed-storage skip conditions; a future change that mutates the supplied converter should give it a per-invocation lifetime. |
| [tests/pyathena/sqlalchemy/test_compiler.py:621][compiler-types] | SQLAlchemy type and expression construction is part of the compiler input. Keep these declarative parameters and their dialect variants. Primitive parameter values also have useful generated IDs; custom IDs are needed when the generated names obscure the scenario. |
| [tests/sqlalchemy/test_suite.py:35][upstream-suite] and [tests/sqlalchemy/conftest.py:11][upstream-plugin] | Compliance tests inherit upstream classes and use SQLAlchemy's combinations, requirements, and pytest plugin. Preserve these interfaces and the sync/async database selection. |
| [tests/sqlalchemy/test_suite.py:974][explicit-skips] | Skipped overrides carry concrete Athena limitations. They differ from the unmarked filesystem placeholders because they report missing coverage as skipped. Preserve the reasons and test selection. |
| [NOTICE:29][adapted-tests] | The notice identifies surviving PyHive-derived cursor/dialect tests, including a native-async port. Preserve attribution and framework behavior when reorganizing adapted material. |
| [tests/pyathena/conftest.py:225][cursor-lifetime] and [tests/pyathena/aio/conftest.py:21][aio-lifetime] | Sync and native-async fixtures own their connections and cursor contexts with their corresponding teardown forms. The implementations need different syntax; a shared replacement is not required for consistency. |
| [tests/pyathena/arrow/test_cursor.py:38][arrow-binary] and [tests/pyathena/aio/arrow/test_cursor.py:22][aio-arrow-binary] | Both execution styles assert binary NULL versus empty bytes. Backend-specific cases and Future versus native-async result handling justify differences in the complete method inventories. Preserve meaningful corresponding scenarios rather than forcing identical test lists. |

The session hooks in [tests/pyathena/conftest.py:15][session-hooks] prepare real AWS resources even for a selected pure-logic test.
This existing behavior is documented in the testing guide.
The organizational changes preserve it; an offline invocation must explicitly exclude those hooks and select self-contained modules.

## Validation boundaries

The initial audit is static source inspection.
Runtime results belong to the implementation revision and are recorded separately in the pull request's TEST section, with commands, dependency versions, and skipped coverage.
Compare the affected modules' collected node IDs before and after the changes, then run the self-contained pandas and Polars tests offline and both disk-usage cases against the configured AWS environment.
Normal CI subsequently exercises the applicable PyAthena suite.

The fixes above address the identified actionable inconsistencies without a broad grouping conversion.
Additional test-correctness findings from implementation review should be evaluated against the same observable-contract and fixture-lifetime criteria.

## File inventory

```text
tests/__init__.py
tests/pyathena/__init__.py
tests/pyathena/aio/__init__.py
tests/pyathena/aio/arrow/__init__.py
tests/pyathena/aio/arrow/test_cursor.py
tests/pyathena/aio/conftest.py
tests/pyathena/aio/pandas/__init__.py
tests/pyathena/aio/pandas/test_cursor.py
tests/pyathena/aio/polars/__init__.py
tests/pyathena/aio/polars/test_cursor.py
tests/pyathena/aio/s3fs/__init__.py
tests/pyathena/aio/s3fs/test_cursor.py
tests/pyathena/aio/spark/__init__.py
tests/pyathena/aio/spark/test_cursor.py
tests/pyathena/aio/sqlalchemy/__init__.py
tests/pyathena/aio/sqlalchemy/test_base.py
tests/pyathena/aio/test_common.py
tests/pyathena/aio/test_connection.py
tests/pyathena/aio/test_cursor.py
tests/pyathena/aio/test_result_set.py
tests/pyathena/arrow/__init__.py
tests/pyathena/arrow/test_async_cursor.py
tests/pyathena/arrow/test_converter.py
tests/pyathena/arrow/test_cursor.py
tests/pyathena/arrow/test_result_set.py
tests/pyathena/arrow/test_util.py
tests/pyathena/conftest.py
tests/pyathena/filesystem/__init__.py
tests/pyathena/filesystem/test_init.py
tests/pyathena/filesystem/test_s3.py
tests/pyathena/filesystem/test_s3_async.py
tests/pyathena/filesystem/test_s3_core.py
tests/pyathena/filesystem/test_s3_errors.py
tests/pyathena/filesystem/test_s3_executor.py
tests/pyathena/filesystem/test_s3_object.py
tests/pyathena/filesystem/test_s3_path.py
tests/pyathena/pandas/__init__.py
tests/pyathena/pandas/test_async_cursor.py
tests/pyathena/pandas/test_converter.py
tests/pyathena/pandas/test_cursor.py
tests/pyathena/pandas/test_reader.py
tests/pyathena/pandas/test_result_set.py
tests/pyathena/pandas/test_util.py
tests/pyathena/polars/__init__.py
tests/pyathena/polars/test_async_cursor.py
tests/pyathena/polars/test_converter.py
tests/pyathena/polars/test_cursor.py
tests/pyathena/polars/test_result_set.py
tests/pyathena/s3fs/__init__.py
tests/pyathena/s3fs/test_async_cursor.py
tests/pyathena/s3fs/test_cursor.py
tests/pyathena/s3fs/test_reader.py
tests/pyathena/spark/__init__.py
tests/pyathena/spark/test_async_cursor.py
tests/pyathena/spark/test_common.py
tests/pyathena/spark/test_spark_cursor.py
tests/pyathena/sqlalchemy/__init__.py
tests/pyathena/sqlalchemy/test_array.py
tests/pyathena/sqlalchemy/test_base.py
tests/pyathena/sqlalchemy/test_compiler.py
tests/pyathena/sqlalchemy/test_map.py
tests/pyathena/sqlalchemy/test_struct.py
tests/pyathena/sqlalchemy/test_temporal.py
tests/pyathena/sqlalchemy/test_types.py
tests/pyathena/tables.py
tests/pyathena/test_async_cursor.py
tests/pyathena/test_connection.py
tests/pyathena/test_converter.py
tests/pyathena/test_cursor.py
tests/pyathena/test_formatter.py
tests/pyathena/test_glue.py
tests/pyathena/test_model.py
tests/pyathena/test_options.py
tests/pyathena/test_parser.py
tests/pyathena/test_result_set.py
tests/pyathena/test_util.py
tests/pyathena/util.py
tests/resources/queries/create_database.sql.jinja2
tests/resources/queries/drop_database.sql.jinja2
tests/sqlalchemy/__init__.py
tests/sqlalchemy/conftest.py
tests/sqlalchemy/profiles.txt
tests/sqlalchemy/test_suite.py
```

[sync-du]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/filesystem/test_s3.py#L4432-L4434
[async-du]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/filesystem/test_s3_async.py#L1491-L1493
[csv-helper]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/pandas/test_result_set.py#L265-L300
[csv-params]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/pandas/test_result_set.py#L305-L389
[csv-oracle]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/pandas/test_result_set.py#L391-L423
[polars-reader]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/polars/test_result_set.py#L259-L268
[filesystem-identities]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/pandas/test_result_set.py#L79-L80
[unused-dtypes]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/pandas/test_result_set.py#L439-L452
[grouping-rule]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/AGENTS.md#L84-L85
[parser-group]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/test_parser.py#L18-L36
[utility-integration]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/pandas/test_util.py#L62-L70
[converter-functions]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/test_converter.py#L19-L30
[utility-writes]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/pandas/test_util.py#L451-L471
[whole-read]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/pandas/test_result_set.py#L52-L58
[ddl-read]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/pandas/test_result_set.py#L139-L181
[pandas-converter]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/pandas/test_cursor.py#L1945-L1971
[s3fs-converter]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/s3fs/test_cursor.py#L543-L566
[compiler-types]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/sqlalchemy/test_compiler.py#L621-L658
[upstream-suite]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/sqlalchemy/test_suite.py#L35-L44
[upstream-plugin]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/sqlalchemy/conftest.py#L11-L39
[explicit-skips]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/sqlalchemy/test_suite.py#L974-L981
[adapted-tests]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/NOTICE#L29-L36
[cursor-lifetime]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/conftest.py#L225-L234
[aio-lifetime]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/aio/conftest.py#L21-L32
[arrow-binary]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/arrow/test_cursor.py#L38-L53
[aio-arrow-binary]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/aio/arrow/test_cursor.py#L22-L36
[session-hooks]: https://github.com/pyathena-dev/PyAthena/blob/200762088e7b0bac45054f22e32a16aa0ce95dbb/tests/pyathena/conftest.py#L15-L56
