<!--
Copyright 2026 The PyAthena authors

Licensed under the MIT License.
See LICENSE or https://opensource.org/licenses/MIT.

SPDX-License-Identifier: MIT
-->

(contributing)=

# Contributing

Contributions, including AI-assisted contributions, are welcome.
This guide describes what contributors need to provide so that maintainers can assess a change and its evidence.

## Discuss the change first

Before implementing a nontrivial change, open an issue or join an existing one and reach agreement with a maintainer on the problem and approach.
A typo or similarly trivial correction can go directly to a pull request.
Changes to behavior, public APIs, resource ownership, or existing design decisions need discussion first.

Use the [bug report](https://github.com/pyathena-dev/PyAthena/issues/new?template=bug_report.md) or [feature and change proposal](https://github.com/pyathena-dev/PyAthena/issues/new?template=feature_request.md) template to start the discussion.
Reporting a problem or requesting a feature does not require implementing it or running the test suite.

For a bug, include a minimal reproduction, the PyAthena and Python versions, and the observed and expected behavior.
For a feature, explain the use case and why the existing API does not meet it.
If you intend to implement a change, describe compatibility implications and the proposed validation, including any AWS resources needed.
If you cannot run that validation, raise the limitation before starting implementation.
An issue or an agreed approach does not guarantee that a pull request will be accepted.

## Take responsibility for the change

Submit changes that you have reviewed and can explain.
You should be able to explain the problem, why the implementation addresses it, its effect on existing users, and its main failure paths.
Understand what your tests establish and where uncertainty remains.
Respond to review questions by investigating the code and service behavior, and fix verified problems.

### AI-assisted contributions

AI assistance is welcome and is used extensively in this project's development.
The same contribution requirements apply regardless of which tools produce the code, tests, documentation, or review comments.

You do not need to recite every line or avoid using tools during review.
You do need to understand the submitted change well enough to explain its design, check its claims, and investigate and repair defects.
Read and verify generated code and explanations before submitting them.
Passing along an AI-generated answer without checking it does not resolve a review question.

Self-review and independent review can help find defects, but neither is evidence that an unrun test passed.
Report only checks that actually ran, distinguish local results from CI results, and disclose skipped or incomplete validation.

## Validate in your own environment

Follow the [testing guide](testing.md) to install the development tools and prepare an AWS test environment.
Run `just format` and `just lint` before submitting code changes, and add regression coverage for changed behavior.

Changes affecting AWS interactions require relevant integration tests against real AWS services in an environment you control.
Mocks are useful for specific failure paths, but do not replace service validation for those changes.
Provision your own test resources and credentials, cover their costs, and clean up resources left by testing.
Maintainers do not provide AWS access or undertake the contributor's initial validation.

Choose coverage based on the affected behavior, including synchronous and asynchronous callers and relevant cursor backends.
SQLAlchemy dialect changes require the relevant PyAthena dialect tests under `tests/pyathena/sqlalchemy/` as well as the SQLAlchemy compliance suite (`just test sqla`).
Changes affecting async dialects also require the relevant tests under `tests/pyathena/aio/sqlalchemy/` and `just test sqla-async`.
The PyAthena dialect tests are included in `just test pyathena`; they are not run by the two compliance-suite commands.
Exercise the regression on the version before the fix where practical, and record the before/after result.
Documentation-only changes need documentation checks; changes limited to logic independent of AWS need appropriate tests for that logic.
Check the testing guide before assuming that selecting a unit test avoids AWS setup.

External-fork pull requests must not be run in the project's AWS integration CI.
Do not submit an untested change expecting a maintainer to approve an AWS CI run to validate it.
Checks that need no AWS access may still run, but their success does not establish integration coverage.

## Open a pull request

Open a draft pull request with the repository's template completed.
Keep WHAT and WHY focused on the change and its agreed issue, and put executed validation in TEST.
Reference the issue and agreed approach, keep the change focused, and update documentation when behavior or public APIs change.
Write commit messages, pull-request text, and code comments in English.

Include a validation record with:

- The commit tested and Python version, plus relevant dependency versions.
- The exact commands and relevant test selection.
- The results, including passed, failed, and skipped counts for tests.
- Whether each result came from real AWS, mocks, another local check, or CI.
- Any failures, skipped coverage, or other limitations and their reasons.

Use sanitized output; do not include credentials or private data in the pull request or logs.
A statement such as "tests pass" or "CI will verify this" is not a validation record.

Review your implementation and tests, then separately check that the description, examples, and claimed guarantees match them.
If another person or model reviews the change, identify that review and its limits accurately.
Do not describe a second pass by the same authoring model as independent review.
The repository's [agent workflow](https://github.com/pyathena-dev/PyAthena/blob/master/AGENTS.md) defines the additional delivery steps used by its coding agents.

Keep the pull request in draft while agreed validation or review work is incomplete.
Maintainers may defer or close changes that lack prior discussion, sufficient validation, or substantive responses to review.

## Licensing and attribution

PyAthena is distributed under the [MIT license](https://github.com/pyathena-dev/PyAthena/blob/master/LICENSE).
Submit original contributions under that license and identify any third-party material in the pull request.

### New original files

New original files added to the repository, including AI-assisted contributions, must carry a copyright and license header.
Use the year the file is first published and the collective attribution `The PyAthena authors`.
For a new Python file first published in 2026, use:

```python
# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
```

Place the header before imports and module documentation, after any required shebang, encoding declaration, or front matter.
Use the equivalent comment syntax for other formats, such as an HTML comment in Markdown.
For files that cannot contain comments, and for generated build artifacts or third-party assets, agree on an appropriate attribution location in the issue rather than inserting an invalid header.

`The PyAthena authors` is a collective project attribution, not a requirement to assign copyright to the maintainer.
Contributors may retain or add their own applicable copyright notices alongside it.
Do not replace another copyright holder's notice with the collective name.
The short notice and license reference follow the approach in [Google's MIT header example](https://opensource.google/documentation/reference/releasing/licenses#mit-header), with an SPDX identifier added.
These headers identify the license; distributions must still include the required license text and notices.
The [REUSE FAQ](https://reuse.software/faq/) explains collective attribution and preservation of existing notices, and [SPDX](https://spdx.org/licenses/MIT.html) provides the MIT identifier and license text.

### Existing and adapted code

Preserve existing copyright and license notices, including the notice in the root `LICENSE`.
The new-file rule does not require unrelated edits to add headers throughout the existing codebase.
Audit provenance before adding or changing attribution in an existing file; mixed authorship and code adapted from other projects need separate consideration.
Header automation and a repository-wide provenance audit are separate work, not claims made by this guide.

The [README](https://github.com/pyathena-dev/PyAthena/blob/master/README.md#license) acknowledges that implementations in PyAthena are based on PyHive.
That acknowledgment is not a file-by-file provenance record.
Before importing or adapting code, identify the source project, source revision, affected files, license, and what was reused.
Record that information in the pull request and durably alongside the affected code or in accompanying attribution documentation, and preserve the upstream notices and license text required for redistribution.
Do not label adapted code as wholly original PyAthena work or replace its upstream license header with the original-file template above.
For existing PyHive-related code, establish its origin from history and source comparison before changing notices.
The same obligations apply to AI-assisted work; disclose known sources and investigate suspected copying instead of assuming generated code is original.
