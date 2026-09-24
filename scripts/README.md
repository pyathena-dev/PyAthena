<!--
Copyright 2026 The PyAthena authors

Licensed under the MIT License.
See LICENSE or https://opensource.org/licenses/MIT.

SPDX-License-Identifier: MIT
-->

# Repository scripts

This directory contains scripts used by development and CI, with regression tests in [tests/](tests/).
Script-specific usage and behavior are documented in the scripts themselves.
AWS test infrastructure templates are in [cloudformation/](../cloudformation/).

## Validation

Run the Python and CloudFormation checks, the license header check, ShellCheck, actionlint, and offline script tests from the repository root:

```bash
mise install shellcheck actionlint
just scripts
```

ShellCheck is pinned in `.mise.toml` and checks `scripts/*.sh`.
The pinned actionlint checks GitHub Actions workflows, including embedded shell commands.
The script tests under `scripts/tests/` require no AWS credentials; the database sweep tests use botocore stubs.
