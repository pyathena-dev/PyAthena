---
# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
name: self-review
description: Run the first PyAthena self-review after creating a Draft PR, checking implementation behavior, public contracts, simplicity, and regression coverage. Also use for the affected scope after a reviewed repair.
---

# Self-review of implementation behavior

Use the scope and evidence rules in [development-workflow](../development-workflow/SKILL.md).
For an explicitly bounded review-only request, honor its literal diff, permitted corpus, and no-write/no-test constraints instead of starting delivery or expanding the review.

Read the full initial diff against the intended behavior and inventory the affected contracts.
Check the following perspectives separately, selecting only the surfaces the change affects.
Perform these passes sequentially yourself; do not report them as independent review.

| Perspective | PyAthena questions |
|---|---|
| Behavior and failure paths | Are DB API return values, exceptions, cursor state, fetch/close behavior, and cancellation preserved? Do synchronous and asynchronous callers share the same assumptions? Are errors translated only when their meaning is known? |
| Data and resource boundaries | Are NULLs, empty results, nested types, precision, names, and quoting handled? Does each relevant pandas/Arrow/Polars/S3 result path preserve conversion behavior? Are streams and temporary resources released on failure? |
| Framework contracts | For SQLAlchemy, check reflection, DDL/checkfirst, catalog/schema/name identity, cache ownership and invalidation, and missing-table versus permission/throttle failures. For fsspec, check listing, pagination, delimiter use, and path normalization against supported dependencies. |
| Simplicity and test quality | Can an existing helper or fixture express this more directly? Does a regression test exercise the failing path and assert observable behavior? Could it pass because the fake omits the real service behavior or because only the happy path runs? |

Trace shared helpers into their actual callers rather than assuming every backend uses the same path.
Check dependency behavior against the versions resolved by the repository and the supported compatibility range when relevant.
Treat potential findings as hypotheses: verify the named code and a concrete failure scenario before editing.
Apply verified defects and useful simplifications within the task's scope, and run affected checks.
Distinguish pre-existing defects from introduced regressions and record out-of-scope findings without silently enlarging the PR.

Record the covered inventory, findings, repairs, and limitations as round one.
Then run [self-review-round-two](../self-review-round-two/SKILL.md), whose initial claim audit remains a full pass even if round one needed only a narrow repair follow-up.
