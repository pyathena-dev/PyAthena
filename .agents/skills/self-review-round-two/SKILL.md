---
# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
name: self-review-round-two
description: Run the second PyAthena self-review after round one, auditing factual claims, caller compatibility, AWS operational effects, and evidence from a user's perspective before Ready.
---

# Self-review of claims and operational behavior

Use the scope and evidence rules in [development-workflow](../development-workflow/SKILL.md).
Honor any explicit review-only restrictions on corpus, commands, and side effects.
This round tests whether the change's explanation and guarantees hold for callers and operators.
Rereading round one's implementation checklist does not complete this round.

Before inspecting implementation details, list actionable factual claims in the PR body, changed docstrings/comments, documentation, commit messages, and issue premises repeated by the change.
For each claim, identify evidence that could disprove it and examine the relevant callers, dependency sources, or measurements.
An issue report alone is not proof of its proposed cause.

| Perspective | PyAthena questions |
|---|---|
| Existing caller | Do defaults, positional/keyword arguments, return shapes, exceptions, optional dependencies, and supported Python/SQLAlchemy versions still work? Does a mutable or one-shot input behave differently across repeated calls? |
| AWS operator | How do botocore and application retries compose? What bounds attempts and elapsed time? Is a quota diagnosis based on the actual API, region/account, and run, or merely on an error string? Does reducing requests preserve freshness and error visibility? |
| Adversarial caller | Can another Inspector, schema, catalog, backend, concurrent request, or DDL operation break a stated cache or lifecycle guarantee? Check absence versus failure and stale versus fresh metadata where applicable. |
| Documentation reader | Do the examples work with the documented dependencies and environment? Search related docs and examples for statements the change makes obsolete, including prose outside the diff. |
| Evidence reviewer | Do tests exercise both success and failure with meaningful assertions? Separate local from CI, synchronous from asynchronous, and fake-provider counts from measured AWS traffic. Does the result belong to the published revision? |

Measure a performance or service claim when the authorized environment permits it, or narrow the claim to the evidence available.
Document unmeasured limits explicitly; do not present a static source review as runtime validation.
Do not add retries or weaken a test to hide an unexplained failure.
For documentation-only changes, walk through the commands and decision paths; do not launch AWS tests just to complete a checklist.

Correct false claims in the PR description and affected prose, and validate any implementation repairs.
Record claims checked, supporting evidence, corrections, and reasoned deferrals as round two.
For small wording changes, this can be a short claim-and-link check, but it remains a distinct recorded pass.
Proceed to [independent-review](../independent-review/SKILL.md) only after both self-review rounds are complete.
