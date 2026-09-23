---
# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
name: independent-review
description: Obtain and collect an independent read-only review of a PyAthena PR after both self-review rounds, with a frozen diff and recorded findings before Ready.
---

# Independent review

Use the scope and evidence rules in [development-workflow](../development-workflow/SKILL.md).
Choose an available reviewer that did not author the change, including its repairs: a different model or a human.
A fresh thread of the authoring model does not satisfy this distinction.
For a Codex-authored change, use Claude Code with model `claude-fable-5-1`, the `max` profile, and effort `high`.
Do not use an Enterprise account for this review.
Resolve the locally configured `max` profile without committing a personal filesystem path, and verify the selected account/profile before sending review content.
An API key or provider environment override must not silently route the review through a different account.
When the profile uses `CLAUDE_CONFIG_DIR`, set it to the resolved `max` directory and check `claude auth status` for first-party Max authentication before launch.
Use the Claude Code CLI directly; no reviewer plugin is required.
For a Claude-authored change, use a different model or a human; a second Claude session is still self-review.
An explicit user override takes precedence over these defaults.
Existing authorization applies within its scope; this skill does not authorize unrelated external disclosure or contacting other people.

## Provide a bounded review package

Freeze the review merge-base SHA and published head SHA, as defined in the workflow skill, and a clean source snapshot before launching.
Use a detached worktree or an exported snapshot of tracked files that matches the reviewed commit.
Exclude credentials, `.env`, local memory, and unrelated artifacts from an external review package.
Supply the literal diff and relevant source/tests plus repository conventions.
Keep author framing out of the review prompt: omit the PR number, PR description, commit message, prior findings, and author conclusions.

Request review only: no edits, builds, tests, commits, pushes, GitHub writes, or access to PR discussion or agent memory.
Ask the reviewer to identify covered surfaces, return `CLEAN` or `FINDINGS`, and give `file:line` plus a concrete failure scenario for each actionable finding.
For a narrow follow-up after a completed initial review, provide the bounded patch-series comparison and affected contracts described in the workflow skill.

## Collect and verify

Wait for completion and read the actual result.
A launched job, successful authentication, or missing output does not establish a completed review.
Record reviewer identity, the requested model/profile and effort where applicable, base/head SHAs, invocation constraints, session/job identifier, coverage, and actual findings.
Verify that the review snapshot and PR worktree remained unchanged.
Label the result as a static review unless the reviewer was separately authorized to execute validation and actually did so.

Verify findings against the code before acting.
Record why any finding is rejected, deferred, or identified as pre-existing.
The author fixes verified regressions, runs affected validation, applies both self-review perspectives to the repair, and obtains an independent follow-up.
A repair that expands the contract requires a full review of the expanded scope.

Publish the result inline, clearly identifying a relayed reviewer result and preserving its material findings and limits.
If the reviewer is unavailable or an execution/approval restriction prevents the review, record the exact reason and keep this step incomplete.
Continue useful authorized work; retain Draft status until the required review is completed or the user explicitly changes the requirement.
