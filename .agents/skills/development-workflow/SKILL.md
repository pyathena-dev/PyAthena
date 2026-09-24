---
# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
name: development-workflow
description: Deliver a PyAthena change through a dedicated worktree, Draft PR, two distinct self-reviews, independent review, and current CI before Ready. Use when implementing or updating a PR, not for a bounded review-only request.
---

# PyAthena PR delivery

Read [AGENTS.md](../../../AGENTS.md) for repository commands and conventions.
User instructions determine scope and authorization; carry existing authorization forward without asking again.
Keep unrelated worktrees and changes intact.

## Prepare and publish

1. Establish the intended behavior and affected surfaces before editing.
   Create one dedicated worktree per PR from the fetched `origin/master`, or from the agreed parent branch for a stacked PR.
2. Implement and validate the change using the applicable checks below.
   Inspect the complete diff, changed paths, and deletions before committing and pushing.
   Keep credentials, `.env`, temporary scripts, and raw review artifacts outside tracked files.
3. Read `.github/PULL_REQUEST_TEMPLATE.md` and create the PR with `gh pr create --draft`.
   Write WHAT and WHY around the final behavior and include validation and its limits.
   Pass multiline text through a temporary file with `--body-file`.
4. Complete [self-review](../self-review/SKILL.md), then [self-review-round-two](../self-review-round-two/SKILL.md), fixing verified findings and validating affected behavior.
5. Collect [independent-review](../independent-review/SKILL.md).
   Repairs pass through both self-review perspectives and an independent follow-up before completion.
6. Check the current PR with `gh pr view` and `gh pr checks` before `gh pr ready`.
   Confirm the published head matches the reviewed head, all applicable checks have completed successfully, and the PR has no merge conflict.
   Pending, cancelled, missing expected checks, and `UNKNOWN` mergeability do not establish readiness.
   Explain intentionally skipped jobs from workflow conditions; a passing rerun of one failed job does not make the remaining failures pass.
   Keep the PR Draft while required review or validation remains incomplete, unless the user explicitly changes that requirement.
   Merging requires the user's instruction.

## Validate the affected behavior

Run `just format` and `just lint` before committing; lint precedes tests.
Choose tests from the affected contracts and callers, and record the actual commands, results, and omissions.
New behavior needs regression coverage that can fail for the original defect.
Prefer existing fixtures and integration classes; do not build a parallel mock framework just to mirror the implementation.

| Changed surface | Validation to consider |
|---|---|
| Cursor, result set, converter, or shared utility | Affected `tests/pyathena/` tests and relevant synchronous, asynchronous, and optional dependency backends |
| SQLAlchemy dialect | Relevant PyAthena tests in `tests/pyathena/sqlalchemy/` plus `just test sqla`; also `tests/pyathena/aio/sqlalchemy/` and `just test sqla-async` for shared reflection or async paths. Both PyAthena directories are included in `just test pyathena`. |
| Filesystem | Affected S3 filesystem tests, including listing and path boundaries |
| Documentation | `just docs lint`; `just docs build` for rendered documentation changes |
| Agent skills | `just docs lint` and `mise exec -- markdownlint-cli2 '.agents/skills/**/*.md'`; inspect links and walk through realistic workflow scenarios |
| Dependency, packaging, or CI configuration | Affected build/install checks and supported Python/dependency combinations from `pyproject.toml`, `uv.lock`, and CI |

The `tests/pyathena/` directory includes AWS integration tests despite the recipe's "unit tests" label.
Before a live AWS run or CI retry, inspect active Test workflows with `gh run list --workflow test.yaml` and check known local test runs.
Separate worktrees do not isolate AWS accounts, quotas, or shared test resources.
When runs overlap, serialize live validation and use targeted `-n 1` runs while iterating; complete the required suite coverage before Ready.
Use `just worktree-env` and `uv run --env-file .env pytest ...` to load the worktree environment without printing credentials.
For a full recipe, use `uv run --env-file .env just test sqla` (or `sqla-async`); linking `.env` alone does not export its variables to `just`.
Do not cancel someone else's run or increase retry limits merely to obtain green CI.
After a failure, identify the failing API, error, retry layer, and run before deciding whether a retry is informative.
Markdown-only changes need no live AWS tests; inspect the actual workflow path filters when interpreting absent jobs.

## Freeze review scope and preserve evidence

For each round, obtain the PR's actual base branch and published head with `gh pr view --json baseRefName,headRefOid`.
Fetch the relevant refs, confirm local HEAD matches the published head, and resolve `git merge-base <head> <fetched-base-branch-tip>` once.
In review commands and records, `<base>` means that merge-base SHA, not the current base-branch tip.
Use literal full merge-base and head SHAs in `git diff <base>..<head>` and in the record.
For a stacked PR, this base comes from its parent branch, not `master`.
Inventory every changed file and the behavior, public contract, tests, and factual claims that need checking.

Each round's initial pass covers the full inventory.
After that round has completed, a narrow repair may use `git range-diff <old-base>..<old-head> <new-base>..<new-head>` and trace the changed hunks into affected callers, tests, and claims.
Verify both old objects with `git cat-file -e <sha>^{commit}` first; missing objects or expanded contracts require a full pass.
A round-one repair does not narrow round two's first pass.
After rebasing, inspect upstream changes that affect those contracts even if the patch series is unchanged; rerun affected checks and obtain current CI.
Do not substitute a tree diff between rebased heads or reset a branch to a moving base to squash it.
When rewriting a published branch is necessary, verify the expected remote head and use an explicit `--force-with-lease=<ref>:<expected-sha>`.

Record each round separately: perspective, full base/head SHAs, covered surfaces, `CLEAN` or `FINDINGS`, concrete scenarios with `file:line`, repairs, and reasoned deferrals.
`CLEAN` means no actionable findings within the stated scope, not that unrun tests passed.
Publish review records as inline comments on relevant diff lines through `gh api repos/OWNER/REPO/pulls/NUMBER/reviews`, using a `comments` array and an empty review body.
Use `event: COMMENT`, not approval of your own PR, and store the JSON request in a temporary file passed with `--input`.
For a clean round, anchor its evidence to a relevant changed line.
Record a repair in its existing thread using `POST repos/OWNER/REPO/pulls/NUMBER/comments/COMMENT_ID/replies` with a `body` field; `COMMENT_ID` must identify the thread's top-level comment, which has no `in_reply_to_id`.
Replies do not use the review request's `comments` array, and GitHub does not support replying to another reply.
For a finding outside the diff, anchor to the related changed line and name the actual `file:line` in the comment; GitHub rejects line anchors outside the diff.
If the user's review-only scope prohibits posting, keep the result in the response instead.

This workflow takes its review sequence and scope tracking from the [flink-connector-gcp skills at aad72cd8](https://github.com/flink-gcp/flink-connector-gcp/tree/aad72cd8e52cec7bdb25c958bf61dc820a549054/.agents/skills), with PyAthena-specific review and validation guidance.
