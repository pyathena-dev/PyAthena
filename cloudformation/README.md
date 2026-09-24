<!--
Copyright 2026 The PyAthena authors

Licensed under the MIT License.
See LICENSE or https://opensource.org/licenses/MIT.

SPDX-License-Identifier: MIT
-->

# Test infrastructure

[github_actions_oidc.yaml](github_actions_oidc.yaml) defines the AWS infrastructure used by PyAthena's integration tests and GitHub Actions.
It includes GitHub OIDC authentication and IAM roles, Athena SQL and Spark workgroups, an S3 staging bucket, and S3 Tables resources with Glue catalog integration.

The template parameters configure the GitHub repository, resource names, and an optional existing OIDC provider.
See the [testing guide](../docs/testing.md#github-actions) for initial stack creation.

The benchmark-specific template is maintained separately in [benchmarks/cloudformation/](../benchmarks/cloudformation/).

## Validation

Run `just lint` from the repository root to check the templates in this directory with cfn-lint alongside the Python checks.
This validation requires no AWS credentials.
The benchmark template is checked separately by `just benchmark lint`.

## Update an existing stack

Use the AWS CLI locally with credentials that can update the stack and its resources.
Run these commands from the repository root with [uv](https://docs.astral.sh/uv/) installed.
Configure the test account profile and region in the gitignored `.env` file using `KEY=value` assignments:

```dotenv
AWS_PROFILE=your-aws-profile
AWS_DEFAULT_REGION=us-west-2
```

In a git worktree, run `just worktree-env` once to link the main checkout's `.env`.
The `uv run --env-file .env` prefix loads this configuration for each AWS command.
Adjust the region and stack name if the stack was created with different settings.

```bash
uv run --env-file .env aws cloudformation describe-stacks \
  --stack-name github-actions-oidc-pyathena \
  --query 'Stacks[0].{Id:StackId,Status:StackStatus,Parameters:Parameters}'
```

Check the selected stack and its current parameters, then apply the local template:

```bash
uv run --env-file .env aws cloudformation deploy \
  --stack-name github-actions-oidc-pyathena \
  --template-file cloudformation/github_actions_oidc.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --no-fail-on-empty-changeset
```

The [`deploy` command](https://docs.aws.amazon.com/cli/latest/reference/cloudformation/deploy.html) applies the update and waits for completion.
It retains existing parameter values when `--parameter-overrides` is omitted; add that option only for parameters being changed or newly required parameters without defaults.
`CAPABILITY_NAMED_IAM` is required for the named IAM roles in this template.
An unchanged template and parameters produce a successful no-op.
If an update fails, inspect the stack events:

```bash
uv run --env-file .env aws cloudformation describe-stack-events \
  --stack-name github-actions-oidc-pyathena
```
