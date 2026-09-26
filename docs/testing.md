<!--
Copyright 2022 The PyAthena authors

Licensed under the MIT License.
See LICENSE or https://opensource.org/licenses/MIT.

SPDX-License-Identifier: MIT
-->

(testing)=

# Testing

PyAthena's main test suites use real AWS services.
Read the [contribution guide](contributing.md) for the validation required with a pull request.
Prepare an environment you control before running tests; the project does not provide contributors with AWS credentials or test resources.

## Development tools

Use a Python version supported by `pyproject.toml` and install [uv](https://docs.astral.sh/uv/getting-started/installation/) and [just](https://just.systems/man/en/packages.html).
From the repository root, install the development dependencies and check the code:

```bash
uv sync --group dev
just format
just lint
```

Run `just lint` before tests, including when invoking pytest directly.
For documentation checks, install the tools pinned in `.mise.toml` with [mise](https://mise.jdx.dev/):

```bash
mise install
just docs lint
just docs build
```

`just docs build` builds documentation from the configured Git refs with sphinx-multiversion.
To check the working tree, including uncommitted documentation changes, also run:

```bash
uv run sphinx-build -b html docs docs/_build/current
```

## AWS environment

Use a dedicated test account or isolated test resources, not production resources.
You are responsible for AWS charges and for removing resources and data left by tests, including after an interrupted run.
Configure AWS credentials using the normal boto3 credential chain, for example a local AWS profile.
Do not commit credentials or include them in test output shared on a pull request.

The tests need an S3 bucket and prefix for staging and data, an Athena SQL workgroup with an S3 query-result location, and an Athena Spark workgroup with a suitable execution role for Spark tests.
The local test identity needs the corresponding Athena, S3, and Glue permissions; additional features require their own service permissions.
The Glue metadata fallback tests call `glue:GetTable`, `glue:GetTables`, and `glue:GetDatabases` directly, including against the S3 Tables catalog; the template below grants them.
The [test infrastructure template](https://github.com/pyathena-dev/PyAthena/blob/master/cloudformation/github_actions_oidc.yaml) describes the resources and permissions used by project CI.
Its GitHub OIDC role is not a local credential setup: contributors must configure their own test identity.

Create a gitignored `.env` file in the repository root, replacing the example values:

```ini
AWS_PROFILE=your-test-profile
AWS_DEFAULT_REGION=us-west-2
AWS_ATHENA_S3_STAGING_DIR=s3://your-test-bucket/pyathena-tests/
AWS_ATHENA_WORKGROUP=your-sql-workgroup
AWS_ATHENA_SPARK_WORKGROUP=your-spark-workgroup
```

Keep the trailing slash on the staging location.
If using credentials through another supported mechanism, omit `AWS_PROFILE`.
The test configuration requires all four `AWS_DEFAULT_REGION` and `AWS_ATHENA_*` variables above even when the selected tests do not use Spark.
Some fixtures use the `primary` SQL workgroup by default; if it is unavailable or unsuitable, also set:

```ini
AWS_ATHENA_DEFAULT_WORKGROUP=your-sql-workgroup
```

In a git worktree, `just worktree-env` links the main checkout's gitignored `.env`.
The link alone does not load its variables; use `uv run --env-file .env` as shown below.

### Managed query result storage

Tests of managed query result storage need a workgroup with managed storage enabled:

```ini
AWS_ATHENA_MANAGED_WORKGROUP=your-managed-workgroup
```

### Amazon S3 Tables

SQLAlchemy tests of Amazon S3 Tables need a table bucket integrated with the AWS analytics services and its registered catalog:

```ini
AWS_ATHENA_S3_TABLES_CATALOG=s3tablescatalog/your-table-bucket
```

Each test process creates its own namespace in the table bucket, named like its schema, and deletes it with any remaining tables at the end; with pytest-xdist that is each worker, not the controller.
The test identity needs `s3tables:CreateNamespace`, `s3tables:DeleteNamespace`, `s3tables:ListTables`, and `s3tables:DeleteTable` on the table bucket.
A session that stops early leaves its namespace behind; `scripts/sweep_databases.py` removes such namespaces once they are more than seven days old.

The S3 Tables tests live in `tests/pyathena/sqlalchemy/test_base.py` and `tests/pyathena/test_glue.py` and run under `just test pyathena`, not the SQLAlchemy compliance-suite commands.
Managed storage and S3 Tables tests skip when their respective optional configuration is absent.
If a change affects one of these features, configure and run its tests; a skip does not validate that change.

## Run tests

The test session hooks upload fixture data to S3 and create and remove Athena/Glue databases and tables.
Selecting a single test under `tests/pyathena/` still invokes the session setup, even when the test itself uses only mocks or pure Python logic.
Do not assume that `-k` or a single test path makes this suite offline.

Run the suites relevant to the change:

```bash
uv run --env-file .env just test pyathena
uv run --env-file .env just test sqla
uv run --env-file .env just test sqla-async
```

Despite the recipe's "unit tests" label, `just test pyathena` includes AWS integration tests.
The recipes use eight pytest workers, so check your account's Athena concurrency quotas.
They also rerun a failed test once when its failure message is an Athena internal error or `Invalid S3 request`, and report the first attempt's traceback.
Failures in the session setup hooks are not rerun, and a direct `pytest` invocation does not rerun.
Do not overlap test runs that share AWS resources or quota.
For a focused run with a single worker, for example:

```bash
uv run --env-file .env pytest -n 1 tests/pyathena/test_cursor.py -v
```

A targeted run helps during development but does not replace other coverage required by the affected callers or features.
The SQLAlchemy compliance suites under `tests/sqlalchemy/` run with different configurations: use `sqla` for synchronous dialects and `sqla-async` for native asyncio dialects.
They do not run PyAthena's own dialect regression tests under `tests/pyathena/sqlalchemy/` and `tests/pyathena/aio/sqlalchemy/`.
Run the relevant PyAthena tests too, either through `just test pyathena` or a focused selection during development:

```bash
uv run --env-file .env pytest -n 1 tests/pyathena/sqlalchemy/ tests/pyathena/aio/sqlalchemy/ -v
```

To invoke the configured tox environments locally:

```bash
uv run --env-file .env just tox
```

Record the tested commit, Python and relevant dependency versions, exact commands, and results in the pull request.
Include failed and skipped tests and explain any unrun coverage.
Separate real AWS results from mock-based tests and static checks.
Sanitize logs before sharing them.

## GitHub Actions

The Test workflow runs for pull requests that change files other than `docs/` and Markdown.
It runs the offline checks (`just lint`) on each of them, including Drafts and external forks, and runs the AWS suites as follows:

| Trigger | PyAthena suite | SQLAlchemy compliance suites | Spark tests |
| --- | --- | --- | --- |
| Draft pull request | No | No | No |
| Ready pull request | Yes | When related files change | When related files change |
| Weekly schedule and manual dispatch | Yes | Yes | Yes |

For the compliance suites, the related files are `pyathena/sqlalchemy/`, `pyathena/aio/sqlalchemy/`, `tests/sqlalchemy/`, and `setup.cfg`.
For the Spark tests, they are `pyathena/spark/`, `pyathena/aio/spark/`, `tests/pyathena/spark/`, and `tests/pyathena/aio/spark/`.
Changes to `pyproject.toml`, `uv.lock`, `justfile`, or the Test workflows run both.
Marking a Draft pull request ready for review starts its AWS jobs.
To run every suite on a branch, dispatch the workflow:

```bash
gh workflow run test.yaml --ref <branch>
```

Project policy excludes external-fork pull requests from AWS integration CI.
Maintainers do not approve those jobs as a substitute for contributor testing.
Checks without AWS access may still run on a fork pull request.

For maintainers, the reusable test workflow uses OIDC to access the project's AWS environment.
Do not treat workflow gating as a substitute for approval controls and AWS IAM trust restrictions.
Do not bypass the fork policy by checking out external pull-request code in a privileged workflow or copying it to an internal branch just to run AWS CI.

Contributors can run tests locally with their own AWS credentials without configuring GitHub OIDC.
If setting up CI in your own fork, replace the project-specific role ARN and resource settings in `.github/workflows/test-suite.yaml` with your own values.
The [GitHub OIDC documentation](https://docs.github.com/en/actions/how-tos/secure-your-work/security-harden-deployments/oidc-in-aws) explains the authentication setup.
You can configure tests against suitable existing resources, or provision new resources with the optional [CloudFormation template](https://github.com/pyathena-dev/PyAthena/blob/master/cloudformation/github_actions_oidc.yaml).
Review its IAM permissions and resources first.
The following `create-stack` example is for new resources: it creates the staging bucket, table bucket, roles, and workgroups and does not adopt resources already created during manual setup.
Choose unused names for all of them.
The template also creates the fixed regional Glue catalog `s3tablescatalog`; do not use this creation example in an account/region where that catalog already exists.
Use existing-resource configuration or plan an explicit infrastructure import/adaptation separately.

For a new stack, after replacing every placeholder and checking the names and regional catalog:

```bash
aws --region us-west-2 cloudformation create-stack \
  --stack-name pyathena-test-infrastructure \
  --capabilities CAPABILITY_NAMED_IAM \
  --template-body file://./cloudformation/github_actions_oidc.yaml \
  --parameters ParameterKey=GitHubOrg,ParameterValue=YOUR_GITHUB_USER \
    ParameterKey=RepositoryName,ParameterValue=PyAthena \
    ParameterKey=BucketName,ParameterValue=YOUR_UNIQUE_TEST_BUCKET \
    ParameterKey=S3TablesBucketName,ParameterValue=YOUR_UNIQUE_TABLE_BUCKET \
    ParameterKey=RoleName,ParameterValue=pyathena-test-ci \
    ParameterKey=SparkRoleName,ParameterValue=pyathena-test-spark \
    ParameterKey=WorkGroupName,ParameterValue=pyathena-test \
    ParameterKey=SparkWorkGroupName,ParameterValue=pyathena-test-spark \
    ParameterKey=ManagedWorkGroupName,ParameterValue=pyathena-test-managed
```

If the account already has the GitHub OIDC provider, supply its ARN as the `OIDCProviderArn` parameter.
The stack does not grant your local identity access automatically, and additional account-level service configuration may be needed for the features you test.
The staging bucket uses CloudFormation `DeletionPolicy: Retain`, so deleting the stack does not remove it; account for that in cleanup and before reusing its name in a new stack.
See the [infrastructure guide](https://github.com/pyathena-dev/PyAthena/blob/master/cloudformation/README.md) for updates to an existing stack.
