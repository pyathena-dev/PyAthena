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

SQLAlchemy tests of Amazon S3 Tables need a table bucket integrated with the AWS analytics services, its registered catalog, and an existing namespace:

```ini
AWS_ATHENA_S3_TABLES_CATALOG=s3tablescatalog/your-table-bucket
AWS_ATHENA_S3_TABLES_NAMESPACE=your_namespace
```

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
Do not overlap test runs that share AWS resources or quota.
For a focused run with a single worker, for example:

```bash
uv run --env-file .env pytest -n 1 tests/pyathena/test_cursor.py -v
```

A targeted run helps during development but does not replace other coverage required by the affected callers or features.
The SQLAlchemy suites run with different configurations: use `sqla` for synchronous dialects and `sqla-async` for native asyncio dialects.

To run the configured Python/environment matrix locally:

```bash
uv run --env-file .env just tox
```

Record the tested commit, Python and relevant dependency versions, exact commands, and results in the pull request.
Include failed and skipped tests and explain any unrun coverage.
Separate real AWS results from mock-based tests and static checks.
Sanitize logs before sharing them.

## GitHub Actions

Project policy excludes external-fork pull requests from AWS integration CI.
Maintainers do not approve those jobs as a substitute for contributor testing.
Checks without AWS access may still run on a fork pull request.

For maintainers, the reusable test workflow uses OIDC to access the project's AWS environment.
Workflow approval and AWS IAM trust restrictions remain separate controls; a job condition alone does not make arbitrary pull-request code safe to execute with AWS permissions.
Do not bypass the fork policy by checking out external pull-request code in a privileged workflow or copying it to an internal branch just to run AWS CI.

Contributors can run tests locally with their own AWS credentials without configuring GitHub OIDC.
If setting up CI in your own fork, replace the project-specific role ARN and resource settings in `.github/workflows/test-suite.yaml` with your own values.
The [GitHub OIDC documentation](https://docs.github.com/en/actions/how-tos/secure-your-work/security-harden-deployments/oidc-in-aws) explains the authentication setup.
The [CloudFormation template](https://github.com/pyathena-dev/PyAthena/blob/master/cloudformation/github_actions_oidc.yaml) is an optional starting point; review its IAM permissions and resources before deploying it in your account.
For example, after replacing every placeholder and checking resource names:

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
    ParameterKey=WorkGroupName,ParameterValue=pyathena-test
```

If the account already has the GitHub OIDC provider, supply its ARN as the `OIDCProviderArn` parameter.
The stack does not grant your local identity access automatically, and additional account-level service configuration may be needed for the features you test.
See the [infrastructure guide](https://github.com/pyathena-dev/PyAthena/blob/master/cloudformation/README.md) for updates to an existing stack.
