(testing)=

# Testing

## Environment variables

Depends on the following environment variables:

```bash
$ export AWS_DEFAULT_REGION=us-west-2
$ export AWS_ATHENA_S3_STAGING_DIR=s3://YOUR_S3_BUCKET/path/to/
$ export AWS_ATHENA_WORKGROUP=pyathena
$ export AWS_ATHENA_SPARK_WORKGROUP=pyathena-spark
```

In addition, you need to create a workgroup with the `Query result location` set to the name specified in the `AWS_ATHENA_WORKGROUP` environment variable.
If primary is not available as the default workgroup, specify an alternative workgroup name for the default in the environment variable `AWS_ATHENA_DEFAULT_WORKGROUP`.

```bash
$ export AWS_ATHENA_DEFAULT_WORKGROUP=DEFAULT_WORKGROUP
```

### Managed query result storage (optional)

To test the managed query result storage feature, create a workgroup with managed storage enabled and set the `AWS_ATHENA_MANAGED_WORKGROUP` environment variable.
If not set, managed storage tests will be skipped.

```bash
$ export AWS_ATHENA_MANAGED_WORKGROUP=pyathena-managed
```

## Run test

The task runner uses [just](https://github.com/casey/just). Install it with `mise use -g just`, `brew install just`, or `cargo install just`.

```bash
$ pip install uv or pipx install uv or brew install uv or mise install uv
$ just test pyathena
$ just test sqla
$ just test sqla-async
```

The `just test` recipes rerun a failed test once when its failure message is an Athena internal error or `Invalid S3 request`, and report the first attempt's traceback.
Failures in the session setup hooks are not rerun, and a direct `pytest` invocation does not rerun.

## Run test multiple Python versions

```bash
$ pip install uv or pipx install uv or brew install uv or mise install uv
$ just tox
```

## Code formatting

The code formatting uses [ruff](https://github.com/astral-sh/ruff).

### Appy format

```bash
$ just format
```

### Lint and check format

```bash
$ just lint
```

## GitHub Actions

The Test workflow runs for pull requests that change files other than `docs/` and Markdown.
It runs the offline checks (`just lint`) on each of them, including Drafts and external forks, and runs the AWS suites as follows:

| Trigger | PyAthena suite | SQLAlchemy tests | Spark tests |
| --- | --- | --- | --- |
| Draft pull request | No | No | No |
| Ready pull request from a branch of this repository | Yes | When related files change | When related files change |
| Weekly schedule and manual dispatch | Yes | Yes | Yes |

The SQLAlchemy tests are the compliance suites and the PyAthena suite's `tests/pyathena/sqlalchemy/` and `tests/pyathena/aio/sqlalchemy/`.
The Spark tests are the PyAthena suite's `tests/pyathena/spark/` and `tests/pyathena/aio/spark/`.
When the SQLAlchemy or Spark tests do not run, the PyAthena suite runs without them.
For the SQLAlchemy tests, the related files are `pyathena/sqlalchemy/`, `pyathena/aio/sqlalchemy/`, `tests/sqlalchemy/`, their PyAthena suite test directories, and `setup.cfg`.
For the Spark tests, they are `pyathena/spark/`, `pyathena/aio/spark/`, and their PyAthena suite test directories.
Changes to `pyproject.toml`, `uv.lock`, `justfile`, or the Test workflows run both.
For a pull request from a branch of this repository that still changes files other than `docs/` and Markdown, marking the Draft ready for review starts the AWS jobs, and converting it back to Draft cancels AWS jobs still running.
To run every suite on a branch, dispatch the workflow:

```bash
gh workflow run test.yaml --ref <branch>
```

GitHub Actions uses OpenID Connect (OIDC) to access AWS resources. You will need to refer to the [GitHub Actions documentation](https://docs.github.com/actions/deployment/security-hardening-your-deployments/configuring-openid-connect-in-amazon-web-services) to configure it.

The CloudFormation templates for creating GitHub OIDC Provider and IAM Role can be found in the [aws-actions/configure-aws-credentials repository](https://github.com/aws-actions/configure-aws-credentials#sample-iam-role-cloudformation-template).

Under [scripts/cloudformation](https://github.com/pyathena-dev/PyAthena/tree/master/scripts/cloudformation) you will also find a CloudFormation template with additional permissions and workgroup settings needed for testing.

The example of the CloudFormation execution command is the following:

```bash
$ aws --region us-west-2 \
    cloudformation create-stack \
    --stack-name github-actions-oidc-pyathena \
    --capabilities CAPABILITY_NAMED_IAM \
    --template-body file://./scripts/cloudformation/github_actions_oidc.yaml \
    --parameters ParameterKey=GitHubOrg,ParameterValue=pyathena-dev \
      ParameterKey=RepositoryName,ParameterValue=PyAthena \
      ParameterKey=BucketName,ParameterValue=laughingman7743-athena \
      ParameterKey=RoleName,ParameterValue=github-actions-oidc-pyathena-test \
      ParameterKey=WorkGroupName,ParameterValue=pyathena-test
```
