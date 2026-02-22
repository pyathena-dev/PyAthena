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

```bash
$ pip install uv or pipx install uv or brew install uv or mise install uv
$ make test
$ make test-sqla
```

## Run test multiple Python versions

```bash
$ pip install uv or pipx install uv or brew install uv or mise install uv
$ make tox
```

## Code formatting

The code formatting uses [ruff](https://github.com/astral-sh/ruff).

### Appy format

```bash
$ make fmt
```

### Lint and check format

```bash
$ make lint
```

## GitHub Actions

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
