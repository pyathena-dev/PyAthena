import os
import random
import string

SQLALCHEMY_CONNECTION_STRING = (
    "awsathena+rest://athena.{region_name}.amazonaws.com:443/"
    "{schema_name}?s3_staging_dir={s3_staging_dir}&location={location}"
)
ASYNC_SQLALCHEMY_CONNECTION_STRING = (
    "awsathena+aiorest://athena.{region_name}.amazonaws.com:443/"
    "{schema_name}?s3_staging_dir={s3_staging_dir}&location={location}"
)


def _random_schema():
    """Return a new test schema name, the shape scripts/sweep_databases.py sweeps.

    Returns:
        ``pyathena_test_`` followed by 10 random lowercase letters and digits.
    """
    return "pyathena_test_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=10))


class Env:
    def __init__(self):
        self.region_name = os.getenv("AWS_DEFAULT_REGION")
        assert self.region_name, "Required environment variable `AWS_DEFAULT_REGION` not found."
        self.s3_staging_dir = os.getenv("AWS_ATHENA_S3_STAGING_DIR")
        assert self.s3_staging_dir, (
            "Required environment variable `AWS_ATHENA_S3_STAGING_DIR` not found."
        )
        self.s3_staging_bucket, self.s3_staging_key = self.s3_staging_dir.replace(
            "s3://", ""
        ).split("/", 1)
        self.work_group = os.getenv("AWS_ATHENA_WORKGROUP")
        assert self.work_group, "Required environment variable `AWS_ATHENA_WORKGROUP` not found."
        self.spark_work_group = os.getenv("AWS_ATHENA_SPARK_WORKGROUP")
        assert self.spark_work_group, (
            "Required environment variable `AWS_ATHENA_SPARK_WORKGROUP` not found."
        )
        self.default_work_group = os.getenv("AWS_ATHENA_DEFAULT_WORKGROUP", "primary")
        self.managed_work_group = os.getenv("AWS_ATHENA_MANAGED_WORKGROUP")
        # Each test process creates the objects of its tests in its own schema.
        self.schema = _random_schema()
        # The read-only tables and views from tests/pyathena/tables.py, and the
        # data files they read, are in their own schema. With pytest-xdist, the
        # controller creates it once and passes its name to the workers, which
        # replace this value in pytest_configure (tests/pyathena/conftest.py).
        self.fixture_schema = _random_schema()
        # Optional Amazon S3 Tables configuration. `s3tables_catalog` is the
        # registered table-bucket catalog, e.g. "s3tablescatalog/<table-bucket>".
        # Each test process creates its own namespace in it, named like the schema,
        # because a table dropped during a listing of a shared namespace fails
        # the listing. The S3 Tables tests skip when the catalog is unset.
        self.s3tables_catalog = os.getenv("AWS_ATHENA_S3_TABLES_CATALOG")
        self.s3tables_namespace = self.schema if self.s3tables_catalog else None

    @property
    def s3_filesystem_test_file_key(self):
        """The S3 key of the read-only file the filesystem tests read.

        Returns:
            The key, under the fixture schema's prefix in the staging directory.
        """
        return f"{self.s3_staging_key}{self.fixture_schema}/filesystem/test_read/test.dat"


ENV = Env()
