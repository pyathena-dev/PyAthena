from pyathena.sqlalchemy.base import AthenaDialect
from pyathena.sqlalchemy.preparer import (
    AthenaDDLIdentifierPreparer,
    AthenaDMLIdentifierPreparer,
)

# Multi-part identifier quoting for S3 Tables. Amazon S3 Tables are addressed by a
# three-part identifier (catalog.namespace.table) whose catalog segment is
# ``s3tablescatalog/<table-bucket>``, so a dotted schema must be split and each
# part quoted independently.


def test_quote_schema_splits_catalog_and_namespace_ddl():
    # DDL preparer quotes with backticks; only the catalog needs quoting
    # (it contains "/"), the namespace does not.
    preparer = AthenaDDLIdentifierPreparer(AthenaDialect())
    assert preparer.quote_schema("s3tablescatalog/bucket.ns") == "`s3tablescatalog/bucket`.ns"


def test_quote_schema_splits_catalog_and_namespace_dml():
    # DML preparer quotes with double quotes (Presto/Trino convention).
    preparer = AthenaDMLIdentifierPreparer(AthenaDialect())
    assert preparer.quote_schema("s3tablescatalog/bucket.ns") == '"s3tablescatalog/bucket".ns'


def test_quote_schema_single_token_unchanged():
    # A schema without a dot must be quoted exactly as before (no splitting).
    preparer = AthenaDDLIdentifierPreparer(AthenaDialect())
    assert preparer.quote_schema("some_db") == "some_db"
