"""Cursors that return Athena query results as Polars DataFrames."""

from pyathena.filesystem import register_s3_filesystem

register_s3_filesystem()
