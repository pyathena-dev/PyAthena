from typing import TYPE_CHECKING

from pyathena.sqlalchemy.base import AthenaDialect
from pyathena.util import strtobool

if TYPE_CHECKING:
    from types import ModuleType


class AthenaPolarsDialect(AthenaDialect):
    """SQLAlchemy dialect for Amazon Athena with Polars DataFrame result format.

    This dialect extends AthenaDialect to use PolarsCursor, which returns
    query results as Polars DataFrames using Polars' native reading capabilities.
    It does not require PyArrow for basic functionality, making it a lightweight
    option for analytical workloads.

    Connection URL Format:
        ``awsathena+polars://{access_key}:{secret_key}@athena.{region}.amazonaws.com/{schema}``

    Query Parameters:
        In addition to the base dialect parameters:
        - unload: If "true", use UNLOAD for Parquet output (better performance
          for large datasets)

    Example:
        >>> from sqlalchemy import create_engine
        >>> engine = create_engine(
        ...     "awsathena+polars://:@athena.us-west-2.amazonaws.com/default"
        ...     "?s3_staging_dir=s3://my-bucket/athena-results/"
        ...     "&unload=true"
        ... )

    See Also:
        :class:`~pyathena.polars.cursor.PolarsCursor`: The underlying cursor
            implementation.
        :class:`~pyathena.sqlalchemy.base.AthenaDialect`: Base dialect class.
    """

    driver = "polars"
    supports_statement_cache = True

    def create_connect_args(self, url):
        from pyathena.polars.cursor import PolarsCursor

        opts = super()._create_connect_args(url)
        opts.update({"cursor_class": PolarsCursor})
        cursor_kwargs = {}
        if "unload" in opts:
            cursor_kwargs.update({"unload": bool(strtobool(opts.pop("unload")))})
        if cursor_kwargs:
            opts.update({"cursor_kwargs": cursor_kwargs})
        return [[], opts]

    @classmethod
    def import_dbapi(cls) -> "ModuleType":
        return super().import_dbapi()
