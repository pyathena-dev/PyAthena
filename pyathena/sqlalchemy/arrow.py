from typing import TYPE_CHECKING

from pyathena.sqlalchemy.base import AthenaDialect
from pyathena.util import strtobool

if TYPE_CHECKING:
    from types import ModuleType


class AthenaArrowDialect(AthenaDialect):
    """SQLAlchemy dialect for Amazon Athena with Apache Arrow result format.

    This dialect extends AthenaDialect to use ArrowCursor, which returns
    query results as Apache Arrow Tables. Arrow format provides efficient
    columnar data representation, making it ideal for analytical workloads
    and integration with data science tools.

    Connection URL Format:
        ``awsathena+arrow://{access_key}:{secret_key}@athena.{region}.amazonaws.com/{schema}``

    Query Parameters:
        In addition to the base dialect parameters:
        - unload: If "true", use UNLOAD for Parquet output (better performance
          for large datasets)

    Example:
        >>> from sqlalchemy import create_engine
        >>> engine = create_engine(
        ...     "awsathena+arrow://:@athena.us-west-2.amazonaws.com/default"
        ...     "?s3_staging_dir=s3://my-bucket/athena-results/"
        ...     "&unload=true"
        ... )

    See Also:
        :class:`~pyathena.arrow.cursor.ArrowCursor`: The underlying cursor
            implementation.
        :class:`~pyathena.sqlalchemy.base.AthenaDialect`: Base dialect class.
    """

    driver = "arrow"
    supports_statement_cache = True

    def create_connect_args(self, url):
        from pyathena.arrow.cursor import ArrowCursor

        opts = super()._create_connect_args(url)
        opts.update({"cursor_class": ArrowCursor})
        cursor_kwargs = {}
        if "unload" in opts:
            cursor_kwargs.update({"unload": bool(strtobool(opts.pop("unload")))})
        if cursor_kwargs:
            opts.update({"cursor_kwargs": cursor_kwargs})
        return [[], opts]

    @classmethod
    def import_dbapi(cls) -> "ModuleType":
        return super().import_dbapi()
