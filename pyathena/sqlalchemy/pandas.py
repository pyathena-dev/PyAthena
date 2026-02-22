from typing import TYPE_CHECKING

from pyathena.sqlalchemy.base import AthenaDialect
from pyathena.util import strtobool

if TYPE_CHECKING:
    from types import ModuleType


class AthenaPandasDialect(AthenaDialect):
    """SQLAlchemy dialect for Amazon Athena with pandas DataFrame result format.

    This dialect extends AthenaDialect to use PandasCursor, which returns
    query results as pandas DataFrames. This integration enables seamless
    use of Athena data in data analysis and machine learning workflows.

    Connection URL Format:
        ``awsathena+pandas://{access_key}:{secret_key}@athena.{region}.amazonaws.com/{schema}``

    Query Parameters:
        In addition to the base dialect parameters:
        - unload: If "true", use UNLOAD for Parquet output (better performance
          for large datasets)
        - engine: CSV parsing engine ("c", "python", or "pyarrow")
        - chunksize: Number of rows per chunk for memory-efficient processing

    Example:
        >>> from sqlalchemy import create_engine
        >>> engine = create_engine(
        ...     "awsathena+pandas://:@athena.us-west-2.amazonaws.com/default"
        ...     "?s3_staging_dir=s3://my-bucket/athena-results/"
        ...     "&unload=true&chunksize=10000"
        ... )

    See Also:
        :class:`~pyathena.pandas.cursor.PandasCursor`: The underlying cursor
            implementation.
        :class:`~pyathena.sqlalchemy.base.AthenaDialect`: Base dialect class.
    """

    driver = "pandas"
    supports_statement_cache = True

    def create_connect_args(self, url):
        from pyathena.pandas.cursor import PandasCursor

        opts = super()._create_connect_args(url)
        opts.update({"cursor_class": PandasCursor})
        cursor_kwargs = {}
        if "unload" in opts:
            cursor_kwargs.update({"unload": bool(strtobool(opts.pop("unload")))})
        if "engine" in opts:
            cursor_kwargs.update({"engine": opts.pop("engine")})
        if "chunksize" in opts:
            cursor_kwargs.update({"chunksize": int(opts.pop("chunksize"))})  # type: ignore[dict-item]
        if cursor_kwargs:
            opts.update({"cursor_kwargs": cursor_kwargs})
        return [[], opts]

    @classmethod
    def import_dbapi(cls) -> "ModuleType":
        return super().import_dbapi()
