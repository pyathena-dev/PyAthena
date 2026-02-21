# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING

from pyathena.aio.sqlalchemy.base import AthenaAioDialect
from pyathena.util import strtobool

if TYPE_CHECKING:
    from types import ModuleType


class AthenaAioPandasDialect(AthenaAioDialect):
    """Async SQLAlchemy dialect for Amazon Athena with pandas DataFrame result format.

    This dialect uses ``AioPandasCursor`` for native asyncio query execution
    with pandas DataFrame results.

    Connection URL Format:
        ``awsathena+aiopandas://{access_key}:{secret_key}@athena.{region}.amazonaws.com/{schema}``

    Query Parameters:
        In addition to the base dialect parameters:
        - unload: If "true", use UNLOAD for Parquet output
        - engine: CSV parsing engine ("c", "python", or "pyarrow")
        - chunksize: Number of rows per chunk for memory-efficient processing

    Example:
        >>> from sqlalchemy.ext.asyncio import create_async_engine
        >>> engine = create_async_engine(
        ...     "awsathena+aiopandas://:@athena.us-west-2.amazonaws.com/default"
        ...     "?s3_staging_dir=s3://my-bucket/athena-results/"
        ...     "&unload=true&chunksize=10000"
        ... )

    See Also:
        :class:`~pyathena.aio.pandas.cursor.AioPandasCursor`: The underlying async cursor.
        :class:`~pyathena.aio.sqlalchemy.base.AthenaAioDialect`: Base async dialect.
    """

    driver = "aiopandas"
    supports_statement_cache = True

    def create_connect_args(self, url):
        from pyathena.aio.pandas.cursor import AioPandasCursor

        opts = super()._create_connect_args(url)
        opts.update({"cursor_class": AioPandasCursor})
        cursor_kwargs = {}
        if "unload" in opts:
            cursor_kwargs.update({"unload": bool(strtobool(opts.pop("unload")))})
        if "engine" in opts:
            cursor_kwargs.update({"engine": opts.pop("engine")})
        if "chunksize" in opts:
            cursor_kwargs.update({"chunksize": int(opts.pop("chunksize"))})  # type: ignore
        if cursor_kwargs:
            opts.update({"cursor_kwargs": cursor_kwargs})
        self._connect_options = opts
        return [[], opts]

    @classmethod
    def import_dbapi(cls) -> "ModuleType":
        return super().import_dbapi()
