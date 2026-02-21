# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING

from pyathena.sqlalchemy.async_base import AthenaAioDialect
from pyathena.util import strtobool

if TYPE_CHECKING:
    from types import ModuleType


class AthenaAioPolarsDialect(AthenaAioDialect):
    """Async SQLAlchemy dialect for Amazon Athena with Polars DataFrame result format.

    This dialect uses ``AioPolarsCursor`` for native asyncio query execution
    with Polars DataFrame results.

    Connection URL Format:
        ``awsathena+aiopolars://{access_key}:{secret_key}@athena.{region}.amazonaws.com/{schema}``

    Query Parameters:
        In addition to the base dialect parameters:
        - unload: If "true", use UNLOAD for Parquet output

    Example:
        >>> from sqlalchemy.ext.asyncio import create_async_engine
        >>> engine = create_async_engine(
        ...     "awsathena+aiopolars://:@athena.us-west-2.amazonaws.com/default"
        ...     "?s3_staging_dir=s3://my-bucket/athena-results/"
        ...     "&unload=true"
        ... )

    See Also:
        :class:`~pyathena.aio.polars.cursor.AioPolarsCursor`: The underlying async cursor.
        :class:`~pyathena.sqlalchemy.async_base.AthenaAioDialect`: Base async dialect.
    """

    driver = "aiopolars"
    supports_statement_cache = True

    def create_connect_args(self, url):
        from pyathena.aio.polars.cursor import AioPolarsCursor

        opts = super()._create_connect_args(url)
        opts.update({"cursor_class": AioPolarsCursor})
        cursor_kwargs = {}
        if "unload" in opts:
            cursor_kwargs.update({"unload": bool(strtobool(opts.pop("unload")))})
        if cursor_kwargs:
            opts.update({"cursor_kwargs": cursor_kwargs})
        self._connect_options = opts
        return [[], opts]

    @classmethod
    def import_dbapi(cls) -> "ModuleType":
        return super().import_dbapi()
