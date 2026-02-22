from typing import TYPE_CHECKING

from pyathena.aio.sqlalchemy.base import AthenaAioDialect
from pyathena.util import strtobool

if TYPE_CHECKING:
    from types import ModuleType


class AthenaAioArrowDialect(AthenaAioDialect):
    """Async SQLAlchemy dialect for Amazon Athena with Apache Arrow result format.

    This dialect uses ``AioArrowCursor`` for native asyncio query execution
    with Apache Arrow Table results.

    Connection URL Format:
        ``awsathena+aioarrow://{access_key}:{secret_key}@athena.{region}.amazonaws.com/{schema}``

    Query Parameters:
        In addition to the base dialect parameters:
        - unload: If "true", use UNLOAD for Parquet output

    Example:
        >>> from sqlalchemy.ext.asyncio import create_async_engine
        >>> engine = create_async_engine(
        ...     "awsathena+aioarrow://:@athena.us-west-2.amazonaws.com/default"
        ...     "?s3_staging_dir=s3://my-bucket/athena-results/"
        ...     "&unload=true"
        ... )

    See Also:
        :class:`~pyathena.aio.arrow.cursor.AioArrowCursor`: The underlying async cursor.
        :class:`~pyathena.aio.sqlalchemy.base.AthenaAioDialect`: Base async dialect.
    """

    driver = "aioarrow"
    supports_statement_cache = True

    def create_connect_args(self, url):
        from pyathena.aio.arrow.cursor import AioArrowCursor

        opts = super()._create_connect_args(url)
        opts.update({"cursor_class": AioArrowCursor})
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
