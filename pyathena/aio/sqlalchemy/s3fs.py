from typing import TYPE_CHECKING

from pyathena.aio.sqlalchemy.base import AthenaAioDialect

if TYPE_CHECKING:
    from types import ModuleType


class AthenaAioS3FSDialect(AthenaAioDialect):
    """Async SQLAlchemy dialect for PyAthena with S3FS cursor.

    This dialect uses ``AioS3FSCursor`` for native asyncio query execution
    with S3 filesystem-based CSV result reading.

    Connection URL Format:
        ``awsathena+aios3fs://{access_key}:{secret_key}@athena.{region}.amazonaws.com/{schema}``

    Example:
        >>> from sqlalchemy.ext.asyncio import create_async_engine
        >>> engine = create_async_engine(
        ...     "awsathena+aios3fs://:@athena.us-east-1.amazonaws.com/database"
        ...     "?s3_staging_dir=s3://bucket/path"
        ... )

    See Also:
        :class:`~pyathena.aio.s3fs.cursor.AioS3FSCursor`: The underlying async cursor.
        :class:`~pyathena.aio.sqlalchemy.base.AthenaAioDialect`: Base async dialect.
    """

    driver = "aios3fs"
    supports_statement_cache = True

    def create_connect_args(self, url):
        from pyathena.aio.s3fs.cursor import AioS3FSCursor

        opts = super()._create_connect_args(url)
        opts.update({"cursor_class": AioS3FSCursor})
        self._connect_options = opts
        return [[], opts]

    @classmethod
    def import_dbapi(cls) -> "ModuleType":
        return super().import_dbapi()
