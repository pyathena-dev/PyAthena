# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING

from pyathena.sqlalchemy.async_base import AthenaAioDialect

if TYPE_CHECKING:
    from types import ModuleType


class AthenaAioRestDialect(AthenaAioDialect):
    """Async SQLAlchemy dialect for Amazon Athena using the standard REST API cursor.

    This dialect uses ``AioCursor`` for native asyncio query execution.
    Results are returned as Python tuples with type conversion handled by
    the default converter.

    Connection URL Format:
        ``awsathena+aiorest://{access_key}:{secret_key}@athena.{region}.amazonaws.com/{schema}``

    Example:
        >>> from sqlalchemy.ext.asyncio import create_async_engine
        >>> engine = create_async_engine(
        ...     "awsathena+aiorest://:@athena.us-west-2.amazonaws.com/default"
        ...     "?s3_staging_dir=s3://my-bucket/athena-results/"
        ... )

    See Also:
        :class:`~pyathena.aio.cursor.AioCursor`: The underlying async cursor.
        :class:`~pyathena.sqlalchemy.async_base.AthenaAioDialect`: Base async dialect.
    """

    driver = "aiorest"
    supports_statement_cache = True

    @classmethod
    def import_dbapi(cls) -> "ModuleType":
        return super().import_dbapi()
