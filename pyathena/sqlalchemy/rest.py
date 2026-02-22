from typing import TYPE_CHECKING

from pyathena.sqlalchemy.base import AthenaDialect

if TYPE_CHECKING:
    from types import ModuleType


class AthenaRestDialect(AthenaDialect):
    """SQLAlchemy dialect for Amazon Athena using the standard REST API cursor.

    This dialect uses the default Cursor implementation, which retrieves
    query results via the GetQueryResults API. Results are returned as
    Python tuples with type conversion handled by the default converter.

    This is the standard dialect for general-purpose Athena access and is
    suitable for most use cases where specialized result formats (Arrow,
    pandas) are not required.

    Connection URL Format:
        ``awsathena+rest://{access_key}:{secret_key}@athena.{region}.amazonaws.com/{schema}``

    Example:
        >>> from sqlalchemy import create_engine
        >>> engine = create_engine(
        ...     "awsathena+rest://:@athena.us-west-2.amazonaws.com/default"
        ...     "?s3_staging_dir=s3://my-bucket/athena-results/"
        ... )

    See Also:
        :class:`~pyathena.cursor.Cursor`: The underlying cursor implementation.
        :class:`~pyathena.sqlalchemy.base.AthenaDialect`: Base dialect class.
    """

    driver = "rest"
    supports_statement_cache = True

    @classmethod
    def import_dbapi(cls) -> "ModuleType":
        return super().import_dbapi()
