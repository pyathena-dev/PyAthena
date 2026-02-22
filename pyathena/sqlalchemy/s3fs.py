from typing import TYPE_CHECKING

from pyathena.sqlalchemy.base import AthenaDialect

if TYPE_CHECKING:
    from types import ModuleType


class AthenaS3FSDialect(AthenaDialect):
    """SQLAlchemy dialect for PyAthena with S3FS cursor.

    This dialect uses the S3FSCursor which reads CSV results via
    PyAthena's S3FileSystem without requiring pandas or pyarrow.

    Example:
        >>> from sqlalchemy import create_engine
        >>> engine = create_engine(
        ...     "awsathena+s3fs://:@athena.us-east-1.amazonaws.com/database"
        ...     "?s3_staging_dir=s3://bucket/path"
        ... )
    """

    driver = "s3fs"
    supports_statement_cache = True

    def create_connect_args(self, url):
        from pyathena.s3fs.cursor import S3FSCursor

        opts = super()._create_connect_args(url)
        opts.update({"cursor_class": S3FSCursor})
        return [[], opts]

    @classmethod
    def import_dbapi(cls) -> "ModuleType":
        return super().import_dbapi()
