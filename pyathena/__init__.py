from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any, overload

from pyathena.error import *  # noqa: F403

if TYPE_CHECKING:
    from pyathena.aio.connection import AioConnection
    from pyathena.connection import Connection, ConnectionCursor
    from pyathena.cursor import Cursor

try:
    from pyathena._version import __version__
except ImportError:
    try:
        from importlib.metadata import version

        __version__ = version("PyAthena")
    except Exception:
        __version__ = "unknown"
user_agent_extra: str = f"PyAthena/{__version__}"

# Globals https://www.python.org/dev/peps/pep-0249/#globals
apilevel: str = "2.0"
threadsafety: int = 2
paramstyle: str = "pyformat"


class DBAPITypeObject(frozenset[str]):
    """Type Objects and Constructors

    https://www.python.org/dev/peps/pep-0249/#type-objects-and-constructors
    """

    def __eq__(self, other: object):
        if isinstance(other, frozenset):
            return frozenset.__eq__(self, other)
        return other in self

    def __ne__(self, other: object):
        if isinstance(other, frozenset):
            return frozenset.__ne__(self, other)
        return other not in self

    def __hash__(self):
        return frozenset.__hash__(self)


# https://docs.aws.amazon.com/athena/latest/ug/data-types.html
STRING: DBAPITypeObject = DBAPITypeObject(("char", "varchar", "map", "array", "row"))
BINARY: DBAPITypeObject = DBAPITypeObject(("varbinary",))
BOOLEAN: DBAPITypeObject = DBAPITypeObject(("boolean",))
NUMBER: DBAPITypeObject = DBAPITypeObject(
    ("tinyint", "smallint", "bigint", "integer", "real", "double", "float", "decimal")
)
DATE: DBAPITypeObject = DBAPITypeObject(("date",))
TIME: DBAPITypeObject = DBAPITypeObject(("time", "time with time zone"))
DATETIME: DBAPITypeObject = DBAPITypeObject(("timestamp", "timestamp with time zone"))
JSON: DBAPITypeObject = DBAPITypeObject(("json",))

Date: type[datetime.date] = datetime.date
Time: type[datetime.time] = datetime.time
Timestamp: type[datetime.datetime] = datetime.datetime


@overload
def connect(*args, cursor_class: None = ..., **kwargs) -> Connection[Cursor]: ...


@overload
def connect(
    *args, cursor_class: type[ConnectionCursor], **kwargs
) -> Connection[ConnectionCursor]: ...


def connect(*args, **kwargs) -> Connection[Any]:
    """Create a new database connection to Amazon Athena.

    This function provides the main entry point for establishing connections
    to Amazon Athena. It follows the DB API 2.0 specification and returns
    a Connection object that can be used to create cursors for executing
    SQL queries.

    Args:
        s3_staging_dir: S3 location to store query results. Required if not
            using workgroups or if the workgroup doesn't have a result location.
            Pass an empty string to explicitly disable S3 staging and skip
            the ``AWS_ATHENA_S3_STAGING_DIR`` environment variable fallback
            (required for workgroups with managed query result storage).
        region_name: AWS region name. If not specified, uses the default region
            from your AWS configuration.
        schema_name: Athena database/schema name. Defaults to "default".
        catalog_name: Athena data catalog name. Defaults to "awsdatacatalog".
        work_group: Athena workgroup name. Can be used instead of s3_staging_dir
            if the workgroup has a result location configured.
        poll_interval: Time in seconds between polling for query completion.
            Defaults to 1.0.
        encryption_option: S3 encryption option for query results. Can be
            "SSE_S3", "SSE_KMS", or "CSE_KMS".
        kms_key: KMS key ID for encryption when using SSE_KMS or CSE_KMS.
        profile_name: AWS profile name to use for authentication.
        role_arn: ARN of IAM role to assume for authentication.
        role_session_name: Session name when assuming a role.
        cursor_class: Custom cursor class to use. If not specified, uses
            the default Cursor class.
        kill_on_interrupt: Whether to cancel running queries when interrupted.
            Defaults to True.
        **kwargs: Additional keyword arguments passed to the Connection constructor.

    Returns:
        A Connection object that can be used to create cursors and execute queries.

    Raises:
        ProgrammingError: If neither s3_staging_dir nor work_group is provided.

    Example:
        >>> import pyathena
        >>> conn = pyathena.connect(
        ...     s3_staging_dir='s3://my-bucket/staging/',
        ...     region_name='us-east-1',
        ...     schema_name='mydatabase'
        ... )
        >>> cursor = conn.cursor()
        >>> cursor.execute("SELECT * FROM mytable LIMIT 10")
        >>> results = cursor.fetchall()
    """
    from pyathena.connection import Connection

    return Connection(*args, **kwargs)


async def aio_connect(*args, **kwargs) -> AioConnection:
    """Create a new async database connection to Amazon Athena.

    This is the async counterpart of :func:`connect`. It returns an
    ``AioConnection`` whose cursors use native ``asyncio`` for polling
    and API calls, keeping the event loop free.

    Args:
        **kwargs: Arguments forwarded to ``AioConnection.create()``.
            See :func:`connect` for the full list of supported arguments.

    Returns:
        An ``AioConnection`` that produces ``AioCursor`` instances by default.

    Example:
        >>> import pyathena
        >>> conn = await pyathena.aio_connect(
        ...     s3_staging_dir='s3://my-bucket/staging/',
        ...     region_name='us-east-1',
        ... )
        >>> async with conn.cursor() as cursor:
        ...     await cursor.execute("SELECT 1")
        ...     print(await cursor.fetchone())
    """
    from pyathena.aio.connection import AioConnection

    return await AioConnection.create(*args, **kwargs)
