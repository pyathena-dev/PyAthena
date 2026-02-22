__all__ = [
    "DataError",
    "DatabaseError",
    "Error",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Warning",
]


class Error(Exception):
    """Base exception class for all PyAthena errors.

    This is the root exception class in the PyAthena exception hierarchy.
    All other PyAthena exceptions inherit from this class, following the
    Python Database API Specification v2.0 (PEP 249).
    """


class Warning(Exception):  # noqa: N818
    """Exception for non-fatal warnings.

    This exception is used to signal warnings in PyAthena operations.
    Note: This class name conflicts with the built-in Warning class,
    but follows the DB API 2.0 specification.
    """


class InterfaceError(Error):
    """Exception for errors related to the database interface.

    Raised when there's an error in the database interface itself,
    such as connection problems or interface misuse.
    """


class DatabaseError(Error):
    """Base exception for database-related errors.

    This is the base class for all exceptions that are related to the
    database itself, rather than the interface. All other database
    error types inherit from this class.
    """


class InternalError(DatabaseError):
    """Exception for internal database errors.

    Raised when there's an internal error in the database system
    that is not due to user actions or programming errors.
    """


class OperationalError(DatabaseError):
    """Exception for errors during database operation processing.

    Raised when Athena query execution fails due to operational issues
    such as query timeouts, resource limits, permission errors, or
    invalid query syntax that wasn't caught at the programming level.
    """


class ProgrammingError(DatabaseError):
    """Exception for programming errors in database operations.

    Raised when there are errors in the way the database interface is
    being used, such as calling methods in the wrong order, using
    invalid parameters, or attempting operations on closed connections.
    """


class IntegrityError(DatabaseError):
    """Exception for data integrity constraint violations.

    Raised when a database operation would violate data integrity
    constraints, such as unique key violations or foreign key
    constraint failures.
    """


class DataError(DatabaseError):
    """Exception for errors due to invalid data.

    Raised when there are problems with the data being processed,
    such as data type conversion errors, values out of range,
    or malformed data structures.
    """


class NotSupportedError(DatabaseError):
    """Exception for unsupported database operations.

    Raised when attempting to use functionality that is not supported
    by Athena, such as transactions (commit/rollback) or certain
    SQL features that are not available in the Athena query engine.
    """
