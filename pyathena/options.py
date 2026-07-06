from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any


@dataclass(frozen=True)
class ExecuteOptions:
    """Shared options for ``Cursor.execute()`` across all cursor implementations.

    This dataclass is the single source of truth for the query-execution
    arguments shared by every SQL cursor type (sync/async/aio and
    pandas/arrow/polars/s3fs variants). It can be passed to ``execute()``
    via the ``options`` keyword argument as an alternative to individual
    keyword arguments:

        >>> from pyathena.options import ExecuteOptions
        >>> options = ExecuteOptions(work_group="primary", cache_size=100)
        >>> cursor.execute("SELECT * FROM my_table", options=options)

    When both ``options`` and individual keyword arguments are provided,
    the individual keyword arguments take precedence. This allows building
    a base ``ExecuteOptions`` once and tweaking it per call:

        >>> cursor.execute("SELECT ...", options=options, work_group="adhoc")

    Passing None for an individual keyword argument is treated as "not
    provided" and leaves the corresponding ``options`` field unchanged; to
    reset a field, use :meth:`merge` or construct a new instance.

    Attributes:
        work_group: Athena workgroup to use for this query. Overrides the
            connection-level workgroup.
        s3_staging_dir: S3 location for query results. Overrides the
            connection-level staging directory.
        cache_size: Number of recent queries to scan for client-side result
            caching. 0 (default) disables the cache lookup, unless
            ``cache_expiration_time`` is set to a positive value, in which
            case all queries within the expiration window are scanned.
        cache_expiration_time: Maximum age in seconds of a cached query
            result to consider for reuse. 0 (default) means no age limit.
        result_reuse_enable: Enable Athena server-side result reuse for this
            query. None (default) falls back to the connection-level setting.
        result_reuse_minutes: Maximum age in minutes of a previous query
            result that Athena should consider for reuse. None (default)
            falls back to the connection-level setting.
        paramstyle: Parameter style for this query ('qmark' or 'pyformat').
            None (default) uses the module-level ``pyathena.paramstyle``.
        on_start_query_execution: Callback invoked with the query ID
            immediately after the StartQueryExecution API call. Invoked by
            synchronous and aio cursors; ``AsyncCursor``-based cursors
            return the query ID directly through their execution model and
            do not invoke it.
        result_set_type_hints: Mapping of column names (or indices) to Athena
            DDL type signatures for precise type conversion within complex
            types. For example:
            ``{"tags": "array(varchar)", "metadata": "map(varchar, integer)"}``
    """

    work_group: str | None = None
    s3_staging_dir: str | None = None
    cache_size: int = 0
    cache_expiration_time: int = 0
    result_reuse_enable: bool | None = None
    result_reuse_minutes: int | None = None
    paramstyle: str | None = None
    on_start_query_execution: Callable[[str], None] | None = None
    result_set_type_hints: dict[str | int, str] | None = None

    @classmethod
    def resolve(cls, options: ExecuteOptions | None, **overrides: Any) -> ExecuteOptions:
        """Return ``options`` (or a default instance) with ``overrides`` applied.

        This is the canonical way for ``execute()`` implementations to combine
        the ``options`` argument with the individual keyword arguments.

        Args:
            options: Base options, or None to start from the defaults.
            **overrides: Field values to apply on top of ``options``.
                None values are ignored.

        Returns:
            The effective ``ExecuteOptions`` for the call.
        """
        return (options if options is not None else cls()).merge(**overrides)

    def merge(self, **overrides: Any) -> ExecuteOptions:
        """Return a new instance with non-None ``overrides`` applied.

        Args:
            **overrides: Field values to apply on top of this instance.
                None values are ignored, so an omitted ``execute()`` keyword
                argument never clobbers a value set on ``options``.

        Returns:
            A new ``ExecuteOptions`` with the overrides applied.

        Raises:
            TypeError: If an override name is not a field of this class.
        """
        return replace(self, **{k: v for k, v in overrides.items() if v is not None})
