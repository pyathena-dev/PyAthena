# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
from typing import Any

from pyathena.aio.cursor import AioCursor
from pyathena.connection import Connection


class AioConnection(Connection[AioCursor]):
    """Async-aware connection to Amazon Athena.

    Wraps the synchronous ``Connection`` with async context manager support
    and provides ``create()`` for non-blocking initialization.

    Example:
        >>> async with await AioConnection.create(
        ...     s3_staging_dir="s3://bucket/path/",
        ...     region_name="us-east-1",
        ... ) as conn:
        ...     async with conn.cursor() as cursor:
        ...         await cursor.execute("SELECT 1")
        ...         print(await cursor.fetchone())
    """

    def __init__(self, **kwargs: Any) -> None:
        if "cursor_class" not in kwargs:
            kwargs["cursor_class"] = AioCursor
        super().__init__(**kwargs)

    @classmethod
    async def create(
        cls,
        **kwargs: Any,
    ) -> "AioConnection":
        """Async factory for creating an ``AioConnection``.

        Runs the (potentially blocking) ``__init__`` in a thread so that
        STS calls (``role_arn`` / ``serial_number``) do not block the loop.

        Args:
            **kwargs: Arguments forwarded to ``AioConnection.__init__``.

        Returns:
            A fully initialized ``AioConnection``.
        """
        return await asyncio.to_thread(cls, **kwargs)

    async def __aenter__(self) -> "AioConnection":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
