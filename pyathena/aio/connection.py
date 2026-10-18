# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Asyncio-aware connection to Amazon Athena."""

from __future__ import annotations

import functools
from typing import Any

from pyathena.aio.cursor import AioCursor
from pyathena.aio.util import create_in_thread
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
        """Initialize the connection with ``AioCursor`` as the default cursor class.

        Args:
            **kwargs: Arguments forwarded to ``Connection.__init__``. If they do not
                include ``cursor_class``, it is set to ``AioCursor``.
        """
        if "cursor_class" not in kwargs:
            kwargs["cursor_class"] = AioCursor
        super().__init__(**kwargs)

    @classmethod
    async def create(
        cls,
        **kwargs: Any,
    ) -> AioConnection:
        """Async factory for creating an ``AioConnection``.

        Runs the (potentially blocking) ``__init__`` in a thread so that
        STS calls (``role_arn`` / ``serial_number``) do not block the loop.
        If the caller is cancelled, a connection built after the cancellation
        is closed (see :func:`~pyathena.aio.util.create_in_thread`).

        Args:
            **kwargs: Arguments forwarded to ``AioConnection.__init__``.

        Returns:
            A fully initialized ``AioConnection``.
        """
        return await create_in_thread(functools.partial(cls, **kwargs))

    async def __aenter__(self) -> AioConnection:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
