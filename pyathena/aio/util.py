# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Optional

from pyathena.util import RetryConfig, retry_api_call


async def async_retry_api_call(
    func: Callable[..., Any],
    config: RetryConfig,
    logger: Optional[logging.Logger] = None,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Execute a function with retry logic in a thread to avoid blocking the event loop.

    Wraps ``retry_api_call`` with ``asyncio.to_thread()`` so that blocking
    boto3 calls do not block the asyncio event loop.

    Args:
        func: The AWS API function to call.
        config: RetryConfig instance specifying retry behavior.
        logger: Optional logger for retry attempt logging.
        *args: Positional arguments to pass to ``retry_api_call``.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The result of the successful function call.
    """
    return await asyncio.to_thread(retry_api_call, func, config, logger, *args, **kwargs)
