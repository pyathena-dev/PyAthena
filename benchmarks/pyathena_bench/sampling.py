# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Event-loop lag sampling, shared by every suite without importing a DataFrame backend."""

from __future__ import annotations

import asyncio
import time


async def heartbeat(interval: float, stop: asyncio.Event, samples: list[float]) -> None:
    """Record how late the event loop wakes a sleeping task, until stopped.

    Args:
        interval: The sleep of each sample, in seconds.
        stop: Event that ends the sampling.
        samples: List to append the lag of each wake-up to, in seconds.
    """
    while not stop.is_set():
        start = time.perf_counter()
        await asyncio.sleep(interval)
        samples.append(max(0.0, time.perf_counter() - start - interval))
