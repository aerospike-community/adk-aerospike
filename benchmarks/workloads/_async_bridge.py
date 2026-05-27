"""Thread-local asyncio loop for sync benchmark methods.

``ai-ecosystem-benchmark`` runs each test in a worker thread pool. ADK
services are async, so each worker gets its own event loop (fork-safe with
the Aerospike client's thread-safe sync client underneath ``to_thread``).
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Coroutine
from typing import TypeVar

_T = TypeVar("_T")

_local = threading.local()


def run_async(coro: Coroutine[object, object, _T]) -> _T:
    loop = getattr(_local, "loop", None)
    if loop is None or loop.is_closed():
        loop = asyncio.new_event_loop()
        _local.loop = loop
    return loop.run_until_complete(coro)
