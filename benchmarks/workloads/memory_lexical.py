"""Lexical memory search and per-turn ingest."""

from __future__ import annotations

import threading
from typing import Any

from google.adk.sessions import Session

from adk_aerospike import AerospikeMemoryService
from ai_ecosystem_benchmark import BaseBenchmarkWorkload
from ._async_bridge import run_async
from ._fixtures import make_event, memory_event_text, memory_query_text
from ._redis_backend import (
    RedisMemoryService,
    close_memory,
    init_memory,
    memory_service,
    purge_user_memory,
)


class MemoryLexicalWorkload(BaseBenchmarkWorkload):
    """Preloads a keyword corpus, then exercises search and incremental ingest.

    ``aerospike_memory_search`` — ``search_memory`` (list-element sec-index, one
        query per token).
    ``aerospike_memory_ingest`` — ``add_session_to_memory`` with a single new
        event (models indexing after each agent turn).
    """

    APP = "bench_eco_memory"

    def __init__(
        self,
        aerospike_connection_string: str | None = None,
        redis_connection_string: str | None = None,
        **params: Any,
    ) -> None:
        super().__init__(
            aerospike_connection_string=aerospike_connection_string,
            redis_connection_string=redis_connection_string,
        )
        self._corpus = int(params.get("corpus", 5000))
        self._query_tokens = int(params.get("query_tokens", 3))
        self._mem: AerospikeMemoryService | RedisMemoryService | None = None
        self._slot = 0
        self._ingest_seq = 0
        self._lock = threading.Lock()

    def setup(self) -> None:
        if self.is_aerospike_enabled():
            assert self.aerospike_connection_string is not None
            self._mem = AerospikeMemoryService.from_uri(self.aerospike_connection_string)
            run_async(self._preload())
        elif self.is_redis_enabled():
            assert self.redis_connection_string is not None
            self._mem = memory_service(self.redis_connection_string)
            run_async(self._preload_redis())
        else:
            raise RuntimeError("no backend connection string configured")

    def between_benchmarks(self) -> None:
        return None

    def teardown(self) -> None:
        if self._mem is not None:
            if isinstance(self._mem, AerospikeMemoryService):
                run_async(self._mem._purge_session_memories(self.APP, "u0", "preload"))
                self._mem.close()
            else:
                run_async(purge_user_memory(self._mem, self.APP, "u0"))
                run_async(close_memory(self._mem))
        self._mem = None

    def aerospike_memory_search(self) -> None:
        mem = self._mem
        assert mem is not None
        with self._lock:
            slot = self._slot
            self._slot += 1
        query = memory_query_text(slot, self._query_tokens)
        run_async(mem.search_memory(app_name=self.APP, user_id="u0", query=query))

    def aerospike_memory_ingest(self) -> None:
        mem = self._mem
        assert mem is not None
        with self._lock:
            n = self._ingest_seq
            self._ingest_seq += 1
        text = memory_event_text(n)
        ev = make_event(text, n, event_id=f"ingest-{n:08d}")
        session = Session(
            id="ingest-sess",
            app_name=self.APP,
            user_id="u0",
            events=[ev],
        )
        run_async(mem.add_session_to_memory(session))

    def redis_memory_search(self) -> None:
        self.aerospike_memory_search()

    def redis_memory_ingest(self) -> None:
        self.aerospike_memory_ingest()

    async def _preload_redis(self) -> None:
        mem = self._mem
        assert isinstance(mem, RedisMemoryService)
        await init_memory(mem)
        await purge_user_memory(mem, self.APP, "u0")
        events = []
        for i in range(self._corpus):
            ev = make_event(memory_event_text(i), i, event_id=f"bench-{i:08d}")
            events.append(ev)
        session = Session(
            id="preload",
            app_name=self.APP,
            user_id="u0",
            events=events,
        )
        await mem.add_session_to_memory(session)

    async def _preload(self) -> None:
        mem = self._mem
        assert mem is not None
        await mem._purge_session_memories(self.APP, "u0", "preload")
        events = []
        for i in range(self._corpus):
            ev = make_event(memory_event_text(i), i, event_id=f"bench-{i:08d}")
            events.append(ev)
        session = Session(
            id="preload",
            app_name=self.APP,
            user_id="u0",
            events=events,
        )
        await mem.add_session_to_memory(session)
