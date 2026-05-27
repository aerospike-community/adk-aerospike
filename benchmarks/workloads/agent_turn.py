"""Composite workload: one agent turn end-to-end."""

from __future__ import annotations

import itertools
import threading
from typing import Any

from google.adk.sessions import Session
from google.adk.sessions.base_session_service import GetSessionConfig

from adk_aerospike import AerospikeMemoryService, AerospikeSessionService
from ai_ecosystem_benchmark import BaseBenchmarkWorkload

from ._async_bridge import run_async
from ._fixtures import (
    filler_text,
    make_event,
    memory_event_text,
    memory_query_text,
    new_session_id,
)


class AgentTurnWorkload(BaseBenchmarkWorkload):
    """Single timed operation matching a typical ADK turn against Aerospike.

    Per call: ``append_event`` → ``get_session`` (recent tail) → ``search_memory``.
    Use this when comparing backends on turn latency rather than isolated ops.
    """

    APP = "bench_eco_turn"

    def __init__(
        self,
        aerospike_connection_string: str | None = None,
        **params: Any,
    ) -> None:
        super().__init__(aerospike_connection_string=aerospike_connection_string)
        self._session_count = int(params.get("session_count", 16))
        self._event_size_bytes = int(params.get("event_size_bytes", 400))
        self._recent_events = int(params.get("recent_events", 20))
        self._query_tokens = int(params.get("query_tokens", 3))
        self._memory_corpus = int(params.get("memory_corpus", 2000))
        self._sess_svc: AerospikeSessionService | None = None
        self._mem_svc: AerospikeMemoryService | None = None
        self._sessions: list[Any] = []
        self._seq = itertools.count()
        self._rr = 0
        self._lock = threading.Lock()

    def setup(self) -> None:
        assert self.aerospike_connection_string is not None
        uri = self.aerospike_connection_string
        self._sess_svc = AerospikeSessionService.from_uri(uri)
        self._mem_svc = AerospikeMemoryService.from_uri(uri)
        run_async(self._seed())

    def between_benchmarks(self) -> None:
        return None

    def teardown(self) -> None:
        if self._sess_svc is not None:
            run_async(self._purge_sessions())
            self._sess_svc.close()
        if self._mem_svc is not None:
            run_async(self._mem_svc._purge_session_memories(self.APP, "u0", "preload"))
            self._mem_svc.close()
        self._sess_svc = None
        self._mem_svc = None
        self._sessions = []

    def aerospike_agent_turn(self) -> None:
        run_async(self._one_turn())

    async def _one_turn(self) -> None:
        sess_svc = self._sess_svc
        mem_svc = self._mem_svc
        assert sess_svc is not None and mem_svc is not None and self._sessions
        with self._lock:
            session = self._sessions[self._rr % len(self._sessions)]
            self._rr += 1
            n = next(self._seq)
        text = filler_text(self._event_size_bytes, seed=n)
        event = make_event(text, n)
        await sess_svc.append_event(session, event)
        config = GetSessionConfig(num_recent_events=self._recent_events)
        await sess_svc.get_session(
            app_name=self.APP,
            user_id="u0",
            session_id=session.id,
            config=config,
        )
        query = memory_query_text(n, self._query_tokens)
        await mem_svc.search_memory(app_name=self.APP, user_id="u0", query=query)

    async def _seed(self) -> None:
        await self._purge_sessions()
        mem = self._mem_svc
        assert mem is not None
        await mem._purge_session_memories(self.APP, "u0", "preload")
        events = [
            make_event(memory_event_text(i), i, event_id=f"bench-{i:08d}")
            for i in range(self._memory_corpus)
        ]
        await mem.add_session_to_memory(
            Session(id="preload", app_name=self.APP, user_id="u0", events=events)
        )
        sess_svc = self._sess_svc
        assert sess_svc is not None
        self._sessions = []
        for _ in range(self._session_count):
            self._sessions.append(
                await sess_svc.create_session(
                    app_name=self.APP,
                    user_id="u0",
                    session_id=new_session_id(),
                )
            )

    async def _purge_sessions(self) -> None:
        svc = self._sess_svc
        assert svc is not None
        resp = await svc.list_sessions(app_name=self.APP, user_id="u0")
        for s in resp.sessions:
            try:
                await svc.delete_session(
                    app_name=self.APP, user_id="u0", session_id=s.id
                )
            except Exception:
                pass
