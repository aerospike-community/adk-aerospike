"""Session write/read patterns from a multi-turn agent loop."""

from __future__ import annotations

import itertools
import threading
from typing import Any

from google.adk.sessions.base_session_service import GetSessionConfig

from adk_aerospike import AerospikeSessionService
from ai_ecosystem_benchmark import BaseBenchmarkWorkload

from ._async_bridge import run_async
from ._fixtures import filler_text, make_event, new_session_id


class SessionHotpathWorkload(BaseBenchmarkWorkload):
    """Models steady-state agent traffic: append turns and read recent context.

    ``aerospike_session_append`` — one ``append_event`` (single-record atomic op).
    ``aerospike_session_get_recent`` — ``get_session`` with ``num_recent_events``
        (tail fast path + ``batch_read`` for scoped state).
    ``aerospike_session_list`` — enumerate sessions for a user (sec-index on uid).
  """

    APP = "bench_eco_session"

    def __init__(
        self,
        aerospike_connection_string: str | None = None,
        **params: Any,
    ) -> None:
        super().__init__(aerospike_connection_string=aerospike_connection_string)
        self._session_count = int(params.get("session_count", 32))
        self._event_size_bytes = int(params.get("event_size_bytes", 200))
        self._recent_events = int(params.get("recent_events", 10))
        self._svc: AerospikeSessionService | None = None
        self._sessions: list[Any] = []
        self._seq = itertools.count()
        self._rr = 0
        self._lock = threading.Lock()

    def setup(self) -> None:
        assert self.aerospike_connection_string is not None
        self._svc = AerospikeSessionService.from_uri(self.aerospike_connection_string)
        run_async(self._purge_and_seed())

    def between_benchmarks(self) -> None:
        return None

    def teardown(self) -> None:
        if self._svc is not None:
            run_async(self._purge_and_seed())
            self._svc.close()
        self._svc = None
        self._sessions = []

    def aerospike_session_append(self) -> None:
        svc = self._svc
        assert svc is not None and self._sessions
        with self._lock:
            session = self._sessions[self._rr % len(self._sessions)]
            self._rr += 1
            n = next(self._seq)
        text = filler_text(self._event_size_bytes, seed=n)
        run_async(svc.append_event(session, make_event(text, n)))

    def aerospike_session_get_recent(self) -> None:
        svc = self._svc
        assert svc is not None and self._sessions
        with self._lock:
            session = self._sessions[self._rr % len(self._sessions)]
            self._rr += 1
        config = GetSessionConfig(num_recent_events=self._recent_events)
        run_async(
            svc.get_session(
                app_name=self.APP,
                user_id="u0",
                session_id=session.id,
                config=config,
            )
        )

    def aerospike_session_list(self) -> None:
        svc = self._svc
        assert svc is not None
        run_async(svc.list_sessions(app_name=self.APP, user_id="u0"))

    async def _purge_and_seed(self) -> None:
        svc = self._svc
        assert svc is not None
        resp = await svc.list_sessions(app_name=self.APP, user_id="u0")
        for s in resp.sessions:
            try:
                await svc.delete_session(
                    app_name=self.APP, user_id="u0", session_id=s.id
                )
            except Exception:
                pass
        self._sessions = []
        for _ in range(self._session_count):
            sess = await svc.create_session(
                app_name=self.APP,
                user_id="u0",
                session_id=new_session_id(),
            )
            self._sessions.append(sess)
