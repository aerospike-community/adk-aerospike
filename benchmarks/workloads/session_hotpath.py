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
from ._redis_backend import (
    RedisSessionService,
    close_session,
    init_session,
    purge_user_sessions,
    session_service,
)


class SessionHotpathWorkload(BaseBenchmarkWorkload):
    """Models steady-state agent traffic: append turns and read recent context.

    ``aerospike_session_append`` — one ``append_event`` (single-record atomic op).
    ``aerospike_session_get_recent`` — ``get_session`` with ``num_recent_events``
        (tail fast path + ``batch_read`` for scoped state).
    ``aerospike_session_list`` — enumerate sessions for a user (manifest + batch_read).
  """

    APP = "bench_eco_session"

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
        self._session_count = int(params.get("session_count", 32))
        self._event_size_bytes = int(params.get("event_size_bytes", 200))
        self._recent_events = int(params.get("recent_events", 10))
        self._svc: AerospikeSessionService | RedisSessionService | None = None
        self._sessions: list[Any] = []
        self._seq = itertools.count()
        self._rr = 0
        self._lock = threading.Lock()

    def setup(self) -> None:
        if self.is_aerospike_enabled():
            assert self.aerospike_connection_string is not None
            self._svc = AerospikeSessionService.from_uri(self.aerospike_connection_string)
            run_async(self._purge_and_seed())
        elif self.is_redis_enabled():
            assert self.redis_connection_string is not None
            self._svc = session_service(self.redis_connection_string)
            run_async(self._purge_and_seed_redis())
        else:
            raise RuntimeError("no backend connection string configured")

    def between_benchmarks(self) -> None:
        return None

    def teardown(self) -> None:
        if self._svc is not None:
            if isinstance(self._svc, AerospikeSessionService):
                run_async(self._purge_and_seed())
                self._svc.close()
            else:
                run_async(self._purge_and_seed_redis())
                run_async(close_session(self._svc))
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

    def redis_session_append(self) -> None:
        self.aerospike_session_append()

    def redis_session_get_recent(self) -> None:
        self.aerospike_session_get_recent()

    def redis_session_list(self) -> None:
        self.aerospike_session_list()

    async def _purge_user_sessions_aerospike(
        self, svc: AerospikeSessionService, app: str, user: str
    ) -> None:
        """Drop all sessions for (app, user), including pre-manifest orphans."""
        import asyncio

        from aerospike import predicates

        from adk_aerospike._internal.schema import Bins

        resp = await svc.list_sessions(app_name=app, user_id=user)
        for s in resp.sessions:
            try:
                await svc.delete_session(app_name=app, user_id=user, session_id=s.id)
            except Exception:
                pass
        query = svc._client.query(svc._schema.namespace, svc._schema.sessions_set)
        query.where(predicates.equals(Bins.USER_ID, user))
        records = await asyncio.to_thread(query.results)
        for _, _, bins in records:
            if bins.get(Bins.APP_NAME) != app:
                continue
            sid = bins.get(Bins.SESSION_ID)
            if not sid:
                continue
            try:
                await svc.delete_session(app_name=app, user_id=user, session_id=sid)
            except Exception:
                pass

    async def _purge_and_seed_redis(self) -> None:
        svc = self._svc
        assert isinstance(svc, RedisSessionService)
        await init_session(svc)
        await purge_user_sessions(svc, self.APP, "u0")
        self._sessions = []
        for _ in range(self._session_count):
            sess = await svc.create_session(
                app_name=self.APP,
                user_id="u0",
                session_id=new_session_id(),
            )
            self._sessions.append(sess)

    async def _purge_and_seed(self) -> None:
        svc = self._svc
        assert svc is not None
        await self._purge_user_sessions_aerospike(svc, self.APP, "u0")
        self._sessions = []
        for _ in range(self._session_count):
            sess = await svc.create_session(
                app_name=self.APP,
                user_id="u0",
                session_id=new_session_id(),
            )
            self._sessions.append(sess)
