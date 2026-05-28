"""Long-session append path — chunk flush under sustained writes."""

from __future__ import annotations

import itertools
from typing import Any

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


class ChunkStressWorkload(BaseBenchmarkWorkload):
    """Appends to one session with a low flush threshold to force chunking.

    ``aerospike_session_append_chunked`` — sequential hot tail + periodic flush
    to immutable chunk records (production long conversations).
    """

    APP = "bench_eco_chunk"

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
        self._event_size_bytes = int(params.get("event_size_bytes", 600))
        self._flush_threshold = int(params.get("flush_threshold_bytes", 200))
        self._svc: AerospikeSessionService | RedisSessionService | None = None
        self._session: Any = None
        self._seq = itertools.count()

    def setup(self) -> None:
        if self.is_aerospike_enabled():
            assert self.aerospike_connection_string is not None
            self._svc = AerospikeSessionService.from_uri(self.aerospike_connection_string)
            self._svc._flush_threshold = self._flush_threshold
            run_async(self._reset_session())
        elif self.is_redis_enabled():
            assert self.redis_connection_string is not None
            self._svc = session_service(self.redis_connection_string)
            run_async(self._reset_session_redis())
        else:
            raise RuntimeError("no backend connection string configured")

    def between_benchmarks(self) -> None:
        return None

    def teardown(self) -> None:
        if self._svc is not None and self._session is not None:
            run_async(
                self._svc.delete_session(
                    app_name=self.APP,
                    user_id="u0",
                    session_id=self._session.id,
                )
            )
            if isinstance(self._svc, AerospikeSessionService):
                self._svc.close()
            else:
                run_async(close_session(self._svc))
        self._svc = None
        self._session = None

    def redis_session_append_chunked(self) -> None:
        self.aerospike_session_append_chunked()

    def aerospike_session_append_chunked(self) -> None:
        svc = self._svc
        session = self._session
        assert svc is not None and session is not None
        n = next(self._seq)
        text = filler_text(self._event_size_bytes, seed=n)
        run_async(svc.append_event(session, make_event(text, n)))

    async def _reset_session(self) -> None:
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
        self._session = await svc.create_session(
            app_name=self.APP,
            user_id="u0",
            session_id=new_session_id(),
        )

    async def _reset_session_redis(self) -> None:
        svc = self._svc
        assert isinstance(svc, RedisSessionService)
        await init_session(svc)
        await purge_user_sessions(svc, self.APP, "u0")
        self._session = await svc.create_session(
            app_name=self.APP,
            user_id="u0",
            session_id=new_session_id(),
        )
