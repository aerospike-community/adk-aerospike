"""Long-session append path — chunk flush under sustained writes."""

from __future__ import annotations

import itertools
from typing import Any

from adk_aerospike import AerospikeSessionService
from ai_ecosystem_benchmark import BaseBenchmarkWorkload

from ._async_bridge import run_async
from ._fixtures import filler_text, make_event, new_session_id


class ChunkStressWorkload(BaseBenchmarkWorkload):
    """Appends to one session with a low flush threshold to force chunking.

    ``aerospike_session_append_chunked`` — sequential hot tail + periodic flush
    to immutable chunk records (production long conversations).
    """

    APP = "bench_eco_chunk"

    def __init__(
        self,
        aerospike_connection_string: str | None = None,
        **params: Any,
    ) -> None:
        super().__init__(aerospike_connection_string=aerospike_connection_string)
        self._event_size_bytes = int(params.get("event_size_bytes", 600))
        self._flush_threshold = int(params.get("flush_threshold_bytes", 200))
        self._svc: AerospikeSessionService | None = None
        self._session: Any = None
        self._seq = itertools.count()

    def setup(self) -> None:
        assert self.aerospike_connection_string is not None
        self._svc = AerospikeSessionService.from_uri(self.aerospike_connection_string)
        self._svc._flush_threshold = self._flush_threshold
        run_async(self._reset_session())

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
            self._svc.close()
        self._svc = None
        self._session = None

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
