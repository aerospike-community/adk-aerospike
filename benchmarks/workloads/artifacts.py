"""Artifact save/load/list — tool output and user uploads."""

from __future__ import annotations

import threading
from typing import Any

from google.genai import types

from adk_aerospike import AerospikeArtifactService
from ai_ecosystem_benchmark import BaseBenchmarkWorkload

from ._async_bridge import run_async
from ._fixtures import filler_text
from ._redis_backend import RedisArtifactBenchStore


class ArtifactsWorkload(BaseBenchmarkWorkload):
    """Session-scoped artifacts at sizes typical of JSON tool results.

    ``aerospike_artifact_save`` — new version per call (~event_size bytes).
    ``aerospike_artifact_load`` — read latest version (single GET).
    ``aerospike_artifact_list_versions`` — sec-index query on fname + filter.
    """

    APP = "bench_eco_artifacts"
    SESSION = "sess0"
    FILENAME = "tool_output.json"

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
        self._payload_bytes = int(params.get("payload_bytes", 4096))
        self._art: AerospikeArtifactService | None = None
        self._redis_art: RedisArtifactBenchStore | None = None
        self._save_seq = 0
        self._lock = threading.Lock()

    def setup(self) -> None:
        payload = filler_text(self._payload_bytes).encode()
        if self.is_aerospike_enabled():
            assert self.aerospike_connection_string is not None
            self._art = AerospikeArtifactService.from_uri(self.aerospike_connection_string)
            part = types.Part(
                inline_data=types.Blob(mime_type="application/json", data=payload)
            )
            run_async(
                self._art.save_artifact(
                    app_name=self.APP,
                    user_id="u0",
                    session_id=self.SESSION,
                    filename=self.FILENAME,
                    artifact=part,
                )
            )
        elif self.is_redis_enabled():
            assert self.redis_connection_string is not None
            self._redis_art = RedisArtifactBenchStore(self.redis_connection_string)
            self._redis_art.save(self.APP, "u0", self.SESSION, self.FILENAME, payload)
        else:
            raise RuntimeError("no backend connection string configured")

    def between_benchmarks(self) -> None:
        return None

    def teardown(self) -> None:
        if self._art is not None:
            try:
                run_async(
                    self._art.delete_artifact(
                        app_name=self.APP,
                        user_id="u0",
                        session_id=self.SESSION,
                        filename=self.FILENAME,
                    )
                )
            except Exception:
                pass
            self._art.close()
        if self._redis_art is not None:
            self._redis_art.delete(self.APP, "u0", self.SESSION, self.FILENAME)
            self._redis_art.close()
        self._art = None
        self._redis_art = None

    def aerospike_artifact_save(self) -> None:
        art = self._art
        assert art is not None
        with self._lock:
            n = self._save_seq
            self._save_seq += 1
        payload = filler_text(self._payload_bytes, seed=n).encode()
        part = types.Part(
            inline_data=types.Blob(mime_type="application/json", data=payload)
        )
        run_async(
            art.save_artifact(
                app_name=self.APP,
                user_id="u0",
                session_id=self.SESSION,
                filename=self.FILENAME,
                artifact=part,
            )
        )

    def aerospike_artifact_load(self) -> None:
        art = self._art
        assert art is not None
        run_async(
            art.load_artifact(
                app_name=self.APP,
                user_id="u0",
                session_id=self.SESSION,
                filename=self.FILENAME,
            )
        )

    def aerospike_artifact_list_versions(self) -> None:
        art = self._art
        assert art is not None
        run_async(
            art.list_artifact_versions(
                app_name=self.APP,
                user_id="u0",
                session_id=self.SESSION,
                filename=self.FILENAME,
            )
        )

    def redis_artifact_save(self) -> None:
        store = self._redis_art
        assert store is not None
        with self._lock:
            n = self._save_seq
            self._save_seq += 1
        payload = filler_text(self._payload_bytes, seed=n).encode()
        store.save(self.APP, "u0", self.SESSION, self.FILENAME, payload)

    def redis_artifact_load(self) -> None:
        store = self._redis_art
        assert store is not None
        store.load_latest(self.APP, "u0", self.SESSION, self.FILENAME)

    def redis_artifact_list_versions(self) -> None:
        store = self._redis_art
        assert store is not None
        store.list_versions(self.APP, "u0", self.SESSION, self.FILENAME)
