"""google-adk-extras Redis helpers for ecosystem benchmarks."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import redis

if TYPE_CHECKING:
    from google_adk_extras.memory.redis_memory_service import RedisMemoryService
    from google_adk_extras.sessions.redis_session_service import RedisSessionService


def _redis_session_module():
    return importlib.import_module("google_adk_extras.sessions.redis_session_service")


def _redis_memory_module():
    return importlib.import_module("google_adk_extras.memory.redis_memory_service")


RedisSessionService = _redis_session_module().RedisSessionService
RedisMemoryService = _redis_memory_module().RedisMemoryService


def parse_redis_uri(uri: str) -> tuple[str, int, int]:
    parsed = urlparse(uri)
    if parsed.scheme not in ("redis", "rediss"):
        raise ValueError(f"expected redis:// URI, got {uri!r}")
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 6379
    path = (parsed.path or "/0").lstrip("/")
    db = int(path.split("/")[0] or "0")
    return host, port, db


def session_service(uri: str) -> RedisSessionService:
    host, port, db = parse_redis_uri(uri)
    cls = _redis_session_module().RedisSessionService
    return cls(host=host, port=port, db=db)


def memory_service(uri: str) -> RedisMemoryService:
    host, port, db = parse_redis_uri(uri)
    cls = _redis_memory_module().RedisMemoryService
    return cls(host=host, port=port, db=db)


def redis_client(uri: str) -> redis.Redis:
    host, port, db = parse_redis_uri(uri)
    return redis.Redis(host=host, port=port, db=db, decode_responses=False)


async def init_session(svc: RedisSessionService) -> None:
    await svc.initialize()


async def close_session(svc: RedisSessionService) -> None:
    await svc.cleanup()


async def init_memory(svc: RedisMemoryService) -> None:
    await svc.initialize()


async def close_memory(svc: RedisMemoryService) -> None:
    await svc.cleanup()


async def purge_user_sessions(svc: RedisSessionService, app: str, user: str) -> None:
    resp = await svc.list_sessions(app_name=app, user_id=user)
    for s in resp.sessions:
        try:
            await svc.delete_session(app_name=app, user_id=user, session_id=s.id)
        except Exception:
            pass


async def purge_user_memory(svc: RedisMemoryService, app: str, user: str) -> None:
    assert svc.client is not None
    svc.client.delete(f"memory:{app}:{user}")


class RedisArtifactBenchStore:
    """Minimal versioned blob store for artifact workload parity."""

    def __init__(self, uri: str, *, key_prefix: str = "bench_art:") -> None:
        self._client = redis_client(uri)
        self._prefix = key_prefix

    def close(self) -> None:
        self._client.close()

    def _meta_key(self, app: str, user: str, session: str, fname: str) -> str:
        return f"{self._prefix}{app}:{user}:{session}:{fname}:meta"

    def _data_key(self, app: str, user: str, session: str, fname: str, ver: int) -> str:
        return f"{self._prefix}{app}:{user}:{session}:{fname}:v:{ver:08d}"

    def save(self, app: str, user: str, session: str, fname: str, payload: bytes) -> int:
        meta_key = self._meta_key(app, user, session, fname)
        ver = int(self._client.incr(meta_key))
        self._client.set(self._data_key(app, user, session, fname, ver), payload)
        return ver

    def load_latest(self, app: str, user: str, session: str, fname: str) -> bytes | None:
        meta_key = self._meta_key(app, user, session, fname)
        ver = self._client.get(meta_key)
        if ver is None:
            return None
        return self._client.get(self._data_key(app, user, session, fname, int(ver)))

    def list_versions(self, app: str, user: str, session: str, fname: str) -> list[int]:
        meta_key = self._meta_key(app, user, session, fname)
        latest = self._client.get(meta_key)
        if latest is None:
            return []
        return list(range(1, int(latest) + 1))

    def delete(self, app: str, user: str, session: str, fname: str) -> None:
        versions = self.list_versions(app, user, session, fname)
        keys = [self._data_key(app, user, session, fname, v) for v in versions]
        keys.append(self._meta_key(app, user, session, fname))
        if keys:
            self._client.delete(*keys)
