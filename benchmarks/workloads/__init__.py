"""ADK Aerospike workloads for ai-ecosystem-benchmark."""

from __future__ import annotations

from typing import Any, Type

from ai_ecosystem_benchmark import BaseBenchmarkWorkload

from .agent_turn import AgentTurnWorkload
from .artifacts import ArtifactsWorkload
from .chunk_stress import ChunkStressWorkload
from .memory_lexical import MemoryLexicalWorkload
from .session_hotpath import SessionHotpathWorkload

WORKLOAD_TYPES: dict[str, Type[BaseBenchmarkWorkload]] = {
    "session_hotpath": SessionHotpathWorkload,
    "memory_lexical": MemoryLexicalWorkload,
    "artifacts": ArtifactsWorkload,
    "agent_turn": AgentTurnWorkload,
    "chunk_stress": ChunkStressWorkload,
}


def build_workload(
    name: str,
    connection_string: str,
    params: dict[str, Any],
    *,
    backend: str = "aerospike",
) -> BaseBenchmarkWorkload:
    try:
        cls = WORKLOAD_TYPES[name]
    except KeyError as exc:
        known = ", ".join(sorted(WORKLOAD_TYPES))
        raise ValueError(f"unknown workload {name!r}; choose from: {known}") from exc
    if backend == "aerospike":
        return cls(aerospike_connection_string=connection_string, **params)
    if backend == "redis":
        return cls(redis_connection_string=connection_string, **params)
    raise ValueError(f"unknown backend {backend!r}; choose from: aerospike, redis")
