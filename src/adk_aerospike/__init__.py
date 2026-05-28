"""Aerospike storage backends for Google ADK.

Public API
----------
- :class:`AerospikeSessionService`  — implements ``BaseSessionService``
- :class:`AerospikeArtifactService` — implements ``BaseArtifactService``
- :class:`AerospikeMemoryService`   — implements ``BaseMemoryService`` (lexical word-overlap; no embedder)
- :func:`register`                  — register ``aerospike://`` URI schemes with ADK's CLI

Typical use::

    from adk_aerospike import AerospikeSessionService

    svc = AerospikeSessionService.from_uri("aerospike://localhost:3000/adk")
"""

from __future__ import annotations

from .artifacts import AerospikeArtifactService
from .memory import AerospikeMemoryService
from .registry import register
from .sessions import AerospikeSessionService

__all__ = [
    "AerospikeArtifactService",
    "AerospikeMemoryService",
    "AerospikeSessionService",
    "register",
]

__version__ = "0.2.0"
