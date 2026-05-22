"""Register ``aerospike://`` URI schemes with ADK's service registry.

ADK v1.23+ exposes ``google.adk.cli.service_registry.get_service_registry()``
with ``register_session_service / register_memory_service / register_artifact_service``
methods. Calling :func:`register` from this module wires our three factories
in by URI scheme so the ``adk`` CLI flags work:

.. code-block:: bash

    adk web --session_db_url=aerospike://localhost:3000/adk \\
            --artifact_storage_uri=aerospike://localhost:3000/adk \\
            --memory_service_uri=aerospike://localhost:3000/adk
"""

from __future__ import annotations

from .artifacts import AerospikeArtifactService
from .memory import AerospikeMemoryService
from .sessions import AerospikeSessionService

_registered = False


def register(*, memory_top_k: int = 10) -> None:
    """Register Aerospike URI handlers with ADK's service registry. Idempotent."""
    global _registered
    if _registered:
        return

    from google.adk.cli.service_registry import get_service_registry

    reg = get_service_registry()
    reg.register_session_service("aerospike", AerospikeSessionService.from_uri)
    reg.register_artifact_service("aerospike", AerospikeArtifactService.from_uri)

    def _memory_factory(uri: str) -> AerospikeMemoryService:
        return AerospikeMemoryService.from_uri(uri, top_k=memory_top_k)

    reg.register_memory_service("aerospike", _memory_factory)

    _registered = True


__all__ = ["register"]
