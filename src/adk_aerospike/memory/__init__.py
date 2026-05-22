"""Aerospike-backed lexical long-term memory for ADK.

Word-overlap matching (same semantics as ADK's ``InMemoryMemoryService``)
executed server-side via Aerospike's list-element secondary index.
No embedder dependency.
"""

from __future__ import annotations

from .service import AerospikeMemoryService

__all__ = ["AerospikeMemoryService"]
