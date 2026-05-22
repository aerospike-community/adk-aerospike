"""AerospikeMemoryService — lexical long-term memory in core Aerospike.

Design
------
Mirrors the semantics of ADK's reference ``InMemoryMemoryService``: tokenize
text into lowercase ``[A-Za-z]+`` words, return memory entries whose word set
intersects the query's. No embeddings, no embedder dependency, no AI/ML
surface area.

The lexical matching runs **server-side** via Aerospike's list-element
secondary index — the canonical Aerospike pattern for keyword/tag search.
On write, text is tokenized in Python and stored as a ``keywords: list[str]``
bin. On search, the query is tokenized and each token fires an indexed
``predicates.contains(keywords, INDEX_TYPE_LIST, token)`` query in parallel;
results are unioned client-side, deduplicated, and ranked by token-overlap
count.

References
----------
- Aerospike 3.8 release notes (feature debut): the list-element predicate is
  the headline example.
- "Query JSON Documents Faster With New CDT Indexing" (Aerospike blog).
- discuss.aerospike.com "Full text research queries": Aerospike staff
  recommend list-bin + secondary index for tag/keyword search; recommend the
  Elasticsearch connector for true full-text needs.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import TYPE_CHECKING, Any, Self

from google.adk.memory import BaseMemoryService
from google.adk.memory.base_memory_service import SearchMemoryResponse
from google.adk.memory.memory_entry import MemoryEntry

from .._internal.client import close_client, make_client
from .._internal.codec import extract_event_text
from .._internal.indexes import ensure_memory_indexes
from .._internal.keys import memory_key
from .._internal.schema import Bins, Schema
from .._internal.uri import parse as parse_uri

if TYPE_CHECKING:
    import aerospike
    from google.adk.events import Event
    from google.adk.sessions import Session

log = logging.getLogger(__name__)

_WORD_RE = re.compile(r"[A-Za-z]+")


class AerospikeMemoryService(BaseMemoryService):
    """Lexical long-term memory in core Aerospike.

    Word-overlap matching (same as ``InMemoryMemoryService``) executed
    server-side via Aerospike's list-element secondary index.
    """

    def __init__(
        self,
        client: aerospike.Client,
        namespace: str,
        *,
        set_prefix: str = "adk_",
        top_k: int = 10,
        ensure_indexes: bool = True,
    ) -> None:
        self._client = client
        self._schema = Schema(namespace=namespace, set_prefix=set_prefix)
        self._top_k = top_k
        if ensure_indexes:
            ensure_memory_indexes(client, self._schema)

    @classmethod
    def from_uri(cls, uri: str, *, top_k: int = 10) -> Self:
        parsed = parse_uri(uri)
        if parsed.scheme != "aerospike":
            raise ValueError(
                f"AerospikeMemoryService requires aerospike:// scheme, got {parsed.scheme!r}"
            )
        client = make_client(parsed)
        return cls(client, parsed.namespace, set_prefix=parsed.set_prefix, top_k=top_k)

    def close(self) -> None:
        close_client(self._client)

    # ---- BaseMemoryService -----------------------------------------------------

    async def add_session_to_memory(self, session: Session) -> None:
        # Replace prior memories for this session (matches InMemoryMemoryService
        # semantics: re-adding the same session overwrites).
        await self._purge_session_memories(
            session.app_name, session.user_id, session.id
        )
        for event in session.events:
            if not event.content or not event.content.parts:
                continue
            text = extract_event_text(event)
            if not text:
                continue
            await self._upsert_memory(
                app_name=session.app_name,
                user_id=session.user_id,
                session_id=session.id,
                event=event,
                text=text,
            )

    async def search_memory(
        self,
        *,
        app_name: str,
        user_id: str,
        query: str,
    ) -> SearchMemoryResponse:
        query_tokens = _tokenize(query)
        if not query_tokens:
            return SearchMemoryResponse(memories=[])

        # Fan out one indexed contains-query per token. Aerospike's list-element
        # index returns all records whose keywords list contains the token.
        # Parallel via to_thread + gather.
        per_token_results = await asyncio.gather(
            *(
                asyncio.to_thread(self._run_token_query, token)
                for token in query_tokens
            )
        )

        # Union, filter by scope, score by token-overlap count, tie-break by ts desc.
        scored: dict[str, tuple[int, float, dict[str, Any]]] = {}
        query_token_set = set(query_tokens)
        for token_results in per_token_results:
            for bins in token_results:
                if bins.get(Bins.APP_NAME) != app_name:
                    continue
                if bins.get(Bins.USER_ID) != user_id:
                    continue
                eid = bins.get(Bins.EVENT_ID, "")
                if not eid or eid in scored:
                    continue
                overlap = len(
                    set(bins.get(Bins.KEYWORDS) or []) & query_token_set
                )
                ts = float(bins.get(Bins.TIMESTAMP) or 0.0)
                scored[eid] = (overlap, ts, bins)

        ranked = sorted(
            scored.values(), key=lambda t: (t[0], t[1]), reverse=True
        )
        return SearchMemoryResponse(
            memories=[_memory_entry_from_bins(b) for _, _, b in ranked[: self._top_k]]
        )

    # ---- internals -------------------------------------------------------------

    async def _upsert_memory(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
        event: Event,
        text: str,
    ) -> None:
        keywords = _tokenize(text)
        bins = {
            Bins.APP_NAME: app_name,
            Bins.USER_ID: user_id,
            Bins.SESSION_ID: session_id,
            Bins.EVENT_ID: event.id,
            Bins.TEXT: text,
            Bins.KEYWORDS: keywords,
            Bins.AUTHOR: event.author,
            Bins.TIMESTAMP: float(event.timestamp or 0.0),
            Bins.CONTENT: event.content.model_dump(mode="json")
            if event.content
            else None,
        }
        pk = (
            self._schema.namespace,
            self._schema.memory_set,
            memory_key(app_name, user_id, session_id, event.id),
        )
        await asyncio.to_thread(self._client.put, pk, bins)

    def _run_token_query(self, token: str) -> list[dict[str, Any]]:
        """Sync indexed query: all records whose ``keywords`` list contains ``token``.

        Designed to be called via ``asyncio.to_thread`` so multiple tokens'
        queries run in parallel.
        """
        import aerospike
        from aerospike import predicates

        q = self._client.query(self._schema.namespace, self._schema.memory_set)
        q.where(predicates.contains(Bins.KEYWORDS, aerospike.INDEX_TYPE_LIST, token))
        records = q.results()
        return [bins for _, _, bins in records]

    async def _purge_session_memories(
        self, app_name: str, user_id: str, session_id: str
    ) -> None:
        from aerospike import exception as ae
        from aerospike import predicates

        query = self._client.query(self._schema.namespace, self._schema.memory_set)
        query.where(predicates.equals(Bins.USER_ID, user_id))
        records = await asyncio.to_thread(query.results)
        for _, _, bins in records:
            if (
                bins.get(Bins.APP_NAME) == app_name
                and bins.get(Bins.SESSION_ID) == session_id
            ):
                pk = (
                    self._schema.namespace,
                    self._schema.memory_set,
                    memory_key(app_name, user_id, session_id, bins[Bins.EVENT_ID]),
                )
                try:
                    await asyncio.to_thread(self._client.remove, pk)
                except ae.RecordNotFound:
                    pass


def _tokenize(text: str) -> list[str]:
    """Lowercase ``[A-Za-z]+`` tokenization matching ``InMemoryMemoryService``.

    Dedupes — the list-element secondary index only needs unique values per
    record, and dedup keeps the per-record list size bounded.
    """
    if not text:
        return []
    return list({m.group(0).lower() for m in _WORD_RE.finditer(text)})


def _memory_entry_from_bins(bins: dict[str, Any]) -> MemoryEntry:
    from google.adk.memory import _utils
    from google.genai import types as genai_types

    content_bin = bins.get(Bins.CONTENT)
    if content_bin:
        content = genai_types.Content.model_validate(content_bin)
    else:
        content = genai_types.Content(
            parts=[genai_types.Part(text=bins.get(Bins.TEXT, ""))]
        )

    ts = bins.get(Bins.TIMESTAMP)
    return MemoryEntry(
        id=bins.get(Bins.EVENT_ID),
        content=content,
        author=bins.get(Bins.AUTHOR),
        timestamp=_utils.format_timestamp(ts) if ts else None,
    )
