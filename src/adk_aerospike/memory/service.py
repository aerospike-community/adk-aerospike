"""AerospikeMemoryService — lexical long-term memory in core Aerospike.

Design
------
Mirrors the semantics of ADK's reference ``InMemoryMemoryService``: tokenize
text into lowercase ``[A-Za-z]+`` words, return memory entries whose word set
intersects the query's. No embeddings, no embedder dependency, no AI/ML
surface area.

Search uses an **inverted index of primary keys**, not secondary indexes:
each ``(app_name, user_id, token)`` is a row ``app:user:kw:<token>`` with a
list of ``{eid, sid, ts}`` refs. A query is ``batch_read`` on those posting
rows (one PK per query token), then ``batch_read`` on the matching memory
rows. Load is partitioned per user, not global per token.

Purge (``add_session_to_memory`` replace semantics) still uses the ``aus``
secondary index to find prior memory rows for a session — a cold path.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, Self

from google.adk.memory import BaseMemoryService
from google.adk.memory.base_memory_service import SearchMemoryResponse
from google.adk.memory.memory_entry import MemoryEntry

from .._internal.client import close_client, make_client
from .._internal.codec import extract_event_text
from .._internal.indexes import ensure_memory_indexes
from .._internal.keys import memory_key, memory_posting_key, scope_tuple
from .._internal.schema import Bins, Schema
from .._internal.uri import parse as parse_uri

if TYPE_CHECKING:
    import aerospike
    from google.adk.events import Event
    from google.adk.sessions import Session

log = logging.getLogger(__name__)

_UNKNOWN_SESSION_ID: Final = "__unknown_session_id__"

_WORD_RE = re.compile(r"[A-Za-z]+")

# Ref map keys inside each posting-list element (short wire names).
_REF_EID: Final = "eid"
_REF_SID: Final = "sid"
_REF_TS: Final = "ts"

# Cap posting-list growth per token; trim oldest entries (front of list).
_MAX_POSTING_LIST_SIZE: Final = 2048
# Cap union size before loading full memory rows for scoring.
_MAX_SEARCH_CANDIDATES: Final = 512

_STOPWORDS: Final = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "by",
        "for",
        "from",
        "in",
        "is",
        "it",
        "of",
        "on",
        "or",
        "that",
        "the",
        "to",
        "was",
        "were",
        "with",
    }
)


class AerospikeMemoryService(BaseMemoryService):
    """Lexical long-term memory in core Aerospike.

    Word-overlap matching (same as ``InMemoryMemoryService``) via per-user
    posting-list primary keys — no list-element secondary index on search.
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

    async def add_events_to_memory(
        self,
        *,
        app_name: str,
        user_id: str,
        events: Sequence[Event],
        session_id: str | None = None,
        custom_metadata: Mapping[str, object] | None = None,
    ) -> None:
        _ = custom_metadata
        scoped_session_id = session_id or _UNKNOWN_SESSION_ID
        for event in events:
            if not event.content or not event.content.parts:
                continue
            text = extract_event_text(event)
            if not text:
                continue
            pk = (
                self._schema.namespace,
                self._schema.memory_set,
                memory_key(app_name, user_id, scoped_session_id, event.id),
            )
            if (await self._batch_read([pk])).get(pk):
                continue
            await self._upsert_memory(
                app_name=app_name,
                user_id=user_id,
                session_id=scoped_session_id,
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

        posting_pks = [
            self._posting_pk(app_name, user_id, token) for token in query_tokens
        ]
        posting_rows = await self._batch_read(posting_pks)

        candidates: dict[str, tuple[str, float]] = {}
        for token, pk in zip(query_tokens, posting_pks, strict=True):
            bins = posting_rows.get(pk)
            if not bins:
                continue
            for ref in bins.get(Bins.MEM_POSTINGS) or []:
                if not isinstance(ref, dict):
                    continue
                eid = ref.get(_REF_EID, "")
                sid = ref.get(_REF_SID, "")
                if not eid or not sid:
                    continue
                ts = float(ref.get(_REF_TS) or 0.0)
                prev = candidates.get(eid)
                if prev is None or ts > prev[1]:
                    candidates[eid] = (sid, ts)
                if len(candidates) >= _MAX_SEARCH_CANDIDATES:
                    break
            if len(candidates) >= _MAX_SEARCH_CANDIDATES:
                break

        if not candidates:
            return SearchMemoryResponse(memories=[])

        memory_pks = [
            (
                self._schema.namespace,
                self._schema.memory_set,
                memory_key(app_name, user_id, sid, eid),
            )
            for eid, (sid, _ts) in candidates.items()
        ]
        memory_rows = await self._batch_read(memory_pks)

        query_token_set = set(query_tokens)
        scored: list[tuple[int, float, dict[str, Any]]] = []
        for eid, (sid, ts) in candidates.items():
            pk = (
                self._schema.namespace,
                self._schema.memory_set,
                memory_key(app_name, user_id, sid, eid),
            )
            bins = memory_rows.get(pk)
            if not bins:
                continue
            if bins.get(Bins.APP_NAME) != app_name:
                continue
            if bins.get(Bins.USER_ID) != user_id:
                continue
            overlap = len(set(bins.get(Bins.KEYWORDS) or []) & query_token_set)
            if overlap == 0:
                continue
            scored.append((overlap, ts, bins))

        scored.sort(key=lambda t: (t[0], t[1]), reverse=True)
        return SearchMemoryResponse(
            memories=[_memory_entry_from_bins(b) for _, _, b in scored[: self._top_k]]
        )

    # ---- internals -------------------------------------------------------------

    def _posting_pk(
        self, app_name: str, user_id: str, token: str
    ) -> tuple[str, str, str]:
        return (
            self._schema.namespace,
            self._schema.memory_set,
            memory_posting_key(app_name, user_id, token),
        )

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
        ts = float(event.timestamp or 0.0)
        bins = {
            Bins.APP_NAME: app_name,
            Bins.USER_ID: user_id,
            Bins.SESSION_ID: session_id,
            Bins.SCOPE_TUPLE: scope_tuple(app_name, user_id, session_id),
            Bins.EVENT_ID: event.id,
            Bins.TEXT: text,
            Bins.KEYWORDS: keywords,
            Bins.AUTHOR: event.author,
            Bins.TIMESTAMP: ts,
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

        ref = {_REF_EID: event.id, _REF_SID: session_id, _REF_TS: ts}
        for token in keywords:
            await self._append_posting_ref(app_name, user_id, token, ref)

    async def _append_posting_ref(
        self,
        app_name: str,
        user_id: str,
        token: str,
        ref: dict[str, Any],
    ) -> None:
        from aerospike_helpers.operations import list_operations, operations as ops_

        pk = self._posting_pk(app_name, user_id, token)
        ops: list[Any] = [
            list_operations.list_append(Bins.MEM_POSTINGS, ref),
            ops_.read(Bins.MEM_POSTINGS),
        ]
        _, _, result = await asyncio.to_thread(self._client.operate, pk, ops)
        postings = result.get(Bins.MEM_POSTINGS) or []
        if len(postings) <= _MAX_POSTING_LIST_SIZE:
            return

        import aerospike

        trim_count = len(postings) - _MAX_POSTING_LIST_SIZE
        await asyncio.to_thread(
            self._client.operate,
            pk,
            [
                list_operations.list_remove_by_index_range(
                    Bins.MEM_POSTINGS,
                    0,
                    aerospike.LIST_RETURN_NONE,
                    trim_count,
                )
            ],
        )

    async def _remove_posting_refs(
        self,
        app_name: str,
        user_id: str,
        keywords: list[str],
        session_id: str,
        event_id: str,
        ts: float,
    ) -> None:
        import aerospike
        from aerospike import exception as ae
        from aerospike_helpers.operations import list_operations

        ref = {_REF_EID: event_id, _REF_SID: session_id, _REF_TS: ts}
        for token in keywords:
            pk = self._posting_pk(app_name, user_id, token)
            try:
                await asyncio.to_thread(
                    self._client.operate,
                    pk,
                    [
                        list_operations.list_remove_by_value(
                            Bins.MEM_POSTINGS,
                            ref,
                            aerospike.LIST_RETURN_NONE,
                        )
                    ],
                )
            except ae.RecordNotFound:
                pass

    async def _purge_session_memories(
        self, app_name: str, user_id: str, session_id: str
    ) -> None:
        from aerospike import exception as ae
        from aerospike import predicates

        query = self._client.query(self._schema.namespace, self._schema.memory_set)
        query.where(
            predicates.equals(
                Bins.SCOPE_TUPLE, scope_tuple(app_name, user_id, session_id)
            )
        )
        records = await asyncio.to_thread(query.results)
        for _, _, bins in records:
            eid = bins.get(Bins.EVENT_ID, "")
            keywords = list(bins.get(Bins.KEYWORDS) or [])
            ts = float(bins.get(Bins.TIMESTAMP) or 0.0)
            if keywords and eid:
                await self._remove_posting_refs(
                    app_name, user_id, keywords, session_id, eid, ts
                )
            pk = (
                self._schema.namespace,
                self._schema.memory_set,
                memory_key(app_name, user_id, session_id, eid),
            )
            try:
                await asyncio.to_thread(self._client.remove, pk)
            except ae.RecordNotFound:
                pass

    async def _batch_read(
        self, keys: list[tuple[str, str, str]]
    ) -> dict[tuple[str, str, str], dict[str, Any] | None]:
        if not keys:
            return {}
        result = await asyncio.to_thread(self._client.batch_read, keys)
        out: dict[tuple[str, str, str], dict[str, Any] | None] = {}
        for key, br in zip(keys, result.batch_records, strict=True):
            if br.result == 0 and br.record is not None:
                _, _, bins = br.record
                out[key] = bins
            else:
                out[key] = None
        return out


def _tokenize(text: str) -> list[str]:
    """Lowercase ``[A-Za-z]+`` tokens; stopwords and dedupe excluded from index."""
    if not text:
        return []
    return list(
        {
            m.group(0).lower()
            for m in _WORD_RE.finditer(text)
            if m.group(0).lower() not in _STOPWORDS
        }
    )


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
