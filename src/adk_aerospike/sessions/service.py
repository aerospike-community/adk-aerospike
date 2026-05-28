"""AerospikeSessionService — Session storage backed by Aerospike KV + CDTs.

Chunked storage layout
----------------------
All event history lives **inline on the session record** in an ``events`` List
CDT bin (the "hot tail"). When the tail exceeds a byte threshold (default 256
KiB), it is sealed atomically into a sibling chunk record keyed
``<session_pk>:c:00000000`` and the tail is reset. Reads concatenate sealed
chunks (in cidx order) with the live tail; only the chunks needed to satisfy
``GetSessionConfig`` are fetched, using server-side ``list_get_by_index_range``
pagination to avoid pulling whole chunks for ``num_recent_events``.

This shape pays back Aerospike's per-record overhead (~64 B PI entry + ~40 B
record header) by ensuring high-cardinality records are KB-to-MB-scale, not
hundreds of bytes. It also makes ``append_event`` a single-record server-side
atomic ``operate()`` in the common case — no MRT needed.

Atomicity & crash safety
------------------------
Fast-path append: one ``operate()`` on the session record. Naturally atomic.

Flush: write chunk record (overwriting any orphan from a prior interrupted
flush), then generation-checked ``operate()`` to clear the tail and bump the
``chunks`` counter. If the gen check fails (another writer raced us), we leave
our chunk write in place as an orphan — readers ignore it (invariant: chunk
``c:N`` is valid iff ``session.chunks > N``); the next successful flush
overwrites it. No data is ever lost because the tail still holds the events
until the gen-checked reset succeeds.

State scoping
-------------
ADK's session state mixes three scopes via key prefixes:

- ``app:foo``   → ``app_state`` set, shared across all users of an app
- ``user:foo``  → ``user_state`` set, shared across a user's sessions
- ``temp:foo``  → never persisted (in-process only)
- *(unprefixed)* → session-scoped, stored on the session record's ``state`` bin
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import TYPE_CHECKING, Any, Self

from google.adk.sessions import BaseSessionService, Session

from .._internal.client import close_client, make_client
from .._internal.codec import (
    estimate_event_size,
    event_from_inline_dict,
    event_to_inline_dict,
)
from .._internal.indexes import ensure_session_indexes
from .._internal.keys import (
    app_state_key,
    chunk_key,
    session_key,
    session_manifest_key,
    user_state_key,
)
from .._internal.schema import (
    DEFAULT_FLUSH_THRESHOLD_BYTES,
    DEFAULT_HUGE_EVENT_BYTES,
    Bins,
    Schema,
    StateScope,
)

# list_sessions metadata only — skip events/state (can be MiB per session).
_LIST_SESSION_BINS: tuple[str, ...] = (
    Bins.APP_NAME,
    Bins.USER_ID,
    Bins.SESSION_ID,
    Bins.LAST_UPDATE,
)
from .._internal.uri import parse as parse_uri

if TYPE_CHECKING:
    import aerospike
    from google.adk.events import Event
    from google.adk.sessions.base_session_service import (
        GetSessionConfig,
        ListSessionsResponse,
    )

log = logging.getLogger(__name__)


class AerospikeSessionService(BaseSessionService):
    """Session / event / state storage on Aerospike Database."""

    def __init__(
        self,
        client: aerospike.Client,
        namespace: str,
        *,
        set_prefix: str = "adk_",
        ensure_indexes: bool = True,
        flush_threshold_bytes: int = DEFAULT_FLUSH_THRESHOLD_BYTES,
        huge_event_bytes: int = DEFAULT_HUGE_EVENT_BYTES,
    ) -> None:
        self._client = client
        self._schema = Schema(namespace=namespace, set_prefix=set_prefix)
        self._flush_threshold = flush_threshold_bytes
        self._huge_event_bytes = huge_event_bytes
        if ensure_indexes:
            ensure_session_indexes(client, self._schema)

    @classmethod
    def from_uri(cls, uri: str) -> Self:
        parsed = parse_uri(uri)
        client = make_client(parsed)
        return cls(client, parsed.namespace, set_prefix=parsed.set_prefix)

    def close(self) -> None:
        close_client(self._client)

    # ---- BaseSessionService ----------------------------------------------------

    async def create_session(
        self,
        *,
        app_name: str,
        user_id: str,
        state: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> Session:
        import aerospike

        session_id = session_id or uuid.uuid4().hex
        session_state, app_delta, user_delta = _partition_state(state or {})
        now = time.time()

        session_pk = self._session_pk(app_name, user_id, session_id)
        session_bins = {
            Bins.APP_NAME: app_name,
            Bins.USER_ID: user_id,
            Bins.SESSION_ID: session_id,
            Bins.STATE: session_state,
            Bins.EVENTS: [],
            Bins.LAST_UPDATE: now,
            Bins.EVENT_SEQ: 0,
            Bins.CHUNKS: 0,
            Bins.TAIL_BYTES: 0,
        }

        await asyncio.to_thread(
            self._client.put,
            session_pk,
            session_bins,
            None,
            {"exists": aerospike.POLICY_EXISTS_CREATE},
        )

        if app_delta:
            await self._merge_scoped_state(
                self._schema.app_state_set,
                app_state_key(app_name),
                app_delta,
            )
        if user_delta:
            await self._merge_scoped_state(
                self._schema.user_state_set,
                user_state_key(app_name, user_id),
                user_delta,
            )

        await self._manifest_add(app_name, user_id, session_id)

        merged = await self._merge_state_for_read(app_name, user_id, session_state)
        log.debug("Created session %s for app=%s user=%s", session_id, app_name, user_id)
        return Session(
            id=session_id,
            app_name=app_name,
            user_id=user_id,
            state=merged,
            events=[],
            last_update_time=now,
        )

    async def get_session(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
        config: GetSessionConfig | None = None,
    ) -> Session | None:
        # Fetch session record + app-state + user-state in a single round trip
        # via batch_read. Three records, one network RTT — same latency as a
        # naïve single GET, while preserving the 1:N normalised layout
        # (avoids fan-out write amplification on app/user state updates).
        session_pk = self._session_pk(app_name, user_id, session_id)
        app_pk = (
            self._schema.namespace,
            self._schema.app_state_set,
            app_state_key(app_name),
        )
        user_pk = (
            self._schema.namespace,
            self._schema.user_state_set,
            user_state_key(app_name, user_id),
        )
        records = await self._batch_read([session_pk, app_pk, user_pk])

        session_rec = records[session_pk]
        if session_rec is None:
            return None

        session_state = session_rec.get(Bins.STATE) or {}
        app_state = (records[app_pk] or {}).get(Bins.STATE) or {}
        user_state = (records[user_pk] or {}).get(Bins.STATE) or {}
        merged = _merge_state(app_state, user_state, session_state)
        events = await self._load_events(
            app_name, user_id, session_id, session_rec, config
        )

        return Session(
            id=session_id,
            app_name=app_name,
            user_id=user_id,
            state=merged,
            events=events,
            last_update_time=session_rec.get(Bins.LAST_UPDATE, 0.0),
        )

    async def list_sessions(
        self,
        *,
        app_name: str,
        user_id: str | None = None,
    ) -> ListSessionsResponse:
        from google.adk.sessions.base_session_service import ListSessionsResponse

        if user_id is not None:
            return ListSessionsResponse(
                sessions=await self._list_sessions_for_user(app_name, user_id)
            )

        from aerospike import predicates

        query = self._client.query(self._schema.namespace, self._schema.sessions_set)
        query.where(predicates.equals(Bins.APP_NAME, app_name))
        records = await asyncio.to_thread(query.results)
        sessions: list[Session] = []
        for _, _, bins in records:
            if bins.get(Bins.APP_NAME) != app_name:
                continue
            sessions.append(
                Session(
                    id=bins[Bins.SESSION_ID],
                    app_name=bins[Bins.APP_NAME],
                    user_id=bins[Bins.USER_ID],
                    state={},
                    events=[],
                    last_update_time=bins.get(Bins.LAST_UPDATE, 0.0),
                )
            )
        return ListSessionsResponse(sessions=sessions)

    async def delete_session(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
    ) -> None:
        from aerospike import exception as ae

        session_pk = self._session_pk(app_name, user_id, session_id)
        try:
            _, _, bins = await asyncio.to_thread(self._client.get, session_pk)
        except ae.RecordNotFound:
            return

        n_chunks = int(bins.get(Bins.CHUNKS, 0))
        # Delete sealed chunks first. Also speculatively remove cidx == n_chunks
        # (covers orphans from an interrupted flush — see _flush_tail).
        for cidx in range(n_chunks + 1):
            chunk_pk = self._chunk_pk(app_name, user_id, session_id, cidx)
            try:
                await asyncio.to_thread(self._client.remove, chunk_pk)
            except ae.RecordNotFound:
                pass

        try:
            await asyncio.to_thread(self._client.remove, session_pk)
        except ae.RecordNotFound:
            pass

        await self._manifest_remove(app_name, user_id, session_id)

    async def append_event(self, session: Session, event: Event) -> Event:
        # Base class: apply temp state in-memory, trim temp from delta, append
        # event to session.events. We persist what remains.
        event = await super().append_event(session, event)
        if event.partial:
            return event

        app_name = session.app_name
        user_id = session.user_id
        session_id = session.id
        now = event.timestamp or time.time()
        event_dict = event_to_inline_dict(event)
        est_size = estimate_event_size(event_dict)

        # State delta: route app/user/session parts independently.
        delta = dict(event.actions.state_delta or {})
        session_delta, app_delta, user_delta = _partition_state(delta)

        if app_delta:
            await self._merge_scoped_state(
                self._schema.app_state_set, app_state_key(app_name), app_delta
            )
        if user_delta:
            await self._merge_scoped_state(
                self._schema.user_state_set,
                user_state_key(app_name, user_id),
                user_delta,
            )

        # Huge-event pre-flush: don't fold a >900 KiB event into an existing
        # tail; seal the current tail first so this event lives alone.
        if est_size >= self._huge_event_bytes:
            await self._flush_tail(app_name, user_id, session_id)

        new_tbytes = await self._append_to_tail(
            app_name, user_id, session_id, event_dict, est_size, session_delta, now
        )

        # Post-flush if tail crossed threshold (or after a huge-event append).
        if new_tbytes >= self._flush_threshold:
            await self._flush_tail(app_name, user_id, session_id)

        session.last_update_time = now
        return event

    # ---- internals -------------------------------------------------------------

    def _session_pk(
        self, app_name: str, user_id: str, session_id: str
    ) -> tuple[str, str, str]:
        return (
            self._schema.namespace,
            self._schema.sessions_set,
            session_key(app_name, user_id, session_id),
        )

    def _chunk_pk(
        self, app_name: str, user_id: str, session_id: str, cidx: int
    ) -> tuple[str, str, str]:
        return (
            self._schema.namespace,
            self._schema.sessions_set,
            chunk_key(app_name, user_id, session_id, cidx),
        )

    def _manifest_pk(self, app_name: str, user_id: str) -> tuple[str, str, str]:
        return (
            self._schema.namespace,
            self._schema.sessions_set,
            session_manifest_key(app_name, user_id),
        )

    async def _manifest_add(
        self, app_name: str, user_id: str, session_id: str
    ) -> None:
        from aerospike_helpers.operations import list_operations

        await asyncio.to_thread(
            self._client.operate,
            self._manifest_pk(app_name, user_id),
            [list_operations.list_append(Bins.SESSION_MANIFEST, session_id)],
        )

    async def _manifest_remove(
        self, app_name: str, user_id: str, session_id: str
    ) -> None:
        import aerospike
        from aerospike import exception as ae
        from aerospike_helpers.operations import list_operations

        try:
            await asyncio.to_thread(
                self._client.operate,
                self._manifest_pk(app_name, user_id),
                [
                    list_operations.list_remove_by_value(
                        Bins.SESSION_MANIFEST,
                        session_id,
                        aerospike.LIST_RETURN_NONE,
                    )
                ],
            )
        except ae.RecordNotFound:
            pass

    async def _list_sessions_for_user(
        self, app_name: str, user_id: str
    ) -> list[Session]:
        from aerospike import exception as ae

        try:
            _, _, manifest_bins = await asyncio.to_thread(
                self._client.get,
                self._manifest_pk(app_name, user_id),
            )
        except ae.RecordNotFound:
            return []

        session_ids: list[str] = manifest_bins.get(Bins.SESSION_MANIFEST) or []
        if not session_ids:
            return []

        session_pks = [
            self._session_pk(app_name, user_id, sid) for sid in session_ids
        ]
        rows = await self._batch_read_bins(session_pks, _LIST_SESSION_BINS)
        sessions: list[Session] = []
        stale_ids: list[str] = []
        for sid, pk in zip(session_ids, session_pks, strict=True):
            bins = rows.get(pk)
            if not bins:
                stale_ids.append(sid)
                continue
            if bins.get(Bins.APP_NAME) != app_name:
                stale_ids.append(sid)
                continue
            if bins.get(Bins.USER_ID) != user_id:
                stale_ids.append(sid)
                continue
            sessions.append(
                Session(
                    id=bins.get(Bins.SESSION_ID, sid),
                    app_name=app_name,
                    user_id=user_id,
                    state={},
                    events=[],
                    last_update_time=bins.get(Bins.LAST_UPDATE, 0.0),
                )
            )
        if stale_ids:
            await self._manifest_remove_stale(app_name, user_id, stale_ids)
        return sessions

    async def _manifest_remove_stale(
        self, app_name: str, user_id: str, session_ids: list[str]
    ) -> None:
        for session_id in session_ids:
            await self._manifest_remove(app_name, user_id, session_id)

    async def _append_to_tail(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        event_dict: dict[str, Any],
        est_size: int,
        session_delta: dict[str, Any],
        now: float,
    ) -> int:
        """Single-RTT atomic append to the tail. Returns the new ``tbytes``."""
        from aerospike_helpers.operations import (
            list_operations,
            map_operations,
            operations as ops_,
        )

        session_pk = self._session_pk(app_name, user_id, session_id)
        ops: list[Any] = []
        if session_delta:
            ops.append(map_operations.map_put_items(Bins.STATE, session_delta))
        ops.extend(
            [
                list_operations.list_append(Bins.EVENTS, event_dict),
                ops_.increment(Bins.EVENT_SEQ, 1),
                ops_.increment(Bins.TAIL_BYTES, est_size),
                ops_.write(Bins.LAST_UPDATE, now),
                ops_.read(Bins.TAIL_BYTES),
            ]
        )
        _, _, result = await asyncio.to_thread(
            self._client.operate, session_pk, ops
        )
        return int(result.get(Bins.TAIL_BYTES, 0))

    async def _flush_tail(
        self, app_name: str, user_id: str, session_id: str
    ) -> None:
        """Seal the current tail as a chunk record; reset the tail.

        Idempotent under concurrent flush attempts and crashes. Invariant: a
        chunk record at ``cidx == N`` is *valid* only when ``session.chunks > N``;
        any chunk written without a matching session-record reset is an orphan
        that the next successful flush overwrites. No data is lost because the
        tail still holds the events until the gen-checked reset commits.
        """
        import aerospike
        from aerospike import exception as ae
        from aerospike_helpers.operations import (
            list_operations,
            operations as ops_,
        )

        session_pk = self._session_pk(app_name, user_id, session_id)
        try:
            _, meta, bins = await asyncio.to_thread(self._client.get, session_pk)
        except ae.RecordNotFound:
            return

        tail_events: list[dict[str, Any]] = bins.get(Bins.EVENTS) or []
        if not tail_events:
            return

        cidx = int(bins.get(Bins.CHUNKS, 0))
        gen = meta["gen"]

        ts_lo = float(tail_events[0].get("ts", 0.0))
        ts_hi = float(tail_events[-1].get("ts", 0.0))

        chunk_pk = self._chunk_pk(app_name, user_id, session_id, cidx)
        chunk_bins = {
            Bins.CHUNK_IDX: cidx,
            Bins.EVENTS: tail_events,
            Bins.TS_LO: ts_lo,
            Bins.TS_HI: ts_hi,
        }
        # Plain PUT — intentionally upsert, not POLICY_EXISTS_CREATE. This is
        # the asymmetry that makes the flush invariant work: session create
        # rejects duplicates (POLICY_EXISTS_CREATE), but chunk PUT must
        # overwrite any orphan left by a prior interrupted flush so the next
        # flush can claim the same cidx cleanly. Readers ignore chunks at
        # cidx >= session.chunks, so an orphan never appears in history; the
        # overwrite simply reclaims its slot.
        await asyncio.to_thread(self._client.put, chunk_pk, chunk_bins)

        # Reset session tail atomically with generation check. If another
        # writer modified the session record between our GET and this op,
        # bail — we'll leave our chunk as an orphan; the next flush replaces
        # it. The tail still has these events, so no loss.
        ops = [
            list_operations.list_clear(Bins.EVENTS),
            ops_.increment(Bins.CHUNKS, 1),
            ops_.write(Bins.TAIL_BYTES, 0),
        ]
        try:
            await asyncio.to_thread(
                self._client.operate,
                session_pk,
                ops,
                {"gen": gen},
                {"gen": aerospike.POLICY_GEN_EQ},
            )
        except ae.RecordGenerationError:
            log.debug(
                "Flush gen-check failed for session %s; orphan chunk c:%d left for next flush",
                session_id,
                cidx,
            )

    async def _load_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        session_bins: dict[str, Any],
        config: GetSessionConfig | None,
    ) -> list[Event]:
        """Load events from the tail + chunks honouring ``config`` filters.

        Fast path: if the tail already satisfies ``num_recent_events``, no
        chunk reads. Otherwise we walk chunks from newest to oldest and
        server-side-paginate via ``list_get_by_index_range``.
        """
        tail: list[dict[str, Any]] = session_bins.get(Bins.EVENTS) or []
        n_chunks = int(session_bins.get(Bins.CHUNKS, 0))

        num_recent = config.num_recent_events if config else None
        after_ts = config.after_timestamp if config else None

        if num_recent == 0:
            return []

        # Collect events newest→oldest until we have enough (or exhaust history).
        collected: list[dict[str, Any]] = []

        def take(events: list[dict[str, Any]]) -> bool:
            """Append from newest end; return True if we have enough."""
            for ev in reversed(events):
                if after_ts is not None and float(ev.get("ts", 0.0)) < after_ts:
                    continue
                collected.append(ev)
                if num_recent is not None and len(collected) >= num_recent:
                    return True
            return False

        if take(tail) is False and n_chunks > 0:
            need = (num_recent - len(collected)) if num_recent is not None else None
            for cidx in range(n_chunks - 1, -1, -1):
                chunk_events = await self._read_chunk_events(
                    app_name, user_id, session_id, cidx, need, after_ts
                )
                if take(chunk_events):
                    break
                if num_recent is not None:
                    need = num_recent - len(collected)

        collected.reverse()
        return [event_from_inline_dict(d) for d in collected]

    async def _read_chunk_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        cidx: int,
        want_last_n: int | None,
        after_ts: float | None,
    ) -> list[dict[str, Any]]:
        """Read events from a chunk.

        - If ``want_last_n`` is set, fetch only the last N via server-side
          ``list_get_by_index_range`` — avoids transferring an entire 256 KiB
          chunk for a small ``num_recent_events``.
        - If ``after_ts`` is set, prune by ``ts_hi`` first (skip chunk
          entirely if all its events are older than the cutoff).
        """
        import aerospike
        from aerospike import exception as ae
        from aerospike_helpers.operations import list_operations, operations as ops_

        chunk_pk = self._chunk_pk(app_name, user_id, session_id, cidx)

        if after_ts is not None:
            try:
                _, _, head = await asyncio.to_thread(
                    self._client.select, chunk_pk, [Bins.TS_HI]
                )
            except ae.RecordNotFound:
                return []
            if float(head.get(Bins.TS_HI, 0.0)) < after_ts:
                return []

        if want_last_n is not None:
            ops = [
                list_operations.list_get_by_index_range(
                    Bins.EVENTS, -want_last_n,
                    aerospike.LIST_RETURN_VALUE, want_last_n,
                ),
            ]
            try:
                _, _, result = await asyncio.to_thread(
                    self._client.operate, chunk_pk, ops
                )
            except ae.RecordNotFound:
                return []
            return result.get(Bins.EVENTS) or []

        try:
            _, _, bins = await asyncio.to_thread(
                self._client.select, chunk_pk, [Bins.EVENTS]
            )
        except ae.RecordNotFound:
            return []
        return bins.get(Bins.EVENTS) or []

    async def _merge_scoped_state(
        self,
        set_name: str,
        primary_key: str,
        delta: dict[str, Any],
    ) -> None:
        """Upsert ``delta`` into the ``state`` Map bin of ``(set_name, primary_key)``."""
        from aerospike_helpers.operations import map_operations

        full_pk = (self._schema.namespace, set_name, primary_key)
        ops = [map_operations.map_put_items(Bins.STATE, delta)]
        await asyncio.to_thread(self._client.operate, full_pk, ops)

    async def _merge_state_for_read(
        self,
        app_name: str,
        user_id: str,
        session_state: dict[str, Any],
    ) -> dict[str, Any]:
        """Read app/user state and merge with session state.

        Uses ``batch_read`` so the two scoped reads share a single round trip.
        """
        app_pk = (
            self._schema.namespace,
            self._schema.app_state_set,
            app_state_key(app_name),
        )
        user_pk = (
            self._schema.namespace,
            self._schema.user_state_set,
            user_state_key(app_name, user_id),
        )
        records = await self._batch_read([app_pk, user_pk])
        app_state = (records[app_pk] or {}).get(Bins.STATE) or {}
        user_state = (records[user_pk] or {}).get(Bins.STATE) or {}
        return _merge_state(app_state, user_state, session_state)

    async def _batch_read(
        self, keys: list[tuple[str, str, str]]
    ) -> dict[tuple[str, str, str], dict[str, Any] | None]:
        """Fetch multiple records in one RTT; return bins-or-None per input key."""
        if not keys:
            return {}
        result = await asyncio.to_thread(self._client.batch_read, keys)
        # BatchRecord.result == 0 means OK; non-zero (typically 2) means
        # RecordNotFound. Match results back to inputs by digest order —
        # batch_read preserves the input order in BatchRecords.batch_records.
        out: dict[tuple[str, str, str], dict[str, Any] | None] = {}
        for input_key, br in zip(keys, result.batch_records, strict=True):
            if br.result == 0 and br.record is not None:
                _, _, bins = br.record
                out[input_key] = bins
            else:
                out[input_key] = None
        return out

    async def _batch_read_bins(
        self,
        keys: list[tuple[str, str, str]],
        bins: tuple[str, ...],
    ) -> dict[tuple[str, str, str], dict[str, Any] | None]:
        """``batch_write`` with per-bin ``read`` ops — one RTT, no fat bins."""
        if not keys:
            return {}
        from aerospike_helpers.batch.records import BatchRecords, Read
        from aerospike_helpers.operations.operations import read as op_read

        ops = [op_read(name) for name in bins]
        batch = BatchRecords([Read(key, ops) for key in keys])
        result = await asyncio.to_thread(self._client.batch_write, batch)
        out: dict[tuple[str, str, str], dict[str, Any] | None] = {}
        for input_key, br in zip(keys, result.batch_records, strict=True):
            if br.result == 0 and br.record is not None:
                _, _, record_bins = br.record
                out[input_key] = record_bins
            else:
                out[input_key] = None
        return out


def _merge_state(
    app_state: dict[str, Any],
    user_state: dict[str, Any],
    session_state: dict[str, Any],
) -> dict[str, Any]:
    """Compose the merged state dict ADK callers expect.

    Matches ``DatabaseSessionService``: app keys come back as ``app:foo``,
    user keys as ``user:foo``, session keys bare. Session-scoped keys win on
    collision (namespaces are disjoint by prefix, so collisions shouldn't
    happen in practice).
    """
    merged: dict[str, Any] = {}
    for k, v in app_state.items():
        merged[f"{StateScope.APP}{k}"] = v
    for k, v in user_state.items():
        merged[f"{StateScope.USER}{k}"] = v
    merged.update(session_state)
    return merged


def _partition_state(
    state: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Split a state dict by prefix into (session, app_delta, user_delta).

    Mirrors ``google.adk.sessions._session_util.extract_state_delta``. ``temp:*``
    keys are dropped (in-process only).
    """
    session_state: dict[str, Any] = {}
    app_delta: dict[str, Any] = {}
    user_delta: dict[str, Any] = {}
    for k, v in state.items():
        if k.startswith(StateScope.TEMP):
            continue
        elif k.startswith(StateScope.APP):
            app_delta[k.removeprefix(StateScope.APP)] = v
        elif k.startswith(StateScope.USER):
            user_delta[k.removeprefix(StateScope.USER)] = v
        else:
            session_state[k] = v
    return session_state, app_delta, user_delta
