"""AerospikeSessionService — Session storage backed by Aerospike KV + CDTs.

Overflow-driven segment layout
------------------------------
Event history lives in append-only **segment** records keyed
``<session_pk>:g:00000000``, ``…:g:00000001``, … Each segment holds a single
``events`` bin: a **K_ORDERED Map** whose keys are
``"{ts_micros:020d}:{event_id}"`` and whose values are the inline event dicts.
The session record itself holds only scoped ``state`` plus a ``cur`` pointer to
the segment currently being appended to.

Two properties fall out of the key choice:

- **Idempotent append.** The key is a pure function of the event, so a retried
  ``map_put`` overwrites the same slot — a timed-out-then-retried append can
  never duplicate an event.
- **Cheap reads.** Because the map is key-ordered (and the key sorts
  chronologically), ``map_get_by_index_range(-N, N)`` returns the last N events
  server-side, and ``map_get_by_key_range`` serves ``after_timestamp``.

Rollover (react, don't predict)
-------------------------------
``append_event`` ``map_put``s into segment ``cur``. The *only* overflow signal
is Aerospike's own ``RecordTooBig`` — there is no client-side byte estimate, no
threshold, no flush. On ``RecordTooBig`` the writer bumps ``cur`` with a
``cur == N`` guarded increment (so concurrent rollovers converge on the same
next index) and retries the ``map_put`` against the new segment. Segments thus
pack to ~``max-record-size`` naturally, one uniform record shape.

Performance: 1-RTT hot path
---------------------------
An append with no state delta is a single ``operate`` on the segment record. An
append that also carries state is one ``batch_write`` coalescing the segment
``map_put`` with the session/app/user ``state`` writes — one RTT regardless of
how many scopes are touched (faster than the previous up-to-three sequential
operates). ``batch_write`` reports per-record results, so a ``RecordTooBig`` on
the segment surfaces while the sibling state writes still commit; rollover then
re-puts only the event. ``cur`` is cached in-process so the hot path never reads
it; a stale cache self-heals via ``RecordTooBig``.

Back-pressure resilience
------------------------
Because every hot-path write is idempotent (stable-key ``map_put``, guarded
``cur`` increment, ``map_put_items`` state), transient back-pressure is retried
with bounded jittered backoff: ``DeviceOverload`` (server write queue full —
rejected, never applied) always, and ambiguous ``TimeoutError`` wherever a
re-apply is a no-op. ``create_session`` retries ``DeviceOverload`` only, since a
``POLICY_EXISTS_CREATE`` put is not timeout-idempotent.

Crash safety
------------
A crash mid-append leaves a segment either updated or not (single-record atomic
op); the stable key makes a re-append a no-op overwrite. A crash after a ``cur``
bump but before the new segment exists is fine — the next ``map_put`` creates
it, and readers treat a missing top segment as empty. There is no seal step and
thus no orphan/"valid iff" invariant to maintain.

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
    event_from_inline_dict,
    event_map_key,
    event_to_inline_dict,
)
from .._internal.indexes import ensure_session_indexes
from .._internal.keys import (
    app_state_key,
    segment_key,
    session_key,
    session_manifest_key,
    user_state_key,
)
from .._internal.schema import (
    Bins,
    Schema,
    StateScope,
)

# Upper-bound sentinel for map_get_by_key_range: every event key begins with a
# digit (0x30-0x39), so ":" (0x3A) sorts strictly after all of them and means
# "to the end of the map". (A None upper bound returns an empty range.)
_KEY_RANGE_END: str = ":"

# Bounded backoff for transient write back-pressure. ``DeviceOverload`` (server
# write queue full) is always safe to retry — the write was rejected, not
# applied. ``TimeoutError`` is ambiguous, but retrying is still safe wherever the
# write is idempotent (the segment ``map_put`` keyed by event id+ts, the
# guarded ``cur`` increment, ``map_put_items`` state) — a re-apply is a no-op.
# Worst-case added latency before giving up ≈ 10+20+40+80+160+320+640+1280+2560+5120 ≈ 10s.
_OVERLOAD_MAX_RETRIES: int = 10
_OVERLOAD_BASE_DELAY: float = 0.01
_OVERLOAD_MAX_DELAY: float = 0.2

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
    ) -> None:
        self._client = client
        self._schema = Schema(namespace=namespace, set_prefix=set_prefix)
        # In-process cache of each session's current (append-target) segment
        # index, keyed by session PK string. Seeded on create/get; re-read on a
        # RecordTooBig rollover. Lets the hot path skip reading ``cur``. A stale
        # entry (another process rolled over) self-heals via RecordTooBig.
        self._cur_cache: dict[str, int] = {}
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
            Bins.LAST_UPDATE: now,
            Bins.CUR_SEGMENT: 0,
        }

        # retry_timeout=False: a POLICY_EXISTS_CREATE put is not idempotent under
        # an ambiguous timeout (a re-apply would raise RecordExistsError). A
        # DeviceOverload means it was rejected, not applied, so that is retried.
        await self._write_retrying_overload(
            self._client.put,
            session_pk,
            session_bins,
            None,
            {"exists": aerospike.POLICY_EXISTS_CREATE},
            retry_timeout=False,
        )
        self._cur_cache[session_key(app_name, user_id, session_id)] = 0

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

        cur = int(session_rec.get(Bins.CUR_SEGMENT, 0))
        self._cur_cache[session_key(app_name, user_id, session_id)] = cur
        events = await self._load_events(app_name, user_id, session_id, cur, config)

        # last_update_time tracks the newest event's timestamp (matching
        # DatabaseSessionService, which writes update_time on every append).
        # events[-1] is the chronologically-newest (segment maps are key-ordered
        # by ts). Fall back to the session record ts when there are no events.
        last_update = session_rec.get(Bins.LAST_UPDATE, 0.0)
        if events:
            last_update = events[-1].timestamp or last_update

        return Session(
            id=session_id,
            app_name=app_name,
            user_id=user_id,
            state=merged,
            events=events,
            last_update_time=last_update,
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

        cur = int(bins.get(Bins.CUR_SEGMENT, 0))
        # Delete segments 0..cur, plus a speculative cur+1 to reclaim a segment
        # left by an interrupted rollover (cur bumped before the new map_put).
        for gidx in range(cur + 2):
            segment_pk = self._segment_pk(app_name, user_id, session_id, gidx)
            try:
                await asyncio.to_thread(self._client.remove, segment_pk)
            except ae.RecordNotFound:
                pass

        try:
            await asyncio.to_thread(self._client.remove, session_pk)
        except ae.RecordNotFound:
            pass

        self._cur_cache.pop(session_key(app_name, user_id, session_id), None)
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
        ev_key = event_map_key(event.id, now)

        # State delta: route app/user/session parts independently.
        delta = dict(event.actions.state_delta or {})
        session_delta, app_delta, user_delta = _partition_state(delta)

        await self._append_event_record(
            app_name,
            user_id,
            session_id,
            ev_key,
            event_dict,
            session_delta,
            app_delta,
            user_delta,
            now,
        )

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

    def _segment_pk(
        self, app_name: str, user_id: str, session_id: str, gidx: int
    ) -> tuple[str, str, str]:
        return (
            self._schema.namespace,
            self._schema.sessions_set,
            segment_key(app_name, user_id, session_id, gidx),
        )

    async def _current_segment(
        self, app_name: str, user_id: str, session_id: str
    ) -> int:
        """Current (append-target) segment index — cached, else read from the
        session record (and cached). Defaults to 0 if the record is unreadable."""
        from aerospike import exception as ae

        pk_str = session_key(app_name, user_id, session_id)
        cached = self._cur_cache.get(pk_str)
        if cached is not None:
            return cached
        try:
            _, _, bins = await asyncio.to_thread(
                self._client.select,
                self._session_pk(app_name, user_id, session_id),
                [Bins.CUR_SEGMENT],
            )
            cur = int(bins.get(Bins.CUR_SEGMENT, 0))
        except ae.RecordNotFound:
            cur = 0
        self._cur_cache[pk_str] = cur
        return cur

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

    async def _write_retrying_overload(
        self, fn: Any, *args: Any, retry_timeout: bool = True
    ) -> Any:
        """Run a write, retrying transient back-pressure with bounded backoff.

        Retries ``DeviceOverload`` and ``MaxErrorRateExceeded`` always (the write
        was rejected, not applied) and ``TimeoutError`` only when ``retry_timeout`` is set (the caller
        asserts the operation is idempotent, so a possibly-applied write is safe
        to repeat). All other Aerospike errors propagate immediately.
        """
        import random

        from aerospike import exception as ae

        delay = _OVERLOAD_BASE_DELAY
        for attempt in range(_OVERLOAD_MAX_RETRIES + 1):
            try:
                return await asyncio.to_thread(fn, *args)
            except (ae.DeviceOverload, ae.MaxErrorRateExceeded):
                if attempt == _OVERLOAD_MAX_RETRIES:
                    raise
            except ae.TimeoutError:
                if not retry_timeout or attempt == _OVERLOAD_MAX_RETRIES:
                    raise
            await asyncio.sleep(delay + random.uniform(0, delay))
            delay = min(delay * 2, _OVERLOAD_MAX_DELAY)
        raise RuntimeError("unreachable")

    async def _append_event_record(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        ev_key: str,
        event_dict: dict[str, Any],
        session_delta: dict[str, Any],
        app_delta: dict[str, Any],
        user_delta: dict[str, Any],
        now: float,
    ) -> None:
        """Persist one event (+ any state delta) in a single round trip.

        No state delta → one ``operate`` (``map_put``) on the current segment.
        Any state delta → one ``batch_write`` coalescing the segment ``map_put``
        with the session/app/user ``state`` writes. ``RecordTooBig`` on the
        segment (raised by ``operate``, or reported per-record by the batch)
        triggers a guarded rollover and a re-put of just the event.
        """
        import aerospike
        from aerospike_helpers.operations import map_operations

        cur = await self._current_segment(app_name, user_id, session_id)
        mpol = {"map_order": aerospike.MAP_KEY_ORDERED}
        seg_op = map_operations.map_put(
            Bins.EVENTS, ev_key, event_dict, map_policy=mpol
        )

        if not (session_delta or app_delta or user_delta):
            await self._place_event(app_name, user_id, session_id, cur, seg_op)
            return

        from aerospike_helpers.batch.records import BatchRecords, Write
        from aerospike_helpers.operations import operations as ops_

        seg_pk = self._segment_pk(app_name, user_id, session_id, cur)
        writes = [Write(seg_pk, [seg_op])]
        if session_delta:
            writes.append(
                Write(
                    self._session_pk(app_name, user_id, session_id),
                    [
                        map_operations.map_put_items(Bins.STATE, session_delta),
                        ops_.write(Bins.LAST_UPDATE, now),
                    ],
                )
            )
        if app_delta:
            writes.append(
                Write(
                    (
                        self._schema.namespace,
                        self._schema.app_state_set,
                        app_state_key(app_name),
                    ),
                    [map_operations.map_put_items(Bins.STATE, app_delta)],
                )
            )
        if user_delta:
            writes.append(
                Write(
                    (
                        self._schema.namespace,
                        self._schema.user_state_set,
                        user_state_key(app_name, user_id),
                    ),
                    [map_operations.map_put_items(Bins.STATE, user_delta)],
                )
            )

        # All ops in the batch are idempotent (segment map_put on a stable key;
        # map_put_items state), so retrying the whole batch on transient
        # back-pressure is safe.
        results = await self._batch_write_with_retry(writes)

        seg_result = results[0]
        # Sibling state writes are independent records; a non-zero there is a
        # genuine error worth surfacing (the segment may have succeeded).
        for r in results[1:]:
            if r != 0:
                raise _batch_error(r)

        if seg_result == 0:
            self._cur_cache[session_key(app_name, user_id, session_id)] = cur
            return
        if seg_result == aerospike.exception.RecordTooBig().code:
            # State already committed; roll over and place just the event.
            await self._place_event(
                app_name, user_id, session_id, cur, seg_op, after_overflow=True
            )
            return
        raise _batch_error(seg_result)

    async def _batch_write_with_retry(self, writes: list[Any]) -> list[int]:
        """``batch_write`` with bounded retry on transient back-pressure.

        Retries the whole batch on a global ``DeviceOverload``/``TimeoutError``
        *or* a per-record ``DeviceOverload`` status (server write queue full).
        Returns the per-record result codes; the caller interprets them
        (``RecordTooBig`` on the segment, etc.). Safe because every op in the
        batch is idempotent.
        """
        import random

        from aerospike import exception as ae
        from aerospike_helpers.batch.records import BatchRecords

        overload = ae.DeviceOverload().code
        delay = _OVERLOAD_BASE_DELAY
        for attempt in range(_OVERLOAD_MAX_RETRIES + 1):
            batch = BatchRecords(writes)
            try:
                await asyncio.to_thread(self._client.batch_write, batch)
            except (ae.DeviceOverload, ae.MaxErrorRateExceeded, ae.TimeoutError):
                if attempt == _OVERLOAD_MAX_RETRIES:
                    raise
            else:
                results = [r.result for r in batch.batch_records]
                if not any(r == overload for r in results):
                    return results
                if attempt == _OVERLOAD_MAX_RETRIES:
                    return results
            await asyncio.sleep(delay + random.uniform(0, delay))
            delay = min(delay * 2, _OVERLOAD_MAX_DELAY)
        raise RuntimeError("unreachable")

    async def _place_event(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        cur: int,
        seg_op: Any,
        *,
        after_overflow: bool = False,
    ) -> None:
        """``map_put`` the event into segment ``cur``, rolling over on overflow.

        ``RecordTooBig`` against a *non-empty* segment means it is full → bump
        ``cur`` (guarded) and retry. ``RecordTooBig`` against an *empty* segment
        means the event alone exceeds ``max-record-size`` — unstorable; re-raise.
        """
        from aerospike import exception as ae

        if after_overflow:
            cur = await self._roll_over(app_name, user_id, session_id, cur)

        max_attempts = 64
        for _ in range(max_attempts):
            seg_pk = self._segment_pk(app_name, user_id, session_id, cur)
            try:
                # Idempotent map_put → safe to retry transient back-pressure.
                await self._write_retrying_overload(
                    self._client.operate, seg_pk, [seg_op]
                )
            except ae.RecordTooBig:
                if await self._segment_size(seg_pk) == 0:
                    raise
                cur = await self._roll_over(app_name, user_id, session_id, cur)
                continue
            self._cur_cache[session_key(app_name, user_id, session_id)] = cur
            return
        raise RuntimeError("append did not converge after rollover retries")

    async def _segment_size(self, seg_pk: tuple[str, str, str]) -> int:
        from aerospike import exception as ae
        from aerospike_helpers.operations import map_operations

        try:
            _, _, res = await asyncio.to_thread(
                self._client.operate, seg_pk, [map_operations.map_size(Bins.EVENTS)]
            )
        except ae.RecordNotFound:
            return 0
        return int(res.get(Bins.EVENTS, 0))

    async def _roll_over(
        self, app_name: str, user_id: str, session_id: str, n: int
    ) -> int:
        """Advance ``cur`` from ``n`` to ``n+1`` with a ``cur == n`` guard.

        The guard makes concurrent rollovers idempotent: only the first writer
        to observe ``cur == n`` performs the bump; the rest get ``FilteredOut``
        and read back the already-advanced value. All callers converge on the
        same next index, so no segment is skipped or double-claimed.
        """
        from aerospike import exception as ae
        from aerospike_helpers.expressions import Eq
        from aerospike_helpers.expressions import IntBin as _IntBin
        from aerospike_helpers.operations import operations as ops_

        session_pk = self._session_pk(app_name, user_id, session_id)
        guard = Eq(_IntBin(Bins.CUR_SEGMENT), n).compile()
        try:
            # Retry-safe under back-pressure: a timed-out-then-applied increment
            # makes the retry see cur == n+1, which fails the cur == n guard
            # (FilteredOut) and reads the already-advanced value below.
            _, _, res = await self._write_retrying_overload(
                self._client.operate,
                session_pk,
                [ops_.increment(Bins.CUR_SEGMENT, 1), ops_.read(Bins.CUR_SEGMENT)],
                None,
                {"expressions": guard},
            )
            new_cur = int(res.get(Bins.CUR_SEGMENT, n + 1))
        except ae.FilteredOut:
            _, _, bins = await asyncio.to_thread(
                self._client.select, session_pk, [Bins.CUR_SEGMENT]
            )
            new_cur = int(bins.get(Bins.CUR_SEGMENT, n + 1))
        self._cur_cache[session_key(app_name, user_id, session_id)] = new_cur
        return new_cur

    async def _load_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        cur: int,
        config: GetSessionConfig | None,
    ) -> list[Event]:
        """Load events from segments ``cur..0`` honouring ``config`` filters.

        Walk segments newest→oldest. Each segment read is server-side: a
        ``map_get_by_index_range(-need, need)`` for ``num_recent_events`` (so we
        never transfer more than the needed tail of a segment), or a
        ``map_get_by_key_range`` from the ``after_timestamp`` cutoff. Stop as
        soon as ``num_recent_events`` is satisfied or history is exhausted.
        """
        num_recent = config.num_recent_events if config else None
        after_ts = config.after_timestamp if config else None

        if num_recent == 0:
            return []

        collected: list[dict[str, Any]] = []  # newest → oldest

        for gidx in range(cur, -1, -1):
            need = (
                (num_recent - len(collected)) if num_recent is not None else None
            )
            entries = await self._read_segment_entries(
                app_name, user_id, session_id, gidx, need, after_ts
            )
            done = False
            for _key, ev in reversed(entries):  # entries are ascending by key
                collected.append(ev)
                if num_recent is not None and len(collected) >= num_recent:
                    done = True
                    break
            if done:
                break
            # after_timestamp: once a whole segment came back empty under the
            # cutoff, every older segment is older still — stop walking.
            if after_ts is not None and not entries:
                break

        collected.reverse()
        return [event_from_inline_dict(d) for d in collected]

    async def _read_segment_entries(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        gidx: int,
        want_last_n: int | None,
        after_ts: float | None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Return ``(key, event_dict)`` pairs from one segment, ascending by key.

        Server-side scoped: ``after_timestamp`` uses ``map_get_by_key_range``;
        otherwise ``map_get_by_index_range`` fetches the last ``want_last_n``
        (or the whole segment when ``want_last_n`` is None).
        """
        import aerospike
        from aerospike import exception as ae
        from aerospike_helpers.operations import map_operations

        seg_pk = self._segment_pk(app_name, user_id, session_id, gidx)
        if after_ts is not None:
            lo = f"{int(after_ts * 1_000_000):020d}:"
            op = map_operations.map_get_by_key_range(
                Bins.EVENTS, lo, _KEY_RANGE_END, aerospike.MAP_RETURN_KEY_VALUE
            )
        elif want_last_n is not None:
            op = map_operations.map_get_by_index_range(
                Bins.EVENTS, -want_last_n, want_last_n,
                aerospike.MAP_RETURN_KEY_VALUE,
            )
        else:
            op = map_operations.map_get_by_index_range(
                Bins.EVENTS, 0, 2**31 - 1, aerospike.MAP_RETURN_KEY_VALUE
            )
        try:
            _, _, res = await asyncio.to_thread(self._client.operate, seg_pk, [op])
        except ae.RecordNotFound:
            return []
        flat = res.get(Bins.EVENTS) or []
        return list(zip(flat[0::2], flat[1::2], strict=True))

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


def _batch_error(result_code: int) -> Exception:
    """Wrap a non-zero ``batch_write`` per-record status as an exception.

    ``RecordTooBig`` is mapped to its specific type (callers may branch on it);
    anything else surfaces as a generic ``AerospikeError`` carrying the code.
    """
    from aerospike import exception as ae

    if result_code == ae.RecordTooBig().code:
        return ae.RecordTooBig()
    err = ae.AerospikeError(f"batch_write sub-operation failed (code {result_code})")
    err.code = result_code
    return err


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
