# Design proposal: overflow-driven session segments

**Status:** proposal / RFC — design decisions resolved (§12), not yet implemented.
**Supersedes:** the `tbytes` / `flush_threshold` / `huge_event_bytes` / size-gate
machinery in `sessions/service.py` (the "hot tail + chunk" model).
**Author context:** written after diagnosing `RecordTooBig` + data loss under
heavy concurrent appends. The size-gate fix made the current model *correct*,
but the model itself predicts overflow client-side instead of reacting to the
server's real limit. This proposal reshapes it.

---

## 1. What's wrong with the current model

The session record carries a "hot tail" `events` List plus a running byte
estimate `tbytes`. We proactively seal the tail into chunk records when the
estimate crosses a soft threshold, and we gate appends with a filter expression
so concurrent writers can't overshoot the 1 MiB `max-record-size`.

The smells:

1. **We guess at bytes.** `estimate_event_size()` is literally `len(str(d))`.
   Every threshold (`flush_threshold_bytes` 256 KiB, `huge_event_bytes` 900 KiB,
   `max_tail_bytes` 768 KiB) is a fudge factor around a guess, padded for
   estimator error. Records under-fill (we flush at ~256 KiB to stay safe below
   1 MiB), so we make ~4× more records than necessary.
2. **Two record shapes in one set** (mutable "session record with inline tail"
   vs immutable "chunk"), distinguished by key shape and bin presence. Readers,
   `delete_session`, and `list_sessions` all special-case the duality.
3. **Append is not idempotent.** `list_append` duplicates on retry, so we lean
   on single-shot writes (`max_retries=0`) and careful flush guards instead of
   safe retries.
4. **Flush is a dance:** chunk PUT → guarded reset, plus a per-session lock and
   a "skip tiny tail" rule, all to keep concurrent flushers from corrupting the
   tail. Necessary only because the tail is a mutable shared List.

## 2. Goals

- **G1 — React, don't predict.** Let a record fill to its real capacity; the
  server's `RecordTooBig` is the rollover signal. No byte estimation, no
  thresholds.
- **G2 — Idempotent appends.** A retried `append_event` for the same event must
  never create a second copy. This makes retries (and read-retries) safe and
  collapses most of the concurrency/crash reasoning.
- **G3 — Cheap last-N reads.** `get_session(num_recent_events=N)` must fetch
  only the last N events server-side, not read+sort whole segments.
- **G4 — One uniform record shape** for events. No tail/chunk duality.
- **G5 — Concurrency-safe and crash-safe** without MRTs on the hot path.

Non-goals: cross-process *event ordering* guarantees beyond client timestamps
(unchanged from today); external blob storage for >1 MiB single events (future
work, noted in §9).

## 3. Core insight — a K_ORDERED Map keyed by `"<ts>:<eid>"` gives G2 **and** G3

The earlier framing posed List (ordered, cheap last-N, *not* idempotent) vs Map
(idempotent, *not* cheap last-N) as a tradeoff. It isn't one. An Aerospike
**key-ordered Map** (`MAP_KEY_ORDERED`) keyed by a composite string

```
key   = f"{ts_micros:020d}:{event_id}"     # sortable prefix + unique suffix
value = <event dict>
```

satisfies both because the key is simultaneously:

- **Stable** — derived from the event's own `timestamp` and `id`, so a retry
  produces the *same* key → `map_put` overwrites the same slot → **idempotent**
  (G2). No counter read, no extra round trip.
- **Chronologically sortable** — the zero-padded `ts` prefix orders entries by
  time, so `map_get_by_index_range(events, -N, N)` returns the **last N
  server-side** (G3), and `map_get_by_key_range(events, lo, hi)` does
  `after_timestamp` server-side.
- **Unique** — the `event_id` suffix breaks `ts` ties, so two events in the same
  microsecond don't collide (no silent overwrite/loss).

### Verified against Aerospike 8.0 / client 19.2.1

```python
mpol = {'map_order': aerospike.MAP_KEY_ORDERED}
for ts, eid in [(100,'a'),(105,'b'),(102,'c'),(100,'d')]:
    c.operate(pk, [map_ops.map_put('ev', f'{ts:020d}:{eid}', {...}, map_policy=mpol)])
c.operate(pk, [map_ops.map_put('ev', f'{105:020d}:b', {...}, map_policy=mpol)])  # re-write

len(map)                                   # -> 4   (idempotent: no dup)
map_get_by_index_range('ev', -2, 2)        # -> [102:c, 105:b]  (chronological last-N)
map_get_by_key_range('ev', '..103:', '~')  # -> [105:b]         (server-side after_ts)
```

`ts_micros` = `int(event.timestamp * 1e6)` (20 digits covers epoch-micros well
past year 9999). If ADK timestamps prove too coarse under bursts, the `eid`
suffix still guarantees uniqueness; only the *relative* order of same-microsecond
events is arbitrary (and stable across reads).

> **The key must be a pure function of the event.** This is non-negotiable for
> idempotency — a retry has to recompute the *identical* key. That rules out any
> assigned sequence number (server- or client-side): a counter would yield a new
> value on retry → a new key → a duplicate. Hence ts+eid, not a `seq`. (Resolved:
> O3 = micros, O4 = drop `seq`; see §12.)

## 4. Record layout

Two record kinds, but now they're cleanly separated by role (not by "is the tail
still hot"):

**Session record** — `app:user:session` (metadata + state only; small, mutable):

| bin     | type | meaning |
|---------|------|---------|
| `app`, `uid`, `sid` | str | denormalised for `list_sessions` sec-index |
| `state` | Map  | session-scoped state (unchanged) |
| `cur`   | int  | current (highest) segment index — the append target |
| `ts`    | float| `last_update_time` |

**Segment record** — `app:user:session:g:NNNNNNNN` (events; append-only,
fills to ~1 MiB then sealed by rollover):

| bin     | type | meaning |
|---------|------|---------|
| `ev`    | Map (K_ORDERED) | `{"<ts>:<eid>": event_dict}` |
| `gidx`  | int  | segment index (discriminator; segments have it, session record doesn't) |

A segment's min/max event timestamps are **not** stored as bins — they are the
first/last keys of the K_ORDERED `ev` map (`map_get_by_index(ev, 0)` / `(ev, -1)`),
always accurate and free to read alongside a last-N fetch (§13).

(Key infix `g:` parallels today's `c:`; pick `g` for "segment" to avoid
confusion during migration. App-state / user-state / manifest records are
unchanged.)

Segments deliberately omit `app/uid/sid`, so — exactly as chunks do today — they
don't appear in the `idx_sess_*` secondary indexes and `list_sessions` returns
only session records.

> **Resolved (O1): always-separate segments.** Uniform record shape (G4) wins;
> short sessions pay one extra read, accepted.

## 5. Append algorithm

```
append_event(session, event):
    super().append_event(...)            # ADK temp-state handling (unchanged)
    route app/user state deltas          # unchanged (separate records)
    apply session-state delta to session record

    key = f"{int(event.timestamp*1e6):020d}:{event.id}"
    n   = cached cur_seg for this session (lazily read from session record)
    loop:
        try:
            operate(segment(n), [
                map_put('ev', key, event_dict, K_ORDERED),
                map_get_by_key_range ts_lo/ts_hi maintenance (or write min/max),
            ])
            return                        # success — idempotent on retry
        except RecordTooBig:
            n = roll_over(n)              # see §6; advances cur_seg
            continue                      # retry append on the fresh segment
```

No `tbytes`, no `flush_threshold`, no proactive flush, no size-gate filter.
The hot path is one `operate()` on one segment record. Overflow is rare (once
per ~1 MiB of events) and its cost (one failed op + a rollover) amortizes over a
whole segment.

## 6. Rollover, idempotency, and concurrency correctness

`roll_over(n)` advances the current segment, tolerating concurrent rollers:

```
roll_over(n):
    operate(session_rec, [increment('cur', 1), read('cur')],
            filter = (cur == n))         # guard: only one writer bumps n -> n+1
    # FilteredOut => someone else already rolled; re-read cur
    cur = (result if committed) else read(session_rec.cur)
    create segment(cur) if not exists    # POLICY_EXISTS_CREATE, ignore RecordExists
    return cur
```

Why this is correct:

- **`RecordTooBig` is a *definitive* non-apply.** `operate` is atomic; on
  `RecordTooBig` nothing was written, so the event is *not* in segment `n`.
  Writing it to `n+1` therefore cannot duplicate it across segments. (Contrast:
  a *timeout* is ambiguous — handled below.)
- **Concurrent rollovers converge.** The `cur == n` filter means exactly one
  writer wins the `n → n+1` bump; losers get `FilteredOut`, re-read `cur`, and
  all land on the same new segment. `POLICY_EXISTS_CREATE` makes segment
  creation a no-op for everyone but the first.
- **Idempotency within a segment (timeouts/retries).** If an append times out
  (ambiguous) we retry the *same* segment with the *same* key → `map_put`
  overwrites the same slot. No duplicate. This is why we only roll on
  `RecordTooBig`, never on timeout.
- **Belt-and-suspenders:** reads dedup by `eid` across the (≤2) boundary
  segments anyway (§7), so even a pathological cross-segment dup is invisible.

This replaces *all* of: the generation/`chunks==C` guard, the per-session flush
lock, the "skip tiny tail" rule, and the size-gate — with one guarded counter
bump and idempotent `map_put`.

## 7. Read algorithms

`cur` drives a newest→oldest walk over segments, same shape as today but with
cheaper per-segment ops:

- **last-N** (`num_recent_events=N`): read `map_get_by_index_range('ev', -N, N)`
  on segment `cur`; if it yields < N, continue to `cur-1`, etc. Each op returns
  only the needed entries — never a whole segment.
- **after_timestamp**: within each segment use
  `map_get_by_key_range('ev', f"{after_micros:020d}:", None)` (server-side); skip
  older segments cheaply by checking the segment's last key
  (`map_get_by_index(ev, -1)`) against the cutoff.
- **full history**: read each segment's `ev` map in `gidx` order (maps return
  key-ordered, so already chronological); concatenate. Dedup by `eid`.
- Reconstruct `Event` via the existing `event_from_inline_dict` codec
  (the value is still the same inline event dict).

## 8. Crash safety

- A crash mid-append leaves the segment either updated or not (atomic op); the
  stable key means a re-append is a no-op overwrite.
- A crash after a successful `cur` bump but before the new segment is created is
  fine: the next append's `POLICY_EXISTS_CREATE` creates it; readers treat a
  missing top segment as empty.
- No "valid iff chunks > N" invariant needed — there is no separate seal step.
  A segment is whatever it contains; the only mutable pointer is `cur`, advanced
  by an idempotent guarded increment.

## 9. Edge cases

- **Single event > max-record-size:** appending to a *freshly created, empty*
  segment still `RecordTooBig` ⇒ the event genuinely cannot be stored inline.
  Raise a clear error (today's behavior), or — future work — spill the oversized
  event to the artifact/object store and store a reference. Detect via "rolled
  to a new segment and it was empty and still too big."
- **Clock skew / non-monotonic `event.timestamp`:** ordering is by client ts;
  same as today. The `eid` suffix preserves uniqueness regardless.
- **`delete_session`:** delete segments `0..cur` (plus a speculative `cur+1` for
  an interrupted rollover) then the session record + manifest entry. Simpler
  than today (no orphan-chunk speculation tied to a seal step).

## 10. What gets deleted

- `schema.DEFAULT_FLUSH_THRESHOLD_BYTES`, `DEFAULT_HUGE_EVENT_BYTES`,
  `DEFAULT_MAX_TAIL_BYTES`; bins `tbytes`, `chunks` (replaced by `cur`), and
  `seq` (dropped — O4).
- `codec.estimate_event_size` (no longer needed).
- `_append_to_tail` size-gate, `_flush_until_below_threshold`, `_flush_tail`,
  `_select_flush_batch`, `_flush_lock` / `_flush_locks`.
- Constructor params `flush_threshold_bytes`, `huge_event_bytes`,
  `max_tail_bytes`.

Net: `sessions/service.py` loses the entire flush subsystem; append/read get
shorter and the model gets one fewer concept ("tail").

## 11. Migration / compatibility

**Resolved (O2): clean break + version bump.** The package is published, so the
change ships as a new release with a `CHANGELOG` entry calling out the
incompatible on-disk layout (old `c:NNNNNNNN` chunk sessions are not read by the
new code). No read-compat shim, no migrator. Bump the version accordingly (minor
or major per the project's pre-1.0 convention — see RELEASING.md).

## 12. Decisions (resolved)

- **O1 — always-separate segments.** No inline segment-0 on the session record;
  uniform record shape. Short sessions pay one extra read.
- **O2 — clean break + version bump.** Ships as a new published release with a
  `CHANGELOG` note; old chunk-format sessions are not read. No shim, no migrator.
- **O3 — key = `"{int(event.timestamp*1e6):020d}:{event_id}"`.** Micros from the
  event timestamp, `eid` suffix for uniqueness/tie-break. A monotonic counter is
  rejected: it would break idempotency (the key must be a pure function of the
  event — see §3).
- **O4 — drop `seq`.** Ordering no longer needs it; maintaining it would add a
  hot-path write to the session record and be inaccurate under idempotent
  retries. If a count is ever needed, derive it lazily as the sum of segment
  `ev` map sizes at read time.
- **O5 — oversized-single-event object-store spill is out of scope.** A lone
  event exceeding `max-record-size` raises a clear error (today's behavior);
  blob-spill is future work.

## 13. Performance — preserving (and beating) the 1-RTT hot path

Hard requirement: the new model must not add round trips to the append hot path.
The events-on-separate-segments split naïvely costs a second RTT (event on the
segment record, session state on the session record), so we coalesce.

**Append RTT budget:**

| case | today | new |
|------|-------|-----|
| event, no state delta | 1 `operate` (session rec) | 1 `operate` (segment) |
| event + session-state delta | 1 `operate` | 1 `batch_write` (segment + session rec) |
| event + app/user/session deltas | **3 sequential `operate`s** | **1 `batch_write`** (all records) |
| segment overflow (≈ once per 1 MiB) | flush dance | 1 `batch_write` + 1 `operate` retry |

So the common path stays 1 RTT and the multi-scope path *improves* from 3
sequential operates to one `batch_write`.

**Coalescing via `batch_write`.** When an append carries any state delta, issue a
single `batch_write` whose records are: the segment (`map_put` the event), the
session record (`map_put_items` session-scoped state), and — if present — the
app- and user-state records. `batch_write` is one round trip and reports results
**per record**. Verified on 8.0/19.2.1: a `RecordTooBig` on the segment record
surfaces as that record's `result == 13` while the sibling state writes still
commit (`result == 0`) and the batch does **not** raise. So overflow handling is:
inspect the segment record's result; on 13, roll over (§6) and re-`map_put` just
the event to the new segment (state already committed — idempotent either way).

**No per-append session-record write for `ts`/ordering.** `last_update_time` and
`after_timestamp` pruning derive from the K_ORDERED map's boundary keys
(`map_get_by_index(ev, -1)` / `(ev, 0)`) — which a normal last-N read already
returns — so a no-state-delta append touches *only* its segment record. The
session record's `ts` is still set in the batch when a state delta is present
(free, the batch already hits it); `get_session` returns
`max(session_rec.ts, newest_event_ts)`.

**`cur` segment cache.** The append target (`cur`) lives on the session record.
It is cached in-process (dict keyed by session PK, seeded at `create_session`/
`get_session`, like the old flush-lock map) so the hot path never reads it. On a
`RecordTooBig` the writer re-reads/bumps `cur` (guarded, §6); a stale cache
(another process rolled over) self-heals via that same `RecordTooBig` path.

**Note — ordering semantics change.** History now orders by `event.timestamp`
(the key), not by insertion. Identical for monotonic timestamps (the normal
case). Two events sharing a microsecond are ordered by `event_id` (arbitrary but
stable). This is recorded in the CHANGELOG.
