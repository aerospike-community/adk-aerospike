# `adk-aerospike` — Design Document

**Audience:** Aerospike DevRel team taking ownership for productionization.
**Status:** alpha (0.0.1), all three ADK storage interfaces implemented end-to-end against ADK 2.0, 39 tests passing.
**Last updated:** 2026-05-22

This document explains *what* we built, *why* we built it this way, and *what
trade-offs* you should know about before defending it to customers or
extending it.

---

## 1. Executive summary

`adk-aerospike` is a Python package that implements Google
[Agent Development Kit](https://adk.dev/)'s three pluggable storage interfaces
on top of a single Aerospike Database cluster:

| ADK interface         | Our class                  | What it stores                                  |
| --------------------- | -------------------------- | ----------------------------------------------- |
| `BaseSessionService`  | `AerospikeSessionService`  | Conversational session state + event history    |
| `BaseArtifactService` | `AerospikeArtifactService` | Versioned binary artifacts (files, blobs)       |
| `BaseMemoryService`   | `AerospikeMemoryService`   | Long-term semantic memory (embeddings + search) |

Plus a `register()` hook that wires `aerospike://` URIs into ADK's CLI so
`adk web --session_db_url=aerospike://localhost:3000/adk` works.

### Why this matters

The ADK third-party storage integration landscape is thin and uneven (full
analysis in §11). The market gaps we fill:

1. **Only package shipping all three interfaces (Session + Artifact + Memory)
   with embedding-based semantic memory backed by a single database.** Redis
   ships Session + Memory but no Artifact and uses an HTTP sidecar. Community
   "extras" packages either skip Memory entirely or implement only keyword
   matching.
2. **No HTTP sidecar.** Native in-process client. Operationally simpler than
   `adk-redis` (which requires an Agent Memory Server on `:8088`).
3. **No vector search dependency.** Memory uses lexical word-overlap
   (same semantics as ADK's reference `InMemoryMemoryService`), executed
   server-side via Aerospike's list-element secondary index. No embeddings,
   no embedder, no AI/ML surface area.
4. **URI-scheme registration** with ADK's `service_registry` — `adk web`
   flags work out of the box. No competitor in the community ecosystem does
   this.
5. **Single-record server-side atomic appends** via Aerospike's CDTs +
   `operate()` — no Multi-Record Transactions (MRTs) needed for the hot path.

---

## 2. Architecture overview

```
                         ┌──────────────────────────┐
                         │   Google ADK Runner      │
                         │  (user's agent code)     │
                         └─────────────┬────────────┘
                                       │
                       ┌───────────────┼───────────────┐
                       │               │               │
              BaseSessionService  BaseArtifactService  BaseMemoryService
                       │               │               │
                       ▼               ▼               ▼
            AerospikeSessionService  ArtifactService  MemoryService
                       │               │               │
                       └───────────────┼───────────────┘
                                       │ asyncio.to_thread()
                                       ▼
                         ┌──────────────────────────┐
                         │  aerospike Python client │
                         │  (sync, C extension)     │
                         └─────────────┬────────────┘
                                       │
                                       ▼
                         ┌──────────────────────────┐
                         │  Aerospike Database      │
                         │  (CE or EE, 7.x or 8.x)  │
                         └──────────────────────────┘
```

### Key architectural choices

- **One Aerospike client per process**, injected into all three services.
  Construction is expensive (DNS, TLS, cluster tend); the client maintains
  internal connection pools.
- **Sync-to-async bridge via `asyncio.to_thread`.** The official aerospike
  Python client is synchronous; ADK interfaces are `async`. We wrap each call
  in `to_thread` so the event loop stays responsive. Default executor sizing
  is fine for moderate concurrency; very high throughput should configure a
  larger executor.
- **All data in one namespace** chosen by the operator. Five sets within it
  (default `adk_` prefix, configurable): `sessions`, `app_state`, `user_state`,
  `artifacts`, `memory`. Multiple ADK installations can share a namespace by
  using distinct prefixes.
- **Secondary indexes auto-created on first connect** — idempotent
  (`IndexFoundError` swallowed), no out-of-band setup step needed.

---

## 3. The ADK session hierarchy (canonical contract)

This is the most important section for DevRel — you'll be explaining this to
customers and writing docs about it. Get this right.

### A `Session` is not one document — it's four logical objects

ADK doesn't return one row per session. It splits the conceptual session into
four logical scopes that compose at read time:

```
                       Session (returned by get_session)
                              │  state dict (merged view of three scopes)
                              │  events list (hydrated from storage)
                              │
  ┌───────────────────────────┼──────────────────────┐
  │                           │                      │
AppState               UserState                SessionState
keyed by               keyed by                 lives on the
(app_name)             (app_name, user_id)      session record
shared by all          shared by one user's     this session only
users of the app       sessions
                              │
                              │   appends produce
                              ▼
                          Event[0..N]
                  one per append, ordered by time
```

**This is Google's design.** The proof is in `DatabaseSessionService`'s
SQLAlchemy schema at
`.venv/lib/python3.11/site-packages/google/adk/sessions/schemas/v1.py`:

```python
class StorageSession(Base):       # v1.py:72-103
  __tablename__ = "sessions"
  app_name, user_id, id          # composite PK
  state: MutableDict             # session-scoped state ONLY
  storage_events: relationship(...)  # 1:N → StorageEvent

class StorageEvent(Base):         # v1.py:164-191
  __tablename__ = "events"
  ...

class StorageAppState(Base):      # v1.py:233-247
  __tablename__ = "app_states"
  app_name (PK), state: MutableDict

class StorageUserState(Base):     # v1.py:249-265
  __tablename__ = "user_states"
  app_name, user_id (composite PK), state: MutableDict
```

Four tables. One per scope. Any backend that wants to behave like
`DatabaseSessionService` must keep these four logical objects.

### State scoping via key prefixes

ADK's `Session.state` is a single Python dict, but key prefixes route entries
to different scopes:

| Prefix in key | Storage location           | Visibility                       |
| ------------- | -------------------------- | -------------------------------- |
| `app:foo`     | `adk_app_state` (per app)  | All users of this app            |
| `user:foo`    | `adk_user_state` (per user) | This user, across their sessions |
| `temp:foo`    | NOT PERSISTED              | In-process, current invocation only |
| (unprefixed)  | On the session record      | This session only                |

**Source of truth:** `google.adk.sessions.state.State` and
`google.adk.sessions._session_util.extract_state_delta`. We reimplement the
same splitter in `sessions/service.py:_partition_state` — keep them
synchronized if ADK ever adds a new prefix.

When ADK calls `get_session`, our service merges all four scopes back into the
single `state` dict the caller expects, re-applying the `app:` / `user:`
prefixes so the shape matches `DatabaseSessionService`.

### The base class does some work for you

`BaseSessionService.append_event` (in
`base_session_service.py:116-167`) is a **concrete method**, not abstract. It:

1. Applies `temp:` keys to the in-memory `session.state` (so subsequent
   agents in the same invocation see them).
2. Trims `temp:` keys from `event.actions.state_delta` (so they don't get
   persisted).
3. Updates `session.state` with the remaining delta.
4. Appends the event to `session.events`.

**Subclasses must call `await super().append_event(...)`** to inherit this
behavior. We do. If anyone forgets, temp-state semantics break silently — a
classic ADK-integration bug.

---

### Why app_state and user_state are separate records (and why `get_session` is still one RTT)

A reasonable question: in NoSQL we usually denormalize. Why not fold
`app_state` and `user_state` into the session record so `get_session` is one
GET?

Because the cardinality is **1:N**, not 1:1:

- One `app_state` row is shared by every session of every user of that app.
  A modest deployment of 1 app × 10K users × 5 sessions each = **50K
  session records sharing one `app_state`**.
- One `user_state` row for `(app, alice)` is shared by every session Alice
  has — typically a handful, but the relationship is 1:N nonetheless.

ADK lets agents update `app:`/`user:` state at runtime via
`event.actions.state_delta`. Three patterns considered:

| Pattern | Reads | Writes (app/user state delta) | Storage | Semantics |
|---|---|---|---|---|
| Sequential GETs | 3 RTTs | 1 write | No duplication | Correct |
| **`batch_read` (current)** | **1 RTT** | **1 write** | **No duplication** | **Correct** |
| Fold into session record | 1 GET | N writes (N = sessions sharing scope) | Duplicated N× | Staleness bug |

Folding in would mean every `state_delta = {"app:flag": true}` triggers tens
of thousands of writes (one per session of every user of that app), plus a
staleness bug: an inactive session wouldn't see the update until re-saved.

The current implementation issues a single Aerospike `batch_read` for the
session record, the `app_state` record, and the `user_state` record — same
wire latency as a naïve one-GET design, while preserving the 1:N normalised
layout. See `AerospikeSessionService._batch_read` and `_merge_state` in
`sessions/service.py`.

## 4. Storage model — the Aerospike side

### Sets

All data lives in **one Aerospike namespace** chosen by the operator. Inside,
five sets (default prefix `adk_`):

| Set              | Holds                                           | Cardinality                |
| ---------------- | ----------------------------------------------- | -------------------------- |
| `adk_sessions`   | Session records + sealed chunk records (two record kinds in one set, see §5) | High (millions+)      |
| `adk_app_state`  | One record per (app)                            | Tiny (count of apps)       |
| `adk_user_state` | One record per (app, user)                      | Medium (count of users)    |
| `adk_artifacts`  | One record per (app, user, scope, filename, version) | Varies by app workload |
| `adk_memory`     | One record per text-bearing event ever added to memory | High (sum of session events × text events) |

### Key format

The primary key separator is `\x1f` (ASCII Unit Separator). It cannot appear
in valid ADK identifiers, so no escaping is needed.

```
adk_sessions       app\x1fuser\x1fsession                     ← session record
adk_sessions       app\x1fuser\x1fsession\x1fc:NNNNNNNN       ← chunk record
adk_app_state      app
adk_user_state     app\x1fuser
adk_artifacts      app\x1fuser\x1fscope\x1ffname\x1fNNNNNNNN  ← scope = session_id, or sentinel "user"
adk_memory         app\x1fuser\x1fsession\x1feventid
```

`POLICY_KEY_SEND` is enabled on every operation — the actual key string is
stored alongside the 20-byte RIPEMD-160 digest, so secondary-index queries
return the readable key. Adds ~30–70 bytes per record; worth it for
debuggability and `aql` browseability.

---

## 5. Design decision: chunked session records (the big one)

This is the most consequential design choice. Read it carefully.

### The problem

A naïve "one record per session, events embedded inline" design hits two
walls at scale:

1. **Aerospike record size cap.** `write-block-size` defaults to 1 MiB
   (configurable up to 8 MiB). A 1500-event session with 1 KiB events exceeds
   this. Cannot store.

2. **Write amplification.** Aerospike updates are read-modify-write of the
   whole record. Appending one event to a session record that already has 999
   events requires reading and rewriting ~1 MiB. Over a 1000-event session
   that's ~1 GiB of cumulative I/O for ~1 MiB of stored data — a **1000×
   amplification**.

Meanwhile, the alternative "one record per event" (which we shipped first)
trades these for the **small records problem**: Aerospike's per-record fixed
overhead (~64 B in primary index RAM + ~40 B on-disk record header) dominates
small payloads, hurting both RAM efficiency and SSD efficiency.

### The solution: hot tail + sealed chunks

A session's events are stored in **two places**:

1. **Session record** (mutable, ≤ ~280 KiB) — `state` Map + `events` List
   (the *hot tail*) + metadata. New events append to the tail.
2. **Chunk records** (immutable, ~256 KiB each) — when the tail reaches the
   flush threshold, it's atomically sealed into a new chunk record and the
   tail resets. Older history is in these chunks.

Both kinds live in the **same `adk_sessions` set**, distinguished by key
shape and bin presence.

### Why one set, not two

A previous iteration used a separate `adk_session_chunks` set. We folded back
to one for operational simplicity:

- One namespace truncate clears everything.
- One set-name to remember.
- Indexes don't multiply.
- Chunks omit the indexed bins (`app/uid/sid`), so they're **invisible** to
  the `idx_sess_uid` / `idx_sess_app` secondary indexes. No client-side
  filtering needed during `list_sessions`.

### Why this pays back the small-records problem

| Record kind | Typical size | Per-record overhead (~104 B) | Ratio |
|---|---|---|---|
| Session, empty state, no events (transient) | ~200 B | 104 B | 52% — bad but very rare |
| Session with active tail (typical) | 5–280 KiB | 104 B | ≤2% — excellent |
| Sealed chunk | ~256 KiB | 104 B | 0.04% — best in our system |

The chunk size is the **lever**. We set it large (256 KiB default) so every
record we store pays back the fixed overhead by orders of magnitude.

### Threshold choice: 256 KiB

- Default Aerospike `write-block-size` = 1 MiB.
- 256 KiB = ¼ of write-block-size → **4× safety margin** for state Map growth,
  retry slack, byte-size estimator error.
- At ~800 B/event average → ~320 events per chunk.
- At ~2 KiB/event heavy traffic → ~128 events per chunk.
- Configurable: `AerospikeSessionService(..., flush_threshold_bytes=...)`.

### Huge-event handling: 900 KiB threshold

A single event over 900 KiB triggers a **pre-flush**: the current tail is
sealed as a chunk *before* the huge event is added, so the huge event lives
alone in a fresh tail (and immediately flushes again post-append). This keeps
us under the 1 MiB write-block-size even for outlier events.

If an event itself exceeds 1 MiB, Aerospike will reject the write. Document
this as a hard limit.

### Write amplification math — the actual win

Appending 1000 events of ~1 KiB each to one session:

| Strategy | Cumulative bytes written | Amplification |
|---|---|---|
| Naïve embedded list (no chunking) | ~1 GiB | 1000× |
| One-record-per-event (our v0.1 design) | ~1 MiB | ~1× (but other costs) |
| **Chunked (256 KiB threshold)** | **~64 MiB** | **64×** |

Chunking is ~16× better than naïve embedded. Worse than per-event records on
this metric alone, but vastly better on `get_session` latency, simplicity,
and the small-records problem. Net win.

### Atomicity & crash safety

**Fast-path append** — 99% of calls. A single server-side atomic `operate()`
on the session record:

```python
[
    map_put_items(state, session_delta),  # if delta present
    list_append(events, event_dict),
    increment(seq, 1),
    increment(tbytes, est_size),
    write(ts, now),
    read(tbytes),                         # returns post-increment value
]
```

One round trip. Atomic by Aerospike's single-record guarantee. **No MRT
needed.** This is one of the cleanest design wins.

**Flush path** — every ~320 events. Two operations:

1. PUT chunk record at key `…\x1fc:NNNNNNNN` (overwrites any orphan from a
   prior interrupted flush).
2. Generation-checked `operate()` on session record: clear tail, increment
   `chunks` counter, reset `tbytes` to 0.

If step 2's gen-check fails (another writer raced us), we leave our chunk
as an orphan and bail. Crash safety is preserved by an explicit invariant.

### The crash-safety invariant

> **A chunk record `c:N` is valid IFF `session.chunks > N`.**

Readers always trust `session.chunks` as authoritative. Any chunk record
with `cidx >= session.chunks` is an orphan from an interrupted flush and is
ignored.

Concrete scenarios:

| Scenario | What happens |
|---|---|
| Two writers race to flush simultaneously | First wins gen-check + bumps chunks. Second's chunk PUT overwrites first's (same key), but gen-check fails → second leaves orphan. First's chunk is the canonical one. |
| Crash between chunk PUT and session reset | Chunk written at `cidx == session.chunks`. Invariant: invalid. Tail still has all events. Next append succeeds normally; eventually triggers another flush which overwrites the orphan and bumps chunks. Self-healing. |
| Reader during concurrent flush | Reader trusts session.chunks. Sees the pre-flush state with events in tail. Once flush commits, next read sees post-flush state with events in chunk + fresh tail. Eventually consistent. |

No data loss in any failure mode because **the tail is the source of truth
until the gen-checked reset commits**.

### Read optimization: server-side pagination

`get_session(num_recent_events=N)` is optimized:

1. GET session record (1 op). If tail has ≥ N events, return last N. **Done.**
2. Else walk chunks newest → oldest. For each chunk, use
   `list_get_by_index_range(events, -K, K)` — server-side returns only the
   last K events without transferring the whole 256 KiB list.

Worst case: `1 + ceil((N − tail_size) / chunk_size)` ops. Best case: 1 op.

For `after_timestamp=T`, chunks store `ts_lo` and `ts_hi`. A `select()` of
just `ts_hi` lets us skip entire chunks where `ts_hi < T` without reading
the events list.

---

## 6. Concrete key examples — what's actually in Aerospike

Scenario: app `support_bot`, user `alice`, session `s-2026-05-22-xyz`.

### After `create_session(...)`

```
SET:  adk_sessions
KEY:  "support_bot\x1falice\x1fs-2026-05-22-xyz"
bins:
  app:     "support_bot"
  uid:     "alice"
  sid:     "s-2026-05-22-xyz"
  state:   {"language": "en", "topic": "billing"}
  events:  []
  ts:      1747929210.117
  seq:     0
  chunks:  0
  tbytes:  0

SET:  adk_app_state                  SET:  adk_user_state
KEY:  "support_bot"                  KEY:  "support_bot\x1falice"
bins:                                bins:
  state: {"tenant": "acme-corp"}       state: {"nickname": "Allie"}
```

### After 3 small `append_event` calls (still under threshold)

```
SET:  adk_sessions
KEY:  "support_bot\x1falice\x1fs-2026-05-22-xyz"
bins:
  app:     "support_bot"
  uid:     "alice"
  sid:     "s-2026-05-22-xyz"
  state:   {"language": "en", "topic": "billing", "turn": 3}
  events:  [
    {"eid": "ev_7f3c9a", "ts": 1747929211.0, "author": "user",
     "content": {"role": "user", "parts": [{"text": "Where's my invoice?"}]},
     "actions": {"state_delta": {"turn": 1}}, "branch": null},
    {"eid": "ev_a1b8e2", "ts": 1747929212.5, "author": "assistant",
     "content": {"role": "model", "parts": [{"text": "Looking it up..."}]},
     "actions": {"state_delta": {"turn": 2}}, "branch": null},
    {"eid": "ev_d4f2c1", "ts": 1747929213.998, "author": "assistant",
     "content": {"role": "model", "parts": [{"text": "Paid May 18."}]},
     "actions": {"state_delta": {"turn": 3}}, "branch": null},
  ]
  ts:      1747929213.998
  seq:     3
  chunks:  0
  tbytes:  478
```

### After ~640 appends → flush triggered

```
SET:  adk_sessions
KEY:  "support_bot\x1falice\x1fs-2026-05-22-xyz"   ← session record (tail reset)
bins:
  app:     "support_bot"
  uid:     "alice"
  sid:     "s-2026-05-22-xyz"
  state:   {"language": "en", "topic": "billing", "turn": 644, ...}
  events:  [
    {"eid": "ev_x9y0z1", "ts": 1747933100.521, ...},   # the 644th event
  ]
  ts:      1747933100.521
  seq:     644
  chunks:  1                          ← one sealed chunk now exists
  tbytes:  412

SET:  adk_sessions
KEY:  "support_bot\x1falice\x1fs-2026-05-22-xyz\x1fc:00000000"   ← chunk record
bins:
  cidx:    0
  events:  [
    {"eid": "ev_7f3c9a", ...},   # event 1
    {"eid": "ev_a1b8e2", ...},   # event 2
    ...
    {"eid": "ev_q8w7e6", ...},   # event 643
  ]
  ts_lo:   1747929211.0
  ts_hi:   1747933095.012
```

**Note what's NOT in the chunk record**: no `app`, `uid`, `sid` bins.
Deliberate — the secondary indexes only fire on records that have those
bins, so chunks stay invisible to `list_sessions`.

### A versioned artifact

```
SET:  adk_artifacts
KEY:  "support_bot\x1falice\x1fs-2026-05-22-xyz\x1freceipt.png\x1f00000000"
bins:
  app, uid, sid, fname, ver, mime
  data:   <bytes — PNG payload>
  ctime:  1747929210.117
  cmeta:  {"source": "user_upload"}
```

User-scoped artifact (`user:` prefix on filename → sentinel `"user"` in
session slot, matching `InMemoryArtifactService`'s path scheme):

```
SET:  adk_artifacts
KEY:  "support_bot\x1falice\x1fuser\x1fuser:avatar.jpg\x1f00000000"
bins: ... sid="user" ...
```

### A memory entry

```
SET:  adk_memory
KEY:  "support_bot\x1falice\x1fs-2026-05-22-xyz\x1fev_d4f2c1"
bins:
  app, uid, sid, eid, author, ts
  text:    "Paid May 18."
  embed:   [0.0123, -0.0456, ..., 0.0021]   # list[float], dim 768 typical
  content: {"role": "model", "parts": [{"text": "Paid May 18."}]}
```

---

## 7. Memory service — design decisions

### Decision: lexical word-overlap (not vector embeddings)

`BaseMemoryService.search_memory(query: str)` takes a free-form text query
and returns matching past events. The ADK contract leaves the matching
algorithm entirely to the implementation; the canonical
`InMemoryMemoryService` uses **lowercase `[A-Za-z]+` word-set overlap** —
no embeddings.

We surveyed every shipping `BaseMemoryService` and found two camps:

| Camp | Implementations | Embedder source |
|---|---|---|
| **Vendor-managed semantic** | `VertexAiMemoryBankService`, `VertexAiRagMemoryService`, Redis `RedisLongTermMemoryService` | Vendor-side managed service (Vertex / Agent Memory Server) |
| **Lexical** | `InMemoryMemoryService`, `adk-database-memory`, `adk-extra-services` (3 backends) | None |

**No community package asks the user to supply an embedder.** Every lexical
backend uses a different mechanism — word-set overlap, SQL `LIKE`, MongoDB
`$text` — but none reach outside the database. We chose to match this
expectation: `adk-aerospike` is a storage backend, not an AI/ML product.

### Decision: execute matching server-side via Aerospike's list-element secondary index

Lexical doesn't have to mean client-side scan. Aerospike's **list-element
secondary index** is the database's first-class primitive for keyword/tag
search; the matching runs server-side via
`predicates.contains(bin, INDEX_TYPE_LIST, value)`.

The pattern is officially endorsed:

- **[Aerospike 3.8 release notes](https://aerospike.com/blog/aerospike-3-8-release/):**
  the feature debut used exactly this query shape as its headline example.
- **[Query JSON Documents Faster with New CDT Indexing](https://aerospike.com/blog/query-json-documents-faster-and-more-with-new-cdt-indexing):**
  *"Get records with a specific integer or string value. In a List: records
  with a list containing 100/\"ABC\"."*
- **[List indexing and querying](https://aerospike.com/docs/develop/data-types/collections/list/index-and-query/):**
  canonical doc; uses an email-list example structurally identical to our
  keyword-list use.
- **[discuss.aerospike.com — Full text research queries](https://discuss.aerospike.com/t/full-text-research-queries/2444):**
  Aerospike staff recommend *"build a secondary index on that list for
  searches"* for the article-with-tags scenario. For genuine full-text
  needs they direct users to the
  [Elasticsearch connector](https://aerospike.com/blog/build-full-text-search-applications-on-aerospike-using-elasticsearch/) —
  we're explicitly in the tag/keyword tier, not the full-text tier.

### Implementation

**Write (`add_session_to_memory`)** — tokenize text in Python, store on the
record:

```python
keywords = list({m.group(0).lower() for m in _WORD_RE.finditer(text)})
client.put(pk, {
    "app": app_name, "uid": user_id, "sid": session_id, "eid": event.id,
    "text": text, "keywords": keywords, "author": event.author, "ts": ...,
    "content": event.content.model_dump(mode="json"),
})
```

The dedupe keeps the per-record list bounded (Aerospike list-element index
limits ~1024 elements/record by default — well above any single chat turn).

**Read (`search_memory`)** — fan out one indexed query per query token:

```python
query_tokens = _tokenize(query)
results = await asyncio.gather(*[
    asyncio.to_thread(self._run_token_query, token) for token in query_tokens
])
# Per-token query inside _run_token_query:
#   q.where(predicates.contains("keywords", INDEX_TYPE_LIST, token))
# Aerospike returns only records whose keywords list contains the token.
```

Then client-side: scope-filter by `(app, user)`, dedupe by event id, rank
by token-overlap count + recency, return top-k as `MemoryEntry` list.

### Trade-offs

| Aspect | Choice | Why |
|---|---|---|
| Algorithm | Lexical word-overlap | Matches `InMemoryMemoryService` semantics; matches community-package norm; no AI dependency |
| Where matching happens | **Aerospike server-side** via list-element index | Canonical Aerospike pattern; scales to millions of memories per user |
| Tokenizer | `[A-Za-z]+` lowercase + dedupe | Identical to `InMemoryMemoryService` |
| Ranking | Token-overlap count, tie-break by recency | Naïve but transparent; client-side, no scoring primitive needed from Aerospike |
| Stemming / fuzzy / phrase | **Not supported** | Out of tier — Aerospike directs users to the Elasticsearch connector for that |
| Embedder | **None — not part of the public API** | Removes AI surface; matches every other community lexical package |

### When this is not enough

If a user genuinely needs semantic search (paraphrase recall, fuzzy match,
multi-language), they should layer a real search engine — either Aerospike's
Elasticsearch connector or a vendor-managed memory service like Vertex
Memory Bank — on top of (or alongside) `adk-aerospike`. We chose not to
fake it with a thin embedder hook that would obscure the real
architectural decision.

---

## 8. Artifact service — design decisions

### Decision: store versions as separate records (not list of versions on one record)

Key shape:
```
adk_artifacts   app\x1fuser\x1fscope\x1ffname\x1fNNNNNNNN
```

The version is part of the primary key. So:
- **Read of a specific version**: 1 GET, no scan.
- **Read of the latest version**: secondary-index query on `fname` →
  collect versions → pick max → GET that version.
- **`list_versions`**: secondary-index query on `fname` → return sorted
  version ints.

Alternative considered: one record per (app, user, scope, fname) holding a
list-of-versions. Rejected because:
- Aerospike record size cap would limit artifact size (latest artifact
  has to fit alongside all old versions in one record).
- Reading "just the latest" still loads all versions.
- Versioning conflicts on concurrent saves require list-CDT operations
  with more complexity.

The current shape lets large artifacts (close to write-block-size) coexist
with many versions.

### Decision: mirror `InMemoryArtifactService._artifact_path` for user-scoped files

Files whose name starts with `user:` are user-scoped (cross-session). ADK's
`InMemoryArtifactService` (see `in_memory_artifact_service.py:56-91`) builds
the storage path as:

```
session-scoped:   {app}/{user}/{session_id}/{filename}
user-scoped:      {app}/{user}/user/{filename}
```

We mirror this: the **session-id slot in the key becomes the sentinel
`"user"`** when the filename starts with `user:`. This means:
- A session literally named `"user"` would collide with user-scoped artifacts
  — same constraint upstream has. Document as a reserved session-id.
- `load_artifact(filename="user:avatar.jpg", session_id="anything")` finds
  the artifact regardless of `session_id`.

### Decision: implement the ADK 2.x extension methods

ADK 2.0 added two abstract methods that weren't in 1.x:
- `list_artifact_versions(...)` → `list[ArtifactVersion]`
- `get_artifact_version(...)` → `ArtifactVersion | None`

`ArtifactVersion` is a Pydantic model with `version`, `canonical_uri`,
`custom_metadata`, `create_time`, `mime_type`. We:
- Store `create_time` (`ctime` bin) and `custom_metadata` (`cmeta` bin) on
  save.
- Build the `canonical_uri` as
  `aerospike://apps/{app}/users/{user}/sessions/{session}/artifacts/{file}/versions/{N}`
  (or `…/users/{user}/artifacts/…` for user-scoped — no session segment).

These are required for the subclass to be instantiable. Any subclass missing
them fails with `TypeError: Can't instantiate abstract class…`.

---

## 9. Connection & client management

### One client per process, injected

Each service holds an `aerospike.Client`. The factory `from_uri(...)` builds
and connects one. For multi-service deployments, instantiate a single client
and pass it to all three:

```python
from adk_aerospike._internal.client import make_client
from adk_aerospike._internal.uri import parse as parse_uri
from adk_aerospike import (
    AerospikeSessionService,
    AerospikeArtifactService,
    AerospikeMemoryService,
)

uri = parse_uri("aerospike://localhost:3000/adk")
client = make_client(uri)
sessions = AerospikeSessionService(client, "adk")
artifacts = AerospikeArtifactService(client, "adk")
memory = AerospikeMemoryService(client, "adk", embedder=my_embedder)
```

### Default policies (in `_internal/client.py`)

| Policy class | Setting | Rationale |
|---|---|---|
| All reads/writes | `key = POLICY_KEY_SEND` | Store readable key on record for debuggability + sec-index returns |
| Writes | `commit_level = COMMIT_ALL` | Durability over a few ms of latency |
| Writes | `max_retries = 0` | Writes are not idempotent by default — never retry |
| Reads | `max_retries = 2` | Idempotent, low cost |
| Read/Write/Operate/Remove | `total_timeout = 1000ms` | Fail fast under load |
| Queries | `total_timeout = 10s` | Index queries take longer |

All overridable via `make_client(uri, policies={...})`.

### TLS, auth

- TLS enabled via URI `?tls=true` or `tls_config={...}` kwarg.
- Default auth is `INTERNAL`. Override via `auth_mode=aerospike.AUTH_EXTERNAL`
  / `AUTH_PKI` / `AUTH_EXTERNAL_INSECURE`.

### Thread-safe, fork-unsafe

The C client is safe to share across asyncio tasks and threads. It is **NOT
safe across `os.fork()`** — child processes must build their own client. Use
`multiprocessing`'s `"spawn"` start method, or instantiate the client
post-fork. Document for Gunicorn/uWSGI deployments.

---

## 10. Operational guidance

### Aerospike server requirements

- **Aerospike Database 7.x or later** (CE or EE). The image
  `aerospike/aerospike-server:latest` is currently 8.1.2.1.
- Recommend `replication-factor = 2` in production.
- Recommend SC (strong consistency) namespace for any agent app where event
  loss is unacceptable. Our schema works in both SC and AP modes — flush is
  crash-safe in either.
- `write-block-size` ≥ 1 MiB. Default is fine. Larger (up to 8 MiB) allows
  bigger artifacts inline.

### Sizing

| Metric | Per-record overhead | Notes |
|---|---|---|
| Primary index | 64 B RAM | Fixed, regardless of record size |
| Storage record header | ~40 B on disk | Plus bin overhead (~8 B per bin) |
| Secondary index entry | ~16 B RAM | Per indexed bin per record |
| `POLICY_KEY_SEND` overhead | 30–70 B on disk | Key string copy |

For 1M concurrent sessions averaging 50 events each:
- PI RAM: 1M × 64 B = **64 MiB** for the index alone (plus chunks if any)
- Storage: ~1M × (200 B state + 50 × 800 B events + overhead) ≈ **40 GB**

For 1B memory entries (massive):
- PI RAM: 1B × 64 B = **64 GB** — sized for it
- Plus secondary index on `uid`: another 16 GB

### Aerospike-in-Docker gotchas (codified in `tests/conftest.py`)

These bit us during development. Document for anyone running the test suite
or setting up local dev:

1. **Cluster-tend leaks container IPs.** The server tells clients its
   container IP (e.g. `172.17.0.2`); clients can't reach it from the host.
   **Fix:** mount a custom `aerospike.conf` setting `access-address 127.0.0.1`
   and `access-port <bound-host-port>`. The host port must be picked
   *before* writing the config; our `_find_free_port()` reserves one first.

2. **Don't mount config at `/etc/aerospike/aerospike.conf`.** The image's
   entrypoint does shell-style env-var substitution from
   `aerospike.template.conf` and writes the result to `aerospike.conf`.
   Mounting at the destination breaks with "read-only filesystem." **Mount
   at `/etc/aerospike/aerospike.template.conf` instead** — our pre-formatted
   config has no `${...}` placeholders, so substitution is a no-op.

3. **Aerospike 8.x requires `cluster-name`** in the `service { ... }` block.
   7.x didn't. Image `:latest` is currently 8.x.

4. **`index_string_create()` is deprecated in Python client 19.x.** Use
   `index_single_value_create(ns, set, bin, INDEX_STRING, name)`. Migrated.

### Tests

```bash
# Unit tests only (no Docker required, ~2s)
.venv/bin/pytest -m "not aerospike"

# Full suite — spins up a testcontainers Aerospike CE container (~2 min)
.venv/bin/pytest
```

Current state: **39 passed, 0 skipped, 0 failed.**

The container is scope=session (started once, reused across tests, torn down
at end). Image `aerospike/aerospike-server:latest`.

---

## 11. Market positioning

DevRel will need this for blog posts, talks, and the ADK integrations
listing submission.

### Comparison table (as of 2026-05)

| Integration | Maintainer | Sess | Art | Mem | Architecture | URI scheme | Vector / embedder |
|---|---|:-:|:-:|:-:|---|---|---|
| **adk-aerospike** (us) | Aerospike | ✓ | ✓ | ✓ lexical | In-process, single backend, no sidecar | `aerospike://` registered | No embedder — lexical word-overlap via list-element secondary index |
| adk-redis | Redis Inc. | ✓ | ✗ | ✓ | HTTP sidecar (Agent Memory Server :8088) + RedisVL | Import-only | Sidecar embeds; not user-injectable |
| adk-python built-in | Google | ✓ (InMemory, SQLAlchemy, Vertex) | ✓ (InMemory, GCS) | ✓ (InMemory, Vertex MemoryBank, Vertex RAG) | In-process / managed cloud | `sqlite://`, `postgresql://`, `mysql://`, `agentengine://`, `gs://` | Vertex hides embedder |
| adk-extra-services | Community | ✓ Mongo, Redis | ✓ S3, Local, Azure | ✗ | In-process | Import-only | N/A |
| google-adk-extras | Community | ✓ SQL, Mongo, Redis, YAML | ✓ Local, S3, SQL, Mongo | ✓ keyword-only | In-process | Import-only | No embedder; term matching |
| adk-database-memory | Community | ✗ | ✗ | ✓ SQLAlchemy keyword | In-process | Import-only | Keyword extraction |
| MongoDB / Pinecone / Qdrant / Couchbase / Chroma | Vendors | ✗ | ✗ | ✗ (just tools) | MCP server | N/A | Vendor-managed |

### Key positioning lines DevRel can use

- **"The only ADK package shipping all three storage interfaces — Session,
  Artifact, and Memory with embedding-based semantic memory — backed by a
  single in-process database. No HTTP sidecar, no managed-cloud dependency,
  no MCP-tool indirection."**

- **"Sub-millisecond agent state on a database that already runs your
  production workloads."** (For Aerospike customers.)

- **"BYO embedder via a plain Python callable. No sidecar config, no
  vendor lock-in on the embedding provider."**

### Caveats DevRel should be aware of

- We are alpha (0.0.1). `google-adk-extras` (0.3.8 beta) is the most
  polished competitor. We can credibly claim *parity or better* on
  capability without overstating maturity.
- Memory search is brute-force. Above ~100K memories per user, performance
  degrades linearly. Numpy vectorization is the documented next step.

---

## 12. Known limits and gaps

Document these for users; pick which ones to close before 1.0.

| Limit | Severity | Mitigation / future work |
|---|---|---|
| Memory search ranking is naïve (token-overlap count, no TF-IDF/BM25) | Low | Ranking happens client-side; can be enhanced without schema change. For true full-text needs, use Aerospike's Elasticsearch connector. |
| Aerospike list-element index has ~1024 elements/record cap | Low | Tokenizer dedupes; typical chat turn well under cap. Cap configurable in namespace config. |
| Session record ≤ ~280 KiB (post-chunking) | Low | By design; rejects state Maps > ~50 KiB |
| Artifact inline ≤ namespace `write-block-size` | Medium | Plan hybrid: large artifacts to S3/GCS with reference here |
| Reserved session-id `"user"` (collides with user-scoped artifact slot) | Low | Inherit ADK upstream's same constraint; document |
| App/user state updates not atomic with session append | Low | ADK's contract permits this; `DatabaseSessionService` doesn't guarantee either |
| Fork-unsafe client | Low | Document for multiprocessing users; recommend `spawn` |
| No Java port | Medium | Mirror in adk-java contrib later (see Firestore PR #601 for layout) |
| Not on PyPI yet | High | Productionization task |
| Not listed in `google/adk-docs` | High | Productionization task |

---

## 13. Productionization checklist (handoff to DevRel)

In rough priority order:

### Must-do before 1.0

- [ ] **PyPI release** as `adk-aerospike`. Wheel + sdist. Set up GH Actions
      for tag-triggered publish. Use trusted publishing (no API key).
- [ ] **Submit to `google/adk-docs`** for inclusion in the integrations
      listing. See
      `https://github.com/google/adk-docs/blob/main/CONTRIBUTING.md#integrations`.
      Use `docs/integrations.md` as the source markdown.
- [ ] **Real-LLM smoke test** for `examples/quickstart.py` — currently it
      requires a Gemini API key; add a CI job (or document) that runs it end
      to end at release time.
- [ ] **Logo asset** at `docs/aerospike.png` (referenced in
      `integrations.md` frontmatter as `catalog_icon`).
- [ ] **Pre-PyPI sanity audit:** secrets in repo, license headers on every
      source file (Apache-2.0), CHANGELOG.md.

### Should-do

- [ ] **Numpy-vectorized cosine search** for `AerospikeMemoryService`. Optional
      `numpy` extra; fall back to pure Python.
- [ ] **Benchmark suite** — micro-benchmarks for `append_event`,
      `get_session`, `search_memory`. Compare with `InMemorySessionService`
      and `DatabaseSessionService(sqlite)`. Publish numbers.
- [ ] **Blog post** announcing the integration. Headlines: triple coverage,
      single-record atomic appends, no sidecar, BYO embedder.
- [ ] **Tutorial** showing a working agent (e.g., billing chatbot) wired up
      with Aerospike for all three services. ~50 lines of code; show how
      `adk web` flags drive it.

### Nice-to-have

- [ ] **adk-java port** at `~/IdeaProjects/GoogleADK`. Mirror the Firestore
      Java contrib pattern (`google/adk-java/contrib/firestore-session-service`).
- [ ] **Hybrid artifact storage** — large artifacts spill to S3/GCS, reference
      stored in Aerospike. Already noted in CLAUDE.md "Next slice."
- [ ] **MRT support** for app/user-state delta routing alongside session
      append — would make a 3-record state-delta atomic. Currently
      independent ops; ADK's contract doesn't require atomicity here, but
      it'd be nice to have.
- [ ] **Sample integration with `aerospike` Aerospike Cloud** — show how to
      deploy ADK + Aerospike Cloud end-to-end on GCP/AWS.

---

## 14. Authoritative source references

When in doubt, the upstream ADK source is the contract. Quick reference:

| File (installed under `.venv/lib/python3.11/site-packages/google/adk/`) | What it defines |
|---|---|
| `sessions/session.py` | `Session` pydantic model (the type we return) |
| `sessions/state.py:64-66` | `APP_PREFIX`, `USER_PREFIX`, `TEMP_PREFIX` |
| `sessions/_session_util.py:37-50` | `extract_state_delta` — the splitter |
| `sessions/schemas/v1.py:72-265` | `StorageSession`/`StorageEvent`/`StorageAppState`/`StorageUserState` SQLAlchemy tables — our reference layout |
| `sessions/base_session_service.py:116-167` | `BaseSessionService.append_event` concrete base — handles temp state |
| `sessions/in_memory_session_service.py` | Simplest reference impl |
| `artifacts/in_memory_artifact_service.py:56-91` | `_artifact_path` user-namespace rule |
| `artifacts/base_artifact_service.py` | Including ADK-2.x extension methods `list_artifact_versions` / `get_artifact_version` and `ArtifactVersion` model |
| `memory/in_memory_memory_service.py` | One-entry-per-event reference shape |
| `memory/base_memory_service.py` | `SearchMemoryResponse` |
| `memory/memory_entry.py` | `MemoryEntry` model |

To verify our schema still matches Google's:

```bash
# The four storage tables Google uses — our sets mirror these
.venv/bin/python -c "
import inspect, re
from google.adk.sessions.schemas import v1
src = inspect.getsource(v1)
for cls in ['StorageSession','StorageEvent','StorageAppState','StorageUserState']:
    print('---', cls, '---')
    m = re.search(rf'class {cls}.*?(?=\nclass |\Z)', src, re.DOTALL)
    print((m.group(0) if m else 'NOT FOUND')[:600])
"

# The state prefix routing rule
.venv/bin/python -c "
import inspect
from google.adk.sessions import _session_util
print(inspect.getsource(_session_util.extract_state_delta))
"

# The artifact user-namespace path rule
.venv/bin/python -c "
import inspect
from google.adk.artifacts.in_memory_artifact_service import InMemoryArtifactService as S
print(inspect.getsource(S._artifact_path))
"
```

If any of these change in a future ADK release, our backend needs to follow.

---

## 15. Contact / ownership

- Original author: ggeorges@aerospike.com (engineering; for technical
  questions on the design)
- Productionization owner: **DevRel team** (TBD)
- Related repos:
  - `~/IdeaProjects/adk-aerospike` (this repo, Python)
  - `~/IdeaProjects/GoogleADK` (placeholder for future Java port)

For ongoing notes and "claude-readable" project context, see
[`CLAUDE.md`](./CLAUDE.md) — that file is auto-loaded by Claude Code sessions
working in this repo.
