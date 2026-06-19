# CLAUDE.md — adk-aerospike

Context for Claude Code working in this repository. Auto-loaded on session start.

## What this project is

A Python package that implements [Google ADK](https://adk.dev/)'s three pluggable storage interfaces — `BaseSessionService`, `BaseArtifactService`, `BaseMemoryService` — on top of **Aerospike Database**.

Target: PyPI release as `adk-aerospike`, eventual listing in `google/adk-docs`, parallel adk-java `contrib/` PR. Status: **alpha; all three services (Session, Artifact, Memory) implemented end-to-end against ADK 2.x and tested.**

## Who the user is

`ggeorges@aerospike.com` — Aerospike engineer. Assume strong Aerospike fluency: CDTs, secondary indexes (including list-element indexes), strong consistency, MRTs (multi-record transactions). Less assumption about ADK internals. **Frame ADK details in terms of Aerospike primitives**, not the other way around.

User style preferences observed in prior sessions:
- Terse responses. End-of-turn one or two sentences.
- Comfortable with you running real commands (docker, pytest) — verify behavior, don't just type-check.
- Values Aerospike-shaped framing ("how does this map to a CDT op?", "what if SC namespace?").
- Wants you to use `TaskCreate` / `TaskUpdate` for multi-step work.

## Market context (why this exists)

The third-party ADK storage integration landscape (as of 2026-05) is thin and uneven:

| DB | Session | Memory | Artifact | Notes |
|---|---|---|---|---|
| Redis | ✅ vendor | ✅ vendor | ❌ | `adk-redis`. Architecturally questionable: uses an HTTP sidecar (Redis Agent Memory Server). |
| MongoDB | ✅ community | ❌ | ❌ | `adk-extra-services`, no memory, no URI registration. |
| Pinecone, Couchbase, Qdrant, Atlas-Vector | ❌ | ❌ | ❌ | **MCP tools only** — not real services. |
| Firestore (Java) | ✅ | ✅ | ❌ | adk-java PR #601 — the pattern to mirror for our Java port. |
| pgvector | (SQL built-in) | blog-only | ❌ | Unmaintained. |

**Full triple-coverage Session + Memory + Artifact in one package: nobody has it. That's our gap.**

Our differentiators:
1. Triple coverage in one package.
2. **URI scheme registration** via `service_registry` — `adk web --session_db_url=aerospike://…` works (no competitor does this).
3. **Lexical memory in core Aerospike** — tokenized at write time; posting-list PKs (`app:user:kw:<token>`) point at memory rows. Same word-overlap semantics as `InMemoryMemoryService`. No embedder, no HTTP sidecar.
4. Mirror in adk-java contrib/ later.

## Current status

| Component | Status | File |
|---|---|---|
| Client / connection | Implemented with best-practice defaults | `src/adk_aerospike/_internal/client.py` |
| URI parser | Implemented | `src/adk_aerospike/_internal/uri.py` |
| Keys / schema / indexes | Implemented | `src/adk_aerospike/_internal/{keys,schema,indexes}.py` |
| Codec helpers | Implemented (state, events, text extraction) | `src/adk_aerospike/_internal/codec.py` |
| `create_session` / `get_session` (segment-walk hydration, server-side last-N pagination via K_ORDERED map ops, single-RTT `batch_read` of session + app/user state) | **Implemented + tested** | `src/adk_aerospike/sessions/service.py` |
| `append_event` (idempotent K_ORDERED `map_put`; overflow-driven segment rollover; 1-RTT `batch_write` coalescing) | **Implemented + tested** | same |
| `list_sessions` / `delete_session` (cascades all segments) | **Implemented + tested** | same |
| `AerospikeArtifactService.*` (incl. `list_artifact_versions`, `get_artifact_version`) | **Implemented + tested** | `src/adk_aerospike/artifacts/service.py` |
| `AerospikeMemoryService.*` (lexical word-overlap; posting-list PK search) | **Implemented + tested** | `src/adk_aerospike/memory/service.py` |
| `register()` (URI schemes) | Implemented | `src/adk_aerospike/registry.py` |
| Tests | **41 passed, 0 skipped, 0 failed** | `tests/` |
| Testcontainers Aerospike fixture | Working | `tests/conftest.py` |

## Next slice (in order)

1. **Hybrid artifact storage** — offload artifacts above the namespace write-block-size to S3/GCS, store only a reference here. Schema bin `data` becomes either bytes (inline) or a URI string (referenced).

2. **adk-java port** — mirror in `~/IdeaProjects/GoogleADK`. See Firestore Java PR #601 for the layout.

3. **Optional opt-in semantic memory** — if customers ask for paraphrase recall later, add a sibling `AerospikeSemanticMemoryService` that uses the Elasticsearch connector or a vendor MemoryBank. Don't fold it into the default service — keep `AerospikeMemoryService` honest as a storage-side keyword lookup.

**Resolved this slice:** rewrote session event storage from the predictive
chunked-tail/flush model to **overflow-driven K_ORDERED segments** (idempotent
`map_put`, `RecordTooBig`-driven rollover, 1-RTT `batch_write` hot path) — fixes
heavy-concurrency data loss / `RecordTooBig` and is faster (multi-scope appends
now match single-scope latency). See `docs/design-session-segments.md`.

## Layout & key conventions

```
src/adk_aerospike/
├── __init__.py
├── registry.py          # PUBLIC: register() — wires aerospike:// URI schemes into ADK
├── _internal/           # PRIVATE shared helpers (pydantic-style _internal package)
│   ├── client.py        # Aerospike client factory + best-practice policies
│   ├── codec.py         # Pydantic <-> bin (de)serialization
│   ├── indexes.py       # Idempotent secondary index creation
│   ├── keys.py          # PK construction; separator is :
│   ├── schema.py        # Schema dataclass, Bins, StateScope constants
│   └── uri.py           # aerospike:// URI parsing
├── sessions/
│   ├── __init__.py      # re-exports AerospikeSessionService
│   └── service.py
├── artifacts/{__init__,service}.py
└── memory/
    ├── __init__.py
    └── service.py       # Lexical word-overlap via posting-list PKs
```

Underscore conventions: `_internal/` = private subpackage, may change without major version bump. `registry.py` has no underscore because `register()` is real public surface.

## Connection best practices (codified in `_internal/client.py`)

- **One client per app**, injected into services. Construction is expensive (DNS + TLS + cluster tend).
- **Async bridge:** sync client wrapped in `asyncio.to_thread` per call. ADK is async; client isn't.
- **Default policies:**
  - `POLICY_KEY_SEND` on read/write/operate/remove — keys recoverable from index queries.
  - `COMMIT_LEVEL_ALL` on writes — durability over a few ms of latency.
  - `max_retries=0` on writes — not safe to retry non-idempotent operations.
  - `max_retries=2` on reads/queries — safe, low cost.
  - `total_timeout=1000ms` reads/writes, `10s` queries.
- **TLS:** URI `?tls=true` or explicit `tls_config={…}` for mTLS.
- **Auth modes:** INTERNAL (default) / EXTERNAL / EXTERNAL_INSECURE / PKI via `auth_mode` factory param.
- **Thread-safe, fork-unsafe** — document for users using `multiprocessing`.

## State scoping (ADK convention)

`Session.state` is one dict but uses key prefixes to route to different scopes:

| Prefix | Storage | Survives across |
|---|---|---|
| `app:foo` | `app_state` set, key=`app_name` | every user of the app |
| `user:foo` | `user_state` set, key=`(app_name, user_id)` | a user's sessions |
| `temp:foo` | not persisted | nothing — in-process only |
| (unprefixed) | `state` Map bin on session record | this session only |

`_partition_state()` (module-private in `sessions/service.py`) splits on write; `_merge_state_for_read()` re-applies prefixes on read. `get_session` returns the same shape as ADK's reference `DatabaseSessionService` (app/user keys prefixed, session keys bare).

## ADK session hierarchy (canonical) — and how we map it

A `Session` returned by `get_session` is **not one row**. ADK splits it into four logical objects that compose at read time:

```
                       Session  (returned to the caller)
                       ├── state: merged view of three scope rows
                       └── events: list, ordered by append sequence
                              │
   ┌──────────────────────────┼──────────────────────┐
   AppState               UserState              SessionState
   keyed by               keyed by               lives on the
   (app_name)             (app_name,user_id)     session record
   shared by every        survives across one    this session only
   user of an app         user's sessions
                              │
                              ▼
                          Event[0..N]   (FK → Session, ordered by timestamp)
```

This is **Google's design**, not ours. The proof is in the SQLAlchemy schema that `DatabaseSessionService` uses — four tables, one per scope.

### Authoritative source files in installed ADK

(All paths relative to `.venv/lib/python3.11/site-packages/google/adk/`)

| File | What it defines | Why it matters to us |
|---|---|---|
| `sessions/session.py` (49 lines) | The `Session` pydantic model returned to users — one `state` dict, one `events` list, `last_update_time` | Defines the *shape* we must hand back from `get_session` |
| `sessions/state.py:64-66` | `State.APP_PREFIX="app:"`, `USER_PREFIX="user:"`, `TEMP_PREFIX="temp:"` | Source of truth for the prefix constants — do not re-define |
| `sessions/_session_util.py:37-50` | `extract_state_delta(state) -> {"app":..., "user":..., "session":...}` | The splitter rule; `_partition_state` in our code is a clone — keep them in sync |
| `sessions/schemas/v1.py:72-265` | `StorageSession`, `StorageEvent`, `StorageAppState`, `StorageUserState` SQLAlchemy tables | **Load-bearing**: Google's own four-table layout — our four sets mirror this |
| `sessions/base_session_service.py:116-167` | Base-class `append_event` (concrete): applies `temp:` to in-memory `session.state`, trims temp from delta, then updates session.state | Our `append_event` calls `super().append_event(...)` first so this happens correctly |
| `sessions/in_memory_session_service.py` | Simplest reference impl — split state, merge on read | Cross-check for the merge-on-read shape |
| `artifacts/in_memory_artifact_service.py:56-91` | Path = `{app}/{user}/user/{filename}` if filename starts with `"user:"`, else `{app}/{user}/{session_id}/{filename}` | Justifies our `"user"` sentinel in the session-slot of the artifact PK |
| `memory/in_memory_memory_service.py` | One stored entry per text-bearing event, keyed by `(app_name, user_id)` | Justifies one `adk_memory` row per text-bearing event |

### Aerospike record ↔ ADK contract map

| Aerospike record | Upstream object | Source citation |
|---|---|---|
| `adk_sessions` session record (bins `app, uid, sid, state, ts, cur`) | `StorageSession` (events live in segment records, not inline) | `schemas/v1.py:72-103` |
| `adk_sessions` segment record (key suffix `:g:08d`, bins `gidx, events (K_ORDERED Map)`) | `StorageEvent` rows — Aerospike-specific append-only segment packing to the write-block-size cap | no upstream parallel — our shape |
| `adk_app_state` row keyed by `"<app>"` | `StorageAppState` — one row per `(app_name)`, prefix stripped | `schemas/v1.py:233-247` + `_session_util.py:44` |
| `adk_user_state` row keyed by `"<app>:<user>"` | `StorageUserState` — one row per `(app_name, user_id)`, prefix stripped | `schemas/v1.py:249-265` + `_session_util.py:46` |
| `temp:*` keys absent from every record | `TEMP_PREFIX` keys are in-process only; never persisted | `state.py:66` + `_session_util.py:48` + `BaseSessionService._trim_temp_delta_state` |
| Inline event Map inside `events` list (bins `eid, ts, author, content, actions, branch`) | `StorageEvent` row fields | `schemas/v1.py:164-191` (we store same fields in a Map element instead of a row) |
| `cur` segment pointer on session + `cur == N` guarded `increment` on rollover | n/a — Aerospike-specific append-target pointer (replaces the dropped `seq` counter) | — |
| `adk_artifacts` PK with `"user"` in the session slot for `user:*` filenames | `InMemoryArtifactService._artifact_path` user-namespace rule | `in_memory_artifact_service.py:56-91` |
| `adk_memory` row per text-bearing event, scoped by `(app, uid)` | `InMemoryMemoryService._session_events[f"{app}/{user}"]` shape | `in_memory_memory_service.py` |
| Posting-list PKs `app:user:kw:token` + memory rows → lexical match | `InMemoryMemoryService.search_memory` word-overlap matching, executed via `batch_read` on posting lists then memory rows | Inverted-index pattern on KV |

### Where we matched exactly vs took an Aerospike-shaped liberty

**Matched contract byte-for-byte:**
- 4-object split (session + app_state + user_state + events) → 4 Aerospike sets
- Prefix routing on write, re-prefix on read (so `get_session` returns identical shape to `DatabaseSessionService`)
- `temp:` dropped at write
- `super().append_event(...)` lets base-class temp handling run first
- Artifact `user:` prefix → sentinel `"user"` in the session-slot (same constraint as upstream: a real session_id of `"user"` would collide — that's an upstream-level constraint we inherit)

**Aerospike primitives instead of SQL primitives — semantically equivalent:**
- Composite SQL PK → `:`-separated string PK (no escaping needed; `:` is invalid in ADK identifiers)
- 1:N FK (events→session) → append-only segment records under the session PK prefix
- ORM ordering by `timestamp` → K_ORDERED map key `"{ts_micros:020d}:{event_id}"` (server-side chronological order; last-N via `map_get_by_index_range`)
- Row-update on state delta → single-RTT atomic `map_put_items` Map CDT (coalesced with the event via `batch_write`)
- `_storage_update_marker` revision check → idempotent `map_put` on a key that is a pure function of the event (retry-safe without a revision marker)
- Client-side text matching → posting-list PKs per query token + `batch_read` on memory rows

### Verify this hierarchy is current

```bash
# Print the four storage tables Google uses — confirms our four sets still mirror upstream
.venv/bin/python -c "
import inspect, re
from google.adk.sessions.schemas import v1
src = inspect.getsource(v1)
for cls in ['StorageSession','StorageEvent','StorageAppState','StorageUserState']:
    print('---', cls, '---')
    m = re.search(rf'class {cls}.*?(?=\nclass |\Z)', src, re.DOTALL)
    print((m.group(0) if m else 'NOT FOUND')[:600])
"

# Print the prefix routing rule
.venv/bin/python -c "import inspect; from google.adk.sessions import _session_util; print(inspect.getsource(_session_util.extract_state_delta))"

# Print the artifact path rule (justifies our 'user' sentinel)
.venv/bin/python -c "import inspect; from google.adk.artifacts.in_memory_artifact_service import InMemoryArtifactService as S; print(inspect.getsource(S._artifact_path))"
```

If any of these change in a future ADK release, our backend needs to follow.

## Data model

See `docs/data-model.md` for the full spec. Quick reference:

Default set prefix `adk_`. Sets:
- `adk_sessions` — key `app:user:session`, bins `app, uid, sid, state (Map), ts, cur`
- `adk_sessions` (also holds event segments) — see "Segment session layout" below
- `adk_app_state` — key `app`, bin `state (Map)`
- `adk_user_state` — key `app:user`, bin `state (Map)`
- `adk_artifacts` — key `app:user:session:fname:ver:08d`, bins `app, uid, sid, aus, fname, ver, mime, data, ctime, cmeta`. For `user:`-prefixed filenames, the session slot is the sentinel `"user"` (matches `InMemoryArtifactService`). `aus = "app:user:sid"` is the composite tenant index bin.
- `adk_memory` — memory row `app:user:session:event_id`; posting row `app:user:kw:token` (bin `mpl`). Search = `batch_read` posting lists per query token, then memory rows. `aus = "app:user:session"` narrows purge.
- `adk_sessions` manifest — key `app:user:sl`, bin `sman` (session id list). `list_sessions(app, user)` uses manifest + bin-projected session reads.

Indexes (auto-created by `_internal/indexes.py` on service init):
- `idx_<prefix>sess_uid`, `idx_<prefix>sess_app` on `sessions`
- `idx_<prefix>art_aus` (composite `app:user:scope`) and `idx_<prefix>art_fname` on `artifacts`
- `idx_<prefix>mem_aus` (composite `app:user:session`, for purge) on `memory`

The composite `aus` indexes on `artifacts` and `memory` are load-bearing for
multi-tenant deployments — they let a tenant-scoped query (e.g. "list
artifacts for app A, user B, session C") return only matching rows in one
sec-index hop, instead of the sec-index-then-Python-filter pattern that
scales linearly in unrelated tenants' rows.

### Segment session layout

The `adk_sessions` set holds **two record kinds** in one set, distinguished
by key shape and bin presence:

**Session record** (small, mutable):
- Key: `app:user:session`
- Bins: `app, uid, sid` (indexed for `list_sessions`), `state (Map)`, `ts`, `cur`
- `cur` is the current (append-target) segment index.

**Segment record** (append-only, packs to ~`max-record-size`):
- Key: `app:user:session:g:NNNNNNNN`
- Bins: `gidx`, `events` — a **K_ORDERED Map** keyed `"{ts_micros:020d}:{event_id}"` → inline event dict.

Segments deliberately omit `app/uid/sid` bins, so they don't appear in the
`idx_sess_uid` / `idx_sess_app` secondary indexes — `list_sessions` queries
return session records only, with no client-side filter step needed.

**Idempotent append.** The map key is a pure function of `(event.id,
event.timestamp)`, so a retried `map_put` overwrites the same slot — an
ambiguous-timeout retry can never duplicate an event. The key also sorts
chronologically, so reads are server-side ordered.

**Rollover (react, don't predict).** No byte estimation, no thresholds, no
flush. Append `map_put`s into segment `cur`; the only overflow signal is a real
`RecordTooBig`. On overflow against a non-empty segment, bump `cur` with a
`cur == N` **guarded `increment`** (concurrent rollovers converge on the same
next index) and retry on the new segment. A `RecordTooBig` against a freshly
empty segment means the lone event exceeds `max-record-size` → raised (O5:
object-store spill is future work).

**Atomicity / 1-RTT hot path.** A no-state-delta append is one atomic
`operate()` (`map_put`) on the segment. An append carrying state is one
`batch_write` coalescing the segment `map_put` with the session/app/user `state`
writes — one RTT regardless of scopes touched (faster than the old up-to-three
sequential operates), with per-record results so a segment `RecordTooBig`
surfaces while sibling state writes still commit. `cur` is cached in-process so
the hot path never reads it; a stale cache self-heals via `RecordTooBig`. No seal
step ⇒ no orphan/"valid iff" invariant. See `docs/tutorials/atomic-session-append.md`.

**Reads:** `get_session` walks segments `cur…0` newest→oldest, using server-side
`map_get_by_index_range(events, -N, N)` for `num_recent_events` and
`map_get_by_key_range` from the `after_timestamp` cutoff; stops once N collected.
`last_update_time` is the newest event's timestamp.

## Aerospike-in-Docker gotchas (learned the hard way; codified in `tests/conftest.py`)

1. **Cluster-tend leaks container IPs.** Server tells clients its container IP (e.g. `172.17.0.2`) → client can't reach it from the host. Fix: custom `aerospike.conf` setting `access-address 127.0.0.1` and `access-port <bound-host-port>`. The host port must be known *before* writing the config, so `_find_free_port()` reserves it first.

2. **Don't mount config at `/etc/aerospike/aerospike.conf`.** The image's entrypoint does shell-style env-var substitution from `aerospike.template.conf` and writes the result to `aerospike.conf`. Mounting at the destination breaks with "read-only filesystem." **Mount at `/etc/aerospike/aerospike.template.conf` instead** — our pre-formatted config has no `${…}` placeholders, so substitution passes through unchanged.

3. **Aerospike 8.x requires `cluster-name`** in `service { … }` (7.x didn't). Image `:latest` is currently 8.1.2.1.

4. **`index_string_create()` is deprecated in Python client 19.x.** Use `index_single_value_create(ns, set, bin, aerospike.INDEX_STRING, name)`. We migrated.

## Running tests

```bash
# All tests — starts Aerospike via testcontainers (~52s)
.venv/bin/pytest

# Unit only (no Docker required; ~2s)
.venv/bin/pytest -m "not aerospike"

# Specific file
.venv/bin/pytest tests/test_sessions.py -v
```

The Aerospike container is `scope="session"` — started once, reused across tests, torn down at end. Image `aerospike/aerospike-server:latest`. Tests confirmed working on macOS arm64.

The venv at `.venv/` already has everything installed (`pip install -e ".[dev]"`).

## Out of scope

- **Not a fork of ADK** — we depend on `google-adk`.
- **Not an MCP tool.** Pinecone/Couchbase/Qdrant/Mongo ship MCP tools; those are LLM-callable, not framework-invoked. We build real `BaseMemoryService`.
- **No HTTP sidecar.** Rejected the `adk-redis` pattern.
- **No vector search.** Aerospike does not have native vector search; lexical memory uses posting-list PKs on core KV.
- **Not on PyPI yet.** Local editable install only.
- **Java port not started in this repo** — separate scaffold at `~/IdeaProjects/GoogleADK`.

## Style guidance

- Follow the patterns already in the repo: `from __future__ import annotations`, `if TYPE_CHECKING:` for type-only imports, `Self` return types on classmethods, `Final` on module constants, `slots=True` on frozen dataclasses.
- Pydantic v2 idioms: `model_dump(mode="json")` / `model_validate(...)`.
- **No comments unless the WHY is non-obvious.** Don't restate what code already says. Don't add planning docs / decision logs unless asked.
- **Specific Aerospike exceptions only** (`aerospike.exception.RecordNotFound`, `RecordExistsError`, `IndexFoundError`). Don't catch generic `Exception`.
- Tests live in `tests/`; mark integration tests with `@pytest.mark.aerospike` so they're excluded from unit runs.

## External references

- ADK Python: https://github.com/google/adk-python
- ADK Java: https://github.com/google/adk-java
- ADK docs: https://adk.dev/
- ADK integration listing guide: https://github.com/google/adk-docs/blob/main/CONTRIBUTING.md#integrations
- Closest competitor (Redis): https://github.com/redis-developer/adk-redis
- Firestore Java pattern to mirror: https://github.com/google/adk-java/pull/601
- Community grab-bag: https://github.com/edu010101/adk-extra-services
