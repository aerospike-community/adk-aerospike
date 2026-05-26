# Aerospike Data Model for ADK

This document fixes the storage layout. Anything that changes here is a breaking
change requiring a major version bump and a migration story.

## Namespace and sets

All data lives in **one Aerospike namespace** chosen by the operator. Within it,
the package uses these sets (default prefix `adk_`, configurable):

| Set            | Primary key                                                | Purpose                                          |
| -------------- | ---------------------------------------------------------- | ------------------------------------------------ |
| `adk_sessions` | `app : user : session` (session record) or `app : user : session : c:NNNNNNNN` (chunk record) | One session record per session; one chunk record per sealed batch of events |
| `adk_app_state`| `app`                                                      | App-scoped state (`app:` prefixed keys)          |
| `adk_user_state`| `app : user`                                           | User-scoped state (`user:` prefixed keys)        |
| `adk_artifacts`| `app : user : session : filename : version:08d` | Versioned binary artifacts                       |
| `adk_memory`   | `app : user : session : event_id`                 | One row per memory entry                         |

`:` is the ASCII unit separator. It is not valid in app/user/session/filename
inputs from ADK, so no escaping is needed.

## Bins

Bin names are kept short (≤14 chars) because Aerospike includes them in every
record. See `adk_aerospike._internal.schema.Bins`.

### `adk_sessions` — session record

| Bin       | Type   | Notes                                                            |
| --------- | ------ | ---------------------------------------------------------------- |
| `app`     | str    | denormalised for index queries                                   |
| `uid`     | str    | denormalised                                                     |
| `sid`     | str    | denormalised                                                     |
| `state`   | Map    | session-scoped state; updated via Map CDT                        |
| `events`  | List   | hot tail of recent events (each item is a Map — see below)       |
| `ts`      | float  | last update time (epoch seconds)                                 |
| `seq`     | int    | monotonic counter — total events ever appended                   |
| `chunks`  | int    | number of sealed chunk records (== next chunk index to write)    |
| `tbytes`  | int    | estimated size of the tail; flush trigger                        |

### `adk_sessions` — chunk record (key suffix `: c:NNNNNNNN`)

| Bin       | Type   | Notes                                                            |
| --------- | ------ | ---------------------------------------------------------------- |
| `cidx`    | int    | chunk index — also serves as discriminator (session has no `cidx`) |
| `events`  | List   | sealed (immutable) batch of events                               |
| `ts_lo`   | float  | timestamp of first event in chunk — `after_timestamp` pruning    |
| `ts_hi`   | float  | timestamp of last event in chunk                                 |

Chunks deliberately **omit `app`/`uid`/`sid` bins** so they are invisible to
the `idx_sess_uid` / `idx_sess_app` secondary indexes — `list_sessions`
returns session records only.

### Event item shape (inside the `events` List)

| Map key   | Type   | Notes                                          |
| --------- | ------ | ---------------------------------------------- |
| `eid`     | str    | `Event.id`                                     |
| `ts`      | float  | event timestamp                                |
| `author`  | str    | agent name / "user"                            |
| `content` | Map    | `genai_types.Content` projected via Pydantic   |
| `actions` | Map    | `EventActions` projected via Pydantic          |
| `branch`  | str    | optional branch label                          |

## Chunking & flush triggers

- **Default flush threshold:** 256 KiB (¼ of Aerospike `write-block-size`).
  When `tbytes >= 256 KiB` after an append, the tail is flushed to a chunk
  record at `c:chunks`, the tail is cleared, and `chunks` is incremented.
- **Huge single event:** if an individual event exceeds 900 KiB, it is
  pre-flushed (current tail sealed first) and then placed in its own fresh
  tail so it doesn't combine with other events into an over-large record.
- **Both thresholds configurable** via `AerospikeSessionService(..., flush_threshold_bytes=..., huge_event_bytes=...)`.

## Atomicity & crash safety

Fast-path append is a **single server-side atomic `operate()`** on the session
record (`list_append + increment(seq) + increment(tbytes) + map_put_items(state) + write(ts)`).
No MRT required.

Flush is two ops: PUT the chunk record (overwriting any orphan from a prior
interrupted flush), then a generation-checked `operate()` to clear the tail
and bump `chunks`. **Invariant:** chunk record `c:N` is *valid* only when
`session.chunks > N`. Any chunk at `cidx >= session.chunks` is an orphan that
readers ignore and that the next successful flush overwrites — no data loss
because the tail still holds the events until the gen-checked reset commits.

## Secondary indexes

Required for the operations below; create on first connect (idempotent).

| Index name             | Set            | Bin     | Type      | Used by              |
| ---------------------- | -------------- | ------- | --------- | -------------------- |
| `idx_<prefix>sess_uid` | `adk_sessions` | `uid`   | string    | `list_sessions(user_id=…)` |
| `idx_<prefix>sess_app` | `adk_sessions` | `app`   | string    | `list_sessions(app_name=…)` |
| `idx_<prefix>art_sid`  | `adk_artifacts`| `sid`   | string    | `list_artifact_keys` |
| `idx_<prefix>art_fname`| `adk_artifacts`| `fname` | string    | `list_versions` / `load_artifact` |
| `idx_<prefix>mem_uid`  | `adk_memory`   | `uid`   | string    | `search_memory` pre-filter |

## State scoping (`app:` / `user:` / `temp:` prefixes)

ADK's `Session.state` is a single dict, but key prefixes route to different
scopes:

- `app:foo` → routed to `adk_app_state` row keyed by `app_name`, bin `state`
- `user:foo` → routed to `adk_user_state` row keyed by `(app_name, user_id)`
- `temp:foo` → never persisted; lives in process memory only
- *(unprefixed)* `foo` → stays in `adk_sessions.state` for this session

When `get_session` rehydrates, it merges all four scopes into one `state` dict.
When `append_event` applies a `state_delta`, the delta is partitioned by prefix
and each piece goes to its corresponding set via a Map CDT operation.

## Memory (core Aerospike, lexical)

Memory lives in a regular Aerospike set. Search is **lexical word-overlap**
— same semantics as ADK's reference `InMemoryMemoryService` — executed
server-side via Aerospike's list-element secondary index. No embeddings,
no embedder dependency.

| Set            | Primary key                                       | Purpose                         |
| -------------- | ------------------------------------------------- | ------------------------------- |
| `adk_memory`   | `app : user : session : event_id`        | One record per memory entry     |

Bins: `app`, `uid`, `sid`, `eid`, `text`, `keywords` (list[str]), `author`,
`ts`, `content` (Map — original event content for reconstruction).

Secondary indexes:
- `idx_<prefix>mem_uid` — scalar string index on `uid`, used by the purge
  step in `add_session_to_memory`.
- `idx_<prefix>mem_kw` — **list-element** index on `keywords`. Queried via
  `predicates.contains(keywords, INDEX_TYPE_LIST, token)` — the canonical
  Aerospike pattern for tag/keyword search.

`search_memory(app_name, user_id, query)`:
1. Tokenizes the query in Python (lowercase `[A-Za-z]+`, dedup).
2. Fires one indexed `predicates.contains` per token, in parallel.
3. Unions matching records client-side, filters by `(app, user)`, dedupes
   by event id.
4. Ranks by token-overlap count, tie-breaks by recency.
5. Returns the top-k as `SearchMemoryResponse`.

For genuine full-text search (stemming, partial match, multi-language,
ranking), Aerospike recommends the
[Elasticsearch connector](https://aerospike.com/blog/build-full-text-search-applications-on-aerospike-using-elasticsearch/) —
out of scope here.

## Record-size considerations

- Aerospike default `write-block-size` is 1 MiB. Sessions with very long state
  Maps may exceed this — keep state small, push large blobs to artifacts.
- Artifacts are capped at the same `write-block-size`. For larger artifacts,
  store an S3/GCS reference instead of inline bytes (planned, not v0.0.1).

## Strong consistency vs AP

Works in both. Because events live inline on the session record (chunked when
large), `append_event` is a single-record atomic `operate()` regardless of
namespace consistency mode — no MRT required for the hot path. Flush is
two ops with optimistic gen-check; the invariant above makes it crash-safe in
either mode.

App/user state updates are independent single-record operates on their own
records; they are not atomic with the session-record append, but ADK's
contract permits this (DatabaseSessionService doesn't make that guarantee
either).
