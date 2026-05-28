# Redis backend performance report (local benchmark)

**Status:** Internal / working notes — not intended for commit or publication.  
**Date:** 2026-05-27  
**Author:** Generated from ecosystem benchmark harness run against local Redis.  
**Raw transcripts:** `*-redis.txt` in this directory.

---

## Executive summary

We ran the adk-aerospike **ecosystem benchmark suite** (`benchmarks/run.py`) against a local **Redis 7** instance, using **google-adk-extras** for session and memory (in-process `redis-py`) and a minimal bench-only artifact store. Six of seven profiles completed; **`memory_heavy` was aborted** after preload because each search scans the full in-memory corpus (50k ZSET members) client-side.

At low load (**smoke**), Redis-backed session ops look fine (low single-digit ms). Under sustained agent-shaped load, **session read and list paths collapse**: whole-session JSON is loaded and deserialized on every `get_session`, and `list_sessions` walks every session key for the user. That architectural mismatch dominates the numbers below—not Redis wire latency alone.

**No Aerospike numbers were collected in the same run**; this document is Redis-only. Use it as a baseline for a future paired run with `--backend aerospike` and the same profiles.

---

## Test environment

| Item | Value |
|---|---|
| Host OS | Linux 6.2.6 (Pop!\_OS / Ubuntu family) |
| Redis image | `redis:7-alpine` |
| Container name | `adk-bench-redis` |
| Endpoint | `redis://127.0.0.1:6379` |
| DB index | `1` for most profiles; `2` for `chunk_stress` and `artifacts` (isolation from in-flight `memory_heavy` preload) |
| Python | 3.12 (project `.venv`) |
| Harness | [ai-ecosystem-benchmark](https://github.com/aerospike-community/ai-ecosystem-benchmark) (git install via `[benchmark]` extra) |
| ADK Redis integration | `google-adk-extras` ≥ 0.3 — `RedisSessionService`, `RedisMemoryService` |
| Artifacts | Bench shim `RedisArtifactBenchStore` in `benchmarks/workloads/_redis_backend.py` (version counter + `SET` per version; not a production ADK artifact service) |

**Not used:** `adk-redis` (Redis Inc.) — that stack targets the Agent Memory Server HTTP sidecar and semantic memory; it is not wire-compatible with these workloads.

---

## Methodology

### Harness behavior

- **Fixed QPS** per profile; scheduler threads pace calls with nanosecond deadlines.
- **Coordinated-omission correction:** latency is measured from *scheduled* start to completion, so worker-pool queueing is included in percentiles (appropriate when the goal is “did the system keep up at this offered load?”).
- **Worker pool** executes sync benchmark methods; ADK async services run via per-thread event loops (`benchmarks/workloads/_async_bridge.py`).
- Metrics reported: **p50 / p90 / p99** (milliseconds), call count, failure count.

### Congestion warnings

The runner emits warnings when **dispatch lag** (time from scheduled start until a worker picks up the call) exceeds a threshold. Several profiles tripped this—especially `sustained` (get/list) and `chunk_stress`. When congestion fires, **p50–p99 reflect backlog + service time**, not service time alone. Interpret sustained and chunk_stress session numbers accordingly.

### Workload mapping (Redis)

| Profile | Workload | Redis tests | Notes |
|---|---|---|---|
| smoke | session_hotpath | append, get_recent, list | 8 sessions, 200 B events |
| sustained | session_hotpath | append, get_recent, list | 64 sessions, 400 B events, 200 QPS |
| agent_turn | agent_turn | composite turn | append → get_recent → memory search |
| memory_mini | memory_lexical | ingest, search | 500-entry corpus |
| memory_heavy | memory_lexical | ingest, search | **Not completed** — 50k corpus |
| chunk_stress | chunk_stress | append to one session | Aerospike chunk flush **not modeled** on Redis |
| artifacts | artifacts | save, load, list_versions | Bench KV shim only |

---

## Results by profile

### smoke (`session_hotpath`)

**Config:** 20 QPS × 5 s per test → 100 calls each; 1 scheduler, 4 workers; 8 sessions, 200 B events.

| Test | Calls | Failures | p50 | p90 | p99 |
|---|---:|---:|---:|---:|---:|
| redis_session_append | 100 | 0 | 1 ms | 2 ms | 2 ms |
| redis_session_get_recent | 100 | 0 | 1 ms | 2 ms | 2 ms |
| redis_session_list | 100 | 0 | 3 ms | 3 ms | 5 ms |

**Takeaway:** Healthy at light load with a small session count and short history.

---

### sustained (`session_hotpath`)

**Config:** 200 QPS × 60 s per test → 12,000 calls each; 4 schedulers, 32 workers; 64 sessions, 400 B events, `num_recent_events=20`.

| Test | Calls | Failures | p50 | p90 | p99 |
|---|---:|---:|---:|---:|---:|
| redis_session_append | 12,000 | 0 | 3 ms | 1,242 ms | 2,215 ms |
| redis_session_get_recent | 12,000 | 0 | 56,371 ms | 102,005 ms | 111,669 ms |
| redis_session_list | 12,000 | 0 | 169,651 ms | 304,943 ms | 347,892 ms |

**Congestion:** Warnings on all three tests (dispatch lag up to ~618 ms on list).

**Takeaway:** Append mostly keeps up (p50 still low; tail from queueing). **Get recent** and **list** do not: extras loads the full `events` JSON blob from a Redis hash and trims client-side; list additionally `HGETALL`s every session for the user. At 200 QPS offered, the worker pool falls minutes behind—percentiles are end-to-end backlog, not Redis RTT.

---

### agent_turn (`agent_turn`)

**Config:** 8 QPS × 15 s → 120 calls; 1 scheduler, 32 workers; 32 sessions; 2,000-entry memory preload; 400 B events; 20 recent events; 3 query tokens.

| Test | Calls | Failures | p50 | p90 | p99 |
|---|---:|---:|---:|---:|---:|
| redis_agent_turn | 120 | 0 | 1,963 ms | 3,020 ms | 3,691 ms |

**Congestion:** Warning (dispatch lag ~135 ms).

**Takeaway:** ~2 s median per composite turn (append + hydrate + lexical search over 2k entries). Dominated by session + memory paths above, not artifact I/O.

---

### memory_mini (`memory_lexical`)

**Config:** 20 QPS × 15 s → 300 calls per test; 1 scheduler, 8 workers; corpus 500; 3 query tokens.

| Test | Calls | Failures | p50 | p90 | p99 |
|---|---:|---:|---:|---:|---:|
| redis_memory_ingest | 300 | 0 | 1 ms | 1 ms | 2 ms |
| redis_memory_search | 300 | 0 | 250 ms | 554 ms | 654 ms |

**Congestion:** Warning on search.

**Takeaway:** Ingest is cheap (`ZADD` one entry). Search does `ZRANGE` on the entire user ZSET (500 JSON members) and filters terms in Python—acceptable for hundreds of entries, does not scale linearly to tens of thousands.

---

### memory_heavy (`memory_lexical`) — **not completed**

**Config (planned):** 30 QPS × 60 s; corpus **50,000**; 2 schedulers, 16 workers.

**What happened:** Preload of 50k events into `memory:bench_eco_memory:u0` succeeded (~51.8k ZSET members observed via `ZCARD`). The search phase began; first congestion warning showed **~50 s dispatch lag**. The run was **terminated** before producing `memory_heavy-redis.txt`—estimated wall time would be hours at offered QPS because each search is O(corpus) client-side.

**Recommendation:** Re-run with reduced `corpus` (e.g. 5k–10k), lower QPS, or swap to a RediSearch/indexed implementation before comparing to Aerospike `memory_heavy`.

---

### chunk_stress (`chunk_stress`)

**Config:** 100 QPS × 30 s → 3,000 calls; 1 scheduler, 8 workers; single session; 600 B events. (Aerospike profile uses low flush threshold to force chunk records; **Redis has no equivalent**—this is sustained append to one growing hash.)

| Test | Calls | Failures | p50 | p90 | p99 |
|---|---:|---:|---:|---:|---:|
| redis_session_append_chunked | 3,000 | 0 | 32,481 ms | 125,628 ms | 154,619 ms |

**Congestion:** Warning (dispatch lag ~122 ms early; grows with session size).

**Takeaway:** Not comparable to Aerospike chunk flush semantics. Models “one long conversation” as a single Redis key whose value grows without bound—append cost and hydration cost rise together; percentiles reflect severe backlog.

---

### artifacts (`artifacts`)

**Config:** 80 QPS × 30 s → 2,400 calls per test; 2 schedulers, 16 workers; ~4 KiB JSON payload.

| Test | Calls | Failures | p50 | p90 | p99 |
|---|---:|---:|---:|---:|---:|
| redis_artifact_save | 2,400 | 0 | 1 ms | 1 ms | 1 ms |
| redis_artifact_load | 2,400 | 0 | 1 ms | 1 ms | 1 ms |
| redis_artifact_list_versions | 2,400 | 0 | 1 ms | 1 ms | 1 ms |

**Takeaway:** The bench shim (INCR + SET per version) is fast and **not representative** of ADK artifact services on Redis (extras has no Redis artifact backend; production would use S3/SQL/local). Useful only as “KV overhead floor,” not product comparison.

---

## Architectural interpretation (why the numbers look this way)

### Session service (google-adk-extras)

- **Storage model:** One Redis hash per session; `events` stored as a **single JSON string** of the full list.
- **append_event:** Read-modify-write on that blob (`HGET` + deserialize + append + serialize + `HSET`).
- **get_session with `num_recent_events`:** Still loads and deserializes **all** events, then slices in Python.
- **list_sessions:** `SMEMBERS` session IDs, then **`HGETALL` per session** to build metadata.

This is the opposite of adk-aerospike’s hot-tail + chunk design (bounded session record, server-side tail read, sec-index list without loading event bodies). Under multi-session sustained load, Redis session **read/list** percentiles in this report are expected to diverge sharply from Aerospike—not because Redis is slow, but because the **integration shape** is O(session history) per read.

### Memory service (google-adk-extras)

- **Storage model:** One sorted set per `(app, user)`; each member is a JSON memory entry.
- **search_memory:** `ZRANGE 0 -1` (full scan) + Python token overlap filter.

Comparable in *intent* to Aerospike lexical memory (keyword overlap), but not in *execution*: Aerospike uses list-element secondary indexes and server-side `predicates.contains` per token; extras Redis path is in-process filter over the full corpus.

### Artifacts

- Bench-only keys `bench_art:…` — not ADK contract. Do not compare to `AerospikeArtifactService` sec-index list versions in a marketing sense.

---

## Limitations and caveats

1. **Single node, local Docker, no TLS, no replication** — not production topology.
2. **One integration path** — google-adk-extras community Redis, not `adk-redis` vendor stack.
3. **No paired Aerospike run** in this session — cross-backend slides need a second pass with identical profiles and `--backend aerospike`.
4. **Congestion-saturated profiles** — sustained/chunk_stress p50+ are system backlog metrics at offered load, not intrinsic Redis op latency.
5. **memory_heavy missing** — cannot draw conclusions at 50k corpus for this backend without redesign or smaller corpus.
6. **chunk_stress name is misleading on Redis** — no chunk records; rename or skip when publishing comparisons.

---

## Suggested follow-ups

| Action | Purpose |
|---|---|
| Run same profiles with `--backend aerospike` and identical QPS | Apples-to-apples comparison table |
| Add `memory_heavy` at 5k corpus or cap runtime | Finish memory scaling curve |
| Capture `redis-cli INFO stats`, `LATENCY DOCTOR`, and container CPU during sustained | Separate wire/server vs client CPU |
| If comparing to vendor Redis ADK, stand up Agent Memory Server + `adk-redis` | Different architecture; label separately |
| Consider RediSearch / per-token indexes for extras memory | Fairer lexical search comparison |

---

## How to reproduce

```bash
# Redis
docker run -d --name adk-bench-redis -p 6379:6379 redis:7-alpine

# Deps
pip install -e ".[dev,benchmark]"

# Example
python benchmarks/run.py --profile smoke --backend redis \
  --uri "redis://127.0.0.1:6379/1" \
  --results-dir benchmarks/results/redis-2026-05-27
```

List profiles: `python benchmarks/run.py --list-profiles`

---

## Appendix: consolidated metrics (p50 / p90 / p99, ms)

| Profile | Test | p50 | p90 | p99 | Failures |
|---|---|---:|---:|---:|---:|
| smoke | session_append | 1 | 2 | 2 | 0 |
| smoke | session_get_recent | 1 | 2 | 2 | 0 |
| smoke | session_list | 3 | 3 | 5 | 0 |
| sustained | session_append | 3 | 1,242 | 2,215 | 0 |
| sustained | session_get_recent | 56,371 | 102,005 | 111,669 | 0 |
| sustained | session_list | 169,651 | 304,943 | 347,892 | 0 |
| agent_turn | agent_turn | 1,963 | 3,020 | 3,691 | 0 |
| memory_mini | memory_ingest | 1 | 1 | 2 | 0 |
| memory_mini | memory_search | 250 | 554 | 654 | 0 |
| chunk_stress | session_append_chunked | 32,481 | 125,628 | 154,619 | 0 |
| artifacts | artifact_save | 1 | 1 | 1 | 0 |
| artifacts | artifact_load | 1 | 1 | 1 | 0 |
| artifacts | artifact_list_versions | 1 | 1 | 1 | 0 |
| memory_heavy | — | — | — | — | run aborted |

---

*End of report.*
