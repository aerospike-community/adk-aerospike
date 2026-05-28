# Redis vs Aerospike — ecosystem benchmark comparison

**Status:** Local working notes (not for commit).  
**Sources:** Aerospike → `benchmarks/LOCAL_PERFORMANCE.md` (2026-05-27, CE @ `127.0.0.1:3000`, `set_prefix=bench_`). Redis → `benchmarks/results/redis-2026-05-27/` (Redis 7 Alpine, `google-adk-extras` session/memory + artifact bench shim).

Same harness (`benchmarks/run.py`), same profile JSON (QPS, runtime, workload params). Different **implementations**: `adk-aerospike` vs `google-adk-extras` Redis.

---

## Headline

| Theme | Aerospike (`adk-aerospike`) | Redis (`google-adk-extras`) |
|---|---|---|
| Session writes | Hot tail + optional chunk flush; append stays ~1–3 ms under load | Whole `events` JSON in one hash; append OK at low QPS, tail blows up on one long session |
| Session reads | `batch_read` + tail fast path (~2 ms smoke; ~103 ms p50 sustained @ 200 QPS) | Full deserialize every `get_session` → **~56 s p50** sustained |
| Session list | Sec-index on `uid`; heavy under 200 QPS but **~41 s p50** vs Redis **~170 s** | `SMEMBERS` + `HGETALL` per session |
| Lexical memory (500 rows) | **10–12 ms** search (SI or posting lists) | **250 ms** search (`ZRANGE` entire ZSET + Python filter) |
| Lexical memory (50k rows) | SI path **broken** locally (timeouts); posting lists not re-run at 50k | Run **aborted** (full scan per query) |
| Long single session | Chunk stress **~2 ms** p50 (flushes amortized) | **~32 s** p50 (unbounded key growth) |
| Artifacts | Real `AerospikeArtifactService`, sec-index list | Bench KV shim only (~1 ms; not comparable product path) |
| Agent turn @ 8 QPS | **15–30 ms** p50 (when tuned, 0 failures) | **~2 s** p50 |

Both stacks hit **worker congestion** on aggressive profiles (`sustained`, `memory_heavy`); Aerospike stays orders of magnitude faster on read/list/memory where the schema matches agent access patterns.

---

## Profile-by-profile (p50, milliseconds)

### `smoke` — session_hotpath (20 QPS × 5s, 8 sessions)

| Test | Aerospike | Redis |
|---|---:|---:|
| append | 1 | 1 |
| get_recent | 2 | 1 |
| list | 4–6 | 3 |

**Contrast:** Essentially tied at light load. Differences only show up once sessions accumulate history and offered QPS rises.

---

### `sustained` — session_hotpath (200 QPS × 60s, 64 sessions, 400 B events)

| Test | Aerospike p50 | Redis p50 | Aerospike p99 | Redis p99 |
|---|---:|---:|---:|---:|
| append | 1 | 3 | 436 ms | 2,215 ms |
| get_recent | 103 | **56,371** | 3,959 ms | **111,669** |
| list | **41,000** | **169,651** | 79,000 ms | 347,892 ms |

**Contrast:** Neither backend “wins” list at this offered load on a laptop — both are queue-saturated. Aerospike **get_recent** is still ~500× lower p50 than Redis because tail + `batch_read` avoid loading full history. Redis re-reads the entire event JSON on every hydrate.

---

### `agent_turn` (8 QPS × 15s, 32 sessions, 2k memory corpus)

| Backend | p50 | p90 | p99 | Failures |
|---|---:|---:|---:|---|
| Aerospike (posting-list memory) | **15** | 20 | 98 | 0 |
| Aerospike (list-element SI memory) | 30 | 36 | 55 | 0 |
| Redis | **1,963** | 3,020 | 3,691 | 0 |

**Contrast:** Composite turn is dominated by memory search + session read. Aerospike’s ~10–12 ms search + ~2 ms get fits in tens of ms; Redis’s session + ZSET scan lands in **seconds**.

---

### `memory_mini` (500 corpus, 20 QPS × 15s)

| Test | Aerospike (posting lists) | Aerospike (SI) | Redis |
|---|---:|---:|---:|
| ingest | 5 | 3 | 1 |
| search | **12** | **10** | **250** |

**Contrast:** Ingest is cheap on both. Search diverges: **server-side index / posting-list PK** vs **client-side full ZSET scan**. ~20× gap at 500 rows; gap grows linearly with corpus on Redis, sublinear on Aerospike.

---

### `memory_heavy` (50k corpus, 30 QPS × 60s)

| Backend | Outcome |
|---|---|
| Aerospike (SI) | Completed but **invalid**: search p50 **476 s**, 1729 failures (1s client timeout) |
| Redis | **Aborted** after preload; each search O(50k) in process |

**Contrast:** Profile defaults are too aggressive for **both** local lexical implementations at 50k without cluster scale or algorithm change (Aerospike posting lists at 50k not recorded here). Not a fair “Redis vs Aerospike” row until both are re-run with tuned QPS or indexing.

---

### `chunk_stress` (100 QPS × 30s, one session, 600 B events)

| Backend | p50 | p99 | Semantics |
|---|---:|---:|---|
| Aerospike | **2** | 2–3 | Low `flush_threshold` → real chunk records; append stays flat |
| Redis | **32,481** | 154,619 | No chunks; single hash grows without bound |

**Contrast:** This profile is **designed for Aerospike’s write-block-size layout**. On Redis it measures “one ever-growing document,” not an equivalent feature.

---

### `artifacts` (80 QPS × 30s, 4 KiB payload)

| Test | Aerospike | Redis (bench shim) |
|---|---:|---:|
| save | 1 | 1 |
| load | 3 | 1 |
| list_versions | 3 | 1 |

**Contrast:** Latencies are similar at this payload size, but **implementations differ**: Aerospike uses versioned records + `aus` sec-index; Redis shim uses `INCR` + `SET`. Aerospike without atomic versioning had **1328 failures** and multi-second tails (see LOCAL_PERFORMANCE) — correctness matters more than raw ms.

---

## Why the gap (design, not “Redis slow”)

```text
Session read (get_recent):
  Aerospike:  GET session tail (+ optional chunk walk) + batch_read app/user state
  Redis:      HGETALL session hash → json.loads(entire events[]) → slice last N

Memory search:
  Aerospike:  predicates.contains on keywords list OR posting-list PK per token
  Redis:      ZRANGE entire user ZSET → json.loads each → Python term match

Long conversation:
  Aerospike:  flush tail to immutable ~256 KiB chunks; hot record stays small
  Redis:      same key, monotonically larger JSON string
```

---

## When each result set is fair to cite

| Claim | Fair? |
|---|---|
| “At smoke-level load, session ops are similar” | Yes |
| “Aerospike sustains tail reads under agent-shaped schema; extras Redis does not” | Yes for this integration |
| “Redis artifact bench = Aerospike artifact service” | **No** — shim vs production service |
| “chunk_stress proves Redis is slower” | **No** — different feature |
| “memory_heavy shows Aerospike wins” | **No** — both failed or need retune at 50k |

---

## Suggested paired re-run

To refresh both columns in one session:

```bash
python benchmarks/run.py --profile <name> --backend aerospike \
  --uri "aerospike://127.0.0.1:3000/test?set_prefix=bench_" \
  --results-dir benchmarks/results/aerospike-2026-05-27

python benchmarks/run.py --profile <name> --backend redis \
  --uri "redis://127.0.0.1:6379/1" \
  --results-dir benchmarks/results/redis-2026-05-27
```

Use the same posting-list memory build on Aerospike when comparing to current `feat/memory-posting-lists` branch.
