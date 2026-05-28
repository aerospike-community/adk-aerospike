# Paired benchmark: Aerospike vs Redis (identical load)

**Status:** Local working notes — not for commit.  
**Date:** 2026-05-27  
**Harness:** `benchmarks/run.py` + [ai-ecosystem-benchmark](https://github.com/aerospike-community/ai-ecosystem-benchmark)  
**Raw output:** `paired_*-{aerospike,redis}.txt`, `run.log` in this directory.

## What “apples to apples” means here

Both backends received the **same offered load** from four shared profiles in `benchmarks/profiles/paired_*.json`:


| Knob                                       | Held constant across backends                                  |
| ------------------------------------------ | -------------------------------------------------------------- |
| QPS, runtime, scheduler/worker counts      | Yes — per profile file                                         |
| Workload type and `workload_params`        | Yes                                                            |
| Event sizes, session counts, memory corpus | Yes                                                            |
| Latency methodology                        | Yes — scheduled-start → completion (coordinated-omission safe) |


Implementations differ (as in any integration benchmark):


|          | Aerospike                                           | Redis                                                             |
| -------- | --------------------------------------------------- | ----------------------------------------------------------------- |
| Package  | `adk-aerospike`                                     | `google-adk-extras` (`RedisSessionService`, `RedisMemoryService`) |
| Endpoint | `aerospike://127.0.0.1:3000/test?set_prefix=bench_` | `redis://127.0.0.1:6379/10`                                       |
| Host     | Local CE (Docker, port 3000)                        | `redis:7-alpine` (`adk-bench-redis`)                              |


**Excluded:** `chunk_stress` (Aerospike chunk flushes have no Redis equivalent), `artifacts` (Redis used a bench KV shim in the earlier run), `memory_heavy` @ 50k (both backends fail or abort at that default).

---

## Profiles (identical load)

### `paired_smoke` — session_hotpath

20 QPS × 5 s → **100 calls** per test; 1 scheduler, 4 workers; **8** sessions, **200 B** events, 10 recent events.

### `paired_session` — session_hotpath

50 QPS × 30 s → **1,500 calls** per test; 2 schedulers, 16 workers; **32** sessions, **400 B** events, 20 recent events.

### `paired_memory` — memory_lexical

15 QPS × 20 s → **300 calls** per test; 1 scheduler, 8 workers; **2,000**-entry preload, **3** query tokens.

### `paired_agent_turn` — composite turn

8 QPS × 15 s → **120 calls**; 1 scheduler, 32 workers; 32 sessions, 400 B events, 2k memory preload, 3 query tokens.  
One call = `append_event` + `get_session(recent)` + `search_memory`.

---

## Results

### `paired_smoke`


| Test               | Aerospike p50 | Redis p50 | Aerospike p99 | Redis p99 | Failures (AS / R) |
| ------------------ | ------------- | --------- | ------------- | --------- | ----------------- |
| session_append     | 1 ms          | 1 ms      | 1 ms          | 2 ms      | 0 / 0             |
| session_get_recent | 1 ms          | 1 ms      | 2 ms          | 2 ms      | 0 / 0             |
| session_list       | 4 ms          | 2 ms      | 5 ms          | 3 ms      | 0 / 0             |


**At light load, parity.** Neither integration is stressed.

---

### `paired_session` (50 QPS, 32 sessions)


| Test               | Aerospike p50 | Redis p50 | AS p90 | R p90 | AS p99 | R p99 | Failures |
| ------------------ | ------------- | --------- | ------ | ----- | ------ | ----- | -------- |
| session_append     | 1 ms          | 2 ms      | 1 ms   | 2 ms  | 2 ms   | 3 ms  | 0 / 0    |
| session_get_recent | 2 ms          | 3 ms      | 3 ms   | 3 ms  | 5 ms   | 7 ms  | 0 / 0    |
| session_list       | **2 ms** ‡    | **8 ms**  | 2 ms   | 10 ms | 3 ms   | 11 ms | 0 / 0    |


‡ **2026-05-27** bin-projected list: manifest `GET` + `batch_write` read of `app/uid/sid/ts` only. Was **8,053 ms** p50 with full `batch_read`; **6,308 ms** before manifest.

**Append and get_recent:** Both keep up at this offered load; latencies stay in the low ms.

**session_list:** With projection, Aerospike ~**2 ms** p50 at 50 QPS (parity with Redis ~8 ms). Prior slowness was pulling full `events` tails on every list, not the manifest key design.

---

### `paired_memory` (2k corpus)


| Test          | Aerospike p50 | Redis p50    | AS p90 | R p90     | AS p99 | R p99     | Failures |
| ------------- | ------------- | ------------ | ------ | --------- | ------ | --------- | -------- |
| memory_ingest | 7 ms          | 1 ms         | 7 ms   | 1 ms      | 8 ms   | 1 ms      | 0 / 0    |
| memory_search | **12 ms**     | **8,858 ms** | 15 ms  | 15,703 ms | 64 ms  | 17,046 ms | 0 / 0    |


**Largest integration gap in the paired suite.** Same 15 QPS offered to search; Aerospike posting-list / index path ~~**12 ms** p50; Redis full `**ZRANGE` + Python filter** ~**8.9 s** p50 (~~**740×**). Ingest is fast on both; search algorithm dominates.

---

### `paired_agent_turn` (8 QPS, 2k memory)


| Backend   | p50        | p90    | p99    | Failures |
| --------- | ---------- | ------ | ------ | -------- |
| Aerospike | **14 ms**  | 17 ms  | 104 ms | 0        |
| Redis     | **112 ms** | 174 ms | 306 ms | 0        |


**~8×** p50 gap with **identical** turn shape and preload. Rough budget on Aerospike: ~1 ms append + ~2 ms get_recent + ~12 ms search ≈ 15 ms. Redis: low session cost at this QPS but search backlog inflates the composite (congestion warning on turn).

---

## Summary table (p50 only)


| Profile           | Test       | Aerospike | Redis    | Ratio (R / AS) |
| ----------------- | ---------- | --------- | -------- | -------------- |
| paired_smoke      | append     | 1 ms      | 1 ms     | 1×             |
| paired_smoke      | get_recent | 1 ms      | 1 ms     | 1×             |
| paired_smoke      | list       | 4 ms      | 2 ms     | 0.5×           |
| paired_session    | append     | 1 ms      | 2 ms     | 2×             |
| paired_session    | get_recent | 2 ms      | 3 ms     | 1.5×           |
| paired_session    | list       | 2 ms ‡    | 8 ms     | 4×             |
| paired_memory     | ingest     | 7 ms      | 1 ms     | 0.1×           |
| paired_memory     | search     | 12 ms     | 8,858 ms | **738×**       |
| paired_agent_turn | turn       | 14 ms     | 112 ms   | **8×**         |


‡ Aerospike list after bin-projected manifest reads (`_batch_read_bins`).

---

## Aerospike re-runs (post manifest + bin-projected list + posting lists)

Same profiles on local CE after implementation fixes (`benchmarks/LOCAL_PERFORMANCE.md`):


| Profile               | Test                       | p50                        |
| --------------------- | -------------------------- | -------------------------- |
| `sustained` @ 200 QPS | append / get_recent / list | 1 / 2 / **2 ms**           |
| `memory_heavy` @ 50k  | ingest / search            | 7 / **12 ms** (0 failures) |


Prior `sustained` list p50 **41 s** and `memory_heavy` SI search **476 s** are obsolete.

## Conclusions (under identical offered load)

1. **Session hot path** — At paired **50 QPS**, Aerospike append/get/list are low-ms with manifest + projected list. Redis comparable on list at that load.
2. **Lexical memory** — Largest **cross-backend** gap: Aerospike ~12 ms search @ 2k–50k; Redis extras O(corpus) client scan.
3. **Agent turn** — Aerospike ~14 ms vs Redis ~112 ms p50 @ 8 QPS (same preload).
4. **Fair scope** — **adk-aerospike** vs **google-adk-extras Redis**, not bare databases.

---

## Reproduce

```bash
pip install -e ".[dev,benchmark]"
docker start adk-bench-redis   # redis://127.0.0.1:6379

OUT=benchmarks/results/paired-2026-05-27
mkdir -p "$OUT"
for p in paired_smoke paired_session paired_memory paired_agent_turn; do
  python benchmarks/run.py --profile "$p" --backend aerospike \
    --uri "aerospike://127.0.0.1:3000/test?set_prefix=bench_" \
    --results-dir "$OUT"
  python benchmarks/run.py --profile "$p" --backend redis \
    --uri "redis://127.0.0.1:6379/10" --results-dir "$OUT"
done
```

Profiles: `benchmarks/profiles/paired_*.json`