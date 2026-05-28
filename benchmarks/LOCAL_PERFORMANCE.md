# Local benchmark results (collected 2026-05-27)

Ad-hoc numbers from development runs on a **single-node Aerospike CE** instance at
`aerospike://127.0.0.1:3000/test?set_prefix=bench_`. **Not** production capacity
planning — use for regression comparison and design validation only.

## Environment


| Item          | Value                                                                                                                  |
| ------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Host          | Linux workstation, local Docker / `127.0.0.1:3000`                                                                     |
| Namespace     | `test`                                                                                                                 |
| Set prefix    | `bench_`                                                                                                               |
| Harness       | [ai-ecosystem-benchmark](https://github.com/aerospike-community/ai-ecosystem-benchmark) via `python benchmarks/run.py` |
| Micro harness | `python benchmarks/benchmark.py` (asyncio, client-side percentiles)                                                    |


Unless noted, runs used `pip install -e ".[dev,benchmark]"` and **0 failures**.

---

## Ecosystem profiles (`run.py`)

### `smoke` — `session_hotpath`

**Profile:** 20 QPS × 5s, 4 workers, 8 sessions, 200 B events.


| Test                 | Calls | p50    | p90     | p99     | Failures | Notes                                    |
| -------------------- | ----- | ------ | ------- | ------- | -------- | ---------------------------------------- |
| `session_append`     | 100   | 1 ms   | 1 ms    | 2–3 ms  | 0        | Single-record `operate`                  |
| `session_get_recent` | 100   | 2 ms   | 2–3 ms  | 2–3 ms  | 0        | `batch_read` × 3                         |
| `session_list`       | 100   | 4–6 ms | 5–24 ms | 6–25 ms | 0        | Sec-index on `uid`; p90 spike on one run |


**Branches:** `feat/ai-ecosystem-benchmarks`, `feat/memory-posting-lists` (session path unchanged).

---

### `chunk_stress` — forced flushes

**Profile:** 100 QPS × 30s, 8 workers, **one session**, 600 B events, `flush_threshold_bytes=200`.


| Test                     | Calls | p50  | p90  | p99    | Failures |
| ------------------------ | ----- | ---- | ---- | ------ | -------- |
| `session_append_chunked` | 3000  | 2 ms | 2 ms | 2–3 ms | 0        |


**Branch:** `feat/ai-ecosystem-benchmarks`.

---

### `artifacts` — 4 KiB inline JSON

**Profile:** 80 QPS × 30s, 16 workers, one filename (contention by design).


| Test                     | Calls | p50  | p90     | p99     | Failures | Implementation                                                          |
| ------------------------ | ----- | ---- | ------- | ------- | -------- | ----------------------------------------------------------------------- |
| `artifact_list_versions` | 2400  | 3 ms | 3 ms    | 3–4 ms  | 0        | `aus` query + filter                                                    |
| `artifact_load`          | 2400  | 3 ms | 3–4 ms  | 4 ms    | 0        | Point `GET`                                                             |
| `artifact_save`          | 2400  | 1 ms | 1 ms    | 1–2 ms  | 0        | **With** atomic `__head__` version (`fix/artifact-atomic-version` / #9) |
| `artifact_save`          | 2400  | 8 ms | 1829 ms | 6174 ms | **1328** | **Without** atomic version (read-then-put race)                         |


**Branch:** `feat/ai-ecosystem-benchmarks` + cherry-picked artifact fix for clean save row.

---

### `memory_mini` — lexical memory (500-row corpus)

**Profile:** 20 QPS × 15s, 8 workers, corpus 500, 3 query tokens.

#### List-element secondary index (`mem_kw`) — before posting lists


| Test            | Calls | p50   | p90 | p99 | Failures |
| --------------- | ----- | ----- | --- | --- | -------- |
| `memory_ingest` | 300   | 3 ms  | —   | —   | 0        |
| `memory_search` | 300   | 10 ms | —   | —   | 0        |


**Branch:** `feat/ai-ecosystem-benchmarks` (main memory service).

#### Posting-list PKs (`app:user:kw:token`)


| Test            | Calls | p50   | p90   | p99   | Failures |
| --------------- | ----- | ----- | ----- | ----- | -------- |
| `memory_ingest` | 300   | 5 ms  | 5 ms  | 6 ms  | 0        |
| `memory_search` | 300   | 12 ms | 16 ms | 82 ms | 0        |


**Branch:** `feat/memory-posting-lists` (#12).

---

### `agent_turn` — composite (append + get_recent + search)

**Profile:** see table below; 32 sessions round-robin, 400 B events, 2k memory corpus preload, 3 query tokens.


| Config                                                  | Calls | p50       | p90   | p99   | Failures | Notes                                           |
| ------------------------------------------------------- | ----- | --------- | ----- | ----- | -------- | ----------------------------------------------- |
| 50 QPS, 16 workers, 30s                                 | 300   | —         | —     | —     | **~50%** | Worker congestion + 1s client timeouts          |
| 15 QPS, 32 workers, 20s                                 | 300   | ~37 s     | ~74 s | ~77 s | **~50%** | Overload artifacts (queue + timeout)            |
| **8 QPS**, 1 sched, 32 workers, 15s + **SI search**     | 120   | 30 ms     | 36 ms | 55 ms | 0        | Tuned profile on `feat/ai-ecosystem-benchmarks` |
| **8 QPS**, 1 sched, 32 workers, 15s + **posting lists** | 120   | **15 ms** | 20 ms | 98 ms | 0        | `feat/memory-posting-lists`                     |


Per-op rough budget (SI path, 8 QPS): append ~1 ms + get_recent ~2 ms + search ~10 ms + async overhead ≈ 30 ms composite.

---

### `memory_heavy` — posting-list memory @ 50k corpus

**Profile:** 30 QPS × 60s, 16 workers, **50k** corpus, 4 query tokens.

| Test | Calls | p50 | p90 | p99 | Failures | Notes |
|---|---:|---:|---:|---:|---|
| `memory_ingest` | 1800 | 7 ms | 7 ms | 9 ms | 0 | Posting-list path (`feat/memory-posting-lists`) |
| `memory_search` | 1800 | 12 ms | 16 ms | 19 ms | 0 | Same |

**Superseded (list-element SI, do not cite):** search p50 476 s, 1729 failures @ same profile.

---

### `sustained` — session hot path @ 200 QPS

**Profile:** 200 QPS × 60s, 32 workers, 64 sessions, 400 B events.

| Test | Calls | p50 | p90 | p99 | Failures | Notes |
|---|---:|---:|---:|---:|---|
| `session_append` | 12000 | 1 ms | 1 ms | 8 ms | 0 | |
| `session_get_recent` | 12000 | 2 ms | 3 ms | 3 ms | 0 | Tail + `batch_read` |
| `session_list` | 12000 | 2 ms | 3 ms | 3 ms | 0 | Manifest + bin-projected list |

**Superseded (full-record list + pre-manifest):** get_recent p50 103 ms; list p50 41 s @ same profile on local CE.

---

## Micro harness (`benchmark.py`)

Reference numbers from `benchmarks/README.md` (MacBook, single-node CE, **not** re-verified in the 2026-05-27 session):


| Scenario                                 | ops/s | p50      | p99      | What it validates                    |
| ---------------------------------------- | ----- | -------- | -------- | ------------------------------------ |
| `append` (200 B, conc=16)                | 5670  | 2.10 ms  | 3.77 ms  | Single-record atomic append          |
| `chunking` (500×600 B)                   | 1869  | 0.47 ms  | 1.01 ms  | Flush amortization                   |
| `get_session` tail (50 chunks, recent=5) | 1729  | 8.67 ms  | 11.84 ms | Fast path vs chunks                  |
| `get_session` full history (50 chunks)   | 43    | 330 ms   | 683 ms   | Chunk walk cost                      |
| `search_memory` (corpus=500, 3 tokens)   | 191   | 37.97 ms | 83.05 ms | SI lexical search (pre-posting-list) |


Reproduce:

```bash
python benchmarks/benchmark.py all --uri "aerospike://127.0.0.1:3000/test?set_prefix=bench_"
```

---

## Summary takeaways


| Area                                            | Local CE verdict                                                                         |
| ----------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Session append / tail read                      | Sub-ms to low ms at modest QPS; matches design                                           |
| Chunking under artificial flush rate            | Flat ~2 ms appends                                                                       |
| Artifacts                                       | Fast with atomic versioning; broken under concurrent save without #9                     |
| Memory search                                   | Posting lists ~12 ms p50 @ 2k–50k corpus on local CE; SI path invalid at 50k              |
| Agent turn                                      | ~15 ms @ 8 QPS when not overloaded                                                       |
| `sustained` @ 200 QPS                           | ~1–3 ms p50 append/get/list with manifest + projected list (local CE, 2026-05-27 re-run)   |


---

## How to reproduce a row

```bash
pip install -e ".[dev,benchmark]"
python benchmarks/run.py --profile <name>
# optional: --uri "aerospike://127.0.0.1:3000/test?set_prefix=bench_"
```

Update this file when re-running after meaningful schema or profile changes.