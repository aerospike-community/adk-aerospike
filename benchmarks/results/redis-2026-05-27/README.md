# Redis benchmark run — 2026-05-27

**Deployment:** Docker `adk-bench-redis` (`redis:7-alpine`), host `127.0.0.1:6379`.

**Backend:** [google-adk-extras](https://pypi.org/project/google-adk-extras/) `RedisSessionService` / `RedisMemoryService` (in-process `redis-py`). Artifacts use a minimal versioned key-value shim in `benchmarks/workloads/_redis_backend.py` (no Redis artifact service in extras).

**Command:**

```bash
python benchmarks/run.py --profile <name> --backend redis --uri "redis://127.0.0.1:6379/1" \
  --results-dir benchmarks/results/redis-2026-05-27
```

| Profile | Status | Notes |
|---|---|---|
| smoke | done | `smoke-redis.txt` |
| sustained | done | `sustained-redis.txt` — heavy worker congestion on get/list |
| agent_turn | done | `agent_turn-redis.txt` |
| memory_mini | done | `memory_mini-redis.txt` |
| memory_heavy | skipped (too slow) | extras does `ZRANGE` entire 50k ZSET per search — re-run with smaller `corpus` |
| chunk_stress | done | `chunk_stress-redis.txt` — one growing session (no chunk records) |
| artifacts | done | `artifacts-redis.txt` — bench KV shim |

## Results summary (p50 / p99 ms)

| Profile | Test | p50 | p99 | failures |
|---|---|---:|---:|---:|
| smoke | session_append | 1 | 2 | 0 |
| smoke | session_get_recent | 1 | 2 | 0 |
| smoke | session_list | 3 | 5 | 0 |
| sustained | session_append | 3 | 2215 | 0 |
| sustained | session_get_recent | 56371 | 111669 | 0 |
| sustained | session_list | 169651 | 347892 | 0 |
| agent_turn | agent_turn | 1963 | 3691 | 0 |
| memory_mini | memory_ingest | 1 | 2 | 0 |
| memory_mini | memory_search | 250 | 654 | 0 |
| chunk_stress | session_append_chunked | 32481 | 154619 | 0 |
| artifacts | artifact_save | 1 | 1 | 0 |
| artifacts | artifact_load | 1 | 1 | 0 |
| artifacts | artifact_list_versions | 1 | 1 | 0 |

Sustained and chunk_stress percentiles are dominated by worker-queue backlog (session JSON grows unbounded; list walks every session key). Treat as comparative signal, not steady-state service time.

Coordinated-omission-safe latencies (scheduled start → completion). Congestion warnings mean queue wait is included in percentiles.
