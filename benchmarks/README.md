# Benchmarks

Two harnesses, one goal: measure `adk-aerospike` under realistic agent-shaped load.

| Harness | Runner | Use when |
|---|---|---|
| **Ecosystem** (`run.py`) | [ai-ecosystem-benchmark](https://github.com/aerospike-community/ai-ecosystem-benchmark) | Fixed QPS, coordinated-omission-safe latency, comparable across Aerospike / Postgres / Redis backends |
| **Micro** (`benchmark.py`) | Standalone asyncio | Falsify a single design claim (append atomicity, chunk flush, tail read, sec-index search) |

Long term, workloads should ship as a pip extra on this repo. Today the ecosystem framework is **not on PyPI** — install from git (see below).

## Ecosystem harness (recommended for cross-backend runs)

### Install

```bash
pip install -e ".[dev,benchmark]"
# benchmark extra pulls ai-ecosystem-benchmark from GitHub
```

Or pin the framework checkout:

```bash
pip install "git+https://github.com/aerospike-community/ai-ecosystem-benchmark.git"
pip install -e .
```

### Run a profile

```bash
python benchmarks/run.py --list-profiles
python benchmarks/run.py --profile smoke
python benchmarks/run.py --profile agent_turn --uri "aerospike://127.0.0.1:3000/test?set_prefix=bench_"
```

Profiles live in `benchmarks/profiles/*.json` (QPS, thread pools, workload params). Override only the URI on the CLI; other knobs belong in the profile file so runs are reproducible.

### Workloads

Workloads subclass `BaseBenchmarkWorkload` and expose `aerospike_*` methods (sync wrappers over async ADK services). List them:

```bash
python benchmarks/run.py --list-workloads
```

| Workload | Real-world model | `aerospike_*` tests |
|---|---|---|
| `session_hotpath` | Multi-session agent loop | `session_append`, `session_get_recent`, `session_list` |
| `memory_lexical` | Keyword memory over prior turns | `memory_search`, `memory_ingest` |
| `artifacts` | Tool JSON / uploads (~4 KiB inline) | `artifact_save`, `artifact_load`, `artifact_list_versions` |
| `agent_turn` | **Composite** one turn | `agent_turn` (append → hydrate → search) |
| `chunk_stress` | Long conversation on one session | `session_append_chunked` (forced flushes) |

Default sizing assumptions (tunable via `workload_params` in profiles):

- Event text **200–600 B** (typical LLM turn + small state delta)
- Recent context window **10–20 events** (`GetSessionConfig.num_recent_events`)
- Memory corpus **5k–50k** text-bearing events; **3–4** query tokens (~1–5% hit rate per token with 256-word vocab)
- Artifacts **4 KiB** JSON inline (under namespace write-block-size)

### Profiles

| Profile | Workload | Intent |
|---|---|---|
| `smoke` | `session_hotpath` | Laptop / CI sanity (~5 s per test) |
| `sustained` | `session_hotpath` | 60 s @ 200 QPS, 64 sessions |
| `agent_turn` | `agent_turn` | End-to-end turn latency |
| `memory_mini` | `memory_lexical` | 500-entry corpus, short local proof |
| `memory_heavy` | `memory_lexical` | 50k corpus search |
| `chunk_stress` | `chunk_stress` | Chunk flush under append load |
| `artifacts` | `artifacts` | Save / load / list versions |

### Adding a workload

1. Create `benchmarks/workloads/my_workload.py` subclassing `BaseBenchmarkWorkload`.
2. Implement `setup` / `between_benchmarks` / `teardown`.
3. Add `aerospike_*` methods (one measurable op each; use `run_async()` from `_async_bridge.py`).
4. Register in `benchmarks/workloads/__init__.py`.
5. Add a JSON profile under `benchmarks/profiles/`.

When `ai-ecosystem-benchmark` lands on PyPI, change the `[benchmark]` extra to a version pin and drop the git URL.

## Micro harness (design validation)

Asyncio script with client-side percentiles. Does **not** use the ecosystem runner.

```bash
python benchmarks/benchmark.py append
python benchmarks/benchmark.py all
```

See scenario table in the docstring — `append`, `chunking`, `read`, `search` map to schema design claims (single-record atomic append, tail vs full history, list-element index).

## What neither harness does

- Mixed workloads at a single QPS mix (use separate profiles or Locust)
- Strong-consistency / cross-DC / MRT scenarios
- Cluster-side metrics (run `asadm` or your observability stack in parallel)

## Data isolation

Ecosystem workloads use `bench_eco_*` app names and `set_prefix=bench_` by default so they do not collide with tests or dev data. Tear down via workload `teardown()` or delete the bench namespace prefix manually.
