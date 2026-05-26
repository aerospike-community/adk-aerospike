# Benchmarks

Asyncio latency/throughput harness for `adk-aerospike`. Designed to falsify
four specific design claims one at a time, not to drive sustained mixed
workloads (use Locust for that).

## Quick start

```bash
# Default: localhost Aerospike on :3000, namespace "test"
python benchmarks/benchmark.py append

# All four scenarios with default parameters
python benchmarks/benchmark.py all

# Custom cluster + scale knobs
python benchmarks/benchmark.py search \
    --uri "aerospike://my-cluster:3000/adk?set_prefix=bench_" \
    --corpus 100000 --ops 2000 --concurrency 64 --query-tokens 5
```

## What each scenario measures

| Scenario | Validates | What "good" looks like |
|---|---|---|
| `append` | `append_event` is a single-record server-side atomic op | Latency flat across concurrency; ops/s scales with concurrency until cluster saturates |
| `chunking` | Chunked tail keeps each append cheap; flushes amortize | p50 sub-ms; p99 a few × p50 (periodic flush spikes); no catastrophic max |
| `read` | `get_session` is one RTT when tail satisfies `num_recent_events` | Fast path flat vs chunk count; `--full-history` shows linear cost in chunks |
| `search` | List-element sec-index keeps memory search sublinear in corpus size | Search latency grows with query token count, not corpus size |

## Reference numbers — local Aerospike CE, MacBook (M-series), single node

These are smoke-test numbers from a development laptop, not production capacity.
Use them as a sanity check; numbers will be very different on real hardware.

```
append (size=200B, concurrency=16)               ops/s=5670   p50=2.10ms  p99=3.77ms
chunking (500 events, 600B each → 500 hydrated)  ops/s=1869   p50=0.47ms  p99=1.01ms
get_session [num_recent_events=5] (50 chunks)    ops/s=1729   p50=8.67ms  p99=11.84ms
get_session [full history]        (50 chunks)    ops/s=  43   p50=330ms   p99=683ms
search_memory (corpus=500, 3 tokens)             ops/s= 191   p50=37.97ms p99=83.05ms
```

Key reads from those numbers:

- **`append_event` is sub-4ms p99** at 16-way concurrency, confirming the
  single-record atomic-operate design.
- **Chunking flush is non-disruptive**: p99=1ms despite 500 events spanning
  many flushes; the max spike of 8ms is the flush itself, amortized over
  ~300 events.
- **Fast-path `get_session` is 40× faster than full-history**: the
  `num_recent_events` / `batch_read` design wins exactly where the design
  predicted.
- **Search at 500 entries is 38ms p50**: dominated by issuing 3 indexed
  queries per call (one per query token). Scale this up to find the corpus
  size where it tips over.

## CLI reference

```
positional:  scenario  {append, chunking, read, search, all}

connection:
  --uri URI                 default: aerospike://127.0.0.1:3000/test

common knobs:
  --ops N                   default: 2000
  --concurrency N           default: 32
  --event-size BYTES        default: 200    (append, chunking)

scenario-specific:
  --events N                default: 5000   (chunking: events per single session)
  --chunks N                default: 10     (read: sealed chunks to pre-build)
  --full-history            default: off    (read: walk all chunks instead of fast path)
  --corpus N                default: 5000   (search: memories to preload)
  --query-tokens N          default: 3      (search: tokens per query)
```

## Suggestions for getting useful numbers

1. **Run against a real cluster** — single-node Docker on macOS is bottlenecked
   by Docker networking, not Aerospike. Either bind-mount with host networking
   or, better, point at a real 3-node deployment.

2. **Sweep, don't single-shot.** The interesting question isn't "what's p99 at
   N=2000?" but "how does p99 change with concurrency / corpus / chunk
   count?" Wrap the script in a shell loop and chart the outputs.

3. **Watch the cluster, not just the client.** Run `asadm` in another terminal
   and look at the `latency` histograms while the benchmark runs — they tell
   you whether you're seeing client-side queueing or actual server latency.

4. **Mind the warmup.** First few hundred ops include JIT, connection setup,
   secondary-index page-ins. Either discard the first 10% of samples or run
   with `--ops` large enough that warmup is noise.

5. **Don't trust microbenchmarks for capacity planning.** This harness shows
   relative behavior of design choices. For real capacity planning, run
   Locust against a production-shaped workload on production-shaped hardware
   and measure under your actual concurrency target.

## ADK official-sample validation

Two additional harnesses live alongside the perf benchmark and validate that
**Google ADK's own sample agents** wire up against our services correctly:

| Script | What it does | Needs LLM API key |
|---|---|---|
| `adk_samples_wiring.py` | For a curated set of samples from `google/adk-samples`: imports the agent, constructs `Runner(agent, session_service, artifact_service, memory_service)`, manually drives `create_session → append_event → get_session → add_session_to_memory → search_memory`, reports per-phase pass/fail | ❌ No — pure wiring smoke |
| `adk_samples_e2e.py` | Same samples, but actually runs one user turn through `Runner.run_async()`. Captures every event the Runner emits, then dumps the Aerospike state | ✅ Yes — `GEMINI_API_KEY` or `GOOGLE_API_KEY` in `.env` |

### Setup

```bash
# 1. Clone the official ADK sample agents next to this repo:
git clone --depth 1 https://github.com/google/adk-samples ../adk-samples

# 2. Some samples need extra deps:
pip install 'a2a-sdk[all]' mcp

# 3. For the E2E script: put your key in .env at the repo root:
echo "GEMINI_API_KEY=AIza..." > .env   # .env is gitignored
```

### Run

```bash
# Phase 1 — wiring only (no LLM cost)
python benchmarks/adk_samples_wiring.py

# Phase 2 — real LLM turns (small Gemini cost)
SLACK_MCP_XOXP_TOKEN=dummy python benchmarks/adk_samples_e2e.py
# (the dummy SLACK token avoids an import-time MCPToolset error in one sample)

# Both accept --uri (cluster) and --samples-path (where you cloned adk-samples).
```

### Reference outcomes (alpha)

7 representative samples (`fun-facts`, `currency-agent`, `llm-auditor`,
`parallel_task_decomposition_execution`, `memory-bank`, `customer-service`,
`blog-writer`) — covering `LlmAgent`, `SequentialAgent`, `ParallelAgent`,
agents with tools, multi-agent workflows.

- **Wiring**: 6 / 7 pass; 1 fails only because the sample pins old SDK versions
  (`a2a-sdk==0.3.3`, `google-adk==1.13.0`) incompatible with current installed
  versions — not an integration issue with us.
- **E2E**: 3 / 3 pass (fun-facts, llm-auditor, customer-service) — real Gemini
  events round-trip through chunked-session storage; multi-agent sequential
  preserves sub-agent event ordering; stateful tool callbacks correctly
  populate `session.state`.

## What this harness deliberately does NOT do

- **No mixed workloads.** Use Locust if you want session-create + reads +
  appends + searches happening together.
- **No replication / SC / MRT measurement.** Wire your cluster appropriately
  before testing those.
- **No cluster-side metric collection.** Run `asadm`, `aerospike-prometheus-exporter`,
  or your standard observability stack in parallel.
- **No long-running stability test.** Run for hours/days with Locust if that's
  what you want to measure.
