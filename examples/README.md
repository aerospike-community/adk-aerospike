# Examples

Runnable demos showing how to use `adk-aerospike`.

| Script | What it shows | Needs LLM API key |
|---|---|---|
| `quickstart.py` | Minimal `LlmAgent` wired with `AerospikeSessionService` and `Runner.run_async` | ✅ Yes — Gemini |
| `with_memory.py` | Session + lexical memory service end-to-end | ✅ Yes — Gemini |
| `adk_samples_wiring.py` | Smoke-test that Google's own [`adk-samples`](https://github.com/google/adk-samples) work with our three services. Imports each sample's `root_agent`, constructs `Runner(...)`, drives `create_session → append_event → get_session → add_session_to_memory → search_memory`. | ❌ No |
| `adk_samples_e2e.py` | Same samples, but actually runs one user turn per agent via `Runner.run_async()` against a real Gemini call. Dumps the resulting Aerospike state. | ✅ Yes — Gemini |

## Setup for the sample harnesses

```bash
# 1. Clone Google's ADK samples next to this repo
git clone --depth 1 https://github.com/google/adk-samples ../adk-samples

# 2. Some samples need extra deps
pip install 'a2a-sdk[all]' mcp

# 3. For E2E only — add your key to .env at the repo root (.env is gitignored)
echo "GEMINI_API_KEY=AIza..." > ../.env
```

## Run

```bash
# Wiring-only (no LLM cost, ~30s):
python examples/adk_samples_wiring.py

# End-to-end with real Gemini turns (~2 min, small Gemini cost):
SLACK_MCP_XOXP_TOKEN=dummy python examples/adk_samples_e2e.py
# (the dummy SLACK token avoids an import-time MCPToolset error in one sample)

# Both scripts accept --uri (any Aerospike cluster) and --samples-path.
```

## Compat shims (built-in)

Several official samples have import-time hooks that fail in a clean
environment. The harnesses apply two narrowly-scoped shims so the samples
can be evaluated against our services:

1. **`google.auth.default` stubbed during sample import only.** Vertex-coupled
   samples (memory-bank, customer-service, blog-writer) construct Vertex
   clients at module load and call `google.auth.default()`, raising
   `DefaultCredentialsError` on hosts without GCP Application Default
   Credentials. We stub the call for the duration of `importlib.import_module(...)`
   and restore the real function before any runtime LLM call so we don't
   poison the auth path.
2. **`google.adk.a2a.utils.agent_to_a2a.to_a2a` stubbed to a no-op.**
   currency-agent pins `a2a-sdk==0.3.3` whose `a2a.server.apps` module path
   moved in 1.0+. The unused `a2a_app` export still constructs.
3. **`GOOGLE_GENAI_USE_VERTEXAI=false`** is set so the SDK uses the Gemini
   Developer API (`GOOGLE_API_KEY` path) rather than auto-picking Vertex
   when ADC happens to be discoverable.

These shims only widen what's *importable*. They don't paper over runtime
problems: a sample that genuinely needs Vertex at runtime would still
need real ADC.

## Reference outcomes (alpha)

Curated set of 7 samples covering `LlmAgent`, `SequentialAgent`,
`ParallelAgent`, multi-agent workflows, and agents with tools, validated
against the AWS Pegasus cluster:

- **Wiring**: **7 / 7 ✓** — all samples import, accept our service trio
  via `Runner(...)`, and round-trip a synthetic event through
  `create_session → append_event → get_session → add_session_to_memory →
  search_memory`.
- **E2E**: **7 / 7 ✓** — all 7 drive real Gemini turns through
  `runner.run_async()`. Cluster state after one run:
  `adk_sessions: 7, adk_memory: 21` (one session per sample, multiple
  text-bearing events from multi-agent samples like `parallel_task`
  which emits 17 events and `blog-writer` with `transfer_to_agent`
  calls).
