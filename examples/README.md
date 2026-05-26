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

## Reference outcomes (alpha)

Curated set of 7 samples covering `LlmAgent`, `SequentialAgent`,
`ParallelAgent`, agents with tools, and multi-agent workflows:

- **Wiring**: 6 / 7 pass. The one failure (`currency-agent`) pins old SDK
  versions (`a2a-sdk==0.3.3`, `google-adk==1.13.0`) incompatible with what's
  installed — a sample-side problem, not our integration.
- **E2E**: 3 / 3 selected samples (`fun-facts`, `llm-auditor`,
  `customer-service`) drive real Gemini turns; multi-agent sequential
  events arrive in order; stateful tool callbacks populate `session.state`
  correctly.
