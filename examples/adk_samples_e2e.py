#!/usr/bin/env python3
"""End-to-end: run a real LLM turn for selected ADK samples through Runner.run_async().

For each sample:
  1. Load .env from the repo root (provides GEMINI_API_KEY / GOOGLE_API_KEY)
  2. Import the agent
  3. Wire a Runner with our three Aerospike services
  4. Push one user turn through ``runner.run_async()`` — REAL Gemini calls
  5. Stream and print every Event the Runner emits
  6. Re-fetch via ``get_session`` to confirm the events landed in storage
  7. Persist via ``add_session_to_memory`` + run a search query

Validates that real LLM-generated Event objects (with text, tool calls,
state deltas) round-trip through our chunked-session and lexical-memory stack.

Usage
-----
1. Clone the samples repo (default ../adk-samples):
     git clone --depth 1 https://github.com/google/adk-samples ../adk-samples
2. Create .env at repo root with at least:
     GEMINI_API_KEY=AIza...    # or GOOGLE_API_KEY=
3. Run:
     python examples/adk_samples_e2e.py
     python examples/adk_samples_e2e.py --samples-path /path/to/adk-samples \
                                          --uri aerospike://my-cluster:3000/Test
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import os
import sys
from pathlib import Path

import aerospike
from dotenv import load_dotenv


# Default to ../adk-samples relative to the adk-aerospike repo root.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_SAMPLES = str(_REPO_ROOT.parent / "adk-samples")
DEFAULT_URI = "aerospike://127.0.0.1:3000/test"


def discover_samples(samples_root: Path) -> list[tuple[str, str, str, str]]:
    """Return (label, parent_dir, package, user_message) tuples.

    The three samples below exercise three different agent shapes:
      - fun-facts        : LlmAgent + google_search tool, single-turn
      - llm-auditor      : SequentialAgent over two sub-agents (critic → reviser)
      - customer-service : LlmAgent with stateful tool callbacks (state_delta)
    """
    py = samples_root / "python" / "agents"
    return [
        ("fun-facts", str(py / "fun-facts"), "fun_facts",
         "Tell me one obscure fun fact about octopuses, in under 40 words."),
        ("llm-auditor", str(py / "llm-auditor"), "llm_auditor",
         "FACT-CHECK THIS: The Eiffel Tower was completed in 1889 "
         "and is located in Berlin, Germany."),
        ("customer-service", str(py / "customer-service"), "customer_service",
         "Hi, I'd like to know my recent orders."),
    ]


async def run_one(label: str, parent: str, pkg: str, user_msg: str, uri: str) -> None:
    from google.adk.runners import Runner
    from google.genai import types

    from adk_aerospike import (
        AerospikeArtifactService,
        AerospikeMemoryService,
        AerospikeSessionService,
    )

    if parent not in sys.path:
        sys.path.insert(0, parent)
    for k in list(sys.modules):
        if k == pkg or k.startswith(pkg + "."):
            sys.modules.pop(k, None)

    mod = importlib.import_module(f"{pkg}.agent")
    agent = getattr(mod, "root_agent", None)
    if agent is None:
        print(f"  SKIP: {pkg}.agent has no root_agent")
        return
    print(f"\n{'='*78}\n>>> SAMPLE: {label}  (agent={type(agent).__name__})")
    print(f">>> user_msg: {user_msg!r}")
    print('=' * 78)

    APP = f"e2e_{label.replace('-', '_')}"
    USER = "alice"
    SID = "s1"

    sess = AerospikeSessionService.from_uri(uri)
    art = AerospikeArtifactService.from_uri(uri)
    mem = AerospikeMemoryService.from_uri(uri)

    try:
        try: await sess.delete_session(app_name=APP, user_id=USER, session_id=SID)
        except Exception: pass
        await sess.create_session(app_name=APP, user_id=USER, session_id=SID)

        runner = Runner(
            agent=agent, app_name=APP,
            session_service=sess, artifact_service=art, memory_service=mem,
        )
        msg = types.Content(role="user", parts=[types.Part(text=user_msg)])

        n_events = 0
        async for ev in runner.run_async(user_id=USER, session_id=SID, new_message=msg):
            n_events += 1
            text = ""
            fc: list[str] = []
            if ev.content and ev.content.parts:
                texts = [p.text for p in ev.content.parts if getattr(p, "text", None)]
                text = " | ".join(texts)[:200]
                for p in ev.content.parts:
                    if getattr(p, "function_call", None):
                        fc.append(f"{p.function_call.name}({list(p.function_call.args or {})})")
                    elif getattr(p, "function_response", None):
                        fc.append(f"→{p.function_response.name}")
            tag = ",".join(fc) if fc else ""
            sep = " | " if tag and text else ""
            print(f"  Event[{n_events:>2}] author={ev.author!r:<14} "
                  f"partial={ev.partial} {tag}{sep}{text!r}"[:240])
        print(f"  Total events emitted by Runner: {n_events}")

        fetched = await sess.get_session(app_name=APP, user_id=USER, session_id=SID)
        n = len(fetched.events) if fetched else 0
        keys = list(fetched.state.keys()) if fetched else []
        print(f"\n  AEROSPIKE round-trip: events={n} state_keys={keys}")

        if fetched:
            await mem.add_session_to_memory(fetched)
            # Use a likely keyword from the user message for a sanity-check search
            words = [w.strip(",.?!") for w in user_msg.lower().split() if len(w) > 4]
            kw = words[0] if words else "the"
            resp = await mem.search_memory(app_name=APP, user_id=USER, query=kw)
            print(f"  MEMORY search(query={kw!r}): {len(resp.memories)} hits")
    finally:
        sess.close(); art.close(); mem.close()


async def truncate_local(uri: str) -> None:
    # Parse hostport from the URI (best-effort; full parse is in our _internal.uri).
    from adk_aerospike._internal.uri import parse as parse_uri
    parsed = parse_uri(uri)
    c = aerospike.client({"hosts": list(parsed.hosts)})
    c.connect()
    for s in ("adk_sessions", "adk_memory",
              "adk_app_state", "adk_user_state", "adk_artifacts"):
        try: c.truncate(parsed.namespace, s, 0)
        except Exception: pass
    c.close()
    print("truncated.\n")


def dump_all(uri: str) -> None:
    from adk_aerospike._internal.uri import parse as parse_uri
    parsed = parse_uri(uri)
    c = aerospike.client({"hosts": list(parsed.hosts)})
    c.connect()
    print(f"\n{'#'*78}\n# FINAL AEROSPIKE STATE\n{'#'*78}\n")
    for setname in ("adk_sessions", "adk_memory"):
        n = 0
        print(f"-- {setname} --")
        for key, _, bins in c.scan(parsed.namespace, setname).results():
            n += 1
            pk = key[2].decode() if isinstance(key[2], (bytes, bytearray)) else key[2]
            extras = []
            if "events" in bins:   extras.append(f"events={len(bins['events'])}")
            if "seq" in bins:      extras.append(f"seq={bins['seq']}")
            if "chunks" in bins:   extras.append(f"chunks={bins['chunks']}")
            if "cidx" in bins:     extras.append(f"cidx={bins['cidx']}")
            if "keywords" in bins: extras.append(f"|keywords|={len(bins['keywords'])}")
            print(f"  {pk!r}  {' '.join(extras)}")
        print(f"  ({n} record(s) total in {setname})\n")
    c.close()


async def main(args: argparse.Namespace) -> None:
    load_dotenv(_REPO_ROOT / ".env", override=True)
    # google-genai SDK looks for GOOGLE_API_KEY; users often set GEMINI_API_KEY.
    if not os.getenv("GOOGLE_API_KEY") and os.getenv("GEMINI_API_KEY"):
        os.environ["GOOGLE_API_KEY"] = os.environ["GEMINI_API_KEY"]
    if not os.getenv("GOOGLE_API_KEY"):
        print("ERROR: GOOGLE_API_KEY (or GEMINI_API_KEY) not set. "
              f"Add it to {_REPO_ROOT/'.env'} or your shell environment.")
        sys.exit(1)

    samples_root = Path(args.samples_path).resolve()
    if not (samples_root / "python" / "agents").exists():
        print(f"ERROR: {samples_root}/python/agents not found. "
              f"Clone the samples repo there or pass --samples-path.")
        sys.exit(2)

    print(f"Using GOOGLE_API_KEY=AIza...{os.environ['GOOGLE_API_KEY'][-4:]}")
    print(f"Cluster      : {args.uri}")
    print(f"Samples root : {samples_root}\n")

    await truncate_local(args.uri)
    samples = discover_samples(samples_root)
    for label, parent, pkg, msg in samples:
        try:
            await run_one(label, parent, pkg, msg, args.uri)
        except Exception as e:
            print(f"  ERROR in {label}: {type(e).__name__}: {str(e)[:300]}")
    dump_all(args.uri)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--samples-path", default=_DEFAULT_SAMPLES,
                   help=f"path to google/adk-samples checkout (default: {_DEFAULT_SAMPLES})")
    p.add_argument("--uri", default=DEFAULT_URI,
                   help=f"Aerospike URI (default: {DEFAULT_URI})")
    asyncio.run(main(p.parse_args()))
