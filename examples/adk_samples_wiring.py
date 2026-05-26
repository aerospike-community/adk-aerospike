#!/usr/bin/env python3
"""Wiring smoke test: each Google ADK official sample agent + our three Aerospike services.

For each sample:
  1. import the agent module
  2. grab `root_agent`
  3. construct a Runner with all three Aerospike services
  4. manually drive: create_session → append_event → get_session
  5. then: add_session_to_memory → search_memory
  6. report PASS/FAIL per phase

Does NOT invoke the LLM (no API key required). Validates that the Runner
accepts our services and that our services round-trip ADK Session/Event
objects produced by each sample's agent definition.

Usage
-----
First clone the official samples repo somewhere (default ../adk-samples):

    git clone --depth 1 https://github.com/google/adk-samples ../adk-samples

Then run (from the adk-aerospike repo root):

    python examples/adk_samples_wiring.py
    python examples/adk_samples_wiring.py --samples-path /path/to/adk-samples
    python examples/adk_samples_wiring.py --uri aerospike://my-cluster:3000/Test

Some samples need extra deps. Install on demand:

    pip install 'a2a-sdk[all]' mcp

Some samples have import-time side effects requiring env vars (e.g.
``parallel_task_decomposition_execution`` constructs an MCPToolset reading
``SLACK_MCP_XOXP_TOKEN``). Set a dummy value or skip the sample.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import os
import sys
import traceback
from pathlib import Path

# Default path: ../adk-samples relative to this script's repo root.
_DEFAULT_SAMPLES = str(Path(__file__).resolve().parent.parent.parent / "adk-samples")
DEFAULT_URI = "aerospike://127.0.0.1:3000/test"


def discover_samples(samples_root: Path) -> list[tuple[str, str, str]]:
    """Return list of (label, parent_dir, package_name) for the curated subset.

    This is the same 7-sample slice we validated in alpha. Add more by editing
    the SAMPLES tuple at the bottom of the function or by passing
    ``--sample LABEL=parent_dir:pkg_name`` on the CLI.
    """
    py = samples_root / "python" / "agents"
    SAMPLES = [
        ("fun-facts",        py / "fun-facts",                                       "fun_facts"),
        ("currency-agent",   py / "currency-agent",                                  "currency_agent"),
        ("llm-auditor",      py / "llm-auditor",                                    "llm_auditor"),
        ("parallel_task",    py / "parallel_task_decomposition_execution",          "parallel_task_decomposition_agent"),
        ("memory-bank",      py / "memory-bank",                                    "app"),
        ("customer-service", py / "customer-service",                               "customer_service"),
        ("blog-writer",      py / "blog-writer",                                    "blogger_agent"),
    ]
    return [(label, str(parent), pkg) for label, parent, pkg in SAMPLES]


async def smoke_one(label: str, parent: str, pkg: str, uri: str) -> dict:
    from google.adk.events import Event, EventActions
    from google.adk.runners import Runner
    from google.genai import types

    from adk_aerospike import (
        AerospikeArtifactService,
        AerospikeMemoryService,
        AerospikeSessionService,
    )

    r: dict = {
        "label": label, "import": "✗", "agent_type": "-", "runner": "✗",
        "create_session": "✗", "append_event": "✗", "get_session": "✗",
        "search_memory": "✗", "errors": [],
    }

    # Per-sample sys.path isolation — same package name in different samples
    # would collide otherwise (e.g. all use `agent.py` at module root).
    if parent in sys.path:
        sys.path.remove(parent)
    sys.path.insert(0, parent)
    for k in list(sys.modules):
        if k == pkg or k.startswith(pkg + "."):
            sys.modules.pop(k, None)

    try:
        mod = importlib.import_module(f"{pkg}.agent")
        r["import"] = "✓"
    except Exception as e:
        r["errors"].append(f"import: {type(e).__name__}: {e}")
        return r

    agent = getattr(mod, "root_agent", None)
    if agent is None:
        r["errors"].append("module has no `root_agent`")
        return r
    r["agent_type"] = type(agent).__name__

    sess = AerospikeSessionService.from_uri(uri)
    art = AerospikeArtifactService.from_uri(uri)
    mem = AerospikeMemoryService.from_uri(uri)

    app_name = f"smoke_{label.replace('-','_')}"
    try:
        try:
            Runner(
                agent=agent, app_name=app_name,
                session_service=sess, artifact_service=art, memory_service=mem,
            )
            r["runner"] = "✓"
        except Exception as e:
            r["errors"].append(f"Runner: {type(e).__name__}: {e}")
            return r

        try:
            s = await sess.create_session(app_name=app_name, user_id="u1", session_id="s1")
            r["create_session"] = "✓"
        except Exception as e:
            r["errors"].append(f"create_session: {type(e).__name__}: {e}")
            return r

        try:
            await sess.append_event(s, Event(
                invocation_id="i1", author="user",
                content=types.Content(role="user",
                    parts=[types.Part(text="ping invoice question")]),
                actions=EventActions(state_delta={"k": "v", "app:t": "acme"})))
            r["append_event"] = "✓"
        except Exception as e:
            r["errors"].append(f"append_event: {type(e).__name__}: {e}")

        try:
            fetched = await sess.get_session(app_name=app_name, user_id="u1", session_id="s1")
            ok = (fetched is not None
                  and len(fetched.events) == 1
                  and fetched.state.get("k") == "v")
            r["get_session"] = "✓" if ok else (
                f"partial(events={len(fetched.events) if fetched else 'None'})")
        except Exception as e:
            r["errors"].append(f"get_session: {type(e).__name__}: {e}")
            fetched = None

        try:
            if fetched:
                await mem.add_session_to_memory(fetched)
                resp = await mem.search_memory(
                    app_name=app_name, user_id="u1", query="invoice")
                r["search_memory"] = "✓" if len(resp.memories) >= 1 else "✗(no hits)"
        except Exception as e:
            r["errors"].append(f"search_memory: {type(e).__name__}: {e}")
    finally:
        try: await sess.delete_session(app_name=app_name, user_id="u1", session_id="s1")
        except Exception: pass
        sess.close(); art.close(); mem.close()

    return r


async def main(args: argparse.Namespace) -> None:
    samples_root = Path(args.samples_path).resolve()
    if not (samples_root / "python" / "agents").exists():
        print(f"ERROR: {samples_root}/python/agents not found. "
              f"Clone the samples repo there or pass --samples-path.")
        sys.exit(2)

    samples = discover_samples(samples_root)
    print(f"adk-aerospike × adk-samples wiring smoke test")
    print(f"Cluster      : {args.uri}")
    print(f"Samples root : {samples_root}\n")

    rows: list[dict] = []
    for label, parent, pkg in samples:
        print(f"--- {label} ---")
        try:
            r = await smoke_one(label, parent, pkg, args.uri)
        except Exception as e:
            r = {"label": label, "errors": [f"harness: {e}\n{traceback.format_exc()}"]}
        rows.append(r)
        for k in ("import", "agent_type", "runner",
                  "create_session", "append_event", "get_session", "search_memory"):
            print(f"  {k:<16} {r.get(k, '?')}")
        for e in r.get("errors", []):
            print(f"  ERR: {e[:300]}")
        print()

    print("=" * 78)
    print(f"{'sample':<18} {'agent':<18} {'imp':<4} {'run':<4} "
          f"{'sess':<5} {'evt':<4} {'get':<8} {'mem':<10}")
    print("-" * 78)
    for r in rows:
        print(f"{r.get('label',''):<18} {r.get('agent_type','-'):<18} "
              f"{r.get('import','-'):<4} {r.get('runner','-'):<4} "
              f"{r.get('create_session','-'):<5} {r.get('append_event','-'):<4} "
              f"{str(r.get('get_session','-'))[:7]:<8} "
              f"{str(r.get('search_memory','-'))[:9]:<10}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--samples-path", default=_DEFAULT_SAMPLES,
                   help=f"path to google/adk-samples checkout (default: {_DEFAULT_SAMPLES})")
    p.add_argument("--uri", default=DEFAULT_URI,
                   help=f"Aerospike URI (default: {DEFAULT_URI})")
    asyncio.run(main(p.parse_args()))
