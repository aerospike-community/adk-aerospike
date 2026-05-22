"""Example: Aerospike Session + Memory wired into a Runner.

Memory is **lexical** — same word-overlap semantics as ADK's
``InMemoryMemoryService``, executed server-side via Aerospike's list-element
secondary index. No embedder, no AI dependency.

Prerequisites
-------------
- Aerospike server on :3000
- ``pip install adk-aerospike``
"""

from __future__ import annotations

import asyncio

from google.adk.agents import LlmAgent
from google.adk.runners import Runner

from adk_aerospike import AerospikeMemoryService, AerospikeSessionService


async def main() -> None:
    session_service = AerospikeSessionService.from_uri("aerospike://localhost:3000/adk")
    memory_service = AerospikeMemoryService.from_uri(
        "aerospike://localhost:3000/adk", top_k=10,
    )

    agent = LlmAgent(
        name="assistant",
        model="gemini-2.0-flash",
        instruction="Use memory to recall context from past sessions when relevant.",
    )

    runner = Runner(
        agent=agent,
        app_name="memdemo",
        session_service=session_service,
        memory_service=memory_service,
    )

    session = await session_service.create_session(app_name="memdemo", user_id="u1")
    # ... run turns via runner, then persist the session to memory ...
    await memory_service.add_session_to_memory(session)


if __name__ == "__main__":
    asyncio.run(main())
