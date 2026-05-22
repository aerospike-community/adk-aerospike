"""End-to-end example: run an ADK agent with Aerospike-backed session storage.

Prerequisites
-------------
- Aerospike server running locally (``docker run -p 3000:3000 aerospike/aerospike-server``)
- ``pip install adk-aerospike google-adk``
- ``GOOGLE_API_KEY`` (or other model credentials) exported

Run::

    python examples/quickstart.py
"""

from __future__ import annotations

import asyncio

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.genai import types

from adk_aerospike import AerospikeSessionService


async def main() -> None:
    session_service = AerospikeSessionService.from_uri(
        "aerospike://localhost:3000/test"
    )

    agent = LlmAgent(
        name="greeter",
        model="gemini-2.0-flash",
        instruction="You are a friendly assistant. Keep replies under 30 words.",
    )

    runner = Runner(
        agent=agent,
        app_name="quickstart",
        session_service=session_service,
    )

    session = await session_service.create_session(
        app_name="quickstart",
        user_id="user-42",
    )

    async for event in runner.run_async(
        user_id="user-42",
        session_id=session.id,
        new_message=types.Content(role="user", parts=[types.Part(text="Hi!")]),
    ):
        if event.content and event.content.parts:
            for part in event.content.parts:
                if part.text:
                    print(part.text)

    session_service.close()


if __name__ == "__main__":
    asyncio.run(main())
