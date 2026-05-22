"""Integration tests for AerospikeMemoryService — lexical word-overlap.

Mirrors ADK's ``InMemoryMemoryService`` semantics: tokenize text into
lowercase ``[A-Za-z]+`` words; return memory entries whose token set
intersects the query's. No embedder.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from adk_aerospike import AerospikeMemoryService

pytestmark = pytest.mark.aerospike


@pytest_asyncio.fixture
async def memory_service(aerospike_uri: str) -> AsyncIterator[AerospikeMemoryService]:
    svc = AerospikeMemoryService.from_uri(aerospike_uri)
    try:
        yield svc
    finally:
        svc.close()


async def test_add_session_and_search(
    memory_service: AerospikeMemoryService,
) -> None:
    from google.adk.events import Event, EventActions
    from google.adk.sessions import Session
    from google.genai import types as genai_types

    session = Session(
        id="m-1",
        app_name="memapp",
        user_id="alice",
        events=[
            Event(
                invocation_id="i1",
                author="user",
                content=genai_types.Content(
                    role="user",
                    parts=[genai_types.Part(text="The python language has duck typing")],
                ),
                actions=EventActions(),
            ),
            Event(
                invocation_id="i2",
                author="user",
                content=genai_types.Content(
                    role="user",
                    parts=[genai_types.Part(text="My favorite color is blue")],
                ),
                actions=EventActions(),
            ),
        ],
    )
    await memory_service.add_session_to_memory(session)

    resp = await memory_service.search_memory(
        app_name="memapp", user_id="alice", query="python duck typing"
    )
    assert len(resp.memories) >= 1
    # Top match must overlap on python/duck/typing — three tokens beats color's zero.
    top_text = resp.memories[0].content.parts[0].text
    assert "python" in top_text.lower()


async def test_search_scoped_to_user(
    memory_service: AerospikeMemoryService,
) -> None:
    from google.adk.events import Event, EventActions
    from google.adk.sessions import Session
    from google.genai import types as genai_types

    alice_session = Session(
        id="ms-alice",
        app_name="scopeapp",
        user_id="alice",
        events=[
            Event(
                invocation_id="ia",
                author="user",
                content=genai_types.Content(
                    role="user", parts=[genai_types.Part(text="alice secret recipe")]
                ),
                actions=EventActions(),
            )
        ],
    )
    bob_session = Session(
        id="ms-bob",
        app_name="scopeapp",
        user_id="bob",
        events=[
            Event(
                invocation_id="ib",
                author="user",
                content=genai_types.Content(
                    role="user", parts=[genai_types.Part(text="bob secret recipe")]
                ),
                actions=EventActions(),
            )
        ],
    )
    await memory_service.add_session_to_memory(alice_session)
    await memory_service.add_session_to_memory(bob_session)

    bob_view = await memory_service.search_memory(
        app_name="scopeapp", user_id="bob", query="secret recipe"
    )
    # Bob should NEVER see alice's memory even though both match the tokens.
    for m in bob_view.memories:
        assert "alice" not in m.content.parts[0].text.lower()


async def test_search_empty_query_returns_empty(
    memory_service: AerospikeMemoryService,
) -> None:
    resp = await memory_service.search_memory(
        app_name="anyapp", user_id="anyone", query="!!!"
    )
    # Query has no [A-Za-z]+ tokens → empty result, no DB hit.
    assert resp.memories == []


async def test_search_ranks_by_token_overlap(
    memory_service: AerospikeMemoryService,
) -> None:
    """Records matching more query tokens should rank higher."""
    from google.adk.events import Event, EventActions
    from google.adk.sessions import Session
    from google.genai import types as genai_types

    session = Session(
        id="rank-1",
        app_name="rankapp",
        user_id="alice",
        events=[
            Event(
                invocation_id="r1",
                author="user",
                content=genai_types.Content(
                    role="user", parts=[genai_types.Part(text="apple")]
                ),
                actions=EventActions(),
            ),
            Event(
                invocation_id="r2",
                author="user",
                content=genai_types.Content(
                    role="user",
                    parts=[genai_types.Part(text="apple banana cherry")],
                ),
                actions=EventActions(),
            ),
        ],
    )
    await memory_service.add_session_to_memory(session)

    resp = await memory_service.search_memory(
        app_name="rankapp", user_id="alice", query="apple banana cherry"
    )
    assert len(resp.memories) >= 1
    # Three-token match must outrank one-token match.
    top_text = resp.memories[0].content.parts[0].text
    assert "banana" in top_text
