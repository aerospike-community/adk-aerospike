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


# ---- additional path coverage ------------------------------------------------


async def test_re_adding_session_overwrites_previous_memories(
    memory_service: AerospikeMemoryService,
) -> None:
    """Calling ``add_session_to_memory`` twice for the same session must
    replace prior memories, not duplicate them. Mirrors
    ``InMemoryMemoryService`` semantics."""
    from google.adk.events import Event, EventActions
    from google.adk.sessions import Session
    from google.genai import types as genai_types

    def make_session(text: str) -> Session:
        return Session(
            id="reuse-1",
            app_name="reuseapp",
            user_id="alice",
            events=[
                Event(
                    invocation_id="i1",
                    author="user",
                    content=genai_types.Content(
                        role="user", parts=[genai_types.Part(text=text)]
                    ),
                    actions=EventActions(),
                ),
            ],
        )

    await memory_service.add_session_to_memory(make_session("first version python"))
    await memory_service.add_session_to_memory(make_session("second version golang"))

    # The first-version tokens (first/python) should no longer match anything
    # for this session — the purge step wiped them.
    first = await memory_service.search_memory(
        app_name="reuseapp", user_id="alice", query="python"
    )
    assert all(
        "python" not in m.content.parts[0].text.lower() for m in first.memories
    )

    second = await memory_service.search_memory(
        app_name="reuseapp", user_id="alice", query="golang"
    )
    assert len(second.memories) == 1
    assert "golang" in second.memories[0].content.parts[0].text.lower()


async def test_search_skips_events_without_text(
    memory_service: AerospikeMemoryService,
) -> None:
    """Events whose content has no text parts (e.g. tool calls, inline data)
    must not produce memory entries."""
    from google.adk.events import Event, EventActions
    from google.adk.sessions import Session
    from google.genai import types as genai_types

    session = Session(
        id="notext-1",
        app_name="notextapp",
        user_id="u",
        events=[
            Event(
                invocation_id="i1",
                author="user",
                content=genai_types.Content(
                    role="user",
                    parts=[
                        genai_types.Part(
                            inline_data=genai_types.Blob(
                                mime_type="image/png", data=b"\x00"
                            )
                        )
                    ],
                ),
                actions=EventActions(),
            ),
            Event(
                invocation_id="i2",
                author="user",
                content=genai_types.Content(
                    role="user", parts=[genai_types.Part(text="searchable text")]
                ),
                actions=EventActions(),
            ),
        ],
    )
    await memory_service.add_session_to_memory(session)

    resp = await memory_service.search_memory(
        app_name="notextapp", user_id="u", query="searchable"
    )
    assert len(resp.memories) == 1
    assert "searchable" in resp.memories[0].content.parts[0].text


async def test_search_scoped_per_app(
    memory_service: AerospikeMemoryService,
) -> None:
    """Same user, two apps, same query — memories must not leak across apps."""
    from google.adk.events import Event, EventActions
    from google.adk.sessions import Session
    from google.genai import types as genai_types

    def s(app: str, marker: str) -> Session:
        return Session(
            id=f"{app}-1",
            app_name=app,
            user_id="alice",
            events=[
                Event(
                    invocation_id="i",
                    author="user",
                    content=genai_types.Content(
                        role="user",
                        parts=[genai_types.Part(text=f"{marker} payload")],
                    ),
                    actions=EventActions(),
                )
            ],
        )

    await memory_service.add_session_to_memory(s("appone", "uniqueone"))
    await memory_service.add_session_to_memory(s("apptwo", "uniquetwo"))

    one = await memory_service.search_memory(
        app_name="appone", user_id="alice", query="uniqueone uniquetwo payload"
    )
    for m in one.memories:
        assert "uniquetwo" not in m.content.parts[0].text


async def test_search_no_match_returns_empty(
    memory_service: AerospikeMemoryService,
) -> None:
    """Query whose tokens don't appear in any memory yields zero results."""
    from google.adk.events import Event, EventActions
    from google.adk.sessions import Session
    from google.genai import types as genai_types

    session = Session(
        id="nomatch-1",
        app_name="nomatchapp",
        user_id="u",
        events=[
            Event(
                invocation_id="i",
                author="user",
                content=genai_types.Content(
                    role="user", parts=[genai_types.Part(text="alpha beta gamma")]
                ),
                actions=EventActions(),
            )
        ],
    )
    await memory_service.add_session_to_memory(session)

    resp = await memory_service.search_memory(
        app_name="nomatchapp", user_id="u", query="xylophone zebra"
    )
    assert resp.memories == []


async def test_top_k_caps_results(aerospike_uri: str) -> None:
    """top_k bounds the response size even when many memories match."""
    from google.adk.events import Event, EventActions
    from google.adk.sessions import Session
    from google.genai import types as genai_types

    svc = AerospikeMemoryService.from_uri(aerospike_uri, top_k=2)
    try:
        events = [
            Event(
                invocation_id=f"i{i}",
                author="user",
                content=genai_types.Content(
                    role="user",
                    parts=[genai_types.Part(text=f"common token entry {i}")],
                ),
                actions=EventActions(),
            )
            for i in range(5)
        ]
        session = Session(
            id="topk-1",
            app_name="topkapp",
            user_id="u",
            events=events,
        )
        await svc.add_session_to_memory(session)

        resp = await svc.search_memory(
            app_name="topkapp", user_id="u", query="common token"
        )
        assert len(resp.memories) == 2
    finally:
        svc.close()


async def test_purge_isolated_across_apps_with_same_user_and_session(
    memory_service: AerospikeMemoryService,
) -> None:
    """Re-adding a session must purge *only* this app's prior memories for
    this user+session — composite ``aus`` index guarantees we don't touch
    another app's data even when user_id and session_id collide."""
    from google.adk.events import Event, EventActions
    from google.adk.sessions import Session
    from google.genai import types as genai_types

    def make(app: str, text: str) -> Session:
        return Session(
            id="collide-1",
            app_name=app,
            user_id="shared-user",
            events=[
                Event(
                    invocation_id="i",
                    author="user",
                    content=genai_types.Content(
                        role="user", parts=[genai_types.Part(text=text)]
                    ),
                    actions=EventActions(),
                )
            ],
        )

    await memory_service.add_session_to_memory(make("appX", "alpha-keyword"))
    await memory_service.add_session_to_memory(make("appY", "beta-keyword"))

    # Re-add appX with a different payload — purge must drop only alpha,
    # leaving appY's beta intact.
    await memory_service.add_session_to_memory(make("appX", "gamma-keyword"))

    y_resp = await memory_service.search_memory(
        app_name="appY", user_id="shared-user", query="beta"
    )
    assert len(y_resp.memories) == 1
    assert "beta" in y_resp.memories[0].content.parts[0].text

    x_resp = await memory_service.search_memory(
        app_name="appX", user_id="shared-user", query="alpha gamma"
    )
    # alpha should be purged; only gamma survives in appX.
    texts = [m.content.parts[0].text for m in x_resp.memories]
    assert any("gamma" in t for t in texts)
    assert all("alpha" not in t for t in texts)


async def test_search_preserves_event_metadata(
    memory_service: AerospikeMemoryService,
) -> None:
    """The returned MemoryEntry must preserve author and content from the
    original event, not just the extracted text."""
    from google.adk.events import Event, EventActions
    from google.adk.sessions import Session
    from google.genai import types as genai_types

    session = Session(
        id="meta-1",
        app_name="metaapp",
        user_id="u",
        events=[
            Event(
                invocation_id="i",
                author="model",
                content=genai_types.Content(
                    role="model",
                    parts=[genai_types.Part(text="reply about elephants")],
                ),
                actions=EventActions(),
            )
        ],
    )
    await memory_service.add_session_to_memory(session)

    resp = await memory_service.search_memory(
        app_name="metaapp", user_id="u", query="elephants"
    )
    assert len(resp.memories) == 1
    m = resp.memories[0]
    assert m.author == "model"
    assert m.content.parts[0].text == "reply about elephants"
