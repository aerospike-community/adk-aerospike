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


# ---- upstream adk-python contract tests (ported) ----------------------------

_MOCK_APP_NAME = "test-app"
_MOCK_USER_ID = "test-user"
_MOCK_OTHER_USER_ID = "another-user"


def _mock_session_1():
    from google.adk.events import Event
    from google.adk.sessions import Session
    from google.genai import types as genai_types

    return Session(
        app_name=_MOCK_APP_NAME,
        user_id=_MOCK_USER_ID,
        id="session-1",
        last_update_time=1000,
        events=[
            Event(
                id="event-1a",
                invocation_id="inv-1",
                author="user",
                timestamp=12345,
                content=genai_types.Content(
                    parts=[genai_types.Part(text="The ADK is a great toolkit.")]
                ),
            ),
            Event(
                id="event-1b",
                invocation_id="inv-2",
                author="user",
                timestamp=12346,
            ),
            Event(
                id="event-1c",
                invocation_id="inv-3",
                author="model",
                timestamp=12347,
                content=genai_types.Content(
                    parts=[
                        genai_types.Part(
                            text="I agree. The Agent Development Kit (ADK) rocks!"
                        )
                    ]
                ),
            ),
        ],
    )


def _mock_session_2():
    from google.adk.events import Event
    from google.adk.sessions import Session
    from google.genai import types as genai_types

    return Session(
        app_name=_MOCK_APP_NAME,
        user_id=_MOCK_USER_ID,
        id="session-2",
        last_update_time=2000,
        events=[
            Event(
                id="event-2a",
                invocation_id="inv-4",
                author="user",
                timestamp=54321,
                content=genai_types.Content(
                    parts=[genai_types.Part(text="I like to code in Python.")]
                ),
            ),
        ],
    )


def _mock_session_different_user():
    from google.adk.events import Event
    from google.adk.sessions import Session
    from google.genai import types as genai_types

    return Session(
        app_name=_MOCK_APP_NAME,
        user_id=_MOCK_OTHER_USER_ID,
        id="session-3",
        last_update_time=3000,
        events=[
            Event(
                id="event-3a",
                invocation_id="inv-5",
                author="user",
                timestamp=60000,
                content=genai_types.Content(parts=[genai_types.Part(text="This is a secret.")]),
            ),
        ],
    )


def _mock_session_with_no_events():
    from google.adk.sessions import Session

    return Session(
        app_name=_MOCK_APP_NAME,
        user_id=_MOCK_USER_ID,
        id="session-4",
        last_update_time=4000,
    )


async def test_add_session_to_memory_skips_empty_events(
    memory_service: AerospikeMemoryService,
) -> None:
    """Ported from adk-python ``test_add_session_to_memory`` (behavioral)."""
    await memory_service.add_session_to_memory(_mock_session_1())

    toolkit = await memory_service.search_memory(
        app_name=_MOCK_APP_NAME, user_id=_MOCK_USER_ID, query="toolkit ADK"
    )
    assert len(toolkit.memories) == 2
    texts = {m.content.parts[0].text for m in toolkit.memories}
    assert "The ADK is a great toolkit." in texts
    assert "I agree. The Agent Development Kit (ADK) rocks!" in texts


async def test_add_events_to_memory_with_explicit_events(
    memory_service: AerospikeMemoryService,
) -> None:
    """Ported from adk-python ``test_add_events_to_memory_with_explicit_events``."""
    session = _mock_session_1()
    await memory_service.add_events_to_memory(
        app_name=session.app_name,
        user_id=session.user_id,
        session_id=session.id,
        events=[session.events[0]],
    )

    result = await memory_service.search_memory(
        app_name=_MOCK_APP_NAME, user_id=_MOCK_USER_ID, query="toolkit"
    )
    assert len(result.memories) == 1
    assert result.memories[0].content.parts[0].text == "The ADK is a great toolkit."


async def test_add_events_to_memory_without_session_id(
    memory_service: AerospikeMemoryService,
) -> None:
    """Ported from adk-python ``test_add_events_to_memory_without_session_id``."""
    session = _mock_session_1()
    await memory_service.add_events_to_memory(
        app_name=session.app_name,
        user_id=session.user_id,
        events=[session.events[0]],
    )

    result = await memory_service.search_memory(
        app_name=_MOCK_APP_NAME, user_id=_MOCK_USER_ID, query="toolkit"
    )
    assert len(result.memories) == 1
    assert result.memories[0].content.parts[0].text == "The ADK is a great toolkit."


async def test_add_events_to_memory_appends_without_replacing(
    memory_service: AerospikeMemoryService,
) -> None:
    """Ported from adk-python ``test_add_events_to_memory_appends_without_replacing``."""
    from google.adk.events import Event
    from google.genai import types as genai_types

    session = _mock_session_1()
    await memory_service.add_session_to_memory(session)

    new_event = Event(
        id="event-1d",
        invocation_id="inv-6",
        author="user",
        timestamp=12348,
        content=genai_types.Content(parts=[genai_types.Part(text="A new fact.")]),
    )
    await memory_service.add_events_to_memory(
        app_name=session.app_name,
        user_id=session.user_id,
        session_id=session.id,
        events=[new_event],
    )

    result = await memory_service.search_memory(
        app_name=_MOCK_APP_NAME, user_id=_MOCK_USER_ID, query="toolkit fact ADK"
    )
    texts = {m.content.parts[0].text for m in result.memories}
    assert "The ADK is a great toolkit." in texts
    assert "I agree. The Agent Development Kit (ADK) rocks!" in texts
    assert "A new fact." in texts


async def test_add_events_to_memory_deduplicates_event_ids(
    memory_service: AerospikeMemoryService,
) -> None:
    """Ported from adk-python ``test_add_events_to_memory_deduplicates_event_ids``."""
    from google.adk.events import Event
    from google.genai import types as genai_types

    session = _mock_session_1()
    await memory_service.add_session_to_memory(session)

    duplicate_event = Event(
        id="event-1a",
        invocation_id="inv-7",
        author="user",
        timestamp=12349,
        content=genai_types.Content(
            parts=[genai_types.Part(text="Updated duplicate text.")]
        ),
    )
    await memory_service.add_events_to_memory(
        app_name=session.app_name,
        user_id=session.user_id,
        session_id=session.id,
        events=[duplicate_event],
    )

    result = await memory_service.search_memory(
        app_name=_MOCK_APP_NAME, user_id=_MOCK_USER_ID, query="toolkit duplicate ADK"
    )
    assert len(result.memories) == 2
    assert all("Updated duplicate text." not in m.content.parts[0].text for m in result.memories)


async def test_add_session_with_no_events_to_memory(
    memory_service: AerospikeMemoryService,
) -> None:
    """Ported from adk-python ``test_add_session_with_no_events_to_memory``."""
    await memory_service.add_session_to_memory(_mock_session_with_no_events())

    result = await memory_service.search_memory(
        app_name=_MOCK_APP_NAME, user_id=_MOCK_USER_ID, query="anything"
    )
    assert result.memories == []


async def test_search_memory_simple_match(
    memory_service: AerospikeMemoryService,
) -> None:
    """Ported from adk-python ``test_search_memory_simple_match``."""
    await memory_service.add_session_to_memory(_mock_session_1())
    await memory_service.add_session_to_memory(_mock_session_2())

    result = await memory_service.search_memory(
        app_name=_MOCK_APP_NAME, user_id=_MOCK_USER_ID, query="Python"
    )
    assert len(result.memories) == 1
    assert result.memories[0].content.parts[0].text == "I like to code in Python."
    assert result.memories[0].author == "user"


async def test_search_memory_case_insensitive_match(
    memory_service: AerospikeMemoryService,
) -> None:
    """Ported from adk-python ``test_search_memory_case_insensitive_match``."""
    await memory_service.add_session_to_memory(_mock_session_1())

    result = await memory_service.search_memory(
        app_name=_MOCK_APP_NAME, user_id=_MOCK_USER_ID, query="development"
    )
    assert len(result.memories) == 1
    assert (
        result.memories[0].content.parts[0].text
        == "I agree. The Agent Development Kit (ADK) rocks!"
    )


async def test_search_memory_multiple_matches(
    memory_service: AerospikeMemoryService,
) -> None:
    """Ported from adk-python ``test_search_memory_multiple_matches``."""
    await memory_service.add_session_to_memory(_mock_session_1())

    result = await memory_service.search_memory(
        app_name=_MOCK_APP_NAME, user_id=_MOCK_USER_ID, query="How about ADK?"
    )
    assert len(result.memories) == 2
    texts = {memory.content.parts[0].text for memory in result.memories}
    assert "The ADK is a great toolkit." in texts
    assert "I agree. The Agent Development Kit (ADK) rocks!" in texts


async def test_search_memory_no_match_upstream(
    memory_service: AerospikeMemoryService,
) -> None:
    """Ported from adk-python ``test_search_memory_no_match``."""
    await memory_service.add_session_to_memory(_mock_session_1())

    result = await memory_service.search_memory(
        app_name=_MOCK_APP_NAME, user_id=_MOCK_USER_ID, query="nonexistent"
    )
    assert not result.memories


async def test_search_memory_is_scoped_by_user_upstream(
    memory_service: AerospikeMemoryService,
) -> None:
    """Ported from adk-python ``test_search_memory_is_scoped_by_user``."""
    await memory_service.add_session_to_memory(_mock_session_1())
    await memory_service.add_session_to_memory(_mock_session_different_user())

    result = await memory_service.search_memory(
        app_name=_MOCK_APP_NAME, user_id=_MOCK_USER_ID, query="secret"
    )
    assert not result.memories

    result_other_user = await memory_service.search_memory(
        app_name=_MOCK_APP_NAME, user_id=_MOCK_OTHER_USER_ID, query="secret"
    )
    assert len(result_other_user.memories) == 1
    assert result_other_user.memories[0].content.parts[0].text == "This is a secret."
