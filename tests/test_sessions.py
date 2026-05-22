"""Integration tests for AerospikeSessionService against a real Aerospike container.

These exercise ``create_session`` and ``get_session`` end-to-end. The
``aerospike_container`` fixture (in ``conftest.py``) handles container
lifecycle.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from adk_aerospike import AerospikeSessionService

pytestmark = pytest.mark.aerospike


@pytest_asyncio.fixture
async def session_service(aerospike_uri: str) -> AsyncIterator[AerospikeSessionService]:
    svc = AerospikeSessionService.from_uri(aerospike_uri)
    try:
        yield svc
    finally:
        svc.close()


async def test_create_and_get_round_trip(session_service: AerospikeSessionService) -> None:
    created = await session_service.create_session(
        app_name="testapp",
        user_id="alice",
        state={"theme": "dark", "count": 3},
    )

    assert created.app_name == "testapp"
    assert created.user_id == "alice"
    assert created.id  # auto-generated UUID
    assert created.state == {"theme": "dark", "count": 3}
    assert created.events == []

    fetched = await session_service.get_session(
        app_name="testapp",
        user_id="alice",
        session_id=created.id,
    )
    assert fetched is not None
    assert fetched.id == created.id
    assert fetched.state == {"theme": "dark", "count": 3}
    assert fetched.last_update_time == pytest.approx(created.last_update_time, abs=0.001)


async def test_explicit_session_id_is_honored(session_service: AerospikeSessionService) -> None:
    created = await session_service.create_session(
        app_name="testapp",
        user_id="alice",
        session_id="my-custom-id",
    )
    assert created.id == "my-custom-id"


async def test_missing_session_returns_none(session_service: AerospikeSessionService) -> None:
    result = await session_service.get_session(
        app_name="testapp",
        user_id="alice",
        session_id="does-not-exist",
    )
    assert result is None


async def test_duplicate_session_id_raises(session_service: AerospikeSessionService) -> None:
    """POLICY_EXISTS_CREATE means creating the same id twice surfaces an error
    rather than silently overwriting."""
    from aerospike import exception as ae

    await session_service.create_session(
        app_name="testapp", user_id="alice", session_id="dup"
    )
    with pytest.raises(ae.RecordExistsError):
        await session_service.create_session(
            app_name="testapp", user_id="alice", session_id="dup"
        )


async def test_temp_state_is_dropped(session_service: AerospikeSessionService) -> None:
    created = await session_service.create_session(
        app_name="testapp",
        user_id="alice",
        state={"keep": "yes", "temp:scratch": "throwaway"},
    )
    assert "keep" in created.state
    assert "temp:scratch" not in created.state

    fetched = await session_service.get_session(
        app_name="testapp", user_id="alice", session_id=created.id
    )
    assert fetched is not None
    assert "temp:scratch" not in fetched.state


async def test_app_state_shared_across_users(session_service: AerospikeSessionService) -> None:
    """``app:`` keys belong to the whole app — Bob should see what Alice wrote."""
    await session_service.create_session(
        app_name="testapp",
        user_id="alice",
        state={"app:tenant": "acme"},
    )
    bob = await session_service.create_session(
        app_name="testapp",
        user_id="bob",
    )
    bob_fetched = await session_service.get_session(
        app_name="testapp", user_id="bob", session_id=bob.id
    )
    assert bob_fetched is not None
    assert bob_fetched.state.get("app:tenant") == "acme"


async def test_user_state_isolated_per_user(session_service: AerospikeSessionService) -> None:
    """``user:`` keys belong to a single user — Bob must not see Alice's."""
    await session_service.create_session(
        app_name="testapp",
        user_id="alice",
        state={"user:nickname": "Allie"},
    )
    bob = await session_service.create_session(
        app_name="testapp",
        user_id="bob",
    )
    bob_fetched = await session_service.get_session(
        app_name="testapp", user_id="bob", session_id=bob.id
    )
    assert bob_fetched is not None
    assert "user:nickname" not in bob_fetched.state


async def test_user_state_persists_across_sessions(session_service: AerospikeSessionService) -> None:
    """``user:`` keys survive when a user starts a new session."""
    await session_service.create_session(
        app_name="testapp",
        user_id="alice",
        state={"user:nickname": "Allie"},
    )
    second = await session_service.create_session(
        app_name="testapp",
        user_id="alice",
    )
    fetched = await session_service.get_session(
        app_name="testapp", user_id="alice", session_id=second.id
    )
    assert fetched is not None
    assert fetched.state.get("user:nickname") == "Allie"


async def test_append_event_persists_and_hydrates(
    session_service: AerospikeSessionService,
) -> None:
    from google.adk.events import Event, EventActions
    from google.genai import types as genai_types

    session = await session_service.create_session(
        app_name="testapp", user_id="alice", session_id="ae-1"
    )
    event = Event(
        invocation_id="i1",
        author="user",
        content=genai_types.Content(
            role="user", parts=[genai_types.Part(text="hello world")]
        ),
        actions=EventActions(state_delta={"score": 42, "user:nick": "Allie"}),
    )
    returned = await session_service.append_event(session, event)
    assert returned is event

    fetched = await session_service.get_session(
        app_name="testapp", user_id="alice", session_id="ae-1"
    )
    assert fetched is not None
    assert len(fetched.events) == 1
    assert fetched.events[0].author == "user"
    assert fetched.state.get("score") == 42
    assert fetched.state.get("user:nick") == "Allie"


async def test_append_event_seq_is_monotonic(
    session_service: AerospikeSessionService,
) -> None:
    from google.adk.events import Event, EventActions
    from google.genai import types as genai_types

    session = await session_service.create_session(
        app_name="testapp", user_id="alice", session_id="ae-2"
    )
    for i in range(3):
        await session_service.append_event(
            session,
            Event(
                invocation_id=f"inv{i}",
                author="user",
                content=genai_types.Content(
                    role="user", parts=[genai_types.Part(text=f"msg {i}")]
                ),
                actions=EventActions(),
            ),
        )
    fetched = await session_service.get_session(
        app_name="testapp", user_id="alice", session_id="ae-2"
    )
    assert fetched is not None
    texts = [e.content.parts[0].text for e in fetched.events]
    assert texts == ["msg 0", "msg 1", "msg 2"]


async def test_get_session_respects_num_recent_events(
    session_service: AerospikeSessionService,
) -> None:
    from google.adk.events import Event, EventActions
    from google.adk.sessions.base_session_service import GetSessionConfig
    from google.genai import types as genai_types

    session = await session_service.create_session(
        app_name="testapp", user_id="alice", session_id="ae-3"
    )
    for i in range(5):
        await session_service.append_event(
            session,
            Event(
                invocation_id=f"x{i}",
                author="user",
                content=genai_types.Content(
                    role="user", parts=[genai_types.Part(text=str(i))]
                ),
                actions=EventActions(),
            ),
        )
    fetched = await session_service.get_session(
        app_name="testapp",
        user_id="alice",
        session_id="ae-3",
        config=GetSessionConfig(num_recent_events=2),
    )
    assert fetched is not None
    assert [e.content.parts[0].text for e in fetched.events] == ["3", "4"]


async def test_list_sessions_strips_events_and_state(
    session_service: AerospikeSessionService,
) -> None:
    from google.adk.events import Event, EventActions
    from google.genai import types as genai_types

    s1 = await session_service.create_session(
        app_name="lsapp", user_id="bob", state={"k": 1}
    )
    await session_service.create_session(app_name="lsapp", user_id="bob")
    await session_service.append_event(
        s1,
        Event(
            invocation_id="i",
            author="user",
            content=genai_types.Content(
                role="user", parts=[genai_types.Part(text="x")]
            ),
            actions=EventActions(),
        ),
    )

    resp = await session_service.list_sessions(app_name="lsapp", user_id="bob")
    assert len(resp.sessions) >= 2
    for s in resp.sessions:
        assert s.app_name == "lsapp"
        assert s.user_id == "bob"
        assert s.events == []
        assert s.state == {}


async def test_chunking_flushes_on_threshold(
    aerospike_uri: str,
) -> None:
    """When the tail exceeds the flush threshold, events spill to a chunk
    record. Hydration concatenates chunk + tail back in order."""
    from google.adk.events import Event, EventActions
    from google.genai import types as genai_types

    # Tiny threshold + tiny huge-event cap so we trigger flush after 2-3
    # small events instead of having to generate 256 KiB of test data.
    svc = AerospikeSessionService(
        client=AerospikeSessionService.from_uri(aerospike_uri)._client,
        namespace="test",
        flush_threshold_bytes=200,
        huge_event_bytes=10_000,
    )
    try:
        s = await svc.create_session(
            app_name="chunkapp", user_id="u", session_id="ck-1"
        )
        # Each event payload is ~80-100 bytes estimated; ~3 events triggers
        # flush at threshold 200.
        for i in range(7):
            await svc.append_event(
                s,
                Event(
                    invocation_id=f"i{i}",
                    author="user",
                    content=genai_types.Content(
                        role="user", parts=[genai_types.Part(text=f"msg-{i}")]
                    ),
                    actions=EventActions(),
                ),
            )

        fetched = await svc.get_session(
            app_name="chunkapp", user_id="u", session_id="ck-1"
        )
        assert fetched is not None
        texts = [e.content.parts[0].text for e in fetched.events]
        assert texts == [f"msg-{i}" for i in range(7)]
    finally:
        svc.close()


async def test_chunked_session_num_recent_events_from_tail_only(
    aerospike_uri: str,
) -> None:
    """num_recent_events small enough to be satisfied by the tail should not
    require chunk reads."""
    from google.adk.events import Event, EventActions
    from google.adk.sessions.base_session_service import GetSessionConfig
    from google.genai import types as genai_types

    svc = AerospikeSessionService(
        client=AerospikeSessionService.from_uri(aerospike_uri)._client,
        namespace="test",
        flush_threshold_bytes=200,
        huge_event_bytes=10_000,
    )
    try:
        s = await svc.create_session(
            app_name="chunkapp", user_id="u", session_id="ck-2"
        )
        for i in range(10):
            await svc.append_event(
                s,
                Event(
                    invocation_id=f"i{i}",
                    author="user",
                    content=genai_types.Content(
                        role="user", parts=[genai_types.Part(text=f"t-{i}")]
                    ),
                    actions=EventActions(),
                ),
            )
        # Ask for only the last 2 — should come out as the most recent two.
        fetched = await svc.get_session(
            app_name="chunkapp",
            user_id="u",
            session_id="ck-2",
            config=GetSessionConfig(num_recent_events=2),
        )
        assert fetched is not None
        texts = [e.content.parts[0].text for e in fetched.events]
        assert texts == ["t-8", "t-9"]
    finally:
        svc.close()


async def test_chunked_session_num_recent_events_walks_chunks(
    aerospike_uri: str,
) -> None:
    """num_recent_events larger than the tail size must walk chunks back."""
    from google.adk.events import Event, EventActions
    from google.adk.sessions.base_session_service import GetSessionConfig
    from google.genai import types as genai_types

    svc = AerospikeSessionService(
        client=AerospikeSessionService.from_uri(aerospike_uri)._client,
        namespace="test",
        flush_threshold_bytes=200,
        huge_event_bytes=10_000,
    )
    try:
        s = await svc.create_session(
            app_name="chunkapp", user_id="u", session_id="ck-3"
        )
        for i in range(10):
            await svc.append_event(
                s,
                Event(
                    invocation_id=f"i{i}",
                    author="user",
                    content=genai_types.Content(
                        role="user", parts=[genai_types.Part(text=f"x-{i}")]
                    ),
                    actions=EventActions(),
                ),
            )
        fetched = await svc.get_session(
            app_name="chunkapp",
            user_id="u",
            session_id="ck-3",
            config=GetSessionConfig(num_recent_events=8),
        )
        assert fetched is not None
        texts = [e.content.parts[0].text for e in fetched.events]
        assert texts == [f"x-{i}" for i in range(2, 10)]
    finally:
        svc.close()


async def test_delete_chunked_session_removes_all_chunks(
    aerospike_uri: str,
) -> None:
    """delete_session removes every sealed chunk plus the session record."""
    from google.adk.events import Event, EventActions
    from google.genai import types as genai_types

    svc = AerospikeSessionService(
        client=AerospikeSessionService.from_uri(aerospike_uri)._client,
        namespace="test",
        flush_threshold_bytes=200,
        huge_event_bytes=10_000,
    )
    try:
        s = await svc.create_session(
            app_name="chunkapp", user_id="u", session_id="ck-4"
        )
        for i in range(10):
            await svc.append_event(
                s,
                Event(
                    invocation_id=f"i{i}",
                    author="user",
                    content=genai_types.Content(
                        role="user", parts=[genai_types.Part(text=f"d-{i}")]
                    ),
                    actions=EventActions(),
                ),
            )

        await svc.delete_session(
            app_name="chunkapp", user_id="u", session_id="ck-4"
        )

        fetched = await svc.get_session(
            app_name="chunkapp", user_id="u", session_id="ck-4"
        )
        assert fetched is None

        # And no chunk records remain — recreating the session should give
        # empty history.
        fresh = await svc.create_session(
            app_name="chunkapp", user_id="u", session_id="ck-4"
        )
        fetched2 = await svc.get_session(
            app_name="chunkapp", user_id="u", session_id=fresh.id
        )
        assert fetched2 is not None
        assert fetched2.events == []
    finally:
        svc.close()


async def test_delete_session_cascades_to_events(
    session_service: AerospikeSessionService,
) -> None:
    from google.adk.events import Event, EventActions
    from google.genai import types as genai_types

    session = await session_service.create_session(
        app_name="delapp", user_id="alice", session_id="d-1"
    )
    await session_service.append_event(
        session,
        Event(
            invocation_id="i",
            author="user",
            content=genai_types.Content(
                role="user", parts=[genai_types.Part(text="bye")]
            ),
            actions=EventActions(),
        ),
    )
    await session_service.delete_session(
        app_name="delapp", user_id="alice", session_id="d-1"
    )
    fetched = await session_service.get_session(
        app_name="delapp", user_id="alice", session_id="d-1"
    )
    assert fetched is None
    # And the event row is gone too (no orphans).
    s2 = await session_service.create_session(
        app_name="delapp", user_id="alice", session_id="d-1"
    )
    fresh = await session_service.get_session(
        app_name="delapp", user_id="alice", session_id=s2.id
    )
    assert fresh is not None
    assert fresh.events == []
