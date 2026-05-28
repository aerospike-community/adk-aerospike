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


# ---- additional path coverage ------------------------------------------------


async def test_mixed_scope_state_partitions_correctly(
    session_service: AerospikeSessionService,
) -> None:
    """A single create with app:/user:/session/temp: keys must route each to
    its own set and drop temp."""
    s = await session_service.create_session(
        app_name="mixapp",
        user_id="alice",
        state={
            "session_only": 1,
            "app:shared": "A",
            "user:nick": "Allie",
            "temp:scratch": "drop",
        },
    )
    fetched = await session_service.get_session(
        app_name="mixapp", user_id="alice", session_id=s.id
    )
    assert fetched is not None
    assert fetched.state == {
        "session_only": 1,
        "app:shared": "A",
        "user:nick": "Allie",
    }


async def test_state_delta_via_append_event_writes_all_scopes(
    session_service: AerospikeSessionService,
) -> None:
    """state_delta on append_event must reach app/user/session sets and not
    leak temp keys."""
    from google.adk.events import Event, EventActions
    from google.genai import types as genai_types

    s = await session_service.create_session(
        app_name="deltaapp", user_id="alice", session_id="ds-1"
    )
    await session_service.append_event(
        s,
        Event(
            invocation_id="i",
            author="user",
            content=genai_types.Content(
                role="user", parts=[genai_types.Part(text="x")]
            ),
            actions=EventActions(
                state_delta={
                    "k": "session-val",
                    "app:tenant": "acme",
                    "user:lang": "en",
                    "temp:secret": "no",
                }
            ),
        ),
    )
    fetched = await session_service.get_session(
        app_name="deltaapp", user_id="alice", session_id="ds-1"
    )
    assert fetched is not None
    assert fetched.state.get("k") == "session-val"
    assert fetched.state.get("app:tenant") == "acme"
    assert fetched.state.get("user:lang") == "en"
    assert "temp:secret" not in fetched.state


async def test_partial_event_is_not_persisted(
    session_service: AerospikeSessionService,
) -> None:
    """Streaming partial events are in-flight LLM tokens; persisting them
    would create a write storm and store intermediate junk. Verify they
    bypass the storage path."""
    from google.adk.events import Event, EventActions
    from google.genai import types as genai_types

    s = await session_service.create_session(
        app_name="partialapp", user_id="alice", session_id="p-1"
    )
    partial = Event(
        invocation_id="inv",
        author="model",
        partial=True,
        content=genai_types.Content(
            role="model", parts=[genai_types.Part(text="streaming...")]
        ),
        actions=EventActions(),
    )
    await session_service.append_event(s, partial)

    fetched = await session_service.get_session(
        app_name="partialapp", user_id="alice", session_id="p-1"
    )
    assert fetched is not None
    assert fetched.events == []


async def test_manifest_tracks_create_and_delete(
    session_service: AerospikeSessionService,
) -> None:
    from adk_aerospike._internal.schema import Bins

    s = await session_service.create_session(
        app_name="mfapp", user_id="u", session_id="mf-1"
    )
    pk = session_service._manifest_pk("mfapp", "u")
    _, _, bins = session_service._client.get(pk)
    assert "mf-1" in (bins.get(Bins.SESSION_MANIFEST) or [])

    await session_service.delete_session(
        app_name="mfapp", user_id="u", session_id=s.id
    )
    try:
        _, _, bins = session_service._client.get(pk)
        assert "mf-1" not in (bins.get(Bins.SESSION_MANIFEST) or [])
    except Exception:
        pass


async def test_list_sessions_prunes_stale_manifest_ids(
    session_service: AerospikeSessionService,
) -> None:
    from aerospike import exception as ae
    from adk_aerospike._internal.schema import Bins

    await session_service.create_session(
        app_name="staleapp", user_id="u", session_id="gone-1"
    )
    session_pk = session_service._session_pk("staleapp", "u", "gone-1")
    session_service._client.remove(session_pk)

    resp = await session_service.list_sessions(app_name="staleapp", user_id="u")
    assert resp.sessions == []

    manifest_pk = session_service._manifest_pk("staleapp", "u")
    try:
        _, _, bins = session_service._client.get(manifest_pk)
    except ae.RecordNotFound:
        bins = {}
    assert "gone-1" not in (bins.get(Bins.SESSION_MANIFEST) or [])


async def test_list_sessions_uses_bin_projection(
    session_service: AerospikeSessionService, monkeypatch: pytest.MonkeyPatch
) -> None:
    from adk_aerospike.sessions import service as session_mod

    captured: list[tuple[str, ...]] = []

    async def _capture(
        self: AerospikeSessionService,
        keys: list[tuple[str, str, str]],
        bins: tuple[str, ...],
    ) -> dict[tuple[str, str, str], dict[str, object] | None]:
        from adk_aerospike._internal.schema import Bins

        captured.append(bins)
        return {
            k: {
                Bins.APP_NAME: "projapp",
                Bins.USER_ID: "u",
                Bins.SESSION_ID: "p-1",
                Bins.LAST_UPDATE: 0.0,
            }
            for k in keys
        }

    monkeypatch.setattr(
        AerospikeSessionService,
        "_batch_read_bins",
        _capture,
    )
    await session_service.create_session(
        app_name="projapp", user_id="u", session_id="p-1"
    )
    await session_service.list_sessions(app_name="projapp", user_id="u")
    assert captured
    assert captured[0] == session_mod._LIST_SESSION_BINS
    assert "events" not in captured[0]
    assert "state" not in captured[0]


async def test_list_sessions_isolated_per_app(
    session_service: AerospikeSessionService,
) -> None:
    """Each app has its own manifest key — sessions from appB never appear
    under appA."""
    await session_service.create_session(
        app_name="appA", user_id="shared-user", session_id="ai-A"
    )
    await session_service.create_session(
        app_name="appB", user_id="shared-user", session_id="ai-B"
    )

    only_a = await session_service.list_sessions(
        app_name="appA", user_id="shared-user"
    )
    ids = {s.id for s in only_a.sessions}
    assert "ai-A" in ids
    assert "ai-B" not in ids


async def test_list_sessions_does_not_return_chunk_records(
    aerospike_uri: str,
) -> None:
    """Chunks are not session rows; list_sessions (manifest path) returns one row."""
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
            app_name="chunklist", user_id="u", session_id="cl-1"
        )
        for i in range(10):
            await svc.append_event(
                s,
                Event(
                    invocation_id=f"i{i}",
                    author="user",
                    content=genai_types.Content(
                        role="user", parts=[genai_types.Part(text=f"e-{i}")]
                    ),
                    actions=EventActions(),
                ),
            )

        resp = await svc.list_sessions(app_name="chunklist", user_id="u")
        matching = [x for x in resp.sessions if x.id == "cl-1"]
        assert len(matching) == 1, (
            f"Expected exactly one session record for cl-1, got {len(matching)} — "
            "chunks may be leaking into the sec-index"
        )
    finally:
        svc.close()


async def test_huge_event_pre_flush_isolates_event_in_own_chunk(
    aerospike_uri: str,
) -> None:
    """An event whose estimated size exceeds ``huge_event_bytes`` triggers a
    pre-flush so the existing tail seals first; the huge event then lands in
    a fresh tail by itself. Verify history reconstructs in order."""
    from google.adk.events import Event, EventActions
    from google.genai import types as genai_types

    svc = AerospikeSessionService(
        client=AerospikeSessionService.from_uri(aerospike_uri)._client,
        namespace="test",
        flush_threshold_bytes=10_000,
        huge_event_bytes=500,
    )
    try:
        s = await svc.create_session(
            app_name="hugeapp", user_id="u", session_id="he-1"
        )
        # Small events first — they live in the tail.
        for i in range(3):
            await svc.append_event(
                s,
                Event(
                    invocation_id=f"sm{i}",
                    author="user",
                    content=genai_types.Content(
                        role="user", parts=[genai_types.Part(text=f"small-{i}")]
                    ),
                    actions=EventActions(),
                ),
            )
        # A "huge" event whose estimated size exceeds huge_event_bytes=500.
        # ~600 byte payload comfortably trips the threshold via str() overhead.
        big_text = "x" * 600
        await svc.append_event(
            s,
            Event(
                invocation_id="huge",
                author="user",
                content=genai_types.Content(
                    role="user", parts=[genai_types.Part(text=big_text)]
                ),
                actions=EventActions(),
            ),
        )
        await svc.append_event(
            s,
            Event(
                invocation_id="after",
                author="user",
                content=genai_types.Content(
                    role="user", parts=[genai_types.Part(text="after")]
                ),
                actions=EventActions(),
            ),
        )

        fetched = await svc.get_session(
            app_name="hugeapp", user_id="u", session_id="he-1"
        )
        assert fetched is not None
        texts = [e.content.parts[0].text for e in fetched.events]
        assert texts == ["small-0", "small-1", "small-2", big_text, "after"]
    finally:
        svc.close()


async def test_after_timestamp_filter_prunes_old_events(
    aerospike_uri: str,
) -> None:
    """``GetSessionConfig.after_timestamp`` drops events older than the cutoff.
    With chunking on, this exercises the ``ts_hi`` chunk-pruning fast path."""
    import time

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
            app_name="tsapp", user_id="u", session_id="ts-1"
        )
        # Three old events with explicit small timestamps → flushed into a chunk.
        for i in range(4):
            await svc.append_event(
                s,
                Event(
                    invocation_id=f"old{i}",
                    author="user",
                    timestamp=100.0 + i,  # well in the past
                    content=genai_types.Content(
                        role="user", parts=[genai_types.Part(text=f"old-{i}")]
                    ),
                    actions=EventActions(),
                ),
            )

        cutoff = time.time() + 100  # future-ish cutoff first, then events past it

        # Two "new" events with timestamps after the cutoff.
        for i in range(2):
            await svc.append_event(
                s,
                Event(
                    invocation_id=f"new{i}",
                    author="user",
                    timestamp=cutoff + 1 + i,
                    content=genai_types.Content(
                        role="user", parts=[genai_types.Part(text=f"new-{i}")]
                    ),
                    actions=EventActions(),
                ),
            )

        fetched = await svc.get_session(
            app_name="tsapp",
            user_id="u",
            session_id="ts-1",
            config=GetSessionConfig(after_timestamp=cutoff),
        )
        assert fetched is not None
        texts = [e.content.parts[0].text for e in fetched.events]
        # Only the post-cutoff events survive; chunk pruning by ts_hi means
        # we shouldn't materialise the old chunk at all.
        assert texts == ["new-0", "new-1"]
    finally:
        svc.close()


async def test_num_recent_events_zero_returns_empty(
    session_service: AerospikeSessionService,
) -> None:
    """Edge case: explicit ``num_recent_events=0`` short-circuits — we should
    not read any chunks or the tail."""
    from google.adk.events import Event, EventActions
    from google.adk.sessions.base_session_service import GetSessionConfig
    from google.genai import types as genai_types

    s = await session_service.create_session(
        app_name="zeroapp", user_id="u", session_id="z-1"
    )
    await session_service.append_event(
        s,
        Event(
            invocation_id="i",
            author="user",
            content=genai_types.Content(
                role="user", parts=[genai_types.Part(text="hello")]
            ),
            actions=EventActions(),
        ),
    )
    fetched = await session_service.get_session(
        app_name="zeroapp",
        user_id="u",
        session_id="z-1",
        config=GetSessionConfig(num_recent_events=0),
    )
    assert fetched is not None
    assert fetched.events == []


async def test_delete_missing_session_is_noop(
    session_service: AerospikeSessionService,
) -> None:
    """``delete_session`` on an unknown id must not raise."""
    await session_service.delete_session(
        app_name="ghostapp", user_id="ghost", session_id="never-existed"
    )


async def test_get_session_with_unrelated_user_returns_none(
    session_service: AerospikeSessionService,
) -> None:
    """Session keys include user_id — a different user can't read alice's
    session even if they guess the id."""
    s = await session_service.create_session(
        app_name="isoapp", user_id="alice", session_id="iso-1"
    )
    out = await session_service.get_session(
        app_name="isoapp", user_id="mallory", session_id=s.id
    )
    assert out is None


async def test_append_event_updates_last_update_time(
    session_service: AerospikeSessionService,
) -> None:
    """The session's ``last_update_time`` must reflect the latest event ts."""
    import time

    from google.adk.events import Event, EventActions
    from google.genai import types as genai_types

    s = await session_service.create_session(
        app_name="utapp", user_id="alice", session_id="ut-1"
    )
    created_ts = s.last_update_time
    time.sleep(0.01)
    await session_service.append_event(
        s,
        Event(
            invocation_id="i",
            author="user",
            content=genai_types.Content(
                role="user", parts=[genai_types.Part(text="hi")]
            ),
            actions=EventActions(),
        ),
    )
    fetched = await session_service.get_session(
        app_name="utapp", user_id="alice", session_id="ut-1"
    )
    assert fetched is not None
    assert fetched.last_update_time > created_ts


# ---- upstream adk-python contract tests (ported) ----------------------------


async def test_session_state_is_not_shared(
    session_service: AerospikeSessionService,
) -> None:
    """Ported from adk-python ``test_session_state_is_not_shared``."""
    from google.adk.events import Event, EventActions

    app_name = "upstream_sess"
    session1 = await session_service.create_session(
        app_name=app_name, user_id="u1", session_id="s1", state={"sk1": "v1"}
    )
    await session_service.append_event(
        session1,
        Event(
            invocation_id="inv1",
            author="user",
            actions=EventActions(state_delta={"sk2": "v2"}),
        ),
    )

    session1_got = await session_service.get_session(
        app_name=app_name, user_id="u1", session_id="s1"
    )
    assert session1_got is not None
    assert session1_got.state.get("sk1") == "v1"
    assert session1_got.state.get("sk2") == "v2"

    session1b = await session_service.create_session(
        app_name=app_name, user_id="u1", session_id="s1b"
    )
    assert session1b.state == {}


async def test_temp_state_visible_across_sequential_events(
    session_service: AerospikeSessionService,
) -> None:
    """Ported from adk-python ``test_temp_state_visible_across_sequential_events``."""
    from google.adk.events import Event, EventActions

    session = await session_service.create_session(
        app_name="upstream_temp", user_id="u1", session_id="s_seq"
    )
    event1 = Event(
        invocation_id="inv1",
        author="agent1",
        actions=EventActions(state_delta={"temp:output": "result_from_a1"}),
    )
    await session_service.append_event(session=session, event=event1)

    assert session.state.get("temp:output") == "result_from_a1"
    assert "temp:output" not in event1.actions.state_delta


async def test_temp_state_not_persisted_in_event_delta(
    session_service: AerospikeSessionService,
) -> None:
    """Ported from adk-python ``test_temp_state_is_not_persisted_in_state_or_events``."""
    from google.adk.events import Event, EventActions

    session = await session_service.create_session(
        app_name="upstream_temp2", user_id="u1", session_id="s1"
    )
    event = Event(
        invocation_id="inv1",
        author="user",
        actions=EventActions(state_delta={"temp:k1": "v1", "sk": "v2"}),
    )
    await session_service.append_event(session=session, event=event)

    assert session.state.get("temp:k1") == "v1"
    assert session.state.get("sk") == "v2"
    assert "temp:k1" not in event.actions.state_delta
    assert event.actions.state_delta.get("sk") == "v2"


async def test_append_event_bytes(
    session_service: AerospikeSessionService,
) -> None:
    """Ported from adk-python ``test_append_event_bytes``."""
    from google.adk.events import Event
    from google.genai import types

    session = await session_service.create_session(
        app_name="upstream_bytes", user_id="user"
    )
    test_content = types.Content(
        role="user",
        parts=[types.Part.from_bytes(data=b"test_image_data", mime_type="image/png")],
    )
    test_grounding_metadata = types.GroundingMetadata(
        search_entry_point=types.SearchEntryPoint(sdk_blob=b"test_sdk_blob")
    )
    event = Event(
        invocation_id="invocation",
        author="user",
        content=test_content,
        grounding_metadata=test_grounding_metadata,
    )
    await session_service.append_event(session=session, event=event)

    assert session.events[0].content == test_content

    fetched = await session_service.get_session(
        app_name="upstream_bytes", user_id="user", session_id=session.id
    )
    assert fetched is not None
    assert len(fetched.events) == 1
    assert fetched.events[0].content == test_content
    assert fetched.events[0].grounding_metadata == test_grounding_metadata


async def test_append_event_complete(
    session_service: AerospikeSessionService,
) -> None:
    """Ported from adk-python ``test_append_event_complete``."""
    from google.adk.events import Event, EventActions
    from google.genai import types

    session = await session_service.create_session(
        app_name="upstream_complete", user_id="user"
    )
    event = Event(
        invocation_id="invocation",
        author="user",
        content=types.Content(role="user", parts=[types.Part(text="test_text")]),
        turn_complete=True,
        partial=False,
        actions=EventActions(
            artifact_delta={"file": 0},
            transfer_to_agent="agent",
            escalate=True,
        ),
        long_running_tool_ids={"tool1"},
        error_code="error_code",
        error_message="error_message",
        interrupted=True,
        grounding_metadata=types.GroundingMetadata(web_search_queries=["query1"]),
        usage_metadata=types.GenerateContentResponseUsageMetadata(
            prompt_token_count=1, candidates_token_count=1, total_token_count=2
        ),
        citation_metadata=types.CitationMetadata(),
        custom_metadata={"custom_key": "custom_value"},
        timestamp=1700000000.123,
        input_transcription=types.Transcription(
            text="input transcription",
            finished=True,
        ),
        output_transcription=types.Transcription(
            text="output transcription",
            finished=True,
        ),
    )
    await session_service.append_event(session=session, event=event)

    fetched = await session_service.get_session(
        app_name="upstream_complete", user_id="user", session_id=session.id
    )
    assert fetched == session


async def test_list_sessions_all_users(
    session_service: AerospikeSessionService,
) -> None:
    """Ported from adk-python ``test_list_sessions_all_users``.

    ``list_sessions`` here returns metadata only (empty ``state`` / ``events``).
    """
    app_name = "upstream_list_all"
    user_id_1 = "user1"
    user_id_2 = "user2"

    for session_id in ("session1a", "session1b"):
        await session_service.create_session(
            app_name=app_name,
            user_id=user_id_1,
            session_id=session_id,
            state={"key": f"value{session_id}"},
        )
    await session_service.create_session(
        app_name=app_name,
        user_id=user_id_2,
        session_id="session2a",
        state={"key": "value2a"},
    )

    sessions_1 = (
        await session_service.list_sessions(app_name=app_name, user_id=user_id_1)
    ).sessions
    assert len(sessions_1) == 2
    assert {s.id for s in sessions_1} == {"session1a", "session1b"}

    sessions_2 = (
        await session_service.list_sessions(app_name=app_name, user_id=user_id_2)
    ).sessions
    assert len(sessions_2) == 1
    assert sessions_2[0].id == "session2a"

    sessions_all = (
        await session_service.list_sessions(app_name=app_name, user_id=None)
    ).sessions
    assert len(sessions_all) == 3
    assert {s.id for s in sessions_all} == {"session1a", "session1b", "session2a"}
    for s in sessions_all:
        assert s.state == {}
        assert s.events == []


async def test_create_and_list_sessions_returns_ids(
    session_service: AerospikeSessionService,
) -> None:
    """Ported from adk-python ``test_create_and_list_sessions`` (metadata shape)."""
    app_name = "upstream_list_ids"
    user_id = "test_user"
    session_ids = [f"session{i}" for i in range(5)]
    for session_id in session_ids:
        await session_service.create_session(
            app_name=app_name,
            user_id=user_id,
            session_id=session_id,
            state={"key": "value" + session_id},
        )

    sessions = (
        await session_service.list_sessions(app_name=app_name, user_id=user_id)
    ).sessions
    assert len(sessions) == len(session_ids)
    assert {s.id for s in sessions} == set(session_ids)


async def test_get_session_with_combined_config(
    session_service: AerospikeSessionService,
) -> None:
    """Ported from adk-python ``test_get_session_with_config`` (combined filters)."""
    from google.adk.events import Event
    from google.adk.sessions.base_session_service import GetSessionConfig

    app_name = "upstream_cfg"
    user_id = "user"
    num_test_events = 5
    session = await session_service.create_session(app_name=app_name, user_id=user_id)
    for i in range(1, num_test_events + 1):
        await session_service.append_event(session, Event(author="user", timestamp=i))

    fetched = await session_service.get_session(
        app_name=app_name, user_id=user_id, session_id=session.id
    )
    assert fetched is not None
    assert len(fetched.events) == num_test_events

    config = GetSessionConfig(num_recent_events=3)
    fetched = await session_service.get_session(
        app_name=app_name, user_id=user_id, session_id=session.id, config=config
    )
    assert fetched is not None
    assert len(fetched.events) == 3
    assert fetched.events[0].timestamp == num_test_events - 3 + 1

    after_timestamp = 4.0
    config = GetSessionConfig(after_timestamp=after_timestamp)
    fetched = await session_service.get_session(
        app_name=app_name, user_id=user_id, session_id=session.id, config=config
    )
    assert fetched is not None
    assert len(fetched.events) == num_test_events - int(after_timestamp) + 1
    assert fetched.events[0].timestamp == after_timestamp

    config = GetSessionConfig(after_timestamp=num_test_events * 10)
    fetched = await session_service.get_session(
        app_name=app_name, user_id=user_id, session_id=session.id, config=config
    )
    assert fetched is not None
    assert not fetched.events

    config = GetSessionConfig(
        after_timestamp=after_timestamp, num_recent_events=3
    )
    fetched = await session_service.get_session(
        app_name=app_name, user_id=user_id, session_id=session.id, config=config
    )
    assert fetched is not None
    assert len(fetched.events) == num_test_events - int(after_timestamp) + 1
