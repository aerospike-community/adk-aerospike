"""Integration tests for AerospikeArtifactService."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from google.genai import types as genai_types

from adk_aerospike import AerospikeArtifactService

pytestmark = pytest.mark.aerospike


@pytest_asyncio.fixture
async def artifact_service(aerospike_uri: str) -> AsyncIterator[AerospikeArtifactService]:
    svc = AerospikeArtifactService.from_uri(aerospike_uri)
    try:
        yield svc
    finally:
        svc.close()


async def test_save_and_load_artifact(
    artifact_service: AerospikeArtifactService,
) -> None:
    part = genai_types.Part(
        inline_data=genai_types.Blob(mime_type="image/png", data=b"\x89PNG\r\n\x1a\n")
    )
    v = await artifact_service.save_artifact(
        app_name="a", user_id="u", session_id="s", filename="logo.png", artifact=part
    )
    assert v == 0

    loaded = await artifact_service.load_artifact(
        app_name="a", user_id="u", session_id="s", filename="logo.png"
    )
    assert loaded is not None
    assert loaded.inline_data is not None
    assert loaded.inline_data.mime_type == "image/png"
    assert loaded.inline_data.data == b"\x89PNG\r\n\x1a\n"


async def test_versioning_is_monotonic(
    artifact_service: AerospikeArtifactService,
) -> None:
    for expected in (0, 1, 2):
        v = await artifact_service.save_artifact(
            app_name="a",
            user_id="u",
            session_id="s2",
            filename="notes.txt",
            artifact=genai_types.Part(text=f"v{expected}"),
        )
        assert v == expected

    versions = await artifact_service.list_versions(
        app_name="a", user_id="u", session_id="s2", filename="notes.txt"
    )
    assert versions == [0, 1, 2]

    latest = await artifact_service.load_artifact(
        app_name="a", user_id="u", session_id="s2", filename="notes.txt"
    )
    assert latest is not None and latest.text == "v2"

    older = await artifact_service.load_artifact(
        app_name="a", user_id="u", session_id="s2", filename="notes.txt", version=1
    )
    assert older is not None and older.text == "v1"


async def test_load_missing_returns_none(
    artifact_service: AerospikeArtifactService,
) -> None:
    out = await artifact_service.load_artifact(
        app_name="a", user_id="u", session_id="s", filename="missing.txt"
    )
    assert out is None


async def test_user_scoped_filename(
    artifact_service: AerospikeArtifactService,
) -> None:
    """``user:`` filename prefix → cross-session-visible artifact."""
    await artifact_service.save_artifact(
        app_name="a",
        user_id="u",
        session_id="sx",
        filename="user:profile.json",
        artifact=genai_types.Part(text="{}"),
    )
    # Different session — still visible.
    loaded = await artifact_service.load_artifact(
        app_name="a", user_id="u", session_id="sy", filename="user:profile.json"
    )
    assert loaded is not None and loaded.text == "{}"


async def test_list_artifact_keys_merges_scopes(
    artifact_service: AerospikeArtifactService,
) -> None:
    await artifact_service.save_artifact(
        app_name="b", user_id="u", session_id="s", filename="a.txt",
        artifact=genai_types.Part(text="A"),
    )
    await artifact_service.save_artifact(
        app_name="b", user_id="u", session_id="s", filename="user:b.txt",
        artifact=genai_types.Part(text="B"),
    )

    with_session = await artifact_service.list_artifact_keys(
        app_name="b", user_id="u", session_id="s"
    )
    assert with_session == ["a.txt", "user:b.txt"]

    without_session = await artifact_service.list_artifact_keys(
        app_name="b", user_id="u"
    )
    assert without_session == ["user:b.txt"]


async def test_delete_artifact_removes_all_versions(
    artifact_service: AerospikeArtifactService,
) -> None:
    for i in range(3):
        await artifact_service.save_artifact(
            app_name="c", user_id="u", session_id="s", filename="tmp.txt",
            artifact=genai_types.Part(text=str(i)),
        )
    await artifact_service.delete_artifact(
        app_name="c", user_id="u", session_id="s", filename="tmp.txt"
    )
    out = await artifact_service.load_artifact(
        app_name="c", user_id="u", session_id="s", filename="tmp.txt"
    )
    assert out is None
    assert await artifact_service.list_versions(
        app_name="c", user_id="u", session_id="s", filename="tmp.txt"
    ) == []


# ---- additional path coverage ------------------------------------------------


async def test_load_specific_version_returns_that_version(
    artifact_service: AerospikeArtifactService,
) -> None:
    """Each saved version is its own record; ``load_artifact(version=N)``
    must return that record, not the latest."""
    for i in range(4):
        await artifact_service.save_artifact(
            app_name="vapp",
            user_id="u",
            session_id="vs",
            filename="picks.txt",
            artifact=genai_types.Part(text=f"version-{i}"),
        )
    for i in range(4):
        got = await artifact_service.load_artifact(
            app_name="vapp", user_id="u", session_id="vs",
            filename="picks.txt", version=i,
        )
        assert got is not None and got.text == f"version-{i}"


async def test_get_artifact_version_metadata(
    artifact_service: AerospikeArtifactService,
) -> None:
    """``get_artifact_version`` returns ArtifactVersion metadata (URI, mime,
    create_time) without the payload bytes."""
    await artifact_service.save_artifact(
        app_name="metaapp",
        user_id="u",
        session_id="ms",
        filename="doc.txt",
        artifact=genai_types.Part(text="hello"),
        custom_metadata={"author": "alice"},
    )
    meta = await artifact_service.get_artifact_version(
        app_name="metaapp", user_id="u", session_id="ms", filename="doc.txt"
    )
    assert meta is not None
    assert meta.version == 0
    assert meta.mime_type == "text/plain"
    assert meta.custom_metadata == {"author": "alice"}
    assert "metaapp" in meta.canonical_uri
    assert "ms" in meta.canonical_uri  # session-scoped URI
    assert meta.create_time > 0


async def test_user_scoped_canonical_uri_omits_session(
    artifact_service: AerospikeArtifactService,
) -> None:
    """User-scoped artifacts produce a session-less canonical URI — distinct
    from the session-scoped one."""
    await artifact_service.save_artifact(
        app_name="cuapp",
        user_id="u",
        session_id="ignored",
        filename="user:profile.json",
        artifact=genai_types.Part(text="{}"),
    )
    meta = await artifact_service.get_artifact_version(
        app_name="cuapp", user_id="u", session_id="any", filename="user:profile.json"
    )
    assert meta is not None
    assert "/sessions/" not in meta.canonical_uri
    assert "users/u/artifacts/user:profile.json" in meta.canonical_uri


async def test_user_scope_and_session_scope_dont_collide(
    artifact_service: AerospikeArtifactService,
) -> None:
    """A user-scoped file and a session-scoped file with the same *bare* name
    live in different key namespaces (user-scope uses the ``"user"`` sentinel
    in the session slot)."""
    await artifact_service.save_artifact(
        app_name="colapp", user_id="u", session_id="s1",
        filename="user:shared.txt",
        artifact=genai_types.Part(text="user-scope"),
    )
    await artifact_service.save_artifact(
        app_name="colapp", user_id="u", session_id="s1",
        filename="shared.txt",
        artifact=genai_types.Part(text="session-scope"),
    )

    user_loaded = await artifact_service.load_artifact(
        app_name="colapp", user_id="u", session_id="s1",
        filename="user:shared.txt",
    )
    sess_loaded = await artifact_service.load_artifact(
        app_name="colapp", user_id="u", session_id="s1",
        filename="shared.txt",
    )
    assert user_loaded is not None and user_loaded.text == "user-scope"
    assert sess_loaded is not None and sess_loaded.text == "session-scope"


async def test_list_versions_missing_returns_empty(
    artifact_service: AerospikeArtifactService,
) -> None:
    versions = await artifact_service.list_versions(
        app_name="empty", user_id="u", session_id="s", filename="nope.txt"
    )
    assert versions == []


async def test_delete_artifact_missing_is_noop(
    artifact_service: AerospikeArtifactService,
) -> None:
    """Deleting an artifact that doesn't exist must not raise."""
    await artifact_service.delete_artifact(
        app_name="noop", user_id="u", session_id="s", filename="never.txt"
    )


async def test_session_scope_isolated_across_sessions(
    artifact_service: AerospikeArtifactService,
) -> None:
    """A session-scoped artifact in session A must not be visible from
    session B (only user-scoped survive the boundary)."""
    await artifact_service.save_artifact(
        app_name="sepapp", user_id="u", session_id="A",
        filename="local.txt", artifact=genai_types.Part(text="only-A"),
    )
    other = await artifact_service.load_artifact(
        app_name="sepapp", user_id="u", session_id="B", filename="local.txt"
    )
    assert other is None


async def test_save_requires_session_for_session_scoped_file(
    artifact_service: AerospikeArtifactService,
) -> None:
    """Non-``user:``-prefixed filenames require a session_id; the service
    should surface the underlying validation error."""
    from google.adk.errors.input_validation_error import InputValidationError

    with pytest.raises(InputValidationError):
        await artifact_service.save_artifact(
            app_name="vapp",
            user_id="u",
            session_id=None,
            filename="needs-session.txt",
            artifact=genai_types.Part(text="x"),
        )


async def test_list_versions_isolated_across_apps_with_same_filename(
    artifact_service: AerospikeArtifactService,
) -> None:
    """Composite ``aus`` index guarantees app A's ``shared.txt`` versions are
    invisible to app B's queries — even though both apps have a file with
    the same filename and the same user_id."""
    await artifact_service.save_artifact(
        app_name="tenantA", user_id="u", session_id="s",
        filename="shared.txt", artifact=genai_types.Part(text="A1"),
    )
    await artifact_service.save_artifact(
        app_name="tenantA", user_id="u", session_id="s",
        filename="shared.txt", artifact=genai_types.Part(text="A2"),
    )
    await artifact_service.save_artifact(
        app_name="tenantB", user_id="u", session_id="s",
        filename="shared.txt", artifact=genai_types.Part(text="B1"),
    )

    a_versions = await artifact_service.list_versions(
        app_name="tenantA", user_id="u", session_id="s", filename="shared.txt"
    )
    b_versions = await artifact_service.list_versions(
        app_name="tenantB", user_id="u", session_id="s", filename="shared.txt"
    )
    assert a_versions == [0, 1]
    assert b_versions == [0]


async def test_list_versions_returns_sorted(
    artifact_service: AerospikeArtifactService,
) -> None:
    """``list_versions`` must return sorted ints regardless of insert order
    (versions are derived monotonically but secondary-index results aren't
    ordered)."""
    for _ in range(5):
        await artifact_service.save_artifact(
            app_name="sortapp", user_id="u", session_id="ss",
            filename="series.txt", artifact=genai_types.Part(text="x"),
        )
    out = await artifact_service.list_versions(
        app_name="sortapp", user_id="u", session_id="ss", filename="series.txt"
    )
    assert out == [0, 1, 2, 3, 4]
