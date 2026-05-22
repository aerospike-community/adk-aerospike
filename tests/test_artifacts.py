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
