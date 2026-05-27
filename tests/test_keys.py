"""Unit tests for key construction — no server required."""

from __future__ import annotations

from adk_aerospike._internal.keys import (
    ARTIFACT_HEAD_SUFFIX,
    SEP,
    app_state_key,
    artifact_head_key,
    artifact_key,
    chunk_key,
    session_key,
    user_state_key,
)


def test_session_key_format():
    assert session_key("app1", "user1", "sess1") == f"app1{SEP}user1{SEP}sess1"


def test_chunk_key_zero_padded():
    k = chunk_key("app1", "user1", "sess1", 7)
    assert k.endswith(f"{SEP}c:00000007")


def test_chunk_key_sortable():
    # Lexicographic ordering of the zero-padded suffix must match numeric ordering.
    keys = [chunk_key("a", "u", "s", i) for i in (1, 12, 3, 100)]
    assert sorted(keys) == [
        chunk_key("a", "u", "s", 1),
        chunk_key("a", "u", "s", 3),
        chunk_key("a", "u", "s", 12),
        chunk_key("a", "u", "s", 100),
    ]


def test_chunk_key_distinct_from_session_key():
    # Session keys have 3 SEP-delimited fields; chunk keys have 4 with the
    # last field prefixed "c:" — no ambiguity.
    assert session_key("a", "u", "s") != chunk_key("a", "u", "s", 0)


def test_app_state_key():
    assert app_state_key("myapp") == "myapp"


def test_user_state_key():
    assert user_state_key("app1", "user1") == f"app1{SEP}user1"


def test_artifact_key_has_version_suffix():
    k = artifact_key("a", "u", "s", "file.png", 3)
    assert k.endswith(f"{SEP}00000003")
    assert "file.png" in k


def test_artifact_head_key_distinct_from_versioned_key():
    head = artifact_head_key("a", "u", "s", "file.png")
    body = artifact_key("a", "u", "s", "file.png", 0)
    assert head.endswith(ARTIFACT_HEAD_SUFFIX)
    assert head != body
