"""Unit tests for the codec (Event <-> inline dict) and tokenizer.

These run without an Aerospike server — they protect the on-record event shape
that lives on every session record and chunk, so a regression here is a hot
silent breaking change for everyone with persisted data.
"""

from __future__ import annotations

import pytest
from google.adk.events import Event, EventActions
from google.genai import types as genai_types

from adk_aerospike._internal.codec import (
    estimate_event_size,
    event_from_inline_dict,
    event_to_inline_dict,
    extract_event_text,
)
from adk_aerospike._internal.schema import (
    EVENT_SCHEMA_VERSION,
    BinName,
    EventFieldName,
)
from adk_aerospike._internal.keys import (
    USER_SCOPE_SID,
    artifact_key,
    artifact_scope_id,
    memory_key,
)
from adk_aerospike.memory.service import _tokenize


def _make_event(
    *,
    text: str = "hi",
    author: str = "user",
    invocation_id: str = "inv",
    actions: EventActions | None = None,
    branch: str | None = None,
) -> Event:
    return Event(
        invocation_id=invocation_id,
        author=author,
        content=genai_types.Content(role=author, parts=[genai_types.Part(text=text)]),
        actions=actions or EventActions(),
        branch=branch,
    )


# ---- codec round-trip ---------------------------------------------------------


def test_event_round_trip_preserves_text_and_author():
    ev = _make_event(text="hello world", author="model")
    d = event_to_inline_dict(ev)
    back = event_from_inline_dict(d)

    assert back.author == "model"
    assert back.content is not None and back.content.parts is not None
    assert back.content.parts[0].text == "hello world"


def test_event_round_trip_preserves_actions_state_delta():
    ev = _make_event(
        actions=EventActions(state_delta={"k": "v", "n": 1, "user:nick": "x"})
    )
    back = event_from_inline_dict(event_to_inline_dict(ev))

    assert back.actions.state_delta == {"k": "v", "n": 1, "user:nick": "x"}


def test_event_round_trip_preserves_branch():
    ev = _make_event(branch="alt-1")
    back = event_from_inline_dict(event_to_inline_dict(ev))
    assert back.branch == "alt-1"


def test_event_round_trip_preserves_id_and_timestamp():
    ev = _make_event()
    # Event auto-assigns id + timestamp at construction.
    d = event_to_inline_dict(ev)
    back = event_from_inline_dict(d)

    assert back.id == ev.id
    assert back.timestamp == pytest.approx(ev.timestamp, abs=1e-6)


def test_event_round_trip_multi_part_content():
    ev = Event(
        invocation_id="inv",
        author="user",
        content=genai_types.Content(
            role="user",
            parts=[
                genai_types.Part(text="first"),
                genai_types.Part(text="second"),
            ],
        ),
        actions=EventActions(),
    )
    back = event_from_inline_dict(event_to_inline_dict(ev))
    assert back.content is not None and back.content.parts is not None
    assert [p.text for p in back.content.parts] == ["first", "second"]


def test_event_inline_dict_shape_is_stable():
    """Schema-evolution guard: changing the on-record key names is a breaking
    change for everyone with persisted data. If you change this list, you also
    need a migration story (and a bump to EVENT_SCHEMA_VERSION)."""
    ev = _make_event()
    d = event_to_inline_dict(ev)
    assert set(d.keys()) == {f.value for f in EventFieldName}


def test_event_schema_version_is_tagged():
    d = event_to_inline_dict(_make_event())
    assert d[EventFieldName.SCHEMA_VERSION] == EVENT_SCHEMA_VERSION


def test_event_from_inline_dict_tolerates_missing_version():
    """Pre-tag (v0) records have no ``_v`` field. The reader must still
    hydrate them — version 0 and 1 share the same on-record shape."""
    v0_record = {
        "eid": "old-event",
        "ts": 100.0,
        "author": "user",
        "content": {"role": "user", "parts": [{"text": "legacy"}]},
        "actions": {},
        "branch": None,
    }
    back = event_from_inline_dict(v0_record)
    assert back.id == "old-event"
    assert back.content.parts[0].text == "legacy"


def test_event_from_inline_dict_ignores_unknown_keys():
    """Forward-compat: a future writer may add fields the current reader
    doesn't know about. Pydantic must skip them, not raise."""
    forward_record = {
        "_v": 99,
        "eid": "future-event",
        "ts": 100.0,
        "author": "user",
        "content": {"role": "user", "parts": [{"text": "future"}]},
        "actions": {},
        "branch": None,
        "future_field": "ignored",
        "another_unknown": 42,
    }
    back = event_from_inline_dict(forward_record)
    assert back.id == "future-event"


def test_extract_event_text_joins_text_parts():
    ev = Event(
        invocation_id="inv",
        author="user",
        content=genai_types.Content(
            role="user",
            parts=[genai_types.Part(text="hello"), genai_types.Part(text="world")],
        ),
        actions=EventActions(),
    )
    assert extract_event_text(ev) == "hello world"


def test_extract_event_text_skips_non_text_parts():
    ev = Event(
        invocation_id="inv",
        author="user",
        content=genai_types.Content(
            role="user",
            parts=[
                genai_types.Part(text="hi"),
                genai_types.Part(
                    inline_data=genai_types.Blob(mime_type="image/png", data=b"\x00")
                ),
                genai_types.Part(text="there"),
            ],
        ),
        actions=EventActions(),
    )
    assert extract_event_text(ev) == "hi there"


def test_extract_event_text_returns_empty_for_no_content():
    ev = Event(invocation_id="inv", author="user", actions=EventActions())
    assert extract_event_text(ev) == ""


def test_estimate_event_size_is_positive_and_monotonic_in_payload():
    small = event_to_inline_dict(_make_event(text="x"))
    big = event_to_inline_dict(_make_event(text="x" * 1000))
    assert estimate_event_size(small) > 0
    assert estimate_event_size(big) > estimate_event_size(small)


# ---- artifact_scope_id --------------------------------------------------------


def test_artifact_scope_id_user_prefix_returns_sentinel():
    assert artifact_scope_id("user:profile.json", None) == USER_SCOPE_SID
    assert artifact_scope_id("user:profile.json", "some-session") == USER_SCOPE_SID


def test_artifact_scope_id_session_required_for_normal_filename():
    with pytest.raises(ValueError, match="session_id"):
        artifact_scope_id("notes.txt", None)


def test_artifact_scope_id_returns_session_when_provided():
    assert artifact_scope_id("notes.txt", "sess-1") == "sess-1"


def test_artifact_key_with_colon_in_filename():
    """Filenames legitimately contain ``:`` (e.g. ``user:profile.json``). The
    key construction must accept them without escaping; we never parse keys
    back — Aerospike hashes them to a digest."""
    k = artifact_key("a", "u", "user", "user:profile.json", 0)
    assert "user:profile.json" in k
    assert k.endswith(":00000000")


def test_memory_key_round_trip_components_present():
    k = memory_key("appA", "userB", "sessC", "evtD")
    assert "appA" in k and "userB" in k and "sessC" in k and "evtD" in k


# ---- tokenizer (memory) -------------------------------------------------------


def test_tokenize_lowercases_and_dedupes():
    assert set(_tokenize("Apple apple APPLE")) == {"apple"}


def test_tokenize_drops_non_alpha():
    assert set(_tokenize("hello, world! 123 foo-bar")) == {
        "hello", "world", "foo", "bar"
    }


def test_tokenize_empty_string_returns_empty():
    assert _tokenize("") == []


def test_tokenize_only_punctuation_returns_empty():
    assert _tokenize("!!! ??? ...") == []


def test_tokenize_unicode_letters_currently_dropped():
    """Doc the current behaviour: ``[A-Za-z]+`` matches only ASCII. Unicode
    letters are dropped. Matches ADK's reference tokenizer; change here means
    diverging from upstream semantics."""
    assert set(_tokenize("café résumé naïve")) == {"caf", "r", "sum", "na", "ve"}
