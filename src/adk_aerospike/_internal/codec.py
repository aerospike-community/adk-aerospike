"""Serialization helpers: ADK Pydantic models <-> Aerospike bin values.

Aerospike bins natively store: int, float, str, bytes, list, dict, plus
Geo/HLL/Map CDT types. Nested dicts/lists of these primitives serialize directly.

ADK uses Pydantic v2 throughout. ``model_dump(mode="json")`` produces a dict of
JSON-serialisable primitives — exactly what Aerospike accepts. We use that as
the wire format and reconstruct via ``Model.model_validate(...)``.

Design notes
------------
- We store ``state`` as a Map bin so we can apply ``state_delta`` updates with
  ``MapOperation.put_items`` in a single round trip — no read-modify-write.
- Events live inline in the ``events`` List bin on the session record (hot
  tail) and in chunk records once flushed. The on-record shape per event is a
  small Map — see :func:`event_to_inline_dict`.
- ``timestamp`` is stored as a float (epoch seconds) — direct Aerospike bin.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from google.adk.events import Event


def event_to_inline_dict(event: Event) -> dict[str, Any]:
    """Project an Event onto the Map shape we store inside the events list.

    Keeps the field set small — no denormalised ``app``/``uid``/``sid`` (those
    live on the parent session record) and no per-event ``seq`` (position in
    the merged list already encodes order).
    """
    dump = event.model_dump(mode="json")
    return {
        "eid": dump.get("id"),
        "ts": dump.get("timestamp", 0.0),
        "author": dump.get("author"),
        "content": dump.get("content"),
        "actions": dump.get("actions"),
        "branch": dump.get("branch"),
    }


def event_from_inline_dict(d: dict[str, Any]) -> Event:
    from google.adk.events import Event

    return Event.model_validate(
        {
            "id": d.get("eid") or "",
            "timestamp": d.get("ts", 0.0),
            "author": d.get("author"),
            "content": d.get("content"),
            "actions": d.get("actions"),
            "branch": d.get("branch"),
        }
    )


def estimate_event_size(event_dict: dict[str, Any]) -> int:
    """Cheap byte estimate for the tail-bytes counter.

    Doesn't need to be exact — used only to decide when to flush. ``str(d)``
    overcounts (extra quote chars, brackets) but stays linear in payload size.
    """
    return len(str(event_dict))


def extract_event_text(event: Event) -> str:
    """Concatenate the text Parts of an event's content. ``""`` if none."""
    if not event.content or not event.content.parts:
        return ""
    return " ".join(p.text for p in event.content.parts if getattr(p, "text", None))
