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

from .schema import EVENT_SCHEMA_VERSION, EventFieldName

if TYPE_CHECKING:
    from google.adk.events import Event


def event_to_inline_dict(event: Event) -> dict[str, Any]:
    """Project an Event onto the Map shape we store inside the events list.

    v2 records store the full ``Event.model_dump(mode="json")`` under
    ``EventFieldName.PAYLOAD`` for lossless round-trip, plus denormalised
    ``eid``/``ts``/``author`` for chunk pruning and debugging. v0/v1 slim
    records remain readable via :func:`event_from_inline_dict`.
    """
    dump = event.model_dump(mode="json")
    return {
        EventFieldName.SCHEMA_VERSION: EVENT_SCHEMA_VERSION,
        EventFieldName.EVENT_ID: dump.get("id"),
        EventFieldName.TIMESTAMP: dump.get("timestamp", 0.0),
        EventFieldName.AUTHOR: dump.get("author"),
        EventFieldName.PAYLOAD: dump,
    }


def event_from_inline_dict(d: dict[str, Any]) -> Event:
    from google.adk.events import Event

    version = d.get(EventFieldName.SCHEMA_VERSION, 0)
    payload = d.get(EventFieldName.PAYLOAD)
    if version >= 2 and isinstance(payload, dict):
        return Event.model_validate(payload)

    # v0/v1 slim records — same field set, no branching beyond this path.
    return Event.model_validate(
        {
            "id": d.get(EventFieldName.EVENT_ID) or "",
            "timestamp": d.get(EventFieldName.TIMESTAMP, 0.0),
            "author": d.get(EventFieldName.AUTHOR),
            "content": d.get(EventFieldName.CONTENT),
            "actions": d.get(EventFieldName.ACTIONS),
            "branch": d.get(EventFieldName.BRANCH),
        }
    )


def event_map_key(event_id: str, timestamp: float) -> str:
    """Stable, chronologically-sortable key for an event in a segment map.

    ``"{ts_micros:020d}:{event_id}"`` — the zero-padded microsecond prefix
    orders entries by time (so ``map_get_by_index_range`` yields chronological
    last-N server-side), and the ``event_id`` suffix guarantees uniqueness when
    two events share a microsecond.

    Crucially the key is a **pure function of the event**: a retried append
    recomputes the identical key, so the ``map_put`` is idempotent (it
    overwrites the same slot rather than creating a duplicate). 20 digits of
    microseconds covers epoch timestamps well past year 9999.
    """
    return f"{int(timestamp * 1_000_000):020d}:{event_id}"


def event_ts_from_map_key(key: str) -> float:
    """Inverse of the timestamp prefix in :func:`event_map_key` (epoch seconds)."""
    micros = int(key.split(":", 1)[0])
    return micros / 1_000_000


def extract_event_text(event: Event) -> str:
    """Concatenate the text Parts of an event's content. ``""`` if none."""
    if not event.content or not event.content.parts:
        return ""
    return " ".join(p.text for p in event.content.parts if getattr(p, "text", None))
