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


def estimate_event_size(event_dict: dict[str, Any]) -> int:
    """Cheap byte estimate for the tail-bytes counter.

    Doesn't need to be exact — used only to decide when to flush.
    ``str(d)`` overcounts by ~2× vs the actual MessagePack encoding Aerospike
    uses on the wire (Python's ``repr`` of dicts includes extra quote chars,
    brackets, and spaces).

    The overcount is **deliberately uncorrected**: it gives the flush
    threshold extra headroom under Aerospike's 1 MiB write-block-size,
    keeping individual chunks well below the limit even when state Maps grow
    or estimator error compounds. The practical effect is that chunks are
    smaller than the byte-budget reading would suggest — which lowers
    flush-stall tail latency at the cost of slightly more chunk records per
    session. Acceptable tradeoff.
    """
    return len(str(event_dict))


def extract_event_text(event: Event) -> str:
    """Concatenate the text Parts of an event's content. ``""`` if none."""
    if not event.content or not event.content.parts:
        return ""
    return " ".join(p.text for p in event.content.parts if getattr(p, "text", None))
