"""Synthetic ADK objects sized like production agent traffic."""

from __future__ import annotations

import uuid

from google.adk.events import Event, EventActions
from google.adk.sessions import Session
from google.genai import types

# Vocabulary large enough that each query token matches ~1–5% of a 10k corpus.
VOCABULARY = [f"w{i:04d}" for i in range(256)]
TOKENS_PER_EVENT = 8


def make_event(text: str, seq: int, *, event_id: str | None = None) -> Event:
    ev = Event(
        invocation_id=f"inv-{seq}",
        author="user" if seq % 2 == 0 else "assistant",
        content=types.Content(role="user", parts=[types.Part(text=text)]),
        actions=EventActions(state_delta={"turn": seq}),
    )
    if event_id is not None:
        ev.id = event_id
    return ev


def filler_text(size_bytes: int, *, seed: int = 0) -> str:
    if size_bytes <= 0:
        return "x"
    unit = f"evt{seed % 1000:03d} "
    repeats = (size_bytes // len(unit)) + 1
    return (unit * repeats)[:size_bytes]


def memory_event_text(index: int) -> str:
    return " ".join(
        VOCABULARY[(index * 17 + j) % len(VOCABULARY)] for j in range(TOKENS_PER_EVENT)
    )


def memory_query_text(slot: int, query_tokens: int) -> str:
    return " ".join(
        VOCABULARY[(slot + k) % len(VOCABULARY)] for k in range(query_tokens)
    )


def new_session_id() -> str:
    return uuid.uuid4().hex
