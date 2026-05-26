#!/usr/bin/env python3
"""Latency/throughput benchmarks for adk-aerospike.

Targets the four design claims that motivated the schema:

  1. append    — append_event is single-record server-side atomic
                  → ~constant latency regardless of concurrency
  2. chunking  — long sessions stay under write-block-size; flush is amortized
                  → most appends are fast; flushes show up as periodic spikes
  3. read      — get_session is one RTT for sessions whose tail satisfies
                 ``num_recent_events``  (batch_read pulls session + app + user)
                  → ~constant latency vs session size up to that boundary
  4. search    — list-element sec-index scales with corpus size
                  → sublinear in corpus size, ~linear in query token count

Usage:
    python benchmarks/benchmark.py SCENARIO [--uri URI] [options]

    SCENARIO: append | chunking | read | search | all

Examples:
    python benchmarks/benchmark.py append --ops 5000 --concurrency 64
    python benchmarks/benchmark.py chunking --events 5000
    python benchmarks/benchmark.py read --chunks 50 --ops 1000
    python benchmarks/benchmark.py search --corpus 10000 --ops 200
    python benchmarks/benchmark.py all
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass

from google.adk.events import Event, EventActions
from google.adk.sessions import Session
from google.genai import types

from adk_aerospike import (
    AerospikeArtifactService,  # noqa: F401  — imported for parity / smoke
    AerospikeMemoryService,
    AerospikeSessionService,
)

DEFAULT_URI = "aerospike://127.0.0.1:3000/test"


# ---------- timing infrastructure --------------------------------------------


@dataclass
class Stats:
    label: str
    samples_ms: list[float]
    elapsed_s: float

    @property
    def n(self) -> int:
        return len(self.samples_ms)

    @property
    def ops_per_s(self) -> float:
        return self.n / self.elapsed_s if self.elapsed_s > 0 else 0.0

    def _pct(self, p: float) -> float:
        if not self.samples_ms:
            return 0.0
        sorted_ = sorted(self.samples_ms)
        idx = min(int(p * len(sorted_)), len(sorted_) - 1)
        return sorted_[idx]

    def render(self) -> str:
        if not self.samples_ms:
            return f"  {self.label}: no samples"
        return (
            f"  {self.label:<30s} n={self.n:>6d}  "
            f"ops/s={self.ops_per_s:>8.1f}  "
            f"p50={self._pct(0.50):>6.2f}ms  "
            f"p95={self._pct(0.95):>7.2f}ms  "
            f"p99={self._pct(0.99):>7.2f}ms  "
            f"max={max(self.samples_ms):>7.2f}ms  "
            f"mean={statistics.mean(self.samples_ms):>6.2f}ms"
        )


@contextmanager
def _timer(samples: list[float]):
    t0 = time.perf_counter()
    yield
    samples.append((time.perf_counter() - t0) * 1000.0)


async def _run_concurrent(coro_factory, n: int, concurrency: int) -> Stats:
    """Run ``n`` coroutine invocations with bounded concurrency. Returns latency Stats."""
    samples: list[float] = []
    sem = asyncio.Semaphore(concurrency)

    async def one(i: int) -> None:
        async with sem:
            with _timer(samples):
                await coro_factory(i)

    t0 = time.perf_counter()
    await asyncio.gather(*(one(i) for i in range(n)))
    return Stats(label="", samples_ms=samples, elapsed_s=time.perf_counter() - t0)


# ---------- helpers ----------------------------------------------------------


def _event(text: str, seq: int) -> Event:
    return Event(
        invocation_id=f"inv-{seq}",
        author="user" if seq % 2 == 0 else "assistant",
        content=types.Content(role="user", parts=[types.Part(text=text)]),
        actions=EventActions(state_delta={"turn": seq}),
    )


async def _new_session(svc: AerospikeSessionService, app: str, user: str) -> Session:
    return await svc.create_session(app_name=app, user_id=user, session_id=uuid.uuid4().hex)


async def _purge_app(svc: AerospikeSessionService, app: str, user: str) -> None:
    """Best-effort cleanup so repeated runs don't pollute each other."""
    resp = await svc.list_sessions(app_name=app, user_id=user)
    for s in resp.sessions:
        try:
            await svc.delete_session(app_name=app, user_id=user, session_id=s.id)
        except Exception:
            pass


# ---------- 1. append --------------------------------------------------------


async def bench_append(uri: str, ops: int, concurrency: int, event_text_size: int) -> Stats:
    """Append ``ops`` events across ``concurrency`` parallel sessions.

    Validates: append_event is a single-record server-side atomic op. Latency
    should be flat across concurrency until the cluster is saturated.
    """
    svc = AerospikeSessionService.from_uri(uri)
    app = "bench_append"
    user = "u"
    try:
        await _purge_app(svc, app, user)
        # One session per concurrent worker — keeps appends going to distinct keys
        # so we measure single-record atomic throughput rather than contention.
        sessions = [await _new_session(svc, app, user) for _ in range(concurrency)]
        text = "x" * max(1, event_text_size)

        async def append_op(i: int) -> None:
            s = sessions[i % concurrency]
            await svc.append_event(s, _event(text, i))

        stats = await _run_concurrent(append_op, ops, concurrency)
        stats.label = f"append (size={event_text_size}B, concurrency={concurrency})"
        await _purge_app(svc, app, user)
        return stats
    finally:
        svc.close()


# ---------- 2. chunking ------------------------------------------------------


async def bench_chunking(uri: str, events: int, event_text_size: int) -> Stats:
    """Append ``events`` events to a SINGLE session, observing flush behaviour.

    Validates: chunking keeps each append cheap; flushes show up as periodic
    p99 spikes, not catastrophic ones. Sequential by design — concurrency
    on a single session is contention, not throughput.
    """
    svc = AerospikeSessionService.from_uri(uri)
    app = "bench_chunking"
    user = "u"
    try:
        await _purge_app(svc, app, user)
        session = await _new_session(svc, app, user)
        text = "x" * max(1, event_text_size)
        samples: list[float] = []
        t0 = time.perf_counter()
        for i in range(events):
            with _timer(samples):
                await svc.append_event(session, _event(text, i))
        elapsed = time.perf_counter() - t0

        # Read back to confirm chunk count
        fetched = await svc.get_session(app_name=app, user_id=user, session_id=session.id)
        n_events = len(fetched.events) if fetched else 0

        stats = Stats(
            label=f"chunking ({events} events, {event_text_size}B each → {n_events} hydrated)",
            samples_ms=samples,
            elapsed_s=elapsed,
        )
        await _purge_app(svc, app, user)
        return stats
    finally:
        svc.close()


# ---------- 3. read ----------------------------------------------------------


async def bench_read(
    uri: str,
    chunks: int,
    ops: int,
    concurrency: int,
    full_history: bool,
) -> Stats:
    """Repeatedly get_session() on a session with ``chunks`` sealed chunks.

    Default mode (fast path): ``num_recent_events=5`` — tail satisfies the
    config, so the read is a single batch_read for session + app_state +
    user_state. **Latency should not grow with chunk count.**

    With ``--full-history``: no config → walks all chunks via server-side
    pagination. Measures the chunk-walk cost (one RTT per chunk).
    """
    from google.adk.sessions.base_session_service import GetSessionConfig

    svc = AerospikeSessionService.from_uri(uri)
    app = "bench_read"
    user = "u"
    try:
        await _purge_app(svc, app, user)
        # Build a session with the requested number of sealed chunks. Use a
        # tiny flush threshold so we hit it quickly without writing a real
        # 256 KiB of test data.
        small = AerospikeSessionService.from_uri(uri)
        small._flush_threshold = 200  # bytes — force flush every ~3 events
        try:
            session = await small.create_session(
                app_name=app, user_id=user, session_id=uuid.uuid4().hex
            )
            for i in range(chunks * 4):  # ~4 events per chunk under 200B threshold
                await small.append_event(session, _event("hello world", i))
            # Add one more event so the tail is non-empty for fast-path tests.
            await small.append_event(session, _event("tail anchor", 9999))
        finally:
            small.close()

        config = None if full_history else GetSessionConfig(num_recent_events=5)
        mode = "full history" if full_history else "num_recent_events=5"

        async def read_op(_i: int) -> None:
            await svc.get_session(
                app_name=app, user_id=user, session_id=session.id, config=config
            )

        stats = await _run_concurrent(read_op, ops, concurrency)
        stats.label = (
            f"get_session [{mode}] (~{chunks} chunks, concurrency={concurrency})"
        )
        await _purge_app(svc, app, user)
        return stats
    finally:
        svc.close()


# ---------- 4. search --------------------------------------------------------


async def bench_search(
    uri: str,
    corpus: int,
    ops: int,
    concurrency: int,
    query_tokens: int,
) -> Stats:
    """Bulk-load ``corpus`` memory entries, then run ``ops`` parallel searches.

    Validates: list-element secondary index scales sublinearly with corpus
    size. Search latency = O(matches per token × query_tokens), not O(corpus).
    """
    mem = AerospikeMemoryService.from_uri(uri)
    app = "bench_search"
    user = "u"

    # Realistic vocabulary scale: each query token should match a small fraction
    # of the corpus, not 20% (which was the case with a 20-word vocabulary +
    # 4 tokens/event). 256 words × 8 tokens/event ≈ 3% match rate per token.
    vocabulary = [f"w{i:04d}" for i in range(256)]
    tokens_per_event = 8

    try:
        # Quick purge: remove all memories for this (app, user) via the service's
        # purge helper (loops over uid sec-index).
        await mem._purge_session_memories(app, user, "warmup")

        # Build a synthetic session with ``corpus`` events, each containing 3-5
        # vocabulary tokens. Distinct event IDs ensure distinct memory rows.
        events: list[Event] = []
        for i in range(corpus):
            # Sample tokens_per_event distinct vocabulary words for this event.
            # Stride by a prime so consecutive events don't share many tokens.
            words = " ".join(
                vocabulary[(i * 17 + j) % len(vocabulary)]
                for j in range(tokens_per_event)
            )
            ev = _event(words, i)
            ev.id = f"bench-{i:08d}"
            events.append(ev)

        session = Session(
            id="bench-sess", app_name=app, user_id=user, events=events
        )
        # add_session_to_memory writes one record per event — fan it out for speed.
        load_start = time.perf_counter()
        await mem.add_session_to_memory(session)
        load_s = time.perf_counter() - load_start
        print(
            f"  preload: {corpus} memories in {load_s:.2f}s "
            f"({corpus / load_s:.0f} writes/s)"
        )

        async def search_op(i: int) -> None:
            # Cycle through the vocabulary so each query hits a different
            # slice of the corpus.
            tokens = " ".join(
                vocabulary[(i + k) % len(vocabulary)] for k in range(query_tokens)
            )
            await mem.search_memory(app_name=app, user_id=user, query=tokens)

        stats = await _run_concurrent(search_op, ops, concurrency)
        stats.label = (
            f"search_memory (corpus={corpus}, tokens={query_tokens}, "
            f"concurrency={concurrency})"
        )
        return stats
    finally:
        mem.close()


# ---------- main -------------------------------------------------------------


async def run(args: argparse.Namespace) -> None:
    scenarios = (
        ["append", "chunking", "read", "search"]
        if args.scenario == "all"
        else [args.scenario]
    )

    print(f"adk-aerospike benchmark — connecting to {args.uri}")
    print()

    if "append" in scenarios:
        print("[1/4] append")
        stats = await bench_append(
            args.uri, args.ops, args.concurrency, args.event_size
        )
        print(stats.render())
        print()

    if "chunking" in scenarios:
        print("[2/4] chunking")
        stats = await bench_chunking(args.uri, args.events, args.event_size)
        print(stats.render())
        print()

    if "read" in scenarios:
        print("[3/4] read")
        stats = await bench_read(
            args.uri, args.chunks, args.ops, args.concurrency, args.full_history
        )
        print(stats.render())
        print()

    if "search" in scenarios:
        print("[4/4] search")
        stats = await bench_search(
            args.uri, args.corpus, args.ops, args.concurrency, args.query_tokens
        )
        print(stats.render())
        print()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("scenario", choices=["append", "chunking", "read", "search", "all"])
    p.add_argument("--uri", default=DEFAULT_URI)
    p.add_argument("--ops", type=int, default=2000, help="# of operations")
    p.add_argument("--concurrency", type=int, default=32)
    p.add_argument("--event-size", type=int, default=200, help="event text bytes (append/chunking)")
    p.add_argument("--events", type=int, default=5000, help="events per session (chunking)")
    p.add_argument("--chunks", type=int, default=10, help="sealed chunks to build (read)")
    p.add_argument("--full-history", action="store_true",
                   help="read scenario: walk all chunks instead of fast-path tail (read)")
    p.add_argument("--corpus", type=int, default=5000, help="memory entries to preload (search)")
    p.add_argument("--query-tokens", type=int, default=3, help="tokens per search query")
    asyncio.run(run(p.parse_args()))


if __name__ == "__main__":
    main()
