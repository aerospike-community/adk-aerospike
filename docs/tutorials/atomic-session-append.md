# Atomic Session Append with ADK and Aerospike

When an AI agent emits a new event, you must persist the event and merge a
state delta — ideally in one atomic step, without race conditions, lost writes,
or multiple round trips.

This tutorial shows how `AerospikeSessionService.append_event` stores events in
an append-only **K_ORDERED Map** segment record and coalesces any state delta
into a single round trip. You will connect to Aerospike, append an event with a
multi-scope state delta, read back the merged session, and verify that
concurrent appends never lose or duplicate data even as segments roll over.

This tutorial requires:

| Component | Requirement |
|-----------|-------------|
| Aerospike Database | 7.x or 8.x, port 3000 (local or remote) |
| Python | 3.11 or newer |
| [adk-aerospike](https://pypi.org/project/adk-aerospike/) | **0.1.0** or newer |
| [aerospike](https://pypi.org/project/aerospike/) Python client | **19+** (`index_single_value_create` API) |
| [google-adk](https://pypi.org/project/google-adk/) | installed automatically with adk-aerospike |

This tutorial covers:

- How events are stored in append-only segment records as a K_ORDERED Map
- How an append carrying state coalesces the event + state writes into one
  `batch_write` (one round trip)
- How a real `RecordTooBig` drives segment rollover (no client-side estimation)
- Verifying correctness under concurrent `append_event` load

## Start Aerospike Database

If you do not already have a node listening on port 3000:

```bash
docker run -d --name aerospike -p 3000:3000 aerospike/aerospike-server
```

## Ensure the database is running

```bash
nc -z localhost 3000 && echo "Aerospike database is running!" \
  || echo "**Aerospike database is not running!**"
```

Output

```plaintext
Aerospike database is running!
```

## Install Python packages

Use one Python interpreter for both `pip install` and running your script.
We recommend a virtual environment:

```bash
python3.11 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
python -m pip install --upgrade pip
python -m pip install "adk-aerospike>=0.1.0" "aerospike>=19"
```

`adk-aerospike` pulls in `google-adk`. Pin **`aerospike>=19`** explicitly —
client 18.x lacks `index_single_value_create` and service construction will fail.

Verify package imports:

```python
import aerospike
from google.adk.events import Event, EventActions
from google.genai import types as genai_types
from adk_aerospike import AerospikeSessionService

print("google-adk, aerospike, adk-aerospike imported.")
```

Output

```plaintext
google-adk, aerospike, adk-aerospike imported.
```

Verify Aerospike connectivity (creates secondary indexes on first connect):

```python
session_service = AerospikeSessionService.from_uri(
    "aerospike://localhost:3000/test"
)
session_service.close()
print("Connected to Aerospike.")
```

Output

```plaintext
Connected to Aerospike.
```

## Connect with production defaults

For real workloads, connect with `from_uri`. Each `append_event` is one round
trip — a single `operate()` on the current segment when there is no state delta,
or one `batch_write` coalescing the event with the state writes when there is —
with no multi-record transaction on the hot path.

```python
from adk_aerospike import AerospikeSessionService

session_service = AerospikeSessionService.from_uri(
    "aerospike://localhost:3000/test"
)
print("Connected to Aerospike.")
```

Output

```plaintext
Connected to Aerospike.
```

Modify the host, port, or namespace in the URI if your cluster differs.

## Understand the session + segment layout

A session row lives in set `adk_sessions` with primary key
`app_name:user_id:session_id`. It is small and holds only scoped state plus a
pointer to the current segment:

| Bin | CDT type | Meaning |
|-----|----------|---------|
| `state` | Map | session-scoped state (`map_put_items` on a state delta) |
| `cur` | integer | current append-target segment index (bumped on rollover) |
| `ts` | float | `last_update_time` |

Events live in **segment records** keyed `app_name:user_id:session_id:g:NNNNNNNN`,
each a single `events` bin holding a **K_ORDERED Map**:

| Bin | CDT type | Meaning |
|-----|----------|---------|
| `events` | Map (K_ORDERED) | `"{ts_micros:020d}:{event_id}"` → inline event Map |
| `gidx` | integer | segment index (segments omit `app/uid/sid`, so `list_sessions` ignores them) |

The map key is a pure function of the event, so a `map_put` is **idempotent** —
a retried append overwrites the same slot and can never duplicate. The key also
sorts chronologically, so `get_session` reads the last N events server-side.

When you call `append_event`, session-scoped, app-scoped, and user-scoped keys
in `state_delta` are partitioned automatically and, together with the event
`map_put`, coalesced into a single `batch_write`. `get_session` batch-reads the
session row plus app/user state and merges prefixed keys for the ADK caller.

## Complete example

The script below walks through more of the append path in one run: several
sequential `append_event` calls with scoped `state_delta`, a read-back of the
merged session, then **64 concurrent writers** that each issue **1000** appends to the same
session (64,000 appends total), with at most four appends in flight at once so
a single Docker CE node is not write-flooded.

Save as `atomic_session_append_demo.py` and run with
`python atomic_session_append_demo.py`.

```python
import asyncio
import time

import aerospike
from google.adk.events import Event, EventActions
from google.genai import types as genai_types

from adk_aerospike import AerospikeSessionService

APP, USER, SID = "demo-app", "user-1", "session-1"
WRITERS = 64
WRITES_PER_WRITER = 1000
MAX_IN_FLIGHT = 4

sem = asyncio.Semaphore(MAX_IN_FLIGHT)


def print_session_record(client: aerospike.Client, label: str) -> None:
    pk = ("test", "adk_sessions", f"{APP}:{USER}:{SID}")
    _, _, bins = client.select(pk, ["cur", "state"])
    print(
        f"{label} — cur_segment={bins.get('cur')}, "
        f"session_state_keys={len(bins.get('state') or {})}"
    )


async def main() -> None:
    client = aerospike.client(
        {
            "hosts": [("127.0.0.1", 3000)],
            "max_error_rate": 100,
            "error_rate_window": 1,
        }
    ).connect()
    session_service = AerospikeSessionService(client, "test")

    session = await session_service.create_session(
        app_name=APP, user_id=USER, session_id=SID
    )

    # --- sequential appends: scoped state deltas accumulate ---
    for i in range(5):
        await session_service.append_event(
            session,
            Event(
                invocation_id=f"seq-{i}",
                author="assistant",
                content=genai_types.Content(
                    role="model",
                    parts=[genai_types.Part(text=f"assistant reply {i}")],
                ),
                actions=EventActions(
                    state_delta={
                        "turn": i,
                        "app:tenant": "acme",
                        "user:locale": "en-US",
                    }
                ),
            ),
        )
        snap = await session_service.get_session(
            app_name=APP, user_id=USER, session_id=SID
        )
        print(
            f"after append {i}: events={len(snap.events)}, "
            f"state={snap.state}"
        )

    print_session_record(client, "after 5 sequential appends")

    # --- concurrent load: 64 writers × 1000 appends each ---
    async def writer(writer_id: int) -> None:
        for i in range(WRITES_PER_WRITER):
            async with sem:
                await session_service.append_event(
                    session,
                    Event(
                        invocation_id=f"w{writer_id:02d}-{i:04d}",
                        author=f"worker-{writer_id}",
                        timestamp=time.time(),
                        content=genai_types.Content(
                            role="user",
                            parts=[
                                genai_types.Part(
                                    text=f"worker {writer_id} write {i}"
                                )
                            ],
                        ),
                    ),
                )
            session.events.clear()

    started = time.perf_counter()
    await asyncio.gather(*(writer(w) for w in range(WRITERS)))
    elapsed = time.perf_counter() - started

    expected = 5 + WRITERS * WRITES_PER_WRITER
    final = await session_service.get_session(
        app_name=APP, user_id=USER, session_id=SID
    )
    print(
        f"\n{WRITERS} writers × {WRITES_PER_WRITER} appends "
        f"(max {MAX_IN_FLIGHT} in flight) in {elapsed:.1f}s — "
        f"stored {len(final.events)} events (expected {expected})"
    )
    print_session_record(client, "after concurrent appends")

    await session_service.delete_session(app_name=APP, user_id=USER, session_id=SID)
    session_service.close()
    print("Connection closed.")


if __name__ == "__main__":
    asyncio.run(main())
```

Output

```plaintext
after append 0: events=1, state={'app:tenant': 'acme', 'turn': 0, 'user:locale': 'en-US'}
after append 1: events=2, state={'app:tenant': 'acme', 'turn': 1, 'user:locale': 'en-US'}
...
after append 4: events=5, state={'app:tenant': 'acme', 'turn': 4, 'user:locale': 'en-US'}
after 5 sequential appends — cur_segment=0, session_state_keys=1

64 writers × 1000 appends (max 4 in flight) in 39.0s — stored 64005 events (expected 64005)
after concurrent appends — cur_segment=78, session_state_keys=1
Connection closed.
```

Each `append_event` does a single `map_put` of the event into the current
segment, keyed `"{ts_micros:020d}:{event_id}"`. Because that key is a pure
function of the event, the write is **idempotent** — a retried or duplicated
append overwrites the same slot and never produces a duplicate. When a segment
fills, the `map_put` returns `RecordTooBig`; the writer advances `cur` with a
`cur == N` guarded `increment` (so concurrent writers all converge on the same
next segment) and retries on the fresh segment. There is no byte estimation, no
flush threshold, and no possibility of an unrecoverable `RecordTooBig` dropping
an event under concurrency: segments simply roll over and pack to the
write-block-size naturally. An append that also carries a `state_delta` coalesces
the event `map_put` and the session/app/user state writes into one `batch_write`
— still a single round trip.

## Next steps

- [Infinite Chat History with ADK and Aerospike](https://github.com/aerospike-community/adk-aerospike/blob/main/docs/tutorials/infinite-chat-history.md) —
  append-only segment records for long agent conversations
- [Aerospike `operate` API](https://aerospike.com/docs/develop/learn/scans-guide/#operate) —
  multi-operation atomic commands
- [Aerospike Map operations](https://aerospike.com/docs/develop/data-types/collections/map) —
  `map_put_items` for session state
- [adk-aerospike on GitHub](https://github.com/aerospike-community/adk-aerospike) —
  source and additional examples
- [Google ADK session service](https://adk.dev/) — `create_session`, `append_event`, `get_session`
