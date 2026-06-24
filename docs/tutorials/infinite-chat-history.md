# Infinite Chat History with ADK and Aerospike

Long-running agent conversations accumulate thousands of events. If your session backend stores the full history in a single database row, you eventually hit Aerospike's per-record write-block-size limit (~1 MiB by default). Writes fail, context is truncated, or the agent crashes mid-turn.

`AerospikeSessionService` solves this by writing events into overflow-driven segment records. Each segment is a K_ORDERED Map that packs events until Aerospike returns `RecordTooBig`, at which point the writer advances `cur` and continues on a fresh segment. Your application keeps using standard ADK APIs (`create_session`, `append_event`, `get_session`) with no segment-awareness required.

This tutorial appends a 20,000-turn synthetic conversation and shows how full hydration, `num_recent_events`, and `after_timestamp` reads all behave as history grows.

## When to use this pattern

Use this tutorial when you:

- Build support bots, coding assistants, or agentic pipelines where conversations run for thousands of turns
- Need recent-event reads to stay fast regardless of total session length
- Want to inspect the raw segment records to verify history is intact after rollover

This tutorial requires:

| Component | Requirement |
|-----------|-------------|
| Aerospike Database | 7.x or 8.x, port 3000 (local or remote) |
| Python | 3.11 or newer |
| [adk-aerospike](https://pypi.org/project/adk-aerospike/) | 0.1.0 or newer |
| [aerospike](https://pypi.org/project/aerospike/) Python client | 19+ (`index_single_value_create` API) |
| [google-adk](https://pypi.org/project/google-adk/) | Installed automatically with `adk-aerospike` |

This tutorial covers:

- Session record vs. segment record layout (`app:user:session` vs.
  `app:user:session:g:NNNNNNNN`)
- Overflow-driven rollover on real `RecordTooBig` (no client-side byte budget)
- Hydrating full history vs. `num_recent_events` / `after_timestamp` reads
  (recent-X latency scales with X, not total event count)
- A 20,000-turn synthetic conversation spanning many segments

## Start Aerospike Database

If you do not already have a node listening on port 3000:

```bash
docker run -d --name aerospike -p 3000:3000 aerospike/aerospike-server
```

## Verify the database is running

```bash
nc -z localhost 3000 && echo "Aerospike database is running!" \
  || echo "**Aerospike database is not running!**"
```

Output:

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

`adk-aerospike` pulls in `google-adk`. Pin `aerospike>=19` explicitly: client 18.x lacks `index_single_value_create` and service construction fails.

Verify package imports:

```python
import aerospike
from google.adk.events import Event, EventActions
from google.genai import types as genai_types
from adk_aerospike import AerospikeSessionService

print("google-adk, aerospike, adk-aerospike imported.")
```

Output:

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

Output:

```plaintext
Connected to Aerospike.
```

## Connect to Aerospike

For real workloads, connect with `from_uri`. Segment rollover is driven by Aerospike's own `RecordTooBig`. There is no flush threshold to tune.

```python
from adk_aerospike import AerospikeSessionService

session_service = AerospikeSessionService.from_uri(
    "aerospike://localhost:3000/test"
)
print("Connected to Aerospike.")
```

Output:

```plaintext
Connected to Aerospike.
```

Modify the host, port, or namespace in the URI if your cluster differs.

## Understand the session and segment layout

A session row lives in set `adk_sessions` with primary key
`app_name:user_id:session_id`. It is small and holds only scoped state plus a
pointer to the current segment:

| Bin | CDT type | Meaning |
|-----|----------|---------|
| `state` | Map | session-scoped state (`map_put_items` on a state delta) |
| `cur` | integer | current append-target segment index (bumped on rollover) |
| `ts` | float | `last_update_time` |

Events live in segment records keyed `app_name:user_id:session_id:g:NNNNNNNN`,
each a single `events` bin holding a K_ORDERED Map:

| Bin | CDT type | Meaning |
|-----|----------|---------|
| `events` | Map (K_ORDERED) | `"{ts_micros:020d}:{event_id}"` → inline event Map |
| `gidx` | integer | segment index (segments omit `app/uid/sid`, so `list_sessions` ignores them) |

When a `map_put` fills a segment, the writer bumps `cur` with a guarded `increment` and retries on the next segment. Readers walk segments `cur..0` and merge with app/user state. Your code only calls `get_session`.

## Complete example

The script below appends 20,000 alternating user/assistant turns (a long
synthetic chat), then exercises full hydration, `num_recent_events`,
`after_timestamp`, a direct read of `cur` plus per-segment event counts, and
`list_sessions`.

Save as `infinite-chat-history-demo.py` and run with `python infinite-chat-history-demo.py`.

```python
import asyncio
import time

import aerospike
from aerospike_helpers.operations import map_operations
from google.adk.events import Event, EventActions
from google.adk.sessions.base_session_service import GetSessionConfig
from google.genai import types as genai_types

from adk_aerospike import AerospikeSessionService

APP, USER, SID = "demo-app", "user-1", "session-1"
TURNS = 20_000
EVENTS_BIN = "events"


def summarize_segments(
    client: aerospike.Client, cur: int
) -> tuple[int, list[int]]:
    counts: list[int] = []
    for gidx in range(cur + 1):
        seg_pk = ("test", "adk_sessions", f"{APP}:{USER}:{SID}:g:{gidx:08d}")
        try:
            _, _, res = client.operate(
                seg_pk, [map_operations.map_size(EVENTS_BIN)]
            )
            n = int(res.get(EVENTS_BIN, 0))
        except Exception:
            n = 0
        if n:
            counts.append(n)
    return len(counts), counts


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
    t0 = time.time()
    started = time.perf_counter()

    for i in range(TURNS):
        author = "user" if i % 2 == 0 else "assistant"
        await session_service.append_event(
            session,
            Event(
                invocation_id=f"turn-{i:05d}",
                author=author,
                timestamp=t0 + i * 0.001,
                content=genai_types.Content(
                    role="user" if author == "user" else "model",
                    parts=[
                        genai_types.Part(
                            text=(
                                f"turn {i} from {author}: "
                                "context line for a long-running agent session. "
                            )
                        )
                    ],
                ),
                actions=EventActions(
                    state_delta={
                        "step": i,
                        "app:tenant": "acme",
                        "user:locale": "en-US",
                    }
                ),
            ),
        )
        session.events.clear()
        if i and i % 5000 == 0:
            print(f"  appended {i}...")

    print(f"append_event x{TURNS} in {time.perf_counter() - started:.1f}s")

    full = await session_service.get_session(
        app_name=APP, user_id=USER, session_id=SID
    )
    print(f"full history: {len(full.events)} events")
    print(f"  first: {full.events[0].content.parts[0].text}")  # type: ignore[union-attr]
    print(f"  last:  {full.events[-1].content.parts[0].text}")  # type: ignore[union-attr]
    print(f"  merged state keys: {list(full.state.keys())}")

    recent5 = await session_service.get_session(
        app_name=APP,
        user_id=USER,
        session_id=SID,
        config=GetSessionConfig(num_recent_events=5),
    )
    texts5 = [e.content.parts[0].text for e in recent5.events]  # type: ignore[union-attr]
    print(f"num_recent_events=5: {texts5}")

    after = await session_service.get_session(
        app_name=APP,
        user_id=USER,
        session_id=SID,
        config=GetSessionConfig(after_timestamp=t0 + 19_990 * 0.001),
    )
    print(f"after_timestamp (last ~10 turns): {len(after.events)} events")

    pk = ("test", "adk_sessions", f"{APP}:{USER}:{SID}")
    _, _, sbins = client.select(pk, ["cur", "state"])
    cur = int(sbins.get("cur", 0))
    n_segs, sizes = summarize_segments(client, cur)
    print(
        f"session record — cur={cur}, segments={n_segs}, "
        f"events_in_segments={sum(sizes)}, "
        f"session_state_keys={len(sbins.get('state') or {})}"
    )
    if sizes:
        print(f"  segment sizes (first 3): {sizes[:3]} ... (last 3): {sizes[-3:]}")

    listed = await session_service.list_sessions(app_name=APP, user_id=USER)
    print(f"list_sessions: {[s.id for s in listed.sessions]}")

    await session_service.delete_session(app_name=APP, user_id=USER, session_id=SID)
    session_service.close()
    print("Connection closed.")


if __name__ == "__main__":
    asyncio.run(main())
```

Output:

```plaintext
  appended 5000...
  appended 10000...
  appended 15000...
append_event x20000 in 21.7s
full history: 20000 events
  first: turn 0 from user: context line for a long-running agent session.
  last:  turn 19999 from assistant: context line for a long-running agent session.
  merged state keys: ['app:tenant', 'step', 'user:locale']
num_recent_events=5: ['turn 19995 from assistant: ...', 'turn 19996 from user: ...', ...]
after_timestamp (last ~10 turns): 10 events
session record — cur=26, segments=27, events_in_segments=20000, session_state_keys=1
  segment sizes (first 3): [770, 769, 769] ... (last 3): [768, 768, 18]
list_sessions: ['session-1']
Connection closed.
```

Elapsed times, segment sizes, and `cur` vary with node configuration. The
invariants to verify are event counts, segment rollover, and read semantics.

`get_session` without a config walks every segment and returns the full 20,000 events in order (~2.8 s on a single Docker Community Edition node for this dataset). Recent reads scale with the number of events requested, not total history. Aerospike uses server-side `map_get_by_index_range(-N, N)` on the newest segment(s):

| `num_recent_events` | p50 latency (20k-event session) |
|---------------------|---------------------------------|
| 5 | ~1 ms |
| 50 | ~5 ms |
| 100 | ~9 ms |
| 500 | ~51 ms |

p50 is the median latency across repeated reads.

`after_timestamp` skips whole segments with `map_get_by_key_range` when their keys fall below the cutoff (~2 ms for the last ten turns here).

## Next steps

- [Atomic Session Append with ADK and Aerospike](https://github.com/aerospike-community/adk-aerospike/blob/main/docs/tutorials/atomic-session-append.md): idempotent `map_put` appends and concurrent writers on one session
- [Aerospike Map operations](https://aerospike.com/docs/develop/data-types/collections/map): K_ORDERED maps and `map_get_by_index_range`
- [adk-aerospike on GitHub](https://github.com/aerospike-community/adk-aerospike): source and additional examples
- [Google ADK session service](https://adk.dev/): `create_session`, `append_event`, `get_session`
