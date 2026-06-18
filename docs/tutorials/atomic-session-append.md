# Atomic Session Append with ADK and Aerospike

When an AI agent emits a new event, you must update the event list, merge a
state delta, bump a sequence counter, and refresh the timestamp — ideally in one
atomic step, without race conditions or multiple round trips.

This tutorial shows how `AerospikeSessionService.append_event` uses Aerospike
**List and Map Complex Data Types (CDTs)** in a single server-side `operate()`
call. You will connect to Aerospike, append an event with a multi-scope state
delta, read back the merged session, and verify concurrent appends.

This tutorial requires:

| Component | Requirement |
|-----------|-------------|
| Aerospike Database | 7.x or 8.x, port 3000 (local or remote) |
| Python | 3.11 or newer |
| [adk-aerospike](https://pypi.org/project/adk-aerospike/) | latest |
| [aerospike](https://pypi.org/project/aerospike/) Python client | **19+** (`index_single_value_create` API) |
| [google-adk](https://pypi.org/project/google-adk/) | installed automatically with adk-aerospike |

This tutorial covers:

- How session-scoped updates map to one atomic `operate()` per append
- Which CDT operations run on the session record (`list_append`, `map_put_items`,
  `increment`, `write`)
- How app- and user-scoped state deltas route to separate records
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
python -m pip install "adk-aerospike" "aerospike>=19"
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

For real workloads, connect with `from_uri`. Session-scoped persistence on each
`append_event` is one atomic `operate()` on the session record — no
multi-record transaction on the hot path.

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

## Understand the session record layout

A session row lives in set `adk_sessions` with primary key
`app_name:user_id:session_id`. The hot-path append updates these bins on that
single record:

| Bin | CDT type | Updated on append |
|-----|----------|-------------------|
| `events` | List | `list_append` — inline event Map |
| `state` | Map | `map_put_items` — session-scoped delta (optional) |
| `seq` | integer | `increment` — monotonic append counter |
| `tbytes` | integer | `increment` — tail size estimator |
| `ts` | float | `write` — `last_update_time` |

App-scoped keys (`app:…`) and user-scoped keys (`user:…`) in `state_delta`
route to separate `adk_app_state` / `adk_user_state` rows, each with their own
atomic `map_put_items` — still one RTT per scope, not bundled into the session
`operate()`.

When you call `append_event`, session-scoped, app-scoped, and user-scoped keys
in `state_delta` are partitioned automatically. `get_session` batch-reads the
session row plus app/user state and merges prefixed keys for the ADK caller.

## Complete example

The script below walks through more of the append path in one run: several
sequential `append_event` calls with scoped `state_delta`, a `get_session` read
after each phase, a direct bin read (`seq`, `tbytes`, tail length), then 64
concurrent appends to the same session.

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


def print_bins(client: aerospike.Client, label: str) -> None:
    pk = ("test", "adk_sessions", f"{APP}:{USER}:{SID}")
    _, _, bins = client.select(pk, ["events", "seq", "tbytes", "state"])
    tail = bins.get("events") or []
    print(
        f"{label} — tail_len={len(tail)}, seq={bins.get('seq')}, "
        f"tbytes={bins.get('tbytes')}, session_state_keys={len(bins.get('state') or {})}"
    )


async def main() -> None:
    client = aerospike.client({"hosts": [("127.0.0.1", 3000)]}).connect()
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

    print_bins(client, "after 5 sequential appends")

    # --- concurrent appends: 64 writers, one session ---
    async def one(i: int) -> None:
        await session_service.append_event(
            session,
            Event(
                invocation_id=f"par-{i:04d}",
                author="tool",
                timestamp=time.time(),
                content=genai_types.Content(
                    role="user",
                    parts=[genai_types.Part(text=f"tool output {i:04d}")],
                ),
                actions=EventActions(state_delta={f"tool:{i:04d}": i}),
            ),
        )

    started = time.perf_counter()
    await asyncio.gather(*(one(i) for i in range(64)))
    elapsed = time.perf_counter() - started

    final = await session_service.get_session(
        app_name=APP, user_id=USER, session_id=SID
    )
    print(
        f"\n64 parallel appends in {elapsed:.2f}s — "
        f"total events={len(final.events)}, state_keys={len(final.state)}"
    )
    print_bins(client, "after concurrent appends")

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
after 5 sequential appends — tail_len=5, seq=5, tbytes=9160, session_state_keys=1

64 parallel appends in 0.02s — total events=69, state_keys=67
after concurrent appends — tail_len=69, seq=69, tbytes=123224, session_state_keys=65
Connection closed.
```

Each `append_event` issues one atomic `operate()` on the session record
(`list_append`, optional `map_put_items`, `increment` on `seq`/`tbytes`, `write`
on `ts`). Concurrent calls serialize on the partition master — no lost events or
sequence gaps.

## Next steps

- [Infinite Chat History with ADK and Aerospike](https://github.com/aerospike-community/adk-aerospike/blob/main/docs/tutorials/infinite-chat-history.md) —
  chunked session records for long agent conversations
- [Aerospike `operate` API](https://aerospike.com/docs/develop/learn/scans-guide/#operate) —
  multi-operation atomic commands
- [Aerospike Map operations](https://aerospike.com/docs/develop/data-types/collections/map) —
  `map_put_items` for session state
- [adk-aerospike on GitHub](https://github.com/aerospike-community/adk-aerospike) —
  source and additional examples
- [Google ADK session service](https://adk.dev/) — `create_session`, `append_event`, `get_session`
