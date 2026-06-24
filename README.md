# adk-aerospike

[![Tests](https://github.com/aerospike-community/adk-aerospike/actions/workflows/tests.yml/badge.svg)](https://github.com/aerospike-community/adk-aerospike/actions/workflows/tests.yml)

Aerospike-backed storage services for [Google Agent Development Kit (ADK)](https://adk.dev/).

**Status:** alpha (0.0.2). All three ADK storage interfaces implemented end-to-end against ADK 2.x.

## Why this exists

[Google ADK](https://adk.dev/) is a framework for building stateful, multi-step AI agents. Every agent turn generates events, state deltas, and artifacts that must survive process restarts and scale to many concurrent users. ADK defines three pluggable storage interfaces and leaves the backend to you.

This package implements all three on [Aerospike](https://aerospike.com/): a real-time NoSQL database with sub-millisecond key-value access, native time to live (TTL) per record, and atomic collection data type (CDT) operations. A single Aerospike cluster backs session state, file artifacts, and lexical memory search without extra services.

## Services

Three implementations of ADK's pluggable storage interfaces, plus URI-scheme
registration so the `adk` CLI can use Aerospike directly:

| ADK interface         | This package               | Backed by                                                      |
| --------------------- | -------------------------- | -------------------------------------------------------------- |
| `BaseSessionService`  | `AerospikeSessionService`  | Aerospike KV + Map CDTs (overflow-driven segment records)      |
| `BaseArtifactService` | `AerospikeArtifactService` | Aerospike KV (one record per version)                          |
| `BaseMemoryService`   | `AerospikeMemoryService`   | Aerospike KV (lexical search, per-token posting-list PKs)                 |

### Why use this

- All three ADK storage interfaces (Session, Artifact, Memory) backed by a single Aerospike cluster.
- In-process client: talks directly to Aerospike with no extra services to deploy or operate.
- Atomic `append_event`: state delta, event write, and timestamp update coalesced into one round trip.
- Unbounded session history: events stored in overflow-driven segment records so long conversations never hit the write-block-size limit.
- Single-RTT `get_session`: one `batch_read` across the session record, app-state record, and user-state record.
- Lexical memory search: text tokenized at write time; each query token reads a posting-list key, then hydrates matching memory rows. Same word-overlap semantics as `InMemoryMemoryService`. No embedder needed.
- `adk web` integration: register the `aerospike://` URI scheme and the CLI flags work without extra configuration.

## Install

```bash
pip install adk-aerospike
```

Requires Python 3.11+ and Aerospike Database 7.x or 8.x (Community or Enterprise).

### Development install

From a clone of this repository (Python 3.11+):

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
python -c "from adk_aerospike import AerospikeSessionService; print('ok')"
```

Release process: [RELEASING.md](RELEASING.md).

## Quickstart

Start a local Aerospike container:

```bash
docker run --rm -d --name aerospike -p 3000-3003:3000-3003 aerospike/aerospike-server:latest
```

Then wire it into an ADK agent:

```python
import asyncio
from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.genai import types
from adk_aerospike import AerospikeSessionService

async def main() -> None:
    session_service = AerospikeSessionService.from_uri(
        "aerospike://localhost:3000/test"
    )
    agent = LlmAgent(name="greeter", model="gemini-2.5-flash",
                     instruction="Be friendly. <30 words.")
    runner = Runner(agent=agent, app_name="quickstart",
                    session_service=session_service)

    session = await session_service.create_session(
        app_name="quickstart", user_id="user-42"
    )
    async for event in runner.run_async(
        user_id="user-42", session_id=session.id,
        new_message=types.Content(role="user", parts=[types.Part(text="Hi!")]),
    ):
        for part in (event.content.parts if event.content else []):
            if part.text:
                print(part.text)

    session_service.close()

asyncio.run(main())
```

See [`examples/quickstart.py`](examples/quickstart.py) for the complete file. A `GOOGLE_API_KEY` is required to call the model. To run the tutorials without an API key, see [`docs/tutorials/`](docs/tutorials/).

## Connection URIs

All three services accept the same `aerospike://` URI scheme:

```
aerospike://[user:pass@]host[:3000][,host2[:port],…]/<namespace>[?option=value]
```

Examples:

```
aerospike://localhost:3000/test
aerospike://user:pass@host1:3000,host2:3000/adk?set_prefix=prod_&tls=true
```

Query parameters:
- `set_prefix=adk_`: default set-name prefix (lets multiple installations share one namespace)
- `tls=true`: enables TLS (use `tls_config=...` kwarg for mTLS details)

## Example: SessionService

```python
import asyncio
from google.adk.events import Event, EventActions
from google.genai import types
from adk_aerospike import AerospikeSessionService

async def main() -> None:
    svc = AerospikeSessionService.from_uri("aerospike://localhost:3000/test")

    # Create a session with initial state (mixes session-, app-, and user-scoped keys)
    session = await svc.create_session(
        app_name="support_bot",
        user_id="alice",
        state={
            "topic": "billing",            # session-scoped
            "app:tenant": "acme-corp",     # shared across all users of the app
            "user:nickname": "Allie",      # shared across alice's sessions
            "temp:scratch": "throwaway",   # in-process only — never persisted
        },
    )
    print(f"session id: {session.id}")

    # Append an event (one server-side atomic op — list_append + state delta + ts bump)
    await svc.append_event(
        session,
        Event(
            invocation_id="i1",
            author="user",
            content=types.Content(role="user",
                                  parts=[types.Part(text="Where's my invoice?")]),
            actions=EventActions(state_delta={"turn": 1}),
        ),
    )

    # Fetch — single batch_read across session + app_state + user_state (1 RTT)
    fetched = await svc.get_session(
        app_name="support_bot", user_id="alice", session_id=session.id
    )
    print(fetched.state)
    # {'topic': 'billing', 'turn': 1, 'app:tenant': 'acme-corp', 'user:nickname': 'Allie'}

    # List a user's sessions (events/state stripped per ADK contract)
    resp = await svc.list_sessions(app_name="support_bot", user_id="alice")
    print(f"{len(resp.sessions)} sessions")

    # Delete cascades to all segment records
    await svc.delete_session(
        app_name="support_bot", user_id="alice", session_id=session.id
    )

    svc.close()

asyncio.run(main())
```

State scoping (matches `google.adk.sessions.state.State`):

| Prefix | Storage location | Visibility |
| --- | --- | --- |
| `app:foo` | `adk_app_state` (one record per app) | All users of this app |
| `user:foo` | `adk_user_state` (one record per (app, user)) | This user across sessions |
| `temp:foo` | NOT PERSISTED | In-process, current invocation only |
| _(unprefixed)_ | On the session record | This session only |

## Example: ArtifactService

```python
import asyncio
from google.genai import types
from adk_aerospike import AerospikeArtifactService

async def main() -> None:
    svc = AerospikeArtifactService.from_uri("aerospike://localhost:3000/test")

    # Session-scoped: visible only within this session
    v = await svc.save_artifact(
        app_name="support_bot", user_id="alice", session_id="s-1",
        filename="receipt.png",
        artifact=types.Part(
            inline_data=types.Blob(mime_type="image/png", data=b"\x89PNG..."),
        ),
    )
    print(f"saved version {v}")  # 0

    # Save again → version 1
    await svc.save_artifact(
        app_name="support_bot", user_id="alice", session_id="s-1",
        filename="receipt.png",
        artifact=types.Part(
            inline_data=types.Blob(mime_type="image/png", data=b"\x89PNG..updated"),
        ),
    )

    # Load latest (or pass version=0 for the first)
    latest = await svc.load_artifact(
        app_name="support_bot", user_id="alice", session_id="s-1",
        filename="receipt.png",
    )
    print(latest.inline_data.mime_type, len(latest.inline_data.data))

    # User-scoped: 'user:' prefix → cross-session visible
    await svc.save_artifact(
        app_name="support_bot", user_id="alice", session_id="s-1",
        filename="user:avatar.jpg",
        artifact=types.Part(text="<jpeg bytes here>"),
    )
    # Same artifact is now visible from any session:
    avatar = await svc.load_artifact(
        app_name="support_bot", user_id="alice", session_id="s-2",  # different session
        filename="user:avatar.jpg",
    )
    assert avatar is not None

    # list_artifact_keys merges session-scoped + user-scoped
    keys = await svc.list_artifact_keys(
        app_name="support_bot", user_id="alice", session_id="s-1",
    )
    print(keys)  # ['receipt.png', 'user:avatar.jpg']

    # ADK 2.x metadata methods
    versions = await svc.list_artifact_versions(
        app_name="support_bot", user_id="alice", session_id="s-1",
        filename="receipt.png",
    )
    for v in versions:
        print(v.version, v.canonical_uri, v.mime_type, v.create_time)

    svc.close()

asyncio.run(main())
```

## Example: MemoryService

Lexical word-overlap search: same semantics as `InMemoryMemoryService`, using per-token posting-list primary keys (`app:user:kw:<token>`). No embedder.

```python
import asyncio
from google.adk.events import Event, EventActions
from google.adk.sessions import Session
from google.genai import types
from adk_aerospike import AerospikeMemoryService

async def main() -> None:
    memory = AerospikeMemoryService.from_uri(
        "aerospike://localhost:3000/test", top_k=10,
    )

    # Persist a session's text events to long-term memory. Text is tokenized
    # into keywords; each token updates a posting-list row and a memory row.
    session = Session(
        id="s-1", app_name="support_bot", user_id="alice",
        events=[
            Event(invocation_id="i", author="user",
                  content=types.Content(role="user",
                      parts=[types.Part(text="Python uses duck typing.")]),
                  actions=EventActions()),
            Event(invocation_id="j", author="user",
                  content=types.Content(role="user",
                      parts=[types.Part(text="My favorite color is blue.")]),
                  actions=EventActions()),
        ],
    )
    await memory.add_session_to_memory(session)

    # Search — batch_read posting lists per query token, union refs,
    # batch_read memory rows, rank by token overlap.
    resp = await memory.search_memory(
        app_name="support_bot", user_id="alice", query="python duck typing",
    )
    for m in resp.memories:
        print(m.author, m.timestamp, m.content.parts[0].text)
    # → user 2026-... Python uses duck typing.

    memory.close()

asyncio.run(main())
```

## Use with `adk web` / `adk run`

Register the URI scheme once, e.g. in a `services.py` next to your agent:

```python
# services.py
import adk_aerospike
adk_aerospike.register()
```

Then drive the CLI normally:

```bash
adk web \
  --session_db_url=aerospike://localhost:3000/adk \
  --artifact_storage_uri=aerospike://localhost:3000/adk \
  --memory_service_uri=aerospike://localhost:3000/adk
```

## Storage layout

Five sets in a single namespace (default prefix `adk_`):

```
adk_sessions      app:user:session                  ← session record (state + segment pointer)
                  app:user:session:g:NNNNNNNN        ← segment record (events, K_ORDERED Map)
                  app:user:sl                        ← session-id manifest (list_sessions)

adk_app_state     app                               ← one per (app)
adk_user_state    app:user                          ← one per (app, user)

adk_artifacts     app:user:session:fname:NNNNNNNN
                  app:user:user:user:fname:NNNNNNNN  ← user-scoped (sentinel "user")

adk_memory        app:user:session:eventid          ← memory row
                  app:user:kw:token                 ← posting list ({eid,sid,ts} refs)
```

The session record is the hot path. Events accumulate in K_ORDERED Map segment records; when a segment fills, Aerospike returns `RecordTooBig` and the writer advances to the next segment. Most `append_event` calls are a single server-side atomic `operate()`; `get_session` is a single `batch_read` (session + app_state + user_state in one RTT) plus segment reads only when the requested event window spans multiple segments.

For the full design (atomicity, key formats, indexes, trade-offs), see [`design.md`](design.md).

## Running tests

CI runs on every push to `main` and on pull requests (see
[`.github/workflows/tests.yml`](.github/workflows/tests.yml)):

- Unit: `pytest -m "not aerospike"` on Python 3.11 and 3.12 (no Docker).
- Integration: starts Aerospike CE with [`scripts/start_aerospike_ce.sh`](scripts/start_aerospike_ce.sh) (`docker run aerospike/aerospike-server:latest`), then `pytest -m aerospike` (~4 minutes).

Locally:

```bash
pip install -e ".[dev]"

# Unit tests only (no Docker required, ~2s)
pytest -m "not aerospike"

# Integration — explicit Aerospike CE container (matches CI)
./scripts/start_aerospike_ce.sh
set -a && source .aerospike-ci.env && set +a
pytest -m aerospike
./scripts/stop_aerospike_ce.sh

# Integration — or let testcontainers start Aerospike for you (no script)
pytest -m aerospike

# Full suite (testcontainers path if env vars unset)
pytest
```

## Documentation

- [`design.md`](design.md): full design document (atomicity, key formats, indexes, trade-offs)
- [`docs/tutorials/`](docs/tutorials/): step-by-step tutorials (no API key required)
- [`docs/data-model.md`](docs/data-model.md): set/bin/index reference
- [`docs/integrations.md`](docs/integrations.md): ADK integration catalog entry
- [`examples/`](examples/): runnable examples including end-to-end validation against Google's official [`adk-samples`](https://github.com/google/adk-samples) (see [`examples/README.md`](examples/README.md))

## Comparison with other ADK storage integrations

| Integration | Maintainer | Sess | Art | Mem | Architecture |
| --- | --- | :-: | :-: | :-: | --- |
| **adk-aerospike** (this) | Aerospike | ✓ | ✓ | ✓ lexical | In-process, single backend |
| adk-redis | Redis Inc. | ✓ | ✗ | ✓ | HTTP sidecar (Agent Memory Server) + RedisVL |
| adk-python built-in | Google | ✓ | ✓ | ✓ | In-process / Vertex managed |
| adk-extra-services | Community | ✓ Mongo/Redis | ✓ S3/Local/Azure | ✗ | In-process |
| google-adk-extras | Community | ✓ SQL/Mongo/Redis | ✓ Local/S3/SQL | ✓ keyword-only | In-process |
| Pinecone / Qdrant / Couchbase / Chroma | Vendors | ✗ | ✗ | ✗ (just tools) | MCP server |

This is the only package shipping Session, Artifact, and Memory in one in-process Aerospike backend. Memory search is lexical word-overlap (same semantics as `InMemoryMemoryService`), not embedding-based.

## License

Apache-2.0
