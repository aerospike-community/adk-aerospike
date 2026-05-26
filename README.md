# adk-aerospike

Aerospike-backed storage services for [Google Agent Development Kit (ADK)](https://adk.dev/).

**Status:** alpha (0.0.1). All three ADK storage interfaces implemented end-to-end against ADK 2.x. 39 tests passing.

## What's in here

Three implementations of ADK's pluggable storage interfaces, plus URI-scheme
registration so the `adk` CLI can use Aerospike directly:

| ADK interface         | This package               | Backed by                                                      |
| --------------------- | -------------------------- | -------------------------------------------------------------- |
| `BaseSessionService`  | `AerospikeSessionService`  | Aerospike KV + Map/List CDTs (chunked session records)         |
| `BaseArtifactService` | `AerospikeArtifactService` | Aerospike KV (one record per version)                          |
| `BaseMemoryService`   | `AerospikeMemoryService`   | Aerospike KV (server-side keyword search via list-element secondary index) |

### Why use this

- **All three ADK storage interfaces in one package** — Session, Artifact,
  and Memory, backed by a single Aerospike cluster.
- **Native in-process client.** Talks directly to Aerospike; nothing extra
  to deploy or operate.
- **Lexical memory search runs server-side in Aerospike** — text is
  tokenized at write time and stored as a `keywords` list bin; queries use
  Aerospike's list-element secondary index via
  `predicates.contains(..., INDEX_TYPE_LIST)`. Same word-overlap semantics
  as ADK's built-in `InMemoryMemoryService`, executed on the database.
- **Single-record server-side atomic appends** — Aerospike Map/List CDTs let
  `append_event` commit state delta, event append, and timestamp bump in one
  round trip.
- **Chunked session records** handle long event histories without hitting
  Aerospike's `write-block-size` limit, while keeping the hot path a single
  operation.
- **Single round-trip `get_session`** — `batch_read` fetches the session
  record, app-state record, and user-state record in one network call.
- **`adk web` integration** — register the `aerospike://` URI scheme and the
  CLI flags work out of the box.
- **No AI/ML dependency.** Memory is a storage backend, not an embedding
  pipeline — no embedder to wire up, no model to host.

## Current Install

**Ensure your python version is 11 before proceeding.**
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .

# To test:
python -c "from adk_aerospike import AerospikeSessionService; print('ok')"
# Should print ok below.
```

Requires Python 3.11+ and Aerospike Database 7.x or 8.x (Community or Enterprise).

## Future Install

```bash
pip install adk-aerospike
```

Requires Python 3.11+ and Aerospike Database 7.x or 8.x (Community or Enterprise).

## Quick start

Spin up a local Aerospike container:

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

See [`examples/quickstart.py`](examples/quickstart.py) for the complete file
(needs a `GOOGLE_API_KEY` to actually call the model).

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
- `set_prefix=adk_` — default set-name prefix (lets multiple installations share one namespace)
- `tls=true` — enables TLS (use `tls_config=...` kwarg for mTLS details)

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

    # Delete cascades to every sealed chunk
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

Lexical word-overlap search — same semantics as `InMemoryMemoryService`,
executed server-side via Aerospike's list-element secondary index. No
embedder.

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
    # client-side into a list[str] of lowercase [A-Za-z]+ words and stored
    # in a `keywords` bin indexed by Aerospike's list-element secondary index.
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

    # Search — query is tokenized, then for each token Aerospike's
    # list-element index returns matching records via predicates.contains.
    # Client-side: union, scope-filter by (app, user), rank by token-overlap.
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

## Storage shape — what ends up in Aerospike

Five sets in a single namespace (default prefix `adk_`):

```
adk_sessions      app:user:session                ← session record (state + hot tail)
                  app:user:session:c:NNNNNNNN  ← sealed chunk record (older events)

adk_app_state     app                                   ← one per (app)
adk_user_state    app:user                           ← one per (app, user)

adk_artifacts     app:user:session:fname:NNNNNNNN
                  app:user:user:user:fname:NNNNNNNN   ← user-scoped (sentinel "user")

adk_memory        app:user:session:eventid
                  bins include keywords: list[str], indexed via list-element
                  secondary index for server-side word-overlap search
```

The session record is the hot path — events accumulate in an inline List bin
(the *hot tail*) until it reaches a 256 KiB threshold, then flush to a sealed
chunk record. Most `append_event` calls are a single server-side atomic
`operate()`; `get_session` is a single `batch_read` (session + app_state +
user_state in one RTT) plus chunk reads only when the requested event
window exceeds the tail.

For the full design (chunking, atomicity, key formats, indexes, trade-offs),
see [`design.md`](design.md).

## Running tests

The integration tests spin up an Aerospike container via `testcontainers`:

```bash
pip install -e ".[dev]"

# Unit tests only (no Docker required, ~2s)
pytest -m "not aerospike"

# Full suite (~2 minutes, requires Docker)
pytest
```

Current state: **39 passed, 0 skipped, 0 failed.**

## Documentation

- [`design.md`](design.md) — full design document (for engineering / DevRel)
- [`docs/data-model.md`](docs/data-model.md) — set/bin/index reference
- [`docs/integrations.md`](docs/integrations.md) — ADK integration catalog entry
- [`examples/`](examples/) — runnable examples, including end-to-end validation against Google's official [`adk-samples`](https://github.com/google/adk-samples) (7/7 wiring + 7/7 real-Gemini E2E — see [`examples/README.md`](examples/README.md))
- [`CLAUDE.md`](CLAUDE.md) — auto-loaded context for Claude Code sessions

## Comparison with other ADK storage integrations

| Integration | Maintainer | Sess | Art | Mem | Architecture |
| --- | --- | :-: | :-: | :-: | --- |
| **adk-aerospike** (this) | Aerospike | ✓ | ✓ | ✓ semantic | In-process, single backend |
| adk-redis | Redis Inc. | ✓ | ✗ | ✓ | HTTP sidecar (Agent Memory Server) + RedisVL |
| adk-python built-in | Google | ✓ | ✓ | ✓ | In-process / Vertex managed |
| adk-extra-services | Community | ✓ Mongo/Redis | ✓ S3/Local/Azure | ✗ | In-process |
| google-adk-extras | Community | ✓ SQL/Mongo/Redis | ✓ Local/S3/SQL | ✓ keyword-only | In-process |
| Pinecone / Qdrant / Couchbase / Chroma | Vendors | ✗ | ✗ | ✗ (just tools) | MCP server |

We're the only package shipping all three storage interfaces with
embedding-based semantic memory, backed by a single in-process database.

## License

Apache-2.0
