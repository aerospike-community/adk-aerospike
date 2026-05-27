---
catalog_title: Aerospike
catalog_description: Aerospike-backed Session, Memory, and Artifact services — sub-millisecond KV, single in-process backend, no sidecar.
catalog_icon: aerospike.png
---

# Aerospike

[Aerospike](https://aerospike.com/) is a distributed, real-time NoSQL database.
The `adk-aerospike` package provides full implementations of all three ADK
storage interfaces on top of a single Aerospike cluster.

## Use cases

- **Low-latency agent state**: sub-millisecond reads/writes for sessions in
  high-throughput agents (chatbots, voice, real-time tool orchestration).
- **Server-side lexical memory**: text is tokenized at write time and stored
  as a `keywords` list bin. Search runs server-side via Aerospike's
  list-element secondary index — same word-overlap semantics as ADK's
  built-in `InMemoryMemoryService`, executed in the database rather than the
  client.
- **One database for the whole agent layer**: sessions, artifacts, and memory
  share a single cluster.

## Prerequisites

- Aerospike Database 7.x or later (Community or Enterprise)
- Python 3.11+

## Installation

```bash
pip install adk-aerospike
```

## Use with agent

```python
from google.adk.agents import LlmAgent
from google.adk.runners import Runner

from adk_aerospike import AerospikeSessionService

session_service = AerospikeSessionService.from_uri(
    "aerospike://localhost:3000/adk"
)

agent = LlmAgent(name="assistant", model="gemini-2.0-flash")
runner = Runner(
    agent=agent,
    app_name="myapp",
    session_service=session_service,
)
```

## Use with `adk web`

In a `services.py` next to your agent:

```python
import adk_aerospike
adk_aerospike.register()
```

Then:

```bash
adk web --session_db_url=aerospike://localhost:3000/adk
```

## Available services

| Service                        | ADK interface          | Notes                                      |
| ------------------------------ | ---------------------- | ------------------------------------------ |
| `AerospikeSessionService`      | `BaseSessionService`   | Session + event + scoped state, chunked at 256 KiB |
| `AerospikeArtifactService`     | `BaseArtifactService`  | Versioned blobs, ≤8 MiB inline             |
| `AerospikeMemoryService`       | `BaseMemoryService`    | Lexical word-overlap via list-element secondary index |

## Resources

- [adk-aerospike on GitHub](https://github.com/aerospike-community/adk-aerospike)
- [Aerospike documentation](https://aerospike.com/docs/)
