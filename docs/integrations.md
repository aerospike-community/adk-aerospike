---
catalog_title: Aerospike
catalog_description: Aerospike-backed Session, Memory, and Artifact services — low-latency KV, native vector search, no sidecar.
catalog_icon: aerospike.png
---

# Aerospike

[Aerospike](https://aerospike.com/) is a distributed, real-time NoSQL database
with native vector search. The `adk-aerospike` package provides full
implementations of all three ADK storage interfaces.

## Use cases

- **Low-latency agent state**: sub-millisecond reads/writes for sessions in
  high-throughput agents (chatbots, voice, real-time tool orchestration).
- **Persistent semantic memory**: embeddings stored as `list[float]` bins in
  core Aerospike; brute-force cosine similarity after a metadata pre-filter on
  `app_name` and `user_id`.
- **One database for the whole agent layer**: sessions, artifacts, and memory
  share a single cluster — no Redis-for-sessions + Pinecone-for-memory split.

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
| `AerospikeSessionService`      | `BaseSessionService`   | Session + event + scoped state             |
| `AerospikeArtifactService`     | `BaseArtifactService`  | Versioned blobs, ≤8 MiB inline             |
| `AerospikeMemoryService`       | `BaseMemoryService`    | Embeddings stored in core Aerospike; brute-force cosine |

## Resources

- [adk-aerospike on GitHub](https://github.com/aerospike/adk-aerospike)
- [Aerospike documentation](https://aerospike.com/docs/)
