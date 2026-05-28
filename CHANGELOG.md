# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `benchmarks/workloads/_redis_backend.py` — `google-adk-extras` session/memory services and a Redis hash store for artifact benchmarks.
- `redis_*` methods on ecosystem workloads (`session_hotpath`, `memory_lexical`, `artifacts`, `agent_turn`, `chunk_stress`) mirroring each `aerospike_*` op.
- `build_workload(..., backend="aerospike"|"redis")` in `benchmarks/workloads/__init__.py`.

## [0.0.1] - 2026-05-27

### Added

- Initial PyPI release.
- `AerospikeSessionService`, `AerospikeArtifactService`, and `AerospikeMemoryService` for Google ADK 2.x.
- `aerospike://` URI support via `adk_aerospike.register()`.
- Chunked session records, server-side lexical memory search, and composite tenant indexes.

[0.0.1]: https://github.com/aerospike-community/adk-aerospike/releases/tag/v0.0.1
