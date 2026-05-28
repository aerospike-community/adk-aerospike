# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Per-user session manifest (`app:user:sl`) and bin-projected `list_sessions(app, user)` metadata reads.
- Posting-list inverted index for lexical memory search (`app:user:kw:<token>` primary keys).
- Data-model diagrams and updated `docs/data-model.md` (manifests, posting rows, artifact head records).
- Ecosystem benchmarks: `--backend redis`, `--results-dir`, `paired_*` profiles, and `[benchmark]` extras (`google-adk-extras`, `redis`, `sqlalchemy`).
- `benchmarks/workloads/_redis_backend.py` and `redis_*` methods on ecosystem workloads for cross-backend comparison via `google-adk-extras`.
- `AerospikeMemoryService.add_events_to_memory` (incremental event ingest with dedupe by event id).
- Upstream ADK contract tests ported from `google/adk-python` for sessions, artifacts, and memory.

### Changed

- `AerospikeMemoryService.search_memory` uses posting-list `batch_read` per query token (no list-element secondary index on `keywords`).
- `AerospikeSessionService.list_sessions` with `user_id` reads the session manifest instead of scanning `idx_*_sess_uid`.
- Inline event codec bumped to v2: full `Event.model_dump` stored under `payload` for lossless round-trip (v0/v1 records remain readable).

## [0.0.1] - 2026-05-27

### Added

- Initial PyPI release.
- `AerospikeSessionService`, `AerospikeArtifactService`, and `AerospikeMemoryService` for Google ADK 2.x.
- `aerospike://` URI support via `adk_aerospike.register()`.
- Chunked session records, server-side lexical memory search, and composite tenant indexes.

[0.0.1]: https://github.com/aerospike-community/adk-aerospike/releases/tag/v0.0.1
