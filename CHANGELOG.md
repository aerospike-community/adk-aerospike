# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-06-22

### Changed

- **BREAKING (session on-disk layout).** `AerospikeSessionService` event history now lives in append-only **segment** records (`app:user:session:g:NNNNNNNN`), each a single K_ORDERED `events` Map keyed `"{ts_micros:020d}:{event_id}"`. This replaces the previous hot-tail-on-the-session-record plus sealed-`c:` chunk-record model. Sessions written by `<= 0.0.2` are **not** read by this version (clean break, no migrator).
- Event history now orders by `event.timestamp` (the map key), not by insertion order. Identical for monotonic timestamps; events sharing a microsecond are tie-broken by `event_id`.

### Added

- Idempotent `append_event`: the segment-map key is a pure function of `(event.id, event.timestamp)`, so a retried/duplicated append overwrites the same slot and can never duplicate an event in history.
- Overflow-driven segment rollover: a real `RecordTooBig` is the only rollover trigger (no client-side byte estimation/thresholds). `cur` is advanced with a `cur == N` guarded increment so concurrent rollovers converge.
- Single-RTT append hot path: a no-state-delta append is one `operate`; an append carrying state is one `batch_write` coalescing the segment write with the session/app/user state writes (multi-scope appends now match single-scope latency instead of issuing up to three sequential operates).
- Bounded retry on transient write back-pressure: the (now-idempotent) append hot path retries `DeviceOverload` and ambiguous `TimeoutError` with jittered backoff, including per-record `DeviceOverload` in the coalesced `batch_write`. `create_session` retries `DeviceOverload` only (a `POLICY_EXISTS_CREATE` put is not timeout-idempotent).

### Fixed

- Heavy-concurrency data loss / unrecoverable `RecordTooBig` on a single hot session: large concurrent appends previously overran the 1 MiB write-block-size and dropped events. Segments now pack to `max-record-size` and roll over with no loss and no duplication.
- A lone event exceeding `max-record-size` now raises a clear `RecordTooBig` instead of looping (object-store spill remains future work).

### Removed

- `flush_threshold_bytes`, `huge_event_bytes`, `max_tail_bytes` constructor params; `codec.estimate_event_size`; `keys.chunk_key`; the `tbytes` / `chunks` / `seq` session bins and the `ts_lo` / `ts_hi` / `cidx` chunk bins.
- Drop `[benchmark]` optional extra from PyPI metadata; use `benchmarks/requirements.txt` instead (PyPI rejects VCS direct dependencies).

## [0.0.2] - 2026-05-28

### Added

- Per-user session manifest (`app:user:sl`) and bin-projected `list_sessions(app, user)` metadata reads.
- Posting-list inverted index for lexical memory search (`app:user:kw:<token>` primary keys).
- Data-model diagrams and updated `docs/data-model.md` (manifests, posting rows, artifact head records).
- Ecosystem benchmarks: `--backend redis`, `--results-dir`, `paired_*` profiles; deps in `benchmarks/requirements.txt` (`google-adk-extras`, `redis`, `sqlalchemy`).
- `benchmarks/workloads/_redis_backend.py` and `redis_*` methods on ecosystem workloads for cross-backend comparison via `google-adk-extras`.
- `AerospikeMemoryService.add_events_to_memory` (incremental event ingest with dedupe by event id).
- Upstream ADK contract tests ported from `google/adk-python` for sessions, artifacts, and memory.

### Changed

- `AerospikeMemoryService.search_memory` uses posting-list `batch_read` per query token (no list-element secondary index on `keywords`).
- `AerospikeSessionService.list_sessions` with `user_id` reads the session manifest instead of scanning `idx_*_sess_uid`.
- Inline event codec bumped to v2: full `Event.model_dump` stored under `payload` for lossless round-trip (v0/v1 records remain readable).

### Fixed

- Concurrent `save_artifact` calls allocate distinct versions via atomic `operate` on a per-file head record (`:__head__`).

## [0.0.1] - 2026-05-27

### Added

- Initial PyPI release.
- `AerospikeSessionService`, `AerospikeArtifactService`, and `AerospikeMemoryService` for Google ADK 2.x.
- `aerospike://` URI support via `adk_aerospike.register()`.
- Chunked session records, server-side lexical memory search, and composite tenant indexes.

[Unreleased]: https://github.com/aerospike-community/adk-aerospike/compare/v0.0.2...HEAD
[0.0.2]: https://github.com/aerospike-community/adk-aerospike/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/aerospike-community/adk-aerospike/releases/tag/v0.0.1
