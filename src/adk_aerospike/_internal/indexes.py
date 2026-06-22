"""Idempotent secondary-index creation.

Aerospike secondary indexes are namespace-wide artefacts created via DDL-like
calls (``client.index_string_create`` / ``client.index_integer_create``). The
service classes call ``ensure_*_indexes`` once at construction time so a fresh
cluster gets the right indexes without an out-of-band setup step.

Best-practice notes
-------------------
- Index creation is **synchronous DDL**: the call returns once the request is
  sent, but the index build may continue in the background. For a brand-new
  cluster the build completes in milliseconds.
- We catch ``IndexFoundError`` (raised when an index already exists with the
  same definition) and swallow it. Any other exception propagates.
- Index *names* are derived from set + bin, so renaming a set or bin requires
  an explicit migration. See ``data-model.md``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .schema import BinName, Schema

if TYPE_CHECKING:
    import aerospike

log = logging.getLogger(__name__)


def ensure_session_indexes(client: aerospike.Client, schema: Schema) -> None:
    """Create secondary indexes used by the SessionService. Idempotent.

    Note: the sessions set holds both session records and segment records.
    Segment records deliberately omit the ``app``/``uid``/``sid`` bins so they
    don't appear in these indexes. ``list_sessions(app, user)`` uses the
    ``app:user:sl`` manifest (PK + bin-projected ``batch_write`` reads), not these indexes.
    """
    import aerospike
    from aerospike import exception as ae

    indexes = (
        # (set, bin, type, index name)  — used by list_sessions(app) without user_id
        (
            schema.sessions_set,
            BinName.USER_ID,
            "string",
            f"idx_{schema.set_prefix}sess_uid",
        ),
        (
            schema.sessions_set,
            BinName.APP_NAME,
            "string",
            f"idx_{schema.set_prefix}sess_app",
        ),
    )

    for set_name, bin_name, kind, idx_name in indexes:
        try:
            client.index_single_value_create(
                schema.namespace, set_name, bin_name, _index_kind(kind), idx_name
            )
            log.info("Created secondary index %s on %s.%s", idx_name, set_name, bin_name)
        except ae.IndexFoundError:
            log.debug("Index %s already exists; skipping", idx_name)


def ensure_artifact_indexes(client: aerospike.Client, schema: Schema) -> None:
    """Create secondary indexes used by the ArtifactService. Idempotent.

    The ``aus`` (app:user:scope) composite index is the load-bearing one — it
    lets ``list_artifact_keys`` and ``_rows_for`` target a single tenant slot
    without a sec-index-then-Python-filter scan over unrelated tenants'
    artifacts. The ``fname`` index is retained for direct filename lookups
    and to enable mixed predicate queries (currently AND-narrowing happens
    client-side by reading ``aus`` rows and filtering by filename).
    """
    import aerospike
    from aerospike import exception as ae

    indexes = (
        # Composite tenant index — used by list_artifact_keys / _rows_for.
        (
            schema.artifacts_set,
            BinName.SCOPE_TUPLE,
            "string",
            f"idx_{schema.set_prefix}art_aus",
        ),
        (
            schema.artifacts_set,
            BinName.FILENAME,
            "string",
            f"idx_{schema.set_prefix}art_fname",
        ),
    )

    for set_name, bin_name, kind, idx_name in indexes:
        try:
            client.index_single_value_create(
                schema.namespace, set_name, bin_name, _index_kind(kind), idx_name
            )
            log.info("Created secondary index %s on %s.%s", idx_name, set_name, bin_name)
        except ae.IndexFoundError:
            log.debug("Index %s already exists; skipping", idx_name)


def ensure_memory_indexes(client: aerospike.Client, schema: Schema) -> None:
    """Create secondary indexes used by the MemoryService. Idempotent.

    ``idx_<prefix>mem_aus`` — scalar index on ``aus`` (``app:user:session``).
    Used only by the purge step in ``add_session_to_memory`` (cold path).
    Search uses posting-list primary keys (``app:user:kw:token``), not SI.
    """
    import aerospike
    from aerospike import exception as ae

    try:
        client.index_single_value_create(
            schema.namespace,
            schema.memory_set,
            BinName.SCOPE_TUPLE,
            aerospike.INDEX_STRING,
            f"idx_{schema.set_prefix}mem_aus",
        )
        log.info("Created scalar index idx_%smem_aus", schema.set_prefix)
    except ae.IndexFoundError:
        log.debug("idx_%smem_aus already exists", schema.set_prefix)


def _index_kind(kind: str) -> int:
    """Map a string literal to ``aerospike.INDEX_*`` constants."""
    import aerospike

    if kind == "string":
        return aerospike.INDEX_STRING
    if kind == "integer":
        return aerospike.INDEX_NUMERIC
    raise ValueError(f"unsupported index kind {kind!r}")
