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

from .schema import Schema

if TYPE_CHECKING:
    import aerospike

log = logging.getLogger(__name__)


def ensure_session_indexes(client: aerospike.Client, schema: Schema) -> None:
    """Create secondary indexes used by the SessionService. Idempotent.

    Note: the sessions set holds both session records and chunk records.
    Chunk records deliberately omit the ``app``/``uid``/``sid`` bins so they
    don't appear in these indexes — ``list_sessions`` queries return session
    records only, without a client-side filter step.
    """
    import aerospike
    from aerospike import exception as ae

    indexes = (
        # (set, bin, type, index name)  — used by list_sessions(app, user)
        (schema.sessions_set, "uid", "string", f"idx_{schema.set_prefix}sess_uid"),
        (schema.sessions_set, "app", "string", f"idx_{schema.set_prefix}sess_app"),
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
    """Create secondary indexes used by the ArtifactService. Idempotent."""
    import aerospike
    from aerospike import exception as ae

    indexes = (
        # used by list_artifact_keys / list_versions
        (schema.artifacts_set, "sid", "string", f"idx_{schema.set_prefix}art_sid"),
        (schema.artifacts_set, "fname", "string", f"idx_{schema.set_prefix}art_fname"),
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

    Two indexes:

    - ``idx_<prefix>mem_kw`` — **list-element index** on the ``keywords`` bin.
      Used by ``search_memory`` for the server-side keyword lookup via
      ``predicates.contains(bin, INDEX_TYPE_LIST, token)``. This is the
      canonical Aerospike pattern for tag/keyword search; see Aerospike 3.8
      release notes and the "Query JSON Documents Faster with New CDT
      Indexing" blog.
    - ``idx_<prefix>mem_uid`` — scalar string index on ``uid``. Used by
      ``add_session_to_memory``'s purge step to find stale memories from a
      prior add of the same session.
    """
    import aerospike
    from aerospike import exception as ae

    # uid scalar index — for the purge query
    try:
        client.index_single_value_create(
            schema.namespace,
            schema.memory_set,
            "uid",
            aerospike.INDEX_STRING,
            f"idx_{schema.set_prefix}mem_uid",
        )
        log.info("Created scalar index idx_%smem_uid", schema.set_prefix)
    except ae.IndexFoundError:
        log.debug("idx_%smem_uid already exists", schema.set_prefix)

    # keywords list-element index — for keyword search
    try:
        client.index_list_create(
            schema.namespace,
            schema.memory_set,
            "keywords",
            aerospike.INDEX_STRING,
            f"idx_{schema.set_prefix}mem_kw",
        )
        log.info("Created list-element index idx_%smem_kw", schema.set_prefix)
    except ae.IndexFoundError:
        log.debug("idx_%smem_kw already exists", schema.set_prefix)


def _index_kind(kind: str) -> int:
    """Map a string literal to ``aerospike.INDEX_*`` constants."""
    import aerospike

    if kind == "string":
        return aerospike.INDEX_STRING
    if kind == "integer":
        return aerospike.INDEX_NUMERIC
    raise ValueError(f"unsupported index kind {kind!r}")
