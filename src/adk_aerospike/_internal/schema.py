"""Single source of truth for Aerospike set, bin, and index names.

Change names here only — every other module imports from this file. Keeping the
schema centralised makes it trivial to evolve the layout (rename a set, add a
bin, introduce a secondary index) without grepping the codebase.

Layout overview
---------------
Namespace: caller-configurable (passed to client + each service). All sets below
live within that single namespace.

Set names use ``set_prefix`` (default ``"adk_"``) so multiple ADK installations
can share one namespace without colliding.

Sessions (single set holds two record kinds)
~~~~~~~~~
- ``{prefix}sessions``      session records AND chunk records — distinguished by
  bin shape (session has ``state`` + denormalised index bins ``app``/``uid``/``sid``;
  chunk has ``cidx``, no index bins)

  Session record bins:
    app, uid, sid                  denormalised for list_sessions sec-indexes
    state    Map                   session-scoped state only
    events   List                  hot tail of recent events
    ts       float                 last update time
    seq      int                   total events ever appended
    chunks   int                   number of sealed chunks (== next chunk index)
    tbytes   int                   estimated byte size of the tail

  Chunk record bins:
    cidx     int                   chunk index (discriminator; no index bins)
    events   List                  sealed (immutable) batch of events
    ts_lo    float                 timestamp of first event in chunk
    ts_hi    float                 timestamp of last event in chunk

- ``{prefix}app_state``     one record per (app_name)
- ``{prefix}user_state``    one record per (app_name, user_id)

Artifacts
~~~~~~~~~
- ``{prefix}artifacts``     one record per (app, user, session, filename, version)

Memory
~~~~~~
Stored in core Aerospike as text + a tokenized ``keywords`` list bin. Search
uses Aerospike's **list-element secondary index** — the canonical pattern for
keyword/tag lookups (see Aerospike 3.8 release notes and the "Query JSON
Documents Faster with New CDT Indexing" blog).

Mirrors the lexical word-overlap semantics of ``InMemoryMemoryService`` upstream
but executes the matching server-side via
``predicates.contains(bin, INDEX_TYPE_LIST, token)``.

- ``{prefix}memory``        one record per memory entry
  bins: app, uid, sid, eid, text, keywords (list[str]), author, ts, content (Map)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

DEFAULT_SET_PREFIX: Final = "adk_"

DEFAULT_FLUSH_THRESHOLD_BYTES: Final = 256 * 1024
"""Tail size at which a session record flushes the hot events list to a chunk.

¼ of the default Aerospike ``write-block-size`` (1 MiB) gives plenty of safety
margin for state Map growth, retry slack, and estimator error.
"""

DEFAULT_HUGE_EVENT_BYTES: Final = 900 * 1024
"""A single event larger than this gets flushed to its own chunk.

Just below the 1 MiB write-block-size so even a huge event fits as a chunk by
itself without bumping into the hard limit.
"""


@dataclass(frozen=True, slots=True)
class Schema:
    """Resolved set/index names for a given installation."""

    namespace: str
    set_prefix: str = DEFAULT_SET_PREFIX

    @property
    def sessions_set(self) -> str:
        return f"{self.set_prefix}sessions"

    @property
    def app_state_set(self) -> str:
        return f"{self.set_prefix}app_state"

    @property
    def user_state_set(self) -> str:
        return f"{self.set_prefix}user_state"

    @property
    def artifacts_set(self) -> str:
        return f"{self.set_prefix}artifacts"

    @property
    def memory_set(self) -> str:
        return f"{self.set_prefix}memory"


class Bins:
    """Bin names — kept short (≤14 chars) because Aerospike includes them in every record."""

    APP_NAME: Final = "app"
    USER_ID: Final = "uid"
    SESSION_ID: Final = "sid"
    EVENT_SEQ: Final = "seq"
    STATE: Final = "state"
    TIMESTAMP: Final = "ts"
    LAST_UPDATE: Final = "ts"
    AUTHOR: Final = "author"
    CONTENT: Final = "content"
    ACTIONS: Final = "actions"
    BRANCH: Final = "branch"
    FILENAME: Final = "fname"
    VERSION: Final = "ver"
    MIME_TYPE: Final = "mime"
    DATA: Final = "data"
    CREATE_TIME: Final = "ctime"
    CUSTOM_META: Final = "cmeta"
    EVENT_ID: Final = "eid"
    TEXT: Final = "text"
    KEYWORDS: Final = "keywords"

    # Composite "app:user:scope" bin — denormalised so secondary-index queries
    # can target a single tenant slot instead of scanning all rows that share
    # a filename / user. See ``keys.scope_tuple``.
    SCOPE_TUPLE: Final = "aus"

    # Chunked-session bins
    EVENTS: Final = "events"
    CHUNKS: Final = "chunks"
    TAIL_BYTES: Final = "tbytes"
    CHUNK_IDX: Final = "cidx"
    TS_LO: Final = "ts_lo"
    TS_HI: Final = "ts_hi"


class StateScope:
    """State key prefixes — see ``google.adk.sessions.State`` for canonical semantics.

    Unprefixed keys are session-scoped; prefixed keys are routed to other sets.
    """

    APP: Final = "app:"
    USER: Final = "user:"
    TEMP: Final = "temp:"
