"""Single source of truth for Aerospike set, bin, and index names.

Change names here only — every other module imports from this file. Keeping the
schema centralised makes it trivial to evolve the layout (rename a set, add a
bin, introduce a secondary index) without grepping the codebase.

Registry
--------
:class:`StorageSet`, :class:`BinName`, and :class:`EventFieldName` are
:class:`enum.StrEnum` values (the wire string Aerospike stores). Each has a
matching entry in :data:`SET_REGISTRY`, :data:`BIN_REGISTRY`, or
:data:`EVENT_FIELD_REGISTRY` with the full English name, value type, and where
the field is used. :class:`Bins` remains a compatibility alias namespace whose
attributes are the same :class:`BinName` members.

See ``docs/data-model.md`` for the human-readable tables (generated from the
same definitions).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
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

EVENT_SCHEMA_VERSION: Final = 1
"""On-record version tag for inline event Maps (``EventFieldName.SCHEMA_VERSION``).

Bumped only when the field set changes in a way the reader must disambiguate.
See :data:`EVENT_FIELD_REGISTRY` and ``codec.event_from_inline_dict``.
"""


class StorageSet(StrEnum):
    """Set name suffix (without ``set_prefix``). Full set = ``{prefix}{suffix}``."""

    SESSIONS = "sessions"
    APP_STATE = "app_state"
    USER_STATE = "user_state"
    ARTIFACTS = "artifacts"
    MEMORY = "memory"


class RecordKind(StrEnum):
    """Which record shape within a set carries a bin."""

    SESSION = "session_record"
    CHUNK = "chunk_record"
    APP_STATE_ROW = "app_state_row"
    USER_STATE_ROW = "user_state_row"
    ARTIFACT = "artifact_record"
    MEMORY = "memory_record"
    INLINE_EVENT = "inline_event_map"


class BinName(StrEnum):
    """Aerospike bin wire names (≤14 chars — stored on every record)."""

    APP_NAME = "app"
    USER_ID = "uid"
    SESSION_ID = "sid"
    SCOPE_TUPLE = "aus"
    EVENT_SEQ = "seq"
    STATE = "state"
    TIMESTAMP = "ts"
    AUTHOR = "author"
    CONTENT = "content"
    ACTIONS = "actions"
    BRANCH = "branch"
    FILENAME = "fname"
    VERSION = "ver"
    MIME_TYPE = "mime"
    DATA = "data"
    CREATE_TIME = "ctime"
    CUSTOM_META = "cmeta"
    EVENT_ID = "eid"
    TEXT = "text"
    KEYWORDS = "keywords"
    MEM_POSTINGS = "mpl"
    SESSION_MANIFEST = "sman"
    EVENTS = "events"
    CHUNKS = "chunks"
    TAIL_BYTES = "tbytes"
    CHUNK_IDX = "cidx"
    TS_LO = "ts_lo"
    TS_HI = "ts_hi"


class EventFieldName(StrEnum):
    """Keys inside each event Map stored in an ``events`` List bin."""

    SCHEMA_VERSION = "_v"
    EVENT_ID = "eid"
    TIMESTAMP = "ts"
    AUTHOR = "author"
    CONTENT = "content"
    ACTIONS = "actions"
    BRANCH = "branch"


@dataclass(frozen=True, slots=True)
class SetDefinition:
    """Metadata for one Aerospike set (suffix under ``Schema.set_prefix``)."""

    suffix: StorageSet
    full_name: str
    primary_key_shape: str
    purpose: str


@dataclass(frozen=True, slots=True)
class BinDefinition:
    """Metadata for one Aerospike bin."""

    name: BinName
    full_name: str
    value_type: str
    sets: frozenset[StorageSet]
    record_kinds: frozenset[RecordKind]
    notes: str = ""


@dataclass(frozen=True, slots=True)
class EventFieldDefinition:
    """Metadata for one key in the inline event Map."""

    name: EventFieldName
    full_name: str
    value_type: str
    adk_field: str
    notes: str = ""


SET_REGISTRY: Final[dict[StorageSet, SetDefinition]] = {
    StorageSet.SESSIONS: SetDefinition(
        suffix=StorageSet.SESSIONS,
        full_name="sessions",
        primary_key_shape="app:user:session  OR  app:user:session:c:NNNNNNNN",
        purpose=(
            "Session records (mutable hot tail) and immutable chunk records "
            "for sealed event batches"
        ),
    ),
    StorageSet.APP_STATE: SetDefinition(
        suffix=StorageSet.APP_STATE,
        full_name="application state",
        primary_key_shape="app",
        purpose="Shared state for all users of an application (keys prefixed app: in ADK)",
    ),
    StorageSet.USER_STATE: SetDefinition(
        suffix=StorageSet.USER_STATE,
        full_name="user state",
        primary_key_shape="app:user",
        purpose="Per-user state surviving across sessions (keys prefixed user: in ADK)",
    ),
    StorageSet.ARTIFACTS: SetDefinition(
        suffix=StorageSet.ARTIFACTS,
        full_name="artifacts",
        primary_key_shape="app:user:session:filename:version:08d",
        purpose="Versioned binary artifacts (inline bytes or future object-store refs)",
    ),
    StorageSet.MEMORY: SetDefinition(
        suffix=StorageSet.MEMORY,
        full_name="memory",
        primary_key_shape="app:user:session:event_id",
        purpose="One lexical memory entry per text-bearing event",
    ),
}


def _bin(
    name: BinName,
    full_name: str,
    value_type: str,
    sets: frozenset[StorageSet],
    record_kinds: frozenset[RecordKind],
    notes: str = "",
) -> BinDefinition:
    return BinDefinition(
        name=name,
        full_name=full_name,
        value_type=value_type,
        sets=sets,
        record_kinds=record_kinds,
        notes=notes,
    )


_ALL_SESSION = frozenset({StorageSet.SESSIONS})
_ARTIFACT = frozenset({StorageSet.ARTIFACTS})
_MEMORY = frozenset({StorageSet.MEMORY})
_APP_USER_STATE = frozenset({StorageSet.APP_STATE, StorageSet.USER_STATE})

BIN_REGISTRY: Final[dict[BinName, BinDefinition]] = {
    BinName.APP_NAME: _bin(
        BinName.APP_NAME,
        "application name",
        "string",
        _ALL_SESSION | _ARTIFACT | _MEMORY,
        frozenset(
            {
                RecordKind.SESSION,
                RecordKind.ARTIFACT,
                RecordKind.MEMORY,
            }
        ),
        "Denormalised for secondary-index queries and operator inspection",
    ),
    BinName.USER_ID: _bin(
        BinName.USER_ID,
        "user identifier",
        "string",
        _ALL_SESSION | _ARTIFACT | _MEMORY,
        frozenset(
            {
                RecordKind.SESSION,
                RecordKind.ARTIFACT,
                RecordKind.MEMORY,
            }
        ),
    ),
    BinName.SESSION_ID: _bin(
        BinName.SESSION_ID,
        "session identifier",
        "string",
        _ALL_SESSION | _ARTIFACT | _MEMORY,
        frozenset(
            {
                RecordKind.SESSION,
                RecordKind.ARTIFACT,
                RecordKind.MEMORY,
            }
        ),
        'For user-scoped artifacts the value is the sentinel "user" (see keys.USER_SCOPE_SID)',
    ),
    BinName.SCOPE_TUPLE: _bin(
        BinName.SCOPE_TUPLE,
        "application user scope composite",
        "string",
        _ARTIFACT | _MEMORY,
        frozenset({RecordKind.ARTIFACT, RecordKind.MEMORY}),
        'Wire value is "app:user:scope" from keys.scope_tuple(); sec-indexed for tenant-local queries',
    ),
    BinName.EVENT_SEQ: _bin(
        BinName.EVENT_SEQ,
        "event sequence counter",
        "int",
        _ALL_SESSION,
        frozenset({RecordKind.SESSION}),
        "Monotonic total events ever appended; incremented atomically on append_event",
    ),
    BinName.STATE: _bin(
        BinName.STATE,
        "state map",
        "Map",
        _ALL_SESSION | _APP_USER_STATE,
        frozenset(
            {
                RecordKind.SESSION,
                RecordKind.APP_STATE_ROW,
                RecordKind.USER_STATE_ROW,
            }
        ),
        "Session-scoped keys only on session records; app/user rows hold their scope",
    ),
    BinName.TIMESTAMP: _bin(
        BinName.TIMESTAMP,
        "timestamp",
        "float",
        _ALL_SESSION | _MEMORY,
        frozenset({RecordKind.SESSION, RecordKind.MEMORY}),
        "Epoch seconds; session record uses this as last_update_time",
    ),
    BinName.AUTHOR: _bin(
        BinName.AUTHOR,
        "event author",
        "string",
        _MEMORY,
        frozenset({RecordKind.MEMORY}),
        'Agent name or "user"',
    ),
    BinName.CONTENT: _bin(
        BinName.CONTENT,
        "event content",
        "Map",
        _MEMORY,
        frozenset({RecordKind.MEMORY}),
        "genai_types.Content projected via Pydantic model_dump(mode=json)",
    ),
    BinName.ACTIONS: _bin(
        BinName.ACTIONS,
        "event actions",
        "Map",
        frozenset(),
        frozenset({RecordKind.INLINE_EVENT}),
        "EventActions projected via Pydantic; only inside inline event Maps",
    ),
    BinName.BRANCH: _bin(
        BinName.BRANCH,
        "branch label",
        "string",
        frozenset(),
        frozenset({RecordKind.INLINE_EVENT}),
        "Optional; only inside inline event Maps",
    ),
    BinName.FILENAME: _bin(
        BinName.FILENAME,
        "artifact filename",
        "string",
        _ARTIFACT,
        frozenset({RecordKind.ARTIFACT}),
        "May contain ':' (e.g. user:profile.json)",
    ),
    BinName.VERSION: _bin(
        BinName.VERSION,
        "artifact version number",
        "int",
        _ARTIFACT,
        frozenset({RecordKind.ARTIFACT}),
        "Also encoded in the primary key suffix as zero-padded decimal",
    ),
    BinName.MIME_TYPE: _bin(
        BinName.MIME_TYPE,
        "MIME type",
        "string",
        _ARTIFACT,
        frozenset({RecordKind.ARTIFACT}),
    ),
    BinName.DATA: _bin(
        BinName.DATA,
        "artifact payload",
        "bytes",
        _ARTIFACT,
        frozenset({RecordKind.ARTIFACT}),
        "Inline bytes; future hybrid storage may store an object URI string here",
    ),
    BinName.CREATE_TIME: _bin(
        BinName.CREATE_TIME,
        "creation time",
        "float",
        _ARTIFACT,
        frozenset({RecordKind.ARTIFACT}),
        "Epoch seconds when the version was saved",
    ),
    BinName.CUSTOM_META: _bin(
        BinName.CUSTOM_META,
        "custom metadata",
        "Map",
        _ARTIFACT,
        frozenset({RecordKind.ARTIFACT}),
    ),
    BinName.EVENT_ID: _bin(
        BinName.EVENT_ID,
        "event identifier",
        "string",
        _MEMORY,
        frozenset({RecordKind.MEMORY}),
        "Same id as Event.id / inline map key eid",
    ),
    BinName.TEXT: _bin(
        BinName.TEXT,
        "extracted plain text",
        "string",
        _MEMORY,
        frozenset({RecordKind.MEMORY}),
        "Concatenated text Parts; used for display and keyword tokenization input",
    ),
    BinName.KEYWORDS: _bin(
        BinName.KEYWORDS,
        "search keywords",
        "list[str]",
        _MEMORY,
        frozenset({RecordKind.MEMORY}),
        "Lowercase [A-Za-z]+ tokens; also drives posting-list maintenance on write",
    ),
    BinName.MEM_POSTINGS: _bin(
        BinName.MEM_POSTINGS,
        "memory posting list",
        "list[map]",
        _MEMORY,
        frozenset({RecordKind.MEMORY}),
        "Inverted index: list of {eid,sid,ts} refs on keys app:user:kw:token",
    ),
    BinName.SESSION_MANIFEST: _bin(
        BinName.SESSION_MANIFEST,
        "session id manifest",
        "list[str]",
        _ALL_SESSION,
        frozenset({RecordKind.SESSION}),
        "Session ids for (app,user) on keys app:user:sl; list_sessions hot path",
    ),
    BinName.EVENTS: _bin(
        BinName.EVENTS,
        "events list",
        "List",
        _ALL_SESSION,
        frozenset({RecordKind.SESSION, RecordKind.CHUNK}),
        "Each element is a Map (inline event shape); hot tail on session, sealed on chunk",
    ),
    BinName.CHUNKS: _bin(
        BinName.CHUNKS,
        "sealed chunk count",
        "int",
        _ALL_SESSION,
        frozenset({RecordKind.SESSION}),
        "Number of valid chunk records; equals the next chunk index to write",
    ),
    BinName.TAIL_BYTES: _bin(
        BinName.TAIL_BYTES,
        "tail byte estimate",
        "int",
        _ALL_SESSION,
        frozenset({RecordKind.SESSION}),
        "Estimated hot-tail size; flush when >= flush_threshold_bytes",
    ),
    BinName.CHUNK_IDX: _bin(
        BinName.CHUNK_IDX,
        "chunk index",
        "int",
        _ALL_SESSION,
        frozenset({RecordKind.CHUNK}),
        "Discriminator: chunk records have cidx; session records do not",
    ),
    BinName.TS_LO: _bin(
        BinName.TS_LO,
        "chunk first-event timestamp",
        "float",
        _ALL_SESSION,
        frozenset({RecordKind.CHUNK}),
        "Epoch seconds; used for after_timestamp pruning",
    ),
    BinName.TS_HI: _bin(
        BinName.TS_HI,
        "chunk last-event timestamp",
        "float",
        _ALL_SESSION,
        frozenset({RecordKind.CHUNK}),
        "Epoch seconds; used to skip whole chunks in after_timestamp reads",
    ),
}

EVENT_FIELD_REGISTRY: Final[dict[EventFieldName, EventFieldDefinition]] = {
    EventFieldName.SCHEMA_VERSION: EventFieldDefinition(
        name=EventFieldName.SCHEMA_VERSION,
        full_name="event schema version",
        value_type="int",
        adk_field="(storage-only)",
        notes=f"Current value {EVENT_SCHEMA_VERSION}; see codec.EVENT_SCHEMA_VERSION",
    ),
    EventFieldName.EVENT_ID: EventFieldDefinition(
        name=EventFieldName.EVENT_ID,
        full_name="event identifier",
        value_type="string",
        adk_field="Event.id",
    ),
    EventFieldName.TIMESTAMP: EventFieldDefinition(
        name=EventFieldName.TIMESTAMP,
        full_name="event timestamp",
        value_type="float",
        adk_field="Event.timestamp",
        notes="Epoch seconds",
    ),
    EventFieldName.AUTHOR: EventFieldDefinition(
        name=EventFieldName.AUTHOR,
        full_name="event author",
        value_type="string",
        adk_field="Event.author",
    ),
    EventFieldName.CONTENT: EventFieldDefinition(
        name=EventFieldName.CONTENT,
        full_name="event content",
        value_type="Map",
        adk_field="Event.content",
        notes="genai_types.Content as JSON-compatible dict",
    ),
    EventFieldName.ACTIONS: EventFieldDefinition(
        name=EventFieldName.ACTIONS,
        full_name="event actions",
        value_type="Map",
        adk_field="Event.actions",
        notes="EventActions as JSON-compatible dict",
    ),
    EventFieldName.BRANCH: EventFieldDefinition(
        name=EventFieldName.BRANCH,
        full_name="branch label",
        value_type="string",
        adk_field="Event.branch",
        notes="Optional",
    ),
}

@dataclass(frozen=True, slots=True)
class Schema:
    """Resolved set/index names for a given installation."""

    namespace: str
    set_prefix: str = DEFAULT_SET_PREFIX

    @property
    def sessions_set(self) -> str:
        return f"{self.set_prefix}{StorageSet.SESSIONS}"

    @property
    def app_state_set(self) -> str:
        return f"{self.set_prefix}{StorageSet.APP_STATE}"

    @property
    def user_state_set(self) -> str:
        return f"{self.set_prefix}{StorageSet.USER_STATE}"

    @property
    def artifacts_set(self) -> str:
        return f"{self.set_prefix}{StorageSet.ARTIFACTS}"

    @property
    def memory_set(self) -> str:
        return f"{self.set_prefix}{StorageSet.MEMORY}"


class Bins:
    """Bin wire names — aliases of :class:`BinName` for existing call sites.

    Prefer :class:`BinName` in new code; both are :class:`str` at runtime.
    """

    APP_NAME: Final = BinName.APP_NAME
    USER_ID: Final = BinName.USER_ID
    SESSION_ID: Final = BinName.SESSION_ID
    SCOPE_TUPLE: Final = BinName.SCOPE_TUPLE
    EVENT_SEQ: Final = BinName.EVENT_SEQ
    STATE: Final = BinName.STATE
    TIMESTAMP: Final = BinName.TIMESTAMP
    LAST_UPDATE: Final = BinName.TIMESTAMP
    AUTHOR: Final = BinName.AUTHOR
    CONTENT: Final = BinName.CONTENT
    ACTIONS: Final = BinName.ACTIONS
    BRANCH: Final = BinName.BRANCH
    FILENAME: Final = BinName.FILENAME
    VERSION: Final = BinName.VERSION
    MIME_TYPE: Final = BinName.MIME_TYPE
    DATA: Final = BinName.DATA
    CREATE_TIME: Final = BinName.CREATE_TIME
    CUSTOM_META: Final = BinName.CUSTOM_META
    EVENT_ID: Final = BinName.EVENT_ID
    TEXT: Final = BinName.TEXT
    KEYWORDS: Final = BinName.KEYWORDS
    MEM_POSTINGS: Final = BinName.MEM_POSTINGS
    SESSION_MANIFEST: Final = BinName.SESSION_MANIFEST
    EVENTS: Final = BinName.EVENTS
    CHUNKS: Final = BinName.CHUNKS
    TAIL_BYTES: Final = BinName.TAIL_BYTES
    CHUNK_IDX: Final = BinName.CHUNK_IDX
    TS_LO: Final = BinName.TS_LO
    TS_HI: Final = BinName.TS_HI


class StateScope:
    """State key prefixes — see ``google.adk.sessions.State`` for canonical semantics.

    Unprefixed keys are session-scoped; prefixed keys are routed to other sets.
    """

    APP: Final = "app:"
    USER: Final = "user:"
    TEMP: Final = "temp:"


def _validate_registries() -> None:
    if set(BIN_REGISTRY) != set(BinName):
        missing = set(BinName) - set(BIN_REGISTRY)
        extra = set(BIN_REGISTRY) - set(BinName)
        raise RuntimeError(f"BIN_REGISTRY out of sync: missing={missing!r} extra={extra!r}")
    if set(EVENT_FIELD_REGISTRY) != set(EventFieldName):
        missing = set(EventFieldName) - set(EVENT_FIELD_REGISTRY)
        extra = set(EVENT_FIELD_REGISTRY) - set(EventFieldName)
        raise RuntimeError(
            f"EVENT_FIELD_REGISTRY out of sync: missing={missing!r} extra={extra!r}"
        )
    if set(SET_REGISTRY) != set(StorageSet):
        raise RuntimeError("SET_REGISTRY must have exactly one entry per StorageSet")


_validate_registries()
