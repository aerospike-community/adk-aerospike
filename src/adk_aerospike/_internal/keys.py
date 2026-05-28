"""Aerospike primary-key construction.

Keys are deterministic strings so we can compute them from ``(app, user, session)``
without a lookup. Aerospike hashes keys into RIPEMD-160 digests internally; the
string form is what we pass to the client.

Format choices
--------------
- Separator is ``":"`` — human-readable in ``aql`` and ``aerolab`` CLI tools.
  ADK identifiers (``app_name``, ``user_id``, ``session_id``) don't contain
  ``:`` in practice. Filenames CAN contain ``:`` (canonical example: the
  ``user:`` prefix routes artifacts to a user-scoped slot — see
  ``artifact_scope_id``). Parsing back to fields is therefore by field count,
  not by splitting on ``:`` — but Aerospike itself hashes the whole string to
  a digest, so this only matters for human inspection of keys.
- Session record:  ``app : user : session``
- Chunk record:    ``app : user : session : c:NNNNNNNN``  (chunk-index
  suffix uses ``c:`` to keep it visually distinct from a session id)
- App-state:       ``app``
- User-state:      ``app : user``
- Artifact:        ``app : user : session : filename : NNNNNNNN``
- Memory:          ``app : user : session : event_id``
"""

from __future__ import annotations

from typing import Final

SEP: Final = ":"
USER_SCOPE_SID: Final = "user"
"""Sentinel session-id slot for user-scoped artifacts.

Matches ``InMemoryArtifactService``'s path scheme (``{app}/{user}/user/{filename}``).
A session literally named ``"user"`` would collide — same constraint upstream
has.
"""

ARTIFACT_HEAD_SUFFIX: Final = "__head__"
"""Suffix on artifact version-counter records (not a stored artifact version)."""

MEMORY_KW_PREFIX: Final = "kw"
"""Infix in memory posting-list keys: ``app:user:kw:token`` (not a memory row)."""

SESSION_MANIFEST_SUFFIX: Final = "sl"
"""Suffix for per-user session-id manifest: ``app:user:sl`` (not a session row)."""

CHUNK_KEY_PREFIX: Final = "c:"
"""Prefix on the chunk-id suffix of chunk record keys, e.g. ``c:00000003``.

Distinguishes a chunk record from a session record sharing the (app, user,
session) triple. Session keys have three SEP-delimited fields; chunk keys
have four, and the fourth always begins with ``c:``.
"""


def session_key(app_name: str, user_id: str, session_id: str) -> str:
    return f"{app_name}{SEP}{user_id}{SEP}{session_id}"


def chunk_key(
    app_name: str, user_id: str, session_id: str, chunk_idx: int
) -> str:
    return (
        f"{app_name}{SEP}{user_id}{SEP}{session_id}"
        f"{SEP}{CHUNK_KEY_PREFIX}{chunk_idx:08d}"
    )


def app_state_key(app_name: str) -> str:
    return app_name


def user_state_key(app_name: str, user_id: str) -> str:
    return f"{app_name}{SEP}{user_id}"


def artifact_key(
    app_name: str,
    user_id: str,
    session_id: str,
    filename: str,
    version: int,
) -> str:
    return (
        f"{app_name}{SEP}{user_id}{SEP}{session_id}{SEP}{filename}{SEP}{version:08d}"
    )


def artifact_head_key(
    app_name: str,
    user_id: str,
    session_id: str,
    filename: str,
) -> str:
    """Primary key for the per-file version counter (``ver`` bin, atomically incremented)."""
    return (
        f"{app_name}{SEP}{user_id}{SEP}{session_id}{SEP}{filename}{SEP}{ARTIFACT_HEAD_SUFFIX}"
    )


def artifact_scope_id(filename: str, session_id: str | None) -> str:
    """Return the session-id slot to use for an artifact key.

    ``"user:"``-prefixed filenames are user-scoped and ignore ``session_id``;
    everything else requires a real ``session_id``.
    """
    if filename.startswith("user:"):
        return USER_SCOPE_SID
    if session_id is None:
        raise ValueError("session_id is required for non-user-scoped artifacts")
    return session_id


def memory_key(app_name: str, user_id: str, session_id: str, event_id: str) -> str:
    return f"{app_name}{SEP}{user_id}{SEP}{session_id}{SEP}{event_id}"


def session_manifest_key(app_name: str, user_id: str) -> str:
    """Primary key for the session-id list for ``(app_name, user_id)``."""
    return f"{app_name}{SEP}{user_id}{SEP}{SESSION_MANIFEST_SUFFIX}"


def memory_posting_key(app_name: str, user_id: str, token: str) -> str:
    """Inverted-index row: all memory event refs for ``(app, user, token)``."""
    return f"{app_name}{SEP}{user_id}{SEP}{MEMORY_KW_PREFIX}{SEP}{token}"


def scope_tuple(app_name: str, user_id: str, scope_id: str) -> str:
    """Composite ``app:user:scope`` value for the denormalised ``aus`` bin.

    Stored as a string so a single secondary-index lookup returns *only* rows
    in this tenant slot — avoids the "fetch by filename then filter
    (app, user) in Python" pattern, which scans every other tenant's rows
    sharing that filename.

    ``scope_id`` is the session id for session-scoped artifacts/memories, the
    ``USER_SCOPE_SID`` sentinel for user-scoped artifacts, or just the
    session id for memories (which are always session-scoped).
    """
    return f"{app_name}{SEP}{user_id}{SEP}{scope_id}"
