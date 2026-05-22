"""Aerospike primary-key construction.

Keys are deterministic strings so we can compute them from ``(app, user, session)``
without a lookup. Aerospike hashes keys into RIPEMD-160 digests internally; the
string form is what we pass to the client.

Format choices
--------------
- Separator is ``"\x1f"`` (ASCII unit separator) — never appears in valid
  app/user/session/filename inputs, so we don't need escaping.
- Session record:  ``app \x1f user \x1f session``
- Chunk record:    ``app \x1f user \x1f session \x1f c:NNNNNNNN`` (8-digit
  zero-padded; ``c:`` prefix avoids collisions with future sentinels)
- App-state:       ``app``
- User-state:      ``app \x1f user``
- Artifact:        ``app \x1f user \x1f session \x1f filename \x1f version:08d``
- Memory:          ``app \x1f user \x1f session \x1f event_id``
"""

from __future__ import annotations

from typing import Final

SEP: Final = "\x1f"
USER_SCOPE_SID: Final = "user"
"""Sentinel session-id slot for user-scoped artifacts.

Matches ``InMemoryArtifactService``'s path scheme (``{app}/{user}/user/{filename}``).
A session literally named ``"user"`` would collide — same constraint upstream
has.
"""

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
