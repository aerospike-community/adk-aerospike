"""Aerospike client construction and connection lifecycle.

Best-practice notes
-------------------

**One client per application.** The ``aerospike`` Python client maintains an
internal connection pool to every node in the cluster. Construction is
expensive (DNS, TLS handshake, cluster tend); a single long-lived client should
be shared across the whole process. Services here accept an injected client so
multiple services (sessions + artifacts) can share one.

**Thread-safe, fork-unsafe.** The client is safe to share across asyncio tasks
and threads. It is *not* safe across ``os.fork()``: child processes must build
their own client (or call ``multiprocessing`` with the "spawn" start method).

**Synchronous client, async bridge.** The official ``aerospike`` package is
synchronous. ADK's interfaces are ``async``. Services wrap each call in
``asyncio.to_thread`` to keep the event loop responsive. The default executor
(a thread pool sized to ``min(32, os.cpu_count() + 4)``) is fine for moderate
concurrency; very high-throughput apps should configure a larger executor.

**Policy defaults.** We set conservative, predictable defaults:

- ``key = POLICY_KEY_SEND`` — store the actual key string alongside the
  record so secondary-index queries can return the key without a second
  lookup. Costs a few bytes per record; worth it.
- ``commit_level = COMMIT_ALL`` (writes) — wait for all replicas to ack
  before returning. Trades a small amount of latency for durability.
- ``max_retries = 0`` on writes — writes are not idempotent by default;
  retrying risks duplicates. Use idempotent operations (Map CDT, generation
  checks) when retries are needed.
- ``max_retries = 2`` on reads/queries — safe to retry, low cost.
- ``total_timeout = 1000ms`` — fail fast under load. Tune per workload.

Override any of these via ``policies={...}`` on the factory.

**TLS** is enabled by adding ``?tls=true`` to the URI (or passing
``tls_config`` to the factory directly). The default config trusts the system
CA store; pass ``cafile`` / ``cert`` / ``keyfile`` for mTLS.

**Auth modes**. Aerospike supports several:

- ``INTERNAL`` (default) — username/password against the cluster's user store
- ``EXTERNAL`` — external auth (LDAP/PKI) over TLS
- ``EXTERNAL_INSECURE`` — external auth over plaintext (test only)
- ``PKI`` — client certificate auth, no password

Pass ``auth_mode`` to the factory to override the default.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from .uri import AerospikeUri

if TYPE_CHECKING:
    import aerospike

log = logging.getLogger(__name__)


def _default_policies() -> dict[str, Any]:
    """Build the default policy map.

    Inlined as a function (not a module constant) because the policy constants
    live on the ``aerospike`` module, which we import lazily to keep this
    module importable without the C extension installed.
    """
    import aerospike

    return {
        "read": {
            "total_timeout": 1000,
            "max_retries": 2,
            "key": aerospike.POLICY_KEY_SEND,
        },
        "write": {
            "total_timeout": 1000,
            "max_retries": 0,
            "key": aerospike.POLICY_KEY_SEND,
            "commit_level": aerospike.POLICY_COMMIT_LEVEL_ALL,
        },
        "operate": {
            "total_timeout": 1000,
            "max_retries": 0,
            "key": aerospike.POLICY_KEY_SEND,
            "commit_level": aerospike.POLICY_COMMIT_LEVEL_ALL,
        },
        "remove": {
            "total_timeout": 1000,
            "max_retries": 0,
            "key": aerospike.POLICY_KEY_SEND,
            "commit_level": aerospike.POLICY_COMMIT_LEVEL_ALL,
        },
        "query": {
            "total_timeout": 10_000,
            "max_retries": 2,
        },
        "batch": {
            "total_timeout": 1000,
            "max_retries": 2,
        },
    }


def make_client(
    uri: AerospikeUri,
    *,
    policies: dict[str, Any] | None = None,
    tls_config: dict[str, Any] | None = None,
    auth_mode: int | None = None,
    extra_config: dict[str, Any] | None = None,
) -> aerospike.Client:
    """Build and connect a sync Aerospike client.

    Parameters
    ----------
    uri
        Parsed connection URI. ``hosts``, ``namespace``, credentials, and
        ``tls`` flag are sourced from here.
    policies
        Optional override map; merged on top of :func:`_default_policies` so
        callers can tweak a single timeout without re-specifying everything.
    tls_config
        Full TLS configuration dict (passed through to the client as-is). If
        the URI sets ``tls=true`` but this is ``None``, a minimal
        ``{"enable": True}`` is supplied — fine for system-CA verification,
        insufficient for mTLS.
    auth_mode
        One of ``aerospike.AUTH_INTERNAL`` / ``AUTH_EXTERNAL`` / ``AUTH_PKI`` /
        ``AUTH_EXTERNAL_INSECURE``. Defaults to ``INTERNAL``.
    extra_config
        Free-form passthrough merged into the top-level config dict. Use for
        ``cluster_name``, ``tend_interval``, ``max_conns_per_node``, etc.

    Returns
    -------
    A connected ``aerospike.Client``. Caller owns its lifecycle and must call
    ``client.close()`` on shutdown.

    Raises
    ------
    aerospike.exception.ClientError
        On invalid configuration.
    aerospike.exception.ConnectionError
        If no cluster node is reachable.
    """
    import aerospike

    config: dict[str, Any] = {
        "hosts": list(uri.hosts),
        "policies": _merge_policies(_default_policies(), policies or {}),
    }

    if uri.tls or tls_config:
        config["tls"] = tls_config or {"enable": True}

    if auth_mode is not None:
        config["auth_mode"] = auth_mode

    if extra_config:
        config.update(extra_config)

    log.info(
        "Connecting to Aerospike: hosts=%s namespace=%s tls=%s",
        uri.hosts,
        uri.namespace,
        bool(config.get("tls")),
    )
    client = aerospike.client(config)

    if uri.username and uri.password:
        client.connect(uri.username, uri.password)
    else:
        client.connect()

    return client


def _merge_policies(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep-merge policy override map onto the base, per-policy-class."""
    merged = {k: dict(v) for k, v in base.items()}
    for key, overrides in override.items():
        merged.setdefault(key, {}).update(overrides)
    return merged


def close_client(client: aerospike.Client) -> None:
    """Idempotent client shutdown.

    The underlying ``client.close()`` is safe to call multiple times, but
    surfacing this helper keeps service ``close()`` methods symmetric and
    one-line.
    """
    try:
        client.close()
    except Exception:  # pragma: no cover - defensive
        log.exception("Error closing Aerospike client")
