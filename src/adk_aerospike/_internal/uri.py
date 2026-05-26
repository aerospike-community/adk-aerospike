"""``aerospike://`` URI parsing for service-registry / CLI integration.

Examples
--------
``aerospike://localhost:3000/adk``
    Single seed node, namespace ``adk``.

``aerospike://user:pass@host1:3000,host2:3000/adk?set_prefix=prod_&tls=true``
    Multi-seed, auth, custom set prefix, TLS.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final
from urllib.parse import parse_qs, urlsplit

_DEFAULT_DB_PORT: Final = 3000
_VALID_SCHEMES: Final = frozenset({"aerospike"})


@dataclass(frozen=True, slots=True)
class AerospikeUri:
    scheme: str
    hosts: tuple[tuple[str, int], ...]
    namespace: str
    username: str | None = None
    password: str | None = None
    set_prefix: str = "adk_"
    tls: bool = False
    extras: dict[str, str] = field(default_factory=dict)


def parse(uri: str) -> AerospikeUri:
    """Parse an ``aerospike://`` URI.

    Raises ``ValueError`` on malformed input.
    """
    parts = urlsplit(uri)
    if parts.scheme not in _VALID_SCHEMES:
        raise ValueError(
            f"unsupported scheme {parts.scheme!r}; expected aerospike://"
        )

    if not parts.netloc:
        raise ValueError(f"missing host in {uri!r}")

    namespace = parts.path.lstrip("/").split("/", 1)[0]
    if not namespace:
        raise ValueError(f"missing namespace (path component) in {uri!r}")

    # urlsplit only parses the first host; comma-separated multi-host strings
    # arrive intact in parts.netloc after the credentials are stripped.
    hostport_str = parts.netloc.rsplit("@", 1)[-1]
    hosts: list[tuple[str, int]] = []
    for hp in hostport_str.split(","):
        host, _, port_s = hp.partition(":")
        port = int(port_s) if port_s else _DEFAULT_DB_PORT
        hosts.append((host, port))

    qs = {k: v[0] for k, v in parse_qs(parts.query).items()}
    set_prefix = qs.pop("set_prefix", "adk_")
    tls = qs.pop("tls", "false").lower() in {"1", "true", "yes"}

    return AerospikeUri(
        scheme=parts.scheme,
        hosts=tuple(hosts),
        namespace=namespace,
        username=parts.username,
        password=parts.password,
        set_prefix=set_prefix,
        tls=tls,
        extras=qs,
    )
