"""``aerospike://`` URI parsing for service-registry / CLI integration.

Examples
--------
``aerospike://localhost:3000/adk``
    Single seed node, namespace ``adk``.

``aerospike://user:pass@host1:3000,host2:3000/adk?set_prefix=prod_&tls=true``
    Multi-seed, auth, custom set prefix, TLS.

``aerospike+avs://localhost:5000/adk``
    AVS variant used by the MemoryService factory.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final
from urllib.parse import parse_qs, urlsplit

_DEFAULT_DB_PORT: Final = 3000
_DEFAULT_AVS_PORT: Final = 5000
_VALID_SCHEMES: Final = frozenset({"aerospike", "aerospike+avs"})


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
    """Parse an ``aerospike://`` or ``aerospike+avs://`` URI.

    Raises ``ValueError`` on malformed input.
    """
    parts = urlsplit(uri)
    if parts.scheme not in _VALID_SCHEMES:
        raise ValueError(
            f"unsupported scheme {parts.scheme!r}; expected aerospike:// or aerospike+avs://"
        )

    if not parts.netloc:
        raise ValueError(f"missing host in {uri!r}")

    namespace = parts.path.lstrip("/").split("/", 1)[0]
    if not namespace:
        raise ValueError(f"missing namespace (path component) in {uri!r}")

    default_port = _DEFAULT_AVS_PORT if parts.scheme.endswith("avs") else _DEFAULT_DB_PORT
    # urlsplit only parses the first host; comma-separated multi-host strings
    # arrive intact in parts.netloc after the credentials are stripped.
    hostport_str = parts.netloc.rsplit("@", 1)[-1]
    hosts: list[tuple[str, int]] = []
    for hp in hostport_str.split(","):
        host, _, port_s = hp.partition(":")
        port = int(port_s) if port_s else default_port
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
