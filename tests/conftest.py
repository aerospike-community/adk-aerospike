"""Test fixtures.

The Aerospike-backed tests are gated behind the ``aerospike`` marker so unit
tests on foundation modules (URI parsing, key construction) can run without
a running server.

To run integration tests::

    pip install -e ".[dev]"
    pytest -m aerospike

A throwaway Aerospike Community Edition container is started via
``testcontainers`` and reused across the test session.

Networking note
---------------
Aerospike's cluster tend protocol reports the server's ``access-address`` /
``access-port`` to clients, who then reconnect using those values. In a
Docker setup, the container's internal IP is not reachable from the host, so
we mount a custom ``aerospike.conf`` that pins ``access-address`` to
``127.0.0.1`` and ``access-port`` to the host-side port we bound. This is the
canonical workaround for Aerospike-in-Docker on macOS / WSL.
"""

from __future__ import annotations

import socket
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

import pytest


def _find_free_port() -> int:
    """Bind to an ephemeral port, immediately release it, and return the number."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


_AEROSPIKE_CONF_TEMPLATE = """\
service {{
    proto-fd-max 15000
    cluster-name adk-test
}}

logging {{
    console {{
        context any info
    }}
}}

network {{
    service {{
        address any
        port 3000
        access-address 127.0.0.1
        access-port {access_port}
    }}
    heartbeat {{
        mode mesh
        port 3002
        interval 150
        timeout 10
    }}
    fabric {{
        port 3001
    }}
    info {{
        port 3003
    }}
}}

namespace test {{
    replication-factor 1
    storage-engine memory {{
        data-size 1G
    }}
    nsup-period 60
}}
"""


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "aerospike: requires a running Aerospike server (testcontainers will provide one)",
    )


@pytest.fixture(scope="session")
def aerospike_container() -> Iterator[dict[str, object]]:
    """Start an Aerospike CE container for the test session."""
    pytest.importorskip("testcontainers")
    pytest.importorskip("aerospike")
    from testcontainers.core.container import DockerContainer

    import aerospike

    host_port = _find_free_port()
    conf_dir = Path(tempfile.mkdtemp(prefix="aerospike-test-"))
    conf_path = conf_dir / "aerospike.conf"
    conf_path.write_text(_AEROSPIKE_CONF_TEMPLATE.format(access_port=host_port))

    # NOTE: We mount at ``aerospike.template.conf``, not ``aerospike.conf``.
    # The image's entrypoint does ``bash``-style env-var substitution from the
    # template and writes the result to ``aerospike.conf``. Mounting our file
    # at the destination breaks that (read-only filesystem). Our pre-formatted
    # config has no ``$(...)`` / ``${...}`` placeholders, so substitution is a
    # no-op and the resulting file is byte-identical.
    container = (
        DockerContainer("aerospike/aerospike-server:latest")
        .with_bind_ports(3000, host_port)
        .with_volume_mapping(str(conf_path), "/etc/aerospike/aerospike.template.conf", "ro")
    )
    container.start()

    # Wait for the server to accept client connections. Cluster tend takes a
    # couple of seconds; allow up to 60s for slow CI.
    deadline = time.time() + 60
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            probe = aerospike.client({"hosts": [("127.0.0.1", host_port)]})
            probe.connect()
            probe.close()
            break
        except Exception as exc:  # noqa: BLE001 — surface only on timeout
            last_err = exc
            time.sleep(1)
    else:
        container.stop()
        raise RuntimeError(f"Aerospike container failed to become ready within 60s: {last_err}")

    try:
        yield {"host": "127.0.0.1", "port": host_port, "namespace": "test"}
    finally:
        container.stop()


@pytest.fixture
def aerospike_uri(aerospike_container: dict[str, object]) -> str:
    c = aerospike_container
    return f"aerospike://{c['host']}:{c['port']}/{c['namespace']}"
