"""Test fixtures.

The Aerospike-backed tests are gated behind the ``aerospike`` marker so unit
tests on foundation modules (URI parsing, key construction) can run without
a running server.

Integration tests need Aerospike Community Edition:

- **CI / explicit setup:** ``scripts/start_aerospike_ce.sh`` starts
  ``aerospike/aerospike-server:latest`` in Docker and writes
  ``.aerospike-ci.env``. Set ``AEROSPIKE_TEST_HOST`` / ``AEROSPIKE_TEST_PORT``
  (or source that file) before ``pytest``.
- **Local default:** if those variables are unset, ``testcontainers`` starts
  the same image with the same config template (``tests/aerospike_ce.conf.template``).

Networking note
---------------
Aerospike's cluster tend protocol reports the server's ``access-address`` /
``access-port`` to clients, who then reconnect using those values. In a
Docker setup, the container's internal IP is not reachable from the host, so
we mount a custom config that pins ``access-address`` to ``127.0.0.1`` and
``access-port`` to the host-side port we bound. This is the canonical
workaround for Aerospike-in-Docker on macOS / WSL / GitHub Actions.
"""

from __future__ import annotations

import os
import socket
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

_CONF_TEMPLATE = Path(__file__).with_name("aerospike_ce.conf.template")


def _find_free_port() -> int:
    """Bind to an ephemeral port, immediately release it, and return the number."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _render_aerospike_conf(access_port: int) -> str:
    return _CONF_TEMPLATE.read_text().replace("__ACCESS_PORT__", str(access_port))


def _connection_from_env() -> dict[str, object] | None:
    host = os.environ.get("AEROSPIKE_TEST_HOST")
    port_s = os.environ.get("AEROSPIKE_TEST_PORT")
    if not host or not port_s:
        return None
    return {
        "host": host,
        "port": int(port_s),
        "namespace": os.environ.get("AEROSPIKE_TEST_NAMESPACE", "test"),
    }


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "aerospike: requires Aerospike CE (env vars or testcontainers)",
    )


@pytest.fixture(scope="session")
def aerospike_container() -> Iterator[dict[str, object]]:
    """Aerospike CE connection settings for the test session.

    Uses ``AEROSPIKE_TEST_*`` when set (CI runs ``scripts/start_aerospike_ce.sh``
    first). Otherwise starts a throwaway container via testcontainers.
    """
    external = _connection_from_env()
    if external is not None:
        yield external
        return

    pytest.importorskip("testcontainers")
    pytest.importorskip("aerospike")
    from testcontainers.core.container import DockerContainer

    import aerospike

    host_port = _find_free_port()
    conf_dir = Path(tempfile.mkdtemp(prefix="aerospike-test-"))
    conf_path = conf_dir / "aerospike.conf"
    conf_path.write_text(_render_aerospike_conf(host_port))

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
