#!/usr/bin/env bash
# Start Aerospike Community Edition in Docker for integration tests / CI.
#
# Writes connection settings to .aerospike-ci.env (source before pytest).
# Uses the same access-address workaround as tests/conftest.py — see
# tests/aerospike_ce.conf.template.
#
# Usage:
#   ./scripts/start_aerospike_ce.sh
#   set -a && source .aerospike-ci.env && set +a && pytest -m aerospike

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

IMAGE="${AEROSPIKE_IMAGE:-aerospike/aerospike-server:latest}"
CONTAINER_NAME="${AEROSPIKE_CONTAINER_NAME:-adk-aerospike-ci}"
NAMESPACE="${AEROSPIKE_TEST_NAMESPACE:-test}"
ENV_FILE="${AEROSPIKE_CI_ENV_FILE:-.aerospike-ci.env}"
TEMPLATE="${ROOT}/tests/aerospike_ce.conf.template"

if [[ ! -f "$TEMPLATE" ]]; then
  echo "missing config template: $TEMPLATE" >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to start Aerospike CE" >&2
  exit 1
fi

HOST_PORT="${AEROSPIKE_TEST_PORT:-}"
if [[ -z "$HOST_PORT" ]]; then
  HOST_PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()')"
fi

CONF_DIR="$(mktemp -d)"
trap 'rm -rf "$CONF_DIR"' EXIT
sed "s/__ACCESS_PORT__/${HOST_PORT}/g" "$TEMPLATE" >"${CONF_DIR}/aerospike.conf"

echo "Starting Aerospike CE (${IMAGE}) as container ${CONTAINER_NAME} on 127.0.0.1:${HOST_PORT} ..."
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

docker run -d \
  --name "$CONTAINER_NAME" \
  -p "${HOST_PORT}:3000" \
  -v "${CONF_DIR}/aerospike.conf:/etc/aerospike/aerospike.template.conf:ro" \
  "$IMAGE"

echo "Waiting for Aerospike CE process (asinfo inside container, up to 90s) ..."
deadline=$((SECONDS + 90))
until docker exec "$CONTAINER_NAME" asinfo -v status 2>/dev/null | grep -qE 'ok|normal'; do
  if (( SECONDS >= deadline )); then
    echo "Aerospike CE did not become ready in time. Container logs:" >&2
    docker logs "$CONTAINER_NAME" 2>&1 | tail -80 >&2 || true
    exit 1
  fi
  sleep 1
done

# asinfo succeeding inside the container does not guarantee the mapped host port
# accepts client connections yet (cluster tend / access-address propagation).
# Mirror tests/conftest.py: probe from the host with the official Python client.
echo "Waiting for host-side client connections on 127.0.0.1:${HOST_PORT} (up to 90s) ..."
host_deadline=$((SECONDS + 90))
until python3 -c "
import aerospike
c = aerospike.client({'hosts': [('127.0.0.1', ${HOST_PORT})]})
c.connect()
c.close()
" 2>/dev/null; do
  if (( SECONDS >= host_deadline )); then
    echo "Aerospike CE did not accept host-side client connections in time." >&2
    echo "Check access-address/access-port in ${TEMPLATE} and port mapping ${HOST_PORT}:3000." >&2
    docker logs "$CONTAINER_NAME" 2>&1 | tail -80 >&2 || true
    exit 1
  fi
  sleep 1
done

cat >"$ENV_FILE" <<EOF
AEROSPIKE_TEST_HOST=127.0.0.1
AEROSPIKE_TEST_PORT=${HOST_PORT}
AEROSPIKE_TEST_NAMESPACE=${NAMESPACE}
EOF

echo "Aerospike CE is ready."
echo "  URI: aerospike://127.0.0.1:${HOST_PORT}/${NAMESPACE}"
echo "  Env: ${ENV_FILE} (source before pytest)"
