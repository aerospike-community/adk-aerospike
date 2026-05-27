#!/usr/bin/env bash
# Stop the Aerospike CE container started by scripts/start_aerospike_ce.sh.

set -euo pipefail

CONTAINER_NAME="${AEROSPIKE_CONTAINER_NAME:-adk-aerospike-ci}"

if docker rm -f "$CONTAINER_NAME" 2>/dev/null; then
  echo "Stopped Aerospike CE container ${CONTAINER_NAME}."
else
  echo "No Aerospike CE container named ${CONTAINER_NAME}."
fi
