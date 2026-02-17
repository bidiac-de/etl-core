#!/usr/bin/env bash
set -euo pipefail

NETWORK="${NETWORK:-etl-demo}"
POSTGRES_NAME="${POSTGRES_NAME:-demo-postgres}"
MARIADB_NAME="${MARIADB_NAME:-demo-mariadb}"
MONGO_NAME="${MONGO_NAME:-demo-mongo}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker command not found." >&2
  exit 1
fi

docker rm -f "${POSTGRES_NAME}" "${MARIADB_NAME}" "${MONGO_NAME}" >/dev/null 2>&1 || true

if [[ "${1:-}" == "--remove-network" ]]; then
  docker network rm "${NETWORK}" >/dev/null 2>&1 || true
fi

echo "Stopped demo containers."
