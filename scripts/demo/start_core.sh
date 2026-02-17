#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CONDA_BIN="${CORE_ROOT}/.conda/bin"
ENV_FILE="${CORE_ROOT}/.env"
ENV_DEMO_FILE="${CORE_ROOT}/.env_demo"

if [[ ! -f "${ENV_FILE}" ]]; then
  if [[ -f "${ENV_DEMO_FILE}" ]]; then
    cp "${ENV_DEMO_FILE}" "${ENV_FILE}"
    echo "Created ${ENV_FILE} from ${ENV_DEMO_FILE}."
  else
    echo "Missing ${ENV_FILE} and ${ENV_DEMO_FILE}." >&2
    exit 1
  fi
fi

# Enforce demo-safe runtime defaults while still allowing explicit overrides.
# We use braces to strictly delimit variable names and ensure robust handling.
export DB_PATH="${DB_PATH:-${CORE_ROOT}/data/etl-core.db}"
export LOG_DIR="${LOG_DIR:-${CORE_ROOT}/logs}"
export ETL_COMPONENT_MODE="${ETL_COMPONENT_MODE:-production}"
export EXECUTION_ENV="${EXECUTION_ENV:-DEV}"

# Read secrets config from env file if not set, handling potential spacing issues if sourced
# But since we use python-dotenv in the app, we should rely on that or export clearly here.
# We will just default them if not set.
export SECRET_BACKEND="${SECRET_BACKEND:-keyring}"
export SECRET_SERVICE="${SECRET_SERVICE:-etl-core}"
CORE_RELOAD="${CORE_RELOAD:-1}"

if [[ "${SECRET_BACKEND}" == "memory" ]]; then
  echo "Warning: SECRET_BACKEND=memory keeps secrets only in process memory."
  echo "For reliable demo credentials across restarts use SECRET_BACKEND=keyring."
fi

mkdir -p "${CORE_ROOT}/data" "${CORE_ROOT}/logs" "$(dirname "${DB_PATH}")"

if [[ ! -x "${CONDA_BIN}/uvicorn" ]]; then
  echo "Missing ${CONDA_BIN}/uvicorn. Install dependencies in .conda first." >&2
  exit 1
fi

echo "Starting ETL Core at http://127.0.0.1:8000"
echo "OpenAPI: http://127.0.0.1:8000/docs"

cd "${CORE_ROOT}"
if [[ "${CORE_RELOAD}" == "1" ]]; then
  PYTHONPATH=src "${CONDA_BIN}/uvicorn" etl_core.main:app --host 127.0.0.1 --port 8000 --reload
else
  PYTHONPATH=src "${CONDA_BIN}/uvicorn" etl_core.main:app --host 127.0.0.1 --port 8000
fi
