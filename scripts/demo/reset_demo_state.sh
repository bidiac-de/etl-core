#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
STUDIO_ROOT="$(cd "${CORE_ROOT}/../etl-studio" && pwd)"

CONFIRM=0
KEEP_LOGS=0

usage() {
  cat <<'USAGE'
Usage: reset_demo_state.sh --yes [--keep-logs]

Resets local demo state for ETL Core + ETL Studio:
- removes Core SQLite DB (+ -wal/-shm/-journal side files)
- removes Studio SQLite DB (+ side files from config.json)
- removes Core log directory unless --keep-logs is provided
USAGE
}

read_env_value() {
  local file_path="$1"
  local key="$2"
  if [[ ! -f "${file_path}" ]]; then
    return 0
  fi
  awk -F '=' -v wanted="${key}" '
    $1 == wanted {
      sub(/^[[:space:]]+/, "", $2)
      sub(/[[:space:]]+$/, "", $2)
      print $2
      exit
    }
  ' "${file_path}"
}

resolve_value() {
  local env_name="$1"
  local fallback="$2"
  local from_env="${!env_name:-}"
  if [[ -n "${from_env}" ]]; then
    printf '%s\n' "${from_env}"
    return 0
  fi

  local value=""
  value="$(read_env_value "${CORE_ROOT}/.env" "${env_name}")"
  if [[ -n "${value}" ]]; then
    printf '%s\n' "${value}"
    return 0
  fi

  value="$(read_env_value "${CORE_ROOT}/.env_demo" "${env_name}")"
  if [[ -n "${value}" ]]; then
    printf '%s\n' "${value}"
    return 0
  fi

  printf '%s\n' "${fallback}"
}

for arg in "$@"; do
  case "${arg}" in
    --yes)
      CONFIRM=1
      ;;
    --keep-logs)
      KEEP_LOGS=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: ${arg}" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "${CONFIRM}" -ne 1 ]]; then
  echo "Refusing destructive reset without --yes." >&2
  usage >&2
  exit 1
fi

CORE_DB_PATH="$(resolve_value "DB_PATH" "${CORE_ROOT}/data/etl-core.db")"
CORE_LOG_DIR="$(resolve_value "LOG_DIR" "${CORE_ROOT}/logs")"

STUDIO_DB_PATH="$(
  python3 - "${STUDIO_ROOT}/config.json" <<'PY'
import json
import os
import sys

config_path = sys.argv[1]
if not os.path.isfile(config_path):
    print("")
    raise SystemExit(0)
try:
    with open(config_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
except Exception:
    print("")
    raise SystemExit(0)

value = data.get("sqlitefilepath", "")
if isinstance(value, str):
    print(value)
else:
    print("")
PY
)"
if [[ -z "${STUDIO_DB_PATH}" ]]; then
  STUDIO_DB_PATH="${STUDIO_ROOT}/etl.db"
fi

declare -a TARGETS=(
  "${CORE_DB_PATH}"
  "${CORE_DB_PATH}-wal"
  "${CORE_DB_PATH}-shm"
  "${CORE_DB_PATH}-journal"
  "${STUDIO_DB_PATH}"
  "${STUDIO_DB_PATH}-wal"
  "${STUDIO_DB_PATH}-shm"
  "${STUDIO_DB_PATH}-journal"
)

declare -a REMOVED=()
declare -a MISSING=()
declare -a FAILED=()

remove_target() {
  local target="$1"
  if [[ -e "${target}" ]]; then
    if rm -rf "${target}" 2>/dev/null; then
      REMOVED+=("${target}")
    else
      FAILED+=("${target}")
    fi
  else
    MISSING+=("${target}")
  fi
}

for target in "${TARGETS[@]}"; do
  remove_target "${target}"
done

if [[ "${KEEP_LOGS}" -eq 0 ]]; then
  remove_target "${CORE_LOG_DIR}"
else
  MISSING+=("${CORE_LOG_DIR} (kept by --keep-logs)")
fi

printf '\nDemo reset summary\n'
printf 'Core DB path: %s\n' "${CORE_DB_PATH}"
printf 'Studio DB path: %s\n' "${STUDIO_DB_PATH}"

printf '\nRemoved (%d)\n' "${#REMOVED[@]}"
for item in "${REMOVED[@]}"; do
  printf '  - %s\n' "${item}"
done

printf '\nMissing/Skipped (%d)\n' "${#MISSING[@]}"
for item in "${MISSING[@]}"; do
  printf '  - %s\n' "${item}"
done

if [[ "${#FAILED[@]}" -gt 0 ]]; then
  printf '\nFailed (%d)\n' "${#FAILED[@]}" >&2
  for item in "${FAILED[@]}"; do
    printf '  - %s\n' "${item}" >&2
  done
  exit 1
fi

echo
echo "Reset completed."

# ---- Live API cleanup (best-effort, skipped when Core isn't reachable) ----
CORE_URL="${CORE_URL:-http://127.0.0.1:8000}"
if curl -sS -o /dev/null -w '' --max-time 2 "${CORE_URL}/jobs/" 2>/dev/null; then
  echo "Core is reachable — deleting jobs via API…"

  # Delete every job through the REST API so in-memory state is consistent
  JOB_IDS="$(curl -sS --max-time 5 "${CORE_URL}/jobs/" 2>/dev/null \
    | python3 -c "import json,sys; [print(j['id']) for j in json.load(sys.stdin)]" 2>/dev/null)"

  deleted=0
  for jid in ${JOB_IDS}; do
    if curl -sS -X DELETE --max-time 5 "${CORE_URL}/jobs/${jid}" >/dev/null 2>&1; then
      deleted=$((deleted + 1))
    fi
  done
  echo "  Deleted ${deleted} job(s) via API."

  # Flush remaining caches
  curl -sS -X POST --max-time 2 "${CORE_URL}/jobs/cache/invalidate" >/dev/null 2>&1
  echo "  Core job caches invalidated."
else
  echo "(Core not reachable — restart Core to clear in-memory state)"
fi
