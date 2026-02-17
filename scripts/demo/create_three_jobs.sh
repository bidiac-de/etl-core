#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORE_URL="${CORE_URL:-http://127.0.0.1:8000}"

# --- Port auto-detection ---
# If the user has not explicitly set port env vars, resolve the actual
# Docker-mapped ports so credentials point to the right place even when
# default ports are occupied by other containers.
_resolve_port() {
  local container="$1" internal_port="$2" fallback="$3"
  local mapped
  mapped="$(docker port "${container}" "${internal_port}/tcp" 2>/dev/null | head -n1 | awk -F: '{print $NF}')" || true
  echo "${mapped:-${fallback}}"
}

if [[ -z "${POSTGRES_PORT:-}" ]]; then
  if [[ -f "${SCRIPT_DIR}/.demo_ports" ]]; then
    source "${SCRIPT_DIR}/.demo_ports"
  else
    POSTGRES_PORT="$(_resolve_port demo-postgres 5432 5432)"
  fi
fi
if [[ -z "${MARIADB_PORT:-}" ]]; then
  if [[ -f "${SCRIPT_DIR}/.demo_ports" ]] && [[ -z "${MARIADB_PORT:-}" ]]; then
    source "${SCRIPT_DIR}/.demo_ports"
  else
    MARIADB_PORT="$(_resolve_port demo-mariadb 3306 3307)"
  fi
fi
if [[ -z "${MONGO_PORT:-}" ]]; then
  if [[ -f "${SCRIPT_DIR}/.demo_ports" ]] && [[ -z "${MONGO_PORT:-}" ]]; then
    source "${SCRIPT_DIR}/.demo_ports"
  else
    MONGO_PORT="$(_resolve_port demo-mongo 27017 27018)"
  fi
fi

POSTGRES_PORT="${POSTGRES_PORT:-5432}"
MARIADB_PORT="${MARIADB_PORT:-3307}"
MONGO_PORT="${MONGO_PORT:-27018}"
EXCEL_INPUT="${EXCEL_INPUT:-/tmp/demo_in.xlsx}"
JSON_OUTPUT="${JSON_OUTPUT:-/tmp/demo_excel_filter_out.json}"
EXECUTE=0
RESET=0
SUMMARY_FILE="${SUMMARY_FILE:-/tmp/etl_demo_job_ids.json}"

for arg in "$@"; do
  case "${arg}" in
    --execute)
      EXECUTE=1
      ;;
    --reset)
      RESET=1
      ;;
    *)
      echo "Unknown option: ${arg}" >&2
      echo "Usage: $0 [--reset] [--execute]" >&2
      exit 1
      ;;
  esac
done

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_cmd curl
require_cmd python3

RESPONSE_STATUS=""
RESPONSE_BODY=""

request_json() {
  local method="$1"
  local endpoint="$2"
  local payload="${3:-}"
  local tmp_file
  tmp_file="$(mktemp)"

  local status="000"
  if [[ -n "${payload}" ]]; then
    status="$(curl -sS -o "${tmp_file}" -w "%{http_code}" -X "${method}" "${CORE_URL}${endpoint}" -H "Content-Type: application/json" --data "${payload}" || true)"
  else
    status="$(curl -sS -o "${tmp_file}" -w "%{http_code}" -X "${method}" "${CORE_URL}${endpoint}" || true)"
  fi

  RESPONSE_STATUS="${status}"
  RESPONSE_BODY="$(cat "${tmp_file}")"
  rm -f "${tmp_file}"
}

ensure_success_status() {
  local label="$1"
  if [[ "${RESPONSE_STATUS}" != 2* ]]; then
    echo "${label} failed (HTTP ${RESPONSE_STATUS})." >&2
    if [[ -n "${RESPONSE_BODY}" ]]; then
      echo "${RESPONSE_BODY}" >&2
    fi
    exit 1
  fi
}

extract_object_id() {
  local raw="$1"
  python3 - "$raw" <<'PY'
import json
import sys

raw = sys.argv[1]
try:
    obj = json.loads(raw)
except Exception:
    print("")
    sys.exit(0)
if isinstance(obj, dict):
    print(obj.get("id", ""))
else:
    print("")
PY
}

extract_job_id() {
  local raw="$1"
  python3 - "$raw" <<'PY'
import json
import sys

raw = sys.argv[1]
try:
    obj = json.loads(raw)
except Exception:
    print("")
    sys.exit(0)
if isinstance(obj, str):
    print(obj)
elif isinstance(obj, dict):
    print(obj.get("id", ""))
else:
    print("")
PY
}

assert_core_ready() {
  request_json "GET" "/setup/capabilities"
  ensure_success_status "Core capability check"

  request_json "GET" "/jobs/"
  ensure_success_status "Core jobs listing preflight"

  python3 - "${RESPONSE_BODY}" <<'PY'
import json
import sys

raw = sys.argv[1]
try:
    parsed = json.loads(raw)
except Exception:
    print("Preflight /jobs/ did not return valid JSON.", file=sys.stderr)
    raise SystemExit(1)

if not isinstance(parsed, list):
    print("Preflight /jobs/ did not return a list.", file=sys.stderr)
    raise SystemExit(1)
PY
}

assert_job_visible() {
  local job_id="$1"
  request_json "GET" "/jobs/"
  ensure_success_status "Post-create /jobs/ verification"

  if ! python3 - "${RESPONSE_BODY}" "${job_id}" <<'PY'
import json
import sys

raw = sys.argv[1]
job_id = sys.argv[2]
try:
    rows = json.loads(raw)
except Exception:
    raise SystemExit(1)

if not isinstance(rows, list):
    raise SystemExit(1)

for row in rows:
    if isinstance(row, dict) and str(row.get("id")) == job_id:
        raise SystemExit(0)

raise SystemExit(1)
PY
  then
    echo "Created job '${job_id}' is missing from /jobs/ listing." >&2
    echo "${RESPONSE_BODY}" >&2
    exit 1
  fi
}

create_credentials() {
  local name="$1"
  local user="$2"
  local password="$3"
  local host="$4"
  local port="$5"
  local database="$6"
  local payload
  local resp
  local id

  payload="$(python3 - "$name" "$user" "$password" "$host" "$port" "$database" <<'PY'
import json
import sys

name, user, password, host, port, database = sys.argv[1:]
obj = {
    "credentials": {
        "name": name,
        "user": user,
        "host": host,
        "port": int(port),
        "database": database,
    }
}
if password != "__NONE__":
    obj["credentials"]["password"] = password
print(json.dumps(obj))
PY
)"

  request_json "POST" "/contexts/credentials" "${payload}"
  ensure_success_status "Create credentials '${name}'"
  resp="${RESPONSE_BODY}"
  id="$(extract_object_id "${resp}")"
  if [[ -z "${id}" ]]; then
    echo "Failed to create credentials '${name}'." >&2
    echo "${resp}" >&2
    exit 1
  fi
   # Verify credentials exist
  request_json "GET" "/contexts/${id}"
  if [[ "${RESPONSE_STATUS}" != 2* ]]; then
      echo "Credentials created but not found (HTTP ${RESPONSE_STATUS})." >&2
      exit 1
  fi
  echo "${id}"
}

create_mapping_context() {
  local name="$1"
  local credentials_id="$2"
  local payload
  local resp
  local id

  payload="$(python3 - "$name" "$credentials_id" <<'PY'
import json
import sys

name, credentials_id = sys.argv[1:]
obj = {
    "context": {
        "name": name,
        "environment": "DEV",
        "credentials_ids": {
            "DEV": credentials_id,
            "TEST": credentials_id,
            "PROD": credentials_id,
        },
    }
}
print(json.dumps(obj))
PY
)"

  request_json "POST" "/contexts/credentials-mapping-context" "${payload}"
  ensure_success_status "Create mapping context '${name}'"
  resp="${RESPONSE_BODY}"
  id="$(extract_object_id "${resp}")"
  if [[ -z "${id}" ]]; then
    echo "Failed to create mapping context '${name}'." >&2
    echo "${resp}" >&2
    exit 1
  fi
  echo "${id}"
}

create_job() {
  local payload="$1"
  local resp
  local id
  request_json "POST" "/jobs/" "${payload}"
  ensure_success_status "Create job"
  resp="${RESPONSE_BODY}"
  id="$(extract_job_id "${resp}")"
  if [[ -z "${id}" ]]; then
    echo "Failed to create job." >&2
    echo "${resp}" >&2
    exit 1
  fi
  echo "${id}"
}

execute_job() {
  local job_id="$1"
  request_json "POST" "/execution/${job_id}" '{"environment":"DEV"}'
  ensure_success_status "Start execution for job '${job_id}'"
}

if [[ "${RESET}" -eq 1 ]]; then
  "${SCRIPT_DIR}/reset_demo_state.sh" --yes
fi

assert_core_ready
"${SCRIPT_DIR}/create_excel_input.sh" "${EXCEL_INPUT}" >/dev/null

stamp="$(date +%Y%m%d-%H%M%S)"

pg_creds_id="$(create_credentials "demo-pg-${stamp}" "postgres" "postgres" "127.0.0.1" "${POSTGRES_PORT}" "srcdb")"
maria_creds_id="$(create_credentials "demo-maria-${stamp}" "etl" "etlpass" "127.0.0.1" "${MARIADB_PORT}" "dstdb")"
mongo_creds_id="$(create_credentials "demo-mongo-${stamp}" "admin" "admin" "127.0.0.1" "${MONGO_PORT}" "mongosrc")"

pg_ctx_id="$(create_mapping_context "ctx-pg-${stamp}" "${pg_creds_id}")"
maria_ctx_id="$(create_mapping_context "ctx-maria-${stamp}" "${maria_creds_id}")"
mongo_ctx_id="$(create_mapping_context "ctx-mongo-${stamp}" "${mongo_creds_id}")"

job1_payload="$(cat <<JSON
{
  "name": "Demo 1 - Postgres Filter to MariaDB (${stamp})",
  "num_of_retries": 0,
  "file_logging": false,
  "strategy_type": "bulk",
  "components": [
    {
      "name": "reader_pg",
      "comp_type": "read_postgresql",
      "description": "Read customers from PostgreSQL",
      "context_id": "${pg_ctx_id}",
      "entity_name": "source_customers",
      "query": "SELECT id, name, city, amount FROM source_customers",
      "routes": { "out": [ { "to": "filter_amount", "in_port": "in" } ] },
      "out_port_schemas": {
        "out": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "name", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" }
        ] }
      }
    },
    {
      "name": "filter_amount",
      "comp_type": "filter",
      "description": "amount > 100",
      "rule": { "column": "amount", "operator": ">", "value": 100 },
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "name", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" }
        ] }
      },
      "routes": { "pass": [ { "to": "writer_maria", "in_port": "in" } ] },
      "out_port_schemas": {
        "pass": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "name", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" }
        ] }
      }
    },
    {
      "name": "writer_maria",
      "comp_type": "write_mariadb",
      "description": "Write filtered customers to MariaDB",
      "context_id": "${maria_ctx_id}",
      "entity_name": "filtered_customers",
      "operation": "insert",
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "name", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" }
        ] }
      }
    }
  ]
}
JSON
)"

job2_payload="$(cat <<JSON
{
  "name": "Demo 2 - Mongo Aggregation to PostgreSQL (${stamp})",
  "num_of_retries": 0,
  "file_logging": false,
  "strategy_type": "bulk",
  "components": [
    {
      "name": "reader_mongo",
      "comp_type": "read_mongodb",
      "description": "Read orders from MongoDB",
      "context_id": "${mongo_ctx_id}",
      "entity_name": "orders_src",
      "query_filter": {},
      "routes": { "out": [ { "to": "agg_sales", "in_port": "in" } ] },
      "out_port_schemas": {
        "out": { "fields": [
          { "name": "customer", "data_type": "string" },
          { "name": "category", "data_type": "string" },
          { "name": "amount", "data_type": "float" }
        ] }
      }
    },
    {
      "name": "agg_sales",
      "comp_type": "aggregation",
      "description": "Aggregate sales by category",
      "group_by": ["category"],
      "aggregations": [
        { "src": "amount", "op": "sum", "dest": "total_amount" },
        { "src": "*", "op": "count", "dest": "row_count" }
      ],
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "customer", "data_type": "string" },
          { "name": "category", "data_type": "string" },
          { "name": "amount", "data_type": "float" }
        ] }
      },
      "routes": { "out": [ { "to": "writer_pg", "in_port": "in" } ] },
      "out_port_schemas": {
        "out": { "fields": [
          { "name": "category", "data_type": "string" },
          { "name": "total_amount", "data_type": "float" },
          { "name": "row_count", "data_type": "integer" }
        ] }
      }
    },
    {
      "name": "writer_pg",
      "comp_type": "write_postgresql",
      "description": "Write aggregation to PostgreSQL",
      "context_id": "${pg_ctx_id}",
      "entity_name": "mongo_sales_agg",
      "operation": "insert",
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "category", "data_type": "string" },
          { "name": "total_amount", "data_type": "float" },
          { "name": "row_count", "data_type": "integer" }
        ] }
      }
    }
  ]
}
JSON
)"

job3_payload="$(cat <<JSON
{
  "name": "Demo 3 - Excel Filter to JSON (${stamp})",
  "num_of_retries": 0,
  "file_logging": false,
  "strategy_type": "bulk",
  "components": [
    {
      "name": "reader_excel",
      "comp_type": "read_excel",
      "description": "Read Excel input",
      "filepath": "${EXCEL_INPUT}",
      "routes": { "out": [ { "to": "filter_excel", "in_port": "in" } ] },
      "out_port_schemas": {
        "out": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "name", "data_type": "string" }
        ] }
      }
    },
    {
      "name": "filter_excel",
      "comp_type": "filter",
      "description": "name != Bob",
      "rule": { "column": "name", "operator": "!=", "value": "Bob" },
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "name", "data_type": "string" }
        ] }
      },
      "routes": { "pass": [ { "to": "writer_json", "in_port": "in" } ] },
      "out_port_schemas": {
        "pass": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "name", "data_type": "string" }
        ] }
      }
    },
    {
      "name": "writer_json",
      "comp_type": "write_json",
      "description": "Write filtered records to JSON",
      "filepath": "${JSON_OUTPUT}",
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "name", "data_type": "string" }
        ] }
      }
    }
  ]
}
JSON
)"

job1_id="$(create_job "${job1_payload}")"
job2_id="$(create_job "${job2_payload}")"
job3_id="$(create_job "${job3_payload}")"
assert_job_visible "${job1_id}"
assert_job_visible "${job2_id}"
assert_job_visible "${job3_id}"

if [[ "${EXECUTE}" -eq 1 ]]; then
  execute_job "${job1_id}"
  execute_job "${job2_id}"
  execute_job "${job3_id}"
fi

python3 - "${SUMMARY_FILE}" \
  "${job1_id}" "${job2_id}" "${job3_id}" \
  "${pg_ctx_id}" "${maria_ctx_id}" "${mongo_ctx_id}" \
  "${EXCEL_INPUT}" "${JSON_OUTPUT}" <<'PY'
import json
import sys

summary_file = sys.argv[1]
job1_id, job2_id, job3_id = sys.argv[2:5]
pg_ctx, maria_ctx, mongo_ctx = sys.argv[5:8]
excel_input, json_output = sys.argv[8:10]

data = {
    "jobs": {
        "postgres_filter_to_mariadb": job1_id,
        "mongodb_aggregation_to_postgresql": job2_id,
        "excel_filter_to_json": job3_id,
    },
    "contexts": {
        "postgres": pg_ctx,
        "mariadb": maria_ctx,
        "mongodb": mongo_ctx,
    },
    "files": {
        "excel_input": excel_input,
        "json_output": json_output,
    },
}

with open(summary_file, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)
PY

echo "Created demo jobs:"
echo "- Job 1 (Postgres -> Filter -> MariaDB): ${job1_id}"
echo "- Job 2 (MongoDB -> Aggregation -> PostgreSQL): ${job2_id}"
echo "- Job 3 (Excel -> Filter -> JSON): ${job3_id}"
if [[ "${EXECUTE}" -eq 1 ]]; then
  echo "All three jobs were executed in environment DEV."
fi
echo "Summary written to ${SUMMARY_FILE}"
