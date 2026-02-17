#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORE_URL="${CORE_URL:-http://127.0.0.1:8000}"

# --- Port auto-detection ---
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

POSTGRES_PORT="${POSTGRES_PORT:-5432}"
MARIADB_PORT="${MARIADB_PORT:-3307}"
EXCEL_OUTPUT="${EXCEL_OUTPUT:-/tmp/story_unified_book.xlsx}"
REGULAR_JSON_OUTPUT="${REGULAR_JSON_OUTPUT:-/tmp/story_regular_review.json}"
SUMMARY_JSON_OUTPUT="${SUMMARY_JSON_OUTPUT:-/tmp/story_city_summary.json}"
SUMMARY_FILE="${SUMMARY_FILE:-/tmp/etl_complex_story_job.json}"
EXECUTE=0
RESET=0

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
    echo "Created story job '${job_id}' is missing from /jobs/ listing." >&2
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
        "password": password,
    }
}
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
  ensure_success_status "Create story job"
  resp="${RESPONSE_BODY}"
  id="$(extract_job_id "${resp}")"
  if [[ -z "${id}" ]]; then
    echo "Failed to create story job." >&2
    echo "${resp}" >&2
    exit 1
  fi
  echo "${id}"
}

execute_job() {
  local job_id="$1"
  request_json "POST" "/execution/${job_id}" '{"environment":"DEV"}'
  ensure_success_status "Start execution for story job '${job_id}'"
}

if [[ "${RESET}" -eq 1 ]]; then
  "${SCRIPT_DIR}/reset_demo_state.sh" --yes
fi

assert_core_ready

stamp="$(date +%Y%m%d-%H%M%S)"

pg_creds_id="$(create_credentials "story-pg-${stamp}" "postgres" "postgres" "127.0.0.1" "${POSTGRES_PORT}" "srcdb")"
maria_creds_id="$(create_credentials "story-maria-${stamp}" "etl" "etlpass" "127.0.0.1" "${MARIADB_PORT}" "dstdb")"

pg_ctx_id="$(create_mapping_context "story-ctx-pg-${stamp}" "${pg_creds_id}")"
maria_ctx_id="$(create_mapping_context "story-ctx-maria-${stamp}" "${maria_creds_id}")"

story_payload="$(cat <<JSON
{
  "name": "Story Demo - OmniChannel Order Triage (${stamp})",
  "num_of_retries": 0,
  "file_logging": false,
  "strategy_type": "row",
  "components": [
    {
      "name": "orders_pg_reader",
      "comp_type": "read_postgresql",
      "description": "Read all incoming orders from PostgreSQL",
      "context_id": "${pg_ctx_id}",
      "entity_name": "story_orders",
      "query": "SELECT id, customer, city, amount::double precision AS amount, channel FROM story_orders ORDER BY id",
      "routes": { "out": [ { "to": "order_splitter", "in_port": "in" } ] },
      "out_port_schemas": {
        "out": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] }
      }
    },
    {
      "name": "order_splitter",
      "comp_type": "split",
      "description": "Duplicate each order into VIP and Standard decision lanes",
      "extra_output_ports": ["vip_lane", "standard_lane"],
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] }
      },
      "out_port_schemas": {
        "vip_lane": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] },
        "standard_lane": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] }
      },
      "routes": {
        "vip_lane": [ { "to": "vip_filter", "in_port": "in" } ],
        "standard_lane": [ { "to": "standard_filter", "in_port": "in" } ]
      }
    },
    {
      "name": "vip_filter",
      "comp_type": "filter",
      "description": "VIP lane keeps high value orders (amount >= 200)",
      "rule": { "column": "amount", "operator": ">=", "value": 200 },
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] }
      },
      "out_port_schemas": {
        "pass": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] }
      },
      "routes": {
        "pass": [
          { "to": "vip_writer_maria", "in_port": "in" },
          { "to": "approved_merge", "in_port": "in" }
        ]
      }
    },
    {
      "name": "standard_filter",
      "comp_type": "filter",
      "description": "Standard lane keeps campaign-eligible orders (100 <= amount < 200)",
      "rule": {
        "logical_operator": "AND",
        "rules": [
          { "column": "amount", "operator": ">=", "value": 100 },
          { "column": "amount", "operator": "<", "value": 200 }
        ]
      },
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] }
      },
      "out_port_schemas": {
        "pass": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] }
      },
      "routes": {
        "pass": [
          { "to": "regular_writer_json", "in_port": "in" },
          { "to": "approved_merge", "in_port": "in" }
        ]
      }
    },
    {
      "name": "approved_merge",
      "comp_type": "merge",
      "description": "Merge approved VIP and Standard orders into one stream",
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] }
      },
      "out_port_schemas": {
        "merge": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] }
      },
      "routes": {
        "merge": [ { "to": "post_merge_split", "in_port": "in" } ]
      }
    },
    {
      "name": "post_merge_split",
      "comp_type": "split",
      "description": "Fan out merged stream to operational and analytic sinks",
      "extra_output_ports": ["to_unified_pg", "to_unified_excel", "to_city_agg"],
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] }
      },
      "out_port_schemas": {
        "to_unified_pg": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] },
        "to_unified_excel": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] },
        "to_city_agg": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] }
      },
      "routes": {
        "to_unified_pg": [ { "to": "unified_writer_pg", "in_port": "in" } ],
        "to_unified_excel": [ { "to": "unified_writer_excel", "in_port": "in" } ],
        "to_city_agg": [ { "to": "city_aggregation", "in_port": "in" } ]
      }
    },
    {
      "name": "unified_writer_pg",
      "comp_type": "write_postgresql",
      "description": "Persist approved stream into PostgreSQL customer book",
      "context_id": "${pg_ctx_id}",
      "entity_name": "story_unified_book",
      "operation": "insert",
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] }
      }
    },
    {
      "name": "unified_writer_excel",
      "comp_type": "write_excel",
      "description": "Write approved stream to Excel for finance handoff",
      "filepath": "${EXCEL_OUTPUT}",
      "sheet_name": "Unified",
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] }
      }
    },
    {
      "name": "city_aggregation",
      "comp_type": "aggregation",
      "description": "Aggregate approved orders by city",
      "group_by": ["city"],
      "aggregations": [
        { "src": "amount", "op": "sum", "dest": "total_amount" },
        { "src": "*", "op": "count", "dest": "order_count" }
      ],
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] }
      },
      "out_port_schemas": {
        "out": { "fields": [
          { "name": "city", "data_type": "string" },
          { "name": "total_amount", "data_type": "float" },
          { "name": "order_count", "data_type": "integer" }
        ] }
      },
      "routes": { "out": [ { "to": "summary_split", "in_port": "in" } ] }
    },
    {
      "name": "summary_split",
      "comp_type": "split",
      "description": "Fan out city summary to data products",
      "extra_output_ports": ["to_summary_json", "to_summary_pg"],
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "city", "data_type": "string" },
          { "name": "total_amount", "data_type": "float" },
          { "name": "order_count", "data_type": "integer" }
        ] }
      },
      "out_port_schemas": {
        "to_summary_json": { "fields": [
          { "name": "city", "data_type": "string" },
          { "name": "total_amount", "data_type": "float" },
          { "name": "order_count", "data_type": "integer" }
        ] },
        "to_summary_pg": { "fields": [
          { "name": "city", "data_type": "string" },
          { "name": "total_amount", "data_type": "float" },
          { "name": "order_count", "data_type": "integer" }
        ] }
      },
      "routes": {
        "to_summary_json": [ { "to": "summary_writer_json", "in_port": "in" } ],
        "to_summary_pg": [ { "to": "summary_writer_pg", "in_port": "in" } ]
      }
    },
    {
      "name": "summary_writer_json",
      "comp_type": "write_json",
      "description": "Publish city summary as JSON for dashboard API",
      "filepath": "${SUMMARY_JSON_OUTPUT}",
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "city", "data_type": "string" },
          { "name": "total_amount", "data_type": "float" },
          { "name": "order_count", "data_type": "integer" }
        ] }
      }
    },
    {
      "name": "summary_writer_pg",
      "comp_type": "write_postgresql",
      "description": "Store city summary in PostgreSQL mart table",
      "context_id": "${pg_ctx_id}",
      "entity_name": "story_city_summary",
      "operation": "insert",
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "city", "data_type": "string" },
          { "name": "total_amount", "data_type": "float" },
          { "name": "order_count", "data_type": "integer" }
        ] }
      }
    },
    {
      "name": "vip_writer_maria",
      "comp_type": "write_mariadb",
      "description": "Write VIP orders to MariaDB CRM intake",
      "context_id": "${maria_ctx_id}",
      "entity_name": "story_vip_customers",
      "operation": "insert",
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] }
      }
    },
    {
      "name": "regular_writer_json",
      "comp_type": "write_json",
      "description": "Persist standard lane orders for campaign review",
      "filepath": "${REGULAR_JSON_OUTPUT}",
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "customer", "data_type": "string" },
          { "name": "city", "data_type": "string" },
          { "name": "amount", "data_type": "float" },
          { "name": "channel", "data_type": "string" }
        ] }
      }
    }
  ]
}
JSON
)"

job_id="$(create_job "${story_payload}")"
assert_job_visible "${job_id}"

if [[ "${EXECUTE}" -eq 1 ]]; then
  execute_job "${job_id}"
fi

python3 - "${SUMMARY_FILE}" \
  "${job_id}" "${pg_ctx_id}" "${maria_ctx_id}" \
  "${EXCEL_OUTPUT}" "${REGULAR_JSON_OUTPUT}" "${SUMMARY_JSON_OUTPUT}" <<'PY'
import json
import sys

summary_file = sys.argv[1]
job_id, pg_ctx, maria_ctx = sys.argv[2:5]
excel_out, regular_json, summary_json = sys.argv[5:8]

data = {
    "job_id": job_id,
    "story": "OmniChannel Order Triage",
    "contexts": {
        "postgres_context_id": pg_ctx,
        "mariadb_context_id": maria_ctx,
    },
    "outputs": {
        "excel_unified": excel_out,
        "regular_lane_json": regular_json,
        "city_summary_json": summary_json,
    },
}

with open(summary_file, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)
PY

echo "Created complex story job: ${job_id}"
if [[ "${EXECUTE}" -eq 1 ]]; then
  echo "Executed complex story job in environment DEV."
fi
echo "Summary written to ${SUMMARY_FILE}"
