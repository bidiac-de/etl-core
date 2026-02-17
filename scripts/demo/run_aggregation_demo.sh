#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORE_URL="${CORE_URL:-http://127.0.0.1:8000}"
INPUT_XLSX="${1:-/tmp/demo_in.xlsx}"
OUTPUT_JSON="${2:-/tmp/agg_out.json}"
JOB_JSON="$(mktemp /tmp/agg_job.XXXXXX.json)"
trap 'rm -f "${JOB_JSON}"' EXIT

if [[ ! -f "${INPUT_XLSX}" ]]; then
  "${SCRIPT_DIR}/create_excel_input.sh" "${INPUT_XLSX}"
fi

cat >"${JOB_JSON}" <<JSON
{
  "name": "ExcelAggDemo",
  "num_of_retries": 0,
  "file_logging": false,
  "strategy_type": "bulk",
  "components": [
    {
      "name": "reader",
      "comp_type": "read_excel",
      "description": "Excel in",
      "filepath": "${INPUT_XLSX}",
      "routes": { "out": [ { "to": "agg", "in_port": "in" } ] },
      "out_port_schemas": {
        "out": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "name", "data_type": "string" }
        ] }
      }
    },
    {
      "name": "agg",
      "comp_type": "aggregation",
      "description": "nunique names",
      "group_by": [],
      "aggregations": [
        { "src": "name", "op": "nunique", "dest": "names" }
      ],
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "id", "data_type": "integer" },
          { "name": "name", "data_type": "string" }
        ] }
      },
      "routes": { "out": [ { "to": "writer", "in_port": "in" } ] },
      "out_port_schemas": {
        "out": { "fields": [
          { "name": "names", "data_type": "integer" }
        ] }
      }
    },
    {
      "name": "writer",
      "comp_type": "write_json",
      "description": "JSON out",
      "filepath": "${OUTPUT_JSON}",
      "in_port_schemas": {
        "in": { "fields": [
          { "name": "names", "data_type": "integer" }
        ] }
      }
    }
  ]
}
JSON

job_id="$(curl -sS -X POST "${CORE_URL}/jobs/" \
  -H "Content-Type: application/json" \
  --data @"${JOB_JSON}" | tr -d '"')"

if [[ -z "${job_id}" || "${job_id}" == "false" ]]; then
  echo "Failed to create aggregation demo job." >&2
  exit 1
fi

curl -sS -X POST "${CORE_URL}/execution/${job_id}" \
  -H "Content-Type: application/json" \
  -d '{"environment":"DEV"}' >/dev/null

echo "Aggregation job executed. Output:"
cat "${OUTPUT_JSON}"
