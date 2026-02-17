#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUT_PATH="${1:-/tmp/demo_in.xlsx}"
if [[ -z "${PYTHON_BIN:-}" ]]; then
  if [[ -x "${CORE_ROOT}/.conda/bin/python" ]]; then
    PYTHON_BIN="${CORE_ROOT}/.conda/bin/python"
  else
    PYTHON_BIN="python3"
  fi
fi

"${PYTHON_BIN}" - "${OUT_PATH}" <<'PY'
import sys
import pandas as pd

out = sys.argv[1]
df = pd.DataFrame(
    [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Cara"},
    ]
)
df.to_excel(out, index=False)
print(out)
PY

echo "Created Excel demo file: ${OUT_PATH}"
