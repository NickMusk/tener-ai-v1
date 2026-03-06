#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-${1:-http://127.0.0.1:8080}}"
MODE="${MODE:-${2:-dry}}"
ADMIN_TOKEN="${TENER_ADMIN_API_TOKEN:-}"

auth_args=()
if [[ -n "${ADMIN_TOKEN}" ]]; then
  auth_args=(-H "Authorization: Bearer ${ADMIN_TOKEN}")
fi

echo "== postgres rollback drill =="
echo "base=${BASE_URL}"
echo "mode=${MODE}"

echo
echo "-- preflight --"
curl -sS "${BASE_URL}/api/db/cutover/preflight" "${auth_args[@]}" | sed -n '1,220p'

echo
echo "-- monitoring --"
curl -sS "${BASE_URL}/api/monitoring/status?limit_jobs=20" "${auth_args[@]}" | sed -n '1,220p'

if [[ "${MODE}" != "run" ]]; then
  echo
  echo "Dry mode only. To execute active switch+rollback drill, run:"
  echo "  MODE=run BASE_URL=${BASE_URL} TENER_ADMIN_API_TOKEN=... bash scripts/postgres_rollback_drill.sh"
  exit 0
fi

echo
echo "-- cutover run (non-strict drill mode) --"
curl -sS -X POST "${BASE_URL}/api/db/cutover/run" \
  "${auth_args[@]}" \
  -H "Content-Type: application/json" \
  -d '{"execute_backfill":false,"strict_parity":false,"auto_switch_read_source":true,"set_dual_strict_on_success":false,"deep":false}' | sed -n '1,240p'

echo
echo "-- rollback --"
curl -sS -X POST "${BASE_URL}/api/db/cutover/rollback" \
  "${auth_args[@]}" \
  -H "Content-Type: application/json" \
  -d '{"disable_dual_strict":true}' | sed -n '1,240p'

echo
echo "-- final status --"
curl -sS "${BASE_URL}/api/db/cutover/status" "${auth_args[@]}" | sed -n '1,240p'

