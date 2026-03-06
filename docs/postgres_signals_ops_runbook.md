# Postgres + Signals Ops Runbook

## Scope
- main API cutover/rollback control
- live signals ingestion health
- monitoring/alert interpretation
- rollback drill procedure

## Prerequisites
- `TENER_DB_BACKEND=dual` (or `postgres` rollout mode)
- `TENER_DB_DSN` configured
- migrations applied (`migrations/*.sql`)
- optional admin auth:
  - `TENER_ADMIN_API_TOKEN` for admin endpoints
  - or auth service with admin scope token

## Monitoring

### Health
- `GET /health`
  - check `db_backend`, `db_runtime_mode`, `db_read_status`, `dual_write`, `db_cutover`, `postgres_migration_status`

### Signal coverage alerts
- `GET /api/monitoring/status?limit_jobs=20`
  - returns `status=ok|warning`
  - `alerts` includes:
    - `signals_missing`
    - `low_signal_coverage`

### Live job signal view
- `GET /api/jobs/{job_id}/signals/live?refresh=1`
  - performs ingestion from runtime sources + returns current ranking/timeline

## Rollback Drill

Dry-run (safe, no switch):
```bash
BASE_URL=https://tener-ai-v1.onrender.com \
TENER_ADMIN_API_TOKEN=... \
bash scripts/postgres_rollback_drill.sh
```

Active drill (switch + rollback):
```bash
MODE=run \
BASE_URL=https://tener-ai-v1.onrender.com \
TENER_ADMIN_API_TOKEN=... \
bash scripts/postgres_rollback_drill.sh
```

The script executes:
1. `GET /api/db/cutover/preflight`
2. `GET /api/monitoring/status`
3. `POST /api/db/cutover/run` (drill mode: non-strict parity, no backfill)
4. `POST /api/db/cutover/rollback`
5. `GET /api/db/cutover/status`

## Manual emergency rollback
```bash
curl -sS -X POST "$BASE_URL/api/db/cutover/rollback" \
  -H "Authorization: Bearer $TENER_ADMIN_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"disable_dual_strict":true}'
```

Expected:
- `status=ok`
- read source switched to sqlite
- dual strict disabled

