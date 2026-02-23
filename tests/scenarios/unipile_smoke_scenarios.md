# Unipile Smoke Scenarios (Opt-In)

This document describes smoke checks for live Unipile integration.

These tests are **opt-in** and run only when:

- `RUN_UNIPILE_SMOKE=1`
- `UNIPILE_API_KEY`, `UNIPILE_ACCOUNT_ID`, `UNIPILE_BASE_URL` are configured

Scenario definitions are stored in:

- `tests/scenarios/unipile_smoke_scenarios.json`

## Purpose

1. Validate that live sourcing returns non-empty candidate sets for broad roles.
2. Validate that verification runs for every sourced profile.
3. Validate that each verification result contains human-readable rules explanation (`human_explanation`).
4. Validate end-to-end execution in `UNIPILE_DRY_RUN=true` mode (no actual message send).

## Scenarios

1. `unipile_backend_global`
- Job: Senior Backend Engineer, Remote.
- Source expected range: `1..15`.
- Verify expected range: `0..15`.

2. `unipile_frontend_global`
- Job: Frontend Engineer, Remote.
- Source expected range: `1..15`.
- Verify expected range: `0..15`.

## Running only Unipile smoke

```bash
cd "/Users/Nick/Documents/Tener prototype"
RUN_UNIPILE_SMOKE=1 \
UNIPILE_API_KEY="..." \
UNIPILE_ACCOUNT_ID="..." \
UNIPILE_BASE_URL="https://api20.unipile.com:15017" \
PYTHONPATH=src python3 -m unittest tests/test_unipile_smoke_scenarios.py -v
```

## Notes

- Keep `UNIPILE_DRY_RUN=true` for smoke tests to avoid unintended outbound communication.
- If these tests are flaky in CI, capture raw provider responses and adjust `source_min/source_max` in JSON per account reality.
