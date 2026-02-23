# E2E Workflow Scenarios (Mock LinkedIn Provider)

This folder defines deterministic end-to-end scenarios for V1 pipeline validation:

1. `source` (find candidates)
2. `verify` (apply matchmaking rules)
3. `add` (persist verified candidates)
4. `outreach` (create conversation + outbound message)
5. `faq` (auto-reply language and intent)

The source of truth for numeric expectations is:

- `tests/scenarios/workflow_e2e_scenarios.json`

## Why ranges, not exact counts

Sourcing now uses multiple query variants and candidate deduplication. Counts are stable with mock data, but we keep ranges (`min/max`) to avoid brittle tests when ranking logic evolves.

## Scenario summary

1. `senior_backend_germany`
- Job: Senior Backend Engineer in Germany.
- Expect sourcing: 6..10 candidates.
- Expect verification: 1..2 verified, 4..9 rejected.
- Expected top verified profile includes `Alex Morgan`.

2. `frontend_typescript_spain`
- Job: Frontend Engineer in Spain.
- Expect sourcing: 8..10 candidates.
- Expect verification: 1..2 verified, 6..9 rejected.
- Expected top verified profile includes `Miguel Santos`.

3. `qa_automation_georgia`
- Job: QA Automation Engineer in Georgia.
- Expect sourcing: 6..8 candidates.
- Expect verification: 1..2 verified, 4..7 rejected.
- Expected top verified profile includes `Anastasia Volkova`.

## Verification explanation requirements

For every verified/rejected profile, tests enforce:

- `notes.human_explanation` exists
- explanation is non-empty
- explanation references scoring logic (`score`)

This ensures rejects are understandable in plain language, based on matchmaking rules.

## Running tests

```bash
cd "/Users/Nick/Documents/Tener prototype"
PYTHONPATH=src python3 -m unittest discover -s tests -v
```
