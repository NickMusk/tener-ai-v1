# Tener AI V1 Workflow Backend

V1 backend prototype for autonomous hiring workflow:
- search candidates on LinkedIn (mock provider by default, Unipile-ready adapter included)
- verify candidates with editable matchmaking rules and JD core-profile extraction
- add candidates to internal DB
- outreach via LinkedIn channel
- auto-answer candidate questions in candidate language
- full operation logging

## Architecture

Agents:
- `SourcingAgent`: candidate discovery
- `VerificationAgent`: scoring + pass/reject using `config/matching_rules.json`
- `OutreachAgent`: first contact message generation
- `FAQAgent`: autonomous Q&A
- `PreResumeCommunicationService` (standalone): manages candidate dialog until CV/resume is received

Core services:
- API server: `src/tener_ai/main.py`
- Workflow orchestration: `src/tener_ai/workflow.py`
- Internal storage: SQLite (`TENER_DB_PATH`, default local `runtime/tener_v1.sqlite3`, on Render default `/var/data/tener_v1.sqlite3`)
- LinkedIn provider layer: `src/tener_ai/linkedin_provider.py`
- Standalone pre-resume service: `src/tener_ai/pre_resume_service.py`

## Pre-Resume Service

The pre-resume block is implemented as a separate service and integrated into the main workflow/webhook path.

It supports:
- state machine (`awaiting_reply`, `engaged_no_resume`, `resume_promised`, `resume_received`, `not_interested`, `unreachable`, `stalled`)
- intent routing for inbound messages
- follow-up cadence with max follow-up cap
- multilingual template fallback

Example:

```python
from tener_ai.pre_resume_service import PreResumeCommunicationService

service = PreResumeCommunicationService()
start = service.start_session(
    session_id="candidate-1",
    candidate_name="Alex",
    job_title="Senior Backend Engineer",
    scope_summary="python, aws, distributed systems",
)
print(start["outbound"])

reply = service.handle_inbound("candidate-1", "What is the salary range?")
print(reply["intent"], reply["outbound"])
```

## Run

```bash
cd "/Users/Nick/Documents/Tener prototype"
PYTHONPATH=src python3 -m tener_ai
```

Server starts on `http://127.0.0.1:8080`.
Server binds `0.0.0.0` and uses `PORT` automatically in cloud.

## Company Profile Test Service

Standalone service for company culture profile scraping/synthesis with mini dashboard.
It now prioritizes job-board pages (Greenhouse/Lever/Workday/etc.) to extract:
- what candidate profiles the company hired for previously
- which culture attributes are emphasized in job ads
- and it generates an operational culture profile (mission/performance/decision/risk/join-vs-avoid)

Run:

```bash
cd "/Users/Nick/Documents/Tener prototype"
PYTHONPATH=src python3 -m tener_company_profile
```

Service defaults:
- API/UI: `http://127.0.0.1:8095`
- dashboard: `GET /dashboard`
- generate: `POST /api/company-profiles/generate`

Recommended env vars for real Google + LLM mode:

```bash
export GOOGLE_CSE_API_KEY="<google_key>"
export GOOGLE_CSE_CX="<google_cse_id>"
export OPENAI_API_KEY="<openai_key>"
PYTHONPATH=src python3 -m tener_company_profile
```

Useful switches:
- `TENER_COMPANY_PROFILE_SEARCH_MODE=bing_rss|duckduckgo_html|brave_html|google_cse|seed`
- `TENER_COMPANY_PROFILE_ALLOW_SEED_FALLBACK=true|false`
- `TENER_COMPANY_PROFILE_MIN_JOB_BOARD_LINKS=3`
- `TENER_COMPANY_PROFILE_ANALYSIS_RULES_PATH=config/instructions/company_culture_profile_analysis_rules.md`
- `TENER_COMPANY_PROFILE_PORT=8095`

## API

### 1) Create Job

```bash
curl -s -X POST http://127.0.0.1:8080/api/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Senior Backend Engineer",
    "jd_text": "Need Python, Django, AWS, Docker. Remote preferred.",
    "location": "Germany",
    "preferred_languages": ["en", "ru"],
    "seniority": "senior"
  }'
```

### 2) Run Full Autonomous Workflow

```bash
curl -s -X POST http://127.0.0.1:8080/api/workflows/execute \
  -H "Content-Type: application/json" \
  -d '{"job_id": 1, "limit": 20}'
```

### 3) List Matched Candidates

```bash
curl -s http://127.0.0.1:8080/api/jobs/1/candidates
```

### 3.1) Get Saved Job Progress (for dashboard refresh)

```bash
curl -s http://127.0.0.1:8080/api/jobs/1/progress
```

### 4) Simulate Inbound Candidate Message

```bash
curl -s -X POST http://127.0.0.1:8080/api/conversations/1/inbound \
  -H "Content-Type: application/json" \
  -d '{"message": "What is the salary range and interview timeline?"}'
```

### 5) Read Logs

```bash
curl -s "http://127.0.0.1:8080/api/logs?limit=200"
```

## Matchmaking Rules

Rules file: `config/matching_rules.json`

You can modify weights/thresholds/skill dictionary without code changes, then reload:

```bash
curl -s -X POST http://127.0.0.1:8080/api/rules/reload
```

## Unipile

If `UNIPILE_API_KEY` is set, provider switches from mock dataset to Unipile API.

```bash
export UNIPILE_API_KEY="<key>"
export UNIPILE_BASE_URL="https://api.unipile.com"
export UNIPILE_ACCOUNT_ID="<your_account_id>"
PYTHONPATH=src python3 -m tener_ai
```

Recommended env vars:

- `UNIPILE_API_KEY`: API key from Unipile.
- `UNIPILE_BASE_URL`: API base URL (default `https://api.unipile.com`).
- `UNIPILE_ACCOUNT_ID`: required for LinkedIn search and outbound delivery.
- `UNIPILE_LINKEDIN_SEARCH_PATH`: default `/api/v1/users/search`.
- `UNIPILE_CHAT_CREATE_PATH`: default `/api/v1/chats`.
- `UNIPILE_CONNECT_CREATE_PATH`: default `/api/v1/users/invite` (override to your workspace endpoint).
- `UNIPILE_LINKEDIN_API_TYPE`: optional, e.g. `classic` or `recruiter`.
- `UNIPILE_LINKEDIN_INMAIL`: optional (`true/false`) to force InMail flag.
- `UNIPILE_DRY_RUN`: optional (`true/false`) to disable actual outbound send.
- `TENER_AGENT_INSTRUCTIONS_PATH`: optional path to agent/stage instruction file (default `config/agent_instructions.json`).
- `TENER_LLM_ENABLED`: optional (`true/false`), default `true`.
- `OPENAI_API_KEY`: optional, enables LLM-generated candidate replies.
- `TENER_LLM_MODEL`: optional model name (default `gpt-4o-mini`).
- `OPENAI_BASE_URL`: optional API base URL (default `https://api.openai.com/v1`).
- `TENER_LLM_TIMEOUT_SECONDS`: optional timeout for LLM calls (default `30`).
- `TENER_FORCED_TEST_IDS_PATH`: optional path to newline-separated LinkedIn IDs to force-inject into every source run (default `config/forced_test_linkedin_ids.txt`).
- `TENER_FORCED_TEST_SCORE`: optional forced verification score for IDs from that file (default `0.99`).

Flow with Unipile enabled:

1. Candidate sourcing uses Unipile LinkedIn Search.
2. In `contact-all` mode (default), low-confidence candidates are marked as `needs_resume` (not rejected) and CV is requested automatically in outreach.
3. Outreach first attempts direct message via Unipile Chats API.
4. If candidate is not first-degree connection, system sends connection request and marks conversation as `waiting_connection` (`pending_connection` in outreach result).
5. After connection accepted, pending outreach is sent automatically by webhook (if connection event is available) or by polling endpoint.
6. Delivery result is stored in message `meta.delivery` and operation logs.

Communication behavior:
- pre-resume and FAQ replies keep deterministic state/intent flow;
- outbound text can be generated by LLM (when `OPENAI_API_KEY` is set), using:
  - agent instruction for current stage
  - full JD text
  - candidate profile
  - recent conversation context
  - fallback template response

Workflow mode env vars:

- `TENER_CONTACT_ALL_MODE`: default `true`; converts pre-CV rejects to `needs_resume`.
- `TENER_REQUIRE_RESUME_BEFORE_FINAL_VERIFY`: default `true`; first outreach asks for CV/resume.

Persistent DB on Render:

- Set `TENER_DB_PATH=/var/data/tener_v1.sqlite3`.
- Attach a persistent disk mounted at `/var/data`.
- Without a disk, redeploy creates a new container filesystem and SQLite data is lost.

Forced test candidate file:

- Default file: `config/forced_test_linkedin_ids.txt`
- Format: one LinkedIn identifier per line (`public_identifier` or provider id), `#` comments are allowed.
- The file is read on each source/verify execution, so updates apply immediately without redeploy.

## Agent Instructions File

All funnel stages can be configured from a dedicated instruction file:
- default path: `config/agent_instructions.json`
- endpoint to inspect active instructions: `GET /api/instructions`
- endpoint to reload file at runtime: `POST /api/instructions/reload`
- long instructions can be stored in separate files via:
  - `"sourcing": {"file": "instructions/linkedin_sourcing_guide.md"}`
  - path is resolved relative to `agent_instructions.json`

This enables per-stage instruction ownership without code edits (sourcing/enrich/verification/add/outreach/faq/pre_resume).

## Integrated Pre-Resume Flow

When a candidate is in `needs_resume`, outreach creates a pre-resume session and all inbound messages for that conversation are routed through pre-resume policy until terminal status.

Tracking endpoints:

- `GET /api/chats/overview` — all candidate chats with latest message + pre-resume status.
- `GET /api/pre-resume/sessions` — current pre-resume sessions across candidates.
- `GET /api/pre-resume/events` — chronological updates/events across all sessions.
- `POST /api/jobs/{job_id}/jd` — update JD text used by recruiter agent context.
- `POST /api/agent/accounts/manual` — add a manual test account + start pre-resume dialog.
- `POST /api/outreach/poll-connections` — cron-friendly poll to detect accepted connections and send pending outreach.

Dashboard (`/dashboard`) now includes a dedicated `Recruiter Agent` tab:
- edit JD context for selected job
- see active accounts/conversations
- add manual test accounts
- send inbound candidate messages and observe agent replies in-chat

## How To Test Standalone Pre-Resume Service

### Automated tests

```bash
cd "/Users/Nick/Documents/Tener prototype"
PYTHONPATH=src python3 -m unittest tests/test_pre_resume_service.py -v
```

### Full test suite

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_*.py'
```

### API contract + e2e scenarios

Runs API end-to-end scenarios over a real local HTTP server and validates response contracts from:

- `tests/scenarios/api_response_contracts.json`
- `tests/scenarios/api_e2e_scenarios.json`

```bash
PYTHONPATH=src python3 -m unittest tests/test_api_e2e_scenarios.py -v
```

Run part-by-part suites:

```bash
# Sourcing + enrichment profile part
PYTHONPATH=src python3 -m unittest \
  tests.test_api_e2e_scenarios.ApiE2EScenariosTests.test_e2e_part_sourcing_enrichment_profile -v

# Candidate communication part
PYTHONPATH=src python3 -m unittest \
  tests.test_api_e2e_scenarios.ApiE2EScenariosTests.test_e2e_part_communication -v

# Interviewing part (pre-resume state flow)
PYTHONPATH=src python3 -m unittest \
  tests.test_api_e2e_scenarios.ApiE2EScenariosTests.test_e2e_part_interviewing -v

# Final composed full-flow e2e
PYTHONPATH=src python3 -m unittest \
  tests.test_api_e2e_scenarios.ApiE2EScenariosTests.test_e2e_full_flow_composed_from_parts -v
```

### API smoke test (standalone service endpoints)

1) Start server:

```bash
PYTHONPATH=src python3 -m tener_ai
```

2) Start a pre-resume session:

```bash
curl -s -X POST http://127.0.0.1:8080/api/pre-resume/sessions/start \
  -H "Content-Type: application/json" \
  -d '{
    "session_id": "cand-1",
    "candidate_name": "Alex",
    "job_title": "Senior Backend Engineer",
    "scope_summary": "python, aws, distributed systems"
  }'
```

3) Send inbound candidate message:

```bash
curl -s -X POST http://127.0.0.1:8080/api/pre-resume/sessions/cand-1/inbound \
  -H "Content-Type: application/json" \
  -d '{"message":"What is the salary range?"}'
```

4) Trigger follow-up:

```bash
curl -s -X POST http://127.0.0.1:8080/api/pre-resume/sessions/cand-1/followup
```

5) Mark unreachable:

```bash
curl -s -X POST http://127.0.0.1:8080/api/pre-resume/sessions/cand-1/unreachable \
  -H "Content-Type: application/json" \
  -d '{"error":"no_connection_with_recipient"}'
```

6) Read session state:

```bash
curl -s http://127.0.0.1:8080/api/pre-resume/sessions/cand-1
```

## Deploy

Render:
- `render.yaml` is included.
- Push this repo to GitHub and create a new Render Blueprint from the repo.
- Runtime is Docker, health check is `/health`.
- Add env vars in Render dashboard (`UNIPILE_API_KEY`, `UNIPILE_ACCOUNT_ID`, etc.) if you want real LinkedIn integration.

Railway:
- `railway.toml` is included.
- Create a new project from this repo; Railway will build from `Dockerfile`.
