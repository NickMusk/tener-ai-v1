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

Core services:
- API server: `src/tener_ai/main.py`
- Workflow orchestration: `src/tener_ai/workflow.py`
- Internal storage: SQLite (`runtime/tener_v1.sqlite3`)
- LinkedIn provider layer: `src/tener_ai/linkedin_provider.py`

## Run

```bash
cd "/Users/Nick/Documents/Tener prototype"
PYTHONPATH=src python3 -m tener_ai
```

Server starts on `http://127.0.0.1:8080`.
Server binds `0.0.0.0` and uses `PORT` automatically in cloud.

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
- `UNIPILE_LINKEDIN_API_TYPE`: optional, e.g. `classic` or `recruiter`.
- `UNIPILE_LINKEDIN_INMAIL`: optional (`true/false`) to force InMail flag.
- `UNIPILE_DRY_RUN`: optional (`true/false`) to disable actual outbound send.

Flow with Unipile enabled:

1. Candidate sourcing uses Unipile LinkedIn Search.
2. In `contact-all` mode (default), low-confidence candidates are marked as `needs_resume` (not rejected) and CV is requested automatically in outreach.
3. Outreach is written to DB and additionally sent via Unipile Chats API.
4. Delivery result is stored in message `meta.delivery` and operation logs.

Workflow mode env vars:

- `TENER_CONTACT_ALL_MODE`: default `true`; converts pre-CV rejects to `needs_resume`.
- `TENER_REQUIRE_RESUME_BEFORE_FINAL_VERIFY`: default `true`; first outreach asks for CV/resume.

## Deploy

Render:
- `render.yaml` is included.
- Push this repo to GitHub and create a new Render Blueprint from the repo.
- Runtime is Docker, health check is `/health`.
- Add env vars in Render dashboard (`UNIPILE_API_KEY`, `UNIPILE_ACCOUNT_ID`, etc.) if you want real LinkedIn integration.

Railway:
- `railway.toml` is included.
- Create a new project from this repo; Railway will build from `Dockerfile`.
