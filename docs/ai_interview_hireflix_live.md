# AI Interview + Hireflix Live Flow (isolated module)

## Render deployment (separate URL)

`render.yaml` now defines a dedicated service: `tener-interview-dashboard`.

Expected URL after deploy:
- `https://tener-interview-dashboard.onrender.com/dashboard`

Required Render envs for this service:
- `TENER_HIREFLIX_API_KEY`
- `TENER_HIREFLIX_POSITION_ID` (for your account: `699d9212c1700764a4fff102`)
- `TENER_INTERVIEW_TOKEN_SECRET`

Service reads JD/candidates from main API via:
- `TENER_INTERVIEW_SOURCE_API_BASE=https://tener-ai-v1.onrender.com`

Custom transcription scoring criteria file:
- `config/interview_transcription_scoring_criteria.json`
- env override: `TENER_INTERVIEW_TRANSCRIPTION_SCORING_CRITERIA_PATH`

Total candidate score formula file:
- `config/interview_total_score_formula.json`
- env override: `TENER_INTERVIEW_TOTAL_SCORE_FORMULA_PATH`
- includes weights (`technical/soft_skills/culture_fit`), missing strategy, and recommendation thresholds

## 0) Get Hireflix `position_id` (assessment analog)

List positions:

```bash
export TENER_HIREFLIX_API_KEY="<YOUR_HIREFLIX_API_KEY>"
curl -sS "https://api.hireflix.com/me" \
  -H "x-api-key: ${TENER_HIREFLIX_API_KEY}" \
  -H "content-type: application/json" \
  --data '{"query":"query { positions { id name active archived } }"}'
```

Create a new position (if needed):

```bash
curl -sS "https://api.hireflix.com/me" \
  -H "x-api-key: ${TENER_HIREFLIX_API_KEY}" \
  -H "content-type: application/json" \
  --data '{"query":"mutation ($input: PositionInputType!) { createPosition(input: $input) { id name active } }","variables":{"input":{"name":"Tener AI API Interview","description":"Isolated AI interview flow","questions":[{"title":"Tell us about your background","timeLimit":{"minutes":1,"seconds":30}},{"title":"Describe a technical challenge you solved","timeLimit":{"minutes":2,"seconds":0}},{"title":"How do you collaborate with cross-functional teams?","timeLimit":{"minutes":1,"seconds":30}}]}}}'
```

For your current account, validated working `position_id`:

`699d9212c1700764a4fff102`

## 1) Start module with real Hireflix adapter

```bash
cd "/Users/Nick/Documents/Tener prototype"
export TENER_INTERVIEW_PROVIDER="hireflix"
export TENER_HIREFLIX_API_KEY="<YOUR_HIREFLIX_API_KEY>"
export TENER_HIREFLIX_POSITION_ID="<YOUR_POSITION_ID>" # e.g. 699d9212c1700764a4fff102
export TENER_INTERVIEW_SOURCE_DB_PATH="./runtime/tener_v1.sqlite3" # where JD/candidates live

# Optional
export TENER_HIREFLIX_BASE_URL="https://api.hireflix.com/me"
export TENER_HIREFLIX_PUBLIC_APP_BASE="https://app.hireflix.com"
export TENER_HIREFLIX_TIMEOUT_SECONDS="30"

export TENER_INTERVIEW_PUBLIC_BASE_URL="http://127.0.0.1:8090"
export TENER_INTERVIEW_TOKEN_SECRET="<LONG_RANDOM_SECRET>"
PYTHONPATH=src python3 -m tener_interview
```

## 2) Open admin dashboard

[http://127.0.0.1:8090/dashboard](http://127.0.0.1:8090/dashboard)

Flow in UI:

1. Select JD from source DB.
2. Click `Create Interview Link` for candidate.
3. Send generated `entry_url` to candidate.
4. Click `Refresh All Sessions For Job` to pull statuses/results.
5. Read ranking in `Leaderboard`.

## 3) Generate Hireflix interview link via API (alternative)

```bash
curl -sS -X POST http://127.0.0.1:8090/api/interviews/sessions/start \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: start-candidate-42" \
  -d '{
    "job_id": 1,
    "candidate_id": 42,
    "candidate_name": "Jane Doe",
    "candidate_email": "jane.doe@example.com",
    "language": "en",
    "ttl_hours": 72
  }'
```

Response contains `entry_url`. Send this URL in LinkedIn.

## 4) Candidate passes interview via link

Candidate opens `entry_url`.
Module resolves `/i/{token}` and redirects (`302`) to Hireflix interview URL.

## 5) Pull results into Tener system

```bash
curl -sS -X POST http://127.0.0.1:8090/api/interviews/sessions/<SESSION_ID>/refresh \
  -H "Content-Type: application/json" \
  -d '{"force": false}'
```

Repeat refresh until status becomes `scored`.

## 6) Show results in dashboard data API

```bash
curl -sS "http://127.0.0.1:8090/api/jobs/1/interview-leaderboard?limit=50"
```

This response is ready for candidate ranking in dashboard.

## 6.1) Get per-question scorecard

```bash
curl -sS "http://127.0.0.1:8090/api/interviews/sessions/<SESSION_ID>/scorecard"
```

This returns dimension scores + `transcription_scoring.question_scores` with `0..100` for each question.

## 7) Batch run for multiple candidates

```bash
curl -sS -X POST http://127.0.0.1:8090/api/steps/interview \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": 1,
    "candidate_ids": [42, 43, 44],
    "mode": "start_or_refresh"
  }'
```

This endpoint updates isolated `job_step_progress` (`step=interview`).
