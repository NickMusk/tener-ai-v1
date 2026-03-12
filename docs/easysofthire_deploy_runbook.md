# EasySoftHire Deploy Runbook

This runbook covers the exact steps required to launch the dedicated `EasySoftHire` production stack.

## Stack

Services defined in [render.yaml](/Users/Nick/Documents/Tener prototype/render.yaml):

- `tener-easysoftgroup`
- `tener-easysoftgroup-interview`

Instance profile:

- [config/instances/easysofthire/instance.json](/Users/Nick/Documents/Tener prototype/config/instances/easysofthire/instance.json)
- [config/instances/easysofthire/company_profile.json](/Users/Nick/Documents/Tener prototype/config/instances/easysofthire/company_profile.json)

## 1. Create services in Render

Create a new Render Blueprint or new services from the repo using the updated [render.yaml](/Users/Nick/Documents/Tener prototype/render.yaml).

Expected public URLs:

- `https://tener-easysoftgroup.onrender.com`
- `https://tener-easysoftgroup-interview.onrender.com`

## 2. Provision databases

Create two Postgres databases in Render:

- one for `tener-easysoftgroup`
- one for `tener-easysoftgroup-interview`

Recommendation:

- do not reuse current Tener production databases
- use separate databases, not separate schemas in one DB

## 3. Configure environment variables

### Main service: `tener-easysoftgroup`

Required secrets:

- `TENER_DB_DSN`
- `TENER_ADMIN_API_TOKEN`
- `UNIPILE_API_KEY`
- `UNIPILE_ACCOUNT_ID`
- `OPENAI_API_KEY`

Configured in `render.yaml` already:

- `TENER_INSTANCE_CONFIG_PATH=config/instances/easysofthire/instance.json`
- `TENER_REQUIRE_INSTANCE_CONFIG=true`
- `TENER_DB_BACKEND=postgres`
- `TENER_DB_READ_SOURCE=postgres`
- `TENER_PUBLIC_BASE_URL=https://tener-easysoftgroup.onrender.com`
- `TENER_INTERVIEW_API_BASE=https://tener-easysoftgroup-interview.onrender.com`
- `TENER_RESUME_STORAGE_DIR=/var/data/resumes`

Optional but recommended:

- `UNIPILE_BASE_URL`
- `OPENAI_BASE_URL`
- `TENER_LINKEDIN_RECRUITER_NAME`

### Interview service: `tener-easysoftgroup-interview`

Required secrets:

- `TENER_INTERVIEW_DB_DSN`
- `TENER_INTERVIEW_ADMIN_TOKEN`
- `TENER_INTERVIEW_TOKEN_SECRET`
- `TENER_HIREFLIX_API_KEY`
- `TENER_HIREFLIX_POSITION_ID`

Configured in `render.yaml` already:

- `TENER_INSTANCE_CONFIG_PATH=config/instances/easysofthire/instance.json`
- `TENER_REQUIRE_INSTANCE_CONFIG=true`
- `TENER_INTERVIEW_PROVIDER=hireflix`
- `TENER_INTERVIEW_DB_BACKEND=postgres`
- `TENER_INTERVIEW_SOURCE_API_BASE=https://tener-easysoftgroup.onrender.com`
- `TENER_INTERVIEW_PUBLIC_BASE_URL=https://tener-easysoftgroup-interview.onrender.com`
- `TENER_INTERVIEW_COMPANY_PROFILE_PATH=config/instances/easysofthire/company_profile.json`

## 4. Deploy

Deploy both services after secrets are set.

Expected startup behavior:

- main service must boot against Postgres
- interview service must boot against Postgres
- interview service must fail startup if Hireflix adapter cannot initialize in strict mode

## 5. Smoke checks

Run:

```bash
cd "/Users/Nick/Documents/Tener prototype"
bash scripts/easysofthire_smoke.sh
```

Required environment for the script:

```bash
export EASYSOFTHIRE_MAIN_BASE="https://tener-easysoftgroup.onrender.com"
export EASYSOFTHIRE_INTERVIEW_BASE="https://tener-easysoftgroup-interview.onrender.com"
export EASYSOFTHIRE_MAIN_TOKEN="<TENER_ADMIN_API_TOKEN>"
export EASYSOFTHIRE_INTERVIEW_TOKEN="<TENER_INTERVIEW_ADMIN_TOKEN>"
```

## 6. Manual product verification

### Main service

1. Open `/`
2. Confirm branding is `EasySoftHire`
3. Confirm `/dashboard` returns `401` without bearer token
4. Confirm `/dashboard` loads with `Authorization: Bearer <main token>`

### Interview service

1. Confirm `/dashboard` returns `401` without bearer token
2. Confirm `/dashboard` loads with `Authorization: Bearer <interview token>`
3. Create one test interview session from the dashboard or API
4. Open the candidate link without any token
5. Confirm the candidate landing is branded `EasySoftHire`
6. Confirm `/i/<token>/start` redirects to Hireflix

## 7. API smoke examples

### Main dashboard auth check

```bash
curl -i https://tener-easysoftgroup.onrender.com/dashboard
curl -i https://tener-easysoftgroup.onrender.com/dashboard \
  -H "Authorization: Bearer ${EASYSOFTHIRE_MAIN_TOKEN}"
```

### Interview dashboard auth check

```bash
curl -i https://tener-easysoftgroup-interview.onrender.com/dashboard
curl -i https://tener-easysoftgroup-interview.onrender.com/dashboard \
  -H "Authorization: Bearer ${EASYSOFTHIRE_INTERVIEW_TOKEN}"
```

### Create a test job in main service

```bash
curl -sS -X POST https://tener-easysoftgroup.onrender.com/api/jobs \
  -H "Authorization: Bearer ${EASYSOFTHIRE_MAIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "EasySoftHire QA Engineer",
    "company": "EasySoftHire",
    "jd_text": "Manual testing, API testing, regression testing, communication with product and engineering teams.",
    "location": "Remote",
    "preferred_languages": ["en"],
    "seniority": "middle"
  }'
```

### Create interview session

```bash
curl -sS -X POST https://tener-easysoftgroup-interview.onrender.com/api/interviews/sessions/start \
  -H "Authorization: Bearer ${EASYSOFTHIRE_INTERVIEW_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": 1,
    "candidate_id": 1,
    "candidate_name": "Test Candidate",
    "candidate_email": "candidate@example.com",
    "language": "en"
  }'
```

## 8. Handoff to EasySoftHire users

Share:

- main app URL
- interview app URL
- main admin bearer token
- interview admin bearer token

Do not share:

- Postgres DSNs
- provider API keys
- Render service access

## 9. Rollback

If deploy is broken:

1. disable access sharing with client immediately
2. roll back Render service to previous deploy
3. do not switch databases back to shared Tener DB
4. re-run the smoke script before re-sharing URLs
