# Tener LS v0.1 Backend

Prototype backend implementation aligned with the architecture in:
- `tener-ls-architecture-versions-plan.md`

Implemented in this version:
- Candidate intake API.
- Tier 1 verification orchestrator with provider abstraction.
- JD-centered manual workflow API + UI:
  - LinkedIn is connected by default.
  - Default test JD is hardcoded and seeded in storage on startup.
  - Add JD.
  - Run each step manually for selected JD:
    1) LinkedIn search
    2) Import candidates
    3) Run verification
- Async verification jobs through queue abstraction:
  - in-memory queue by default,
  - BullMQ if `REDIS_URL` is configured.
- Persistence abstraction:
  - in-memory repository by default,
  - PostgreSQL if `DATABASE_URL` is configured.
- Tier 1 checks:
  - OIG LEIE (local dataset provider, placeholder for file->DB importer).
  - SAM.gov (live API adapter, requires API key).
  - OFAC SDN (local dataset provider, placeholder for file/API adapter).
  - FDA Debarment (local dataset provider, placeholder for file->DB importer).
- Compliance aggregation with traffic light + progress.

## Run

1. Install dependencies:
```bash
npm install
```

2. Configure environment:
```bash
cp .env.example .env
```

3. Start development server:
```bash
npm run dev
```

## API

- `GET /health`
- `GET /api/v1/linkedin/status`
- `GET /api/v1/jds`
- `GET /api/v1/jds/default`
- `POST /api/v1/jds`
- `GET /api/v1/jds/:jobDescriptionId`
- `POST /api/v1/jds/:jobDescriptionId/steps/linkedin-search`
- `POST /api/v1/jds/:jobDescriptionId/steps/import-candidates`
- `POST /api/v1/jds/:jobDescriptionId/steps/run-verification`
- `GET /api/v1/jds/:jobDescriptionId/candidates`
- `POST /api/v1/candidates`
- `GET /api/v1/candidates`
- `GET /api/v1/candidates/:candidateId`
- `GET /api/v1/candidates/:candidateId/compliance`
- `GET /api/v1/candidates/:candidateId/compliance/full` (all 15 checks with lifecycle + ETA)
- `POST /api/v1/candidates/:candidateId/compliance/run` (async enqueue, returns `202`)
- `POST /api/v1/candidates/:candidateId/compliance/run-sync` (direct run)
- `GET /api/v1/candidates/compliance-jobs/:jobId`

## Example

Open UI:
```bash
open http://localhost:3000
```

In UI, you can use the "Quick Run Default Test JD" block without selecting a JD card.

Create candidate:
```bash
curl -sS -X POST http://localhost:3000/api/v1/candidates \
  -H "Content-Type: application/json" \
  -d '{"fullName":"James T. Powell","dob":"1982-11-30","state":"NY"}'
```

Run compliance:
```bash
curl -sS -X POST http://localhost:3000/api/v1/candidates/<candidate-id>/compliance/run
```

Get job status:
```bash
curl -sS http://localhost:3000/api/v1/candidates/compliance-jobs/<job-id>
```

## Architecture Notes

- `src/verification/orchestrator.ts` is the central orchestration layer.
- `src/verification/providers/provider.ts` is the abstraction seam for self-hosted and partner providers.
- `src/services/jobDescriptionService.ts` owns manual JD workflow execution.
- `src/linkedin/defaultLinkedInProvider.ts` keeps LinkedIn connected by default in current version.
- `src/repositories/postgresCandidateRepository.ts` enables persistence via `DATABASE_URL`.
- `src/repositories/postgresJobDescriptionRepository.ts` persists JD workflows via `DATABASE_URL`.
- `src/queue/bullMqVerificationJobQueue.ts` enables queue-first execution via `REDIS_URL`.

## Render Deployment

The repo includes `render.yaml` blueprint for:
- Web service (`tener-ls-api`)
- Managed PostgreSQL (`tener-ls-db`)
- Managed Redis (`tener-ls-redis`)
