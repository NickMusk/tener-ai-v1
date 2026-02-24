# Tener LS v0.1 Backend

Prototype backend implementation aligned with the architecture in:
- `tener-ls-architecture-versions-plan.md`

Implemented in this version:
- Candidate intake API.
- Tier 1 verification orchestrator with provider abstraction.
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
- `POST /api/v1/candidates`
- `GET /api/v1/candidates`
- `GET /api/v1/candidates/:candidateId`
- `GET /api/v1/candidates/:candidateId/compliance`
- `POST /api/v1/candidates/:candidateId/compliance/run` (async enqueue, returns `202`)
- `POST /api/v1/candidates/:candidateId/compliance/run-sync` (direct run)
- `GET /api/v1/candidates/compliance-jobs/:jobId`

## Example

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
- `src/repositories/postgresCandidateRepository.ts` enables persistence via `DATABASE_URL`.
- `src/queue/bullMqVerificationJobQueue.ts` enables queue-first execution via `REDIS_URL`.

## Render Deployment

The repo includes `render.yaml` blueprint for:
- Web service (`tener-ls-api`)
- Managed PostgreSQL (`tener-ls-db`)
- Managed Redis (`tener-ls-redis`)
