import { randomUUID } from "crypto";
import { Pool } from "pg";
import { Candidate, CandidateProfile } from "../domain/model";
import { buildPendingChecks, calculateProgress, calculateTrafficLight } from "../verification/trafficLight";
import { CandidateRepository } from "./candidateRepository";

interface CandidateRow {
  id: string;
  profile: CandidateProfile;
  compliance: Candidate["compliance"];
  created_at: Date | string;
  updated_at: Date | string;
}

const mapRow = (row: CandidateRow): Candidate => ({
  id: row.id,
  profile: row.profile,
  compliance: row.compliance,
  createdAt: new Date(row.created_at).toISOString(),
  updatedAt: new Date(row.updated_at).toISOString()
});

export class PostgresCandidateRepository implements CandidateRepository {
  constructor(private readonly pool: Pool) {}

  async init(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS candidates (
        id TEXT PRIMARY KEY,
        profile JSONB NOT NULL,
        compliance JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
  }

  async create(profile: CandidateProfile): Promise<Candidate> {
    const now = new Date().toISOString();
    const checks = buildPendingChecks();
    const candidate: Candidate = {
      id: randomUUID(),
      profile,
      compliance: {
        checks,
        progress: calculateProgress(checks),
        trafficLight: calculateTrafficLight(checks)
      },
      createdAt: now,
      updatedAt: now
    };

    await this.pool.query(
      `
      INSERT INTO candidates (id, profile, compliance, created_at, updated_at)
      VALUES ($1, $2::jsonb, $3::jsonb, $4, $5)
      `,
      [candidate.id, JSON.stringify(candidate.profile), JSON.stringify(candidate.compliance), candidate.createdAt, candidate.updatedAt]
    );

    return candidate;
  }

  async save(candidate: Candidate): Promise<void> {
    const updatedAt = new Date().toISOString();
    candidate.updatedAt = updatedAt;

    await this.pool.query(
      `
      UPDATE candidates
      SET profile = $2::jsonb,
          compliance = $3::jsonb,
          updated_at = $4
      WHERE id = $1
      `,
      [candidate.id, JSON.stringify(candidate.profile), JSON.stringify(candidate.compliance), updatedAt]
    );
  }

  async findById(id: string): Promise<Candidate | undefined> {
    const { rows } = await this.pool.query<CandidateRow>(
      `
      SELECT id, profile, compliance, created_at, updated_at
      FROM candidates
      WHERE id = $1
      `,
      [id]
    );

    if (rows.length === 0) {
      return undefined;
    }

    return mapRow(rows[0]);
  }

  async list(): Promise<Candidate[]> {
    const { rows } = await this.pool.query<CandidateRow>(
      `
      SELECT id, profile, compliance, created_at, updated_at
      FROM candidates
      ORDER BY created_at DESC
      `
    );

    return rows.map(mapRow);
  }
}
