import { randomUUID } from "crypto";
import { Pool } from "pg";
import { buildDefaultTestJobDescription, DEFAULT_TEST_JD_ID } from "../domain/defaultJobDescription";
import { JobDescription } from "../domain/jobDescription";
import { CreateJobDescriptionInput, JobDescriptionRepository } from "./jobDescriptionRepository";

interface JobDescriptionRow {
  id: string;
  document: JobDescription;
  created_at: Date | string;
  updated_at: Date | string;
}

const buildInitialJobDescription = (input: CreateJobDescriptionInput): JobDescription => {
  const now = new Date().toISOString();

  return {
    id: randomUUID(),
    title: input.title,
    company: input.company,
    location: input.location,
    keywords: input.keywords,
    createdAt: now,
    updatedAt: now,
    steps: {
      linkedinSearch: { status: "NOT_STARTED" },
      importCandidates: { status: "NOT_STARTED" },
      runVerification: { status: "NOT_STARTED" }
    },
    linkedinCandidates: [],
    importedCandidateIds: [],
    verificationJobIds: []
  };
};

const mapRow = (row: JobDescriptionRow): JobDescription => {
  const value = row.document;
  value.createdAt = new Date(row.created_at).toISOString();
  value.updatedAt = new Date(row.updated_at).toISOString();
  return value;
};

export class PostgresJobDescriptionRepository implements JobDescriptionRepository {
  constructor(private readonly pool: Pool) {}

  async init(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS job_descriptions (
        id TEXT PRIMARY KEY,
        document JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    const seeded = buildDefaultTestJobDescription();
    await this.pool.query(
      `
      INSERT INTO job_descriptions (id, document, created_at, updated_at)
      VALUES ($1, $2::jsonb, $3, $4)
      ON CONFLICT (id) DO NOTHING
      `,
      [seeded.id, JSON.stringify(seeded), seeded.createdAt, seeded.updatedAt]
    );
  }

  async create(input: CreateJobDescriptionInput): Promise<JobDescription> {
    const item = buildInitialJobDescription(input);
    await this.pool.query(
      `
      INSERT INTO job_descriptions (id, document, created_at, updated_at)
      VALUES ($1, $2::jsonb, $3, $4)
      `,
      [item.id, JSON.stringify(item), item.createdAt, item.updatedAt]
    );
    return item;
  }

  async save(jobDescription: JobDescription): Promise<void> {
    jobDescription.updatedAt = new Date().toISOString();
    await this.pool.query(
      `
      UPDATE job_descriptions
      SET document = $2::jsonb,
          updated_at = $3
      WHERE id = $1
      `,
      [jobDescription.id, JSON.stringify(jobDescription), jobDescription.updatedAt]
    );
  }

  async findById(id: string): Promise<JobDescription | undefined> {
    const { rows } = await this.pool.query<JobDescriptionRow>(
      `
      SELECT id, document, created_at, updated_at
      FROM job_descriptions
      WHERE id = $1
      `,
      [id]
    );

    if (rows.length === 0) {
      return undefined;
    }

    return mapRow(rows[0]);
  }

  async list(): Promise<JobDescription[]> {
    const { rows } = await this.pool.query<JobDescriptionRow>(
      `
      SELECT id, document, created_at, updated_at
      FROM job_descriptions
      ORDER BY (id = $1) DESC, created_at DESC
      `,
      [DEFAULT_TEST_JD_ID]
    );

    return rows.map(mapRow);
  }
}
