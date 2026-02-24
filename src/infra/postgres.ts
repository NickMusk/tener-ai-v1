import { Pool } from "pg";

export const createPostgresPool = (databaseUrl: string): Pool =>
  new Pool({
    connectionString: databaseUrl,
    ssl: databaseUrl.includes("render.com") ? { rejectUnauthorized: false } : undefined
  });
