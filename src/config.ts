export const config = {
  port: Number(process.env.PORT ?? 3000),
  databaseUrl: process.env.DATABASE_URL ?? "",
  redisUrl: process.env.REDIS_URL ?? "",
  samGov: {
    apiKey: process.env.SAM_GOV_API_KEY ?? "",
    baseUrl: process.env.SAM_GOV_BASE_URL ?? "https://api.sam.gov/entity-information/v4/exclusions"
  }
};
