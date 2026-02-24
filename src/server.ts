import { createApp } from "./app";
import { checkSources } from "./domain/checks";
import { createPostgresPool } from "./infra/postgres";
import { BullMqVerificationJobQueue } from "./queue/bullMqVerificationJobQueue";
import { InMemoryVerificationJobQueue } from "./queue/inMemoryVerificationJobQueue";
import { InMemoryCandidateRepository } from "./repositories/inMemoryCandidateRepository";
import { PostgresCandidateRepository } from "./repositories/postgresCandidateRepository";
import { CandidateService } from "./services/candidateService";
import { config } from "./config";
import { VerificationOrchestrator } from "./verification/orchestrator";
import { LocalDatasetProvider } from "./verification/providers/localDatasetProvider";
import { fdaDebarmentDataset, leieDataset, ofacDataset } from "./verification/providers/mockDatasets";
import { SamGovProvider } from "./verification/providers/samGovProvider";

const bootstrap = async (): Promise<void> => {
  const repository = config.databaseUrl
    ? new PostgresCandidateRepository(createPostgresPool(config.databaseUrl))
    : new InMemoryCandidateRepository();

  if (repository.init) {
    await repository.init();
  }

  const orchestrator = new VerificationOrchestrator([
    new LocalDatasetProvider({
      checkType: "OIG_LEIE",
      source: checkSources.OIG_LEIE,
      dataset: leieDataset
    }),
    new SamGovProvider({
      apiKey: config.samGov.apiKey,
      baseUrl: config.samGov.baseUrl
    }),
    new LocalDatasetProvider({
      checkType: "OFAC_SDN",
      source: checkSources.OFAC_SDN,
      dataset: ofacDataset
    }),
    new LocalDatasetProvider({
      checkType: "FDA_DEBARMENT",
      source: checkSources.FDA_DEBARMENT,
      dataset: fdaDebarmentDataset
    })
  ]);

  const candidateService = new CandidateService(repository, orchestrator);

  const queue = config.redisUrl
    ? new BullMqVerificationJobQueue({
        redisUrl: config.redisUrl,
        processor: async (candidateId: string) => {
          await candidateService.runTier1ComplianceNow(candidateId);
        }
      })
    : new InMemoryVerificationJobQueue(async (candidateId: string) => {
        await candidateService.runTier1ComplianceNow(candidateId);
      });

  candidateService.setJobQueue(queue);

  const app = createApp(candidateService);

  app.listen(config.port, () => {
    process.stdout.write(`tener-ls-v01 running on port ${config.port}\n`);
    process.stdout.write(
      `repository=${config.databaseUrl ? "postgres" : "in-memory"} queue=${config.redisUrl ? "bullmq" : "in-memory"}\n`
    );
  });
};

bootstrap().catch((error) => {
  const message = error instanceof Error ? error.stack ?? error.message : String(error);
  process.stderr.write(`bootstrap_failed: ${message}\n`);
  process.exit(1);
});
