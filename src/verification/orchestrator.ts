import { checkSources, tier1CheckTypes } from "../domain/checks";
import { Candidate, VerificationCheckResult } from "../domain/model";
import { buildPendingChecks, calculateProgress, calculateTrafficLight } from "./trafficLight";
import { VerificationProvider } from "./providers/provider";

export interface VerificationRunResult {
  checks: VerificationCheckResult[];
  progress: string;
  trafficLight: "GREEN" | "YELLOW" | "RED";
  completedAt: string;
  durationMs: number;
}

export class VerificationOrchestrator {
  private readonly providersByType: Map<string, VerificationProvider>;

  constructor(providers: VerificationProvider[]) {
    this.providersByType = new Map(providers.map((provider) => [provider.checkType, provider]));
  }

  async runTier1(candidate: Candidate): Promise<VerificationRunResult> {
    const startedAt = Date.now();
    const pendingChecks = buildPendingChecks();

    const checks = await Promise.all(
      pendingChecks.map(async (pendingCheck): Promise<VerificationCheckResult> => {
        const provider = this.providersByType.get(pendingCheck.checkType);

        if (!provider) {
          return {
            ...pendingCheck,
            status: "ERROR",
            summary: `No provider registered for ${pendingCheck.checkType}.`,
            error: "ProviderNotRegistered"
          };
        }

        try {
          return await provider.run(candidate);
        } catch (error) {
          const message = error instanceof Error ? error.message : "Unknown verification error";
          return {
            checkType: pendingCheck.checkType,
            source: checkSources[pendingCheck.checkType],
            status: "ERROR",
            summary: `Check failed: ${message}`,
            confidence: 0,
            matchedRecords: [],
            checkedAt: new Date().toISOString(),
            latencyMs: 0,
            error: message
          };
        }
      })
    );

    const orderedChecks = tier1CheckTypes.map(
      (checkType) => checks.find((check) => check.checkType === checkType) as VerificationCheckResult
    );

    return {
      checks: orderedChecks,
      progress: calculateProgress(orderedChecks),
      trafficLight: calculateTrafficLight(orderedChecks),
      completedAt: new Date().toISOString(),
      durationMs: Date.now() - startedAt
    };
  }
}
