import { Candidate, VerificationCheckResult } from "../../domain/model";
import { VerificationProvider } from "./provider";

interface SamGovProviderOptions {
  apiKey: string;
  baseUrl: string;
}

interface SamGovResponse {
  totalRecords?: number;
  excludedRecords?: unknown[];
}

export class SamGovProvider implements VerificationProvider {
  readonly checkType = "SAM_EXCLUSIONS" as const;
  private readonly apiKey: string;
  private readonly baseUrl: string;

  constructor(options: SamGovProviderOptions) {
    this.apiKey = options.apiKey;
    this.baseUrl = options.baseUrl;
  }

  async run(candidate: Candidate): Promise<VerificationCheckResult> {
    const startedAt = Date.now();

    if (!this.apiKey) {
      return {
        checkType: this.checkType,
        source: "SAM.gov API",
        status: "PENDING",
        summary: "SAM.gov API key is not configured. Check requires environment setup.",
        confidence: 0,
        matchedRecords: [],
        checkedAt: new Date().toISOString(),
        latencyMs: Date.now() - startedAt
      };
    }

    try {
      const query = new URLSearchParams({
        api_key: this.apiKey,
        q: candidate.profile.fullName,
        includeSections: "exclusionDetails"
      });

      const response = await fetch(`${this.baseUrl}?${query.toString()}`, {
        headers: {
          Accept: "application/json"
        }
      });

      if (!response.ok) {
        return {
          checkType: this.checkType,
          source: "SAM.gov API",
          status: "ERROR",
          summary: `SAM.gov request failed with HTTP ${response.status}.`,
          confidence: 0,
          matchedRecords: [],
          checkedAt: new Date().toISOString(),
          latencyMs: Date.now() - startedAt,
          error: `HTTP ${response.status}`
        };
      }

      const data = (await response.json()) as SamGovResponse;
      const totalRecords = data.totalRecords ?? 0;
      const excludedRecords = data.excludedRecords ?? [];

      return {
        checkType: this.checkType,
        source: "SAM.gov API",
        status: totalRecords > 0 ? "FLAGGED" : "CLEAR",
        summary:
          totalRecords > 0
            ? `SAM.gov returned ${totalRecords} potential exclusion records.`
            : "No SAM.gov exclusions found.",
        confidence: totalRecords > 0 ? 0.8 : 0.95,
        matchedRecords: totalRecords > 0 ? (excludedRecords as Array<Record<string, unknown>>) : [],
        checkedAt: new Date().toISOString(),
        latencyMs: Date.now() - startedAt
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unexpected SAM.gov error.";
      return {
        checkType: this.checkType,
        source: "SAM.gov API",
        status: "ERROR",
        summary: "SAM.gov check could not be completed.",
        confidence: 0,
        matchedRecords: [],
        checkedAt: new Date().toISOString(),
        latencyMs: Date.now() - startedAt,
        error: message
      };
    }
  }
}
