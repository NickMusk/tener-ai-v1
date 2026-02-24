import { CheckType } from "../../domain/checks";
import { Candidate, VerificationCheckResult } from "../../domain/model";
import { normalizeName } from "./nameUtils";
import { LocalDatasetRecord } from "./mockDatasets";
import { VerificationProvider } from "./provider";

interface LocalDatasetProviderOptions {
  checkType: CheckType;
  source: string;
  dataset: LocalDatasetRecord[];
}

export class LocalDatasetProvider implements VerificationProvider {
  readonly checkType: CheckType;
  private readonly source: string;
  private readonly dataset: LocalDatasetRecord[];

  constructor(options: LocalDatasetProviderOptions) {
    this.checkType = options.checkType;
    this.source = options.source;
    this.dataset = options.dataset;
  }

  async run(candidate: Candidate): Promise<VerificationCheckResult> {
    const startedAt = Date.now();
    const candidateName = normalizeName(candidate.profile.fullName);

    const matchedRecords = this.dataset.filter((record) => {
      const nameMatched = normalizeName(record.fullName) === candidateName;

      if (!nameMatched) {
        return false;
      }

      if (!record.dob || !candidate.profile.dob) {
        return true;
      }

      return record.dob === candidate.profile.dob;
    });

    const isFlagged = matchedRecords.length > 0;

    return {
      checkType: this.checkType,
      source: this.source,
      status: isFlagged ? "FLAGGED" : "CLEAR",
      summary: isFlagged
        ? `Potential match found in ${this.source}. Manual review required.`
        : `No records found in ${this.source}.`,
      confidence: isFlagged ? 0.75 : 0.95,
      matchedRecords: matchedRecords.map((record) => record.details),
      checkedAt: new Date().toISOString(),
      latencyMs: Date.now() - startedAt
    };
  }
}
