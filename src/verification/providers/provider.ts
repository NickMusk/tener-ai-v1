import { CheckType } from "../../domain/checks";
import { Candidate, VerificationCheckResult } from "../../domain/model";

export interface VerificationProvider {
  checkType: CheckType;
  run(candidate: Candidate): Promise<VerificationCheckResult>;
}
