import { CheckType } from "./checks";

export type CheckStatus = "PENDING" | "CLEAR" | "FLAGGED" | "ERROR";
export type ComplianceTrafficLight = "GREEN" | "YELLOW" | "RED";

export interface CandidateProfile {
  fullName: string;
  dob?: string;
  state?: string;
  licenseNumber?: string;
  deaNumber?: string;
  jobDescriptionId?: string;
  source?: "LINKEDIN" | "MANUAL";
  headline?: string;
  linkedinProfileUrl?: string;
}

export interface VerificationCheckResult {
  checkType: CheckType;
  source: string;
  status: CheckStatus;
  summary: string;
  confidence: number;
  matchedRecords: Array<Record<string, unknown>>;
  checkedAt: string;
  latencyMs: number;
  error?: string;
}

export interface ComplianceState {
  trafficLight: ComplianceTrafficLight;
  progress: string;
  checks: VerificationCheckResult[];
  lastRunAt?: string;
}

export interface Candidate {
  id: string;
  profile: CandidateProfile;
  compliance: ComplianceState;
  createdAt: string;
  updatedAt: string;
}
