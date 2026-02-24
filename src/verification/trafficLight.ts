import { checkSources, tier1CheckTypes } from "../domain/checks";
import { ComplianceTrafficLight, VerificationCheckResult } from "../domain/model";

export const buildPendingChecks = (): VerificationCheckResult[] =>
  tier1CheckTypes.map((checkType) => ({
    checkType,
    source: checkSources[checkType],
    status: "PENDING",
    summary: "Check is queued and has not started yet.",
    confidence: 0,
    matchedRecords: [],
    checkedAt: new Date().toISOString(),
    latencyMs: 0
  }));

export const calculateProgress = (checks: VerificationCheckResult[]): string => {
  const completed = checks.filter((check) => check.status !== "PENDING").length;
  return `${completed}/${checks.length}`;
};

export const calculateTrafficLight = (
  checks: VerificationCheckResult[]
): ComplianceTrafficLight => {
  if (checks.some((check) => check.status === "FLAGGED")) {
    return "RED";
  }

  if (checks.some((check) => check.status === "PENDING" || check.status === "ERROR")) {
    return "YELLOW";
  }

  return "GREEN";
};
