export type VerificationJobStatus = "QUEUED" | "RUNNING" | "COMPLETED" | "FAILED" | "UNKNOWN";

export interface VerificationJobSnapshot {
  id: string;
  status: VerificationJobStatus;
  candidateId: string;
  createdAt: string;
  updatedAt: string;
  error?: string;
}

export type Tier1Processor = (candidateId: string) => Promise<void>;

export interface VerificationJobQueue {
  enqueueTier1(candidateId: string): Promise<VerificationJobSnapshot>;
  getJob(jobId: string): Promise<VerificationJobSnapshot>;
}
