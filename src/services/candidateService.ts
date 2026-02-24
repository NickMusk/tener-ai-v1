import { Candidate, CandidateProfile, ComplianceState } from "../domain/model";
import { HttpError } from "../http/httpError";
import { VerificationJobQueue, VerificationJobSnapshot } from "../queue/types";
import { CandidateRepository } from "../repositories/candidateRepository";
import { VerificationOrchestrator } from "../verification/orchestrator";

export class CandidateService {
  private jobQueue?: VerificationJobQueue;

  constructor(
    private readonly repository: CandidateRepository,
    private readonly orchestrator: VerificationOrchestrator
  ) {}

  setJobQueue(jobQueue: VerificationJobQueue): void {
    this.jobQueue = jobQueue;
  }

  async createCandidate(profile: CandidateProfile): Promise<Candidate> {
    return this.repository.create(profile);
  }

  async listCandidates(): Promise<Candidate[]> {
    return this.repository.list();
  }

  async getCandidate(candidateId: string): Promise<Candidate> {
    const candidate = await this.repository.findById(candidateId);

    if (!candidate) {
      throw new HttpError(404, `Candidate ${candidateId} was not found.`);
    }

    return candidate;
  }

  async getCompliance(candidateId: string): Promise<ComplianceState> {
    return (await this.getCandidate(candidateId)).compliance;
  }

  async runTier1ComplianceNow(candidateId: string): Promise<ComplianceState> {
    const candidate = await this.getCandidate(candidateId);
    const runResult = await this.orchestrator.runTier1(candidate);

    candidate.compliance = {
      checks: runResult.checks,
      progress: runResult.progress,
      trafficLight: runResult.trafficLight,
      lastRunAt: runResult.completedAt
    };

    await this.repository.save(candidate);

    return candidate.compliance;
  }

  async enqueueTier1Compliance(candidateId: string): Promise<VerificationJobSnapshot> {
    await this.getCandidate(candidateId);

    if (!this.jobQueue) {
      throw new HttpError(500, "Verification job queue is not configured.");
    }

    return this.jobQueue.enqueueTier1(candidateId);
  }

  async getVerificationJob(jobId: string): Promise<VerificationJobSnapshot> {
    if (!this.jobQueue) {
      throw new HttpError(500, "Verification job queue is not configured.");
    }

    return this.jobQueue.getJob(jobId);
  }
}
