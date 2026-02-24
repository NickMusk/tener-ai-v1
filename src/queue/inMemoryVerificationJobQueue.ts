import { randomUUID } from "crypto";
import { HttpError } from "../http/httpError";
import { Tier1Processor, VerificationJobQueue, VerificationJobSnapshot } from "./types";

export class InMemoryVerificationJobQueue implements VerificationJobQueue {
  private readonly jobs = new Map<string, VerificationJobSnapshot>();
  private readonly processor: Tier1Processor;

  constructor(processor: Tier1Processor) {
    this.processor = processor;
  }

  async enqueueTier1(candidateId: string): Promise<VerificationJobSnapshot> {
    const now = new Date().toISOString();
    const job: VerificationJobSnapshot = {
      id: randomUUID(),
      status: "QUEUED",
      candidateId,
      createdAt: now,
      updatedAt: now
    };

    this.jobs.set(job.id, job);

    setTimeout(async () => {
      const running = this.jobs.get(job.id);
      if (!running) {
        return;
      }

      running.status = "RUNNING";
      running.updatedAt = new Date().toISOString();
      this.jobs.set(job.id, running);

      try {
        await this.processor(candidateId);
        running.status = "COMPLETED";
        running.updatedAt = new Date().toISOString();
        this.jobs.set(job.id, running);
      } catch (error) {
        running.status = "FAILED";
        running.updatedAt = new Date().toISOString();
        running.error = error instanceof Error ? error.message : "Unknown job error";
        this.jobs.set(job.id, running);
      }
    }, 0);

    return job;
  }

  async getJob(jobId: string): Promise<VerificationJobSnapshot> {
    const job = this.jobs.get(jobId);
    if (!job) {
      throw new HttpError(404, `Verification job ${jobId} was not found.`);
    }

    return job;
  }
}
