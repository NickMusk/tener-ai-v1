import { Queue, Worker, QueueEvents, Job } from "bullmq";
import IORedis from "ioredis";
import { HttpError } from "../http/httpError";
import { Tier1Processor, VerificationJobQueue, VerificationJobSnapshot } from "./types";

interface BullMqVerificationJobQueueOptions {
  redisUrl: string;
  processor: Tier1Processor;
}

export class BullMqVerificationJobQueue implements VerificationJobQueue {
  private readonly queue: Queue<{ candidateId: string }>;
  private readonly worker: Worker<{ candidateId: string }>;
  private readonly queueEvents: QueueEvents;

  constructor(options: BullMqVerificationJobQueueOptions) {
    const connection = new IORedis(options.redisUrl, {
      maxRetriesPerRequest: null,
      enableReadyCheck: false
    });

    const queueName = "credential_verification_tier1";
    this.queue = new Queue(queueName, { connection });
    this.queueEvents = new QueueEvents(queueName, { connection });
    this.worker = new Worker(
      queueName,
      async (job: Job<{ candidateId: string }>) => {
        await options.processor(job.data.candidateId);
      },
      { connection }
    );
  }

  async enqueueTier1(candidateId: string): Promise<VerificationJobSnapshot> {
    const job = await this.queue.add(
      "run-tier1-compliance",
      { candidateId },
      { attempts: 3, removeOnComplete: 1000, removeOnFail: 1000 }
    );

    const now = new Date().toISOString();

    return {
      id: job.id as string,
      status: "QUEUED",
      candidateId,
      createdAt: now,
      updatedAt: now
    };
  }

  async getJob(jobId: string): Promise<VerificationJobSnapshot> {
    const job = await this.queue.getJob(jobId);
    if (!job) {
      throw new HttpError(404, `Verification job ${jobId} was not found.`);
    }

    const state = await job.getState();
    const status = this.mapState(state);
    const now = new Date().toISOString();

    return {
      id: String(job.id),
      status,
      candidateId: job.data.candidateId,
      createdAt: job.timestamp ? new Date(job.timestamp).toISOString() : now,
      updatedAt: now,
      error: job.failedReason || undefined
    };
  }

  private mapState(state: string): VerificationJobSnapshot["status"] {
    if (state === "waiting" || state === "delayed") {
      return "QUEUED";
    }

    if (state === "active") {
      return "RUNNING";
    }

    if (state === "completed") {
      return "COMPLETED";
    }

    if (state === "failed") {
      return "FAILED";
    }

    return "UNKNOWN";
  }
}
