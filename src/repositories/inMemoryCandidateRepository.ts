import { randomUUID } from "crypto";
import { Candidate, CandidateProfile } from "../domain/model";
import { CandidateRepository } from "./candidateRepository";
import { buildPendingChecks, calculateProgress, calculateTrafficLight } from "../verification/trafficLight";

export class InMemoryCandidateRepository implements CandidateRepository {
  private readonly store = new Map<string, Candidate>();

  async create(profile: CandidateProfile): Promise<Candidate> {
    const now = new Date().toISOString();
    const checks = buildPendingChecks();

    const candidate: Candidate = {
      id: randomUUID(),
      profile,
      compliance: {
        checks,
        progress: calculateProgress(checks),
        trafficLight: calculateTrafficLight(checks)
      },
      createdAt: now,
      updatedAt: now
    };

    this.store.set(candidate.id, candidate);
    return candidate;
  }

  async save(candidate: Candidate): Promise<void> {
    candidate.updatedAt = new Date().toISOString();
    this.store.set(candidate.id, candidate);
  }

  async findById(id: string): Promise<Candidate | undefined> {
    return this.store.get(id);
  }

  async list(): Promise<Candidate[]> {
    return [...this.store.values()];
  }
}
