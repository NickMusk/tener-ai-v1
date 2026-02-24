import { randomUUID } from "crypto";
import { JobDescription } from "../domain/jobDescription";
import { CreateJobDescriptionInput, JobDescriptionRepository } from "./jobDescriptionRepository";

const buildInitialJobDescription = (input: CreateJobDescriptionInput): JobDescription => {
  const now = new Date().toISOString();

  return {
    id: randomUUID(),
    title: input.title,
    company: input.company,
    location: input.location,
    keywords: input.keywords,
    createdAt: now,
    updatedAt: now,
    steps: {
      linkedinSearch: { status: "NOT_STARTED" },
      importCandidates: { status: "NOT_STARTED" },
      runVerification: { status: "NOT_STARTED" }
    },
    linkedinCandidates: [],
    importedCandidateIds: [],
    verificationJobIds: []
  };
};

export class InMemoryJobDescriptionRepository implements JobDescriptionRepository {
  private readonly store = new Map<string, JobDescription>();

  async create(input: CreateJobDescriptionInput): Promise<JobDescription> {
    const jobDescription = buildInitialJobDescription(input);
    this.store.set(jobDescription.id, jobDescription);
    return jobDescription;
  }

  async save(jobDescription: JobDescription): Promise<void> {
    jobDescription.updatedAt = new Date().toISOString();
    this.store.set(jobDescription.id, jobDescription);
  }

  async findById(id: string): Promise<JobDescription | undefined> {
    return this.store.get(id);
  }

  async list(): Promise<JobDescription[]> {
    return [...this.store.values()].sort((a, b) => b.createdAt.localeCompare(a.createdAt));
  }
}
