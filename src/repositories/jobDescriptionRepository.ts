import { JobDescription } from "../domain/jobDescription";

export interface CreateJobDescriptionInput {
  title: string;
  company: string;
  location?: string;
  keywords?: string;
}

export interface JobDescriptionRepository {
  init?(): Promise<void>;
  create(input: CreateJobDescriptionInput): Promise<JobDescription>;
  save(jobDescription: JobDescription): Promise<void>;
  findById(id: string): Promise<JobDescription | undefined>;
  list(): Promise<JobDescription[]>;
}
