import { JobDescription, LinkedInCandidatePreview } from "../domain/jobDescription";

export interface LinkedInProvider {
  readonly connected: boolean;
  searchByJobDescription(jobDescription: JobDescription): Promise<LinkedInCandidatePreview[]>;
}
