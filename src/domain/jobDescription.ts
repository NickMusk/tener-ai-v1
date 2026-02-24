export type StepStatus = "NOT_STARTED" | "RUNNING" | "COMPLETED" | "FAILED";

export interface JobStepState {
  status: StepStatus;
  lastRunAt?: string;
  details?: string;
}

export interface LinkedInCandidatePreview {
  id: string;
  fullName: string;
  headline: string;
  location: string;
  profileUrl: string;
}

export interface JobDescription {
  id: string;
  title: string;
  company: string;
  location?: string;
  keywords?: string;
  createdAt: string;
  updatedAt: string;
  steps: {
    linkedinSearch: JobStepState;
    importCandidates: JobStepState;
    runVerification: JobStepState;
  };
  linkedinCandidates: LinkedInCandidatePreview[];
  importedCandidateIds: string[];
  verificationJobIds: string[];
}
