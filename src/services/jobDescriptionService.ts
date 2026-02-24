import { JobDescription } from "../domain/jobDescription";
import { HttpError } from "../http/httpError";
import { LinkedInProvider } from "../linkedin/provider";
import { CreateJobDescriptionInput, JobDescriptionRepository } from "../repositories/jobDescriptionRepository";
import { CandidateService } from "./candidateService";

const inferStateFromLocation = (location?: string): string | undefined => {
  if (!location || !location.includes(",")) {
    return undefined;
  }

  const chunks = location.split(",");
  return chunks[chunks.length - 1].trim() || undefined;
};

export class JobDescriptionService {
  constructor(
    private readonly repository: JobDescriptionRepository,
    private readonly candidateService: CandidateService,
    private readonly linkedInProvider: LinkedInProvider
  ) {}

  async createJobDescription(input: CreateJobDescriptionInput): Promise<JobDescription> {
    return this.repository.create(input);
  }

  async listJobDescriptions(): Promise<JobDescription[]> {
    return this.repository.list();
  }

  async getJobDescription(jobDescriptionId: string): Promise<JobDescription> {
    const jobDescription = await this.repository.findById(jobDescriptionId);
    if (!jobDescription) {
      throw new HttpError(404, `Job description ${jobDescriptionId} was not found.`);
    }

    return jobDescription;
  }

  getLinkedInStatus(): { connected: boolean; mode: "default" | "disabled" } {
    return {
      connected: this.linkedInProvider.connected,
      mode: this.linkedInProvider.connected ? "default" : "disabled"
    };
  }

  async runLinkedInSearch(jobDescriptionId: string): Promise<JobDescription> {
    const jd = await this.getJobDescription(jobDescriptionId);

    jd.steps.linkedinSearch = {
      status: "RUNNING",
      details: "Searching candidates on LinkedIn...",
      lastRunAt: new Date().toISOString()
    };
    await this.repository.save(jd);

    try {
      const candidates = await this.linkedInProvider.searchByJobDescription(jd);

      jd.linkedinCandidates = candidates;
      jd.importedCandidateIds = [];
      jd.verificationJobIds = [];
      jd.steps.linkedinSearch = {
        status: "COMPLETED",
        details: `Found ${candidates.length} candidates.`,
        lastRunAt: new Date().toISOString()
      };
      jd.steps.importCandidates = { status: "NOT_STARTED" };
      jd.steps.runVerification = { status: "NOT_STARTED" };
      await this.repository.save(jd);
      return jd;
    } catch (error) {
      jd.steps.linkedinSearch = {
        status: "FAILED",
        details: error instanceof Error ? error.message : "LinkedIn search failed.",
        lastRunAt: new Date().toISOString()
      };
      await this.repository.save(jd);
      return jd;
    }
  }

  async runImportCandidates(jobDescriptionId: string): Promise<JobDescription> {
    const jd = await this.getJobDescription(jobDescriptionId);
    if (jd.linkedinCandidates.length === 0) {
      throw new HttpError(400, "No LinkedIn candidates found. Run LinkedIn Search first.");
    }

    jd.steps.importCandidates = {
      status: "RUNNING",
      details: "Importing candidates to pipeline...",
      lastRunAt: new Date().toISOString()
    };
    await this.repository.save(jd);

    try {
      const importedIds: string[] = [];
      for (const preview of jd.linkedinCandidates) {
        const created = await this.candidateService.createCandidate({
          fullName: preview.fullName,
          state: inferStateFromLocation(preview.location),
          source: "LINKEDIN",
          headline: preview.headline,
          linkedinProfileUrl: preview.profileUrl,
          jobDescriptionId: jd.id
        });
        importedIds.push(created.id);
      }

      jd.importedCandidateIds = importedIds;
      jd.verificationJobIds = [];
      jd.steps.importCandidates = {
        status: "COMPLETED",
        details: `Imported ${importedIds.length} candidates into pipeline.`,
        lastRunAt: new Date().toISOString()
      };
      jd.steps.runVerification = { status: "NOT_STARTED" };
      await this.repository.save(jd);
      return jd;
    } catch (error) {
      jd.steps.importCandidates = {
        status: "FAILED",
        details: error instanceof Error ? error.message : "Candidate import failed.",
        lastRunAt: new Date().toISOString()
      };
      await this.repository.save(jd);
      return jd;
    }
  }

  async runVerification(jobDescriptionId: string): Promise<JobDescription> {
    const jd = await this.getJobDescription(jobDescriptionId);
    if (jd.importedCandidateIds.length === 0) {
      throw new HttpError(400, "No imported candidates found. Run Import Candidates first.");
    }

    jd.steps.runVerification = {
      status: "RUNNING",
      details: "Starting compliance checks for imported candidates...",
      lastRunAt: new Date().toISOString()
    };
    await this.repository.save(jd);

    try {
      const jobIds: string[] = [];
      for (const candidateId of jd.importedCandidateIds) {
        const job = await this.candidateService.enqueueTier1Compliance(candidateId);
        jobIds.push(job.id);
      }

      jd.verificationJobIds = jobIds;
      jd.steps.runVerification = {
        status: "COMPLETED",
        details: `Verification jobs queued for ${jobIds.length} candidates.`,
        lastRunAt: new Date().toISOString()
      };
      await this.repository.save(jd);
      return jd;
    } catch (error) {
      jd.steps.runVerification = {
        status: "FAILED",
        details: error instanceof Error ? error.message : "Verification run failed.",
        lastRunAt: new Date().toISOString()
      };
      await this.repository.save(jd);
      return jd;
    }
  }

  async getImportedCandidates(jobDescriptionId: string) {
    const jd = await this.getJobDescription(jobDescriptionId);
    return this.candidateService.listCandidatesByIds(jd.importedCandidateIds);
  }
}
