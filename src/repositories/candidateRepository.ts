import { Candidate, CandidateProfile } from "../domain/model";

export interface CandidateRepository {
  init?(): Promise<void>;
  create(profile: CandidateProfile): Promise<Candidate>;
  save(candidate: Candidate): Promise<void>;
  findById(id: string): Promise<Candidate | undefined>;
  list(): Promise<Candidate[]>;
}
