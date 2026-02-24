import { JobDescription } from "./jobDescription";

export const DEFAULT_TEST_JD_ID = "jd-default-ls-smoke";

export const buildDefaultTestJobDescription = (): JobDescription => {
  const now = new Date().toISOString();

  return {
    id: DEFAULT_TEST_JD_ID,
    title: "Senior QA/QC Scientist (Test Job)",
    company: "NovaBio",
    location: "San Diego, CA",
    keywords: "gmp,fda,part 11,quality systems",
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
