import { createHash } from "crypto";
import { JobDescription, LinkedInCandidatePreview } from "../domain/jobDescription";
import { LinkedInProvider } from "./provider";

const names = [
  "Sarah Chen",
  "Mark Rivera",
  "James T. Powell",
  "Olivia Brooks",
  "Daniel Kim",
  "Priya Nair",
  "Lucas Martin",
  "Emily Zhao"
];

const headlines = [
  "Senior QA/QC Scientist",
  "Regulatory Affairs Specialist",
  "Clinical Research Associate",
  "Quality Systems Manager",
  "GxP Compliance Lead"
];

const locations = ["San Diego, CA", "Boston, MA", "Austin, TX", "Newark, NJ", "New York, NY"];

export class DefaultLinkedInProvider implements LinkedInProvider {
  readonly connected = true;

  async searchByJobDescription(jobDescription: JobDescription): Promise<LinkedInCandidatePreview[]> {
    const seed = `${jobDescription.title}:${jobDescription.company}:${jobDescription.location ?? ""}:${jobDescription.keywords ?? ""}`;
    const hash = createHash("sha256").update(seed).digest("hex");

    const result: LinkedInCandidatePreview[] = [];
    for (let index = 0; index < 5; index += 1) {
      const source = parseInt(hash.slice(index * 2, index * 2 + 2), 16);
      const name = names[source % names.length];
      const headline = headlines[(source + index) % headlines.length];
      const location = jobDescription.location || locations[(source + 3) % locations.length];
      const slug = name.toLowerCase().replace(/\s+/g, "-").replace(/[.]/g, "");

      result.push({
        id: `${slug}-${index + 1}`,
        fullName: name,
        headline,
        location,
        profileUrl: `https://www.linkedin.com/in/${slug}-${index + 1}`
      });
    }

    return result;
  }
}
