import unittest
from pathlib import Path

from tener_ai.matching import MatchingEngine


class MatchingEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        root = Path(__file__).resolve().parents[1]
        self.engine = MatchingEngine(str(root / "config" / "matching_rules.json"))

    def test_verifies_relevant_candidate(self) -> None:
        job = {
            "title": "Senior Backend Engineer",
            "jd_text": "Need Python, Django, AWS, Docker, PostgreSQL",
            "location": "Germany",
            "preferred_languages": ["en"],
            "seniority": "senior",
        }
        profile = {
            "linkedin_id": "ln_test_1",
            "full_name": "Alex",
            "headline": "Senior Backend Engineer",
            "location": "Berlin, Germany",
            "languages": ["en"],
            "skills": ["python", "django", "aws", "docker", "postgresql"],
            "years_experience": 8,
        }

        result = self.engine.verify(job=job, profile=profile)
        self.assertEqual(result.status, "verified")
        self.assertGreaterEqual(result.score, 0.8)

    def test_rejects_missing_fields(self) -> None:
        job = {
            "title": "Any",
            "jd_text": "Need Python",
            "preferred_languages": ["en"],
        }
        profile = {
            "linkedin_id": "ln_test_2",
            "full_name": "No Skills Candidate",
            "headline": "Engineer",
            "languages": ["en"],
            "years_experience": 3,
        }

        result = self.engine.verify(job=job, profile=profile)
        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.score, 0.0)

    def test_remote_location_does_not_penalize(self) -> None:
        job = {
            "title": "Senior Backend Engineer",
            "jd_text": "Need Python, AWS, Docker",
            "location": "Remote",
            "preferred_languages": ["en"],
            "seniority": "senior",
        }
        profile = {
            "linkedin_id": "ln_test_remote",
            "full_name": "Remote Candidate",
            "headline": "Senior Backend Engineer 8 years experience",
            "location": "Dubai",
            "languages": ["en"],
            "skills": ["python", "aws", "docker"],
            "years_experience": 8,
        }
        result = self.engine.verify(job=job, profile=profile)
        self.assertEqual(result.notes["components"]["location_match"], 1.0)

    def test_explicit_job_requirements_override_jd_parsing(self) -> None:
        job = {
            "title": "Manual QA Engineer",
            "jd_text": "About Tener.ai platform with Go, Docker, LLM and recruiting automation copy.",
            "location": "Remote",
            "preferred_languages": ["en"],
            "seniority": "junior",
            "must_have_skills": ["manual testing", "api testing", "regression"],
            "nice_to_have_skills": ["sql", "postman"],
        }

        requirements = self.engine.build_job_requirements(job)
        self.assertEqual(requirements["must_have_skills"], ["manual testing", "api testing", "regression"])
        self.assertEqual(requirements["nice_to_have_skills"], ["sql", "postman"])

    def test_nice_to_have_missing_does_not_force_reject(self) -> None:
        job = {
            "title": "Manual QA Engineer",
            "jd_text": "Manual QA role",
            "location": "Remote",
            "preferred_languages": ["en"],
            "seniority": "junior",
            "must_have_skills": ["manual testing", "api testing"],
            "nice_to_have_skills": ["sql"],
        }
        profile = {
            "linkedin_id": "ln_test_qa",
            "full_name": "QA Candidate",
            "headline": "Manual QA Engineer",
            "location": "Remote",
            "languages": ["en"],
            "skills": ["manual testing", "api testing"],
            "years_experience": 2,
        }

        result = self.engine.verify(job=job, profile=profile)
        self.assertEqual(result.status, "verified")
        self.assertEqual(result.notes["required_skills"], ["manual testing", "api testing"])
        self.assertEqual(result.notes["nice_to_have_skills"], ["sql"])
        self.assertEqual(result.notes["matched_nice_to_have_skills"], [])
        self.assertGreaterEqual(result.notes["components"]["must_have_match"], 1.0)


if __name__ == "__main__":
    unittest.main()
