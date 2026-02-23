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


if __name__ == "__main__":
    unittest.main()
