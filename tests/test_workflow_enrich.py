import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.matching import MatchingEngine
from tener_ai.workflow import WorkflowService


class _FakeProvider:
    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        return []

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        enriched = dict(profile)
        enriched["headline"] = "Senior Backend Engineer | Python AWS"
        enriched["skills"] = ["python", "aws", "docker"]
        enriched["years_experience"] = 8
        raw = dict(enriched.get("raw") or {})
        raw["enriched"] = True
        enriched["raw"] = raw
        return enriched

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {"provider": "fake", "sent": False}


class WorkflowEnrichTests(unittest.TestCase):
    def test_verify_uses_enriched_profiles(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "db.sqlite3"))
            db.init_schema()
            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = _FakeProvider()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(provider),  # type: ignore[arg-type]
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python, AWS, Docker",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            profile = {
                "linkedin_id": "ln_enrich_1",
                "full_name": "Enrich Candidate",
                "headline": "",
                "location": "Dubai",
                "languages": ["en"],
                "skills": [],
                "years_experience": 0,
                "raw": {},
            }

            result = workflow.verify_profiles(job_id=job_id, profiles=[profile])
            self.assertEqual(result["total"], 1)
            self.assertEqual(result["enriched_total"], 1)
            self.assertEqual(result["enrich_failed"], 0)

            item = result["items"][0]
            self.assertTrue(item["profile"]["raw"].get("enriched"))
            self.assertIn("python", item["profile"]["skills"])
            self.assertEqual(item["status"], "verified")


if __name__ == "__main__":
    unittest.main()
