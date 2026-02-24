import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.matching import MatchingEngine
from tener_ai.workflow import DEFAULT_FORCED_TEST_SCORE, WorkflowService

FORCED_TEST_ID = "olena-bachek-b8523121a"


class FakeUnipileProvider:
    def __init__(self) -> None:
        self.queries: List[str] = []

    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        self.queries.append(query)
        if query == FORCED_TEST_ID:
            return [
                {
                    "linkedin_id": "ACoAATestProvider123",
                    "unipile_profile_id": "ACoAATestProvider123",
                    "attendee_provider_id": "ACoAATestProvider123",
                    "full_name": "Olena Bachek",
                    "headline": "Backend Engineer",
                    "location": "Poland",
                    "languages": ["en"],
                    "skills": [],
                    "years_experience": 1,
                    "raw": {
                        "public_identifier": FORCED_TEST_ID,
                    },
                }
            ]
        return [
            {
                "linkedin_id": "regular-candidate-1",
                "full_name": "Regular Candidate",
                "headline": "Backend Developer",
                "location": "Remote",
                "languages": ["en"],
                "skills": [],
                "years_experience": 1,
                "raw": {},
            }
        ]

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        enriched = dict(profile)
        enriched["linkedin_id"] = "ACoAADc0-FUBAMKDmKggoixvfVaLiocMh19_JDU"
        enriched["attendee_provider_id"] = "ACoAADc0-FUBAMKDmKggoixvfVaLiocMh19_JDU"
        enriched["unipile_profile_id"] = "ACoAADc0-FUBAMKDmKggoixvfVaLiocMh19_JDU"
        enriched["raw"] = {
            "search": {"forced_test_candidate": True, "public_identifier": FORCED_TEST_ID},
            "detail": {"first_name": "Olena", "last_name": "Bachek"},
        }
        return enriched

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {"provider": "fake", "sent": False}


class ForcedTestCandidateTests(unittest.TestCase):
    def test_forced_profile_is_injected_and_gets_high_score(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "forced_test_candidate.sqlite3"))
            db.init_schema()
            ids_file = Path(td) / "forced_ids.txt"
            ids_file.write_text(f"{FORCED_TEST_ID}\n", encoding="utf-8")

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = FakeUnipileProvider()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(provider),  # type: ignore[arg-type]
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                forced_test_ids_path=str(ids_file),
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python AWS distributed systems",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )

            source = workflow.source_candidates(job_id=job_id, limit=1)
            self.assertEqual(source["total"], 1)
            self.assertEqual(provider.queries[-1], FORCED_TEST_ID)
            forced_profile = source["profiles"][0]
            self.assertEqual((forced_profile.get("raw") or {}).get("public_identifier"), FORCED_TEST_ID)

            verify = workflow.verify_profiles(job_id=job_id, profiles=source["profiles"])
            self.assertEqual(verify["total"], 1)
            item = verify["items"][0]
            self.assertEqual(item["status"], "verified")
            self.assertGreaterEqual(item["score"], DEFAULT_FORCED_TEST_SCORE)
            self.assertTrue((item.get("notes") or {}).get("forced_test_candidate"))


if __name__ == "__main__":
    unittest.main()
