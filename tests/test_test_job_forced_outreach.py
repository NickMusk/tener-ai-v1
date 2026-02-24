from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.matching import MatchingEngine
from tener_ai.workflow import WorkflowService

FORCED_TEST_ID = "olena-bachek-b8523121a"
FORCED_PROVIDER_ID = "ACoAADc0-FUBAMKDmKggoixvfVaLiocMh19_JDU"


class _Provider:
    def __init__(self) -> None:
        self.sent_to: List[str] = []

    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        return []

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        self.sent_to.append(str(candidate_profile.get("linkedin_id") or ""))
        return {"provider": "stub", "sent": True, "chat_id": f"chat-{len(self.sent_to)}"}

    def send_connection_request(self, candidate_profile: Dict[str, Any], message: str | None = None) -> Dict[str, Any]:
        return {"provider": "stub", "sent": True, "request_id": "req-1"}

    def check_connection_status(self, candidate_profile: Dict[str, Any]) -> Dict[str, Any]:
        return {"provider": "stub", "connected": True}


class TestJobForcedOutreachTests(unittest.TestCase):
    def test_test_job_outreach_sends_only_to_forced_candidates(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "test_job_forced.sqlite3"))
            db.init_schema()
            ids_file = Path(td) / "forced_ids.txt"
            ids_file.write_text(f"{FORCED_TEST_ID}\n", encoding="utf-8")

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = _Provider()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(provider),  # type: ignore[arg-type]
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                forced_test_ids_path=str(ids_file),
            )

            job_id = db.insert_job(
                title="Prod smoke job",
                jd_text="Senior Backend Engineer with Python and AWS",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )

            forced_profile = {
                "linkedin_id": FORCED_PROVIDER_ID,
                "unipile_profile_id": FORCED_PROVIDER_ID,
                "attendee_provider_id": FORCED_PROVIDER_ID,
                "full_name": "Forced Candidate",
                "headline": "Backend",
                "location": "Remote",
                "languages": ["en"],
                "skills": [],
                "years_experience": 5,
                "raw": {"public_identifier": FORCED_TEST_ID, "forced_test_candidate": True},
            }
            regular_profile = {
                "linkedin_id": "regular-candidate-1",
                "unipile_profile_id": "regular-candidate-1",
                "attendee_provider_id": "regular-candidate-1",
                "full_name": "Regular Candidate",
                "headline": "Backend",
                "location": "Remote",
                "languages": ["en"],
                "skills": [],
                "years_experience": 5,
                "raw": {},
            }

            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[
                    {
                        "profile": forced_profile,
                        "score": 0.99,
                        "status": "verified",
                        "notes": {"forced_test_candidate": True, "forced_test_identifier": FORCED_TEST_ID},
                    },
                    {"profile": regular_profile, "score": 0.7, "status": "verified", "notes": {}},
                ],
            )

            candidate_ids = [item["candidate_id"] for item in added["added"]]
            out = workflow.outreach_candidates(job_id=job_id, candidate_ids=candidate_ids)

            self.assertTrue(out.get("test_job_forced_only_active"))
            self.assertEqual(out.get("test_filter_skipped"), 1)
            self.assertEqual(out.get("total"), 1)
            self.assertEqual(provider.sent_to, [FORCED_PROVIDER_ID])


if __name__ == "__main__":
    unittest.main()
