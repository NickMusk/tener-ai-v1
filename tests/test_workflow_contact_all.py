import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.matching import MatchingEngine
from tener_ai.workflow import WorkflowService


class _StubLinkedInProvider:
    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {"sent": False, "provider": "stub", "error": "no_connection"}


class WorkflowContactAllTests(unittest.TestCase):
    def test_contact_all_mode_converts_reject_to_needs_resume_and_requests_cv(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "workflow_contact_all.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = _StubLinkedInProvider()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(provider),
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                contact_all_mode=True,
                require_resume_before_final_verify=True,
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python, AWS, Docker, distributed systems, microservices.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            profile = {
                "linkedin_id": "ln_contact_all_1",
                "full_name": "Low Evidence Candidate",
                "headline": "Software Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": [],
                "years_experience": 1,
                "raw": {},
            }

            verify = workflow.verify_profiles(job_id=job_id, profiles=[profile])
            self.assertEqual(verify["verified"], 0)
            self.assertEqual(verify["rejected"], 0)
            self.assertEqual(verify["needs_resume"], 1)
            self.assertEqual(verify["items"][0]["status"], "needs_resume")

            added = workflow.add_verified_candidates(job_id=job_id, verified_items=verify["items"])
            self.assertEqual(added["total"], 1)
            self.assertEqual(added["added"][0]["status"], "needs_resume")

            saved = db.list_candidates_for_job(job_id)
            self.assertEqual(len(saved), 1)
            self.assertEqual(saved[0]["status"], "needs_resume")

            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[added["added"][0]["candidate_id"]])
            self.assertEqual(outreach["total"], 1)
            self.assertTrue(outreach["items"][0]["request_resume"])
            self.assertEqual(outreach["items"][0]["screening_status"], "needs_resume")

            messages = db.list_messages(outreach["conversation_ids"][0])
            self.assertEqual(len(messages), 1)
            meta = messages[0].get("meta") or {}
            self.assertTrue(meta.get("request_resume"))
            self.assertIn("CV", messages[0].get("content", ""))


if __name__ == "__main__":
    unittest.main()
