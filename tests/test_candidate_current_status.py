from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.matching import MatchingEngine
from tener_ai.pre_resume_service import PreResumeCommunicationService
from tener_ai.workflow import WorkflowService


class _DeliveredProvider:
    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        return []

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {"provider": "stub", "sent": True, "chat_id": "chat-status-1"}

    def send_connection_request(self, candidate_profile: Dict[str, Any], message: str | None = None) -> Dict[str, Any]:
        return {"provider": "stub", "sent": True, "request_id": "req-1"}

    def check_connection_status(self, candidate_profile: Dict[str, Any]) -> Dict[str, Any]:
        return {"provider": "stub", "connected": True}


class CandidateCurrentStatusTests(unittest.TestCase):
    def test_candidate_status_progression_added_outreached_dialogue_cv_received(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "candidate_current_status.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(_DeliveredProvider()),  # type: ignore[arg-type]
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                pre_resume_service=PreResumeCommunicationService(
                    templates_path=str(root / "config" / "outreach_templates.json")
                ),
                contact_all_mode=True,
                require_resume_before_final_verify=True,
                stage_instructions={"pre_resume": "request cv and track status"},
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python and AWS",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            profile = {
                "linkedin_id": "ln-status-1",
                "unipile_profile_id": "ln-status-1",
                "attendee_provider_id": "ln-status-1",
                "full_name": "Status Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": [],
                "years_experience": 5,
                "raw": {},
            }
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[{"profile": profile, "score": 0.61, "status": "needs_resume", "notes": {}}],
            )
            candidate_id = int(added["added"][0]["candidate_id"])

            rows = db.list_candidates_for_job(job_id)
            self.assertEqual(rows[0]["current_status_key"], "added")

            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            self.assertEqual(outreach["sent"], 1)
            conversation_id = int(outreach["items"][0]["conversation_id"])

            rows = db.list_candidates_for_job(job_id)
            self.assertEqual(rows[0]["current_status_key"], "outreached")

            workflow.process_inbound_message(conversation_id=conversation_id, text="Tell me more")
            rows = db.list_candidates_for_job(job_id)
            self.assertEqual(rows[0]["current_status_key"], "in_dialogue")

            workflow.process_inbound_message(
                conversation_id=conversation_id,
                text="Here is my resume https://example.com/status-candidate-cv.pdf",
            )
            rows = db.list_candidates_for_job(job_id)
            self.assertEqual(rows[0]["current_status_key"], "cv_received")
            self.assertEqual(rows[0]["current_status_label"], "CV Received")


if __name__ == "__main__":
    unittest.main()
