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
            self.assertEqual(rows[0]["candidate_lifecycle_key"], "ready_for_outreach")

            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            self.assertEqual(outreach["sent"], 0)
            self.assertEqual(outreach["pending_connection"], 1)
            conversation_id = int(outreach["items"][0]["conversation_id"])

            rows = db.list_candidates_for_job(job_id)
            self.assertEqual(rows[0]["current_status_key"], "outreach_pending_connection")
            self.assertEqual(rows[0]["candidate_lifecycle_key"], "connect_sent_waiting_acceptance")

            polled = workflow.poll_pending_connections(job_id=job_id, limit=20)
            self.assertEqual(polled["connected"], 1)
            self.assertEqual(polled["sent"], 1)

            rows = db.list_candidates_for_job(job_id)
            self.assertEqual(rows[0]["current_status_key"], "outreached")
            self.assertEqual(rows[0]["candidate_lifecycle_key"], "connected_first_message_sent")

            workflow.process_inbound_message(conversation_id=conversation_id, text="Tell me more")
            rows = db.list_candidates_for_job(job_id)
            self.assertEqual(rows[0]["current_status_key"], "in_dialogue")
            self.assertEqual(rows[0]["candidate_lifecycle_key"], "dialogue_started")

            workflow.process_inbound_message(
                conversation_id=conversation_id,
                text="Here is my resume https://example.com/status-candidate-cv.pdf",
            )
            rows = db.list_candidates_for_job(job_id)
            self.assertEqual(rows[0]["current_status_key"], "cv_received")
            self.assertEqual(rows[0]["current_status_label"], "CV Received")
            self.assertEqual(rows[0]["candidate_lifecycle_key"], "resume_received")

    def test_candidate_status_marks_interview_passed_when_scored_above_threshold(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "candidate_interview_passed.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(_DeliveredProvider()),  # type: ignore[arg-type]
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                contact_all_mode=True,
                require_resume_before_final_verify=True,
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python and AWS",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            profile = {
                "linkedin_id": "ln-status-interview-pass",
                "unipile_profile_id": "ln-status-interview-pass",
                "attendee_provider_id": "ln-status-interview-pass",
                "full_name": "Interview Passed Candidate",
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
            db.update_candidate_match_status(
                job_id=job_id,
                candidate_id=candidate_id,
                status="interview_scored",
                extra_notes={"interview_status": "scored", "interview_total_score": 84.0},
            )

            rows = db.list_candidates_for_job(job_id)
            self.assertEqual(rows[0]["current_status_key"], "interview_passed")
            self.assertEqual(rows[0]["current_status_label"], "Interview Passed")
            self.assertEqual(rows[0]["candidate_lifecycle_key"], "interview_passed")
            self.assertEqual(rows[0]["candidate_lifecycle_label"], "Interview passed")
            self.assertEqual(rows[0]["candidate_lifecycle_detail"], "Score 84.0")

    def test_candidate_ats_stage_marks_interview_failed_when_scored_below_threshold(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "candidate_interview_failed.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(_DeliveredProvider()),  # type: ignore[arg-type]
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                contact_all_mode=True,
                require_resume_before_final_verify=True,
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python and AWS",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            profile = {
                "linkedin_id": "ln-status-interview-fail",
                "unipile_profile_id": "ln-status-interview-fail",
                "attendee_provider_id": "ln-status-interview-fail",
                "full_name": "Interview Failed Candidate",
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
            db.update_candidate_match_status(
                job_id=job_id,
                candidate_id=candidate_id,
                status="interview_scored",
                extra_notes={"interview_status": "scored", "interview_total_score": 44.8},
            )

            rows = db.list_candidates_for_job(job_id)
            self.assertEqual(rows[0]["current_status_key"], "interview_scored")
            stage = db.derive_candidate_ats_stage(rows[0])
            self.assertEqual(stage["ats_stage_key"], "interview_failed")
            self.assertEqual(stage["ats_stage_label"], "Interview Failed")
            self.assertEqual(stage["ats_stage_detail"], "Score 44.8")


if __name__ == "__main__":
    unittest.main()
