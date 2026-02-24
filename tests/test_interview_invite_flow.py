from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.matching import MatchingEngine
from tener_ai.pre_resume_service import PreResumeCommunicationService
from tener_ai.workflow import WorkflowService


class _LinkedInProvider:
    def __init__(self) -> None:
        self.sent: List[Dict[str, Any]] = []

    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        return []

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        self.sent.append(
            {
                "linkedin_id": str(candidate_profile.get("linkedin_id") or ""),
                "message": message,
            }
        )
        return {"provider": "stub", "sent": True, "chat_id": f"chat-{len(self.sent)}"}

    def send_connection_request(self, candidate_profile: Dict[str, Any], message: str | None = None) -> Dict[str, Any]:
        return {"provider": "stub", "sent": True, "request_id": "req-1"}

    def check_connection_status(self, candidate_profile: Dict[str, Any]) -> Dict[str, Any]:
        return {"provider": "stub", "connected": True}


class _InterviewClient:
    def __init__(self) -> None:
        self.sessions: Dict[str, Dict[str, Any]] = {}
        self._counter = 0

    def start_session(
        self,
        job_id: int,
        candidate_id: int,
        candidate_name: str,
        conversation_id: int,
        language: str = "en",
        candidate_email: str | None = None,
        ttl_hours: int | None = None,
    ) -> Dict[str, Any]:
        self._counter += 1
        session_id = f"iv-{self._counter}"
        entry_url = f"https://interview.local/i/{session_id}"
        self.sessions[session_id] = {
            "status": "invited",
            "summary": {"total_score": None},
            "entry_url": entry_url,
            "job_id": job_id,
            "candidate_id": candidate_id,
        }
        return {
            "session_id": session_id,
            "status": "invited",
            "entry_url": entry_url,
            "provider": {"name": "mock"},
        }

    def refresh_session(self, session_id: str, force: bool = False) -> Dict[str, Any]:
        session = self.sessions.get(session_id, {})
        return {
            "session_id": session_id,
            "status": session.get("status"),
            "summary": session.get("summary") or {},
            "entry_url": session.get("entry_url"),
        }

    def get_session(self, session_id: str) -> Dict[str, Any]:
        return self.refresh_session(session_id=session_id, force=False)


class InterviewInviteFlowTests(unittest.TestCase):
    def test_opt_in_triggers_interview_link_and_followup(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "interview_invite.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = _LinkedInProvider()
            interview = _InterviewClient()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(provider),  # type: ignore[arg-type]
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                pre_resume_service=PreResumeCommunicationService(templates_path=str(root / "config" / "outreach_templates.json")),
                interview_client=interview,
                contact_all_mode=True,
                require_resume_before_final_verify=True,
                interview_followup_delays_hours=[0.01, 0.01],
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python, AWS and distributed systems.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            profile = {
                "linkedin_id": "ln-interview-1",
                "full_name": "Candidate Interview",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": [],
                "years_experience": 4,
                "raw": {},
            }
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[{"profile": profile, "score": 0.72, "status": "needs_resume", "notes": {}}],
            )
            candidate_id = int(added["added"][0]["candidate_id"])
            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            conversation_id = int(outreach["conversation_ids"][0])

            result = workflow.process_inbound_message(
                conversation_id=conversation_id,
                text="I agree to async pre-vetting interview, send the link.",
            )
            self.assertEqual(result["mode"], "pre_resume")
            self.assertEqual(result["intent"], "pre_vetting_opt_in")
            self.assertTrue((result.get("interview") or {}).get("started"))

            row = db.list_candidates_for_job(job_id)[0]
            notes = row.get("verification_notes") if isinstance(row.get("verification_notes"), dict) else {}
            self.assertEqual(row["status"], "interview_invited")
            self.assertTrue(str((notes or {}).get("interview_session_id") or "").startswith("iv-"))
            self.assertTrue(str((notes or {}).get("interview_entry_url") or "").startswith("https://interview.local/"))

            due_at = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
            db.update_candidate_match_status(
                job_id=job_id,
                candidate_id=candidate_id,
                status="interview_invited",
                extra_notes={"interview_next_followup_at": due_at, "interview_followups_sent": 0, "interview_status": "invited"},
            )
            followup = workflow.run_due_interview_followups(job_id=job_id, limit=20)
            self.assertEqual(followup["sent"], 1)

            messages = db.list_messages(conversation_id)
            types = {(m.get("meta") or {}).get("type") for m in messages}
            self.assertIn("interview_invite", types)
            self.assertIn("interview_followup", types)

            session_id = str((notes or {}).get("interview_session_id") or "")
            interview.sessions[session_id]["status"] = "scored"
            interview.sessions[session_id]["summary"] = {"total_score": 82.5}
            sync = workflow.sync_interview_progress(job_id=job_id, limit=20, force_refresh=False)
            self.assertGreaterEqual(sync["updated"], 1)

            row_after = db.list_candidates_for_job(job_id)[0]
            notes_after = row_after.get("verification_notes") if isinstance(row_after.get("verification_notes"), dict) else {}
            self.assertEqual(row_after["status"], "interview_scored")
            self.assertAlmostEqual(float(notes_after.get("interview_total_score")), 82.5, places=2)


if __name__ == "__main__":
    unittest.main()
