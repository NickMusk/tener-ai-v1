from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

from tener_ai import main as api_main
from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.matching import MatchingEngine
from tener_ai.workflow import WorkflowService


class _ConnectionAwareProvider:
    def __init__(self) -> None:
        self.connected = False
        self.connection_requests: List[str] = []
        self.sent_messages: List[str] = []

    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        return []

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        self.sent_messages.append(message)
        if not self.connected:
            return {
                "sent": False,
                "provider": "linkedin",
                "error": "Unipile HTTP error 422: errors/no_connection_with_recipient recipient is not first degree connection",
            }
        return {"sent": True, "provider": "linkedin", "chat_id": "chat-scheduler-connected"}

    def send_connection_request(self, candidate_profile: Dict[str, Any], message: str | None = None) -> Dict[str, Any]:
        self.connection_requests.append(message or "")
        return {"sent": True, "provider": "linkedin", "request_id": "req-scheduler-1"}

    def check_connection_status(self, candidate_profile: Dict[str, Any]) -> Dict[str, Any]:
        return {"connected": self.connected, "provider": "stub"}


class OutreachSchedulerTests(unittest.TestCase):
    def test_connection_poll_scheduler_tick_advances_waiting_connection_conversations(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outreach_scheduler.sqlite3"))
            db.init_schema()
            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = _ConnectionAwareProvider()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(provider),
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                contact_all_mode=True,
                require_resume_before_final_verify=True,
            )

            previous_services = api_main.SERVICES
            api_main.SERVICES = {"db": db, "workflow": workflow}
            try:
                job_id = db.insert_job(
                    title="Senior Backend Engineer",
                    jd_text="Need Python, AWS and distributed systems.",
                    location="Remote",
                    preferred_languages=["en"],
                    seniority="senior",
                )
                profile = {
                    "linkedin_id": "ln-scheduler-1",
                    "full_name": "Scheduler Candidate",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": [],
                    "years_experience": 4,
                    "raw": {},
                }
                added = workflow.add_verified_candidates(
                    job_id=job_id,
                    verified_items=[{"profile": profile, "score": 0.51, "status": "needs_resume", "notes": {}}],
                )
                candidate_id = int(added["added"][0]["candidate_id"])

                outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
                self.assertEqual(outreach["pending_connection"], 1)
                conversation_id = int(outreach["items"][0]["conversation_id"])
                self.assertEqual(str((db.get_conversation(conversation_id) or {}).get("status") or ""), "waiting_connection")

                provider.connected = True
                result = api_main._run_outreach_connection_poll_scheduler_tick(poll_limit=20)

                self.assertEqual(int(result.get("checked") or 0), 1)
                self.assertEqual(int(result.get("connected") or 0), 1)
                self.assertEqual(int(result.get("sent") or 0), 1)
                self.assertEqual(str((db.get_conversation(conversation_id) or {}).get("status") or ""), "active")
                self.assertEqual(str((db.get_candidate_match(job_id=job_id, candidate_id=candidate_id) or {}).get("status") or ""), "outreach_sent")

                logs = db.list_logs(limit=20)
                scheduler_logs = [
                    row for row in logs if str(row.get("operation") or "") == "scheduler.outreach.poll_connections"
                ]
                self.assertTrue(scheduler_logs)
                self.assertEqual(int((scheduler_logs[0].get("details") or {}).get("connected") or 0), 1)
                self.assertEqual(int((scheduler_logs[0].get("details") or {}).get("sent") or 0), 1)
            finally:
                api_main.SERVICES = previous_services

    def test_connection_poll_scheduler_skips_paused_jobs(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outreach_scheduler_paused.sqlite3"))
            db.init_schema()
            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = _ConnectionAwareProvider()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(provider),
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                contact_all_mode=True,
                require_resume_before_final_verify=True,
            )

            previous_services = api_main.SERVICES
            api_main.SERVICES = {"db": db, "workflow": workflow}
            try:
                job_id = db.insert_job(
                    title="Senior Backend Engineer",
                    jd_text="Need Python, AWS and distributed systems.",
                    location="Remote",
                    preferred_languages=["en"],
                    seniority="senior",
                )
                profile = {
                    "linkedin_id": "ln-scheduler-paused-1",
                    "full_name": "Paused Scheduler Candidate",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": [],
                    "years_experience": 4,
                    "raw": {},
                }
                added = workflow.add_verified_candidates(
                    job_id=job_id,
                    verified_items=[{"profile": profile, "score": 0.51, "status": "needs_resume", "notes": {}}],
                )
                candidate_id = int(added["added"][0]["candidate_id"])
                outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
                conversation_id = int(outreach["items"][0]["conversation_id"])

                db.pause_job(job_id=job_id, reason="ops")
                provider.connected = True
                result = api_main._run_outreach_connection_poll_scheduler_tick(poll_limit=20)

                self.assertEqual(int(result.get("checked") or 0), 1)
                self.assertEqual(int(result.get("skipped") or 0), 1)
                self.assertEqual(int(result.get("sent") or 0), 0)
                self.assertEqual(str((db.get_conversation(conversation_id) or {}).get("status") or ""), "waiting_connection")
            finally:
                api_main.SERVICES = previous_services


if __name__ == "__main__":
    unittest.main()
