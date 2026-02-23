from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

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
        enriched = dict(profile)
        raw = dict(enriched.get("raw") or {})
        raw["network_distance"] = "FIRST_DEGREE" if self.connected else "SECOND_DEGREE"
        enriched["raw"] = raw
        return enriched

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        self.sent_messages.append(message)
        if not self.connected:
            return {
                "sent": False,
                "provider": "linkedin",
                "error": "Unipile HTTP error 422: errors/no_connection_with_recipient recipient is not first degree connection",
            }
        return {"sent": True, "provider": "linkedin", "chat_id": "chat-connected"}

    def send_connection_request(self, candidate_profile: Dict[str, Any], message: str | None = None) -> Dict[str, Any]:
        self.connection_requests.append(message or "")
        return {"sent": True, "provider": "linkedin", "request_id": "req-1"}

    def check_connection_status(self, candidate_profile: Dict[str, Any]) -> Dict[str, Any]:
        return {"connected": self.connected, "provider": "stub"}


class OutreachConnectionFlowTests(unittest.TestCase):
    def test_outreach_queues_message_until_connection_is_accepted(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outreach_connection.sqlite3"))
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

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python, AWS and distributed systems.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            profile = {
                "linkedin_id": "ln-conn-1",
                "full_name": "Conn Candidate",
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
            self.assertEqual(outreach["sent"], 0)
            self.assertEqual(outreach["pending_connection"], 1)
            self.assertEqual(outreach["failed"], 0)
            self.assertEqual(len(provider.connection_requests), 1)
            self.assertEqual(outreach["items"][0]["delivery_status"], "pending_connection")

            conversation_id = int(outreach["items"][0]["conversation_id"])
            conversation = db.get_conversation(conversation_id)
            self.assertEqual(conversation["status"], "waiting_connection")

            provider.connected = True
            polled = workflow.poll_pending_connections(job_id=job_id, limit=20)
            self.assertEqual(polled["checked"], 1)
            self.assertEqual(polled["connected"], 1)
            self.assertEqual(polled["sent"], 1)
            self.assertEqual(polled["failed"], 0)

            updated_conversation = db.get_conversation(conversation_id)
            self.assertEqual(updated_conversation["status"], "active")

            match = db.get_candidate_match(job_id=job_id, candidate_id=candidate_id)
            self.assertEqual(match["status"], "outreach_sent")

            messages = db.list_messages(conversation_id)
            last_outbound = [m for m in messages if m.get("direction") == "outbound"][-1]
            self.assertEqual((last_outbound.get("meta") or {}).get("type"), "outreach_after_connection")
            self.assertTrue((last_outbound.get("meta") or {}).get("delivery", {}).get("sent"))


if __name__ == "__main__":
    unittest.main()
