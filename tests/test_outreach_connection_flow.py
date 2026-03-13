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
            self.assertEqual(len(provider.sent_messages), 0)
            self.assertEqual(len(provider.connection_requests), 1)
            self.assertEqual(outreach["items"][0]["delivery_status"], "pending_connection")
            self.assertEqual(str(outreach["items"][0].get("planned_action_kind") or ""), "connect_request")

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

    def test_send_after_connection_reasserts_match_status_when_connection_is_still_required(self) -> None:
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
                "linkedin_id": "ln-conn-2",
                "full_name": "Still Pending Candidate",
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

            db.update_candidate_match_status(
                job_id=job_id,
                candidate_id=candidate_id,
                status="needs_resume",
            )

            delivery = workflow._deliver_pending_outreach_message(conversation_id, db.get_candidate(candidate_id) or {})
            self.assertFalse(bool(delivery.get("sent")))

            conversation = db.get_conversation(conversation_id)
            self.assertEqual(str((conversation or {}).get("status") or ""), "waiting_connection")

            match = db.get_candidate_match(job_id=job_id, candidate_id=candidate_id)
            self.assertEqual(str((match or {}).get("status") or ""), "outreach_pending_connection")

    def test_reconcile_waiting_connection_match_statuses_dry_run_does_not_mutate_match(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outreach_reconcile.sqlite3"))
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
            candidate_id = db.upsert_candidate(
                {
                    "linkedin_id": "ln-reconcile-dry",
                    "full_name": "Dry Run Candidate",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": [],
                    "years_experience": 4,
                    "raw": {},
                },
                source="linkedin",
            )
            db.create_candidate_match(
                job_id=job_id,
                candidate_id=candidate_id,
                score=0.51,
                status="needs_resume",
                verification_notes={},
            )
            conversation_id = db.create_conversation(job_id=job_id, candidate_id=candidate_id, channel="linkedin")
            db.update_conversation_status(conversation_id=conversation_id, status="waiting_connection")

            result = workflow.reconcile_waiting_connection_match_statuses(job_id=job_id, limit=10, dry_run=True)
            self.assertTrue(bool(result.get("dry_run")))
            self.assertEqual(int(result.get("candidates_total") or 0), 1)
            self.assertEqual(int(result.get("updated") or 0), 0)
            self.assertEqual(str(result["items"][0].get("status") or ""), "pending")
            self.assertEqual(str(result["items"][0].get("previous_status") or ""), "needs_resume")

            match = db.get_candidate_match(job_id=job_id, candidate_id=candidate_id)
            self.assertEqual(str((match or {}).get("status") or ""), "needs_resume")

    def test_reconcile_waiting_connection_match_statuses_updates_stale_match_status(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outreach_reconcile.sqlite3"))
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
            stale_candidate_id = db.upsert_candidate(
                {
                    "linkedin_id": "ln-reconcile-exec-stale",
                    "full_name": "Stale Candidate",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": [],
                    "years_experience": 4,
                    "raw": {},
                },
                source="linkedin",
            )
            healthy_candidate_id = db.upsert_candidate(
                {
                    "linkedin_id": "ln-reconcile-exec-healthy",
                    "full_name": "Healthy Candidate",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": [],
                    "years_experience": 4,
                    "raw": {},
                },
                source="linkedin",
            )
            db.create_candidate_match(
                job_id=job_id,
                candidate_id=stale_candidate_id,
                score=0.51,
                status="verified",
                verification_notes={},
            )
            db.create_candidate_match(
                job_id=job_id,
                candidate_id=healthy_candidate_id,
                score=0.51,
                status="outreach_pending_connection",
                verification_notes={},
            )
            stale_conversation_id = db.create_conversation(job_id=job_id, candidate_id=stale_candidate_id, channel="linkedin")
            db.update_conversation_status(conversation_id=stale_conversation_id, status="waiting_connection")
            healthy_conversation_id = db.create_conversation(job_id=job_id, candidate_id=healthy_candidate_id, channel="linkedin")
            db.update_conversation_status(conversation_id=healthy_conversation_id, status="waiting_connection")

            result = workflow.reconcile_waiting_connection_match_statuses(job_id=job_id, limit=10, dry_run=False)
            self.assertFalse(bool(result.get("dry_run")))
            self.assertEqual(int(result.get("candidates_total") or 0), 1)
            self.assertEqual(int(result.get("updated") or 0), 1)
            self.assertEqual(str(result["items"][0].get("previous_status") or ""), "verified")
            self.assertEqual(str(result["items"][0].get("target_status") or ""), "outreach_pending_connection")

            stale_match = db.get_candidate_match(job_id=job_id, candidate_id=stale_candidate_id)
            self.assertEqual(str((stale_match or {}).get("status") or ""), "outreach_pending_connection")
            notes = (stale_match or {}).get("verification_notes") or {}
            self.assertEqual(str(notes.get("reconciliation_reason") or ""), "waiting_connection_status_drift")
            self.assertEqual(str(notes.get("reconciliation_previous_status") or ""), "verified")

            healthy_match = db.get_candidate_match(job_id=job_id, candidate_id=healthy_candidate_id)
            self.assertEqual(str((healthy_match or {}).get("status") or ""), "outreach_pending_connection")

    def test_list_waiting_connection_status_drifts_handles_missing_last_message_timestamp(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outreach_reconcile_selector.sqlite3"))
            db.init_schema()

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python, AWS and distributed systems.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            candidate_id = db.upsert_candidate(
                {
                    "linkedin_id": "ln-reconcile-selector",
                    "full_name": "Selector Candidate",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": [],
                    "years_experience": 4,
                    "raw": {},
                },
                source="linkedin",
            )
            db.create_candidate_match(
                job_id=job_id,
                candidate_id=candidate_id,
                score=0.51,
                status="needs_resume",
                verification_notes={},
            )
            conversation_id = db.create_conversation(job_id=job_id, candidate_id=candidate_id, channel="linkedin")
            db.update_conversation_status(conversation_id=conversation_id, status="waiting_connection")

            rows = db.list_waiting_connection_status_drifts(job_id=job_id, limit=10)
            self.assertEqual(len(rows), 1)
            self.assertEqual(int(rows[0].get("conversation_id") or 0), conversation_id)
            self.assertEqual(str(rows[0].get("match_status") or ""), "needs_resume")


if __name__ == "__main__":
    unittest.main()
